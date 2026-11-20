package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.ida.IDA
import java.util.UUID
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, CurrentAllocationsReply, CurrentAllocationsRequest, HeldVote, PendingAllocation, ReadResponse, TxAccept, TxFinalized, TxHeartbeat, TxMessage, TxPrepare, TxResolved, TxStatusRequest, TxStatusResponse}
import org.aspen_ddp.aspen.common.objects.{Key, Metadata, ObjectId, ObjectPointer, ObjectType, ReadError, ReadType, Value}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, ObjectUpdate, TransactionHorizon, RefcountUpdate, TransactionDescription, TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.{AdmissionTier, CrashRecoveryLog, CrlAdmission, StoreRecoveryState,
  TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendTiming, Commit, CommitState, Completion, Read}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{ExpiryTally, TransactionStatusCache, Tx}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.common.util.{LogRateLimit, Varint}
import scribe.Logging
import org.aspen_ddp.aspen.server.host.JoinCeiling
import org.aspen_ddp.aspen.client.{ReadLocks, ObjectState as ClientObjectState}

import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}


/** Frontend.admitPrepare's answer. Store.receiveTransactionMessage drops a prepare that is not Admitted without a
  * reply: Full while the device's CRL is past the store's lane, Oversized when the transaction's CRL bytes on this
  * store exceed CrashRecoveryLog.maxTransactionBytes, which no later prepare of it can fix (crash-recovery-log 10.20)
  */
enum PrepareAdmission:
  case Admitted, Full, Oversized

object Frontend {

  /** A repair's deletion of an object was refused because the object is in use on the store (a read, a commit or a
    * deletion in flight, or a transaction holding it): the repair keeps its entry and tries again on a later pass
    */
  final class RepairDeleteDeferred(message: String) extends Exception(message)

  /** The fields of a loaded object a repair write changes, as they were before it: metadata, data and, for a KeyValue
    * object, its bounds and the prior entry of every key the repair installs or deletes (repairWriteComplete)
    */
  private final case class PreRepairState(metadata: Metadata, data: DataBuffer,
    kv: Option[(Option[Key], Option[Key], Option[Value], Option[Value], Map[Key, Option[ValueState]])])

  /** How long a store withholds its vote from drivers' prepares while waiting for a client's object data.
    * Several client retransmissions long, since the only cost of a late abort is a client retry.
    */
  val MissingDataWait: Duration = Duration(5, SECONDS)

  /** How long a repair reconcile may wait for its object to settle (RepairSnapshot.awaitSettle, repair-process 8.2).
    * Checked on the store's heartbeat, so a parked repair that never sees its object settle answers stale up to a
    * heartbeat period later
    */
  val RepairSettleWait: FiniteDuration = FiniteDuration(1, SECONDS)

  /** How long a client's read may wait for a committed transaction it names (Read.awaitCommit) to release its locks on
    * the object (parkRead). Checked on the store's heartbeat, so a parked read is answered with the state as it then is
    * up to a heartbeat period later
    */
  val ParkedReadWait: FiniteDuration = FiniteDuration(1, SECONDS)

  /** The most reads a store parks at once. A read arriving with this many parked is answered at once */
  val MaxParkedReads: Int = 1024

  /** How long a UnackedCommits lookup may go unanswered before the store heartbeat warns of it. It is not made again:
    * its read retries until the tree is readable, and its answer always reaches this Frontend (see retryUnackedLookups)
    */
  val UnackedLookupTimeout: FiniteDuration = FiniteDuration(5, SECONDS)

  /** How far a transaction's start timestamp, which its client chose, may lead the receiving host's wall clock for the
    * store to take part in it. A prepare for an unknown transaction started further ahead is muted (Tx.muted), as at a
    * rebuilt store, and the client's retransmission is answered once the host's clock catches up. A store therefore
    * joins a transaction only if it started at most this long after the host's clock when the store first saw it. An
    * old incarnation stopped before its rebuild began, so every transaction it joined started before its rebuilt
    * store's acceptor-since time, whatever the client's clock, provided this bound plus the skew between the hosts'
    * clocks stays under RebuildingStore.AcceptorSinceMargin (paxos-transactions 10.10). It also bounds how far
    * timestamps that clients ratchet their HLCs from can run ahead of the hosts' clocks
    */
  val MaxStartLead: FiniteDuration = TransactionHorizon.MaxStartLead

  /** A rebuilt store's query of its peers' held votes (inheritedVoteLock) is repeated after this, doubling to
    * InheritedVoteQueryMaxInterval, while it holds any */
  val InheritedVoteQueryInitialInterval: FiniteDuration = FiniteDuration(1, SECONDS)
  val InheritedVoteQueryMaxInterval: FiniteDuration = FiniteDuration(30, SECONDS)

  /** How long past the horizon an unresolved Tx waits before it queries its peers' status (expiryHeartbeat). It covers
    * the start lead a store tolerates and the idle self-drive delay (the heartbeat timeout plus one heartbeat), so that
    * every store that could still join T has refused it, and every Tx that could learn T's outcome from a status cache
    * has had its driver try
    */
  def expiryMargin(txHeartbeatTimeout: FiniteDuration, heartbeatPeriod: FiniteDuration): FiniteDuration =
    MaxStartLead + txHeartbeatTimeout + heartbeatPeriod

  /** The expiry margin with the host's default 1 s heartbeat and its 5 s heartbeat timeout */
  val DefaultExpiryMargin: FiniteDuration =
    expiryMargin(FiniteDuration(5, SECONDS), FiniteDuration(1, SECONDS))

  /** The most store heartbeats between an expiring Tx's status queries: they start one heartbeat apart and double */
  val MaxExpiryQueryInterval: Int = 8

  /** An undecidable expiring Tx (ExpiryTally.Decision.Hold) is logged at its first such round, then every this many */
  val UndecidableLogRounds: Int = 60

  /** How often, at most, a store warns of a client prepare it refuses (refusePrepare). While a status cache eviction's
    * watermark is the binding part of the floor, every client prepare is refused
    */
  val RefusalLogPeriod: FiniteDuration = FiniteDuration(10, SECONDS)

  /** How often, at most, a store warns of an oversized prepare it drops (admitPrepare) */
  val OversizedLogPeriod: FiniteDuration = FiniteDuration(10, SECONDS)

  sealed abstract class ReadKind

  case class NetworkRead(clientId: ClientId, requestUUID: UUID, readType: ReadType, pointer: ObjectPointer) extends ReadKind

  case class TransactionRead(transactionId: TransactionId) extends ReadKind

  /** A repair's snapshot of the store's copy, taken before the repair reads the object */
  case class SnapshotRead(pointer: ObjectPointer, p: Promise[RepairSnapshot]) extends ReadKind

  /** A repair's install of `read`, checked against `snapshot` */
  case class ReconcileRead(snapshot: RepairSnapshot, read: ClientObjectState, p: Promise[ReconcileResult])
    extends ReadKind
}

class Frontend(val storeId: StoreId,
               val backend: Backend,
               val objectCache: ObjectCache,
               val net: Messenger,
               val crl: CrashRecoveryLog,
               val statusCache: TransactionStatusCache,
               /** The store's effective slice size limit, enforced by its transactions. None means unlimited. */
               val maxSliceSize: Option[Long] = None,
               val missingDataWait: Duration = Frontend.MissingDataWait,
               /** The admission of the store's device's CRL: see admitPrepare */
               val admission: CrlAdmission = CrlAdmission.Unlimited,
               /** Looks up recovered commit votes in the pool's UnackedCommits tree: see requestUnackedLookup */
               val unackedLookup: UnackedCommitLookup = UnackedCommitLookup.Never,
               /** Runs a task on this store's executor: how completions arriving on other threads, such as a repair's
                 * backend write, get back to the Frontend's state. Inline by default, for tests with synchronous
                 * backends
                 */
               val post: (() => Unit) => Unit = task => task(),
               /** Set for a rebuilt store (RebuildingStore.AcceptorSinceFile): its Paxos acceptor state covers only
                 * transactions started at or after it. Its old incarnation may have promised or accepted in an older
                 * one before that state was lost, and a fresh promise or vote could then let a quorum choose against
                 * the value already chosen. A prepare for an older transaction it does not know is muted (Tx.muted):
                 * proven committed, or forgotten unanswered, as if the store were offline. The prepare's start timestamp
                 * is the client's, so this holds only because no store joins a transaction started ahead of its host's
                 * clock by more than MaxStartLead (startsTooFarAhead). None: an ordinary store
                 */
               val acceptorSince: Option[HLCTimestamp] = None,
               /** Set for a rebuilt store whose inherited votes are not yet known to be released
                 * (RebuildingStore.InheritedVotesFile): from when, in wallClockMillis milliseconds, its peers' held
                 * votes are complete. See inheritedVoteLock. None: none inherited
                 */
               inheritedVotesAfter: Option[Long] = None,
               /** Asks the pool's other stores for the votes they hold: see inheritedVoteLock */
               val heldVoteQuery: HeldVoteQuery = HeldVoteQuery.Never,
               /** Recovers nothing at construction: transaction messages are held until recover is called. For
                 * stores whose backend's completion handler is set after they are built (StoreLoader), where a read
                 * a recovered transaction issued in the constructor would never complete
                 */
               deferRecovery: Boolean = false,
               /** The host's wall clock in milliseconds (startsTooFarAhead). Not HLCTimestamp.now: the host's own client
                 * ratchets the JVM-wide HLC forward from the timestamps it reads. Injected by tests
                 */
               val wallClockMillis: () => Long = () => System.currentTimeMillis(),
               /** How long after its start a transaction may be joined (forgetFloorMillis). Injected by tests */
               val horizon: FiniteDuration = TransactionHorizon.Horizon,
               /** When the store was loaded on this host, in wallClockMillis milliseconds (forgetFloorMillis). None:
                 * now. Every load builds a new Frontend, restart and transfer-in included. Injected by tests
                 */
               loadedAt: Option[Long] = None,
               /** When the store last arrived on this host from another (a transfer in, an adoption), in wallClockMillis
                 * milliseconds (StoreLoader.ArrivedAtFile; loadFloorMillis). None: never, or a new store. Injected by
                 * tests
                 */
               arrivedAt: Option[Long] = None,
               /** The host's join ceiling (server.host.JoinCeiling): the store joins only transactions started below
                 * it (startsTooFarAhead), and its previous life on this host joined none at or above the ceiling
                 * that life ran under (loadFloorMillis). Unbounded: no gate and no bound, for tests
                 */
               val joinCeiling: JoinCeiling = JoinCeiling.Unbounded,
               /** How long past the horizon an unresolved Tx starts its status queries (Frontend.expiryMargin) */
               val expiryMargin: FiniteDuration = Frontend.DefaultExpiryMargin,
               /** How long a parked re-read waits for the commit it names (parkRead). A test seam */
               val parkedReadWait: FiniteDuration = Frontend.ParkedReadWait,
               /** Called once, on the store's executor, when the store fail-stops (failStop): its host takes it
                 * offline and rebuilds it. The default only logs, for tests
                 */
               onFailStop: Throwable => Unit = _ => (),
               /** Which life of the store's data this is. None: StoreIncarnation.default(storeId). A default cannot
                 * name storeId in the same parameter list
                 */
               incarnationOverride: Option[StoreIncarnation] = None,
               /** The IDA of the store's pool (StoreConfig.ida), which a description must name for it
                 * (DescriptionValidator). None skips that check: tests only
                 */
               val poolIDA: Option[IDA] = None,
               /** The lane of `admission` this store's new transactions use: its pool's (StoreConfig.admissionTier) */
               val admissionTier: AdmissionTier = AdmissionTier.Data,
               /** Called on the store's executor when an UnackedCommits lookup finds the entry of a commit vote that
                 * now proves the commit to its driver (Tx.provesCommitToItsDriver): the Store hands the proof to a
                 * driver already running (Store.giveProof). The default does nothing, for tests
                 */
               onCommitProven: TransactionId => Unit = _ => (),
               /** Expiry decided T committed here (ExpiryTally.Decision.Commit): the Store has a driver finalize it */
               onExpiryCommit: TransactionDescription => Unit = _ => ()) extends Logging {

  /** The limit every growing KeyValue update must fit, at vote, apply re-check and a repair install beside held
    * reservations: maxSliceSize lowered to the pool IDA's IDA.maxKVSliceSize, so the union of committed updates repair
    * and rebuild install fits maxSliceSize (transaction-requirements R16). Without a pool IDA (tests) it is
    * maxSliceSize
    */
  val maxKVSliceSize: Option[Long] = maxSliceSize.map(m => poolIDA.fold(m)(_.maxKVSliceSize(m)))

  /** Which life of the store's data this is: prepare replies carry it (TxPrepareResponse.incarnation) */
  val incarnation: StoreIncarnation = incarnationOverride.getOrElse(StoreIncarnation.default(storeId))

  import Frontend._

  // A refused client prepare is warned of once per RefusalLogPeriod at most (refusePrepare)
  private val refusalLog = new LogRateLimit(RefusalLogPeriod.toMillis, wallClockMillis)

  // A dropped oversized prepare is warned of once per OversizedLogPeriod at most (admitPrepare)
  private val oversizedLog = new LogRateLimit(OversizedLogPeriod.toMillis, wallClockMillis)

  /** When the store was loaded on this host: it remembers nothing of transactions it saw before (forgetFloorMillis) */
  val loadedAtMillis: Long = loadedAt.getOrElse(wallClockMillis())

  /** The earliest start, in milliseconds of the host's wall clock, of a transaction this store may join without
    * holding it already. Below it, the store may have known the transaction and forgotten its outcome: it no longer
    * holds a Tx for it, and the host's status cache may have dropped its entry. Joining afresh could then decide it
    * again, against its outcome. The floor is the latest of: the horizon before now (the status cache outlasts it, so
    * an entry expires only once its transaction is below it); the store's load on this host (a restart, or a transfer
    * from a host whose cache this one lacks) plus a lead (loadFloorMillis); and the status cache's eviction watermark
    * (an entry dropped for size, which adds MaxStartLead the same way)
    */
  def forgetFloorMillis: Long =
    math.max(math.max(wallClockMillis() - horizon.toMillis, loadFloorMillis), statusCache.evictionWatermarkMillis)

  /** What bounds the starts of the transactions the store joined on this host before this load (JoinCeiling.boundAtLoad).
    * Taken once, as the Frontend is built, before the store can join anything
    */
  private val ceilingAtLoad: Option[Long] = joinCeiling.boundAtLoad(storeId)

  /** The load's part of the forget floor: the load plus MaxStartLead, since before its load the store may have joined,
    * and forgotten, a transaction started up to MaxStartLead ahead of its host's clock, or the host's join ceiling
    * that the store's previous life on this host ran under, if lower (JoinCeiling: a restart longer than the lead plus
    * JoinCeiling.Step then refuses only what started before the host went down plus about that much). Plus
    * RebuildingStore.AcceptorSinceMargin instead while the store's arrival from another host (a transfer in, an
    * adoption) is less than that margin before the load: it may have joined such a transaction on that host, whose
    * clock may lead this one's by up to the skew the margin also covers
    * (paxos-transactions 10.10). A restart within the margin of the arrival keeps it, as the restarted store may
    * still have forgotten one
    */
  val loadFloorMillis: Long =
    val margin = RebuildingStore.AcceptorSinceMargin.toMillis
    if arrivedAt.exists(_ + margin > loadedAtMillis) then
      loadedAtMillis + margin
    else
      // Joins on this host were below the ceiling they ran under (JoinCeiling.boundAtLoad), and those on the host it
      // arrived from below its arrival plus the margin
      val sinceLoad = loadedAtMillis + MaxStartLead.toMillis
      val onThisHost = ceilingAtLoad.fold(sinceLoad)(math.min(sinceLoad, _))
      arrivedAt.fold(onThisHost)(arrived => math.max(onThisHost, arrived + margin))

  /** Whether a transaction started now on this host's clock is at or above the load's part of the floor, so the store
    * may join it: past the load's lead (loadFloorMillis). Tests wait for it after a reload
    */
  def loadFloorPassed: Boolean = wallClockMillis() >= loadFloorMillis

  /** The transaction started below the forget floor: a prepare for it, unless the store holds it (or the status cache
    * knows it finalized, when the prepare is ignored), is refused (receivePrepare), and its designated leader does not
    * drive it (Store). So no Tx for it can arise here any more: what applied stubs rely on to expire (expireStubs)
    */
  def belowForgetFloor(txd: TransactionDescription): Boolean = belowForgetFloor(txd.startTimestamp)

  def belowForgetFloor(startTimestamp: HLCTimestamp): Boolean = startTimestamp.wallTime < forgetFloorMillis

  /** T started before this rebuilt store's acceptor (acceptorSince): its old incarnation may have promised or
    * accepted in T, and that state was lost without T being decided. For T's expiry the store is then never a
    * non-holder, whatever it holds now: it may be the only other holder of a chosen value (cross-document-review 2.2)
    */
  def mayHaveForgottenUndecided(startTimestamp: HLCTimestamp): Boolean = acceptorSince.exists(startTimestamp < _)

  /** T started more than the horizon plus the expiry margin ago: an unresolved Tx for it expires (expiryHeartbeat),
    * and a driver for it with no outcome is stopped once its store holds no Tx (Store.heartbeat)
    */
  def pastExpiry(txd: TransactionDescription): Boolean =
    Tx.pastHorizon(txd, wallClockMillis(), horizon, expiryMargin)

  /** Written only on the store's executor. `@volatile` so that `mayHoldTransaction` may read one snapshot of it from
    * another thread (the repair scan), as Store.hasTransactions reads `transactionDrivers`
    */
  @volatile var transactions: Map[TransactionId, Tx] = Map()

  /** Transactions waiting for a lock holder, by holder. See Tx.awaitableHolders */
  private val collisionWaiters = scala.collection.mutable.HashMap[TransactionId, List[Tx]]()

  /** The transaction started more than MaxStartLead ahead of this host's wall clock, or at or above the host's join
    * ceiling (a renewal fell behind, JoinCeiling.mayJoin): a prepare for it, if unknown, is muted (see MaxStartLead),
    * and its designated leader does not drive it (Store) */
  def startsTooFarAhead(txd: TransactionDescription): Boolean =
    val start = txd.startTimestamp.wallTime
    start > wallClockMillis() + MaxStartLead.toMillis || !joinCeiling.mayJoin(start)

  def holdsLocks(txid: TransactionId): Boolean = transactions.get(txid).exists(_.holdsLocks)

  /** Callable from any thread: whether the store holds `txid` live (recovered or active), or may once the CRL state it
    * is loading is recovered. Reads one snapshot of each `@volatile` field and touches no other state
    */
  def mayHoldTransaction(txid: TransactionId): Boolean = awaitingCrl || transactions.contains(txid)

  /** The store's CRL state is still to be recovered (awaitRecovery): see Store.driveTransaction */
  def recoveryPending: Boolean = awaitingCrl

  /** The allocations in this store's pool held by transactions it has not seen finalized and not seen abort, for a
    * rebuilding peer (rebuild-process 5.3). None while the CRL is still loading: transactions it does not know yet
    * may be among them. A committed transaction stays held until TxFinalized, which comes only once its
    * AllocationFinalizationAction has recorded the objects in the AllocationTree, so an allocation that may still be
    * missing from the tree is listed by every store that holds its transaction
    */
  def currentAllocations(): Option[List[PendingAllocation]] =
    if awaitingCrl then
      None
    else
      Some(transactions.valuesIterator.flatMap: tx =>
        val txd = tx.txd
        if txd.allocatingObjects.isEmpty || tx.isFinalized || tx.knownOutcome.contains(false) then
          None
        else
          val pointers = txd.allReferencedObjectsSet.iterator
            .filter(p => p.poolId == storeId.poolId && txd.allocatingObjects.contains(p.id))
            .toList
          if pointers.isEmpty then None
          else Some(PendingAllocation(txd.transactionId, tx.knownOutcome.contains(true), pointers))
      .toList)

  /** The transactions this store holds whose commit vote may still count: outcome unknown, and a vote that is commit or
    * not yet decided (a collision wait or an in-flight commit may still decide commit). A muted Tx never votes commit.
    * Each with its objects in this store's pool. Answered with currentAllocations: None while the CRL loads.
    * Also every held Tx known committed and not yet finalized (`HeldVote.committed`): a rebuilt store's lost
    * incarnation may have voted for it, and the rebuilt copy may lack its update (inheritedGrowthLocks). A committed Tx
    * is held by each of its commit voters until TxFinalized, and its finalizer marks a voter that never acknowledged
    * it before finalizing, so a transaction no store reports this way has its marks written
    */
  def heldVotes(): List[HeldVote] =
    transactions.valuesIterator.flatMap: tx =>
      val committedUnfinalized = tx.knownOutcome.contains(true) && !tx.isFinalized
      if !committedUnfinalized && (tx.muted || tx.isFinalized || tx.knownOutcome.isDefined ||
                                   tx.vote == TransactionDisposition.VoteAbort) then
        None
      else
        val txd = tx.txd
        val oids = txd.allReferencedObjectsSet.iterator.filter(_.poolId == storeId.poolId).map(_.id).toSet
        if oids.isEmpty then None
        else Some(HeldVote(txd.transactionId, txd.startTimestamp, oids, committed = committedUnfinalized))
    .toList

  def receiveCurrentAllocationsRequest(req: CurrentAllocationsRequest): Unit =
    currentAllocations().foreach: allocations =>
      net.sendClientResponse(CurrentAllocationsReply(req.fromClient, storeId, req.queryUUID, allocations, heldVotes()))

  // ---- Inherited votes (a rebuilt store; cross-document-review.md, locks across crashes and copies) ----
  //
  // A commit vote of this store's lost incarnation still counts for its transaction T (a driver's round, a client's
  // vote count, expiry rule B), but this store holds none of T's locks: its state came from reads that ignore locks.
  // Granting a commit vote on an object T locks to a transaction started after acceptorSince could let both commit,
  // T after the other with its requirement broken. So until the store knows which transactions its old votes may still
  // back, it votes abort on every object, and then on the objects of those transactions until its peers show them
  // resolved. T can still commit with the old vote counted only with writeThreshold - 1 live commit voters among the
  // other stores, so HeldVoteQuery's replies include one of them. A reply is complete only once every vote on such a T
  // has been cast: the old incarnation voted only before the loss, so before the rebuild pass began, and no store
  // joins T past its start plus the horizon (the forget floor), which inheritedVotesAfter adds to the pass start with
  // the clock margins. The vote is an abort, not a wait: one abort vote does not abort a transaction its peers commit

  /** None: every object is locked (no query since the gate has completed) */
  private var inheritedVotes: Option[Map[TransactionId, Set[ObjectId]]] =
    if inheritedVotesAfter.isDefined then None else Some(Map())
  private var inheritedObjects: Set[ObjectId] = Set()
  private var heldVoteQueryInFlight: Boolean = false
  private var heldVoteQueryNotBefore: Long = inheritedVotesAfter.getOrElse(0L)
  private var heldVoteQueryInterval: Long = InheritedVoteQueryInitialInterval.toMillis

  /** A transaction started at or after acceptorSince votes abort on `oid` (InheritedVoteLock) */
  def inheritedVoteLock(oid: ObjectId): Boolean = inheritedVotes match
    case None => true
    case Some(votes) => votes.nonEmpty && inheritedObjects.contains(oid)

  /** The transactions this store's inherited votes may still back. None: not known yet, every object is locked */
  def inheritedVoteTransactions: Option[Set[TransactionId]] = inheritedVotes.map(_.keySet)

  // ---- Inherited growth (a rebuilt store; transaction-requirements.md R16, cross-document-review.md 2.3) ----
  //
  // THE RARE CASE THIS EXISTS FOR. Everything below is idle unless all of these happen together:
  //   1. a store is lost while its old incarnation holds a commit vote for a transaction T that grows a KeyValue
  //      object O;
  //   2. T is still unresolved when the rebuild reads O, and commits afterwards, so the rebuilt copy of O lacks T's
  //      update while T still counts this store among its writeThreshold commit voters;
  //   3. before repair installs T here, another transaction grows O near the KeyValue limit and this store votes
  //      commit on it.
  // Each store admits KeyValue growth only within maxKVSliceSize (t/w of the slice) of the slice it holds, and the
  // argument that this keeps the union of committed updates within maxSliceSize counts each store once (R16). In step
  // 3 the store checks the new growth against a slice without T, while T is counted at its old incarnation: the store
  // is counted twice, the union can exceed maxSliceSize, and repair and an FSIF rebuild can then never install O
  // (CommitError.SliceTooLarge) until it shrinks. The inherited-vote lock above does not cover it: it ends when T's
  // resolution reaches the store, and a resolution carries no data.
  //
  // So a rebuilt store votes abort on any update that GROWS an object it may hold without such a T
  // (InheritedGrowthLock, Tx.determineDisposition) until a repair shows it holds T. Updates that grow nothing pass, so
  // an object can always be shrunk. The objects are found at the store's first held-vote round, which runs only past
  // the inherited-votes gate, when every vote on such a T has been cast (heldVotesAnswered):
  //   - T still unresolved: reported by a peer (the inherited votes). Its objects are under the inherited-vote lock
  //     until T is released, and guarded against growth from then on (setInheritedVotes).
  //   - T committed, not yet finalized: reported by a peer as committed (HeldVote.committed); guarded at once.
  //   - T finalized before the round: its finalizer first marked this store, which never acknowledged T, in the
  //     ErrorTree or UnackedCommits tree. The round lists every KeyValue object with an entry against the store, read
  //     after the peers answered (HeldVoteQuery.growthScan), and guards them all: more than needed, but only until the
  //     repair those entries already call for.
  // The scan first marks every KeyValue object of the reported transactions (missed at HLCTimestamp.Zero), so each
  // object has an entry whatever T's outcome, and repair comes to it. A guard ends at a complete whole-object repair
  // from a snapshot taken after the guard was set (inheritedGrowthRepaired): that read began after T committed, so it
  // reflects T (repair-process.md E1). A repair that cannot release a guard, or of a KeyValue object under the
  // inherited-vote lock (every object until the first round completes), answers stale, so the entry stays for the
  // repair that can: an entry consumed while the round runs could otherwise leave a guard no repair ever ends. Only
  // KeyValue objects are guarded: a DataObject's updates form a revision chain each voter checked in full (R16). The
  // inherited-votes file stays while any guard does: a reload repeats the round, and finds what is still guarded by
  // the entries the guards keep.

  /** Objects this rebuilt store guards against growth, each with the repair-snapshot sequence number when its guard
    * was set: only a snapshot taken after it releases it. Empty on a store that was never rebuilt */
  private var inheritedGrowthLocks: Map[ObjectId, Long] = Map()
  /** The objects with entries against this store that the first round's scan found. An inherited transaction released
    * later guards only these: only they are sure to be repaired */
  private var inheritedGrowthScanned: Set[ObjectId] = Set()
  /** Counts repair snapshots (snapshotForRepair, RepairSnapshot.takenAt) */
  private var repairSnapshotSeq: Long = 0L

  /** A growing update of `oid` votes abort (InheritedGrowthLock): see Inherited growth */
  def inheritedGrowthLock(oid: ObjectId): Boolean = inheritedGrowthLocks.contains(oid)

  /** Whether any object is guarded against growth: false, and free to test, on a store that was never rebuilt */
  def holdsInheritedGrowthLocks: Boolean = inheritedGrowthLocks.nonEmpty

  private def guardGrowth(oids: Iterable[ObjectId]): Unit =
    oids.foreach(oid => inheritedGrowthLocks += oid -> repairSnapshotSeq)

  /** Removes the inherited-votes file once neither an inherited vote nor a growth guard is held */
  private def removeInheritedStateWhenDone(): Unit =
    if inheritedVotes.exists(_.isEmpty) && inheritedGrowthLocks.isEmpty then
      RebuildingStore.removeInheritedVotes(backend.path)

  /** The first round's scan answered (HeldVoteQuery.growthScan): every object with an entry against this store, the
    * reported transactions' objects now among them. The objects are guarded, then the inherited votes are set */
  private def growthScanAnswered(result: Try[Set[ObjectId]], unresolved: Map[TransactionId, Set[ObjectId]]): Unit =
    heldVoteQueryInFlight = false
    result match
      case Failure(err) =>
        logger.warn(s"Store $storeId: inherited-growth scan failed, retrying the held-vote round: $err")
      case Success(objects) =>
        if inheritedVotes.isEmpty then
          if objects.nonEmpty then
            logger.info(s"Store $storeId: guarding ${objects.size} objects against growth until repaired " +
                        "(inherited growth)")
          inheritedGrowthScanned = objects
          guardGrowth(objects)
          setInheritedVotes(unresolved)

  /** The result of a repair of `objectId`, which is guarded against growth. A complete whole-object repair from a
    * snapshot taken after the guard was set releases it; any other complete one answers stale instead, keeping its
    * entry for a repair that can release the guard */
  private def inheritedGrowthRepaired(objectId: ObjectId, snapshot: RepairSnapshot, result: Try[ReconcileResult],
                                      p: Promise[ReconcileResult]): Unit =
    result match
      case Success(r) if r.complete && inheritedGrowthLocks.contains(objectId) =>
        if snapshot.only.isEmpty && inheritedGrowthLocks.get(objectId).exists(snapshot.takenAt > _) then
          inheritedGrowthLocks -= objectId
          logger.info(s"Store $storeId: object $objectId is repaired, so no longer guarded against growth")
          removeInheritedStateWhenDone()
          p.success(r)
        else
          p.success(r.copy(deferredKeys = Set(), objectLevelDeferred = false, stale = true))
      case _ => p.complete(result)

  private def holdsInheritedVotes: Boolean = inheritedVotes.forall(_.nonEmpty)

  /** Store heartbeat: asks the peers again, once past the gate and the backoff, while any inherited vote is held */
  def retryHeldVoteQuery(): Unit =
    if holdsInheritedVotes && !heldVoteQueryInFlight && wallClockMillis() >= heldVoteQueryNotBefore then
      heldVoteQueryInFlight = true
      heldVoteQuery.query(storeId, result => post(() => heldVotesAnswered(result)))

  /** T's outcome is decided (a TxResolved, proof included, or a TxFinalized for it reached this store, which is one of
    * T's stores): T can no longer commit after a transaction this store votes on from now, so its inherited vote is
    * released at once rather than at the next query */
  private def inheritedVoteDecided(txid: TransactionId): Unit = inheritedVotes match
    case Some(votes) if votes.contains(txid) => setInheritedVotes(votes - txid)
    case _ => ()

  private def setInheritedVotes(next: Map[TransactionId, Set[ObjectId]]): Unit =
    if !inheritedVotes.contains(next) then
      logger.info(s"Store $storeId: inherited votes now held for ${next.keySet}")
    // A released transaction may have committed, and this store may lack its update: its objects are guarded against
    // growth from now on (Inherited growth)
    inheritedVotes.foreach: votes =>
      guardGrowth(votes.iterator.filter((txid, _) => !next.contains(txid)).flatMap(_._2)
        .filter(inheritedGrowthScanned.contains).toList)
    inheritedVotes = Some(next)
    inheritedObjects = next.valuesIterator.flatten.toSet
    removeInheritedStateWhenDone()

  private def heldVotesAnswered(result: Try[List[HeldVote]]): Unit =
    heldVoteQueryInFlight = false
    result match
      case Failure(err) =>
        logger.warn(s"Store $storeId: query of its peers' held votes failed, retrying: $err")
      case Success(held) =>
        // A transaction started at or after acceptorSince never had this store's old vote
        val inherited = held.filter(v => acceptorSince.forall(v.startTimestamp < _))
        val unresolved = inherited.filterNot(_.committed).groupMapReduce(_.transactionId)(_.objectIds)(_ ++ _)
        inheritedVotes match
          case None =>
            // The first round: before the inherited votes are set, the growth scan lists what to guard (Inherited
            // growth), read after the peers answered. Every object stays locked until it answers
            heldVoteQueryInFlight = true
            heldVoteQuery.growthScan(storeId, inherited.iterator.flatMap(_.objectIds).toSet,
              scanned => post(() => growthScanAnswered(scanned, unresolved)))
          case Some(votes) =>
            setInheritedVotes(votes.filter((txid, _) => unresolved.contains(txid)))
    heldVoteQueryNotBefore = wallClockMillis() + heldVoteQueryInterval
    heldVoteQueryInterval = math.min(heldVoteQueryInterval * 2, InheritedVoteQueryMaxInterval.toMillis)

  def awaitLocksReleased(holder: TransactionId, waiter: Tx): Unit =
    val others = collisionWaiters.getOrElse(holder, Nil).filterNot(_ eq waiter)
    collisionWaiters(holder) = waiter :: others

  /** `holder` released its locks, so the transactions waiting for it decide their votes */
  def locksReleased(holder: TransactionId): Unit =
    collisionWaiters.remove(holder).foreach(_.foreach(_.collisionHolderReleased(holder)))

  // ---- One in-flight transaction commit per object ----
  //
  // An object accepts its next transaction only once the previous transaction commit to it has been persisted by the
  // backend, so at most one committed-but-unpersisted transaction per object is in the CRL. A transaction touching an
  // object with a commit in flight withholds its vote until the commit persists (no timer: a backend commit always
  // completes or fails), and a committed transaction holds its own commit until it is its turn on every object it
  // changes. Locks still release when a commit is applied in memory: the in-flight marker, not the lock, orders the
  // followers. An object a transaction only checks or locks gets no commit and so is never in flight

  /** Outstanding backend commits of transactions' changes, deletions included, by object: commit with
    * `commit`, and repair writes (see reconcile)
    */
  private val txCommitsInFlight = scala.collection.mutable.HashMap[ObjectId, Int]()

  /** Committed transactions holding their commit until it is their turn, by object, oldest first. See holdCommit */
  private val heldCommits = scala.collection.mutable.HashMap[ObjectId, List[Tx]]()

  /** Transactions withholding their vote until no commit of the object is in flight, oldest first */
  private val commitWaiters = scala.collection.mutable.HashMap[ObjectId, List[Tx]]()

  /** Objects a transaction's backend commit failed to write. Never cleared: the store reloads with a new Frontend */
  private val failedCommits = scala.collection.mutable.HashSet[ObjectId]()

  /** A transaction's commit of `oid` is outstanding, or a committed transaction holds one waiting its turn */
  def commitInFlight(oid: ObjectId): Boolean = txCommitsInFlight.contains(oid) || heldCommits.contains(oid)

  /** Committed transactions waiting to apply for the commit votes they would run over (Tx.awaitLockHolders), by
    * object they change
    */
  private val lockWaitingCommits = scala.collection.mutable.HashMap[ObjectId, List[Tx]]()

  /** The committed transactions changing `oid` that wait to apply (Tx.awaitLockHolders), oldest first. A new vote
    * whose locks one of them would wait for waits for it (Tx.lockWaitBlocked), so it cannot be starved; any other new
    * vote proceeds. These waits are left out of commitInFlight, which orders held commits: the votes they wait for
    * must never queue their own commits behind them
    */
  def lockWaiters(oid: ObjectId): List[Tx] = lockWaitingCommits.getOrElse(oid, Nil)

  /** `tx`, committed, waits to apply its changes of `oids` until each of `holders` releases its locks
    * (locksReleased -> Tx.collisionHolderReleased)
    */
  def awaitLockHolders(oids: Set[ObjectId], holders: Set[TransactionId], tx: Tx): Unit =
    oids.foreach(oid => lockWaitingCommits(oid) = lockWaitingCommits.getOrElse(oid, Nil).filterNot(_ eq tx) :+ tx)
    holders.foreach(awaitLocksReleased(_, tx))

  /** `tx` no longer waits to apply: it is applying, or queued for its turn */
  def lockWaitEnded(oids: Set[ObjectId], tx: Tx): Unit = oids.foreach(oid => removeFrom(lockWaitingCommits, oid, tx))

  /** A committed transaction's wait to apply ended: the votes withheld for it (lockWaiters) decide, unless a commit of
    * the object is now in flight, whose completion tells them instead
    */
  def lockWaitWakeVoters(oids: Set[ObjectId]): Unit = oids.foreach(commitTurnFree)

  /** Whether the Tx `holder` changes `oid` here (Tx.changes) */
  def changes(holder: TransactionId, oid: ObjectId): Boolean = transactions.get(holder).exists(_.changes(oid))

  /** A transaction's backend commit of `oid` failed: every later transaction on it votes abort until the store reloads */
  def commitFailed(oid: ObjectId): Boolean = failedCommits.contains(oid)

  /** Treats `oid` as if a backend commit of it had failed (commitFailed): every later transaction on it votes abort, and
    * repair leaves it alone, until the store reloads. For an object a recovered commit vote could not load on a store
    * whose writes are frozen (Tx.objectLoadFailed), which must keep serving rather than fail-stop
    */
  def poisonObject(oid: ObjectId): Unit = failedCommits += oid

  /** `waiter` withholds its vote until each of `oids` has no commit in flight: it is then told through
    * Tx.commitPersisted, once per object
    */
  def awaitCommitPersisted(oids: Set[ObjectId], waiter: Tx): Unit =
    oids.foreach(oid => commitWaiters(oid) = commitWaiters.getOrElse(oid, Nil).filterNot(_ eq waiter) :+ waiter)

  /** `waiter` no longer waits: it resolved */
  def stopAwaitingCommits(oids: Set[ObjectId], waiter: Tx): Unit =
    oids.foreach(oid => removeFrom(commitWaiters, oid, waiter))

  /** Queues `tx`'s commit of `oids` behind the commits of them in flight and those already held. Queues are appended to
    * together, so the oldest held commit heads the queue of every object it changes and the held commits cannot
    * wait on one another in a cycle. `tx` is told through Tx.commitPersisted when it may be its turn
    */
  def holdCommit(oids: Set[ObjectId], tx: Tx): Unit =
    oids.foreach(oid => heldCommits(oid) = heldCommits.getOrElse(oid, Nil) :+ tx)

  /** Whether `tx`, holding its commit of `oids`, may issue it now: no commit of them is in flight and it heads their
    * queues
    */
  def heldCommitReady(oids: Set[ObjectId], tx: Tx): Boolean =
    oids.forall(oid => !txCommitsInFlight.contains(oid) && heldCommits.get(oid).exists(_.head eq tx))

  /** `tx` has issued the commits it held, so the next in the queues may follow once they persist */
  def heldCommitIssued(oids: Set[ObjectId], tx: Tx): Unit =
    oids.foreach(oid => removeFrom(heldCommits, oid, tx))
    oids.foreach(commitTurnFree)

  private def removeFrom(m: scala.collection.mutable.HashMap[ObjectId, List[Tx]], oid: ObjectId, tx: Tx): Unit =
    m.get(oid).foreach: l =>
      val rest = l.filterNot(_ eq tx)
      if rest.isEmpty then m.remove(oid) else m(oid) = rest

  /** Tells whoever is next on `oid`, unless a commit of it is still in flight: the oldest held commit, or else every
    * transaction waiting to vote
    */
  private def commitTurnFree(oid: ObjectId): Unit =
    if !txCommitsInFlight.contains(oid) then
      // A parked repair goes first, at the one moment the object is settled: told first, a waiting transaction would
      // start the next commit. A repair write it starts takes the turn, and its completion passes it on
      retryParkedRepair(oid)
      if !txCommitsInFlight.contains(oid) then
        heldCommits.get(oid) match
          case Some(next :: _) => next.commitPersisted(oid)
          case _ => commitWaiters.remove(oid).foreach(_.foreach(_.commitPersisted(oid)))

  /** Outstanding backend commits, oldest first. Each holds a reference to its state (see retain) from commit until
    * the backend reports it complete, so the state is served and checked from memory rather than read from a backend
    * that may not have it yet. `start` times the commit as the store sees it: BackendTiming
    */
  private case class InFlight(os: ObjectState, start: Long, deletes: Boolean)
  private val inFlight = scala.collection.mutable.HashMap[(ObjectId, TransactionId), List[InFlight]]()

  /** Outstanding backend commits deleting each object. Reads that would reach the backend get the answer it will give
    * once the deletion is applied
    */
  private val deleting = scala.collection.mutable.HashMap[ObjectId, Int]()

  /** The states of outstanding non-deletion backend commits of each object, newest first. A backend read completing
    * meanwhile may return the state such a commit replaces, so it is never cached. See backendReadComplete
    */
  private val committing = scala.collection.mutable.HashMap[ObjectId, List[ObjectState]]()

  /** A repair reconcile waiting for its object to settle (RepairSnapshot.awaitSettle, repair-process 8.2),
    * until `until` (wallClockMillis)
    */
  private final class ParkedRepair(val snapshot: RepairSnapshot, val read: ClientObjectState,
                                   val p: Promise[ReconcileResult], val until: Long)

  /** At most one per object: one scan repairs an object at a time, and a newer request replaces an older one.
    * Declared before the constructor's recovery, which can release held messages (releaseHeldMessages)
    */
  private val parkedRepairs = scala.collection.mutable.HashMap[ObjectId, ParkedRepair]()

  /** A client's read waiting for committed transactions it names (Read.awaitCommit) to release their locks on its
    * object, or, for `allocation`, for this store's unapplied commit vote on the object's allocation to apply or abort,
    * until `until` (wallClockMillis). See parkRead and parkAllocationRead
    */
  private final class ParkedRead(val clientId: ClientId, val pointer: ObjectPointer, val readType: ReadType,
                                 val awaitCommit: Set[TransactionId], val until: Long, val allocation: Boolean = false)

  /** By object, then read: a resent read replaces its entry. Declared before the constructor's recovery, whose Txs
    * release locks (answerParkedReads)
    */
  private val parkedReads = scala.collection.mutable.HashMap[ObjectId, scala.collection.mutable.HashMap[UUID, ParkedRead]]()
  private var parkedReadCount: Int = 0
  private val commitStage = s"store commit (${backend.getClass.getSimpleName})"

  /** A transaction's backend commits on this store, made through commit.
    *
    * The store deletes a committed transaction's CRL state only once every backend commit it issued for that
    * transaction has succeeded. Until then the CRL entry, object data included, outlives the Tx, which is forgotten at
    * finalization. A failed commit leaves the record, and so the CRL entry, for the life of the loaded store: a reloaded
    * store recovers the transaction from it and re-applies it through the UnackedCommits entry its TxCommitted earned
    * (keptForReplay), retrying the commit.
    *
    * @param outstanding commits not yet reported complete by the backend
    * @param failed some commit failed
    * @param finalized TxFinalized arrived while commits were outstanding: the CRL deletion waits for them
    * @param stub the CRL state is replaced by an applied stub with this start rather than deleted (see
    *             transactionFinalized)
    */
  private final class TxCommits(var outstanding: Int, var failed: Boolean, var finalized: Boolean,
                                var stub: Option[HLCTimestamp] = None)
  private val txCommits = scala.collection.mutable.HashMap[TransactionId, TxCommits]()

  /** Finalized transactions whose commit is held for its turn (Tx.finalized, finalizeDeferred): forgotten, but their
    * commits are not issued yet, so they have no TxCommits. Every Tx that deferred is listed: a replay's Tx for the
    * same txid may defer too, and the transaction's state is concluded only by the last of them to issue its commit,
    * in whatever order they issue (transactionFinalized). A later TxFinalized listing this store in `owed` is passed
    * to every listed Tx (Tx.stubOwed), so the one that concludes writes the stub where the state would be deleted
    */
  private val deferredFinalizations = scala.collection.mutable.HashMap[TransactionId, List[Tx]]()

  /** `tx` was finalized while its commit is held for its turn: it calls transactionFinalized once it is issued */
  def finalizationDeferred(tx: Tx): Unit =
    val listed = deferredFinalizations.getOrElse(tx.transactionId, Nil)
    if !listed.exists(_ eq tx) then
      deferredFinalizations(tx.transactionId) = listed :+ tx

  /** A transaction that allocates objects here, whose CRL state may still hold their updates. `tx` is its latest Tx,
    * kept after the Tx is forgotten: its state may stay in the CRL meanwhile (its backend commits outstanding or
    * failed). `concluded`: its state's deletion or stub is queued in the CRL, not yet known durable
    */
  private final class AllocationRecord(var tx: Tx, var objects: Set[ObjectId], var concluded: Boolean)

  /** Transactions that allocate objects here and may still hold their updates in the CRL (crash-recovery-log 3.4),
    * by txid and by object. Empty for every object no unconcluded allocation covers, so an ordinary deletion consults
    * nothing else
    */
  private val allocationRecords = scala.collection.mutable.HashMap[TransactionId, AllocationRecord]()
  private val allocationsOf = scala.collection.mutable.HashMap[ObjectId, Set[TransactionId]]()

  private def registerAllocation(tx: Tx, hosted: List[ObjectPointer]): Unit =
    val allocated = tx.txd.allocatingObjects.intersect(hosted.iterator.map(_.id).toSet)
    if allocated.nonEmpty then
      val txid = tx.transactionId
      allocationRecords.get(txid) match
        case Some(r) =>
          r.tx = tx
          r.concluded = false
          r.objects ++= allocated
        case None => allocationRecords(txid) = new AllocationRecord(tx, allocated, false)
      allocated.foreach(oid => allocationsOf(oid) = allocationsOf.getOrElse(oid, Set()) + txid)

  private def unregisterAllocation(txid: TransactionId, oids: Set[ObjectId]): Unit =
    oids.foreach: oid =>
      val rest = allocationsOf.getOrElse(oid, Set()) - txid
      if rest.isEmpty then allocationsOf.remove(oid) else allocationsOf(oid) = rest

  /** `txid`'s CRL state was deleted or replaced by a stub: no save of it may follow. Its record is dropped once that is
    * durable, unless a newer Tx registered it again meanwhile */
  private def allocationConcluded(txid: TransactionId): Unit =
    allocationRecords.get(txid).foreach: r =>
      r.concluded = true
      crl.whenDurable(storeId, () => post { () =>
        allocationRecords.get(txid).filter(_ eq r).filter(_.concluded).foreach: r =>
          allocationRecords.remove(txid)
          unregisterAllocation(txid, r.objects)
      })

  /** Runs `proceed` once no CRL state of a transaction allocating `oid` here holds its update durably: the store is
    * about to delete `oid` from its backend, and a restart that recovered the update would allocate the object again
    * over the deletion (crash-recovery-log 3.4). An unconcluded allocation's state is saved again without it
    * (Tx.persistWithoutUpdateOf); a concluded one's deletion or stub only has to be durable (whenDurable). Inline when
    * no allocation covers `oid`, which is every deletion but one soon after its object's allocation
    */
  private def awaitAllocationsDurable(oid: ObjectId)(proceed: () => Unit): Unit =
    val txids = allocationsOf.getOrElse(oid, Set())
    if txids.isEmpty then
      proceed()
    else
      var pending = txids.size
      def oneDone(): Unit =
        pending -= 1
        if pending == 0 then proceed()
      logger.info(s"Store $storeId deletes object $oid soon after transactions ${txids.mkString(", ")} allocated it: " +
        s"the deletion waits until their CRL state durably holds no update of it")
      txids.foreach: txid =>
        allocationRecords.get(txid) match
          case Some(r) if !r.concluded =>
            // The update is gone from its state for good: later deletions of the object need nothing from it
            r.objects -= oid
            unregisterAllocation(txid, Set(oid))
            if r.objects.isEmpty then allocationRecords.remove(txid)
            r.tx.persistWithoutUpdateOf(oid)(() => oneDone())
          case _ =>
            crl.whenDurable(storeId, () => post(() => oneDone()))

  /** A transaction whose KeyValue update of objects here has no required revision, so it may Insert keys blindly
    * (`KeyValueUpdate.DoesNotExist`, `MayExist` or no requirement on the key), and whose CRL state may still hold that
    * update. Kept, as an AllocationRecord is, until the state's deletion or stub is durable
    */
  private final class BlindInsertRecord(var tx: Tx, var objects: Set[ObjectId], var concluded: Boolean)

  /** Transactions that may Insert keys blindly into objects here and whose CRL state may still hold the update, by
    * txid and by object (transaction-requirements 9.4). Filled by the update's shape alone; its keys are decoded only
    * when a key deletion meets it (awaitBlindInsertsDurable). Empty for every object no recent blind write covers
    */
  private val blindInsertRecords = scala.collection.mutable.HashMap[TransactionId, BlindInsertRecord]()
  private val blindInsertsOf = scala.collection.mutable.HashMap[ObjectId, Set[TransactionId]]()

  private def registerBlindInserts(tx: Tx, hosted: List[ObjectPointer]): Unit =
    val ids = hosted.iterator.map(_.id).toSet
    val blind = tx.txd.requirements.iterator.collect {
      case r: KeyValueUpdate if r.requiredRevision.isEmpty && ids.contains(r.objectPointer.id) => r.objectPointer.id
    }.toSet
    if blind.nonEmpty then
      val txid = tx.transactionId
      blindInsertRecords.get(txid) match
        case Some(r) =>
          r.tx = tx
          r.concluded = false
          r.objects ++= blind
        case None => blindInsertRecords(txid) = new BlindInsertRecord(tx, blind, false)
      blind.foreach(oid => blindInsertsOf(oid) = blindInsertsOf.getOrElse(oid, Set()) + txid)

  private def unregisterBlindInserts(txid: TransactionId, oids: Set[ObjectId]): Unit =
    oids.foreach: oid =>
      val rest = blindInsertsOf.getOrElse(oid, Set()) - txid
      if rest.isEmpty then blindInsertsOf.remove(oid) else blindInsertsOf(oid) = rest

  private def registerReplayHazards(tx: Tx, hosted: List[ObjectPointer]): Unit =
    registerAllocation(tx, hosted)
    registerBlindInserts(tx, hosted)
    if repairInstalls.nonEmpty then
      hosted.foreach(p => repairInstalls.get(p.id).foreach(at => tx.repairInstalled(p.id, at)))

  /** The latest object timestamp a repair write installed on each object, or the entry's minimum timestamp a complete
    * repair pass that changed nothing deleted its entry at, while a transaction started before it may still join here
    * (above the forget floor; pruned at expiryHeartbeat). Such a transaction's Tx is told (Tx.repairInstalled), at the
    * record or at its creation: the install, or the mark the pass's entry delete removed, may hold a later
    * transaction's deletion of a key the transaction blindly Inserts, which its apply would write back at this store
    * alone, so the apply reports the object (Tx.issueCommits, transaction-requirements 9.4). Empty except within a
    * horizon of a repair pass of the object, and at load: the load's floor refuses every transaction started before it
    */
  private val repairInstalls = scala.collection.mutable.HashMap[ObjectId, HLCTimestamp]()

  private def recordRepairInstall(oid: ObjectId, at: HLCTimestamp): Unit =
    if repairInstalls.get(oid).forall(_ < at) then repairInstalls(oid) = at
    transactions.valuesIterator.foreach(_.repairInstalled(oid, at))

  /** The latest start timestamp of a committed transaction whose Delete of each key applied here, a no-op over an
    * absent key included, while a transaction started before it may still apply here: one may still join (above the
    * forget floor) or is held (a held Tx applies below the floor too). Pruned at expiryHeartbeat. A transaction that
    * did not vote commit here held no key lock that ordered such a deletion after its blind Insert of the key, so its
    * apply of the Insert over the absent key may write back a key a later transaction deleted (Tx.issueCommits,
    * transaction-requirements 9.4). Only a deletion started at or after the Insert's transaction can be ordered after
    * it: one ordered after it at a store that applied the Insert checked an object timestamp at least the Insert's.
    * Empty at load: the load's floor refuses every transaction started before it
    */
  private val keyDeletions =
    scala.collection.mutable.HashMap[ObjectId, scala.collection.mutable.HashMap[Key, HLCTimestamp]]()

  /** A committed update started at `at` deleted `keys` of `oid` here (Tx.issueCommits) */
  def recordKeyDeletions(oid: ObjectId, keys: Set[Key], at: HLCTimestamp): Unit =
    if keys.nonEmpty then
      val byKey = keyDeletions.getOrElseUpdate(oid, scala.collection.mutable.HashMap())
      keys.foreach(key => if byKey.get(key).forall(_ < at) then byKey(key) = at)

  /** Whether a committed deletion of `key` of `oid` started at or after `since` applied here: see keyDeletions */
  def keyDeletedSince(oid: ObjectId, key: Key, since: HLCTimestamp): Boolean =
    keyDeletions.nonEmpty && keyDeletions.get(oid).flatMap(_.get(key)).exists(_ >= since)

  /** `txid`'s CRL state was deleted or replaced by a stub: no save of it may follow. Its records are dropped once that
    * is durable, unless a newer Tx registered them again meanwhile
    */
  private def stateConcluded(txid: TransactionId): Unit =
    allocationConcluded(txid)
    blindInsertRecords.get(txid).foreach: r =>
      r.concluded = true
      crl.whenDurable(storeId, () => post { () =>
        blindInsertRecords.get(txid).filter(_ eq r).filter(_.concluded).foreach: r =>
          blindInsertRecords.remove(txid)
          unregisterBlindInserts(txid, r.objects)
      })

  /** Runs `proceed` once no CRL state of another transaction that applied a blind Insert of one of `deleted` (keys of
    * `oid`, about to be removed from the backend by `by`'s commit or by a repair write) holds that update durably. A
    * recovered commit vote re-applies an Insert of an absent key (Tx.issueCommits), which is right only if no deletion
    * of that key followed the update it holds (transaction-requirements 10.5c).
    *
    * A covering transaction whose update of `oid` is applied here has its state saved again without it
    * (Tx.persistWithoutUpdateOf); its recovered vote then finds the update missing and takes it as applied. Applied
    * means: it issued its commit of `oid` here, or it voted commit here, since a commit voter holds the key's lock from
    * its vote until it applies, and a deletion of the key applies before it only as a recovered vote sharing the lock,
    * cast after the update persisted (10.5a). A concluded one's deletion or stub only has to be durable (whenDurable).
    * Any other (a non-voter that has not applied) holds an update the store does not hold, which it may yet apply:
    * nothing waits for it.
    *
    * Inline when no blind write covers `oid`, which is every key deletion but one soon after a blind write of its
    * object; only then are the keys decoded (`deleted`, the covering transactions' inserts)
    */
  private def awaitBlindInsertsDurable(oid: ObjectId, by: Option[TransactionId], deleted: () => Set[Key])
                                      (proceed: () => Unit): Unit =
    val covering = blindInsertsOf.getOrElse(oid, Set()) -- by
    if covering.isEmpty then
      proceed()
    else
      val keys = deleted()
      val hazards = if keys.isEmpty then Nil else covering.toList.flatMap: txid =>
        blindInsertRecords.get(txid).filter(r => r.tx.insertedKeys(oid).exists(keys.contains)).map(txid -> _)
      if hazards.isEmpty then
        proceed()
      else
        var pending = hazards.size
        def oneDone(): Unit =
          pending -= 1
          if pending == 0 then proceed()
        logger.info(s"Store $storeId deletes keys of object $oid soon after transactions " +
          s"${hazards.map(_._1).mkString(", ")} inserted them: the deletion waits until their CRL state durably holds " +
          s"no update of it")
        hazards.foreach: (txid, r) =>
          if r.concluded then
            crl.whenDurable(storeId, () => post(() => oneDone()))
          else if r.tx.holdsUpdateOf(oid) then
            // The update is gone from its state for good: later deletions of its keys need nothing from it
            r.objects -= oid
            unregisterBlindInserts(txid, Set(oid))
            if r.objects.isEmpty then blindInsertRecords.remove(txid)
            r.tx.persistWithoutUpdateOf(oid)(() => oneDone())
          else
            oneDone()

  private var pendingReads: Map[ObjectId, List[ReadKind]] = Map()

  /** The state of every object a transaction or an in-flight backend commit references (see retain), consulted before
    * the cache.
    *
    * Requirement locks live on these states, so every transaction must see the same instance for as long as any
    * transaction references it, whatever the cache does: a state that is evicted, or dropped because it can no longer
    * be served, is never replaced by a fresh load while it is referenced. One that can no longer be served is marked
    * reloadRequired instead, and requirement checks fail on it. It is replaced by a fresh load once no locks remain on
    * it, or when the last reference to it is released.
    */
  private var referenced: Map[ObjectId, ObjectState] = Map()

  /** Transaction messages held, newest first, until recovery completes. See awaitRecovery and recoverTransactions */
  private var heldMessages: List[TxMessage] = Nil

  /** The store's CRL state is still being loaded (from a crl_save.log), or its recovery is deferred. See
    * awaitRecovery. `@volatile` for mayHoldTransaction
    */
  @volatile private var awaitingCrl: Boolean = false

  /** Recovered transactions whose objects have not all loaded yet, and whether each relocks: a commit vote, unresolved
    * or with a restored Committed resolution, which takes its locks again at completeLoad (Tx.lock). A restored abort,
    * and a Tx without a commit vote, take none
    */
  private var recoveringTransactions: Map[TransactionId, Boolean] = Map()

  /** Recovered transactions that have loaded their objects but do not relock, waiting for every one that relocks to
    * complete first: those with a persisted vote, then those without. See recoveredTransactionLoaded
    */
  private var deferredRecovered: List[Tx] = Nil
  private var undecidedRecovered: List[Tx] = Nil

  private def holding: Boolean =
    awaitingCrl || recoveringTransactions.nonEmpty || deferredRecovered.nonEmpty || undecidedRecovered.nonEmpty

  /** What a transaction this Frontend admitted holds in the device's CRL admission, until released.
    *
    * @param bytes      the bytes charged so far
    * @param dataCounted the objects whose update data is charged: each is charged once, however often it arrives
    * @param serializedTxd the description serialized to size it, kept for the Tx's recovery state until taken
    */
  private final class Admitted(var bytes: Long, var dataCounted: Set[ObjectId], var serializedTxd: Option[DataBuffer])
  // Before recoverTransactions, which counts what it recovers (see admitPrepare)
  private val admitted = scala.collection.mutable.HashMap[TransactionId, Admitted]()

  /** A recovered commit vote's, or a held late commit's, UnackedCommits lookups (see Tx.awaitsUnackedLookup). Each lookup has its own request
    * id, from a counter of this Frontend's lookups, and its answer names it.
    *
    * @param latest the id of the latest lookup made
    * @param inFlight the latest lookup has not been answered
    * @param startedAt when the latest lookup was made (System.nanoTime)
    * @param firstCounting the first id whose answer finding no entry counts: lookups made before the latest abort
    *                      arrived do not, since the entry may have been written after they read the tree
    * @param failed the latest lookup failed: retryUnackedLookups makes another
    * @param failures lookups failed in a row, for logging
    * @param stallWarned the latest lookup was logged as unanswered past UnackedLookupTimeout
    */
  private final class UnackedLookup(var latest: Long, var inFlight: Boolean, var startedAt: Long,
                                      var firstCounting: Long, var failed: Boolean, var failures: Int,
                                      var stallWarned: Boolean = false)
  // Before recoverTransactions: a recovered transaction that loads at once requests its lookup inside it
  private val unackedLookups = scala.collection.mutable.HashMap[TransactionId, UnackedLookup]()
  private var nextUnackedRequest: Long = 0L

  /** The transactions this store holds an applied stub for (StoreRecoveryState), with their start timestamps: it holds
    * their updates. Loaded at recovery, dropped by expireStubs. Written only on the store's executor; `@volatile` so
    * tests may read hasAppliedStub from their own thread */
  @volatile private var stubs: Map[TransactionId, HLCTimestamp] = Map()

  /** Each stub's expiry (stubExpiresAt) and txid, soonest first, so a heartbeat looks only at the stubs due
    * (expireStubs). One entry per stub recorded; an entry whose stub is gone, or carries another start, is stale and
    * skipped when it comes up */
  private val stubExpiry = scala.collection.mutable.PriorityQueue[(Long, TransactionId)]()(
    Ordering.by[(Long, TransactionId), Long](_._1).reverse)

  /** Stubs past their expiry that a Tx for their transaction still kept, rechecked at each heartbeat (expireStubs) */
  private val heldStubs = scala.collection.mutable.HashSet[TransactionId]()

  /** The wall-clock milliseconds past which a stub for a transaction started at `start` may be dropped: the horizon
    * plus the expiry margin after it (as Tx.pastHorizon) */
  private def stubExpiresAt(start: HLCTimestamp): Long = start.wallTime + horizon.toMillis + expiryMargin.toMillis

  /** Holds a stub not already held, queued for its expiry. Returns whether it is new */
  private def addStub(txid: TransactionId, start: HLCTimestamp): Boolean =
    if stubs.contains(txid) then
      false
    else
      stubs += (txid -> start)
      stubExpiry.enqueue((stubExpiresAt(start), txid))
      true

  /** What the stubs are charged to the device's admission (CrlAdmission.AppliedStubBytes each), until released */
  private var stubBytesCharged: Long = 0L

  /** What the transactions whose CRL state a stub replaces were admitted for, released once the stub is durable */
  private val stubReplacedAdmissions = scala.collection.mutable.HashMap[TransactionId, Long]()

  private def chargeStubs(n: Int): Unit =
    val bytes = n * CrlAdmission.AppliedStubBytes
    admission.charge(bytes)
    stubBytesCharged += bytes

  private def releaseStub(): Unit =
    val bytes = math.min(CrlAdmission.AppliedStubBytes, stubBytesCharged)
    admission.release(bytes)
    stubBytesCharged -= bytes

  // Undeferred (tests only), the state is read here, which may block on the CRL
  if deferRecovery then awaitRecovery() else recoverState(Await.result(crl.readStoreState(storeId), Duration.Inf))

  /** Counts and recreates the store's recovered state, then releases what was retained for the store: what the
    * device's open, its last close, a loaded crl_save.log (CrlAdmission.tryChargeLoaded) or a store this one replaces
    * (StoreRouter.install) counted for the same records. Counted first, then released, so the records are never
    * uncounted in between, where other stores could admit into the gap (crash-recovery-log 6.1, A2). A recovery that
    * throws leaves the retained bytes counted too, which only over-counts */
  private def recoverState(state: StoreRecoveryState): Unit =
    // Counted without a check, as recovered transactions are: the CRL already holds them
    chargeStubs(state.appliedStubs.count((txid, start) => addStub(txid, start)))
    recoverTransactions(state.transactions)
    admission.releaseRetained(storeId)

  /** Recreates the transactions in `states`, which the CRL holds for this store, that are not already known.
    *
    * Transaction messages are held until every recovered transaction has loaded its objects, and with them taken the
    * locks of its recovered commit vote. A transaction started meanwhile could otherwise load a shared object first and
    * take conflicting locks.
    */
  private def recoverTransactions(states: List[TransactionRecoveryState]): Unit =
    // Every recovered transaction is registered before any of their reads starts: one that loads synchronously (its
    // objects cached) must not find the others missing and release the held messages early
    val recovered = states.flatMap { trs =>
      val txd = TransactionDescription.deserialize(trs.serializedTxd)
      if transactions.contains(txd.transactionId) then
        None
      else
        val pointers = txd.allHostedObjects(storeId)
        admitRecovered(txd.transactionId, trs)
        val tx = new Tx(trs, txd, this, net, crl, statusCache, pointers, recovered = true)
        transactions += (txd.transactionId -> tx)
        registerReplayHazards(tx, pointers)
        // A transaction with no objects here never loads any
        if pointers.nonEmpty then
          recoveringTransactions += (txd.transactionId -> (trs.disposition == TransactionDisposition.VoteCommit &&
            trs.status != TransactionStatus.Aborted))
        Some((tx, pointers))
    }
    recovered.foreach((tx, pointers) => pointers.foreach(ptr => readObjectForTransaction(tx, ptr)))

  /** A recovered transaction has loaded (or failed to load) all its objects. A commit vote that relocks (unresolved,
    * or with a restored Committed resolution, which it then applies) completes its load at once, taking its locks
    * again. Every other one completes only once every recovered commit vote that relocks has done so, those with a
    * persisted vote first. One applying a restored commit resolution earlier would
    * find no lock holder to wait for in lock order (Tx.awaitLockHolders) and could apply over a vote's update first,
    * failing its re-check (transaction-recovery.md 7.2); one deciding its vote earlier could take locks that conflict
    * with a recovered vote's.
    */
  def recoveredTransactionLoaded(tx: Tx, hasVote: Boolean): Unit =
    val relocks = recoveringTransactions.getOrElse(tx.transactionId, false)
    recoveringTransactions -= tx.transactionId
    if relocks then
      tx.completeLoad()
    else if hasVote then
      deferredRecovered = deferredRecovered :+ tx
    else
      undecidedRecovered = undecidedRecovered :+ tx
    if !recoveringTransactions.valuesIterator.contains(true) then
      val deferred = deferredRecovered ++ undecidedRecovered
      deferredRecovered = Nil
      undecidedRecovered = Nil
      deferred.foreach(_.completeLoad())
    releaseHeldMessages()

  /** The store's CRL state is being loaded after this Frontend was created (from a crl_save.log), or its recovery is
    * deferred (deferRecovery). Until recover is called, transaction messages are held: started without their
    * recovery state, transactions would hold no locks and a prepare would replace the recovered Paxos acceptor state
    * with an initial one.
    */
  def awaitRecovery(): Unit = awaitingCrl = true

  /** `state` is the store's recovery state, read from the CRL by the caller (CrashRecoveryLog.readStoreState, which
    * does not block): recreates its transactions and loads its applied stubs, which are not transactions. Held
    * messages are handled, in arrival order, once those transactions have loaded their objects. The flag is cleared
    * only once they are registered, so mayHoldTransaction, read from other threads, never reads false in between
    */
  def recover(state: StoreRecoveryState): Unit =
    recoverState(state)
    awaitingCrl = false
    releaseHeldMessages()

  // ---- Applied stubs (StoreRecoveryState) ----

  /** Whether the store holds an applied stub for `txid`: it already holds the transaction's update */
  def hasAppliedStub(txid: TransactionId): Boolean = stubs.contains(txid)

  /** The store's applied stubs, with their transactions' starts. Tests only */
  def appliedStubs: Map[TransactionId, HLCTimestamp] = stubs

  /** Records that the store holds `txid`'s update: an applied stub, saved in the CRL in place of the transaction's
    * state, for a transaction the store is forgetting. `onDurable` runs on the store's executor once the stub is
    * durable, after what the transaction was admitted for is released (its CRL state is replaced).
    *
    * That admission is taken from the transaction now, unless `keepAdmission` (a newer Tx for it is live and holds the
    * same record, see concludeTransactionState): a prepare arriving before the stub is durable is then admitted
    * afresh, never through the record of the state being replaced.
    *
    * The stub is charged to the device's admission without a check (CrlAdmission.AppliedStubBytes), so it is never
    * dropped or held back, while new prepares feel its bytes: the CRL's evacuations carry stubs too. It carries the
    * transaction's start (`start`), by which expireStubs drops it, releasing the charge; retained at close until the
    * store's entries leave the CRL.
    *
    * It must not replace the state of a transaction whose backend commits here are outstanding or failed: that state
    * (object data included) is what a reloaded store recovers the transaction from. Stubs are written only where
    * deleteTransactionState would run.
    */
  def recordAppliedStub(txid: TransactionId, start: HLCTimestamp, keepAdmission: Boolean = false)
                       (onDurable: => Unit): Unit =
    require(!txCommits.contains(txid),
      s"Store $storeId: an applied stub cannot replace transaction $txid's CRL state while its backend commits are " +
      s"outstanding or failed")
    if addStub(txid, start) then
      chargeStubs(1)
    if !keepAdmission then
      admitted.remove(txid).foreach: a =>
        stubReplacedAdmissions(txid) = stubReplacedAdmissions.getOrElse(txid, 0L) + a.bytes
    stateConcluded(txid)
    crl.saveAppliedStub(storeId, txid, stubs(txid), () => post { () =>
      stubReplacedAdmissions.remove(txid).foreach(admission.release)
      onDurable
    })

  /** Drops the applied stub for `txid`, if the store holds one, deletes it from the CRL and releases its charge. The
    * deletion is queued before the release, so no admission the release makes room for can reach the CRL ahead of it
    * (crash-recovery-log Lemma C (1)). Only expireStubs drops one; visible for tests */
  private[server] def dropAppliedStub(txid: TransactionId): Unit =
    if stubs.contains(txid) then
      stubs -= txid
      heldStubs -= txid
      crl.deleteAppliedStub(storeId, txid)
      releaseStub()

  /** Drops every applied stub whose transaction is past the horizon plus the expiry margin (stubExpiresAt) and for
    * which the store holds no Tx, whether live, waiting on a held commit (deferredFinalizations), on its backend
    * commits (txCommits). Called at heartbeat (expiryHeartbeat). Only the
    * stubs due are looked at (stubExpiry), plus those a Tx kept past theirs (heldStubs), so a heartbeat costs
    * O(k log n) for k stubs due, not a pass over all n.
    *
    * A stub only ever keeps a Tx for its transaction at this store from applying the update again. Past the margin T is
    * below the forget floor, so a prepare for it creates no Tx (receivePrepare: refused, or ignored when the host's
    * status cache knows T finalized), and with none held no Tx for T can arise here again: the stub can suppress
    * nothing. This bounds the stubs by their rate times the horizon plus the margin, whether or not repair scans run
    * (crash-recovery-log 3.6). An UnackedCommits entry left without its stub is harmless: no
    * Tx is left to apply through it, and the scan's reconcile is exact
    */
  private def expireStubs(now: Long): Unit =
    def txHeld(txid: TransactionId): Boolean =
      transactions.contains(txid) || deferredFinalizations.contains(txid) || txCommits.contains(txid)
    if heldStubs.nonEmpty then
      heldStubs.toList.foreach: txid =>
        if !txHeld(txid) then
          dropAppliedStub(txid)
    while stubExpiry.nonEmpty && stubExpiry.head._1 < now do
      val (expiresAt, txid) = stubExpiry.dequeue()
      stubs.get(txid) match
        case Some(start) if stubExpiresAt(start) == expiresAt =>
          if txHeld(txid) then heldStubs += txid else dropAppliedStub(txid)
        case _ => // Stale: the stub was dropped, or is held again under its own entry

  // ---- UnackedCommits lookups of recovered commit votes and held late commits (Tx.awaitsUnackedLookup) ----

  /** `tx` needs a UnackedCommits lookup started now: it has loaded, or an abort arrived. Only lookups made from now
    * on can show the entry absent. One already in flight is not repeated: if it finds no entry, another is made
    */
  def requestUnackedLookup(tx: Tx): Unit =
    unackedLookups.get(tx.transactionId) match
      case Some(l) =>
        l.firstCounting = nextUnackedRequest
        if !l.inFlight then
          startUnackedLookup(tx.transactionId, l)
      case None =>
        val l = new UnackedLookup(-1L, false, 0L, nextUnackedRequest, false, 0)
        unackedLookups(tx.transactionId) = l
        startUnackedLookup(tx.transactionId, l)

  private def startUnackedLookup(txid: TransactionId, l: UnackedLookup): Unit =
    val requestId = nextUnackedRequest
    nextUnackedRequest += 1
    l.latest = requestId
    l.inFlight = true
    l.startedAt = System.nanoTime()
    l.failed = false
    l.stallWarned = false
    // Through `post`, so a store replaced meanwhile drops the answer rather than handing it to its successor
    unackedLookup.lookup(storeId, txid, requestId,
      result => post(() => unackedLookupComplete(txid, requestId, result)))

  /** The answer to UnackedCommits lookup `requestId`, delivered on the store's executor.
    *
    * An entry found resolves the Tx committed, whichever lookup found it. It applies through the entry unless the
    * entry names another incarnation of this store (Tx.unackedCommitFound). One naming none (a driver that did not
    * count the votes never heard from this store) applies too: every Tx that awaits a lookup holds its own commit vote,
    * which may be the counted vote the commit relied on (transaction-recovery.md, 7). Otherwise it is a replay's vote,
    * which passed T's pin, so the pinned object is still before T here and T's committed update is what it lacks. One
    * naming another incarnation concludes it committed with nothing applied (Tx.unackedCommitNotOwed): the vote that
    * counted was lost to a rebuild. Either way it is never taken as absent.
    * None found releases the aborts it held, or makes a held late commit apply nothing, if the lookup was made after
    * the latest of them arrived; otherwise, if it was the latest lookup, another is made.
    * A failure of the latest lookup keeps the aborts held, and retryUnackedLookups tries again. Ignored once the Tx
    * no longer needs it (it learned its outcome another way), and for a lookup this Frontend did not make
    */
  def unackedLookupComplete(txid: TransactionId, requestId: Long, result: Try[UnackedLookupResult]): Unit =
    unackedLookups.get(txid).filter(_.latest >= requestId).foreach: l =>
      transactions.get(txid).filter(_.awaitsUnackedLookup) match
        case None => unackedLookups.remove(txid)
        case Some(tx) =>
          val isLatest = requestId == l.latest
          if isLatest then
            l.inFlight = false
          result match
            case Success(UnackedLookupResult.Found(found)) =>
              unackedLookups.remove(txid)
              found match
                case Some(other) if other != incarnation =>
                  // Another incarnation's vote counted (this store was rebuilt since): T committed, but nothing is
                  // owed to this incarnation. See Tx.unackedCommitNotOwed
                  logger.info(s"Store $storeId (incarnation ${incarnation.uuid}): the UnackedCommits entry for " +
                    s"transaction $txid names incarnation ${other.uuid}: applying nothing through it")
                  tx.unackedCommitNotOwed()
                case _ =>
                  // Its own incarnation, or none (a driver that did not count the votes never heard from this store).
                  // Forgotten by the Tx itself only for a held late commit: see Tx.unackedCommitFound
                  if found.isEmpty then
                    logger.info(s"Store $storeId: the UnackedCommits entry for transaction $txid names no " +
                      s"incarnation: applying through it, as this store holds its own commit vote")
                  tx.unackedCommitFound()
              if tx.provesCommitToItsDriver then
                onCommitProven(txid)
            case Success(UnackedLookupResult.Absent) if requestId >= l.firstCounting =>
              unackedLookups.remove(txid)
              val held = tx.unackedCommitAbsent()
              // A held abort is a real outcome: it is taken over an expiry discard
              if held.isEmpty && tx.expiryDiscardDue then
                expireTransaction(tx)
              else
                held.foreach(handleTransactionMessage)
            case Success(UnackedLookupResult.Absent) =>
              if isLatest then
                startUnackedLookup(txid, l)
            case Failure(err) =>
              if isLatest then
                l.failed = true
                l.failures += 1
                if l.failures == 1 || l.failures % 60 == 0 then
                  logger.warn(s"Store $storeId: UnackedCommits lookup of transaction $txid failed " +
                    s"(${l.failures} in a row), retrying at heartbeat: $err")

  /** Store heartbeat: makes another lookup for each one that failed. One still unanswered is left to finish, however
    * long it takes: its read of the tree retries until the tree's metadata pool is readable, so another would only run
    * beside it, and many would pile up through a metadata outage. Its answer cannot be lost: lookups start only from
    * recovery or messages, after the store is installed, and `post` drops an answer only once another store replaced
    * this one, and this Frontend with it. One unanswered past `Frontend.UnackedLookupTimeout` is logged once
    */
  def retryUnackedLookups(now: Long = System.nanoTime()): Unit =
    unackedLookups.toList.foreach: (txid, l) =>
      if !transactions.get(txid).exists(_.awaitsUnackedLookup) then
        unackedLookups.remove(txid)
      else if l.failed then
        startUnackedLookup(txid, l)
      else if l.inFlight && !l.stallWarned && now - l.startedAt >= UnackedLookupTimeout.toNanos then
        l.stallWarned = true
        logger.warn(s"Store $storeId: UnackedCommits lookup of transaction $txid unanswered for " +
          s"$UnackedLookupTimeout, still waiting for it")

  // ---- Expiry of unresolved transactions past the horizon (transaction horizon design, section 2) ----

  /** An expiring Tx's status queries. Each round has its own request id and counts only its own replies.
    *
    * @param replies this round's replies by store, this store's own included unless it is silent to itself
    *                (Tx.ownExpiryStatus); a store absent from it was silent
    * @param countdown store heartbeats left until the next round
    * @param interval heartbeats between the next round and the one after: 1, doubling to MaxExpiryQueryInterval
    * @param decided this round's replies decided: later ones of it are ignored
    */
  private final class ExpiryRound(var requestUUID: UUID, var replies: Map[StoreId, Option[TxStatusResponse.TxStatus]],
                                  var countdown: Int, var interval: Int, var decided: Boolean,
                                  var undecidableRounds: Int = 0)
  private val expiryRounds = scala.collection.mutable.HashMap[TransactionId, ExpiryRound]()

  /** Store heartbeat: each Tx still unfinalized past the horizon plus the expiry margin (Tx.mayExpire: unresolved, or
    * resolved and waiting for a TxFinalized that may never come) queries the status of every other store of its
    * transaction, first at the heartbeat it is found so, then after 1, 2, 4 and then every MaxExpiryQueryInterval
    * heartbeats, until it decides from the replies (ExpiryTally): it adopts an outcome, is discarded (concluded, if it
    * knows its outcome: Tx.concludeKnownOutcome), or keeps holding. Only the replies decide, so a Tx whose peers still
    * hold T loses nothing by asking.
    *
    * A Tx held only to decide its applied stub (Tx.forgetLookupPending) asks nothing: past the margin it is forgotten
    * without one (Tx.forgetWithoutStubPastExpiry, transaction-recovery 5.4), and its lookup is dropped
    */
  def expiryHeartbeat(): Unit =
    val now = wallClockMillis()
    expireStubs(now)
    if repairInstalls.nonEmpty then
      repairInstalls.filterInPlace((_, at) => !belowForgetFloor(at))
    if keyDeletions.nonEmpty then
      val oldestHeld = transactions.valuesIterator.map(_.txd.startTimestamp).minOption
      keyDeletions.filterInPlace: (_, byKey) =>
        byKey.filterInPlace((_, at) => !belowForgetFloor(at) || oldestHeld.exists(_ <= at))
        byKey.nonEmpty
    expiryRounds.keys.toList.foreach: txid =>
      if !transactions.get(txid).exists(_.mayExpire) then
        expiryRounds.remove(txid)
    transactions.values.toList.foreach: tx =>
      if tx.forgetLookupPending && tx.pastHorizon(now, horizon, expiryMargin) then
        unackedLookups.remove(tx.transactionId)
        tx.forgetWithoutStubPastExpiry()
      else if tx.mayExpire && tx.pastHorizon(now, horizon, expiryMargin) then
        expiryRounds.get(tx.transactionId) match
          case None =>
            tx.beginExpiry()
            val round = new ExpiryRound(UUID.randomUUID(), Map(), 0, 1, false)
            expiryRounds(tx.transactionId) = round
            startExpiryRound(tx, round)
          case Some(round) =>
            round.countdown -= 1
            if round.countdown <= 0 then
              startExpiryRound(tx, round)

  private def startExpiryRound(tx: Tx, round: ExpiryRound): Unit =
    round.requestUUID = UUID.randomUUID()
    round.decided = false
    round.countdown = round.interval
    round.interval = math.min(round.interval * 2, MaxExpiryQueryInterval)
    // Its own state, frozen like its peers' (it is past expiry). Never an outcome it knows from its resolution: that is
    // what its peers are asked to confirm or end (ExpiryTally.decide's `known`). An outcome it has only from the status
    // cache (Tx.outcomeDecided) is kept, so it adopts it as it would from a peer. A commit vote not yet durable leaves
    // it silent (Tx.ownExpiryStatus): rule G holds until a later round counts the vote
    round.replies = tx.ownExpiryStatus match
      case None => Map()
      case Some(own) => Map(storeId -> Some(
        if tx.knownOutcome.nonEmpty then own.copy(status = TransactionStatus.Unresolved, finalized = false, late = false)
        else own))
    val txd = tx.txd
    // Every store of the transaction, since any outcome-bearing reply decides; only the primary pool's count otherwise
    (txd.allDataStores - storeId).foreach: to =>
      net.sendTransactionMessage(TxStatusRequest(to, storeId, txd.transactionId, round.requestUUID, txd.startTimestamp))
    decideExpiry(tx, round)

  private def receiveStatusResponse(m: TxStatusResponse): Unit =
    for
      round <- expiryRounds.get(m.transactionId) if round.requestUUID == m.requestUUID && !round.decided
      tx <- transactions.get(m.transactionId) if tx.mayExpire
    do
      round.replies += (m.from -> m.status)
      decideExpiry(tx, round)

  private def decideExpiry(tx: Tx, round: ExpiryRound): Unit =
    ExpiryTally.decide(tx.txd, round.replies, tx.knownOutcome) match
      case ExpiryTally.Decision.Hold(undecidable) =>
        if undecidable then
          round.undecidableRounds += 1
          if round.undecidableRounds == 1 || round.undecidableRounds % UndecidableLogRounds == 0 then
            logger.error(s"UNDECIDABLE TRANSACTION ${tx.transactionId} on store $storeId: every store of it has " +
              s"reported, and too many of its primary pool are rebuilt or forgot it to rule out a chosen abort, so its " +
              s"outcome cannot be proven. Its locks and CRL state are held. Replies: ${round.replies}")
      case ExpiryTally.Decision.Commit =>
        round.decided = true
        logger.warn(s"Expiring Tx ${tx.transactionId} on store $storeId decides commit: abort was never chosen and " +
          s"can no longer be, and its update is held at the read threshold. Applying it and finalizing; repair " +
          s"restores the stores that missed it. Replies: ${round.replies}")
        val txd = tx.txd
        // To every store, as a driver's resolution: other holders apply it, and the stores without a Tx record it
        (txd.allDataStores - storeId).foreach: to =>
          net.sendTransactionMessage(TxResolved(to, storeId, txd.transactionId, committed = true, late = false,
            proof = false))
        handleTransactionMessage(TxResolved(storeId, storeId, txd.transactionId, committed = true, late = false,
          proof = false))
        // A driver finalizes it: its missed-update marks drive the repair
        onExpiryCommit(txd)
      case ExpiryTally.Decision.Adopt(committed, finalizedOrLate) =>
        round.decided = true
        val from = round.replies.collectFirst:
          case (store, Some(s)) if s.status != TransactionStatus.Unresolved => store
        .getOrElse(storeId)
        logger.info(s"Expiring Tx ${tx.transactionId} on store $storeId adopts the outcome reported by $from: " +
          s"${if committed then "committed" else "aborted"}${if finalizedOrLate then ", finalized" else ""}")
        // As the outcome's own messages would deliver it. An abort is final once resolved (a driver finalizes it at
        // once), so it is adopted as finalized: the Tx is then forgotten
        val adopted =
          if !committed then
            TxFinalized(storeId, from, tx.transactionId, committed = false, late = false, owed = Set(),
              tx.txd.startTimestamp)
          else
            TxResolved(storeId, from, tx.transactionId, committed = true, late = finalizedOrLate, proof = false)
        handleTransactionMessage(adopted)
      case ExpiryTally.Decision.Discard(dataLost) =>
        round.decided = true
        if dataLost then
          logger.error(s"Expiring Tx ${tx.transactionId} on store $storeId: too few of its commit voters hold its " +
            s"update to restore it at the read threshold, and it may have committed. Unless it was finalized (its " +
            s"applied copies are not reported), a committed update is lost. Discarding it. Replies: ${round.replies}")
        if tx.knownOutcome.nonEmpty then
          expiryRounds.remove(tx.transactionId)
          tx.concludeKnownOutcome()
        else if tx.needsUnackedCheck then
          // A commit vote may be owed T: discarded only once a lookup started now finds no entry (unackedLookupComplete)
          if tx.decideExpiryDiscard() then
            logger.info(s"Expiring Tx ${tx.transactionId} on store $storeId: its peers can no longer decide it; " +
              s"looking up its UnackedCommits entry before discarding it")
            requestUnackedLookup(tx)
        else
          expireTransaction(tx)

  /** Discards `tx` past the horizon, with no decision: no quorum of its primary pool can ever decide T again, and this
    * store is owed nothing. Its locks and objects are released, its CRL state deleted as for a forgotten abort, and it
    * is forgotten. The status cache is left alone: no outcome is known. Its floor then refuses every later prepare for
    * T, so the store never rejoins it
    */
  private def expireTransaction(tx: Tx): Unit =
    if transactions.get(tx.transactionId).exists(_ eq tx) then
      logger.warn(s"Store $storeId discards unresolved transaction ${tx.transactionId} past the transaction horizon: " +
        s"too few of its peers hold it to ever decide it")
      tx.expire()
      transactions -= tx.transactionId
      expiryRounds.remove(tx.transactionId)
      unackedLookups.remove(tx.transactionId)
      if tx.muted then releaseAdmission(tx.transactionId) else deleteTransactionState(tx.transactionId)

  private def releaseHeldMessages(): Unit =
    if !holding then
      parkedRepairs.keys.toList.foreach(retryParkedRepair)
      val held = heldMessages.reverse
      heldMessages = Nil
      held.foreach(handleTransactionMessage)


  // ---- Backpressure: see CrlAdmission (Admitted and admitted are declared above recoverTransactions) ----

  /** Whether a prepare may be handled.
    *
    * A transaction this Frontend admitted (recovered ones included) is, unless its prepare brings object data not yet
    * counted: that data is charged only while the device's admission is below this store's tier's ceiling, as a new
    * transaction is, and the prepare is dropped (Full) otherwise (crash-recovery-log 6.1, A1). Charged unchecked,
    * data arriving after any number of data-less admissions made below the ceiling could each bring up to
    * `maxTransactionBytes`. The client retransmits; a data-less prepare of it is still handled. A known transaction
    * with no admission record is admitted too: it is one whose CRL state was deleted, which saves nothing more and
    * answers with its outcome (Tx.stateDeleted, A4). So is one finalized in the status cache, which is answered from
    * the cache and never reaches the CRL. Any other is counted for its description, object data and CRL record, and
    * admitted only while the device's admission is below this store's tier's ceiling (CrlAdmission lanes: a metadata
    * or bootstrap store keeps room for the marker batches data transactions finalize with). The status cache is
    * host-wide, so a transaction merely started on another store is not known here.
    *
    * Oversized: the transaction's counted bytes, with data already counted for an admitted one, would exceed
    * `crl.maxTransactionBytes`, so saving it would kill the CRL (crash-recovery-log 10.20). Nothing is charged. Data
    * arriving after a data-less prepare is dropped the same way, leaving the store as one that never received it: a commit elsewhere
    * reaches it through missed-update repair, and the data-less admission is released by the usual paths
    */
  def admitPrepare(m: TxPrepare): PrepareAdmission =
    val result = checkAdmission(m)
    // A full lane asks the CRL to grow (idempotent, any thread); the retransmitted prepare meets the raised limit
    if result == PrepareAdmission.Full then
      crl.requestGrowth()
    result

  private def checkAdmission(m: TxPrepare): PrepareAdmission =
    val txid = m.txd.transactionId
    admitted.get(txid) match
      case Some(a) =>
        val added = uncounted(a, m.objectUpdates)
        val bytes = dataBytes(added)
        if added.isEmpty then
          PrepareAdmission.Admitted
        else if a.bytes + bytes > crl.maxTransactionBytes then
          oversized(txid, a.bytes + bytes)
        else if admission.tryAdmit(bytes, admissionTier) then
          countData(a, added, bytes)
          PrepareAdmission.Admitted
        else
          PrepareAdmission.Full
      case None if transactions.contains(txid) || statusCache.getStatus(txid).exists(_.finalized) =>
        PrepareAdmission.Admitted
      // Checked before serializing: a prepare that will be dropped costs nothing more
      case None if admission.full(admissionTier) => PrepareAdmission.Full
      case None =>
        val stxd = m.txd.serialize()
        val bytes = CrlAdmission.TxOverheadBytes + stxd.size + dataBytes(m.objectUpdates)
        if bytes > crl.maxTransactionBytes then
          oversized(txid, bytes)
        else if admission.tryAdmit(bytes, admissionTier) then
          admitted(txid) = new Admitted(bytes, m.objectUpdates.iterator.map(_.objectId).toSet, Some(stxd))
          PrepareAdmission.Admitted
        else
          PrepareAdmission.Full

  private def oversized(txid: TransactionId, bytes: Long): PrepareAdmission =
    oversizedLog.admit().foreach: held =>
      logger.warn(s"Store $storeId drops transaction $txid: its $bytes CRL bytes here exceed the device CRL's " +
                  s"${crl.maxTransactionBytes} per transaction, so it can never commit with this store. Its " +
                  s"client writes too much to one store in one transaction${LogRateLimit.heldText(held)}")
    PrepareAdmission.Oversized

  /** The CRL bytes CrlAdmission counts for the object data of `updates` */
  private def dataBytes(updates: List[ObjectUpdate]): Long =
    updates.iterator.map(ou => CrlAdmission.objectUpdateBytes(ou.data.size)).sum

  /** The updates of `updates` whose data `a` has not counted yet */
  private def uncounted(a: Admitted, updates: List[ObjectUpdate]): List[ObjectUpdate] =
    updates.filterNot(ou => a.dataCounted.contains(ou.objectId))

  /** Charges `a` for the data of `updates` not counted yet, without a limit check: only for data the CRL already
    * holds (admitRecovered) */
  private def chargeData(a: Admitted, updates: List[ObjectUpdate]): Unit =
    val added = uncounted(a, updates)
    if added.nonEmpty then
      val bytes = dataBytes(added)
      admission.charge(bytes)
      countData(a, added, bytes)

  /** Records that `a` is charged `bytes` for the data of `added` */
  private def countData(a: Admitted, added: List[ObjectUpdate], bytes: Long): Unit =
    a.bytes += bytes
    a.dataCounted ++= added.iterator.map(_.objectId)

  /** Counts a transaction recovered from the CRL, without a limit check (the CRL already holds it), as admitPrepare
    * counts a new one: repeated restarts under load must not stack uncounted live bytes in the device CRL. One whose
    * prepare was admitted while the store awaited its saved state is charged only for data not yet counted
    */
  private def admitRecovered(txid: TransactionId, trs: TransactionRecoveryState): Unit =
    admitted.get(txid) match
      case Some(a) => chargeData(a, trs.objectUpdates)
      case None =>
        // As the device's open counted it (CrashRecoveryLog.recoveredBytesByStore), which this replaces
        val a = new Admitted(CrlAdmission.recoveredBytes(trs), trs.objectUpdates.iterator.map(_.objectId).toSet, None)
        admission.charge(a.bytes)
        admitted(txid) = a

  /** The description `admitPrepare` serialized for `txid`, if not taken yet */
  private def takeSerializedTxd(txid: TransactionId): Option[DataBuffer] =
    admitted.get(txid).flatMap: a =>
      val s = a.serializedTxd
      a.serializedTxd = None
      s

  /** Releases what `txid` was admitted for, if anything: its CRL entry was deleted, or it is forgotten without one.
    * Idempotent
    */
  def releaseAdmission(txid: TransactionId): Unit =
    admitted.remove(txid).foreach(a => admission.release(a.bytes))

  /** Forgets a Tx that concluded itself (Tx.applyNothing, or a late commit), as a TxFinalized forgets one */
  def forget(tx: Tx): Unit =
    if transactions.get(tx.transactionId).exists(_ eq tx) then
      transactions -= tx.transactionId
      tx.releaseObjects() // Already done unless some of its objects had not loaded

  /** Drops a muted Tx that was not proven committed (Tx.muted): it holds no locks and saved nothing. One muted for
    * starting too far ahead was logged when it was created */
  def forgetMuted(tx: Tx): Unit =
    if transactions.get(tx.transactionId).exists(_ eq tx) then
      if acceptorSince.exists(tx.txd.startTimestamp < _) then
        logger.info(s"Store $storeId takes no part in transaction ${tx.transactionId}: it started before this " +
                    s"rebuilt store's acceptor existed")
      transactions -= tx.transactionId
      tx.releaseObjects()
      stateConcluded(tx.transactionId) // It saved nothing
      releaseAdmission(tx.transactionId)

  /** Deletes a transaction's CRL entry and releases its admission */
  def deleteTransactionState(txid: TransactionId): Unit =
    crl.deleteTransaction(storeId, txid)
    stateConcluded(txid)
    releaseAdmission(txid)

  /** Releases everything this Frontend's transactions were admitted for: it was built and never served. A store
    * that served retains them instead (retainAdmissions): its records may be live in the CRL */
  def releaseAllAdmissions(): Unit =
    admitted.valuesIterator.foreach(a => admission.release(a.bytes))
    admitted.clear()
    stubReplacedAdmissions.valuesIterator.foreach(admission.release)
    stubReplacedAdmissions.clear()
    admission.release(stubBytesCharged)
    stubBytesCharged = 0L

  /** Moves everything this Frontend's transactions and stubs were admitted for into what the device's admission
    * retains for the store (CrlAdmission.retain), still counted: the store is closing or replaced, and its entries
    * stay live in the CRL until they leave it or the store's next recovery counts them afresh (recoverState). A stub
    * not yet durable has not replaced its transaction's state: that stays counted too */
  def retainAdmissions(): Unit =
    admission.retain(storeId, admitted.valuesIterator.map(_.bytes).sum + stubReplacedAdmissions.valuesIterator.sum +
      stubBytesCharged)
    admitted.clear()
    stubReplacedAdmissions.clear()
    stubBytesCharged = 0L

  /** Closes the store. Its entries stay live in the CRL unless the closer takes them out, so what its transactions
    * were admitted for stays counted, retained for the store (retainAdmissions) */
  def close(): Future[Unit] =
    closed = true
    failParkedRepairs()
    dropParkedReads()
    retainAdmissions()
    backend.close()

  def path: os.Path = backend.path

  /** Hands `os` to the backend and keeps it in memory until the backend reports the commit complete.
    * Applies a transaction's committed update; transactions and repairs on the object wait behind it
    * (txCommitsInFlight).
    *
    * @param deletedKeys the keys of `os` the update deletes: asked only when a blind write covers the object
    *                    (awaitBlindInsertsDurable)
    */
  def commit(os: ObjectState, cs: CommitState, txid: TransactionId,
             deletedKeys: () => Set[Key] = () => Set()): Unit =
    val deletes = os.metadata.refcount.count == 0
    if deletes then
      // Deleted: the state is no longer served, nor checked by transactions that still reference it
      discard(os.objectId)
    else
      objectCache.insert(os)
    // A discarded state whose locks are still held stays the one transactions see (see referenced). A newer state
    // committed meanwhile is served through the cache until then
    val lockedOther = referenced.get(os.objectId).exists(r => !(r eq os) && r.reloadRequired && r.lockHolders.nonEmpty)
    retain(os, shared = !lockedOther)
    val key = (cs.objectId, txid)
    inFlight(key) = inFlight.getOrElse(key, Nil) :+ InFlight(os, System.nanoTime(), deletes)
    if deletes then
      deleting(os.objectId) = deleting.getOrElse(os.objectId, 0) + 1
    else
      committing(os.objectId) = os :: committing.getOrElse(os.objectId, Nil)
    // Before the backend commit: a synchronous backend completes inside it
    txCommits.getOrElseUpdate(txid, new TxCommits(0, false, false)).outstanding += 1
    txCommitsInFlight(os.objectId) = txCommitsInFlight.getOrElse(os.objectId, 0) + 1
    // A deletion reaches the backend only once no allocation's CRL state can bring the object back
    // (crash-recovery-log 3.4), and a key's deletion only once no blind Insert's CRL state can bring the key back
    // (awaitBlindInsertsDurable). Until then it is in flight like any commit: reads, snapshots and transactions see the
    // deletion. A store stopped or closed meanwhile never issues it, and keeps this transaction's CRL state, so a
    // reload applies the deletion again
    if deletes then
      awaitAllocationsDurable(os.objectId)(() => if !stopped && !closed then backend.commit(cs, txid))
    else
      awaitBlindInsertsDurable(os.objectId, Some(txid), deletedKeys)(() =>
        if !stopped && !closed then backend.commit(cs, txid))

  /** A transaction was finalized. Its CRL state is deleted now, unless backend commits this store issued for it are
    * outstanding or failed: the store deletes a committed transaction's CRL state only once every backend commit it
    * issued for that transaction has succeeded (see TxCommits). A reloaded store recovers the rest from the CRL, and its
    * recovered vote re-applies them through its UnackedCommits entry (TxCommitted.keptForReplay). Aborted
    * transactions, and those that committed nothing here, are deleted at once.
    *
    * With `stub` (the transaction's start) the state is replaced by an applied stub instead (recordAppliedStub), at
    * the same point: the store holds the committed transaction's update and an UnackedCommits entry naming it may
    * exist (replay-pins addendum A), so it must never apply the transaction again through that entry. A failed backend
    * commit keeps the CRL state while the store is loaded, so no stub replaces it; a reloaded store recovers the
    * transaction from it.
    * Called by the Tx as it is forgotten (`by`), and by a TxFinalized for a transaction the store holds no Tx for.
    * While any Tx of the transaction has a deferred finalization (finalizationDeferred) the state is left alone: a
    * listed Tx's commit is not issued yet and still needs it. A listed Tx is removed from the list by its own call; the
    * call that empties the list concludes the state. A call by any other Tx (a replay's, forgotten while a held commit
    * waits), or by a TxFinalized with no Tx, changes nothing else. A stub the caller owes passes to every Tx still
    * listed, so whichever concludes the state writes it
    */
  def transactionFinalized(txid: TransactionId, stub: Option[HLCTimestamp] = None, by: Option[Tx] = None): Unit =
    val waiting = deferredFinalizations.getOrElse(txid, Nil).filterNot(tx => by.exists(_ eq tx))
    if waiting.isEmpty then
      deferredFinalizations.remove(txid)
      txCommits.get(txid) match
        case Some(r) =>
          r.finalized = true
          r.stub = r.stub.orElse(stub)
        case None => concludeTransactionState(txid, stub, keepAdmission = false)
    else
      deferredFinalizations(txid) = waiting
      if stub.nonEmpty then
        waiting.foreach(_.stubOwed())

  /** Deletes the CRL state of a finalized transaction, or replaces it by an applied stub. A stub already held
    * suffices: the state is only deleted.
    *
    * @param keepAdmission a newer Tx for the transaction (a replay's) is live and shares its admission record: it
    *                      keeps it, and releases it when it is forgotten
    */
  private def concludeTransactionState(txid: TransactionId, stub: Option[HLCTimestamp], keepAdmission: Boolean): Unit =
    if stub.nonEmpty && !stubs.contains(txid) then
      recordAppliedStub(txid, stub.get, keepAdmission)(())
    else
      crl.deleteTransaction(storeId, txid)
      stateConcluded(txid)
      if !keepAdmission then
        releaseAdmission(txid)

  private def transactionCommitComplete(txid: TransactionId, succeeded: Boolean): Unit =
    txCommits.get(txid).foreach { r =>
      r.outstanding -= 1
      if !succeeded then
        r.failed = true
      if r.outstanding == 0 && !r.failed then
        txCommits.remove(txid)
        if r.finalized then
          // The Tx that finalized it was forgotten then: a live one is a later Tx for the same transaction
          concludeTransactionState(txid, r.stub, keepAdmission = transactions.contains(txid))
    }

  /** Called when a transaction takes `os` for one of its objects, and by commit for the commit's duration. A state that
    * only this transaction can know, such as that of an object it allocates, is not shared with others
    * (`shared = false`). The first reference pins the state in the cache: counted, and never evicted.
    */
  def retain(os: ObjectState, shared: Boolean): Unit =
    os.references += 1
    if os.references == 1 then
      objectCache.pin(os)
    if shared then
      referenced += (os.objectId -> os)

  /** Releases a reference taken by retain. The last one unpins the state: still valid and cached, it becomes evictable
    * again as the most recently used entry
    */
  def release(os: ObjectState): Unit =
    os.references -= 1
    if os.references == 0 then
      if referenced.get(os.objectId).exists(_ eq os) then
        referenced -= os.objectId
      objectCache.unpin(os)

  /** Stops serving an object's in-memory state: it was deleted, or it differs from what the backend holds. Later
    * reads load it from the backend. A state still referenced by transactions is marked reloadRequired rather than
    * forgotten, so that the locks on it keep being honoured. See referenced.
    */
  def discard(objectId: ObjectId): Unit =
    referenced.get(objectId).foreach(_.reloadRequired = true)
    objectCache.get(objectId).foreach(_.reloadRequired = true)
    objectCache.remove(objectId)

  /** The state transactions must use for an object, if one is in memory */
  private def stateForTransaction(objectId: ObjectId): Option[ObjectState] =
    referenced.get(objectId) match
      // Transactions holding locks on a discarded state still reference it. Checks against it fail until they resolve
      case Some(os) if !os.reloadRequired || os.lockHolders.nonEmpty => Some(os)
      case Some(_) =>
        referenced -= objectId
        cachedState(objectId)
      case None => cachedState(objectId)

  /** An in-memory state that may be served */
  private def servableState(objectId: ObjectId): Option[ObjectState] =
    referenced.get(objectId).filter(!_.reloadRequired).orElse(cachedState(objectId))

  private def cachedState(objectId: ObjectId): Option[ObjectState] =
    objectCache.get(objectId).filter(!_.reloadRequired)

  /** The store can no longer be trusted (failStop): it handles nothing more and waits for its host to unload it.
    * `@volatile` for reads from other threads
    */
  @volatile private var stopped: Boolean = false

  /** close ran: a deletion still waiting for its allocations' CRL state (awaitAllocationsDurable) is not issued */
  private var closed: Boolean = false

  def failStopped: Boolean = stopped

  /** Fail-stops the store: it can no longer be trusted to hold what its votes promised. From now on it drops every
    * transaction message, read and repair, as an offline store would, and `onFailStop` tells its host, which marks it
    * rebuild-required and unloads it. Never for a condition that freezes writes, such as a full device: such a store
    * keeps serving reads (Backlog: "Full storage devices").
    *
    * The triggers (Tx.issueCommits, Tx.objectLoadFailed) are a commit voter that cannot hold the update it voted for:
    * its locks are gone, so without the update a conflicting transaction could count this store's commit vote against
    * the committed one (transaction-requirements 8.5).
    */
  def failStop(reason: String): Unit =
    if !stopped then
      stopped = true
      failParkedRepairs()
      dropParkedReads()
      logger.error(s"Store $storeId fail-stops: $reason. It handles nothing more and will be rebuilt")
      onFailStop(new IllegalStateException(s"Store $storeId fail-stopped: $reason"))

  def receiveTransactionMessage(msg: TxMessage): Unit =
    if stopped then
      ()
    else if holding || heldMessages.nonEmpty then
      heldMessages = msg :: heldMessages
    else
      handleTransactionMessage(msg)

  private def handleTransactionMessage(msg: TxMessage): Unit = msg match {
    case m: TxPrepare => receivePrepare(m)
    case m: TxAccept => transactions.get(m.transactionId).foreach(tx => tx.receiveAccept(m))
    case m: TxResolved =>
      inheritedVoteDecided(m.transactionId)
      transactions.get(m.transactionId) match
        case Some(tx) => tx.receiveResolved(m)
        // Remembered for a prepare that arrives after this (see receivePrepare). A late outcome implies the
        // transaction was finalized, so it is recorded finalized, as the Tx paths do (Tx.applyNothing). Not a proof:
        // it is meant for drivers and is no outcome (Tx.proveCommitted). The cache is monotone, so a finalized entry
        // stays finalized
        case None =>
          if !m.proof then
            statusCache.updateStatus(m.transactionId, statusOf(m.committed), finalized = m.late, late = m.late)
    case m: TxFinalized =>
      inheritedVoteDecided(m.transactionId)
      transactions.get(m.transactionId) match
        case Some(tx) =>
          // Not if the Tx holds the abort for a UnackedCommits lookup
          if tx.receiveFinalized(m) then
            tx.releaseObjects() // Already done unless some of its objects had not loaded
            transactions -= m.transactionId
        case None =>
          // Remembered for a prepare that arrives after this. See receivePrepare
          statusCache.updateStatus(m.transactionId, statusOf(m.committed), finalized = true, late = m.late)
          // Named as owed an UnackedCommits entry, with no Tx: the store applied the transaction and forgot it (on an
          // earlier TxFinalized that did not list it, or with its backend commits still outstanding), or never saw it.
          // Either way it must not apply it through that entry: one that never saw it is restored by ErrorTree repair
          // from the majority (replay-pins addendum A, case 1)
          if m.committed && !m.late && m.owed.contains(storeId) && !stubs.contains(m.transactionId) then
            // While a commit is held for its turn the state is not deletable yet, nor the update applied: the listed
            // Txs take the stub
            transactionFinalized(m.transactionId, stub = Some(m.startTimestamp))
    case m: TxHeartbeat => transactions.get(m.transactionId).foreach(tx => tx.receiveHeartbeat(m))
    case m: TxStatusRequest =>
      transactions.get(m.transactionId) match
        case Some(tx) => tx.receiveStatusRequest(m)
        case None => receiveStatusRequestWithoutTx(m)
    case m: TxStatusResponse => receiveStatusResponse(m)
    case _ => // Other TxMessages are not relevant
  }

  /** A store with no Tx answers with a cached outcome, or "unknown" when it can never join the transaction (below its
    * forget floor, so the answer is permanent). Above the floor it could still create a Tx: no reply. A rebuilt store
    * whose old incarnation may have held T (mayHaveForgottenUndecided) never answers as a non-holder: no reply but a
    * cached outcome, and that one counted as possibly holding acceptor state
    */
  private def receiveStatusRequestWithoutTx(m: TxStatusRequest): Unit =
    val amnesiac = mayHaveForgottenUndecided(m.startTimestamp)
    val reply = statusCache.getStatus(m.transactionId).filter(_.status != TransactionStatus.Unresolved) match
      case Some(e) =>
        Some(TxStatusResponse.TxStatus(e.status, e.finalized, e.late, holdsAcceptorState = amnesiac, amnesiac = amnesiac))
      case None if amnesiac =>
        // Muted for good: it never joins T, and reports that its old incarnation may have accepted in it
        Some(TxStatusResponse.TxStatus(TransactionStatus.Unresolved, finalized = false, late = false,
          holdsAcceptorState = true, amnesiac = true))
      case None if belowForgetFloor(m.startTimestamp) => None
      case None => return
    net.sendTransactionMessage(TxStatusResponse(m.from, storeId, m.transactionId, m.requestUUID, reply))

  private def receivePrepare(m: TxPrepare): Unit = transactions.get(m.txd.transactionId) match {
    case Some(tx) =>
      logger.trace(s"**** EXISTING TX: ${m.txd.transactionId}. $tx")
      tx.receivePrepare(m)
    // A prepare can arrive after the transaction's resolution (or finalization) did, as when this store was slow or the
    // client's prepare is a retransmission. Started afresh, the Tx would never learn the outcome it missed: a commit vote
    // would hold its requirement locks until finalization, which may itself wait on transactions those locks block, or
    // indefinitely once nothing drives the finalized transaction. Store has already answered a finalized one from the
    // status cache, so it is ignored. A resolved one takes no locks; it still learns its outcome from TxFinalized.
    case None if statusCache.getStatus(m.txd.transactionId).exists(_.finalized) =>
      logger.trace(s"**** IGNORING PREPARE FOR FINALIZED TX: ${m.txd.transactionId}")
      // Admitted while held for recovery, before the finalization was known: no Tx will release it
      releaseAdmission(m.txd.transactionId)
      // Either a retransmission or this store's data arriving too late to be used
      if m.fromClient && m.objectUpdates.nonEmpty then
        val age = (HLCTimestamp.now - m.txd.startTimestamp).toMillis
        logger.warn(s"Client prepare with data for Tx ${m.txd.transactionId} on store ${m.to} arrived after finalization, " +
          s"${age}ms after the transaction started")

    case None =>
      logger.trace(s"**** CREATING NEW TX: ${m.txd.transactionId}")
      val cached = statusCache.getStatus(m.txd.transactionId)
      val known = cached.filter(_.status != TransactionStatus.Unresolved)
      // An Unresolved entry counts as none: another store's Tx on this host wrote it, which says nothing of the outcome
      val beforeAcceptor = acceptorSince.exists(m.txd.startTimestamp < _)
      // The rebuilt store's mute (acceptorSince) keeps its semantics: its muted Tx may still prove the commit.
      // Below the floor a resolved, unfinalized cache entry creates no Tx either (a finalized one was ignored above):
      // the host's cache is shared, so another store's Tx may have written it after this store forgot T, and a Tx
      // created here could then apply T again once this store's applied stub has expired (paxos-transactions 10.2)
      if !beforeAcceptor && belowForgetFloor(m.txd) then
        refusePrepare(m)
      else
        createTx(m, known, beforeAcceptor)
  }

  /** Refuses a prepare for a transaction started below the forget floor that the store does not hold: no Tx, no
    * promise, no vote, no reply, as if the store were offline. It may have forgotten the transaction, and a store
    * never rejoins a transaction it forgot
    */
  private def refusePrepare(m: TxPrepare): Unit =
    // Admitted (Store.receiveTransactionMessage) for a Tx that is not created
    releaseAdmission(m.txd.transactionId)
    if m.fromClient then
      refusalLog.admit().foreach: held =>
        val lag = wallClockMillis() - m.txd.startTimestamp.wallTime
        logger.warn(s"Store $storeId takes no part in transaction ${m.txd.transactionId}: it started ${lag}ms before " +
                    s"this host's clock, below the store's forget floor (horizon ${horizon.toMillis}ms, or the store's " +
                    s"load or a status cache eviction since). The client's clock runs slow, or the transaction is that " +
                    s"old${LogRateLimit.heldText(held)}")

  private def createTx(m: TxPrepare, known: Option[TransactionStatusCache.Entry], beforeAcceptor: Boolean): Unit = {
    val tooFarAhead = !beforeAcceptor && startsTooFarAhead(m.txd)
    val muted = beforeAcceptor || tooFarAhead
    if tooFarAhead then
      val lead = m.txd.startTimestamp.wallTime - wallClockMillis()
      if lead > MaxStartLead.toMillis then
        logger.warn(s"Store $storeId takes no part in transaction ${m.txd.transactionId} yet: it started ${lead}ms " +
                    s"ahead of this host's clock (limit ${MaxStartLead.toMillis}ms). The client's clock runs fast, " +
                    s"or this host's runs slow")
      else
        logger.warn(s"Store $storeId takes no part in transaction ${m.txd.transactionId} yet: it started at or past " +
                    s"the host's join ceiling ${joinCeiling.current.getOrElse(0L)}, whose renewal has fallen behind")
    val trs = TransactionRecoveryState.initial(m.to, m.txd, m.objectUpdates, takeSerializedTxd(m.txd.transactionId))
    val pointers = m.txd.allHostedObjects(m.to)
    val tx = new Tx(trs, m.txd, this, net, crl, statusCache, pointers,
      muted = muted)
    transactions += (m.txd.transactionId -> tx)
    registerReplayHazards(tx, pointers)
    tx.receivePrepare(m)
    // The new Tx recorded itself as Unresolved, which the monotone cache does not let replace a known outcome
    known.foreach { _ =>
      // A muted Tx takes no part, a cached outcome included
      if !muted then
        tx.outcomeDecided(known.get.status == TransactionStatus.Committed)
    }
    pointers.foreach(ptr => readObjectForTransaction(tx, ptr))
  }

  private def statusOf(committed: Boolean): TransactionStatus.Value =
    if committed then TransactionStatus.Committed else TransactionStatus.Aborted

  def backendOperationComplete(completion: Completion): Unit = completion match {
    case r: Read => backendReadComplete(r.objectId, r.result)
    case c: Commit =>
      val key = (c.objectId, c.transactionId)
      var turnFree = false
      inFlight.get(key) match
        case Some(f :: rest) =>
          if rest.isEmpty then inFlight.remove(key) else inFlight(key) = rest
          BackendTiming.report.record(commitStage, System.nanoTime() - f.start)
          transactionCommitComplete(c.transactionId, c.result.isLeft)
          if c.result.isRight then
            failedCommits += c.objectId
          val n = txCommitsInFlight.getOrElse(c.objectId, 1) - 1
          if n == 0 then
            txCommitsInFlight.remove(c.objectId)
            turnFree = true
          else
            txCommitsInFlight(c.objectId) = n
          if !f.deletes then
            committingComplete(f.os)
          c.result match
            case Left(_) =>
              release(f.os)
              if f.deletes then
                val n = deleting.getOrElse(c.objectId, 1) - 1
                if n == 0 then deleting.remove(c.objectId) else deleting(c.objectId) = n
            case Right(err) =>
              // The state stays pinned and served, and a deletion keeps answering ObjectNotFound, while the store is
              // loaded: the store may have voted to commit at the write threshold, so it must neither lose the
              // committed state nor return the pre-commit one. A transaction's CRL state is kept too (see TxCommits).
              // Backlog.txt covers handling this properly
              logger.error(s"Store $storeId: backend commit of object ${c.objectId} for transaction " +
                s"${c.transactionId} failed: $err. Its committed state stays in memory while the store is loaded")
        case _ =>
      transactions.get(c.transactionId).foreach { tx =>
        tx.commitComplete(c.objectId, c.result)
      }
      // Last: the next transaction on the object may commit, and with a synchronous backend complete, inside this
      if turnFree then
        commitTurnFree(c.objectId)
  }

  /** A non-deletion backend commit of `os` completed, successfully or not */
  private def committingComplete(os: ObjectState): Unit =
    val (before, after) = committing.getOrElse(os.objectId, Nil).span(s => !(s eq os))
    val rest = before ++ after.drop(1)
    if rest.isEmpty then committing.remove(os.objectId) else committing(os.objectId) = rest

  /** A miss on `objectId` here is a commit vote on its allocation that is not applied yet, not an absence. Scans the
    * held transactions, so it runs only on a backend miss for a client read (Tx.allocationUnapplied)
    */
  private def allocationPending(objectId: ObjectId): Boolean =
    !deleting.contains(objectId) && transactions.valuesIterator.exists(_.allocationUnapplied(objectId))

  /** Answers a client's read. Dropped while holding for recovered transactions, as a loading store is silent: until a
    * recovered commit vote has loaded its objects and taken its locks, this store would report the state the vote's
    * transaction replaces, unlocked, and a repair trusting the read could undo that commit on a store that applied it
    */
  def readObjectForNetwork(clientId: ClientId, readUUID: UUID, pointer: ObjectPointer, readType: ReadType,
                           awaitCommit: Set[TransactionId] = Set()): Unit =
    if stopped then
      logger.trace(s"Store $storeId dropped read $readUUID: fail-stopped")
    else if holding then
      logger.trace(s"Store $storeId dropped read $readUUID: holding for recovered transactions")
    else
      unparkRead(pointer.id, readUUID)
      if awaitCommit.isEmpty || !parkRead(clientId, readUUID, pointer, readType, awaitCommit) then
        readForNetwork(clientId, readUUID, pointer, readType)

  /** Parks a re-read whose client knows a transaction committed that still holds a write lock on the object here
    * (Read.awaitCommit): answered now, it would only show the lock again, and the client would re-read until this
    * store applies the commit. It is answered once none of the named transactions holds a lock on the object
    * (answerParkedReads), or at parkedReadWait with the state as it is then (expireParkedReads). Never past
    * MaxParkedReads. Only an in-memory state can hold locks, so a read that would reach the backend is not parked.
    * Safety is unaffected: a parked read only delays its own answer
    */
  private def parkRead(clientId: ClientId, readUUID: UUID, pointer: ObjectPointer, readType: ReadType,
                       awaitCommit: Set[TransactionId]): Boolean =
    val objectId = pointer.id
    val waits = servableState(objectId).exists(_.lockedWriteTransactions.exists(awaitCommit.contains))
    if waits && parkedReadCount < Frontend.MaxParkedReads then
      logger.trace(s"Store $storeId: read $readUUID of $objectId waits for the commit of $awaitCommit to apply")
      parkedReads.getOrElseUpdate(objectId, scala.collection.mutable.HashMap())(readUUID) =
        ParkedRead(clientId, pointer, readType, awaitCommit, wallClockMillis() + parkedReadWait.toMillis)
      parkedReadCount += 1
      true
    else
      false

  /** Parks a client's read that missed in the backend while this store holds an unapplied commit vote on the object's
    * allocation (allocationPending). Answered now it could only say AllocationPending, which the client re-reads at
    * once: the allocation either applies (doCommit then serves its state) or aborts (an absence). It is answered once
    * no held transaction has the allocation pending (answerParkedReads, from the allocating Tx's unlock), or at
    * parkedReadWait with AllocationPending if it still is (expireParkedReads). Never past MaxParkedReads. Safety is
    * unaffected: a parked read only delays its own answer
    */
  private def parkAllocationRead(netRead: NetworkRead, pointer: ObjectPointer): Boolean =
    if parkedReadCount < Frontend.MaxParkedReads then
      logger.trace(s"Store $storeId: read ${netRead.requestUUID} of ${pointer.id} waits for its allocation to apply")
      unparkRead(pointer.id, netRead.requestUUID)
      parkedReads.getOrElseUpdate(pointer.id, scala.collection.mutable.HashMap())(netRead.requestUUID) =
        ParkedRead(netRead.clientId, pointer, netRead.readType, Set(), wallClockMillis() + parkedReadWait.toMillis,
          allocation = true)
      parkedReadCount += 1
      true
    else
      false

  private def parkedReadReady(objectId: ObjectId, r: ParkedRead, locks: => Set[TransactionId]): Boolean =
    if r.allocation then !allocationPending(objectId) else !r.awaitCommit.exists(locks.contains)

  private def unparkRead(objectId: ObjectId, readUUID: UUID): Unit =
    if parkedReadCount > 0 then
      parkedReads.get(objectId).foreach: reads =>
        if reads.remove(readUUID).isDefined then
          parkedReadCount -= 1
          if reads.isEmpty then parkedReads.remove(objectId)

  /** Answers the parked reads (parkRead) of `objectIds` none of whose named transactions still holds a lock there.
    * Called by every Tx that releases its locks (Tx.unlock), after a commit's update is applied in memory
    */
  def answerParkedReads(objectIds: Iterable[ObjectId]): Unit =
    if parkedReadCount > 0 then
      objectIds.foreach: objectId =>
        parkedReads.get(objectId).foreach: reads =>
          lazy val locks = servableState(objectId).map(_.lockedWriteTransactions).getOrElse(Set())
          val ready = reads.filter((_, r) => parkedReadReady(objectId, r, locks)).toList
          ready.foreach((readUUID, _) => unparkRead(objectId, readUUID))
          ready.foreach((readUUID, r) => readObjectForNetwork(r.clientId, readUUID, r.pointer, r.readType))

  /** Answers, with the state as it now is, every parked read past its deadline: AllocationPending for an allocation
    * read whose allocation is still pending, which would otherwise only park again. Called on the store's heartbeat */
  def expireParkedReads(): Unit =
    if parkedReadCount > 0 then
      val now = wallClockMillis()
      val due = parkedReads.toList.flatMap((objectId, reads) =>
        reads.toList.collect { case (readUUID, r) if r.until <= now => (objectId, readUUID, r) })
      due.foreach((objectId, readUUID, _) => unparkRead(objectId, readUUID))
      due.foreach: (objectId, readUUID, r) =>
        if r.allocation && allocationPending(objectId) then
          net.sendClientResponse(ReadResponse(r.clientId, storeId, readUUID, HLCTimestamp.now,
            Left(ReadError.AllocationPending)))
        else
          readObjectForNetwork(r.clientId, readUUID, r.pointer, r.readType)

  /** Drops every parked read unanswered, as an offline store would: the store fail-stopped or is closing, and the
    * clients retransmit
    */
  private def dropParkedReads(): Unit =
    parkedReads.clear()
    parkedReadCount = 0

  private def readForNetwork(clientId: ClientId, readUUID: UUID, pointer: ObjectPointer, readType: ReadType): Unit = {
    val objectId = pointer.id

    servableState(objectId) match
      case Some(os) =>
        logger.trace(s"Reading Cached object for read $readUUID. Revision ${os.metadata.revision}")
        val cs = ReadResponse.CurrentState(os.metadata.revision, os.metadata.refcount, os.metadata.timestamp,
          os.data.size, PartialRead.filter(os.objectType, os.data, readType), os.lockedWriteTransactions,
          os.lockedKeys, os.objectLevelLocked)

        val rr = ReadResponse(clientId, storeId, readUUID, HLCTimestamp.now, Right(cs) )
        net.sendClientResponse(rr)

      case None if deleting.contains(objectId) =>
        net.sendClientResponse(ReadResponse(clientId, storeId, readUUID, HLCTimestamp.now,
          Left(ReadError.ObjectNotFound)))

      case None =>
        logger.trace(s"Reading uncached object from backing store for read $readUUID")

        val nr = NetworkRead(clientId, readUUID, readType, pointer)

        pendingReads.get(objectId) match
          case Some(lst) =>
            pendingReads += (objectId -> (nr :: lst))

          case None =>
            pendingReads += (objectId -> (nr :: Nil))
            backend.read(pointer)
  }

  /** Records the store's copy of the object for a repair (Reconcile): first step, before the repair reads it. An
    * object the store does not hold, or is deleting, gives an empty snapshot
    */
  def snapshotForRepair(pointer: ObjectPointer, stamped: Promise[RepairSnapshot]): Unit =
    val objectId = pointer.id
    // Numbered when asked for, which is no later than the state it records (Inherited growth)
    repairSnapshotSeq += 1
    val takenAt = repairSnapshotSeq
    val p = Promise[RepairSnapshot]()
    stamped.completeWith(p.future.map(_.copy(takenAt = takenAt))(using ExecutionContext.parasitic))
    if stopped then p.failure(new IllegalStateException(s"Store $storeId fail-stopped"))
    else servableState(objectId) match
      case Some(os) => p.success(RepairSnapshot.of(os))
      case None if deleting.contains(objectId) => p.success(RepairSnapshot.missing(objectId))
      case None if pointer.poolId != storeId.poolId =>
        p.failure(new IllegalArgumentException(s"Store $storeId cannot repair object $objectId of pool ${pointer.poolId}"))
      case None => readForRepair(pointer, SnapshotRead(pointer, p))

  /** Makes the store's copy agree with `read` (Reconcile.plan), changing only what is still as `snapshot` recorded.
    * `p` completes with what was left undone once the backend has the change
    */
  def reconcileForRepair(snapshot: RepairSnapshot, read: ClientObjectState, p: Promise[ReconcileResult]): Unit =
    val objectId = read.pointer.id
    if !stopped && read.pointer.objectType == ObjectType.KeyValue && inheritedVoteLock(objectId) then
      // A rebuilt store's inherited vote may back a growth of the object that has yet to commit, or the first held-vote
      // round has yet to find what to guard: the entry must stay for a repair the guard can rely on (Inherited
      // growth). Only KeyValue objects are guarded
      p.success(ReconcileResult(Set(), false, stale = true))
    else if !stopped && inheritedGrowthLocks.contains(objectId) then
      val guarded = Promise[ReconcileResult]()
      guarded.future.onComplete(r => post(() => inheritedGrowthRepaired(objectId, snapshot, r, p)))(
        using ExecutionContext.parasitic)
      reconcileUnguarded(snapshot, read, guarded)
    else
      reconcileUnguarded(snapshot, read, p)

  private def reconcileUnguarded(snapshot: RepairSnapshot, read: ClientObjectState,
                                 p: Promise[ReconcileResult]): Unit =
    val objectId = read.pointer.id
    if stopped then p.failure(new IllegalStateException(s"Store $storeId fail-stopped"))
    else servableState(objectId) match
      case Some(os) => reconcile(snapshot, read, p, Some(os))
      // A repair would write the repairer's state after the deletion, resurrecting the slice. Left for a later pass
      case None if deleting.contains(objectId) => p.success(ReconcileResult(Set(), false, stale = true))
      case None if read.pointer.poolId != storeId.poolId =>
        p.failure(new IllegalArgumentException(s"Store $storeId cannot repair object $objectId of pool ${read.pointer.poolId}"))
      case None => readForRepair(read.pointer, ReconcileRead(snapshot, read, p))

  private def readForRepair(pointer: ObjectPointer, kind: ReadKind): Unit =
    pendingReads.get(pointer.id) match
      case Some(lst) => pendingReads += (pointer.id -> (kind :: lst))
      case None =>
        pendingReads += (pointer.id -> (kind :: Nil))
        backend.read(pointer)

  /** Repairs a missed deletion by dropping the object from this store.
    *
    * There is nothing to read first: the object is gone everywhere else, so there is no state to compare against.
    * Refused (RepairDeleteDeferred) while the object is in use here, as reconcile refuses: a backend read in flight
    * could otherwise answer, and cache, the state being deleted, and a commit or transaction on it would race the
    * deletion (repair-process 9.2). While the backend removal is outstanding the object counts as deleting, as for a
    * transaction's deletion, so reads, snapshots and transactions get the answer the backend will give. The cache is
    * evicted for the same reason `commit` evicts it when a refcount reaches zero.
    */
  def deleteObjectForRepair(objectId: ObjectId,
                            storePointer: Array[Byte],
                            completion: Promise[Unit]): Unit =
    if stopped then
      completion.failure(new IllegalStateException(s"Store $storeId fail-stopped"))
    else if holding || pendingReads.contains(objectId) || commitInFlight(objectId) || committing.contains(objectId) ||
      deleting.contains(objectId) || referenced.contains(objectId) then
      completion.failure(new Frontend.RepairDeleteDeferred(
        s"Store $storeId: object $objectId is in use; its missed deletion is repaired on a later pass"))
    else
      logger.trace(s"Repairing missed deletion of object $objectId")
      discard(objectId)
      deleting(objectId) = deleting.getOrElse(objectId, 0) + 1
      val done = Promise[Unit]()
      done.future.onComplete { result =>
        post(() =>
          val n = deleting.getOrElse(objectId, 1) - 1
          if n == 0 then deleting.remove(objectId) else deleting(objectId) = n)
        completion.complete(result)
      }(using scala.concurrent.ExecutionContext.parasitic)
      awaitAllocationsDurable(objectId): () =>
        if stopped || closed then done.failure(new IllegalStateException(s"Store $storeId stopped"))
        else backend.repairDelete(objectId, storePointer, done)

  private def readObjectForTransaction(transaction: Tx, pointer: ObjectPointer): Unit =
    val objectId = pointer.id

    logger.trace(s"Loading object for Tx: ${transaction.transactionId}. Object: $objectId")

    // An object the transaction allocates is read like any other: the store may already hold it (a replayed
    // allocation, transaction-requirements 9.4), and the transaction must then see that state and refuse to overwrite
    // it. Only a backend miss gives the fresh state (readCompleted)
    stateForTransaction(objectId) match
      case Some(os) =>
        logger.trace(s"Loading object from CACHE for Tx: ${transaction.transactionId}. Object: $objectId")
        transaction.objectLoaded(os)

      case None if deleting.contains(objectId) =>
        transaction.objectLoadFailed(objectId, ReadError.ObjectNotFound)

      case None =>
        val tr = TransactionRead(transaction.transactionId)

        pendingReads.get(objectId) match
          case Some(lst) =>
            pendingReads += (objectId -> (tr :: lst))

          case None =>
            pendingReads += (objectId -> (tr :: Nil))
            logger.trace(s"Loading object from Backend for Tx: ${transaction.transactionId}. Object: $objectId")
            backend.read(pointer)

  /** Applies Reconcile.plan to `loaded`, the store's copy (None when it holds none), and hands the result to the
    * backend. Keys and object-level parts this store holds locked are deferred along with those the read reports: a
    * store that did not answer the read may hold locks of its own, and its locked states must keep them
    */
  private def reconcile(snapshot: RepairSnapshot, read: ClientObjectState, p: Promise[ReconcileResult],
                        loaded: Option[ObjectState]): Unit =
    val objectId = read.pointer.id
    // Backend writes of one object are unordered, so a repair writes only while no other write of the object is
    // outstanding, and only over the state the store serves. Otherwise it is left for a later pass: this is rare
    val served = servableState(objectId)
    val unsettled = !settledForRepair(objectId) ||
      (loaded match
        // Another in-memory state is the one served or locked: the loaded copy may be stale. With no cache, an
        // uncached load is the store's state
        case Some(os) => referenced.get(objectId).exists(r => !(r eq os)) || served.exists(c => !(c eq os))
        case None => served.nonEmpty)
    if unsettled && snapshot.awaitSettle && !stopped then
      parkRepair(snapshot, read, p)
    else if unsettled then
      p.success(ReconcileResult(Set(), false, stale = true))
    else
      reconcileSettled(snapshot, read, p, loaded)

  private def parkRepair(snapshot: RepairSnapshot, read: ClientObjectState, p: Promise[ReconcileResult]): Unit =
    val objectId = read.pointer.id
    parkedRepairs.remove(objectId).foreach(_.p.trySuccess(ReconcileResult(Set(), false, stale = true)))
    logger.trace(s"Store $storeId: repair of $objectId waits for the object to settle")
    parkedRepairs(objectId) = ParkedRepair(snapshot, read, p, wallClockMillis() + Frontend.RepairSettleWait.toMillis)

  /** Whether `objectId` has none of the in-flight work that makes a repair of it unsettled. The served-state check
    * of reconcile is made again when the parked repair runs.
    *
    * While holding for recovered transactions, their locks are not all taken: the store's own locks would not defer
    * what they cover. A committed transaction that changes the object and has yet to apply here, held for its turn
    * (heldCommits, within commitInFlight) or waiting in lock order for a commit vote whose locks may lie on another
    * object only (lockWaitingCommits), is unsettled work too: a completed pass would consume the store's ErrorTree mark
    * for it, and its later blind Insert of an absent key could write back a key a later transaction deleted, with no
    * mark left (transaction-recovery 7.4). A parked repair runs once the wait ends and the commit persists
    * (lockWaitWakeVoters, commitTurnFree)
    */
  private def settledForRepair(objectId: ObjectId): Boolean =
    !(holding || commitInFlight(objectId) || lockWaitingCommits.contains(objectId) || commitFailed(objectId) ||
      committing.contains(objectId) || deleting.contains(objectId))

  /** Runs the repair parked on `objectId` once the object is settled. It runs once: still unsettled then (the store
    * serves another state, say), it answers stale
    */
  private def retryParkedRepair(objectId: ObjectId): Unit =
    if parkedRepairs.nonEmpty && settledForRepair(objectId) then
      parkedRepairs.remove(objectId).foreach: parked =>
        // Its promise is already the guarded one, if the object is guarded (reconcileForRepair)
        reconcileUnguarded(parked.snapshot.copy(awaitSettle = false), parked.read, parked.p)

  /** Answers stale every parked repair past its deadline. Called on the store's heartbeat */
  def expireParkedRepairs(): Unit =
    if parkedRepairs.nonEmpty then
      val now = wallClockMillis()
      parkedRepairs.filter((_, parked) => parked.until <= now).foreach: (objectId, parked) =>
        parkedRepairs.remove(objectId)
        parked.p.trySuccess(ReconcileResult(Set(), false, stale = true))

  private def failParkedRepairs(): Unit =
    val parked = parkedRepairs.values.toList
    parkedRepairs.clear()
    parked.foreach(_.p.tryFailure(new IllegalStateException(s"Store $storeId stopped")))

  private def reconcileSettled(snapshot: RepairSnapshot, read: ClientObjectState, p: Promise[ReconcileResult],
                               loaded: Option[ObjectState]): Unit =
    val current = loaded.map(RepairSnapshot.of).getOrElse(RepairSnapshot.missing(read.pointer.id))
    // A proof transaction showed the read current on the proven parts, so the locks it reports there are ignored
    val reported = ReadLocks(read.readLocks.keys -- snapshot.proven.keys,
                             read.readLocks.objectLevel && !snapshot.proven.objectLevel)
    val locks = loaded match
      case None => reported
      case Some(os) =>
        ReadLocks(reported.keys ++ os.lockedKeys, reported.objectLevel || os.objectLevelLocked)
    val plan = Reconcile.plan(snapshot, current, read, storeId, locks)

    if plan.changesNothing then
      // The store may lack a deletion of a key while the read lacks it too, so a complete pass's entry delete clears
      // a mark for it: a blind Insert of the key by a transaction started before the deletion is then reported
      // (Tx.repairInstalled), as after a repair write. That deletion started no later than the entry's minimum
      // timestamp, so the pass is recorded there. An incomplete pass deletes no entry, nor does the sweep (Zero)
      val min = snapshot.minTimestamp
      if plan.result.complete && min > HLCTimestamp.Zero && read.timestamp >= min then
        recordRepairInstall(read.pointer.id, min)
      p.success(plan.result)
    else if !repairFitsReservations(loaded, plan) then
      logger.trace(s"Store $storeId: repair of ${read.pointer.id} would overfill the slice beside its held commit " +
        "votes' reservations. Left for a later pass")
      p.success(ReconcileResult(Set(), false, stale = true))
    else
      // What a failed write restores (repairWriteComplete). Only the fields the plan changes: a locked key's ValueState
      // is deferred, so never replaced
      val prior = loaded.map: os =>
        val touched = plan.installs.keySet ++ plan.deletes
        PreRepairState(os.metadata, os.data, os.kvState.map(kvs =>
          (kvs.min, kvs.max, kvs.left, kvs.right, touched.map(key => key -> kvs.content.get(key)).toMap)))
      val os = loaded.getOrElse(
        new ObjectState(read.pointer.id, Metadata.Zeroed, read.pointer.objectType, DataBuffer.Empty))
      os.kvState.foreach { kvs =>
        plan.installs.foreach { (key, vs) =>
          kvs.content += key -> new ValueState(vs.value, vs.revision, vs.timestamp, None)
        }
        kvs.content --= plan.deletes
        plan.objectLevel.foreach { level =>
          kvs.min = level.min
          kvs.max = level.max
          kvs.left = level.left
          kvs.right = level.right
        }
        os.data = kvs.encode()
      }
      plan.data.foreach(d => os.data = d)
      val md = os.metadata
      os.metadata = Metadata(plan.revision.getOrElse(md.revision), plan.refcount.getOrElse(md.refcount),
        plan.timestamp.getOrElse(md.timestamp))
      logger.trace(s"Store $storeId: repair of ${os.objectId} installs ${plan.installs.size} keys, deletes " +
        s"${plan.deletes.size}, leaves ${plan.result}")
      recordRepairInstall(os.objectId, os.metadata.timestamp)

      // Held as commit holds an in-flight commit's state: pinned, referenced and served from memory, so nothing reads
      // the pre-repair state from the backend meanwhile. Counted as an in-flight commit too: transactions on the
      // object wait behind the write as behind a transaction's backend commit, and are released when it completes
      val objectId = os.objectId
      objectCache.insert(os)
      retain(os, shared = true)
      committing(objectId) = os :: committing.getOrElse(objectId, Nil)
      txCommitsInFlight(objectId) = txCommitsInFlight.getOrElse(objectId, 0) + 1
      val done = Promise[Unit]()
      done.future.onComplete { result =>
        post(() => repairWriteComplete(os, result.isSuccess, prior))
        p.complete(result.map(_ => plan.result.copy(changed = true)))
      }(using scala.concurrent.ExecutionContext.parasitic)
      // A key the repair removes reaches the backend only once no blind Insert's CRL state can bring it back, as for a
      // transaction's key deletion (awaitBlindInsertsDurable)
      val removed = plan.deletes.toSet
      awaitBlindInsertsDurable(objectId, None, () => removed): () =>
        if stopped || closed then done.tryFailure(new IllegalStateException(s"Store $storeId stopped"))
        else backend.repair(CommitState(read.pointer, os.metadata, os.objectType, os.data), done)

  /** Whether `plan`, applied to `loaded`, keeps the slice within maxKVSliceSize beside the growth the commit votes held
    * on the object reserve (ObjectState.sliceReservations). Only a checked vote or an apply may otherwise change a
    * slice that holds reservations, which is what keeps the slice plus every reservation within the limit, so each
    * commit voter's apply re-check fits (transaction-requirements R16). A plan that grows the slice is measured as a
    * vote is (RequirementsChecker.checkKVSliceSize). One that does not fit is left for a later pass, once the votes
    * have applied or been released. A plan that grows neither bytes nor entries always fits
    */
  private def repairFitsReservations(loaded: Option[ObjectState], plan: ReconcilePlan): Boolean =
    val fits = for
      limit <- maxKVSliceSize
      os <- loaded
      if os.sliceReservations.nonEmpty
      kvs <- os.kvState
    yield
      val level = plan.objectLevel
      val content = kvs.content -- plan.deletes ++ plan.installs.map((key, vs) =>
        key -> new ValueState(vs.value, vs.revision, vs.timestamp, None))
      def sansCount(size: Int, count: Int): Long = size.toLong - Varint.getUnsignedIntEncodingLength(count)
      val after = KVObjectState.encodedSize(level.fold(kvs.min)(_.min), level.fold(kvs.max)(_.max),
        level.fold(kvs.left)(_.left), level.fold(kvs.right)(_.right), content)
      val growth = SliceGrowth(sansCount(after, content.size) - sansCount(kvs.encodedSize, kvs.content.size),
        content.size - kvs.content.size)
      try
        RequirementsChecker.checkKVSliceSize(os.objectId, kvs, growth, os.sliceReservations.valuesIterator, limit)
        true
      catch case _: RequirementsChecker.ObjectErr => false
    fits.getOrElse(true)

  /** A repair's backend write of `os` completed: its state is no longer held in memory for the write, and the
    * transactions waiting behind it may proceed.
    *
    * A failed write leaves memory ahead of the backend, so the fields the repair changed are restored to `prior`, the
    * loaded state it was planned on. That state was settled, so it is the backend's, and while the write was in
    * flight no transaction locked, checked or applied to the object (it counts as an in-flight commit). The instance
    * stays the one served and the one lock holders reference: discarding it would fail their committed apply
    * (StaleObjectState). A repair of an object the store did not hold (`prior` empty) discards its state instead. The
    * repair's promise fails, so its entry is kept for a later pass. Unlike a failed transaction commit it is not
    * recorded in failedCommits, so later transactions on the object are not refused: no transaction relied on the
    * repaired state being durable
    */
  private def repairWriteComplete(os: ObjectState, succeeded: Boolean, prior: Option[PreRepairState]): Unit =
    val objectId = os.objectId
    committingComplete(os)
    release(os)
    if !succeeded then
      prior match
        case Some(pre) =>
          logger.warn(s"Store $storeId: repair write of object $objectId failed. Its pre-repair state is restored")
          os.metadata = pre.metadata
          os.data = pre.data
          for kvs <- os.kvState; (min, max, left, right, entries) <- pre.kv do
            kvs.min = min
            kvs.max = max
            kvs.left = left
            kvs.right = right
            kvs.content = entries.foldLeft(kvs.content): (content, entry) =>
              entry match
                case (key, Some(vs)) => content + (key -> vs)
                case (key, None) => content - key
        case None =>
          logger.warn(s"Store $storeId: repair write of object $objectId failed. Its state is reloaded from the backend")
          discard(objectId)
    val n = txCommitsInFlight.getOrElse(objectId, 1) - 1
    if n == 0 then
      txCommitsInFlight.remove(objectId)
      commitTurnFree(objectId)
    else
      txCommitsInFlight(objectId) = n

  def backendReadComplete(objectId: ObjectId,
                          result: Either[ReadState, ReadError.Value]): Unit = {
    // Taken before dispatch: handling a read can start another read of the same object (a transaction message held
    // for recovery is handled once a recovered transaction loads), which must not join this completed one
    val waiting = pendingReads.get(objectId)
    pendingReads -= objectId

    // A read issued before a deletion's commit may return the state the deletion is replacing
    val effective = if deleting.contains(objectId) then Right(ReadError.ObjectNotFound) else result

    effective match {
      case Left(rs) =>
        // A state still referenced by transactions is never replaced. See referenced. While a non-deletion commit of
        // the object is in flight, the read may have returned the state that commit replaces: the committed state is
        // served instead, if it can be, and otherwise the load answers the waiting readers without being cached
        val loaded = new ObjectState(objectId, rs.metadata, rs.objectType, rs.data)
        val os = referenced.get(objectId).filter(!_.reloadRequired) match
          case Some(inUse) => inUse
          case None =>
            committing.get(objectId) match
              case Some(newest :: _) => if newest.reloadRequired then loaded else newest
              case _ =>
                objectCache.insert(loaded)
                loaded

        waiting.foreach { lpr =>
          lpr.foreach {
            // Dropped, as at arrival, if recovery started meanwhile (see readObjectForNetwork)
            case netRead: NetworkRead if holding =>
              logger.trace(s"Store $storeId dropped read ${netRead.requestUUID}: holding for recovered transactions")

            case netRead: NetworkRead =>
              logger.trace(s"Completed backend load for read ${netRead.requestUUID}. SUCCESS. Sending read response")
              val cs = ReadResponse.CurrentState(os.metadata.revision, os.metadata.refcount, os.metadata.timestamp,
                os.data.size, PartialRead.filter(os.objectType, os.data, netRead.readType), os.lockedWriteTransactions,
                os.lockedKeys, os.objectLevelLocked)

              val rr = ReadResponse(netRead.clientId, storeId, netRead.requestUUID, HLCTimestamp.now, Right(cs) )
              net.sendClientResponse(rr)

            case tr: TransactionRead =>
              logger.trace(s"Completed read for transaction ${tr.transactionId}. Object: ${os.objectId}")
              transactions.get(tr.transactionId).foreach { tx => tx.objectLoaded(stateForTransaction(objectId).getOrElse(os)) }

            case SnapshotRead(_, p) => p.success(RepairSnapshot.of(os))

            case ReconcileRead(snapshot, read, p) =>
              logger.trace(s"Completed read for repair. Object: ${os.objectId}")
              reconcile(snapshot, read, p, Some(os))
          }
        }

      case Right(err) =>
        waiting.foreach { lpr =>
          lpr.foreach {
            case netRead: NetworkRead if holding =>
              logger.trace(s"Store $storeId dropped read ${netRead.requestUUID}: holding for recovered transactions")

            // The miss is older than a state committed meanwhile (an allocation applied while the load was in flight):
            // the committed state is served instead, as the success branch serves a newer in-flight state
            case netRead: NetworkRead if err == ReadError.ObjectNotFound && servableState(objectId).isDefined =>
              logger.trace(s"Store $storeId: backend miss for read ${netRead.requestUUID} of $objectId superseded by a " +
                "committed state")
              readForNetwork(netRead.clientId, netRead.requestUUID, netRead.pointer, netRead.readType)

            case netRead: NetworkRead =>
              logger.trace(s"Completed backend load for read ${netRead.requestUUID}. ERROR $err. Sending read response")
              val pending = err == ReadError.ObjectNotFound && allocationPending(objectId)
              if !pending || !parkAllocationRead(netRead, netRead.pointer) then
                val reply = if pending then ReadError.AllocationPending else err
                val rr = ReadResponse(netRead.clientId, storeId, netRead.requestUUID, HLCTimestamp.now, Left(reply) )
                net.sendClientResponse(rr)

            case tr: TransactionRead => transactions.get(tr.transactionId).foreach { tx =>
              // A miss is what lets a transaction allocate the object here; a deletion in flight is no miss
              if err == ReadError.ObjectNotFound && !deleting.contains(objectId) &&
                tx.txd.allocatingObjects.contains(objectId) then
                tx.allocationLoaded(objectId)
              else
                tx.objectLoadFailed(objectId, err)
            }

            case SnapshotRead(_, p) => p.success(RepairSnapshot.missing(objectId))

            case ReconcileRead(snapshot, read, p) =>
              // Not found because a deletion's commit is in flight: a repair would resurrect the slice
              if deleting.contains(objectId) then p.success(ReconcileResult(Set(), false, stale = true))
              else reconcile(snapshot, read, p, None)
          }
        }
    }
  }
}
