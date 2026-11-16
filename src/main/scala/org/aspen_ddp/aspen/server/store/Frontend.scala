package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.ida.IDA
import java.util.UUID
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, CurrentAllocationsReply, CurrentAllocationsRequest, PendingAllocation, ReadResponse, TxAccept, TxFinalized, TxHeartbeat, TxMessage, TxPrepare, TxResolved, TxStatusRequest, TxStatusResponse}
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectPointer, ObjectType, ReadError, ReadType}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, TransactionHorizon, RefcountUpdate, TransactionDescription, TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.{AdmissionTier, CrashRecoveryLog, CrlAdmission, StoreRecoveryState,
  TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendTiming, Commit, CommitState, Completion, Read}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{ExpiryTally, TransactionStatusCache, Tx}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.common.util.LogRateLimit
import scribe.Logging
import org.aspen_ddp.aspen.client.{ReadLocks, ObjectState as ClientObjectState}

import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}


object Frontend {

  /** A repair's deletion of an object was refused because the object is in use on the store (a read, a commit or a
    * deletion in flight, or a transaction holding it): the repair keeps its entry and tries again on a later pass
    */
  final class RepairDeleteDeferred(message: String) extends Exception(message)

  /** How long a store withholds its vote from drivers' prepares while waiting for a client's object data.
    * Several client retransmissions long, since the only cost of a late abort is a client retry.
    */
  val MissingDataWait: Duration = Duration(5, SECONDS)

  /** How long a repair reconcile may wait for its object to settle (RepairSnapshot.awaitSettle, repair F8). Checked
    * on the store's heartbeat, so a parked repair that never sees its object settle answers stale up to a heartbeat
    * period later
    */
  val RepairSettleWait: FiniteDuration = FiniteDuration(1, SECONDS)

  /** How long a UnackedCommits lookup may go unanswered before the store heartbeat makes another. An answer can be
    * lost: one completing before its store is installed in the StoreRouter is dropped
    */
  val UnackedLookupTimeout: FiniteDuration = FiniteDuration(5, SECONDS)

  /** How far a transaction's start timestamp, which its client chose, may lead the receiving host's wall clock for the
    * store to take part in it. A prepare for an unknown transaction started further ahead is muted (Tx.muted), as at a
    * rebuilt store, and the client's retransmission is answered once the host's clock catches up. A store therefore
    * joins a transaction only if it started at most this long after the host's clock when the store first saw it. An
    * old incarnation stopped before its rebuild began, so every transaction it joined started before its rebuilt
    * store's acceptor-since time, whatever the client's clock, provided this bound plus the skew between the hosts'
    * clocks stays under RebuildingStore.AcceptorSinceMargin (paxos P-N2). It also bounds how far timestamps that
    * clients ratchet their HLCs from can run ahead of the hosts' clocks
    */
  val MaxStartLead: FiniteDuration = TransactionHorizon.MaxStartLead

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

  /** The most store heartbeats between retries of a quarantined object's verification (Frontend.quarantine) */
  val MaxQuarantineRetryInterval: Int = 60

  /** How often, at most, a store warns of a client prepare it refuses (refusePrepare). While a status cache eviction's
    * watermark is the binding part of the floor, every client prepare is refused
    */
  val RefusalLogPeriod: FiniteDuration = FiniteDuration(10, SECONDS)

  sealed abstract class ReadKind

  case class NetworkRead(clientId: ClientId, requestUUID: UUID, readType: ReadType) extends ReadKind

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
               /** Verifies quarantined objects through repair: see quarantine */
               val quarantineVerifier: QuarantineVerifier = QuarantineVerifier.Never,
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
               /** How long past the horizon an unresolved Tx starts its status queries (Frontend.expiryMargin) */
               val expiryMargin: FiniteDuration = Frontend.DefaultExpiryMargin,
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
               onCommitProven: TransactionId => Unit = _ => ()) extends Logging {

  /** Which life of the store's data this is: prepare replies carry it (TxPrepareResponse.incarnation) */
  val incarnation: StoreIncarnation = incarnationOverride.getOrElse(StoreIncarnation.default(storeId))

  import Frontend._

  // A refused client prepare is warned of once per RefusalLogPeriod at most (refusePrepare)
  private val refusalLog = new LogRateLimit(RefusalLogPeriod.toMillis, wallClockMillis)

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

  /** The load's part of the forget floor: the load plus MaxStartLead, since before its load the store may have joined,
    * and forgotten, a transaction started up to MaxStartLead ahead of its host's clock. Plus
    * RebuildingStore.AcceptorSinceMargin instead while the store's arrival from another host (a transfer in, an
    * adoption) is less than that margin before the load: it may have joined such a transaction on that host, whose
    * clock may lead this one's by up to the skew the margin also covers (paxos P-N2). A restart within the margin of
    * the arrival keeps it, as the restarted store may still have forgotten one
    */
  val loadFloorMillis: Long =
    val margin = RebuildingStore.AcceptorSinceMargin.toMillis
    if arrivedAt.exists(_ + margin > loadedAtMillis) then loadedAtMillis + margin
    else loadedAtMillis + MaxStartLead.toMillis

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

  /** The transaction started more than MaxStartLead ahead of this host's wall clock: a prepare for it, if unknown, is
    * muted (see MaxStartLead), and its designated leader does not drive it (Store) */
  def startsTooFarAhead(txd: TransactionDescription): Boolean =
    txd.startTimestamp.wallTime > wallClockMillis() + MaxStartLead.toMillis

  def holdsLocks(txid: TransactionId): Boolean = transactions.get(txid).exists(_.holdsLocks)

  /** Callable from any thread: whether the store holds `txid` live (recovered or active), or may once the CRL state it
    * is loading is recovered. Reads one snapshot of each `@volatile` field and touches no other state
    */
  def mayHoldTransaction(txid: TransactionId): Boolean = awaitingCrl || transactions.contains(txid)

  /** The store's CRL state is still to be recovered (awaitRecovery): see Store.driveTransaction */
  def recoveryPending: Boolean = awaitingCrl

  /** The allocations in this store's pool held by transactions it has not seen finalized and not seen abort, for a
    * rebuilding peer (Rebuild F10). None while the CRL is still loading: transactions it does not know yet may be
    * among them. A committed transaction stays held until TxFinalized, which comes only once its
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

  def receiveCurrentAllocationsRequest(req: CurrentAllocationsRequest): Unit =
    currentAllocations().foreach: allocations =>
      net.sendClientResponse(CurrentAllocationsReply(req.fromClient, storeId, req.queryUUID, allocations))

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

  /** A transaction's backend commit of `oid` failed: every later transaction on it votes abort until the store reloads */
  def commitFailed(oid: ObjectId): Boolean = failedCommits.contains(oid)

  /** Treats `oid` as if a backend commit of it had failed (commitFailed): every later transaction on it votes abort, and
    * repair leaves it alone, until the store reloads. For an object a recovered commit vote could not load on a store
    * whose writes are frozen (Tx.objectLoadFailed), which must keep serving rather than fail-stop
    */
  def poisonObject(oid: ObjectId): Unit = failedCommits += oid

  // ---- Quarantine (transaction-requirements F5 residual, Todo 2.16) ----

  /** A quarantined object: see quarantine.
    *
    * @param minTimestamp the latest start of the transactions that quarantined it: a verification must read the object
    *                     at least this new
    * @param txids        the transactions whose CRL state the quarantine keeps
    * @param attempt      identifies the latest verification: an answer to an older one is ignored
    * @param countdown    store heartbeats until the next verification, while none is outstanding
    * @param interval     heartbeats between the next failed verification and the one after it, doubling to
    *                     MaxQuarantineRetryInterval
    */
  private final class Quarantine(val pointer: ObjectPointer, var minTimestamp: HLCTimestamp,
                                 var txids: Set[TransactionId], var attempt: Long, var inFlight: Boolean,
                                 var countdown: Int, var interval: Int, var failures: Int)
  private val quarantined = scala.collection.mutable.HashMap[ObjectId, Quarantine]()

  /** Finalized transactions whose CRL state a quarantine keeps (concludeTransactionState), and the start timestamp of
    * an applied stub owed, if any: concluded once no quarantine names them
    */
  private val quarantinedConclusions = scala.collection.mutable.HashMap[TransactionId, Option[HLCTimestamp]]()

  /** The object is quarantined: every transaction on it votes abort (ObjectQuarantined) and a committed one applies
    * nothing to it, until repair verifies the store's copy
    */
  def isQuarantined(oid: ObjectId): Boolean = quarantined.contains(oid)

  /** Quarantines placed by this Frontend (quarantine), for tests that cannot observe one before its verification clears
    * it
    */
  @volatile private[aspen] var testingOnlyQuarantines: Int = 0

  /** Quarantines an object a recovered commit vote could not apply its committed update to (Tx.issueCommits). Before
    * the crash the store may have applied it and a later transaction superseded it, or the state may have been
    * invalidated under the recovered locks: the vote then counts with neither its lock nor its update. The store does
    * not tell these apart. Every transaction on the object votes abort, as the lock would have made it, and committed
    * ones apply nothing to it (reported, so their finalizers mark the store for repair), until a verification
    * (QuarantineVerifier) shows the store's copy matches a read of the object no older than `minTimestamp`, the
    * transaction's start: what repair requires before it drops an ErrorTree entry for the transaction. Repair is not
    * held off the object, unlike commitFailed.
    *
    * In memory only: the quarantining transaction's CRL state, object data included, is kept until the quarantine
    * clears (concludeTransactionState), so a reloaded store recovers it, fails the same re-check, and quarantines the
    * object again. Nothing is written for this. It replaces the fail-stop a live voter gets, so it declares no lost store
    */
  def quarantine(pointer: ObjectPointer, minTimestamp: HLCTimestamp, txid: TransactionId, reason: String): Unit =
    val oid = pointer.id
    logger.error(s"Store $storeId quarantines object $oid: recovered commit vote of transaction $txid cannot apply its " +
      s"committed update ($reason). Every transaction on it votes abort until repair verifies the store's copy is at " +
      s"least as new as $minTimestamp")
    testingOnlyQuarantines += 1
    quarantined.get(oid) match
      case Some(q) =>
        q.txids += txid
        if q.minTimestamp < minTimestamp then
          q.minTimestamp = minTimestamp
          startVerification(oid, q)
      case None =>
        val q = new Quarantine(pointer, minTimestamp, Set(txid), 0, false, 0, 1, 0)
        quarantined(oid) = q
        startVerification(oid, q)

  private def startVerification(oid: ObjectId, q: Quarantine): Unit =
    q.attempt += 1
    q.inFlight = true
    val attempt = q.attempt
    quarantineVerifier.verify(storeId, q.pointer, q.minTimestamp,
      result => post(() => verificationComplete(oid, attempt, result)))

  /** A verification of a quarantined object answered. Complete: the quarantine clears and the CRL state it kept is
    * concluded. Otherwise it is retried at heartbeat, after 1 heartbeat doubling to MaxQuarantineRetryInterval
    */
  def verificationComplete(oid: ObjectId, attempt: Long, result: Try[Boolean]): Unit =
    quarantined.get(oid).filter(_.attempt == attempt).foreach: q =>
      q.inFlight = false
      result match
        case Success(true) =>
          logger.info(s"Store $storeId: repair verified quarantined object $oid at least as new as " +
            s"${q.minTimestamp}. Quarantine cleared")
          quarantined.remove(oid)
          q.txids.foreach: txid =>
            if !quarantined.valuesIterator.exists(_.txids.contains(txid)) then
              quarantinedConclusions.remove(txid).foreach: stub =>
                concludeTransactionState(txid, stub, keepAdmission = transactions.contains(txid))
        case other =>
          q.failures += 1
          q.countdown = q.interval
          q.interval = math.min(q.interval * 2, MaxQuarantineRetryInterval)
          val why = other match
            case Failure(err) => s"failed: $err"
            case _ => "is not complete yet (stale, or deferred by locks)"
          logger.warn(s"Store $storeId: verification of quarantined object $oid $why (${q.failures} in a row). " +
            s"Retrying in ${q.countdown} heartbeats")

  /** Store heartbeat: starts the verifications whose retry is due */
  def retryQuarantineVerifications(): Unit =
    quarantined.toList.foreach: (oid, q) =>
      if !q.inFlight then
        q.countdown -= 1
        if q.countdown <= 0 then
          startVerification(oid, q)

  private def quarantineKeeps(txid: TransactionId): Boolean =
    quarantined.valuesIterator.exists(_.txids.contains(txid))

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

  /** A repair reconcile waiting for its object to settle (RepairSnapshot.awaitSettle, repair F8), until `until`
    * (wallClockMillis)
    */
  private final class ParkedRepair(val snapshot: RepairSnapshot, val read: ClientObjectState,
                                   val p: Promise[ReconcileResult], val until: Long)

  /** At most one per object: one scan repairs an object at a time, and a newer request replaces an older one.
    * Declared before the constructor's recovery, which can release held messages (releaseHeldMessages)
    */
  private val parkedRepairs = scala.collection.mutable.HashMap[ObjectId, ParkedRepair]()
  private val commitStage = s"store commit (${backend.getClass.getSimpleName})"

  /** A transaction's backend commits on this store, made through commit.
    *
    * The store deletes a committed transaction's CRL state only once every backend commit it issued for that
    * transaction has succeeded. Until then the CRL entry, object data included, outlives the Tx, which is forgotten at
    * finalization. A failed commit leaves the record, and so the CRL entry, for the life of the loaded store: a reloaded
    * store recovers the transaction from it, and its transaction driver concludes it.
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

  /** Recovered transactions whose objects have not all loaded yet, and whether each has a persisted vote */
  private var recoveringTransactions: Map[TransactionId, Boolean] = Map()

  /** Recovered transactions without a persisted vote that have loaded their objects, waiting to decide it. See
    * recoveredTransactionLoaded
    */
  private var undecidedRecovered: List[Tx] = Nil

  private def holding: Boolean = awaitingCrl || recoveringTransactions.nonEmpty || undecidedRecovered.nonEmpty

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
    */
  private final class UnackedLookup(var latest: Long, var inFlight: Boolean, var startedAt: Long,
                                      var firstCounting: Long, var failed: Boolean, var failures: Int)
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

  private def recoverState(state: StoreRecoveryState): Unit =
    // Counted without a check, as recovered transactions are: the CRL already holds them
    chargeStubs(state.appliedStubs.count((txid, start) => addStub(txid, start)))
    recoverTransactions(state.transactions)

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
        // A transaction with no objects here never loads any
        if pointers.nonEmpty then
          recoveringTransactions += (txd.transactionId -> (trs.disposition != TransactionDisposition.Undetermined))
        Some((tx, pointers))
    }
    recovered.foreach((tx, pointers) => pointers.foreach(ptr => readObjectForTransaction(tx, ptr)))

  /** A recovered transaction has loaded (or failed to load) all its objects. One with a persisted vote completes its
    * load at once, taking the locks of a commit vote. One without decides its vote only once every recovered
    * transaction with a persisted vote has done so: deciding earlier, it could take locks that conflict with theirs.
    */
  def recoveredTransactionLoaded(tx: Tx, hasVote: Boolean): Unit =
    recoveringTransactions -= tx.transactionId
    if hasVote then
      tx.completeLoad()
    else
      undecidedRecovered = undecidedRecovered :+ tx
    if !recoveringTransactions.valuesIterator.contains(true) then
      val undecided = undecidedRecovered
      undecidedRecovered = Nil
      undecided.foreach(_.completeLoad())
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
    * dropped or held back, while new prepares feel its bytes: the CRL's copy-forward carries stubs too. It carries the
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
    crl.saveAppliedStub(storeId, txid, stubs(txid), () => post { () =>
      stubReplacedAdmissions.remove(txid).foreach(admission.release)
      onDurable
    })

  /** Drops the applied stub for `txid`, if the store holds one, deletes it from the CRL and releases its charge. Only
    * expireStubs drops one; visible for tests */
  private[server] def dropAppliedStub(txid: TransactionId): Unit =
    if stubs.contains(txid) then
      stubs -= txid
      heldStubs -= txid
      releaseStub()
      crl.deleteAppliedStub(storeId, txid)

  /** Drops every applied stub whose transaction is past the horizon plus the expiry margin (stubExpiresAt) and for
    * which the store holds no Tx, whether live, waiting on a held commit (deferredFinalizations), on its backend
    * commits (txCommits) or on a quarantine (quarantinedConclusions). Called at heartbeat (expiryHeartbeat). Only the
    * stubs due are looked at (stubExpiry), plus those a Tx kept past theirs (heldStubs), so a heartbeat costs
    * O(k log n) for k stubs due, not a pass over all n.
    *
    * A stub only ever keeps a Tx for its transaction at this store from applying the update again. Past the margin T is
    * below the forget floor, so a prepare for it creates no Tx (receivePrepare: refused, or ignored when the host's
    * status cache knows T finalized), and with none held no Tx for T can arise here again: the stub can suppress
    * nothing. This bounds the stubs by their rate times the horizon plus the margin, whether or not repair scans run
    * (crash-recovery-log F22, repair RP-N6, Todo 4.6). An UnackedCommits entry left without its stub is harmless: no
    * Tx is left to apply through it, and the scan's reconcile is exact
    */
  private def expireStubs(now: Long): Unit =
    def txHeld(txid: TransactionId): Boolean =
      transactions.contains(txid) || deferredFinalizations.contains(txid) || txCommits.contains(txid) ||
        quarantinedConclusions.contains(txid)
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
    // Through `post`, so a store replaced meanwhile drops the answer rather than handing it to its successor
    unackedLookup.lookup(storeId, txid, requestId,
      result => post(() => unackedLookupComplete(txid, requestId, result)))

  /** The answer to UnackedCommits lookup `requestId`, delivered on the store's executor.
    *
    * An entry found resolves the Tx committed, whichever lookup found it. It applies through the entry only if the
    * entry names this store's own incarnation (Tx.unackedCommitFound); one naming another incarnation, or none,
    * concludes it committed with nothing applied (Tx.unackedCommitNotOwed). Either way it is never taken as absent.
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
              if found.contains(incarnation) then
                // Forgotten by the Tx itself only for a held late commit: see Tx.unackedCommitFound
                tx.unackedCommitFound()
              else
                // Another incarnation's vote counted (this store was rebuilt since), or the driver never heard from
                // this store: T committed, but nothing is owed to this incarnation. See Tx.unackedCommitNotOwed
                logger.info(s"Store $storeId (incarnation ${incarnation.uuid}): the UnackedCommits entry for " +
                  s"transaction $txid names incarnation ${found.fold("unknown")(_.uuid.toString)}: applying nothing " +
                  s"through it")
                tx.unackedCommitNotOwed()
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

  /** Store heartbeat: makes another lookup for each one that failed, and for each unanswered after
    * `Frontend.UnackedLookupTimeout`, whose answer may have been lost
    */
  def retryUnackedLookups(now: Long = System.nanoTime()): Unit =
    unackedLookups.toList.foreach: (txid, l) =>
      val timedOut = l.inFlight && now - l.startedAt >= UnackedLookupTimeout.toNanos
      if l.failed || timedOut then
        if transactions.get(txid).exists(_.awaitsUnackedLookup) then
          if timedOut then
            logger.warn(s"Store $storeId: UnackedCommits lookup of transaction $txid went unanswered " +
              s"for $UnackedLookupTimeout, looking up again")
          startUnackedLookup(txid, l)
        else
          unackedLookups.remove(txid)

  // ---- Expiry of unresolved transactions past the horizon (transaction horizon design, section 2) ----

  /** An expiring Tx's status queries. Each round has its own request id and counts only its own replies.
    *
    * @param replies this round's replies by store, this store's own included; a store absent from it was silent
    * @param countdown store heartbeats left until the next round
    * @param interval heartbeats between the next round and the one after: 1, doubling to MaxExpiryQueryInterval
    * @param decided this round's replies decided: later ones of it are ignored
    */
  private final class ExpiryRound(var requestUUID: UUID, var replies: Map[StoreId, Option[TxStatusResponse.TxStatus]],
                                  var countdown: Int, var interval: Int, var decided: Boolean)
  private val expiryRounds = scala.collection.mutable.HashMap[TransactionId, ExpiryRound]()

  /** Store heartbeat: each Tx still unfinalized past the horizon plus the expiry margin (Tx.mayExpire: unresolved, or
    * resolved and waiting for a TxFinalized that may never come) queries the status of every other store of its
    * transaction, first at the heartbeat it is found so, then after 1, 2, 4 and then every MaxExpiryQueryInterval
    * heartbeats, until it decides from the replies (ExpiryTally): it adopts an outcome, is discarded (concluded, if it
    * knows its outcome: Tx.concludeKnownOutcome), or keeps holding. Only the replies decide, so a Tx whose peers still
    * hold T loses nothing by asking.
    *
    * A Tx held only to decide its applied stub (Tx.forgetLookupPending) asks nothing: past the margin it is forgotten
    * without one (Tx.forgetWithoutStubPastExpiry, R-N10), and its lookup is dropped
    */
  def expiryHeartbeat(): Unit =
    val now = wallClockMillis()
    expireStubs(now)
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
    // Its own acceptor state: a muted Tx holds none. Never an outcome: one it already knows is what its peers are asked
    // to confirm or end (ExpiryTally.decide's `known`)
    round.replies = Map(storeId -> Some(TxStatusResponse.TxStatus(TransactionStatus.Unresolved, finalized = false,
      late = false, holdsAcceptorState = !tx.muted)))
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
      case ExpiryTally.Decision.Hold => ()
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
      case ExpiryTally.Decision.Discard =>
        round.decided = true
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
    * A transaction this Frontend admitted (recovered ones included) always is, since dropping it would free nothing
    * and could strand it. Object data it brings that is not yet counted is charged without a limit check. A known
    * transaction always is too, as is one finalized in the status cache, which is answered from the cache and never
    * reaches the CRL. Any other is counted for its description, object data and CRL record, and admitted only while
    * the device's admission is below this store's tier's ceiling (CrlAdmission lanes: a metadata or bootstrap store
    * keeps room for the marker batches data transactions finalize with). The status cache is host-wide, so a transaction merely started on another
    * store is not known here.
    */
  def admitPrepare(m: TxPrepare): Boolean =
    val txid = m.txd.transactionId
    admitted.get(txid) match
      case Some(a) =>
        chargeData(a, m.objectUpdates)
        true
      case None if transactions.contains(txid) || statusCache.getStatus(txid).exists(_.finalized) => true
      // Checked before serializing: a prepare that will be dropped costs nothing more
      case None if admission.full(admissionTier) => false
      case None =>
        val stxd = m.txd.serialize()
        val bytes = CrlAdmission.TxOverheadBytes + stxd.size +
          m.objectUpdates.iterator.map(ou => CrlAdmission.objectUpdateBytes(ou.data.size)).sum
        if admission.tryAdmit(bytes, admissionTier) then
          admitted(txid) = new Admitted(bytes, m.objectUpdates.iterator.map(_.objectId).toSet, Some(stxd))
          true
        else
          false

  /** Charges `a` for the data of `updates` not counted yet, without a limit check */
  private def chargeData(a: Admitted, updates: List[ObjectUpdate]): Unit =
    val added = updates.filterNot(ou => a.dataCounted.contains(ou.objectId))
    if added.nonEmpty then
      val bytes = added.iterator.map(ou => CrlAdmission.objectUpdateBytes(ou.data.size)).sum
      admission.charge(bytes)
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
      releaseAdmission(tx.transactionId)

  /** Deletes a transaction's CRL entry and releases its admission */
  def deleteTransactionState(txid: TransactionId): Unit =
    crl.deleteTransaction(storeId, txid)
    releaseAdmission(txid)

  /** Releases everything this Frontend's transactions were admitted for: it is closing, or was built and never
    * served */
  def releaseAllAdmissions(): Unit =
    admitted.valuesIterator.foreach(a => admission.release(a.bytes))
    admitted.clear()
    stubReplacedAdmissions.valuesIterator.foreach(admission.release)
    stubReplacedAdmissions.clear()
    admission.release(stubBytesCharged)
    stubBytesCharged = 0L

  /** Closes the store. Its entries stay live in the CRL unless the closer takes them out, so what its transactions
    * were admitted for stays counted, retained for the store (CrlAdmission.retain) */
  def close(): Future[Unit] =
    failParkedRepairs()
    // A stub not yet durable has not replaced its transaction's state: that stays counted too
    admission.retain(storeId, admitted.valuesIterator.map(_.bytes).sum + stubReplacedAdmissions.valuesIterator.sum +
      stubBytesCharged)
    admitted.clear()
    stubReplacedAdmissions.clear()
    stubBytesCharged = 0L
    backend.close()

  def path: os.Path = backend.path

  /** Hands `os` to the backend and keeps it in memory until the backend reports the commit complete.
    * Applies a transaction's committed update; transactions and repairs on the object wait behind it
    * (txCommitsInFlight).
    */
  def commit(os: ObjectState, cs: CommitState, txid: TransactionId): Unit =
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
    backend.commit(cs, txid)

  /** A transaction was finalized. Its CRL state is deleted now, unless backend commits this store issued for it are
    * outstanding or failed: the store deletes a committed transaction's CRL state only once every backend commit it
    * issued for that transaction has succeeded (see TxCommits). A reloaded store recovers the rest from the CRL, and its
    * transaction driver concludes them. Aborted transactions, and those that committed nothing here, are deleted at once.
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
    if quarantineKeeps(txid) then
      // Kept until the quarantine clears (verificationComplete): a reloaded store recovers it and quarantines again
      quarantinedConclusions(txid) = quarantinedConclusions.get(txid).flatten.orElse(stub)
    else if stub.nonEmpty && !stubs.contains(txid) then
      recordAppliedStub(txid, stub.get, keepAdmission)(())
    else
      crl.deleteTransaction(storeId, txid)
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

  def failStopped: Boolean = stopped

  /** Fail-stops the store: it can no longer be trusted to hold what its votes promised. From now on it drops every
    * transaction message, read and repair, as an offline store would, and `onFailStop` tells its host, which marks it
    * rebuild-required and unloads it. Never for a condition that freezes writes, such as a full device: such a store
    * keeps serving reads (Backlog: "Full storage devices").
    *
    * The triggers (Tx.issueCommits, Tx.objectLoadFailed) are a commit voter that cannot hold the update it voted for:
    * its locks are gone, so without the update a conflicting transaction could count this store's commit vote against
    * the committed one (transaction-requirements F5, F6).
    */
  def failStop(reason: String): Unit =
    if !stopped then
      stopped = true
      failParkedRepairs()
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
    * forget floor, so the answer is permanent). Above the floor it could still create a Tx: no reply
    */
  private def receiveStatusRequestWithoutTx(m: TxStatusRequest): Unit =
    val reply = statusCache.getStatus(m.transactionId).filter(_.status != TransactionStatus.Unresolved) match
      case Some(e) => Some(TxStatusResponse.TxStatus(e.status, e.finalized, e.late, holdsAcceptorState = false))
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
      // created here could then apply T again once this store's applied stub has expired (Todo 4.6)
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
      logger.warn(s"Store $storeId takes no part in transaction ${m.txd.transactionId} yet: it started ${lead}ms " +
                  s"ahead of this host's clock (limit ${MaxStartLead.toMillis}ms). The client's clock runs fast, " +
                  s"or this host's runs slow")
    val trs = TransactionRecoveryState.initial(m.to, m.txd, m.objectUpdates, takeSerializedTxd(m.txd.transactionId))
    val pointers = m.txd.allHostedObjects(m.to)
    val tx = new Tx(trs, m.txd, this, net, crl, statusCache, pointers,
      muted = muted)
    transactions += (m.txd.transactionId -> tx)
    tx.receivePrepare(m)
    // The new Tx recorded itself as Unresolved, which the monotone cache does not let replace a known outcome
    known.foreach { _ =>
      // A muted Tx takes no part, a cached outcome included
      if !muted then
        tx.outcomeDecided()
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
  def readObjectForNetwork(clientId: ClientId, readUUID: UUID, pointer: ObjectPointer, readType: ReadType): Unit =
    if stopped then
      logger.trace(s"Store $storeId dropped read $readUUID: fail-stopped")
    else if holding then
      logger.trace(s"Store $storeId dropped read $readUUID: holding for recovered transactions")
    else
      readForNetwork(clientId, readUUID, pointer, readType)

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

        val nr = NetworkRead(clientId, readUUID, readType)

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
  def snapshotForRepair(pointer: ObjectPointer, p: Promise[RepairSnapshot]): Unit =
    val objectId = pointer.id
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
    * deletion (repair F6). While the backend removal is outstanding the object counts as deleting, as for a
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
      backend.repairDelete(objectId, storePointer, done)

  private def readObjectForTransaction(transaction: Tx, pointer: ObjectPointer): Unit =
    val objectId = pointer.id

    logger.trace(s"Loading object for Tx: ${transaction.transactionId}. Object: $objectId")

    // An object the transaction allocates is read like any other: the store may already hold it (a replayed
    // allocation, transaction-requirements F11), and the transaction must then see that state and refuse to overwrite
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
    // While holding for recovered transactions, their locks are not all taken: the store's own locks would not defer
    // what they cover
    val unsettled = holding || commitInFlight(objectId) || commitFailed(objectId) || committing.contains(objectId) ||
      deleting.contains(objectId) ||
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
    * of reconcile is made again when the parked repair runs
    */
  private def settledForRepair(objectId: ObjectId): Boolean =
    !(holding || commitInFlight(objectId) || commitFailed(objectId) || committing.contains(objectId) ||
      deleting.contains(objectId))

  /** Runs the repair parked on `objectId` once the object is settled. It runs once: still unsettled then (the store
    * serves another state, say), it answers stale
    */
  private def retryParkedRepair(objectId: ObjectId): Unit =
    if parkedRepairs.nonEmpty && settledForRepair(objectId) then
      parkedRepairs.remove(objectId).foreach: parked =>
        reconcileForRepair(parked.snapshot.copy(awaitSettle = false), parked.read, parked.p)

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
      p.success(plan.result)
    else
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
        post(() => repairWriteComplete(os, result.isSuccess))
        p.complete(result.map(_ => plan.result.copy(changed = true)))
      }(using scala.concurrent.ExecutionContext.parasitic)
      backend.repair(CommitState(read.pointer, os.metadata, os.objectType, os.data), done)

  /** A repair's backend write of `os` completed: its state is no longer held in memory for the write, and the
    * transactions waiting behind it may proceed.
    *
    * A failed write leaves memory ahead of the backend, so the state is only discarded: the next use reloads the
    * pre-repair state from the backend, and the repair's promise fails, so its entry is kept for a later pass. Unlike
    * a failed transaction commit it is not recorded in failedCommits, so later transactions on the object are not
    * refused: no transaction relied on the repaired state being durable
    */
  private def repairWriteComplete(os: ObjectState, succeeded: Boolean): Unit =
    val objectId = os.objectId
    committingComplete(os)
    release(os)
    if !succeeded then
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

            case netRead: NetworkRead =>
              logger.trace(s"Completed backend load for read ${netRead.requestUUID}. ERROR $err. Sending read response")
              val reply = if err == ReadError.ObjectNotFound && allocationPending(objectId) then
                ReadError.AllocationPending
              else
                err
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
