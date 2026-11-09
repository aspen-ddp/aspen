package org.aspen_ddp.aspen.server.store

import java.util.UUID
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, OpportunisticRebuild, ReadResponse, TxAccept, TxFinalized, TxHeartbeat, TxMessage, TxPrepare, TxResolved, TxStatusRequest}
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, ReadError, ReadType}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, RefcountUpdate, TransactionDescription, TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlAdmission, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendTiming, Commit, CommitState, Completion, Read}
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionStatusCache, Tx}
import scribe.Logging
import org.aspen_ddp.aspen.client.{ReadLocks, ObjectState as ClientObjectState}

import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}


object Frontend {

  /** How long a store withholds its vote from drivers' prepares while waiting for a client's object data.
    * Several client retransmissions long, since the only cost of a late abort is a client retry.
    */
  val MissingDataWait: Duration = Duration(5, SECONDS)

  /** How long a TracelessCommits lookup may go unanswered before the store heartbeat makes another. An answer can be
    * lost: one completing before its store is installed in the StoreRouter is dropped
    */
  val TracelessLookupTimeout: FiniteDuration = FiniteDuration(5, SECONDS)

  sealed abstract class ReadKind

  case class NetworkRead(clientId: ClientId, requestUUID: UUID, readType: ReadType) extends ReadKind

  case class TransactionRead(transactionId: TransactionId) extends ReadKind

  case class OpportuneRebuild(or: OpportunisticRebuild) extends ReadKind

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
               /** Looks up recovered commit votes in the pool's TracelessCommits tree: see requestTracelessLookup */
               val tracelessLookup: TracelessCommitLookup = TracelessCommitLookup.Never,
               /** Runs a task on this store's executor: how completions arriving on other threads, such as a repair's
                 * backend write, get back to the Frontend's state. Inline by default, for tests with synchronous
                 * backends
                 */
               val post: (() => Unit) => Unit = task => task()) extends Logging {

  import Frontend._

  /** Written only on the store's executor. `@volatile` so that `mayHoldTransaction` may read one snapshot of it from
    * another thread (the repair scan), as Store.hasTransactions reads `transactionDrivers`
    */
  @volatile var transactions: Map[TransactionId, Tx] = Map()

  /** Transactions waiting for a lock holder, by holder. See Tx.awaitableHolders */
  private val collisionWaiters = scala.collection.mutable.HashMap[TransactionId, List[Tx]]()

  def holdsLocks(txid: TransactionId): Boolean = transactions.get(txid).exists(_.holdsLocks)

  /** Callable from any thread: whether the store holds `txid` live (recovered or active), or may once the CRL state it
    * is loading is recovered. Reads one snapshot of each `@volatile` field and touches no other state
    */
  def mayHoldTransaction(txid: TransactionId): Boolean = awaitingCrl || transactions.contains(txid)

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
    * `forTransaction = true`, and repair writes (see reconcile). Rebuild commits are not counted
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
      heldCommits.get(oid) match
        case Some(next :: _) => next.commitPersisted(oid)
        case _ => commitWaiters.remove(oid).foreach(_.foreach(_.commitPersisted(oid)))

  /** Outstanding backend commits, oldest first. Each holds a reference to its state (see retain) from commit until
    * the backend reports it complete, so the state is served and checked from memory rather than read from a backend
    * that may not have it yet. `start` times the commit as the store sees it: BackendTiming
    */
  private case class InFlight(os: ObjectState, start: Long, deletes: Boolean, forTransaction: Boolean)
  private val inFlight = scala.collection.mutable.HashMap[(ObjectId, TransactionId), List[InFlight]]()

  /** Outstanding backend commits deleting each object. Reads that would reach the backend get the answer it will give
    * once the deletion is applied
    */
  private val deleting = scala.collection.mutable.HashMap[ObjectId, Int]()

  /** The states of outstanding non-deletion backend commits of each object, newest first. A backend read completing
    * meanwhile may return the state such a commit replaces, so it is never cached. See backendReadComplete
    */
  private val committing = scala.collection.mutable.HashMap[ObjectId, List[ObjectState]]()
  private val commitStage = s"store commit (${backend.getClass.getSimpleName})"

  /** A transaction's backend commits on this store, made through commit with `forTransaction = true`.
    *
    * The store deletes a committed transaction's CRL state only once every backend commit it issued for that
    * transaction has succeeded. Until then the CRL entry, object data included, outlives the Tx, which is forgotten at
    * finalization. A failed commit leaves the record, and so the CRL entry, for the life of the loaded store: a reloaded
    * store recovers the transaction from it, and its transaction driver concludes it.
    *
    * @param outstanding commits not yet reported complete by the backend
    * @param failed some commit failed
    * @param finalized TxFinalized arrived while commits were outstanding: the CRL deletion waits for them
    */
  private final class TxCommits(var outstanding: Int, var failed: Boolean, var finalized: Boolean)
  private val txCommits = scala.collection.mutable.HashMap[TransactionId, TxCommits]()

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

  /** The store's CRL state is still being loaded (from a crl_save.log). See awaitRecovery. `@volatile` for
    * mayHoldTransaction
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

  /** A recovered commit vote's TracelessCommits lookups (see Tx.needsTracelessCheck). Each lookup has its own request
    * id, from a counter of this Frontend's lookups, and its answer names it.
    *
    * @param latest the id of the latest lookup made
    * @param inFlight the latest lookup has not been answered
    * @param startedAt when the latest lookup was made (System.nanoTime)
    * @param firstCounting the first id whose answer finding no entry counts: lookups made before the latest abort
    *                      arrived do not, since the entry may have been written after they read the tree
    * @param failed the latest lookup failed: retryTracelessLookups makes another
    * @param failures lookups failed in a row, for logging
    */
  private final class TracelessLookup(var latest: Long, var inFlight: Boolean, var startedAt: Long,
                                      var firstCounting: Long, var failed: Boolean, var failures: Int)
  // Before recoverTransactions: a recovered transaction that loads at once requests its lookup inside it
  private val tracelessLookups = scala.collection.mutable.HashMap[TransactionId, TracelessLookup]()
  private var nextTracelessRequest: Long = 0L

  recoverTransactions()

  /** Recreates the transactions the CRL holds for this store that are not already known.
    *
    * Transaction messages are held until every recovered transaction has loaded its objects, and with them taken the
    * locks of its recovered commit vote. A transaction started meanwhile could otherwise load a shared object first and
    * take conflicting locks.
    */
  private def recoverTransactions(): Unit =
    // Every recovered transaction is registered before any of their reads starts: one that loads synchronously (its
    // objects cached, or all allocated) must not find the others missing and release the held messages early
    val recovered = crl.getFullRecoveryState(storeId).flatMap { trs =>
      val txd = TransactionDescription.deserialize(trs.serializedTxd)
      if transactions.contains(txd.transactionId) then
        None
      else
        val pointers = txd.allHostedObjects(storeId)
        admitRecovered(txd.transactionId, trs)
        val tx = new Tx(trs, txd, this, net, crl, statusCache, Nil, pointers, recovered = true)
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

  /** The store's CRL state is being loaded after this Frontend was created (from a crl_save.log). Until recover is
    * called, transaction messages are held: started without their recovery state, transactions would hold no locks
    * and a prepare would replace the recovered Paxos acceptor state with an initial one.
    */
  def awaitRecovery(): Unit = awaitingCrl = true

  /** The CRL now holds the store's recovery state: recreates its transactions. Held messages are handled, in arrival
    * order, once those transactions have loaded their objects
    */
  def recover(): Unit =
    awaitingCrl = false
    recoverTransactions()
    releaseHeldMessages()

  // ---- TracelessCommits lookups of recovered commit votes (Tx.needsTracelessCheck) ----

  /** `tx` needs a TracelessCommits lookup started now: it has loaded, or an abort arrived. Only lookups made from now
    * on can show the entry absent. One already in flight is not repeated: if it finds no entry, another is made
    */
  def requestTracelessLookup(tx: Tx): Unit =
    tracelessLookups.get(tx.transactionId) match
      case Some(l) =>
        l.firstCounting = nextTracelessRequest
        if !l.inFlight then
          startTracelessLookup(tx.transactionId, l)
      case None =>
        val l = new TracelessLookup(-1L, false, 0L, nextTracelessRequest, false, 0)
        tracelessLookups(tx.transactionId) = l
        startTracelessLookup(tx.transactionId, l)

  private def startTracelessLookup(txid: TransactionId, l: TracelessLookup): Unit =
    val requestId = nextTracelessRequest
    nextTracelessRequest += 1
    l.latest = requestId
    l.inFlight = true
    l.startedAt = System.nanoTime()
    l.failed = false
    tracelessLookup.lookup(storeId, txid, requestId)

  /** The answer to TracelessCommits lookup `requestId`, delivered on the store's executor.
    *
    * An entry found concludes the Tx as committed, whichever lookup found it. None found releases the aborts it held,
    * if the lookup was made after the latest of them arrived; otherwise, if it was the latest lookup, another is made.
    * A failure of the latest lookup keeps the aborts held, and retryTracelessLookups tries again. Ignored once the Tx
    * no longer needs it (it learned its outcome another way), and for a lookup this Frontend did not make
    */
  def tracelessLookupComplete(txid: TransactionId, requestId: Long, result: Try[Boolean]): Unit =
    tracelessLookups.get(txid).filter(_.latest >= requestId).foreach: l =>
      transactions.get(txid).filter(_.needsTracelessCheck) match
        case None => tracelessLookups.remove(txid)
        case Some(tx) =>
          val isLatest = requestId == l.latest
          if isLatest then
            l.inFlight = false
          result match
            case Success(true) =>
              tracelessLookups.remove(txid)
              tx.tracelessCommitFound()
              tx.releaseObjects() // Already done unless some of its objects had not loaded
              transactions -= txid
            case Success(false) if requestId >= l.firstCounting =>
              tracelessLookups.remove(txid)
              tx.tracelessCommitAbsent().foreach(handleTransactionMessage)
            case Success(false) =>
              if isLatest then
                startTracelessLookup(txid, l)
            case Failure(err) =>
              if isLatest then
                l.failed = true
                l.failures += 1
                if l.failures == 1 || l.failures % 60 == 0 then
                  logger.warn(s"Store $storeId: TracelessCommits lookup of recovered transaction $txid failed " +
                    s"(${l.failures} in a row), retrying at heartbeat: $err")

  /** Store heartbeat: makes another lookup for each one that failed, and for each unanswered after
    * `Frontend.TracelessLookupTimeout`, whose answer may have been lost
    */
  def retryTracelessLookups(now: Long = System.nanoTime()): Unit =
    tracelessLookups.toList.foreach: (txid, l) =>
      val timedOut = l.inFlight && now - l.startedAt >= TracelessLookupTimeout.toNanos
      if l.failed || timedOut then
        if transactions.get(txid).exists(_.needsTracelessCheck) then
          if timedOut then
            logger.warn(s"Store $storeId: TracelessCommits lookup of recovered transaction $txid went unanswered " +
              s"for $TracelessLookupTimeout, looking up again")
          startTracelessLookup(txid, l)
        else
          tracelessLookups.remove(txid)

  private def releaseHeldMessages(): Unit =
    if !holding then
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
    * the device's admission has room. The status cache is host-wide, so a transaction merely started on another
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
      case None if admission.full => false
      case None =>
        val stxd = m.txd.serialize()
        val bytes = CrlAdmission.TxOverheadBytes + stxd.size +
          m.objectUpdates.iterator.map(ou => CrlAdmission.objectUpdateBytes(ou.data.size)).sum
        if admission.tryAdmit(bytes) then
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
        val a = new Admitted(CrlAdmission.TxOverheadBytes + trs.serializedTxd.size, Set(), None)
        admission.charge(a.bytes)
        admitted(txid) = a
        chargeData(a, trs.objectUpdates)

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

  /** Deletes a transaction's CRL entry and releases its admission */
  def deleteTransactionState(txid: TransactionId): Unit =
    crl.deleteTransaction(storeId, txid)
    releaseAdmission(txid)

  /** Releases everything this Frontend's transactions were admitted for: it is closing, or was built and never
    * served */
  def releaseAllAdmissions(): Unit =
    admitted.valuesIterator.foreach(a => admission.release(a.bytes))
    admitted.clear()

  /** Closes the store. Its entries stay live in the CRL unless the closer takes them out, so what its transactions
    * were admitted for stays counted, retained for the store (CrlAdmission.retain) */
  def close(): Future[Unit] =
    admission.retain(storeId, admitted.valuesIterator.map(_.bytes).sum)
    admitted.clear()
    backend.close()

  def path: os.Path = backend.path

  /** Hands `os` to the backend and keeps it in memory until the backend reports the commit complete.
    *
    * @param forTransaction the commit applies a transaction's committed update, rather than a rebuild
    */
  def commit(os: ObjectState, cs: CommitState, txid: TransactionId, forTransaction: Boolean = false): Unit =
    val deletes = os.metadata.refcount.count == 0
    if deletes then
      // Deleted: the state is no longer served, nor checked by transactions that still reference it
      discard(os.objectId)
    else
      objectCache.insert(os)
    // A discarded state whose locks are still held stays the one transactions see (see referenced). A newer state
    // committed meanwhile, as by an opportunistic rebuild of a fresh load, is served through the cache until then
    val lockedOther = referenced.get(os.objectId).exists(r => !(r eq os) && r.reloadRequired && r.lockHolders.nonEmpty)
    retain(os, shared = !lockedOther)
    val key = (cs.objectId, txid)
    inFlight(key) = inFlight.getOrElse(key, Nil) :+ InFlight(os, System.nanoTime(), deletes, forTransaction)
    if deletes then
      deleting(os.objectId) = deleting.getOrElse(os.objectId, 0) + 1
    else
      committing(os.objectId) = os :: committing.getOrElse(os.objectId, Nil)
    // Before the backend commit: a synchronous backend completes inside it
    if forTransaction then
      txCommits.getOrElseUpdate(txid, new TxCommits(0, false, false)).outstanding += 1
      txCommitsInFlight(os.objectId) = txCommitsInFlight.getOrElse(os.objectId, 0) + 1
    backend.commit(cs, txid)

  /** A transaction was finalized. Its CRL state is deleted now, unless backend commits this store issued for it are
    * outstanding or failed: the store deletes a committed transaction's CRL state only once every backend commit it
    * issued for that transaction has succeeded (see TxCommits). A reloaded store recovers the rest from the CRL, and its
    * transaction driver concludes them. Aborted transactions, and those that committed nothing here, are deleted at once.
    */
  def transactionFinalized(txid: TransactionId): Unit =
    txCommits.get(txid) match
      case Some(r) => r.finalized = true
      case None => deleteTransactionState(txid)

  private def transactionCommitComplete(txid: TransactionId, succeeded: Boolean): Unit =
    txCommits.get(txid).foreach { r =>
      r.outstanding -= 1
      if !succeeded then
        r.failed = true
      if r.outstanding == 0 && !r.failed then
        txCommits.remove(txid)
        if r.finalized then
          deleteTransactionState(txid)
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

  def receiveTransactionMessage(msg: TxMessage): Unit =
    if holding || heldMessages.nonEmpty then
      heldMessages = msg :: heldMessages
    else
      handleTransactionMessage(msg)

  private def handleTransactionMessage(msg: TxMessage): Unit = msg match {
    case m: TxPrepare => receivePrepare(m)
    case m: TxAccept => transactions.get(m.transactionId).foreach(tx => tx.receiveAccept(m))
    case m: TxResolved =>
      transactions.get(m.transactionId) match
        case Some(tx) => tx.receiveResolved(m)
        // Remembered for a prepare that arrives after this. See receivePrepare
        case None =>
          if !statusCache.getStatus(m.transactionId).exists(_.finalized) then
            statusCache.updateStatus(m.transactionId, statusOf(m.committed))
    case m: TxFinalized =>
      transactions.get(m.transactionId) match
        case Some(tx) =>
          // Not if the Tx holds the abort for a TracelessCommits lookup
          if tx.receiveFinalized(m) then
            tx.releaseObjects() // Already done unless some of its objects had not loaded
            transactions -= m.transactionId
        // Remembered for a prepare that arrives after this. See receivePrepare
        case None => statusCache.updateStatus(m.transactionId, statusOf(m.committed), finalized = true)
    case m: TxHeartbeat => transactions.get(m.transactionId).foreach(tx => tx.receiveHeartbeat(m))
    case m: TxStatusRequest => transactions.get(m.transactionId).foreach(tx => tx.receiveStatusRequest(m))
    case _ => // Other TxMessages are not relevant
  }

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
      val known = statusCache.getStatus(m.txd.transactionId).filter(_.status != TransactionStatus.Unresolved)
      val trs = TransactionRecoveryState.initial(m.to, m.txd, m.objectUpdates, takeSerializedTxd(m.txd.transactionId))
      val pointers = m.txd.allHostedObjects(m.to)
      val tx = new Tx(trs, m.txd, this, net, crl, statusCache, m.preTxRebuilds, pointers)
      transactions += (m.txd.transactionId -> tx)
      tx.receivePrepare(m)
      known.foreach { entry =>
        tx.outcomeDecided()
        statusCache.updateStatus(m.txd.transactionId, entry.status) // The new Tx recorded itself as Unresolved
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
      // A rebuild commit can share its (object, transaction) key with a transaction's commit of the object. Completions
      // are matched to the oldest record for the key, which assumes the backend completes one key's commits in the
      // order it received them. Otherwise the transaction's commit could be counted persisted when the rebuild's
      // completes, letting its followers through early
      inFlight.get(key) match
        case Some(f :: rest) =>
          if rest.isEmpty then inFlight.remove(key) else inFlight(key) = rest
          BackendTiming.report.record(commitStage, System.nanoTime() - f.start)
          if f.forTransaction then
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

  def readObjectForNetwork(clientId: ClientId, readUUID: UUID, pointer: ObjectPointer, readType: ReadType): Unit = {
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

  def readObjectForOpportunisticRebuild(op: OpportunisticRebuild): Unit = {
    val objectId = op.pointer.id
    servableState(objectId) match {
      case Some(os) => opportunisticRebuild(op, os)
      case None if deleting.contains(objectId) => // Being deleted: nothing to rebuild
      case None =>
        if op.pointer.poolId == storeId.poolId then
          pendingReads.get(objectId) match {
            case Some(lst) =>
              pendingReads += (objectId -> (OpportuneRebuild(op) :: lst))

            case None =>
              pendingReads += (objectId -> (OpportuneRebuild(op) :: Nil))
              backend.read(op.pointer)
          }
    }
  }

  /** Records the store's copy of the object for a repair (Reconcile): first step, before the repair reads it. An
    * object the store does not hold, or is deleting, gives an empty snapshot
    */
  def snapshotForRepair(pointer: ObjectPointer, p: Promise[RepairSnapshot]): Unit =
    val objectId = pointer.id
    servableState(objectId) match
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
    servableState(objectId) match
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
    * There is nothing to read first: the object is gone everywhere else, so there is no state
    * to compare against. We evict the cache for the same reason `commit` does when a refcount
    * reaches zero, then hand the removal to the backend.
    */
  def deleteObjectForRepair(objectId: ObjectId,
                            storePointer: Array[Byte],
                            completion: Promise[Unit]): Unit =
    logger.trace(s"Repairing missed deletion of object $objectId")
    discard(objectId)
    backend.repairDelete(objectId, storePointer, completion)

  private def readObjectForTransaction(transaction: Tx, pointer: ObjectPointer): Unit =
    val objectId = pointer.id

    logger.trace(s"Loading object for Tx: ${transaction.transactionId}. Object: $objectId")

    if transaction.txd.allocatingObjects.contains(pointer.id) then
      logger.trace(s"Allocating new object: $objectId in Tx: ${transaction.transactionId}.")

      val metadata = Metadata(
        ObjectRevision.Allocating,
        ObjectRefcount.Allocating,
        transaction.txd.startTimestamp
      )

      val os = new ObjectState(objectId, metadata, pointer.objectType, DataBuffer.Empty)

      transaction.objectLoaded(os)
    else
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

  private def opportunisticRebuild(op: OpportunisticRebuild, os: ObjectState): Unit = {
    // Backend writes of one object are unordered: skipped while another is outstanding
    if (os.metadata.revision == op.revision && !commitInFlight(op.pointer.id)) {
      val rc = if (op.refcount.updateSerial > os.metadata.refcount.updateSerial)
        op.refcount
      else
        os.metadata.refcount

      os.metadata = Metadata(op.revision, rc, op.timestamp)
      os.data = op.data
      val cs = CommitState(op.pointer, os.metadata, os.objectType, os.data)
      val txid = TransactionId(op.revision.lastUpdateTxUUID)
      // No need to wait for this to complete
      commit(os, cs, txid)
    }
  }

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
    val unsettled = commitInFlight(objectId) || commitFailed(objectId) || committing.contains(objectId) ||
      deleting.contains(objectId) ||
      (loaded match
        // Another in-memory state is the one served or locked: the loaded copy may be stale. With no cache, an
        // uncached load is the store's state
        case Some(os) => referenced.get(objectId).exists(r => !(r eq os)) || served.exists(c => !(c eq os))
        case None => served.nonEmpty)
    if unsettled then
      p.success(ReconcileResult(Set(), false, stale = true))
    else
      reconcileSettled(snapshot, read, p, loaded)

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
        p.complete(result.map(_ => plan.result))
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

            case OpportuneRebuild(op) =>
              logger.trace(s"Completed read for OpportuneRebuild. Object: ${os.objectId}")
              opportunisticRebuild(op, os)

            case SnapshotRead(_, p) => p.success(RepairSnapshot.of(os))

            case ReconcileRead(snapshot, read, p) =>
              logger.trace(s"Completed read for repair. Object: ${os.objectId}")
              reconcile(snapshot, read, p, Some(os))
          }
        }

      case Right(err) =>
        waiting.foreach { lpr =>
          lpr.foreach {
            case netRead: NetworkRead =>
              logger.trace(s"Completed backend load for read ${netRead.requestUUID}. ERROR $err. Sending read response")
              val rr = ReadResponse(netRead.clientId, storeId, netRead.requestUUID, HLCTimestamp.now, Left(err) )
              net.sendClientResponse(rr)

            case tr: TransactionRead => transactions.get(tr.transactionId).foreach { tx => tx.objectLoadFailed(objectId, err) }

            case _: OpportuneRebuild => // Can't guarantee correctness so we need to ignore this

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
