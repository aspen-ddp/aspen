package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID
import org.aspen_ddp.aspen.client.{DataObjectRangeState, DataObjectRangesState, DataObjectState, KeyValueObjectState, MetadataObjectState, ObjectState, ReadDriverClient, ReadError, ReadTimeout}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.{Read, ReadResponse}
import org.aspen_ddp.aspen.common.objects.{ByteRanges, DataObjectPointer, FullObject, KeyRange, KeySet, KeyValueObjectPointer, MetadataOnly, ObjectPointer, ReadError, ReadType}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import scribe.{Level, Logging}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, SECONDS}

abstract class BaseReadDriver(
                               val client: ReadDriverClient,
                               val objectPointer: ObjectPointer,
                               val ida: IDA,
                               val readUUID:UUID,
                               val comment: String,
                               val readType: ReadType = FullObject()
                    ) extends ReadDriver with Logging {

  logger.info(s"Read UUID $readUUID: Beginning read of ${objectPointer.objectType} object ${objectPointer.id}. Comment: $comment")

  /** Reads that return only part of the object (some keys, a key range, byte ranges) or none of its content
    * (metadata only) */
  private val isPartial: Boolean = readType match
    case _: KeySet | _: KeyRange | _: MetadataOnly | _: ByteRanges => true
    case _ => false

  // Detect invalid combinations
  val objectReader: ObjectReader = (objectPointer, readType) match {
    case (p: KeyValueObjectPointer, _:MetadataOnly) => new KeyValueObjectReader(true, p, ida, readUUID)
    case (p: KeyValueObjectPointer, _) => new KeyValueObjectReader(false, p, ida, readUUID, isPartial)

    case (p: DataObjectPointer,     _:MetadataOnly) => new DataObjectReader(true, p, ida, readUUID)
    case (p: DataObjectPointer,     _:FullObject) => new DataObjectReader(false, p, ida, readUUID)
    case (p: DataObjectPointer,     _: ByteRanges) => new DataObjectRangeReader(p, ida, readUUID, readType)
    case (_: DataObjectPointer,     rt) => throw new IllegalArgumentException(s"Read type $rt is not supported for DataObjects")

    case _ => throw new AssertionError("Invalid read combination")
  }

  protected var retryCount = 0

  private val startNanos = System.nanoTime()
  private var lastResponses: Map[StoreId, Either[ReadError.Value, ReadResponse.CurrentState]] = Map()
  private var committedLockRereads = 0
  private var committedLockStoresReported: Set[StoreId] = Set()

  /** Stores whose last reply went to the object reader holding locks of transactions not then known to have
    * committed. A read racing a commit gets such replies, and the reply showing the commit may be the next one */
  private var undecidedLockStores: Map[StoreId, Set[TransactionId]] = Map()

  /** Known from the transaction status cache, or from another store having applied it: stores apply only committed
    * transactions. Applied shows in another store's object revision or, for a blind KV insert (which leaves the
    * object's revision unchanged), in a key's revision. A store's own reply is not counted, so a reply cannot cause
    * its own re-read */
  private def holdsKnownCommittedLock(store: StoreId, locks: Set[TransactionId]): Boolean =
    knownCommitted(store, locks).nonEmpty

  /** The subset of the store's locks known to belong to committed transactions (see holdsKnownCommittedLock) */
  private def knownCommitted(store: StoreId, locks: Set[TransactionId]): Set[TransactionId] =
    if locks.isEmpty then
      Set()
    else
      val othersApplied = lastResponses.iterator.collect:
        case (s, Right(cs)) if s != store => cs.revision.lastUpdateTxUUID
      .toSet
      locks.filter: txid =>
        othersApplied.contains(txid.uuid) || objectReader.appliedElsewhere(store, txid.uuid) ||
          client.txStatusCache.getTransactionResolution(txid).exists(_._1)

  /** Re-reads, once per reply, every store whose locks have since been learned to belong to committed transactions.
    * Its reply is stale, and it was the only reply that store will send before the next full resend */
  private def rereadStoresWithNewlyCommittedLocks(): Unit =
    val ready = undecidedLockStores.collect:
      case (store, locks) if holdsKnownCommittedLock(store, locks) => store -> knownCommitted(store, locks)
    ready.foreach: (store, committed) =>
      undecidedLockStores -= store
      committedLockRereads += 1
      sendReadRequest(store, committed)

  /** A settled read (`settle`) hands the object reader no store's reply until that store holds none of the write locks
    * its first reply held: such a reply is re-read with `awaitCommit` naming them, so the store parks it until they
    * release. Locks a store takes after its first reply are not waited for, so a busy object cannot hold the read
    * forever. The wait is not bounded by the horizon, though: it lasts until each of the first reply's lock holders
    * resolves (commit, abort or expiry) at that store, which lasts as long as a store of that transaction is silent
    * (expiry rule G, server/transaction/CLAUDE.md). The wait is required: such a holder may have committed. A caller
    * that must not wait forever times the read out (AspenClient.settledReaderBy) and fails.
    *
    * Why this makes the result reflect every transaction T that committed before the read began: T committed means
    * `writeThreshold` stores of this object's pool voted commit, and a commit voter holds T's locks until it applies
    * T (`Tx.doCommit` releases them and applies in one step on the store's executor). So each of those voters' first
    * replies shows T applied or holds T's lock, and a voter's reply reaches the object reader only once T is applied.
    * Stores that neither applied T nor hold its lock number at most `width - writeThreshold`, below the read
    * threshold, so no revision preceding T can gather `consistentRestoreThreshold` matching replies
    */
  private var settling = false

  /** Settled reads: each store's locks still awaited (from its first reply). A store's entry is empty once settled */
  private var awaitedLocks: Map[StoreId, Set[TransactionId]] = Map()

  /** Settled reads: when each store's last awaiting re-read was sent (nanoTime), the backoff before its next one, and
    * the stores with a delayed re-read scheduled. A store at Frontend.MaxParkedReads answers an awaiting re-read at
    * once, still holding the locks; re-sending at once would ping-pong at network speed for as long as the holder
    * takes to resolve (indefinitely while a store of it is silent), on a store already overloaded */
  private var settleSentAt: Map[StoreId, Long] = Map()
  private var settleBackoff: Map[StoreId, FiniteDuration] = Map()
  private var settleDelayed: Set[StoreId] = Set()

  /** The clock settled re-reads are timed by; tests override it */
  protected def nanoTime(): Long = System.nanoTime()

  def settle(): Unit = synchronized { settling = true }

  /** Re-reads a settling store awaiting `locks`. A reply to an awaiting re-read that came back sooner than the
    * store's backoff (not parked: the store is at its cap) is re-read after the backoff, which then doubles up to
    * Frontend.ParkedReadWait. Any other reply (the first, one after a full resend, or a parked read answered at its
    * expiry) is re-read at once. While a delayed re-read is scheduled, no other is sent */
  private def rereadSettling(store: StoreId, locks: Set[TransactionId]): Unit =
    if !settleDelayed.contains(store) then
      val now = nanoTime()
      val backoff = settleBackoff.getOrElse(store, BaseReadDriver.SettleBackoffStart)
      if settleSentAt.get(store).exists(sent => now - sent < backoff.toNanos) then
        settleDelayed += store
        settleBackoff += store -> (backoff * 2).min(BaseReadDriver.SettleBackoffMax)
        client.backgroundTaskManager.schedule(backoff):
          synchronized:
            settleDelayed -= store
            val awaited = awaitedLocks.getOrElse(store, Set())
            if !promise.isCompleted && awaited.nonEmpty then
              settleSentAt += store -> nanoTime()
              sendReadRequest(store, awaited)
      else
        settleSentAt += store -> now
        sendReadRequest(store, locks)

  /** For a settled read: the awaited locks the store's reply still holds, recording its first reply's locks. Empty
    * once the store has settled: its later replies go to the object reader as in an ordinary read */
  private def unsettledLocks(store: StoreId, locks: Set[TransactionId]): Set[TransactionId] =
    if !settling then
      Set()
    else
      val pending = awaitedLocks.getOrElse(store, locks).intersect(locks)
      awaitedLocks += store -> pending
      pending

  private def elapsedMillis: Long = (System.nanoTime() - startNanos) / 1000000

  /** The last reply of each store: revision, timestamp and write locks, or the error */
  private def responseSummary: String =
    lastResponses.toList.sortBy(_._1.poolIndex).map {
      case (store, Left(err)) => s"${store.poolIndex}: $err"
      case (store, Right(cs)) =>
        s"${store.poolIndex}: ${cs.revision.lastUpdateTxUUID}@${cs.timestamp} " +
          s"locks=${BaseReadDriver.lockSummary(cs.lockedWriteTransactions)}"
    }.mkString("; ")

  /** A slow read: every store's last reply shows what the result was chosen from */
  private def reportIfSlow(outcome: String): Unit =
    if elapsedMillis >= BaseReadDriver.SlowReadMillis then
      logger.warn(s"Read UUID $readUUID of object ${objectPointer.id} took ${elapsedMillis}ms and $outcome. " +
        s"Retries: $retryCount. Re-reads for locks of committed transactions: $committedLockRereads. " +
        s"Last replies: $responseSummary")

  protected val promise: Promise[ObjectState] = Promise()

  def readResult: Future[ObjectState] = promise.future

  def begin(): Unit = sendReadRequests()

  def shutdown(): Unit = {}

  protected def onComplete(): Unit = ()

  def timeOut(): Unit = synchronized {
    if !promise.isCompleted then
      logger.info(s"Read UUID $readUUID: Timed out reading object ${objectPointer.id}")
      reportIfSlow("timed out")
      promise.failure(new ReadTimeout(objectPointer))
      onComplete()
  }

  /** Sends a Read request to the specified store. */
  protected def sendReadRequestNoLogMessage(dataStoreId: StoreId): Unit = {
    client.messenger.sendClientRequest(Read(dataStoreId, client.clientId, readUUID, objectPointer, readType))
  }

  /** Sends a Read request to the specified store. */
  protected def sendReadRequest(dataStoreId: StoreId, awaitCommit: Set[TransactionId] = Set()): Unit = {
    logger.trace(s"Read UUID $readUUID: Sending read request for object ${objectPointer.id} to store $dataStoreId. Comment: $comment")
    client.messenger.sendClientRequest(Read(dataStoreId, client.clientId, readUUID, objectPointer, readType, awaitCommit))
  }

  /** Sends a Read request to all stores that have not already responded. May be called outside a synchronized block */
  protected def sendReadRequests(): Unit = {
    logger.trace(s"Read UUID $readUUID: sending read requests to all stores for object ${objectPointer.id}. ${(0 until ida.width).map(i => StoreId(objectPointer.poolId, i.toByte)).toList}")
    synchronized {
      retryCount += 1
      // debugLogStatus restores the object and prints every key, so it must not run unless its output is wanted
      if (retryCount > 3 && logger.includes(Level.Trace)) {
        //logger.debug(s"RESENDING READ REQUESTS for Read UUID $readUUID")
        objectReader.debugLogStatus(s"RESENDING READ REQUESTS for Read UUID $readUUID",
          s => logger.trace(s))
      }
    }
    (0 until ida.width).foreach(i => sendReadRequest(StoreId(objectPointer.poolId, i.toByte)))
  }

  def receiveReadResponse(response:ReadResponse): Boolean = synchronized {
    logger.trace(s"Read UUID $readUUID: Received read response from store ${response.fromStore}")

    lastResponses += response.fromStore -> response.result

    undecidedLockStores -= response.fromStore

    val committedLocks = response.result match {
      case Left(_) => Set[TransactionId]()
      case Right(cs) => knownCommitted(response.fromStore, cs.lockedWriteTransactions)
    }
    val hasLocksForKnownCommittedTransactions = committedLocks.nonEmpty

    val settleLocks = response.result match
      case Left(_) => Set[TransactionId]()
      case Right(cs) => unsettledLocks(response.fromStore, cs.lockedWriteTransactions)

    val isUnknownStoreResponse = response.result match
      case Left(err) =>
        if err == ReadError.StoreNotFound then
          client.messenger.dropCacheForStore(response.fromStore)
          true
        else
          false
      case Right(_) => false

    // A commit voter that has not applied the object's allocation (transaction-requirements 9.4). Not an answer: the
    // allocation applies, or aborts and the store then answers ObjectNotFound. A threshold of such replies must not
    // end the read, or a client told its allocation committed (from the stores' votes, before any applied it) could
    // not read its own new object. The store parks the re-read until the allocation applies (Frontend.parkAllocationRead)
    val isAllocationPending = response.result == Left(ReadError.AllocationPending)

    if (isUnknownStoreResponse || isAllocationPending || hasLocksForKnownCommittedTransactions || settleLocks.nonEmpty) {
      // We know for certain that the response from this store has out-of-date information, or a settled read awaits
      // the locks its first reply held. Transactions _should_ resolve quickly so we re-read from the store at once (a
      // settled re-read the store answered without parking backs off first: rereadSettling)
      if hasLocksForKnownCommittedTransactions then
        committedLockRereads += 1
        if elapsedMillis >= BaseReadDriver.SlowReadMillis && !committedLockStoresReported.contains(response.fromStore) then
          committedLockStoresReported += response.fromStore
          logger.warn(s"Read UUID $readUUID of object ${objectPointer.id}: store ${response.fromStore} still holds a " +
            s"lock of a committed transaction after ${elapsedMillis}ms. Replies so far: $responseSummary")
      if settleLocks.nonEmpty && !hasLocksForKnownCommittedTransactions then
        rereadSettling(response.fromStore, settleLocks)
      else
        sendReadRequest(response.fromStore, committedLocks ++ settleLocks)
    }
    else {
      if settling then
        settleBackoff -= response.fromStore
        settleSentAt -= response.fromStore
      response.result.foreach: cs =>
        if cs.lockedWriteTransactions.nonEmpty then
          undecidedLockStores += response.fromStore -> cs.lockedWriteTransactions

      objectReader.receiveReadResponse(response).foreach { e =>
        if (!promise.isCompleted) {
          val result = e match {
            case Left(err) => Left(err)
            case Right(os) =>

              // Ensure any commit transactions will use timestamps after all read objects last update time
              os match {
                case dos: DataObjectState => HLCTimestamp.update(dos.timestamp)
                case rs: DataObjectRangeState => HLCTimestamp.update(rs.timestamp)
                case rs: DataObjectRangesState => HLCTimestamp.update(rs.timestamp)
                case kvos: KeyValueObjectState => HLCTimestamp.update(kvos.lastUpdateTimestamp)
                case mos: MetadataObjectState => HLCTimestamp.update(mos.timestamp)
              }

              Right(os)
          }

          result match {
            case Left(err) =>
              logger.info(s"Read UUID $readUUID: Failed to read object ${objectPointer.id}. Reason: $err")
              reportIfSlow(s"failed: $err")
              promise.failure(err)
              onComplete()

            case Right(obj) =>
              obj match {
                case rs: DataObjectRangeState => logger.info(s"Read UUID $readUUID: Successfully read DataObject range of ${objectPointer.id} Rev ${rs.revision} Ref ${rs.refcount} Offset ${rs.offset} Size ${rs.data.size}")
                case rs: DataObjectRangesState => logger.info(s"Read UUID $readUUID: Successfully read DataObject ranges of ${objectPointer.id} Rev ${rs.revision} Ref ${rs.refcount} Ranges ${rs.ranges.size}")
                case dos: DataObjectState => logger.info(s"Read UUID $readUUID: Successfully read DataObject ${objectPointer.id} Rev ${dos.revision} Ref ${dos.refcount} Size ${dos.data.size} Hash ${dos.data.hashString}")
                case kvos: KeyValueObjectState => logger.info(s"Read UUID $readUUID: Successfully read KeyValueObject ${objectPointer.id} Rev ${kvos.revision} Ref ${kvos.refcount} Num Entries ${kvos.contents.size}")
                case mos: MetadataObjectState => logger.info(s"Read UUID $readUUID: Successfully read MetadataObject ${objectPointer.id} Rev ${mos.revision} Ref ${mos.refcount}")
              }
              reportIfSlow(s"returned revision ${obj.revision.lastUpdateTxUUID}")
              promise.success(obj)
              onComplete()
          }
        }
      }
    }
    if !promise.isCompleted && undecidedLockStores.nonEmpty then
      rereadStoresWithNewlyCommittedLocks()
    objectReader.receivedResponsesFromAllStores
  }


}

object BaseReadDriver:

  /** noErrorRecoveryReadDriver: how often an incomplete read is re-sent, when it is reported hung, and how many
    * resends it makes before giving up (a test that has failed must not keep posting reads) */
  val ResendEvery: FiniteDuration = FiniteDuration(100, MILLISECONDS)
  val HungAfter: FiniteDuration = FiniteDuration(2, SECONDS)
  val MaxResends: Int = 600

  /** Settled reads: the first and largest backoff before re-reading a store that answered an awaiting re-read without
    * parking it. The largest is how long a store parks a read, so a store that cannot park is asked no more often than
    * one that can */
  val SettleBackoffStart: FiniteDuration = FiniteDuration(10, MILLISECONDS)
  val SettleBackoffMax: FiniteDuration = org.aspen_ddp.aspen.server.store.Frontend.ParkedReadWait

  /** Reads slower than this are logged with every store's last reply */
  val SlowReadMillis: Long = 2000

  /** An object under load can hold thousands of locks, and every client thread waits while a line is written */
  val MaxLocksReported: Int = 3

  private[read] def lockSummary(locks: Set[TransactionId]): String =
    val shown = locks.iterator.take(MaxLocksReported).map(_.uuid).mkString(",")
    if locks.size > MaxLocksReported then
      s"${locks.size}[$shown,...]"
    else
      s"${locks.size}[$shown]"

  def noErrorRecoveryReadDriver(
                                 client: ReadDriverClient,
                                 objectPointer: ObjectPointer,
                                 ida: IDA,
                                 readUUID:UUID,
                                 comment: String,
                                 readType: ReadType): ReadDriver =

    new BaseReadDriver(client, objectPointer, ida, readUUID, comment, readType = readType):

      given ec: ExecutionContext = this.client.clientContext

      val test = this.client.getSystemAttribute("unittest.name").getOrElse("UNKNOWN TEST")

      // Re-sends the read every ResendEvery until it completes, as SimpleReadDriver does with backoff. Replies are
      // only a snapshot: a read racing a commit can get replies that cannot be restored, and the immediate re-read
      // covers only stores holding a lock known to have committed. Resent on the client's context, so a lockstep
      // network is still driven from the test's thread. A store that stays behind still hangs the read: resending
      // never hides a missed update
      val myRunnable: Runnable = () =>
        var count = 0
        while ! promise.isCompleted && count < MaxResends do
          Thread.sleep(ResendEvery.toMillis)
          count += 1
          if !promise.isCompleted then
            ec.execute(() => if !promise.isCompleted then sendReadRequests())
          if count * ResendEvery.toMillis == HungAfter.toMillis then
            objectReader.debugLogStatus(s"*** HUNG READ in test $test", s => logger.warn(s))

      val checkerThread = new Thread(myRunnable, "waitForReadCompleteThread")

      checkerThread.start()


