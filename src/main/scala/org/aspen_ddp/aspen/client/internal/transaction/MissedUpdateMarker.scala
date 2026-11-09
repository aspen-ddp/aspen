package org.aspen_ddp.aspen.client.internal.transaction

import java.nio.{ByteBuffer, ByteOrder}

import org.aspen_ddp.aspen.client.{AspenClient, StopRetrying, Transaction, TransactionCreationError, UnknownStoragePool}
import org.aspen_ddp.aspen.client.tkvl.TKVLInsert
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import scribe.Logging

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Try}

object MissedUpdateMarker:
  val DefaultMaxEntriesPerBatch = 256

  /** A store missed `missedTimestamp`'s update of an object */
  final case class MissedUpdate(store: StoreId,
                                objectId: ObjectId,
                                missedTimestamp: HLCTimestamp,
                                storePointer: Array[Byte])

  /** A store missed a transaction's traceless change to some of its objects */
  final case class MissedTracelessCommit(store: StoreId, transactionId: TransactionId, entry: TracelessCommitEntry)

  private[transaction] def errorKey(store: StoreId, objectId: ObjectId): Key =
    val keyBytes = new Array[Byte](17)
    val bb = ByteBuffer.wrap(keyBytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(store.poolIndex)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)
    Key(keyBytes)

/** Records missed updates in their pools' ErrorTrees, and missed traceless commits in their TracelessCommits trees,
  * through one write at a time per pool. A write covers both trees in one transaction and holds at most
  * `maxEntriesPerBatch` entries of the two together.
  *
  * A slow commit misses every object on every store at once, and all of a pool's entries go to its trees. Writing them
  * separately floods those trees' objects with reads and colliding transactions faster than they can complete, which
  * makes further commits slower. Entries that arrive during a write wait for the next one, so the writes never overlap
  * and a backlog makes each one larger (up to the cap) rather than adding more of them.
  */
class MissedUpdateMarker(client: AspenClient,
                         maxEntriesPerBatch: Int = MissedUpdateMarker.DefaultMaxEntriesPerBatch) extends Logging:
  import MissedUpdateMarker.*

  // Room for one entry of each kind, so neither can starve the other
  require(maxEntriesPerBatch >= 2, s"maxEntriesPerBatch must be at least 2: $maxEntriesPerBatch")

  private given ExecutionContext = client.clientContext

  private final class Queued(var update: MissedUpdate):
    val written: Promise[Unit] = Promise()

  private final class QueuedTraceless(val commit: MissedTracelessCommit):
    val written: Promise[Unit] = Promise()

  private final class Batch(val updates: List[Queued], val traceless: List[QueuedTraceless])

  private final class PoolQueue:
    val tracelessPending: mutable.LinkedHashMap[(Byte, TransactionId), QueuedTraceless] = mutable.LinkedHashMap()
    val pending: mutable.LinkedHashMap[(Byte, ObjectId), Queued] = mutable.LinkedHashMap()
    var writing = false

  private val queues: mutable.Map[PoolId, PoolQueue] = mutable.Map()

  /** Completes once all of `updates` and `traceless` are recorded, or fails if one of their pools' writes gives up */
  def mark(updates: List[MissedUpdate], traceless: List[MissedTracelessCommit] = Nil): Future[Unit] =
    val (written, batches) = synchronized:
      val written = updates.map(enqueue) ++ traceless.map(enqueueTraceless)
      val pools = (updates.map(_.store.poolId) ++ traceless.map(_.store.poolId)).distinct
      (written, pools.flatMap(p => nextBatch(p, wasWriting = false).map(p -> _)))

    batches.foreach((poolId, batch) => write(poolId, batch))
    Future.sequence(written).map(_ => ())

  /** The entries waiting for a write */
  def queued: Int = synchronized(queues.valuesIterator.map(q => q.pending.size + q.tracelessPending.size).sum)

  // An entry can only say one thing, so a store missing an object repeatedly needs one write of the newest miss:
  // a repair that catches the store up to it is past the older ones too
  private def enqueue(update: MissedUpdate): Future[Unit] =
    val queue = queues.getOrElseUpdate(update.store.poolId, new PoolQueue)
    val key = (update.store.poolIndex, update.objectId)
    queue.pending.get(key) match
      case Some(existing) =>
        if update.missedTimestamp > existing.update.missedTimestamp then
          existing.update = update
        existing.written.future
      case None =>
        val queued = new Queued(update)
        queue.pending(key) = queued
        queued.written.future

  // A transaction's entry for a store is written once, so a repeat is the same entry
  private def enqueueTraceless(commit: MissedTracelessCommit): Future[Unit] =
    val queue = queues.getOrElseUpdate(commit.store.poolId, new PoolQueue)
    queue.tracelessPending.getOrElseUpdate((commit.store.poolIndex, commit.transactionId), new QueuedTraceless(commit)).written.future

  /** Must hold the lock. `wasWriting` says that the caller's write has just finished */
  private def nextBatch(poolId: PoolId, wasWriting: Boolean): Option[Batch] =
    val queue = queues(poolId)
    if wasWriting then
      queue.writing = false

    if queue.writing || (queue.pending.isEmpty && queue.tracelessPending.isEmpty) then
      None
    else
      // The cap covers both kinds. Each gets up to half of it first, so a backlog of one cannot starve the other, and
      // whatever one leaves unused goes to the other
      val tracelessShare = math.min(queue.tracelessPending.size, maxEntriesPerBatch / 2)
      val updatesCount = math.min(queue.pending.size, maxEntriesPerBatch - tracelessShare)
      val tracelessCount = math.min(queue.tracelessPending.size, maxEntriesPerBatch - updatesCount)
      val batch = queue.pending.take(updatesCount).toList
      batch.foreach((key, _) => queue.pending.remove(key))
      val traceless = queue.tracelessPending.take(tracelessCount).toList
      traceless.foreach((key, _) => queue.tracelessPending.remove(key))
      queue.writing = true
      Some(Batch(batch.map(_._2), traceless.map(_._2)))

  private def write(poolId: PoolId, batch: Batch): Unit =
    writeBatch(poolId, batch.updates.map(_.update), batch.traceless.map(_.commit)).onComplete: result =>
      finished(poolId, batch, result)

  private def finished(poolId: PoolId, batch: Batch, result: Try[Unit]): Unit =
    result match
      case Failure(err) =>
        logger.warn(s"Gave up recording ${batch.updates.size} missed updates (ErrorTree) and " +
                    s"${batch.traceless.size} missed traceless commits (TracelessCommits) for pool $poolId: $err")
      case _ =>
    batch.updates.foreach(_.written.complete(result))
    batch.traceless.foreach(_.written.complete(result))
    synchronized(nextBatch(poolId, wasWriting = true)).foreach(write(poolId, _))

  private def writeBatch(poolId: PoolId, updates: List[MissedUpdate], traceless: List[MissedTracelessCommit]): Future[Unit] =
    val inserts = updates.map: u =>
      TKVLInsert(errorKey(u.store, u.objectId),
                 MissedUpdateFinalizationAction.encodeErrorEntry(u.missedTimestamp, u.storePointer))

    val tracelessInserts = traceless.map: t =>
      TKVLInsert(TracelessCommitEntry.key(t.store, t.transactionId), Value(t.entry.encode))

    def onFail(err: Throwable): Future[Unit] = err match
      case err: UnknownStoragePool =>
        // StoragePool must have been deleted
        throw StopRetrying(err)

      // Always permanent. Other errors stay on the retry path: the error tree deletes entries, so a
      // join racing the insert's scan can raise a transient InvalidObject.
      case err: TransactionCreationError => throw StopRetrying(err)

      case _ => Future.unit

    client.retryStrategy.retryUntilSuccessful(onFail):
      for
        pool <- client.getStoragePool(poolId)
        tx = client.newTransaction()
        _ <- if inserts.isEmpty then Future.unit else pool.errorTree.prepareUpdates(inserts, Nil)(using tx)
        _ <- if tracelessInserts.isEmpty then Future.unit
             else pool.tracelessCommitsTree.prepareUpdates(tracelessInserts, Nil)(using tx)
        _ = MissedUpdateFinalizationAction.limitTracking(poolId, tx)
        _ <- commitErrorTreeTransaction(tx)
      yield ()

  // Test seam: lets a test observe or delay the transactions of a write
  private[aspen] def commitErrorTreeTransaction(tx: Transaction): Future[Unit] = tx.commit().map(_ => ())
