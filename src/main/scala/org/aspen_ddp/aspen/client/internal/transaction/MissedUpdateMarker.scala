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

  /** A store owes a commit it voted on and did not acknowledge, for some of its objects */
  final case class MissedUnackedCommit(store: StoreId, transactionId: TransactionId, entry: UnackedCommitEntry)

  private[transaction] def errorKey(store: StoreId, objectId: ObjectId): Key =
    val keyBytes = new Array[Byte](17)
    val bb = ByteBuffer.wrap(keyBytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(store.poolIndex)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)
    Key(keyBytes)

/** Records missed updates in their pools' ErrorTrees, and missed unacked commits in their UnackedCommits trees,
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

  private final class QueuedUnacked(val commit: MissedUnackedCommit):
    val written: Promise[Unit] = Promise()

  private final class Batch(val updates: List[Queued], val unacked: List[QueuedUnacked])

  private final class PoolQueue:
    val unackedPending: mutable.LinkedHashMap[(Byte, TransactionId), QueuedUnacked] = mutable.LinkedHashMap()
    val pending: mutable.LinkedHashMap[(Byte, ObjectId), Queued] = mutable.LinkedHashMap()
    var writing = false

  private val queues: mutable.Map[PoolId, PoolQueue] = mutable.Map()

  /** Completes once all of `updates` and `unacked` are recorded, or fails if one of their pools' writes gives up */
  def mark(updates: List[MissedUpdate], unacked: List[MissedUnackedCommit] = Nil): Future[Unit] =
    val (written, batches) = synchronized:
      val written = updates.map(enqueue) ++ unacked.map(enqueueUnacked)
      val pools = (updates.map(_.store.poolId) ++ unacked.map(_.store.poolId)).distinct
      (written, pools.flatMap(p => nextBatch(p, wasWriting = false).map(p -> _)))

    batches.foreach((poolId, batch) => write(poolId, batch))
    Future.sequence(written).map(_ => ())

  /** The entries waiting for a write */
  def queued: Int = synchronized(queues.valuesIterator.map(q => q.pending.size + q.unackedPending.size).sum)

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
  private def enqueueUnacked(commit: MissedUnackedCommit): Future[Unit] =
    val queue = queues.getOrElseUpdate(commit.store.poolId, new PoolQueue)
    queue.unackedPending.getOrElseUpdate((commit.store.poolIndex, commit.transactionId), new QueuedUnacked(commit)).written.future

  /** Must hold the lock. `wasWriting` says that the caller's write has just finished */
  private def nextBatch(poolId: PoolId, wasWriting: Boolean): Option[Batch] =
    val queue = queues(poolId)
    if wasWriting then
      queue.writing = false

    if queue.writing || (queue.pending.isEmpty && queue.unackedPending.isEmpty) then
      None
    else
      // The cap covers both kinds. Each gets up to half of it first, so a backlog of one cannot starve the other, and
      // whatever one leaves unused goes to the other
      val unackedShare = math.min(queue.unackedPending.size, maxEntriesPerBatch / 2)
      val updatesCount = math.min(queue.pending.size, maxEntriesPerBatch - unackedShare)
      val unackedCount = math.min(queue.unackedPending.size, maxEntriesPerBatch - updatesCount)
      val batch = queue.pending.take(updatesCount).toList
      batch.foreach((key, _) => queue.pending.remove(key))
      val unacked = queue.unackedPending.take(unackedCount).toList
      unacked.foreach((key, _) => queue.unackedPending.remove(key))
      queue.writing = true
      Some(Batch(batch.map(_._2), unacked.map(_._2)))

  private def write(poolId: PoolId, batch: Batch): Unit =
    writeBatch(poolId, batch.updates.map(_.update), batch.unacked.map(_.commit)).onComplete: result =>
      finished(poolId, batch, result)

  private def finished(poolId: PoolId, batch: Batch, result: Try[Unit]): Unit =
    result match
      case Failure(err) =>
        logger.warn(s"Gave up recording ${batch.updates.size} missed updates (ErrorTree) and " +
                    s"${batch.unacked.size} missed unacked commits (UnackedCommits) for pool $poolId: $err")
      case _ =>
    batch.updates.foreach(_.written.complete(result))
    batch.unacked.foreach(_.written.complete(result))
    synchronized(nextBatch(poolId, wasWriting = true)).foreach(write(poolId, _))

  private def writeBatch(poolId: PoolId, updates: List[MissedUpdate], unacked: List[MissedUnackedCommit]): Future[Unit] =
    // Blind upserts. A mark only asks for a repair pass, so a concurrent mark of the same key must not abort this one,
    // and a replay of the batch costs at most one more pass: the transaction is replay-safe without a pin
    val inserts = updates.map: u =>
      TKVLInsert(errorKey(u.store, u.objectId),
                 MissedUpdateFinalizationAction.encodeErrorEntry(u.missedTimestamp, u.storePointer), blind = true)

    val unackedInserts = unacked.map: t =>
      TKVLInsert(UnackedCommitEntry.key(t.store, t.transactionId), Value(t.entry.encode), blind = true)

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
        _ = tx.replaySafe("missed-update marks are idempotent")
        _ <- if inserts.isEmpty then Future.unit else pool.errorTree.prepareUpdates(inserts, Nil)(using tx)
        _ <- if unackedInserts.isEmpty then Future.unit
             else pool.unackedCommitsTree.prepareUpdates(unackedInserts, Nil)(using tx)
        _ = MissedUpdateFinalizationAction.limitTracking(poolId, tx)
        _ <- commitErrorTreeTransaction(tx)
      yield ()

  // Test seam: lets a test observe or delay the transactions of a write
  private[aspen] def commitErrorTreeTransaction(tx: Transaction): Future[Unit] = tx.commit().map(_ => ())
