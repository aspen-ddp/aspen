package org.aspen_ddp.aspen.client.internal.transaction

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, StopRetrying, Transaction, TransactionCreationError, UnknownStoragePool}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, SerializedFinalizationAction, TransactionDescription}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}

class MissedUpdateFinalizationAction(val client: AspenClient,
                                     val txd: TransactionDescription) extends FinalizationAction {

  given ExecutionContext = client.clientContext

  private var commitErrors: Map[StoreId, List[ObjectId]] = Map()

  private var markTask: Option[ScheduledTask] = None

  private val completionPromise: Promise[Unit] = Promise()

  logger.debug(s"Created MissedUpdateFinalizationAction for Tx ${txd.transactionId}")

  def complete: Future[Unit] = completionPromise.future

  override def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = synchronized {
    this.commitErrors = commitErrors

    if txd.allDataStores.size == commitErrors.keysIterator.toSet.size && !commitErrors.valuesIterator.exists(_.nonEmpty) then
      logger.trace(s"All stores responded to commit of tx ${txd.transactionId} without error. No MissedUpdates.")
      markTask.foreach(_.cancel())
      if (!completionPromise.isCompleted)
        completionPromise.success(())
  }

  def execute(): Unit = synchronized {
    markTask = Some(client.backgroundTaskManager.schedule(MissedUpdateFinalizationAction.errorTimeout) {
      markMissedUpdates()
    })
  }

  // Records missed updates with the missed transaction's timestamp and storePointer bytes. Object
  // deletion removes the pool's allocationTree entry, which is the only other place those bytes
  // are persisted, so without this a repair pass cannot locate a deleted object's per-store state.
  //
  // An empty storePointer means "this object has no storePointer". A repair pass treats it as
  // "delete by ObjectId alone" rather than as an error, which RocksDB stores still rely on.
  // See StoreRepairer.repairDeletion in aspen.server.repair.
  def markMissedUpdates(): Unit = synchronized {
    // Build a map from ObjectId to ObjectPointer to recover pointers for ids in commitErrors
    val idToPointer = txd.allReferencedObjectsSet.map(ptr => ptr.id -> ptr).toMap

    val errors = txd.allDataStores.foldLeft(List[(StoreId, ObjectId, Array[Byte])]()) { (l, storeId) =>
      commitErrors.get(storeId) match
        case None =>
          txd.allHostedObjects(storeId).map(ptr => (storeId, ptr.id, ptr.storePointer)) ++ l
        case Some(lst) =>
          // Note: this case is not reachable today (ids in commitErrors come from RequirementsChecker,
          // which only reports r.objectPointer.id from this same txd's requirements), but we handle it
          // defensively by looking up pointers from allReferencedObjectsSet, falling back to EmptyArray.
          lst.map { objectId =>
            val storePointer = idToPointer.get(objectId).map(_.storePointer).getOrElse(ObjectPointer.EmptyArray)
            (storeId, objectId, storePointer)
          } ++ l
    }

    Future.sequence(errors.map(t => markMissedUpdate(t._1, t._2, t._3))).foreach { _ =>
      synchronized {
        if (!completionPromise.isCompleted)
          completionPromise.success(())
      }
    }
  }

  def markMissedUpdate(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte]): Future[Unit] = {
    //println(s"Marking missed update for Tx ${txd.transactionId}. Store: ${storeId} Object: $objectId")

    def onFail(err: Throwable): Future[Unit] = { err match {
      case err: UnknownStoragePool =>
        // StoragePool must have been deleted
        throw StopRetrying(err)

      // Always permanent. Other errors stay on the retry path: the error tree deletes entries, so a
      // join racing the insert's scan can raise a transient InvalidObject.
      case err: TransactionCreationError => throw StopRetrying(err)

      case _ => Future.unit
    }}

    val keyBytes = new Array[Byte](17)
    val bb = ByteBuffer.wrap(keyBytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(storeId.poolIndex)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)

    val key = Key(keyBytes)

    client.retryStrategy.retryUntilSuccessful(onFail) {
      logger.trace(s"Marking missed update for Tx ${txd.transactionId}. Store: ${storeId} Object: $objectId")
      for {
        pool <- client.getStoragePool(storeId.poolId)
        tx = client.newTransaction()
        _ <- pool.errorTree.set(key,
               MissedUpdateFinalizationAction.encodeErrorEntry(txd.startTimestamp, storePointer))(using tx)
        _ = MissedUpdateFinalizationAction.limitTracking(storeId.poolId, tx)
        _ <- tx.commit()
      } yield {
        logger.trace(s"COMPLETED - Marking missed update for Tx ${txd.transactionId}. Store: ${storeId} Object: $objectId")
        ()
      }
    }
  }
}

object MissedUpdateFinalizationAction extends RegisteredTypeFactory with FinalizationActionFactory {
  val typeUUID: UUID = UUID.fromString("E1F61997-61A7-4C86-A211-0EBA601DEDA3")

  /** Disables missed-update tracking on `tx`, an ErrorTree write about `entryPool`, only when the
    * write itself touches that pool. Only then could a missed update of the write mark an entry in
    * the same tree. Metadata placement keeps every other pool's ErrorTree out of the pool it describes.
    */
  def limitTracking(entryPool: PoolId, tx: Transaction): Unit =
    if tx.referencedPoolIds.contains(entryPool) then
      tx.disableMissedUpdateTracking()

  var errorTimeout = Duration(250, MILLISECONDS)

  /** An errorTree entry's value: the missed transaction's start timestamp, big-endian, then the
    * object's storePointer bytes (possibly empty).
    *
    * The timestamp is the one a store gives the object when it applies that transaction, so a
    * repair that installs a state at least that new has caught the store up past the miss. The
    * entry's own key timestamp cannot serve: it belongs to the later transaction that wrote it.
    */
  def encodeErrorEntry(missedTimestamp: HLCTimestamp, storePointer: Array[Byte]): Value =
    val bb = ByteBuffer.allocate(HLCTimestamp.EncodedSize + storePointer.length)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(missedTimestamp.asLong)
    bb.put(storePointer)
    Value(bb.array())

  /** Inverse of `encodeErrorEntry`: the missed transaction's timestamp and the storePointer */
  def decodeErrorEntry(value: Value): (HLCTimestamp, Array[Byte]) =
    val bb = ByteBuffer.wrap(value.bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    val ts = HLCTimestamp(bb.getLong())
    val storePointer = new Array[Byte](bb.remaining())
    bb.get(storePointer)
    (ts, storePointer)

  def createFinalizationAction(client: AspenClient,
                               txd: TransactionDescription,
                               data: Array[Byte]): FinalizationAction = {

    new MissedUpdateFinalizationAction(client, txd)
  }

  def createSerializedFA(missedCommitDelayInMs: Int): SerializedFinalizationAction = {
    SerializedFinalizationAction(FinalizationActionId(typeUUID), Array[Byte]())
  }

}