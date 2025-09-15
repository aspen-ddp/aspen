package org.aspen_ddp.aspen.client.internal.transaction

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, StopRetrying, Transaction, UnknownStoragePool}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, SerializedFinalizationAction, TransactionDescription}
import org.aspen_ddp.aspen.common.util.BackgroundTask.ScheduledTask

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
      println(s"All stores responded to commit of tx ${txd.transactionId} without error. No MissedUpdates.")
      logger.trace(s"All stores responded to commit of tx ${txd.transactionId} without error. No MissedUpdates.")
      markTask.foreach(_.cancel())
      if (!completionPromise.isCompleted)
        completionPromise.success(())
  }

  def execute(): Unit = synchronized {
    markTask = Some(client.backgroundTasks.schedule(MissedUpdateFinalizationAction.errorTimeout) {
      markMissedUpdates()
    })
  }

  def markMissedUpdates(): Unit = synchronized {
    val errors = txd.allDataStores.foldLeft(List[(StoreId, ObjectId)]()) { (l, storeId) =>
      commitErrors.get(storeId) match
        case None => txd.allHostedObjects(storeId).map(ptr => (storeId, ptr.id)) ++ l
        case Some(lst) => lst.map(objectId => (storeId, objectId)) ++ l
    }

    Future.sequence(errors.map(t => markMissedUpdate(t._1, t._2))).foreach { _ =>
      synchronized {
        if (!completionPromise.isCompleted)
          completionPromise.success(())
      }
    }
  }

  def markMissedUpdate(storeId: StoreId, objectId: ObjectId): Future[Unit] = {
    //println(s"Marking missed update for Tx ${txd.transactionId}. Store: ${storeId} Object: $objectId")

    def onFail(err: Throwable): Future[Unit] = { err match {
      case err: UnknownStoragePool =>
        // StoragePool must have been deleted
        throw StopRetrying(err)

      case _ => Future.unit
    }}

    val keyBytes = new Array[Byte](17)
    val bb = ByteBuffer.wrap(keyBytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(storeId.poolIndex)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)

    val key = Key(keyBytes)

    client.retryStrategy.retryUntilSuccessful(onFail _) {
      logger.trace(s"Marking missed update for Tx ${txd.transactionId}. Store: ${storeId} Object: $objectId")
      for {
        pool <- client.getStoragePool(storeId.poolId)
        tx = client.newTransaction()
        _ = tx.disableMissedUpdateTracking()
        _ <- pool.get.errorTree.set(key, Value(Array()))(tx)
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

  var errorTimeout = Duration(250, MILLISECONDS)

  def createFinalizationAction(client: AspenClient,
                               txd: TransactionDescription,
                               data: Array[Byte]): FinalizationAction = {

    new MissedUpdateFinalizationAction(client, txd)
  }

  def createSerializedFA(missedCommitDelayInMs: Int): SerializedFinalizationAction = {
    SerializedFinalizationAction(FinalizationActionId(typeUUID), Array[Byte]())
  }

}