package org.aspen_ddp.aspen.client.internal.allocation

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, ConflictingRequirements, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, TransactionDescription}
import scribe.Logging
import org.aspen_ddp.aspen.common.util.someOrThrow

import scala.concurrent.{ExecutionContext, Future, Promise}

class AllocationFinalizationAction(val client: AspenClient,
                                   val txd: TransactionDescription,
                                   val newObject: ObjectPointer) extends FinalizationAction with Logging {

  given ExecutionContext = client.clientContext

  private val completionPromise: Promise[Unit] = Promise()

  logger.debug(s"Created AllocationFinalizationAction for Tx ${txd.transactionId}, object ${newObject.id}")
  def complete: Future[Unit] = completionPromise.future

  /** Records the new object in its pool's allocation tree. At least once, and possibly long after the allocating
    * transaction was finalized, so the object is read first:
    *  - gone: there is nothing to record, and the action completes without a transaction
    *  - its entry is already present with this pointer: an earlier run recorded it
    *  - otherwise a checked insert, with a lock on the object's revision as just read
    *
    * Neither the insert of an absent key (`DoesNotExist`) nor the lock is a pin, so the transaction is marked
    * replay-safe: a replay cannot re-add the entry once the object is deleted, because a deleted object fails the
    * revision lock. A late first run reads the current revision, so it cannot fail forever.
    */
  def execute(): Unit = client.retryStrategy.retryUntilSuccessful {
    val key = Key(newObject.id.toBytes)
    client.readMetadataOptional(newObject).flatMap:
      case None =>
        logger.debug(s"AllocationFA for Tx ${txd.transactionId}: object ${newObject.id} is gone, nothing to record")
        Future.unit
      case Some(state) =>
        for
          pool <- client.getStoragePool(newObject.poolId)
          existing <- pool.allocationTree.get(key)
          _ <- existing match
            case Some(vs) if java.util.Arrays.equals(vs.value.bytes, newObject.toArray) => Future.unit
            case _ =>
              val tx = client.newTransaction()
              tx.replaySafe("cannot re-add once the object is gone: its revision lock fails")
              for
                _ <- pool.allocationTree.set(key, Value(newObject.toArray))(using tx)
                // An allocation tree node allocated by this very object's transaction can hold its own entry: the
                // update of the node then requires the object to exist, and a lock on it would conflict
                _ = try tx.lockRevision(newObject, state.revision)
                    catch case _: ConflictingRequirements => ()
                _ <- tx.commit()
              yield ()
        yield ()
    .map: _ =>
      logger.debug(s"AllocationFA Completed Successfully for Tx ${txd.transactionId}, Object ${newObject.id}")
      completionPromise.trySuccess(())
  }
}

object AllocationFinalizationAction extends RegisteredTypeFactory with FinalizationActionFactory {
  val typeUUID: UUID = UUID.fromString("821049AD-D2A8-4D17-8080-E01A4678C8B2")

  def createFinalizationAction(client: AspenClient,
                               txd: TransactionDescription,
                               data: Array[Byte]): FinalizationAction = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    val newObject = ObjectPointer.fromByteBuffer(bb)

    //println(s"****************** CRATED ALLOC FA for ${newObject.id}")

    new AllocationFinalizationAction(client, txd, newObject)
  }

  def addToTransaction(newObject: ObjectPointer,
                       tx: Transaction): Unit = {
    tx.addFinalizationAction(FinalizationActionId(typeUUID), Some(newObject.toArray))
  }

}