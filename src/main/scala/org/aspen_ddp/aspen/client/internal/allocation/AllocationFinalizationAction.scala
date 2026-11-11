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
    * transaction was finalized. The entry is never skipped: a read straight after resolution can precede the stores
    * applying the allocation, so an object that does not read is not taken to be gone (skipping its entry would lose
    * it on repair and rebuild).
    *  - its entry is already present with this pointer: an earlier run recorded it
    *  - the object reads: a checked insert, with a lock on the object's revision as just read
    *  - it does not read: a checked insert alone
    *
    * Neither the insert of an absent key (`DoesNotExist`) nor the lock is a pin, so the transaction is marked
    * replay-safe. A replay never drops an entry. While the object is readable the lock makes a replay after its
    * deletion fail; a rare run or replay after the deletion that cannot read it can leave an orphan entry, which costs
    * the repair scan a lookup.
    */
  def execute(): Unit = client.retryStrategy.retryUntilSuccessful {
    val key = Key(newObject.id.toBytes)
    for
      pool <- client.getStoragePool(newObject.poolId)
      existing <- pool.allocationTree.get(key)
      _ <- existing match
        case Some(vs) if java.util.Arrays.equals(vs.value.bytes, newObject.toArray) => Future.unit
        case _ =>
          client.readMetadataOptional(newObject).flatMap: ostate =>
            val tx = client.newTransaction()
            tx.replaySafe("never drops an entry; a rare re-run after the object's deletion can leave an orphan entry")
            for
              _ <- pool.allocationTree.set(key, Value(newObject.toArray))(using tx)
              // An allocation tree node allocated by this very object's transaction can hold its own entry: the
              // update of the node then requires the object to exist, and a lock on it would conflict
              _ = ostate.foreach: state =>
                    try tx.exclusiveLockRevision(newObject, state.revision)
                    catch case _: ConflictingRequirements => ()
              _ <- tx.commit()
            yield ()
    yield
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