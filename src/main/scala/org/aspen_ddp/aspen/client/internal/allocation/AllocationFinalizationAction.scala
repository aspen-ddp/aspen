package org.aspen_ddp.aspen.client.internal.allocation

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, ConflictingRequirements, InvalidObject, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, StopRetrying, Transaction}
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
    * transaction was finalized.
    *  - its entry is already present with this pointer: an earlier run recorded it
    *  - the object reads: a checked insert, with a shared lock on the object's revision as just read
    *  - the read proves it deleted (`InvalidObject.provesDeletion`): there is nothing to record, and the action
    *    completes without a transaction
    *  - any other read failure: the attempt fails and is retried
    *
    * A read straight after resolution can precede the stores applying the allocation. Its commit voters then answer
    * `AllocationPending`, not `ObjectNotFound`, and the read waits for them to apply it (BaseReadDriver re-reads such
    * a store, which parks the re-read): a live object is never taken to be gone (skipping its entry would lose it on
    * repair and rebuild). Residual: an object already below
    * writeThreshold live copies reads as deleted and gets no entry; it was unreadable already.
    *
    * Neither the insert of an absent key (`DoesNotExist`) nor the lock is a pin, so the transaction is marked
    * replay-safe: once the object is deleted its revision lock fails, so a replay or a later run never leaves an
    * orphan entry (transaction-requirements 9.4). The lock is shared: it fails on a deleted or changed object
    * just as an exclusive one does, and two runs of the action (a leader's and a recovery driver's finalizer) do not
    * collide on it. It still collides with a concurrent write of the object, whose writer retries.
    */
  // A failure fails the action, and the driver starts the finalizer again (FinalizerRunner.start)
  def execute(): Unit = client.retryStrategy.retryUntilSuccessful {
    val key = Key(newObject.id.toBytes)
    for
      pool <- client.getStoragePool(newObject.poolId)
      existing <- pool.allocationTree.get(key)
      _ <- existing match
        case Some(vs) if java.util.Arrays.equals(vs.value.bytes, newObject.toArray) => Future.unit
        case _ =>
          client.readMetadata(newObject).map(Some(_)).recover:
            case e: InvalidObject if e.provesDeletion => None
          .flatMap:
            case None =>
              logger.debug(s"AllocationFA for Tx ${txd.transactionId}: object ${newObject.id} is deleted, nothing " +
                "to record")
              Future.unit
            case Some(state) =>
              val tx = client.newTransaction()
              tx.replaySafe("cannot add the entry once the object is deleted: its revision lock fails")
              for
                _ <- pool.allocationTree.set(key, Value(newObject.toArray))(using tx)
                // An allocation tree node allocated by this very object's transaction can hold its own entry: the
                // update of the node then requires the object to exist, and a lock on it would conflict
                _ = try tx.sharedLockRevision(newObject, state.revision)
                    catch case _: ConflictingRequirements => ()
                _ <- tx.commit()
              yield ()
    yield
      logger.debug(s"AllocationFA Completed Successfully for Tx ${txd.transactionId}, Object ${newObject.id}")
      completionPromise.trySuccess(())
  }.failed.foreach(completionPromise.tryFailure)
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

  // Added by TransactionBuilder.addAllocatingObject for every allocated object

}