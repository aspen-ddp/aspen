package org.aspen_ddp.aspen.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory, ObjectAllocator, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, KeyValueObjectPointer, ObjectId, Value}
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, TransactionDescription}

import scala.concurrent.{ExecutionContext, Future, Promise}

class JoinFinalizationAction(val client: AspenClient,
                             val txd: TransactionDescription,
                             val rootManager: RootManager,
                             val tier: Int,
                             val deleteMinimum: Key,
                             val deleteNode: KeyValueObjectPointer) extends FinalizationAction {

  implicit val ec: ExecutionContext = client.clientContext

  private val completionPromise: Promise[Unit] = Promise()

  def complete: Future[Unit] = completionPromise.future

  def execute(): Unit = {

    val fcomplete = client.retryStrategy.retryUntilSuccessful {

      def setNewRoot(rootTier: Int, ordering: KeyOrdering, rootNode: KeyValueListNode): Future[Unit] = {
        implicit val tx: Transaction = client.newTransaction()

        // Root node always has a pointer to the AbsoluteMinimum node
        val newRoot = KeyValueObjectPointer(rootNode.contents(Key.AbsoluteMinimum).value.bytes)

        tx.setRefcount(rootNode.pointer, rootNode.refcount, rootNode.refcount.decrement())

        for {
          _ <- rootManager.prepareRootUpdate(tier-1, newRoot)
          _ <- tx.commit()
        } yield ()
      }

      def deleteFromExistingTier(rootTier: Int, ordering: KeyOrdering, rootNode: KeyValueListNode): Future[Unit] = {

        val fe = TieredKeyValueList.fetchContainingNode(client, rootTier, tier, ordering, deleteMinimum, rootNode, Set())

        def delete(e: Either[Set[ObjectId], KeyValueListNode]): Future[Unit] = {
          e match {
            case Left(_) => Future.failed(new BrokenTree)
            case Right(node) =>

              if (!node.contents.contains(deleteMinimum))
                Future.successful(())
              else {
                implicit val tx: Transaction = client.newTransaction()

                def onJoin(min: Key, ptr: KeyValueObjectPointer): Future[Unit] = {
                  JoinFinalizationAction.addToTransaction(rootManager, tier + 1, min, ptr, tx)
                  Future.successful(())
                }

                node.delete(deleteMinimum, onJoin)

                tx.commit().map(_=>())
              }
          }
        }

        for {
          e <- fe
          _ <- delete(e)
        } yield ()
      }

      rootManager.getRootNode().flatMap { t =>
        val (rootTier, ordering, orootNode) = t
        orootNode match {
          case None =>  Future.successful(())// Nothing to do
          case Some(rootNode) =>
            if (tier == rootTier && rootNode.contents.size == 2)
              setNewRoot(rootTier, ordering, rootNode)
            else
              deleteFromExistingTier(rootTier, ordering, rootNode)
        }
      }
    }

    completionPromise.completeWith(fcomplete)
  }
}

object JoinFinalizationAction extends RegisteredTypeFactory with FinalizationActionFactory {
  val typeUUID: UUID = UUID.fromString("C9EA4384-74A9-4044-8C64-7641D328F529")

  def createFinalizationAction(client: AspenClient,
                               txd: TransactionDescription,
                               data: Array[Byte]): FinalizationAction = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    val msb = bb.getLong()
    val lsb = bb.getLong()
    val tier = bb.getInt()
    val elen = bb.getInt()
    val klen = bb.getInt()
    val emgr = new Array[Byte](elen)
    val karr = new Array[Byte](klen)
    bb.get(emgr)
    bb.get(karr)
    val ptr = KeyValueObjectPointer(bb)

    val rootManagerTypeUUID = new UUID(msb, lsb)

    val factory = client.typeRegistry.getType[RootManagerFactory](rootManagerTypeUUID).get

    val rootManager = factory.createRootManager(client, emgr)

    new JoinFinalizationAction(client, txd, rootManager, tier, Key(karr), ptr)
  }

  def addToTransaction(mgr: RootManager,
                       tier: Int,
                       deleteMinimum: Key,
                       deleteNode: KeyValueObjectPointer,
                       tx: Transaction): Unit = {
    val emgr = mgr.encode()
    val arr = new Array[Byte](8 + 8 + 4 + 4 + 4 + emgr.length + deleteMinimum.bytes.length + deleteNode.encodedSize)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(mgr.typeId.uuid.getMostSignificantBits)
    bb.putLong(mgr.typeId.uuid.getLeastSignificantBits)
    bb.putInt(tier)
    bb.putInt(emgr.length)
    bb.putInt(deleteMinimum.bytes.length)
    bb.put(emgr)
    bb.put(deleteMinimum.bytes)
    deleteNode.encodeInto(bb)
    tx.addFinalizationAction(FinalizationActionId(typeUUID), Some(arr))
  }

}
