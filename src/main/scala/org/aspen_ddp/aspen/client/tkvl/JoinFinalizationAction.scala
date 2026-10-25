package org.aspen_ddp.aspen.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory, ObjectAllocator, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectId, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, TransactionDescription}

import scala.concurrent.{ExecutionContext, Future, Promise}

class JoinFinalizationAction(val client: AspenClient,
                             val txd: TransactionDescription,
                             val rootManager: RootManager,
                             val tier: Int,
                             val deleteMinimum: Key,
                             val deleteNode: ObjectPointer) extends FinalizationAction {

  given ExecutionContext = client.clientContext

  private val completionPromise: Promise[Unit] = Promise()

  def complete: Future[Unit] = completionPromise.future

  def execute(): Unit = {

    val fcomplete = client.retryStrategy.retryUntilSuccessful {

      def setNewRoot(rootTier: Int, ordering: KeyOrdering, rootNode: ListNode): Future[Unit] = {
        given tx: Transaction = client.newTransaction()

        // Root node always has a pointer to the AbsoluteMinimum node
        val newRoot = ObjectPointer(rootNode.get(Key.AbsoluteMinimum).get.bytes)

        tx.setRefcount(rootNode.pointer, rootNode.refcount, rootNode.refcount.decrement())

        for {
          _ <- rootManager.prepareRootUpdate(tier-1, Some(newRoot))
          _ <- tx.commit()
        } yield ()
      }

      def deleteFromExistingTier(rootTier: Int, ordering: KeyOrdering, rootNode: ListNode): Future[Unit] = {

        val fe = TierEngine.fetchContainingNode(client, rootTier, tier, ordering, deleteMinimum, rootNode, Set())

        def delete(e: Either[Set[ObjectId], ListNode]): Future[Unit] = {
          e match {
            case Left(_) => Future.failed(new BrokenTree)
            case Right(node) =>

              if (node.get(deleteMinimum).isEmpty)
                Future.unit
              else {
                given tx: Transaction = client.newTransaction()

                def onJoin(min: Key, ptr: ObjectPointer): Future[Unit] = {
                  JoinFinalizationAction.addToTransaction(rootManager, tier + 1, min, ptr, tx)
                  Future.unit
                }

                // A join reads the right node before staging its updates, so commit only once the delete is staged
                for {
                  _ <- TierEngine.encodingFor(node).delete(node, deleteMinimum, onJoin)
                  _ <- tx.commit()
                } yield ()
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
          case None =>  Future.unit// Nothing to do
          case Some(rootNode) =>
            if (tier == rootTier && rootNode.size == 2)
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
    val kind = TreeKind.fromCode(bb.get())
    val elen = bb.getInt()
    val klen = bb.getInt()
    val emgr = new Array[Byte](elen)
    val karr = new Array[Byte](klen)
    bb.get(emgr)
    bb.get(karr)
    val ptr = ObjectPointer.fromByteBuffer(bb)
    if !Root.pointerMatchesKind(kind, ptr) then
      throw new IllegalArgumentException(s"Node pointer $ptr does not match tree kind $kind")

    val rootManagerTypeUUID = new UUID(msb, lsb)

    val factory = client.typeRegistry.getType[RootManagerFactory](rootManagerTypeUUID).get

    val rootManager = factory.createRootManager(client, emgr)

    new JoinFinalizationAction(client, txd, rootManager, tier, Key(karr), ptr)
  }

  def addToTransaction(mgr: RootManager,
                       tier: Int,
                       deleteMinimum: Key,
                       deleteNode: ObjectPointer,
                       tx: Transaction): Unit =
    tx.addFinalizationAction(FinalizationActionId(typeUUID), Some(encode(mgr, tier, deleteMinimum, deleteNode)))

  /** The finalization action's payload. The tree kind is written so the node pointer can be checked against it. */
  private[tkvl] def encode(mgr: RootManager, tier: Int, deleteMinimum: Key, deleteNode: ObjectPointer): Array[Byte] = {
    val emgr = mgr.encode()
    val arr = new Array[Byte](8 + 8 + 4 + 1 + 4 + 4 + emgr.length + deleteMinimum.bytes.length + deleteNode.encodedSize)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(mgr.typeId.uuid.getMostSignificantBits)
    bb.putLong(mgr.typeId.uuid.getLeastSignificantBits)
    bb.putInt(tier)
    bb.put(TreeKind.of(deleteNode).code)
    bb.putInt(emgr.length)
    bb.putInt(deleteMinimum.bytes.length)
    bb.put(emgr)
    bb.put(deleteMinimum.bytes)
    deleteNode.encodeInto(bb)
    arr
  }

}
