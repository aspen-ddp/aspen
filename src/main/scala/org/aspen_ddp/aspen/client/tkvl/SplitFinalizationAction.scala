package org.aspen_ddp.aspen.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.client.internal.read.BaseReadDriver
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, KeyValueObjectPointer, ObjectId, Value}
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, TransactionDescription}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class SplitFinalizationAction(val client: AspenClient,
                              val txd: TransactionDescription,
                              val rootManager: RootManager,
                              val tier: Int,
                              val newMinimum: Key,
                              val newNode: KeyValueObjectPointer) extends FinalizationAction {

  given ExecutionContext = client.clientContext

  private val completionPromise: Promise[Unit] = Promise()

  def complete: Future[Unit] = completionPromise.future

  def execute(): Unit = {

    val fcomplete = client.retryStrategy.retryUntilSuccessful {

      def createNewRoot(rootTier: Int, ordering: KeyOrdering, rootNode: KeyValueListNode): Future[Unit] = {

        implicit val tx: Transaction = client.newTransaction()

        val rootContent = Map(
          rootNode.minimum -> Value(rootNode.pointer.toArray),
          newMinimum -> Value(newNode.toArray)
        )

        for {
          alloc <- rootManager.getAllocatorForTier(tier)
          guard <- rootManager.getRootRevisionGuard()
          nroot <- alloc.allocateKeyValueObject(guard, rootContent)
          _ <- rootManager.prepareRootUpdate(tier, nroot)
          _ <- tx.commit()
        } yield ()
      }

      def insertIntoExistingTier(rootTier: Int, ordering: KeyOrdering, rootNode: KeyValueListNode): Future[Unit] = {

        implicit val tx: Transaction = client.newTransaction()

        val fe = TieredKeyValueList.fetchContainingNode(client, rootTier, tier, ordering, newMinimum, rootNode, Set())
        val fnodeSize = rootManager.getMaxNodeSize(tier)
        val falloc = rootManager.getAllocatorForTier(tier)

        def prepareInsert(e: Either[Set[ObjectId], KeyValueListNode], nodeSize: Int, alloc: ObjectAllocator): Future[Unit] = {
          e match {
            case Left(_) => Future.failed(new BrokenTree)
            case Right(node) =>

              def onSplit(min: Key, ptr: KeyValueObjectPointer): Future[Unit] = {
                SplitFinalizationAction.addToTransaction(rootManager, tier+1, min, ptr, tx)
                Future.successful(())
              }

              node.insert(newMinimum, Value(newNode.toArray), nodeSize, alloc, onSplit).map(_=>())

          }
        }

        for {
          e <- fe
          nodeSize <- fnodeSize

          alloc <- falloc

          _ <- prepareInsert(e, nodeSize, alloc)

          _ <- tx.commit()
        } yield ()
      }

      rootManager.getRootNode().flatMap { t =>
        val (rootTier, ordering, orootNode) = t

        orootNode match {
          case None => Future.successful(()) // shouldn't be possible
          case Some(rootNode) =>
            if (tier > rootTier)
              createNewRoot(rootTier, ordering, rootNode)
            else
              insertIntoExistingTier(rootTier, ordering, rootNode)
        }
      }
    }

    completionPromise.completeWith(fcomplete)
  }
}

object SplitFinalizationAction extends RegisteredTypeFactory with FinalizationActionFactory {
  val typeUUID: UUID = UUID.fromString("68C3D242-CEA0-49D7-AA14-AB8E16D32FAD")

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

    new SplitFinalizationAction(client, txd, rootManager, tier, Key(karr), ptr)
  }

  def addToTransaction(mgr: RootManager,
                       tier: Int,
                       newMinimum: Key,
                       newNode: KeyValueObjectPointer,
                       tx: Transaction): Unit = {
    val emgr = mgr.encode()
    val arr = new Array[Byte](8 + 8 + 4 + 4 + 4 + emgr.length + newMinimum.bytes.length + newNode.encodedSize)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(mgr.typeId.uuid.getMostSignificantBits)
    bb.putLong(mgr.typeId.uuid.getLeastSignificantBits)
    bb.putInt(tier)
    bb.putInt(emgr.length)
    bb.putInt(newMinimum.bytes.length)
    bb.put(emgr)
    bb.put(newMinimum.bytes)
    newNode.encodeInto(bb)
    tx.addFinalizationAction(FinalizationActionId(typeUUID), Some(arr))
  }

}
