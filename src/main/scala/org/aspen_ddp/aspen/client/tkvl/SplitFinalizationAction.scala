package org.aspen_ddp.aspen.client.tkvl

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectId, ObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, TransactionDescription}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class SplitFinalizationAction(val client: AspenClient,
                              val txd: TransactionDescription,
                              val rootManager: RootManager,
                              val tier: Int,
                              val newMinimum: Key,
                              val newNode: ObjectPointer) extends FinalizationAction {

  given ExecutionContext = client.clientContext

  private val completionPromise: Promise[Unit] = Promise()

  def complete: Future[Unit] = completionPromise.future

  def execute(): Unit = {

    val fcomplete = client.retryStrategy.retryUntilSuccessful {

      def createNewRoot(rootTier: Int, ordering: KeyOrdering, rootNode: ListNode): Future[Unit] = {

        given tx: Transaction = client.newTransaction()

        val rootContent = Map(
          rootNode.minimum -> Value(rootNode.pointer.toArray),
          newMinimum -> Value(newNode.toArray)
        )

        for {
          alloc <- rootManager.getAllocatorForTier(tier)
          nroot <- TierEngine.encodingFor(rootNode).allocate(alloc, ordering, rootContent)
          _ <- rootManager.prepareRootUpdate(tier, Some(nroot))
          _ <- tx.commit()
        } yield ()
      }

      /** Adds the new node's entry to `tier`. At least once, and possibly long after the split was finalized, so the
        * new node is read first:
        *  - gone (a later join deleted it): there is nothing to index, and the action completes without a transaction
        *  - its entry is already present with this pointer: an earlier run added it
        *  - otherwise a checked insert, with a lock on the new node's revision as just read
        *
        * On a KeyValue-object tree neither the insert of an absent key (`DoesNotExist`) nor the lock is a pin, so the
        * transaction is marked replay-safe: a replay cannot re-add the entry once the node is gone, because a deleted
        * node fails the revision lock. A late first run reads the current revision, so it cannot fail forever.
        */
      def insertIntoExistingTier(rootTier: Int, ordering: KeyOrdering, rootNode: ListNode): Future[Unit] = {

        def prepareInsert(node: ListNode, nodeRevision: ObjectRevision, nodeSize: Int,
                          alloc: ObjectAllocator): Future[Unit] = {
          given tx: Transaction = client.newTransaction()
          tx.replaySafe("cannot re-add once the node is gone: its revision lock fails")

          def onSplit(min: Key, ptr: ObjectPointer): Future[Unit] = {
            SplitFinalizationAction.addToTransaction(rootManager, tier+1, min, ptr, tx)
            Future.unit
          }

          for {
            _ <- TierEngine.encodingFor(node).insert(node, newMinimum, Value(newNode.toArray), nodeSize, alloc,
                   onSplit, PutRequirement.Checked)
            _ = tx.exclusiveLockRevision(newNode, nodeRevision)
            _ <- tx.commit()
          } yield ()
        }

        client.readMetadataOptional(newNode).flatMap {
          case None => Future.unit // Deleted by a later join: nothing to index

          case Some(newNodeState) =>
            TierEngine.fetchContainingNode(client, rootTier, tier, ordering, newMinimum, rootNode, Set()).flatMap {
              case Left(_) =>
                // Tree is likely being deleted. Not much we can do
                Future.unit

              case Right(node) =>
                node.get(newMinimum) match {
                  case Some(v) if java.util.Arrays.equals(v.bytes, newNode.toArray) => Future.unit // Already indexed
                  case _ =>
                    for {
                      nodeSize <- rootManager.getMaxNodeSize(tier)
                      alloc <- rootManager.getAllocatorForTier(tier)
                      _ <- prepareInsert(node, newNodeState.revision, nodeSize, alloc)
                    } yield ()
                }
            }
        }
      }

      // Fresh, never cached: a retry after a collision, or a repeat run, must see what the last attempt changed
      rootManager.getFreshRootNode().flatMap { t =>
        val (rootTier, ordering, orootNode) = t

        orootNode match {
          case None => 
            // Tree must have been deleted. Should be safe to ignore
            Future.unit 
            
          case Some(rootNode) =>
            if (tier > rootTier)
              createNewRoot(rootTier, ordering, rootNode)
            else
              insertIntoExistingTier(rootTier, ordering, rootNode)
        }
      }.recover {
        // A misconfigured tier, or an index entry (the new minimum plus a pointer) too large for the tier's nodes,
        // cannot be fixed by retrying, and failing would leave the transaction unfinalized forever. The split itself
        // already committed and the new node is linked from its left neighbour, so the tree stays correct; only the
        // upper tier's index entry for the new node is skipped, which descent tolerates. Log and complete.
        case err: NodeSizeExceedsAllocatorLimit =>
          logger.error(s"Skipping TKVL split insert of $newMinimum into tier $tier: ${err.getMessage}")
        case _: NodeSizeExceeded =>
          logger.error(s"Skipping TKVL split insert of $newMinimum into tier $tier: the entry exceeds the tier's " +
            "node size")
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

    new SplitFinalizationAction(client, txd, rootManager, tier, Key(karr), ptr)
  }

  def addToTransaction(mgr: RootManager,
                       tier: Int,
                       newMinimum: Key,
                       newNode: ObjectPointer,
                       tx: Transaction): Unit =
    tx.addFinalizationAction(FinalizationActionId(typeUUID), Some(encode(mgr, tier, newMinimum, newNode)))

  /** The finalization action's payload. The tree kind is written so the node pointer can be checked against it. */
  private[tkvl] def encode(mgr: RootManager, tier: Int, newMinimum: Key, newNode: ObjectPointer): Array[Byte] = {
    val emgr = mgr.encode()
    val arr = new Array[Byte](8 + 8 + 4 + 1 + 4 + 4 + emgr.length + newMinimum.bytes.length + newNode.encodedSize)
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(mgr.typeId.uuid.getMostSignificantBits)
    bb.putLong(mgr.typeId.uuid.getLeastSignificantBits)
    bb.putInt(tier)
    bb.put(TreeKind.of(newNode).code)
    bb.putInt(emgr.length)
    bb.putInt(newMinimum.bytes.length)
    bb.put(emgr)
    bb.put(newMinimum.bytes)
    newNode.encodeInto(bb)
    arr
  }

}
