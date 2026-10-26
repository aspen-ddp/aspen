package org.aspen_ddp.aspen.client.tkvl

import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory, InvalidObject, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectPointer}
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, TransactionDescription}

import scala.concurrent.{ExecutionContext, Future, Promise}

/** Removes an emptied node from its tier.
  *
  * A node is left empty when its last key is deleted and it cannot be joined with its right neighbor: the rightmost
  * node of a tier has no neighbor to absorb, and the joined node would be too large if the empty node's minimum is
  * longer than the neighbor's. Such a node keeps its entry in the tier above, which stops tier reduction. This action
  * points the empty node's predecessor at the empty node's link, drops its refcount and, in the same transaction, adds
  * a `JoinFinalizationAction` that removes its entry from the tier above (reducing tiers if that empties the root).
  *
  * The predecessor now stores the neighbor's minimum in place of the empty node's. That is smaller whenever the join
  * was declined for size, but the neighbor may have changed since. If the predecessor no longer fits it splits in the
  * normal manner, and a `SplitFinalizationAction` adds the new node to the tier above. If no split point fits, the
  * node is left empty.
  *
  * `tier` is the tier of that entry, one above the node's own, matching `JoinFinalizationAction`.
  *
  * Nothing is done if the node is gone, is no longer empty or has no predecessor. Because the node's revision is
  * required and bumped, a racing insert into it fails this transaction and the retry re-checks, and a racing
  * removal that would rewrite it after this one fails instead.
  */
class TailRemovalFinalizationAction(val client: AspenClient,
                                    val txd: TransactionDescription,
                                    val rootManager: RootManager,
                                    val tier: Int,
                                    val tailMinimum: Key,
                                    val tailNode: ObjectPointer) extends FinalizationAction {

  given ExecutionContext = client.clientContext

  private val completionPromise: Promise[Unit] = Promise()

  def complete: Future[Unit] = completionPromise.future

  def execute(): Unit = {
    val fcomplete = client.retryStrategy.retryUntilSuccessful {
      rootManager.getRootNode().flatMap { t =>
        val (rootTier, ordering, orootNode) = t
        orootNode match {
          case Some(rootNode) if tier <= rootTier => removeEmptyNode(rootTier, ordering, rootNode)
          case _ => Future.unit // The tree shrank past this node
        }
      }
    }

    completionPromise.completeWith(fcomplete)
  }

  private def removeEmptyNode(rootTier: Int, ordering: KeyOrdering, rootNode: ListNode): Future[Unit] = {
    val encoding = TierEngine.encodingFor(rootNode)
    val nodeTier = tier - 1

    def read(pointer: ObjectPointer, minimum: Key): Future[ListNode] =
      encoding.read(client, pointer, minimum, ordering, s"Removing empty node ${tailNode.id}")

    // The node whose link is the empty node, or None if nothing links to it
    def findPredecessor(): Future[Option[ListNode]] =
      TierEngine.fetchPredecessor(client, rootTier, nodeTier, ordering, tailMinimum, tailNode.id, rootNode).flatMap {
        case Right(opred) => Future.successful(opred)
        case Left(_) => findPredecessorFromHead()
      }

    // Only when the search through the tiers above runs out of candidates, as when racing joins deleted the nodes it
    // was following. A scan of the whole tier is the correct, if slow, way to look.
    def findPredecessorFromHead(): Future[Option[ListNode]] = {
      def scan(node: ListNode): Future[Option[ListNode]] = node.link match {
        case None => Future.successful(None)
        case Some(l) if l.pointer.id == tailNode.id => Future.successful(Some(node))
        case Some(l) => read(l.pointer, l.minimum).flatMap(scan)
      }

      TierEngine.fetchContainingNode(client, rootTier, nodeTier, ordering, Key.AbsoluteMinimum, rootNode, Set())
        .flatMap {
          case Left(_) => Future.failed(new BrokenTree)
          case Right(head) => scan(head)
        }
    }

    def unlink(tail: ListNode): Future[Unit] =
      findPredecessor().flatMap {
        case None => Future.unit // Already unlinked
        case Some(pred) =>
          given tx: Transaction = client.newTransaction()

          def onSplit(min: Key, ptr: ObjectPointer): Future[Unit] = {
            SplitFinalizationAction.addToTransaction(rootManager, tier, min, ptr, tx)
            Future.unit
          }

          for {
            maxNodeSize <- rootManager.getMaxNodeSize(nodeTier)
            alloc <- rootManager.getAllocatorForTier(nodeTier)
            _ <- encoding.relink(pred, tail.link, maxNodeSize, alloc, onSplit)
            // A version bump rather than a revision lock: deleting the node must change its revision, so that a
            // racing removal of its right neighbor, which read it as that neighbor's predecessor, cannot still
            // rewrite it and leave its own predecessor linked to the deleted neighbor
            _ = tx.bumpVersion(tail.pointer, tail.revision)
            _ = tx.setRefcount(tail.pointer, tail.refcount, tail.refcount.decrement())
            _ = JoinFinalizationAction.addToTransaction(rootManager, tier, tailMinimum, tailNode, tx)

            // The predecessor may itself be empty, as when its own removal is still pending
            _ = if pred.entries.isEmpty then
              TierEngine.prepareEmptyNodeRemoval(rootManager, nodeTier, pred)

            _ <- tx.commit()
          } yield ()
      }.recover {
        // A predecessor that cannot be split to hold the link, or a misconfigured tier, cannot be fixed by retrying,
        // and failing would leave the transaction unfinalized forever. The empty node stays in the tier, which is
        // correct, only slower to scan, and keeps its entry above.
        case err: NodeSizeExceedsAllocatorLimit =>
          logger.error(s"Leaving empty TKVL node ${tailNode.id} in tier $nodeTier: ${err.getMessage}")
        case _: NodeSizeExceeded =>
          logger.error(s"Leaving empty TKVL node ${tailNode.id} in tier $nodeTier: its predecessor cannot hold the " +
            "link to the node's right neighbor")
      }

    Future.delegate(read(tailNode, tailMinimum)).map(Some(_)).recover {
      case _: InvalidObject => None
    }.flatMap {
      case Some(tail) if tail.entries.isEmpty => unlink(tail)
      case _ => Future.unit
    }
  }
}

object TailRemovalFinalizationAction extends RegisteredTypeFactory with FinalizationActionFactory {
  val typeUUID: UUID = UUID.fromString("A72B6D85-D87E-45B7-AC58-9CE6E4DE6AFB")

  def createFinalizationAction(client: AspenClient,
                               txd: TransactionDescription,
                               data: Array[Byte]): FinalizationAction = {
    val (rootManager, tier, minimum, ptr) = JoinFinalizationAction.decode(client, data)
    new TailRemovalFinalizationAction(client, txd, rootManager, tier, minimum, ptr)
  }

  def addToTransaction(mgr: RootManager,
                       tier: Int,
                       tailMinimum: Key,
                       tailNode: ObjectPointer,
                       tx: Transaction): Unit =
    tx.addFinalizationAction(FinalizationActionId(typeUUID),
      Some(JoinFinalizationAction.encode(mgr, tier, tailMinimum, tailNode)))
}
