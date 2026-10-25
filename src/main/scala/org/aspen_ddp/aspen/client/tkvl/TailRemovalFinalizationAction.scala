package org.aspen_ddp.aspen.client.tkvl

import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory, InvalidObject, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectPointer}
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, TransactionDescription}

import scala.concurrent.{ExecutionContext, Future, Promise}

/** Removes an emptied rightmost node from its tier.
  *
  * A join absorbs a node's right neighbor, so the rightmost node of a tier can never be joined away and is left empty
  * when its last key is deleted. Such a node keeps its entry in the tier above, which stops tier reduction. This action
  * unlinks the empty node from its left neighbor, drops its refcount and, in the same transaction, adds a
  * `JoinFinalizationAction` that removes its entry from the tier above (reducing tiers if that empties the root).
  *
  * `tier` is the tier of that entry, one above the node's own, matching `JoinFinalizationAction`.
  *
  * Nothing is done if the node is gone, is no longer empty or is no longer the last node of its tier. Because the
  * node's revision is locked, a racing insert into it fails this transaction and the retry re-checks.
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
          case Some(rootNode) if tier <= rootTier => removeTail(rootTier, ordering, rootNode)
          case _ => Future.unit // The tree shrank past this node
        }
      }
    }

    completionPromise.completeWith(fcomplete)
  }

  private def removeTail(rootTier: Int, ordering: KeyOrdering, rootNode: ListNode): Future[Unit] = {
    val encoding = TierEngine.encodingFor(rootNode)
    val nodeTier = tier - 1

    def read(pointer: ObjectPointer, minimum: Key): Future[ListNode] =
      encoding.read(client, pointer, minimum, ordering, s"Removing empty tail node ${tailNode.id}")

    // The node whose link is the tail, or None if nothing links to it
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

          encoding.unlinkRight(pred)
          tx.lockRevision(tail.pointer, tail.revision)
          tx.setRefcount(tail.pointer, tail.refcount, tail.refcount.decrement())

          JoinFinalizationAction.addToTransaction(rootManager, tier, tailMinimum, tailNode, tx)

          // The predecessor is now the last node of the tier, so it too is removable if it was already empty
          if pred.entries.isEmpty then
            TierEngine.prepareTailRemoval(rootManager, nodeTier, pred)

          tx.commit().map(_ => ())
      }

    Future.delegate(read(tailNode, tailMinimum)).map(Some(_)).recover {
      case _: InvalidObject => None
    }.flatMap {
      case Some(tail) if tail.entries.isEmpty && tail.link.isEmpty => unlink(tail)
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
