package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject}
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectId, ObjectPointer, Value}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** The tree logic shared by every `TreeKind`: descent through the tiers and the tree-level operations built on it.
  * Works only with `ListNode` and `ObjectPointer`; everything that depends on how nodes are stored goes through
  * the `NodeEncoding` of the tree's kind. The kind comes from the root node's pointer (see `TreeKind.of`), which
  * `Root` guarantees matches `Root.kind`.
  *
  * The kind-specific facades (`TieredKeyValueList`) sit on top of this and convert nodes and entries to the types
  * their callers see.
  */
private[tkvl] class TierEngine(val client: AspenClient, val rootManager: RootManager):

  given ExecutionContext = client.clientContext

  import TierEngine.*

  /** Descends to the tier-0 node containing `key` and hands it to `f`. An empty tree yields `empty`. */
  private def withContainingNode[A](key: Key, empty: => A)(f: ListNode => Future[A]): Future[A] =
    rootManager.getRootNode().flatMap: (tier, ordering, oroot) =>
      oroot match
        case None => Future.successful(empty)
        case Some(root) =>
          fetchContainingNode(client, tier, 0, ordering, key, root, Set()).flatMap:
            case Left(_) => throw new BrokenTree()
            case Right(node) => f(node)

  /** The tier-0 node whose range holds `key`, or None for an empty tree */
  def getContainingNode(key: Key): Future[Option[ListNode]] =
    withContainingNode(key, None)(node => Future.successful(Some(node)))

  def get(key: Key): Future[Option[Value]] =
    getContainingNode(key).map(_.flatMap(_.get(key)))

  /** Hands the leftmost tier-0 node to `fold`, which walks the tier rightward. An empty tree yields `z`. */
  def foldLeft[B](z: B)(fold: ListNode => Future[B]): Future[B] =
    withContainingNode(Key.AbsoluteMinimum, z)(fold)

  /** Descends to the tier-0 node containing `startKey` and hands it to `walk`.
   *
   *  The tier-0 walks of the facades differ only in which node-level method they delegate to;
   *  sharing the descent keeps that the only difference between them. Each keeps its own name
   *  so the node-level diagnostics still report the method the caller invoked.
   */
  def walkFrom(startKey: Key, walk: ListNode => Future[Unit]): Future[Unit] =
    withContainingNode(startKey, ())(walk)

object TierEngine:

  /** The encoding of the tree `node` belongs to */
  private[tkvl] def encodingFor(node: ListNode): NodeEncoding = NodeEncoding(TreeKind.of(node.pointer))

  /** How many times a traversal restarts after reading a node that a racing join deleted.
   *
   *  A restart only fails again if yet another join races it, so exhausting this means the
   *  tree really does point at a deleted node.
   */
  private[tkvl] val MaxJoinRaceRestarts = 10

  /*
   Returns Left if the tree is broken or Right with the node on targetTier whose range holds target
   */
  private[tkvl] def fetchContainingNode(client: AspenClient,
                                        currentTier: Int,
                                        targetTier: Int,
                                        ordering: KeyOrdering,
                                        target: Key,
                                        currentNode: ListNode,
                                        initialBlacklist: Set[ObjectId],
                                        reversePath: List[ListNode] = Nil): Future[Either[Set[ObjectId], ListNode]] =
    given ExecutionContext = client.clientContext

    fetchContainingNodePath(client, currentTier, targetTier, ordering, target,
      currentNode, initialBlacklist).map:
      case Left(l) => Left(l)
      case Right(rpath) => Right(rpath.head)

  /*
   Returns Left if the tree is broken or Right with a list of nodes for each
   tier in the path down to the requested node in reverse order. The top of
   the tree will be the last node in the list

   A join deletes the right-hand node in the same transaction that rewrites the left node's
   right pointer, so a right scan that read the left node just before the join reads a deleted
   node and fails with InvalidObject. Upper-tier reads are already tolerant of this (fetchNode
   blacklists them), so any InvalidObject here comes from the target-tier scan. currentNode is
   the leftmost node of its tier, which a join never deletes, so refreshing it and descending
   again sees the list as the join left it.

   Tier reduction does delete the root, though, so the refresh fails if one ran after the caller
   read it. The deleted root held only its AbsoluteMinimum child, which became the new root, so
   the restart descends from that child one tier down instead.
   */
  private[tkvl] def fetchContainingNodePath(client: AspenClient,
                                            currentTier: Int,
                                            targetTier: Int,
                                            ordering: KeyOrdering,
                                            target: Key,
                                            currentNode: ListNode,
                                            initialBlacklist: Set[ObjectId],
                                            reversePath: List[ListNode] = Nil): Future[Either[Set[ObjectId], List[ListNode]]] =

    given ExecutionContext = client.clientContext

    def leftmostChild(encoding: NodeEncoding, stale: ListNode): Future[ListNode] =
      stale.get(Key.AbsoluteMinimum) match
        case None => Future.failed(new BrokenTree())
        case Some(v) =>
          encoding.read(client, ObjectPointer(v.bytes), Key.AbsoluteMinimum, ordering,
            s"TKVL restart below deleted root ${stale.pointer.id}")

    def refresh(encoding: NodeEncoding, node: ListNode): Future[ListNode] =
      encoding.read(client, node.pointer, node.minimum, ordering,
        s"Refresh TKVL node ${node.pointer.id}. Minimum: ${node.minimum}")

    def attempt(encoding: NodeEncoding,
                tier: Int,
                start: ListNode,
                restarts: Int): Future[Either[Set[ObjectId], List[ListNode]]] =
      descend(client, encoding, tier, targetTier, ordering, target, start, initialBlacklist, reversePath).recoverWith:
        case _: InvalidObject if restarts < MaxJoinRaceRestarts =>
          refresh(encoding, start).map(n => (tier, n)).recoverWith:
            case _: InvalidObject if tier > targetTier => leftmostChild(encoding, start).map(n => (tier - 1, n))
          .flatMap((t, n) => attempt(encoding, t, n, restarts + 1))

    Future.delegate(attempt(encodingFor(currentNode), currentTier, currentNode, 0))

  /** Scans right from `node` to the node of its tier whose range holds `target`, or the last node of the tier.
    * Fails with `BelowMinimumError` if `target` is below `node`'s range.
    */
  private[tkvl] def scanToContainingNode(client: AspenClient,
                                         encoding: NodeEncoding,
                                         node: ListNode,
                                         target: Key): Future[ListNode] =
    given ExecutionContext = client.clientContext

    def scan(n: ListNode): Future[ListNode] = n.link match
      case None => Future.successful(n)
      case Some(right) =>
        if n.keyInRange(target) then
          Future.successful(n)
        else
          encoding.read(client, right.pointer, right.minimum, n.ordering,
            s"Scanning right TKVL node ${right.pointer.id}. Minimum: ${right.minimum}. target: $target").flatMap(scan)

    // exit immediately if the requested key is below the minimum range
    if node.ordering.compare(target, node.minimum) < 0 then
      Future.failed(new BelowMinimumError(node.minimum, target))
    else
      scan(node)

  private def descend(client: AspenClient,
                      encoding: NodeEncoding,
                      currentTier: Int,
                      targetTier: Int,
                      ordering: KeyOrdering,
                      target: Key,
                      currentNode: ListNode,
                      initialBlacklist: Set[ObjectId],
                      reversePath: List[ListNode]): Future[Either[Set[ObjectId], List[ListNode]]] = {

    given ExecutionContext = client.clientContext

    if (currentTier == targetTier) {
      // Once we're on the right tier, we can rely on consistent right pointers to scan to the
      // containing node
      scanToContainingNode(client, encoding, currentNode, target).map(n => Right(n :: reversePath))
    } else {

      val p = Promise[Either[Set[ObjectId], List[ListNode]]]()

      def rtry(candidates: List[(Key, ObjectPointer)], blacklist: Set[ObjectId]): Unit = {
        if (candidates.isEmpty) {
          p.success(Left(blacklist + currentNode.pointer.id))
        } else {
          fetchNode(client, encoding, ordering, targetTier, candidates.head._1, candidates.head._2, blacklist) foreach {
            case Left(blklst) => rtry(candidates.tail, blklst)
            case Right(next) => descend(client, encoding, currentTier-1, targetTier, ordering, target,
              next, blacklist, currentNode :: reversePath).onComplete {
              case Failure(err) => p.failure(err)
              case Success(Left(blklst)) => p.success(Left(blklst + next.pointer.id))
              case Success(Right(rpath)) => p.success(Right(rpath))
            }
          }
        }
      }

      val initialCandidates = currentNode.entries.iterator.
        filter(t => ordering.compare(t._1, target) <= 0).
        map(t => t._1 -> ObjectPointer(t._2.bytes)).
        filter(t => !initialBlacklist.contains(t._2.id)).
        toList.
        sortWith((l, r) => ordering.compare(l._1, r._1) < 0).
        reverse

      rtry(initialCandidates, initialBlacklist)

      p.future
    }

  }

  private def fetchNode(client: AspenClient,
                        encoding: NodeEncoding,
                        ordering: KeyOrdering,
                        targetTier: Int,
                        minimum: Key,
                        pointer: ObjectPointer,
                        blacklist: Set[ObjectId]): Future[Either[Set[ObjectId], ListNode]] = {

    given ExecutionContext = client.clientContext

    if (blacklist.contains(pointer.id)) {
      Future.successful(Left(blacklist))
    } else {
      val p = Promise[Either[Set[ObjectId], ListNode]]()

      Future.delegate(encoding.read(client, pointer, minimum, ordering,
        s"TKVL read of tier $targetTier, key: $minimum")) onComplete {
        case Failure(_) => p.success(Left(blacklist + pointer.id))
        case Success(node) => p.success(Right(node))
      }

      p.future
    }
  }
