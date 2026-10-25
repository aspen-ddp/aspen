package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject, ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectId, ObjectPointer, ObjectRevision, Value}

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

  def set(key: Key,
          value: Value,
          requirement: Option[Either[Boolean, ObjectRevision]] = None)
         (using t: Transaction): Future[Unit] =

    def onSplit(newMinimum: Key, newNode: ObjectPointer): Future[Unit] =
      SplitFinalizationAction.addToTransaction(rootManager, 1, newMinimum, newNode, t)
      Future.unit

    def empty(): Future[Unit] =
      // `requirement` is intentionally dropped. createInitialNode only commits
      // if the tree is still empty, in which case the key cannot exist and a
      // DoesNotExist requirement is already satisfied.
      rootManager.createInitialNode(Map(key -> value)).map(_=>())

    def nonEmpty(tier: Int, ordering: KeyOrdering, root: ListNode): Future[Unit] =
      val encoding = encodingFor(root)
      for
        alloc <- rootManager.getAllocatorForTier(0)
        maxNodeSize <- rootManager.getMaxNodeSize(0)

        e <- fetchContainingNode(client, tier, 0, ordering, key, root, Set())

        node = e match
          case Left(_) => throw new BrokenTree()
          case Right(n) => n

        _ <- encoding.insert(node, key, value, maxNodeSize, alloc, onSplit, requirement)
      yield
        ()

    rootManager.getRootNode().flatMap: (tier, ordering, oroot) =>
      oroot match
        case None => empty()
        case Some(root) => nonEmpty(tier, ordering, root)

  def delete(key: Key)(using t: Transaction): Future[Unit] =
    def onJoin(delMinimum: Key, delNode: ObjectPointer): Future[Unit] =
      JoinFinalizationAction.addToTransaction(rootManager, 1, delMinimum, delNode, t)
      Future.unit
    deleteFromContainingNode(key, node => encodingFor(node).delete(node, key, onJoin))

  /** Descends to the tier-0 node containing `key` and hands it to `del`.
   *
   *  Deleting a node's last key joins it with its right neighbour, which `del` reads. If a racing
   *  join already absorbed that neighbour, the containing node was read before the join and the
   *  read fails with InvalidObject. `del` adds nothing to the transaction before that read, so
   *  the delete restarts from a fresh root.
   */
  def deleteFromContainingNode(key: Key, del: ListNode => Future[Unit]): Future[Unit] =

    def attempt(restarts: Int): Future[Unit] =
      rootManager.getRootNode().flatMap: (tier, ordering, oroot) =>
        oroot match
          case None => Future.unit
          case Some(root) =>
            for
              e <- fetchContainingNode(client, tier, 0, ordering, key, root, Set())
              node = e match
                case Left(_) => throw new BrokenTree()
                case Right(n) => n
              _ <- del(node).recoverWith:
                case _: InvalidObject if restarts < MaxJoinRaceRestarts => attempt(restarts + 1)
            yield
              ()

    attempt(0)

  /** Deletes every node of the tree, then updates the root to an empty tree. `deleteDataList` deletes the tier-0
    * list starting at the leftmost tier-0 node it is handed, which lets a facade visit each entry in its own
    * terms. Upper tiers are deleted through the tree's encoding.
    */
  def deleteTree(deleteDataList: ListNode => Future[Unit]): Future[Unit] =

    def readOptional(encoding: NodeEncoding, pointer: ObjectPointer, ordering: KeyOrdering): Future[Option[ListNode]] =
      Future.delegate(encoding.read(client, pointer, Key.AbsoluteMinimum, ordering, "")).map(Some(_)).recover:
        case _: InvalidObject => None

    def fetchRootNodes(encoding: NodeEncoding, path: List[(Int, ListNode)]): Future[List[(Int, ListNode)]] =
      val (tier, node) = path.head
      if tier == 0 then
        Future.successful(path)
      else
        node.get(Key.AbsoluteMinimum) match
          case None => Future.successful(path)
          case Some(v) =>
            readOptional(encoding, ObjectPointer(v.bytes), node.ordering).flatMap:
              case None => Future.successful(path)
              case Some(downNode) =>
                val nextTierDown = tier - 1
                fetchRootNodes(encoding, (nextTierDown, downNode) :: path)

    def rdelete(encoding: NodeEncoding, path: List[(Int, ListNode)]): Future[Unit] =
      if path.isEmpty then
        Future.unit
      else
        val (tier, node) = path.head

        val fdelete =
          if tier == 0 then
            deleteDataList(node)
          else
            encoding.deleteList(client, NodeLink(Key.AbsoluteMinimum, node.pointer), node.ordering, None)

        for
          _ <- fdelete
          _ <- rdelete(encoding, path.tail)
        yield
          ()

    for
      (tier, ordering, onode) <- rootManager.getRootNode()

      _ <- onode match
        case None => Future.unit
        case Some(node) =>
          val encoding = encodingFor(node)
          for
            path <- fetchRootNodes(encoding, List((tier, node)))
            _ <- rdelete(encoding, path)
            tx = client.newTransaction()
            _ <- rootManager.prepareRootUpdate(0, None)(using tx)
            _ <- tx.commit()
          yield
            ()
    yield
      ()

  /** See `TieredKeyValueList.splitTree` */
  def splitTree(splitAtKey: Key, inclusive: Boolean = false)
               (using t: Transaction): Future[Root] =

    // Nodes above tier 0 on the descent path need not hold splitAtKey, since navigation between
    // tiers is not consistent, so each split first scans right to the node that does.
    def splitContaining(encoding: NodeEncoding,
                        node: ListNode,
                        odown: Option[ObjectPointer],
                        alloc: ObjectAllocator): Future[NodeLink] =
      scanToContainingNode(client, encoding, node, splitAtKey).flatMap: containing =>
        encoding.splitAt(containing, splitAtKey, inclusive, odown, alloc)

    def rinsert(encoding: NodeEncoding,
                tier: Int,
                rpath: List[ListNode],
                downPointer: ObjectPointer): Future[ObjectPointer] =
      if rpath.isEmpty then
        Future.successful(downPointer)
      else
        val odown = if tier > 0 then Some(downPointer) else None
        for
          alloc <- rootManager.getAllocatorForTier(tier)
          down <- splitContaining(encoding, rpath.head, odown, alloc)
          kvp <- rinsert(encoding, tier + 1, rpath.tail, down.pointer)
        yield
          kvp

    // Returns the new tree's tier along with its root. The path has one node per tier, and it
    // can be a tier shorter than `tier` if a tier reduction raced the descent.
    def nonEmpty(encoding: NodeEncoding,
                 tier: Int,
                 ordering: KeyOrdering,
                 root: ListNode): Future[(Int, ObjectPointer)] =
      for
        e <- fetchContainingNodePath(client, tier, 0, ordering, splitAtKey, root, Set())

        rpath = e match
          case Left(_) => throw new BrokenTree()
          case Right(p) => p

        // There will always be a tier0 entry, split that then recurse through
        // the full path
        alloc <- rootManager.getAllocatorForTier(0)

        down <- splitContaining(encoding, rpath.head, None, alloc)

        kvp <- rinsert(encoding, 1, rpath.tail, down.pointer)
      yield
        (rpath.length - 1, kvp)

    for
      oldRoot <- rootManager.getRoot()
      encoding = NodeEncoding(oldRoot.kind)
      (tier, ordering, oroot) <- rootManager.getRootNode()
      (newTier, ptr) <- oroot match
        case Some(root) => nonEmpty(encoding, tier, ordering, root)
        case None =>
          for
            _ <- rootManager.createInitialNode(Map())
            alloc <- rootManager.getAllocatorForTier(0)
            ptr <- encoding.allocate(alloc, Map[Key, Value]())
          yield
            (tier, ptr)
    yield
      Root(newTier, ordering, Some(ptr), oldRoot.nodeAllocator, oldRoot.kind)

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
