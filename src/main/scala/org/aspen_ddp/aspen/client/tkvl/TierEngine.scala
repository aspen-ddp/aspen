package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AspenClient, InvalidObject, ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectId, ObjectPointer, ObjectRevision, Value}

import scribe.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** The tree logic shared by every `TreeKind`: descent through the tiers and the tree-level operations built on it.
  * Works only with `ListNode` and `ObjectPointer`; everything that depends on how nodes are stored goes through
  * the `NodeEncoding` of the tree's kind. The kind comes from the root node's pointer (see `TreeKind.of`), which
  * `Root` guarantees matches `Root.kind`.
  *
  * The kind-specific facades (`TieredKeyValueList`, `DataObjectTKVL`) sit on top of this and convert nodes and
  * entries to the types their callers see.
  */
private[tkvl] class TierEngine(val client: AspenClient, val rootManager: RootManager) extends Logging:

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

  /** Hands every tier-0 node to `fn`, left to right, starting from `z`. An empty tree yields `z`. A failing read
    * or a throwing `fn` fails the result.
    */
  def foldLeft[B](z: B)(fn: (B, ListNode) => B): Future[B] =
    def fold(acc: B, node: ListNode): Future[B] =
      val next = fn(acc, node)
      node.link match
        case None => Future.successful(next)
        case Some(link) => readRight(node, link, "foldLeft").flatMap(fold(next, _))

    withContainingNode(Key.AbsoluteMinimum, z)(node => Future.delegate(fold(z, node)))

  /** Visits every entry in the tree, in ascending key order. Exactly `foreachFrom(Key.AbsoluteMinimum, fn)`:
    * every KeyOrdering compares every key as greater than or equal to the empty key.
    *
    * A failing `fn`, whether it returns a failed future or throws, is logged and the walk continues to the next
    * key. A failing read fails the returned future. `fn` is handed the node holding the key.
    */
  def foreach(fn: (ListNode, Key, Value) => Future[Unit]): Future[Unit] =
    walk(Key.AbsoluteMinimum, "foreach", (_, _) => true, (_, _) => false, fn)

  /** Visits every entry at or above `minKey`, in ascending key order. The bound is inclusive. See `foreach`. */
  def foreachFrom(minKey: Key, fn: (ListNode, Key, Value) => Future[Unit]): Future[Unit] =
    walk(minKey, "foreachFrom", (_, _) => true, (_, _) => false, fn)

  /** Visits every entry in `[minKey, maxKey)`, in ascending key order. A node whose minimum is at or above `maxKey`
    * holds nothing in range and neither does anything to its right, so the walk ends without reading it.
    * See `foreach`.
    */
  def foreachInRange(minKey: Key, maxKey: Key, fn: (ListNode, Key, Value) => Future[Unit]): Future[Unit] =
    walk(minKey, "foreachInRange", (k, o) => o.compare(k, maxKey) < 0, (m, o) => o.compare(maxKey, m) <= 0, fn)

  /** Shared body of the entry walks. Descends to the tier-0 node holding `minKey` and walks right from it, handing
    * `fn` each entry at or above `minKey` for which `below` holds. The walk ends at the end of the tier or before
    * reading a node whose minimum satisfies `past`. `label` names the walk the caller invoked, in read diagnostics
    * and the `fn`-failure log.
    *
    * Every node's entries are filtered by `minKey`, not just the first node's, so the walk stays correct however
    * it is started.
    */
  private def walk(minKey: Key,
                   label: String,
                   below: (Key, KeyOrdering) => Boolean,
                   past: (Key, KeyOrdering) => Boolean,
                   fn: (ListNode, Key, Value) => Future[Unit]): Future[Unit] =

    def inRange(node: ListNode): List[(Key, Value)] =
      node.entries.filter((k, _) => node.ordering.compare(k, minKey) >= 0 && below(k, node.ordering))

    // The only place a node is paired with a list of entries, so the walk has nowhere to pair a node with some
    // other node's entries.
    def advance(node: ListNode): Future[Unit] = visit(node, inRange(node))

    def visit(node: ListNode, entries: List[(Key, Value)]): Future[Unit] = entries match
      case (key, value) :: rest =>
        Future.delegate(fn(node, key, value)).transformWith:
          case Failure(err) =>
            logger.error(s"Failure in TKVL $label: $err", err)
            visit(node, rest)
          case Success(_) =>
            visit(node, rest)

      case Nil =>
        node.link match
          case None => Future.unit
          case Some(link) =>
            if past(link.minimum, node.ordering) then
              Future.unit
            else
              readRight(node, link, label).flatMap(advance)

    withContainingNode(minKey, ())(advance)

  /** Reads the node `link` points to, the right neighbour of `node`. `label` names the walk, for diagnostics. */
  private def readRight(node: ListNode, link: NodeLink, label: String): Future[ListNode] =
    encodingFor(node).read(client, link.pointer, link.minimum, node.ordering,
      s"$label() TKVL node ${node.pointer.id}. Minimum: ${node.minimum}.")

  def set(key: Key,
          value: Value,
          requirement: Option[Either[Boolean, ObjectRevision]] = None)
         (using t: Transaction): Future[Unit] =
    prepareUpdates(List(BatchOp.Put(key, value, requirement)))

  def delete(key: Key)(using t: Transaction): Future[Unit] =
    prepareUpdates(List(BatchOp.Remove(key)))

  /** Applies `ops`, at most one per key, to the tree in `t`. Each tier-0 node is updated once, so any number of keys
    * sharing a node can change together. The ops are split among the down pointers of each node on the way down and
    * the subtrees are descended in parallel, then each tier-0 node's ops are applied by `ListBatch`. A failure once an
    * earlier node is staged aborts `t`.
    *
    * An empty tree gets its initial node holding the puts. Their requirements are dropped and deletes have nothing to
    * delete: createInitialNode only commits if the tree is still empty, in which case no key can exist.
    */
  private[tkvl] def prepareUpdates(ops: List[BatchOp])(using t: Transaction): Future[Unit] =

    def onSplit(newMinimum: Key, newNode: ObjectPointer): Future[Unit] =
      SplitFinalizationAction.addToTransaction(rootManager, 1, newMinimum, newNode, t)
      Future.unit

    def onJoin(delMinimum: Key, delNode: ObjectPointer): Future[Unit] =
      JoinFinalizationAction.addToTransaction(rootManager, 1, delMinimum, delNode, t)
      Future.unit

    def empty(): Future[Unit] =
      val puts = ops.collect { case p: BatchOp.Put => p.key -> p.value }
      if puts.isEmpty then
        Future.unit
      else
        rootManager.createInitialNode(puts.toMap)

    // Every read of the batch. A racing join can delete a node read along the way, which fails the read with
    // InvalidObject. Nothing is staged yet, so the batch restarts from a fresh root.
    def locateTargets(restarts: Int): Future[Option[List[ListBatch.Target]]] =
      rootManager.getRootNode().flatMap: (tier, ordering, oroot) =>
        oroot match
          case None => Future.successful(None)
          case Some(root) =>
            val fetched = for
              located <- locate(tier, root, BatchOp.sorted(ops, ordering))
              _ = if located.unplaced.nonEmpty then throw new BrokenTree()
              targets <- ListBatch.withJoinSources(client, located.assigned(ordering), located.read)
            yield
              Some(targets)
            fetched.recoverWith:
              case _: InvalidObject if restarts < MaxJoinRaceRestarts => locateTargets(restarts + 1)

    if ops.isEmpty then
      Future.unit
    else
      locateTargets(0).flatMap:
        case None => empty()
        case Some(targets) =>
          for
            alloc <- rootManager.getAllocatorForTier(0)
            maxNodeSize <- rootManager.getMaxNodeSize(0)
            emptied <- ListBatch.stage(targets, maxNodeSize, alloc, onSplit, onJoin)
          yield
            emptied.foreach(node => prepareEmptyNodeRemoval(rootManager, 0, node))

  /** Finds the tier-0 node holding each of `ops`, ascending and all at or above `node`'s minimum, descending from
    * `node` on `tier`. On an upper tier, each op goes to the greatest entry at or below its key and each entry's ops
    * descend in parallel. A child that cannot be read, or below which some ops cannot be placed, hands those ops to
    * the entry to its left, as `descend` falls back to the next candidate; ops with no entry left to try are returned
    * unplaced. On tier 0, `ListBatch.assign` scans right from the node.
    */
  private def locate(tier: Int, node: ListNode, ops: List[BatchOp]): Future[Located] =
    if tier == 0 then
      ListBatch.assign(client, node, ops).map: (assigned, read) =>
        Located(assigned, read.iterator.map(n => n.pointer.id -> n).toMap, Nil)
    else
      val encoding = encodingFor(node)
      val ordering = node.ordering
      val entries = node.entries.map((k, v) => k -> ObjectPointer(v.bytes)).toIndexedSeq

      // Candidates for one entry's ops: the entry, then every entry to its left, as `descend` tries them
      def viaCandidates(candidates: List[(Key, ObjectPointer)],
                        ops: List[BatchOp],
                        blacklist: Set[ObjectId]): Future[Located] = candidates match
        case Nil => Future.successful(Located(Nil, Map(), ops))
        case (minimum, pointer) :: rest =>
          fetchNode(client, encoding, ordering, tier - 1, minimum, pointer, blacklist).flatMap:
            case Left(bl) => viaCandidates(rest, ops, bl)
            case Right(child) =>
              locate(tier - 1, child, ops).flatMap: located =>
                if located.unplaced.isEmpty then
                  Future.successful(located)
                else
                  viaCandidates(rest, located.unplaced, blacklist + child.pointer.id)
                    .map(located.copy(unplaced = Nil).merge)

      // The index of the greatest entry at or below each op's key, or -1 for none
      def entryIndex(key: Key): Int = entries.lastIndexWhere(e => ordering.compare(e._1, key) <= 0)

      val groups = ops.groupBy(op => entryIndex(op.key)).toList
      val unplaced = groups.collect { case (-1, gops) => gops }.flatten

      Future.traverse(groups.filter(_._1 >= 0)): (index, gops) =>
        viaCandidates(entries.take(index + 1).reverse.toList, BatchOp.sorted(gops, ordering), Set())
      .map(_.foldLeft(Located(Nil, Map(), unplaced))((a, b) => a.merge(b)))

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
                        alloc: ObjectAllocator,
                        maxNodeSize: Int): Future[NodeLink] =
      scanToContainingNode(client, encoding, node, splitAtKey).flatMap: containing =>
        encoding.splitAt(containing, splitAtKey, inclusive, maxNodeSize, odown, alloc)

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
          maxNodeSize <- rootManager.getMaxNodeSize(tier)
          down <- splitContaining(encoding, rpath.head, odown, alloc, maxNodeSize)
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
        maxNodeSize <- rootManager.getMaxNodeSize(0)

        down <- splitContaining(encoding, rpath.head, None, alloc, maxNodeSize)

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
            ptr <- encoding.allocate(alloc, ordering, Map[Key, Value]())
          yield
            (tier, ptr)
    yield
      Root(newTier, ordering, Some(ptr), oldRoot.nodeAllocator, oldRoot.kind)

object TierEngine:

  /** Where a batch's ops landed: the tier-0 nodes holding them, every tier-0 node read and the ops that could not be
    * placed. Stale upper tiers can lead two groups of ops to the same tier-0 node, so `assigned` merges them.
    */
  private final case class Located(found: List[ListBatch.Assigned],
                                   read: Map[ObjectId, ListNode],
                                   unplaced: List[BatchOp]):

    def merge(other: Located): Located =
      Located(found ++ other.found, read ++ other.read, unplaced ++ other.unplaced)

    /** One entry per node, left to right, the first read of a node kept */
    def assigned(ordering: KeyOrdering): List[ListBatch.Assigned] =
      found.groupBy(_.node.pointer.id).values.toList
        .map(as => ListBatch.Assigned(as.head.node, BatchOp.sorted(as.flatMap(_.ops), ordering)))
        .sortWith((a, b) => ordering.compare(a.node.minimum, b.node.minimum) < 0)

  /** Registers a `TailRemovalFinalizationAction` for an empty `node` that is not the head of its tier (a tier's head
    * has the absolute minimum and is never removed), to remove it from its list. `tier` is `node`'s own. Call after
    * the delete that emptied it is staged. The action does nothing if the node is no longer empty by then.
    */
  private[tkvl] def prepareEmptyNodeRemoval(rootManager: RootManager, tier: Int, node: ListNode)
                                           (using tx: Transaction): Unit =
    if node.ordering.compare(node.minimum, Key.AbsoluteMinimum) != 0 then
      TailRemovalFinalizationAction.addToTransaction(rootManager, tier + 1, node.minimum, node.pointer, tx)

  /** The encoding of the tree `node` belongs to */
  private[tkvl] def encodingFor(node: ListNode): NodeEncoding = NodeEncoding(TreeKind.of(node.pointer))

  /** How many times a traversal restarts after reading a node that a racing join deleted.
   *
   *  A restart only fails again if yet another join races it, so exhausting this means the
   *  tree really does point at a deleted node.
   */
  private[tkvl] val MaxJoinRaceRestarts = 10

  /*
   Returns Left if the tree is broken or Right with the node on targetTier whose range holds target.

   With linksTo, which names a node on targetTier whose minimum is target, this instead finds the node on
   targetTier immediately to its left. See fetchPredecessor.
   */
  private[tkvl] def fetchContainingNode(client: AspenClient,
                                        currentTier: Int,
                                        targetTier: Int,
                                        ordering: KeyOrdering,
                                        target: Key,
                                        currentNode: ListNode,
                                        initialBlacklist: Set[ObjectId],
                                        reversePath: List[ListNode] = Nil,
                                        linksTo: Option[ObjectId] = None): Future[Either[Set[ObjectId], ListNode]] =
    given ExecutionContext = client.clientContext

    fetchContainingNodePath(client, currentTier, targetTier, ordering, target,
      currentNode, initialBlacklist, reversePath, linksTo).map:
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
                                            reversePath: List[ListNode] = Nil,
                                            linksTo: Option[ObjectId] = None): Future[Either[Set[ObjectId], List[ListNode]]] =

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
      descend(client, encoding, tier, targetTier, ordering, target, start, initialBlacklist, reversePath, linksTo)
        .recoverWith:
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

  /** Scans right from `node`, whose minimum must be below `target`, for the node whose link points at `linkedId`.
    * `target` is that linked node's minimum. Returns the node that links to it or, when nothing in the scanned range
    * does (it was already unlinked), the last node scanned.
    */
  private def scanToLinkingNode(client: AspenClient,
                                encoding: NodeEncoding,
                                node: ListNode,
                                target: Key,
                                linkedId: ObjectId): Future[ListNode] =
    given ExecutionContext = client.clientContext

    def scan(n: ListNode): Future[ListNode] = n.link match
      case Some(right) if right.pointer.id != linkedId && n.ordering.compare(right.minimum, target) < 0 =>
        encoding.read(client, right.pointer, right.minimum, n.ordering,
          s"Scanning right TKVL node ${right.pointer.id} for the node linking to $linkedId").flatMap(scan)
      case _ => Future.successful(n)

    scan(node)

  /** The node on `nodeTier` whose link points at `linkedNode`, whose minimum is `linkedMinimum`. None if no node links
    * to it, as when it is the head of its tier or was already unlinked. Left if the search ran out of candidates in the
    * tiers above, as when racing joins deleted the nodes it was following, and the caller has to look from the head of
    * `nodeTier` instead.
    *
    * Descends through the tiers above `nodeTier` like a lookup of `linkedMinimum`, except that it takes the entry
    * strictly below `linkedMinimum`, so the work is a descent plus a short scan right instead of a walk of the tier.
    */
  private[tkvl] def fetchPredecessor(client: AspenClient,
                                     rootTier: Int,
                                     nodeTier: Int,
                                     ordering: KeyOrdering,
                                     linkedMinimum: Key,
                                     linkedNode: ObjectId,
                                     rootNode: ListNode): Future[Either[Set[ObjectId], Option[ListNode]]] =
    given ExecutionContext = client.clientContext

    fetchContainingNode(client, rootTier, nodeTier, ordering, linkedMinimum, rootNode, Set(),
      linksTo = Some(linkedNode)).map:
      case Left(l) => Left(l)
      case Right(n) => Right(n.link.filter(_.pointer.id == linkedNode).map(_ => n))

  private def descend(client: AspenClient,
                      encoding: NodeEncoding,
                      currentTier: Int,
                      targetTier: Int,
                      ordering: KeyOrdering,
                      target: Key,
                      currentNode: ListNode,
                      initialBlacklist: Set[ObjectId],
                      reversePath: List[ListNode],
                      linksTo: Option[ObjectId]): Future[Either[Set[ObjectId], List[ListNode]]] = {

    given ExecutionContext = client.clientContext

    if (currentTier == targetTier) {
      // Once we're on the right tier, we can rely on consistent right pointers to scan to the
      // containing node
      val fscan = linksTo match
        case None => scanToContainingNode(client, encoding, currentNode, target)
        case Some(id) => scanToLinkingNode(client, encoding, currentNode, target, id)
      fscan.map(n => Right(n :: reversePath))
    } else {

      val p = Promise[Either[Set[ObjectId], List[ListNode]]]()

      def rtry(candidates: List[(Key, ObjectPointer)], blacklist: Set[ObjectId]): Unit = {
        if (candidates.isEmpty) {
          p.success(Left(blacklist + currentNode.pointer.id))
        } else {
          fetchNode(client, encoding, ordering, targetTier, candidates.head._1, candidates.head._2, blacklist) foreach {
            case Left(blklst) => rtry(candidates.tail, blklst)
            case Right(next) => descend(client, encoding, currentTier-1, targetTier, ordering, target,
              next, blacklist, currentNode :: reversePath, linksTo).onComplete {
              case Failure(err) => p.failure(err)
              // `next` holds nothing to descend into, as when joins removed its first entries but left its
              // minimum. Fall back to the next candidate to the left: the scan right from below it reaches the
              // target, since navigation within a tier is consistent.
              case Success(Left(blklst)) => rtry(candidates.tail, blklst + next.pointer.id)
              case Success(Right(rpath)) => p.success(Right(rpath))
            }
          }
        }
      }

      // A predecessor search excludes target's own entry so the greatest candidate lies strictly to its left
      val initialCandidates = currentNode.entries.iterator.
        filter(t => if linksTo.isDefined then ordering.compare(t._1, target) < 0 else ordering.compare(t._1, target) <= 0).
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
