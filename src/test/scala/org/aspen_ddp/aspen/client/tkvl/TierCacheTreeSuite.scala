package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, ObjectPointer}

import scala.collection.immutable.TreeMap
import scala.concurrent.Future

/** A tree instance reused across operations caches its upper tiers. These tests run a warm instance against changes
  * made through another instance of the same tree (as another client would make them) and check that the
  * stale cache entries are evicted without changing any result.
  *
  * Every tree starts with keys 1 to 30, which gives three tiers for both kinds with room in the rightmost tier-1 node
  * for one more tier-0 split.
  */
class TierCacheTreeSuite extends TreeKindSuiteBase:

  def nodeSize(kind: TreeKind): Int = kind match
    case TreeKind.KeyValueObjects => 400
    case TreeKind.DataObjects => 200

  private val initial = keys(1 to 30)

  /** A tree holding `initial`, as a warm (default-cached) instance and an uncached one, with their root managers */
  private final case class Fixture(kind: TreeKind,
                                   warm: Tree,
                                   rm: KVObjectRootManager,
                                   other: Tree,
                                   otherRm: KVObjectRootManager):
    def tiers: Future[List[List[ListNode]]] = allTiers(kind, otherRm)

  private def fixture(kind: TreeKind): Future[Fixture] =
    for
      (_, created) <- newTree(kind)
      otherRm = new KVObjectRootManager(client, created.treeKey, created.pointer, NoCache)
      other = open(kind, otherRm)
      _ <- setAll(other, initial)
      tiers <- allTiers(kind, otherRm)
    yield
      assert(tiers.size == 3)
      val rm = new KVObjectRootManager(client, otherRm.treeKey, otherRm.pointer, TierCache.default())
      Fixture(kind, open(kind, rm), rm, other, otherRm)

  private def bytesOf(k: Key): Option[List[Byte]] = Some(value(k.bytes.last).bytes.toList)

  /** Counts the object reads of `f`, once everything outstanding has finished */
  private def counted[T](f: => Future[T]): Future[(T, Map[ObjectId, Int])] =
    waitForTransactionsToComplete().flatMap(_ => net.countReads(f))

  private def holding(nodes: List[ListNode], k: Key): ListNode = nodes.find(_.keyInRange(k)).get

  /** Inserts keys after `from`, one transaction each through the uncached instance, until `split` reports the tree's
    * shape changed. Returns the keys inserted.
    */
  private def insertUntil(f: Fixture, from: Int)(split: List[List[ListNode]] => Boolean): Future[List[Key]] =
    def loop(i: Int, inserted: List[Key]): Future[List[Key]] =
      assert(i < from + 40)
      setAll(f.other, List(Key(i))).flatMap(_ => f.tiers).flatMap: tiers =>
        if split(tiers) then
          Future.successful((Key(i) :: inserted).reverse)
        else
          loop(i + 1, Key(i) :: inserted)
    loop(from, Nil)

  /** Warms the cache through a lookup of the last key, then splits the rightmost tier-0 node through the uncached
    * instance. Returns the rightmost tier-1 node from before the split, which is cached and now stale, and the keys
    * inserted.
    */
  private def staleSplit(f: Fixture): Future[(ListNode, List[Key])] =
    for
      before <- f.tiers
      parent = before(1).last
      _ <- f.warm.get(initial.last)
      _ = assert(f.rm.cache.node(1, parent.pointer.id).isDefined)
      inserted <- insertUntil(f, initial.last.bytes.last + 1)(_(2).size > before(2).size)
      after <- f.tiers
    yield
      // The split only added an entry to the parent, which still has room
      assert(after(1).map(_.pointer.id) == before(1).map(_.pointer.id))
      (parent, inserted)

  /** A fixture whose warm instance is warmed, then the tree deleted through `b`, a second default-cached instance
    * of the same tree, as another client would hold. Returns the fixture and `b`.
    */
  private def deletedElsewhere(kind: TreeKind): Future[(Fixture, Tree)] =
    for
      f <- fixture(kind)
      _ <- f.warm.get(Key(15))
      _ = assert(f.rm.cache.root.isDefined)
      b = open(kind, new KVObjectRootManager(client, f.rm.treeKey, f.rm.pointer, TierCache.default()))
      _ <- b.deleteTree(None)
      _ <- waitForTransactionsToComplete()
    yield
      (f, b)

  for kind <- TreeKind.values do

    atest(s"$kind: a warm lookup reads only tier 0"):
      val k = Key(15)
      for
        f <- fixture(kind)
        _ <- f.warm.get(k)
        (v, reads) <- counted(f.warm.get(k))
        leaves <- tier0Nodes(kind, f.otherRm)
      yield
        assert(v == bytesOf(k))
        assert(reads == Map(holding(leaves, k).pointer.id -> 1))

    atest(s"$kind: a stale cached parent after a split is evicted"):
      for
        f <- fixture(kind)
        (parent, inserted) <- staleSplit(f)
        k = inserted.last
        (v, _) <- counted(f.warm.get(k))
        evicted = f.rm.cache.node(1, parent.pointer.id).isEmpty
        (v2, reads2) <- counted(f.warm.get(k))
        (v3, reads3) <- counted(f.warm.get(k))
        leaves <- tier0Nodes(kind, f.otherRm)
      yield
        assert(v == bytesOf(k))
        assert(evicted)
        assert(v2 == bytesOf(k))
        // The parent is read again and now leads straight to the new node
        assert(reads2 == Map(parent.pointer.id -> 1, holding(leaves, k).pointer.id -> 1))
        assert(v3 == bytesOf(k))
        assert(reads3 == Map(holding(leaves, k).pointer.id -> 1))

    atest(s"$kind: a stale tier-2 node is detected"):
      for
        f <- fixture(kind)
        before <- f.tiers
        // Warming through the first key leaves the rightmost tier-1 node uncached
        _ <- f.warm.get(initial.head)
        cachedRoot = f.rm.cache.root.flatMap(_.node).get
        _ = assert(f.rm.cache.node(1, before(1).last.pointer.id).isEmpty)
        inserted <- insertUntil(f, initial.last.bytes.last + 1)(_(1).size > before(1).size)
        after <- f.tiers
        k = inserted.last
        v <- f.warm.get(k)
      yield
        assert(after.size == 3)
        assert(cachedRoot.pointer.id == after.head.head.pointer.id)
        assert(cachedRoot.entries.size < after.head.head.entries.size)
        assert(v == bytesOf(k))
        assert(f.rm.cache.root.isEmpty)

    atest(s"$kind: a cached parent pointing at a joined-away child is evicted"):
      for
        f <- fixture(kind)
        before <- f.tiers
        parent = before(1).find(_.entries.size >= 3).get
        children = parent.entries.map((_, v) => ObjectPointer(v.bytes).id)
        leaves = before(2).filter(n => children.contains(n.pointer.id))
        (left, joined) = (leaves(0), leaves(1))
        m = joined.entries.head._1
        _ <- f.warm.get(m)
        _ = assert(f.rm.cache.node(1, parent.pointer.id).isDefined)
        _ <- deleteAll(f.other, left.entries.map(_._1))
        joinedExists <- exists(joined.pointer)
        v <- f.warm.get(m)
      yield
        assert(!joinedExists)
        assert(v == bytesOf(m))
        assert(f.rm.cache.node(1, parent.pointer.id).isEmpty)

    atest(s"$kind: a set routed by a stale parent lands correctly"):
      for
        f <- fixture(kind)
        (_, inserted) <- staleSplit(f)
        k = Key(inserted.last.bytes.last + 1)
        _ <- inTransaction(tx => f.warm.set(k, value(k.bytes.last))(using tx))
        _ <- waitForTransactionsToComplete()
        fresh = open(kind, new KVObjectRootManager(client, f.rm.treeKey, f.rm.pointer, NoCache))
        v <- fresh.get(k)
        entries <- fresh.entries()
      yield
        assert(v == bytesOf(k))
        assert(entries == expectedEntries(initial ++ inserted :+ k))

    atest(s"$kind: concurrent operations on one warm instance"):
      for
        f <- fixture(kind)
        (_, inserted) <- staleSplit(f)
        leaves <- tier0Nodes(kind, f.otherRm)
        all = initial ++ inserted
        // One overwrite per tier-0 node, of a value of the same size, so no transaction collides or splits
        setKeys = leaves.map(_.entries.head._1)
        newValue = (k: Key) => value(k.bytes.last + 100)
        getKeys = all.filterNot(setKeys.contains)
        model = TreeMap.from(all.map(k => k -> bytesOf(k).get))(using ordering) ++
          setKeys.map(k => k -> newValue(k).bytes.toList)
        // Each set between two gets
        ops = getKeys.grouped(2).toList.zipAll(setKeys.map(k => List(Right(k))), Nil, Nil)
          .flatMap((gs, s) => gs.map(Left(_)) ++ s)
        results <- Future.sequence(ops.map:
          case Left(k) => f.warm.get(k).map(v => Some(k -> v))
          case Right(k) => inTransaction(tx => f.warm.set(k, newValue(k))(using tx)).map(_ => None))
        _ <- waitForTransactionsToComplete()
        entries <- f.warm.entries()
      yield
        assert(results.flatten.forall((k, v) => v == model.get(k)))
        assert(entries == model.toList)

    atest(s"$kind: splitTree clears the caller's cache"):
      val moved = Key(25)
      for
        f <- fixture(kind)
        _ <- f.warm.get(moved)
        tx = client.newTransaction()
        newRoot <- f.warm.splitTree(Key(15), false)(using tx)
        fnewRm = KVObjectRootManager.setNewRoot(client, f.rm.pointer, Key(Array[Byte](1)), true, newRoot)(using tx)
        _ <- tx.commit()
        _ <- waitForTransactionsToComplete()
        cached = f.rm.cache.root
        (_, _, freshRoot) <- f.otherRm.getFreshRootNode()
        newRm <- fnewRm
        left <- f.warm.get(moved)
        right <- open(kind, newRm).get(moved)
      yield
        assert(cached.forall(rs => rs.node.map(n => (n.pointer.id, n.revision)) ==
          freshRoot.map(n => (n.pointer.id, n.revision))))
        assert(left.isEmpty)
        assert(right == bytesOf(moved))

    atest(s"$kind: deleteTree then reuse of the same instance"):
      val k = Key(15)
      for
        f <- fixture(kind)
        _ <- f.warm.get(k)
        _ <- f.warm.deleteTree(None)
        _ <- waitForTransactionsToComplete()
        gone <- f.warm.get(k)
        _ <- setAll(f.warm, List(k))
        v <- f.warm.get(k)
        entries <- f.warm.entries()
      yield
        assert(gone.isEmpty)
        assert(v == bytesOf(k))
        assert(entries == expectedEntries(List(k)))

    atest(s"$kind: a tree deleted through another instance"):
      for
        (f, _) <- deletedElsewhere(kind)
        gone <- f.warm.get(Key(15))
        emptied <- f.warm.entries()
      yield
        assert(gone.isEmpty)
        assert(emptied.isEmpty)

    atest(s"$kind: a tree recreated through another instance"):
      val recreated = keys(1 to 3)
      for
        (f, b) <- deletedElsewhere(kind)
        gone <- f.warm.get(Key(15))
        _ <- setAll(b, recreated)
        values <- Future.sequence(recreated.map(f.warm.get))
        entries <- f.warm.entries()
      yield
        assert(gone.isEmpty)
        assert(values == recreated.map(bytesOf))
        assert(entries == expectedEntries(recreated))

    atest(s"$kind: tier reduction with a warm cache"):
      // Deletes from the left through the warm instance, one transaction each, until the tree loses a tier
      def deleteUntilReduced(f: Fixture, remaining: List[Key]): Future[List[Key]] =
        assert(remaining.size > 1)
        deleteAll(f.warm, List(remaining.head)).flatMap(_ => numTiers(f.otherRm)).flatMap: tiers =>
          if tiers < 2 then
            Future.successful(remaining.tail)
          else
            deleteUntilReduced(f, remaining.tail)
      for
        f <- fixture(kind)
        _ <- Future.sequence(initial.map(f.warm.get))
        remaining <- deleteUntilReduced(f, initial)
        _ <- waitForTransactionsToComplete()
        values <- Future.sequence(initial.map(f.warm.get))
        entries <- f.warm.entries()
      yield
        assert(values == initial.map(k => if remaining.contains(k) then bytesOf(k) else None))
        assert(entries == expectedEntries(remaining))

    atest(s"$kind: tier growth with a warm cache"):
      // Inserts ascending keys from `from` through `tree`, one transaction each, until the root is on `tier`
      def insertUntilTier(tree: Tree, rm: RootManager, from: Int, tier: Int): Future[Int] =
        assert(from < 100)
        setAll(tree, List(Key(from))).flatMap(_ => numTiers(rm)).flatMap: tiers =>
          if tiers >= tier then
            Future.successful(from)
          else
            insertUntilTier(tree, rm, from + 1, tier)
      for
        (_, created) <- newTree(kind)
        rm = new KVObjectRootManager(client, created.treeKey, created.pointer, TierCache.default())
        tree = open(kind, rm)
        // Counts tiers without refreshing the warm instance's cached root
        counter = new KVObjectRootManager(client, created.treeKey, created.pointer, NoCache)
        last1 <- insertUntilTier(tree, counter, 1, 1)
        _ <- Future.sequence(keys(1 to last1).map(tree.get))
        _ = assert(rm.cache.root.isDefined)
        last <- insertUntilTier(tree, counter, last1 + 1, 2)
        _ <- waitForTransactionsToComplete()
        values <- Future.sequence(keys(1 to last).map(tree.get))
        entries <- tree.entries()
      yield
        assert(values == keys(1 to last).map(bytesOf))
        assert(entries == expectedEntries(keys(1 to last)))
