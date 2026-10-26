package org.aspen_ddp.aspen.client.tkvl

import java.util.UUID

import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Key, ObjectId, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

/** Verifies the DataObjectNodeEncoding planning functions without a cluster. A node is its encoded bytes, so its
  * size is exactly `encode(...).length`, which is what a store would hold.
  */
class DataObjectNodePlanSuite extends AnyFunSuite:
  import DataObjectNodeEncoding.*

  private val ordering = ByteArrayKeyOrdering

  /** Pointers of differing encoded sizes, as for pools of differing width */
  private def pointer(n: Int, storePointerLength: Int = 3): DataObjectPointer =
    DataObjectPointer(ObjectId(new UUID(7, n)), PoolId(new UUID(8, 0)), new Array[Byte](storePointerLength))

  private def key(bytes: Int*): Key = Key(bytes.map(_.toByte).toArray)

  private def padded(length: Int, fill: Int): Key = Key(Array.fill[Byte](length)(fill.toByte))

  private def value(length: Int): Value = Value(new Array[Byte](length))

  private case class Node(pointer: DataObjectPointer, content: NodeContent)

  private var declinedJoins = 0
  private var multiSplits = 0
  private var batchJoins = 0
  private var splitRemovals = 0

  private class Tier(val maxNodeSize: Int, pointerLengths: Seq[Int] = Seq(3)):
    private var nextId = 1
    var stuck: Set[DataObjectPointer] = Set.empty
    var nodes: Vector[Node] = Vector.empty

    def newPointer(): DataObjectPointer =
      val n = nextId
      nextId += 1
      pointer(n, pointerLengths(n % pointerLengths.size))

    nodes = Vector(Node(newPointer(), NodeContent(Key.AbsoluteMinimum, None, Nil)))

    def indexFor(k: Key): Int = nodes.lastIndexWhere(n => ordering.compare(n.content.minimum, k) <= 0)

    private def sorted(entries: List[(Key, Value)]): List[(Key, Value)] =
      entries.sortWith((a, b) => ordering.compare(a._1, b._1) < 0)

    private def applyRewrite(index: Int, link: Option[NodeLink], plan: RewritePlan): Unit =
      val n = nodes(index)
      plan match
        case Rewrite(entries) => nodes = nodes.updated(index, n.copy(content = NodeContent(n.content.minimum, link, entries)))
        case NeedsSplit(all) =>
          val rightPointer = newPointer()
          val split = planSplit(n.content.minimum, link, all, rightPointer, maxNodeSize)
          val left = Node(n.pointer, NodeContent(n.content.minimum, Some(NodeLink(split.newMinimum, rightPointer)), split.left))
          val right = Node(rightPointer, NodeContent(split.newMinimum, link, split.right))
          nodes = nodes.patch(index, Vector(left, right), 1)

    /** Throws NodeSizeExceeded like the executors do */
    def insert(k: Key, v: Value): Unit =
      val index = indexFor(k)
      val n = nodes(index)
      applyRewrite(index, n.content.link, planInsert(n.pointer, n.content, ordering, k, v, maxNodeSize))

    def delete(k: Key): Unit =
      val index = indexFor(k)
      val n = nodes(index)
      planDelete(n.content, k) match
        case NothingToDelete =>
        case RemoveEntry(remaining) => nodes = nodes.updated(index, n.copy(content = n.content.copy(entries = remaining)))
        case ConsiderJoin(link) =>
          val right = nodes(index + 1)
          assert(right.pointer == link.pointer)
          planJoin(n.content, right.content, maxNodeSize) match
            case Some(joined) => nodes = nodes.patch(index, Vector(n.copy(content = joined)), 2)
            case None =>
              declinedJoins += 1
              nodes = nodes.updated(index, n.copy(content = n.content.copy(entries = Nil)))
      removeEmptyNodes()

    /** As the executor's split of `all` in node `index`: one split if one fits, else a multi-split, planned again
      * with larger pointers while the created ones do not fit
      */
    private def splitNode(index: Int, all: List[(Key, Value)]): Unit =
      val n = nodes(index)
      val c = n.content
      val rightPointer = newPointer()
      splitIndex(c.minimum, all, c.link, rightPointer, maxNodeSize) match
        case Some(_) =>
          val split = planSplit(c.minimum, c.link, all, rightPointer, maxNodeSize)
          val left = Node(n.pointer, NodeContent(c.minimum, Some(NodeLink(split.newMinimum, rightPointer)), split.left))
          nodes = nodes.patch(index, Vector(left, Node(rightPointer, NodeContent(split.newMinimum, c.link, split.right))), 1)
        case None =>
          def attempt(sizing: DataObjectPointer, created: List[DataObjectPointer], remaining: Int)
              : (List[List[(Key, Value)]], List[DataObjectPointer]) =
            val runs = planMultiSplit(c.minimum, c.link, all, sizing, maxNodeSize)
            val all2 = created ++ List.fill(math.max(0, runs.size - 1 - created.size))(newPointer())
            val ptrs = all2.take(runs.size - 1)
            if multiSplitFits(c.minimum, c.link, runs, ptrs, maxNodeSize) then (runs, ptrs)
            else if remaining == 0 then throw new NodeSizeExceeded
            else attempt(ptrs.maxBy(_.encodedSize), all2, remaining - 1)
          val (runs, ptrs) = attempt(rightPointer, List(rightPointer), 3)
          multiSplits += 1
          val links = runs.tail.zip(ptrs).map((r, p) => Some(NodeLink(r.head._1, p))) :+ c.link
          val left = Node(n.pointer, NodeContent(c.minimum, links.head, runs.head))
          val created = runs.tail.indices.map(j => Node(ptrs(j), NodeContent(runs(j + 1).head._1, links(j + 1), runs(j + 1))))
          nodes = nodes.patch(index, left +: created, 1)

    /** Applies a batch as `ListBatch` does, see the KeyValue suite's `batch` */
    def batch(ops: List[BatchOp]): Unit =
      val saved = nodes
      try
        val groups = BatchOp.sorted(ops, ordering).groupBy(op => indexFor(op.key))
        val plans = groups.toList.map: (i, gops) =>
          val n = nodes(i)
          i -> planBatch(n.pointer, n.content, ordering, BatchOp.sorted(gops, ordering), maxNodeSize)
        plans.sortBy(-_._1).foreach: (i, plan) =>
          val n = nodes(i)
          plan match
            case None =>
            case Some(Rewrite(Nil)) if n.content.link.isDefined && !groups.contains(i + 1) =>
              planJoin(n.content, nodes(i + 1).content, maxNodeSize) match
                case Some(joined) =>
                  batchJoins += 1
                  nodes = nodes.patch(i, Vector(n.copy(content = joined)), 2)
                case None => nodes = nodes.updated(i, n.copy(content = n.content.copy(entries = Nil)))
            case Some(Rewrite(entries)) => nodes = nodes.updated(i, n.copy(content = n.content.copy(entries = entries)))
            case Some(NeedsSplit(all)) => splitNode(i, all)
        removeEmptyNodes()
      catch
        case e: Throwable =>
          nodes = saved
          throw e

    /** What `TailRemovalFinalizationAction` does for every emptied node */
    def removeEmptyNodes(): Unit =
      nodes.filter(n => n.content.entries.isEmpty && n.content.minimum.bytes.nonEmpty).foreach: n =>
        val index = nodes.indexWhere(_.pointer == n.pointer)
        val pred = nodes(index - 1)
        try
          val plan = planRelink(pred.content, n.content.link, maxNodeSize)
          if plan.isInstanceOf[NeedsSplit] then splitRemovals += 1
          applyRewrite(index - 1, n.content.link, plan)
          nodes = nodes.filterNot(_.pointer == n.pointer)
        catch
          case _: NodeSizeExceeded => stuck += n.pointer

    def entries: List[(Key, Value)] = nodes.flatMap(_.content.entries).toList

    def checkInvariants(context: String): Unit =
      nodes.zipWithIndex.foreach: (n, i) =>
        val c = n.content
        val bytes = encode(c.entries, c.minimum, c.link)
        assert(bytes.length == encodedSize(c.entries, c.minimum, c.link), s"$context: node $i size mismatch")
        assert(bytes.length <= maxNodeSize, s"$context: node $i is ${bytes.length} bytes, over $maxNodeSize")
        val decoded = decode(bytes)
        assert(decoded.minimum.bytes.toList == c.minimum.bytes.toList && decoded.entries.map(_._1.bytes.toList) == c.entries.map(_._1.bytes.toList), s"$context: node $i does not round trip")
        val upper = nodes.lift(i + 1).map(_.content.minimum)
        c.entries.foreach: (k, _) =>
          assert(ordering.compare(k, c.minimum) >= 0, s"$context: key below node $i's minimum")
          assert(upper.forall(u => ordering.compare(k, u) < 0), s"$context: key above node $i's range")
        assert(c.link.map(_.minimum.bytes.toList) == upper.map(_.bytes.toList), s"$context: node $i's link does not lead to node ${i + 1}")
        assert(c.link.forall(_.pointer == nodes(i + 1).pointer), s"$context: node $i's link points at the wrong node")
        assert(i == 0 || c.entries.nonEmpty || stuck.contains(n.pointer), s"$context: node $i is empty")
      val keys = entries.map(_._1)
      assert(keys == keys.sortWith((a, b) => ordering.compare(a, b) < 0), s"$context: tier is out of order")

  private def randomKey(rnd: Random): Key =
    val len = if rnd.nextInt(4) == 0 then 1 + rnd.nextInt(120) else 1 + rnd.nextInt(6)
    Key(Array.tabulate[Byte](len)(i => if i == len - 1 then rnd.nextInt(256).toByte else (1 + rnd.nextInt(3)).toByte))

  private def randomValue(rnd: Random): Value = value(if rnd.nextBoolean() then rnd.nextInt(8) else rnd.nextInt(150))

  private def asBytes(entries: List[(Key, Value)]) = entries.map((k, v) => k.bytes.toList -> v.bytes.toList)

  for limit <- List(200, 400, 1024); pointerLengths <- List(Seq(3), Seq(3, 30, 60)); seed <- 1 to 10 do
    test(s"random inserts, overwrites, deletes and removals keep every node within the limit (limit $limit, pointers ${pointerLengths.mkString("/")}, seed $seed)"):
      val rnd = new Random(seed)
      val tier = new Tier(limit, pointerLengths)
      var model = Map.empty[List[Byte], List[Byte]]
      var applied = 0

      for step <- 0 until 300 do
        val k = randomKey(rnd)
        val v = randomValue(rnd)
        val context = s"step $step"

        if rnd.nextInt(10) < 4 then
          val victim = if model.nonEmpty && rnd.nextInt(5) > 0 then Key(model.keys.drop(rnd.nextInt(model.size)).head.toArray) else k
          tier.delete(victim)
          model -= victim.bytes.toList
        else
          val before = asBytes(tier.entries)
          try
            tier.insert(k, v)
            model += k.bytes.toList -> v.bytes.toList
            applied += 1
          catch
            case _: NodeSizeExceeded =>
              assert(asBytes(tier.entries) == before, context)

        tier.checkInvariants(context)

      assert(asBytes(tier.entries).toMap == model)
      assert(applied > 30)

  private def put(k: Key, v: Value, mustNotExist: Boolean = false): BatchOp =
    BatchOp.Put(k, v, if mustNotExist then Some(Left(true)) else None)

  for limit <- List(200, 400, 1024); pointerLengths <- List(Seq(3), Seq(3, 30, 60)); seed <- 1 to 3 do
    test(s"random batches keep every node within the limit and match the model (limit $limit, pointers ${pointerLengths.mkString("/")}, seed $seed)"):
      val rnd = new Random(seed)
      val tier = new Tier(limit, pointerLengths)
      var model = Map.empty[List[Byte], List[Byte]]
      var applied = 0

      for step <- 0 until 60 do
        val size = if rnd.nextInt(5) == 0 then 50 + rnd.nextInt(150) else 1 + rnd.nextInt(12)
        val held = model.keys.toVector
        // Values small enough for the limit that a large batch is rarely rejected for one entry
        def randomValue(rnd: Random): Value = value(if rnd.nextBoolean() then rnd.nextInt(8) else rnd.nextInt(limit / 6))
        val ops = (0 until size).map: _ =>
          rnd.nextInt(10) match
            case r if r < 3 && held.nonEmpty => BatchOp.Remove(Key(held(rnd.nextInt(held.size)).toArray))
            case r if r < 4 => BatchOp.Remove(randomKey(rnd))
            case r if r < 6 && held.nonEmpty => put(Key(held(rnd.nextInt(held.size)).toArray), randomValue(rnd))
            case _ => put(randomKey(rnd), randomValue(rnd))
        .groupBy(_.key).values.map(_.head).toList

        val before = asBytes(tier.entries)
        try
          tier.batch(ops)
          model = ops.foldLeft(model):
            case (m, p: BatchOp.Put) => m + (p.key.bytes.toList -> p.value.bytes.toList)
            case (m, BatchOp.Remove(k)) => m - k.bytes.toList
          applied += 1
        catch
          case _: NodeSizeExceeded => assert(asBytes(tier.entries) == before, s"step $step")

        tier.checkInvariants(s"step $step")
        assert(asBytes(tier.entries).toMap == model, s"step $step")

      assert(applied > 20)

  test("a must-not-exist put of a held key is rejected and a revision requirement is unsupported"):
    val ptr = pointer(1)
    val content = NodeContent(Key.AbsoluteMinimum, None, List(key(1) -> value(1)))
    intercept[org.aspen_ddp.aspen.common.objects.KeyAlreadyExists](
      planBatch(ptr, content, ordering, List(put(key(1), value(2), mustNotExist = true)), 1024))
    intercept[UnsupportedRequirement](
      planBatch(ptr, content, ordering, List(BatchOp.Put(key(2), value(2), Some(Right(org.aspen_ddp.aspen.common.objects.ObjectRevision.Null)))), 1024))
    assert(planBatch(ptr, content, ordering, List(put(key(2), value(2), mustNotExist = true)), 1024).isDefined)

  for seed <- 1 to 5 do
    test(s"a batch of one op plans what the single-key plan functions do (seed $seed)"):
      val rnd = new Random(seed)
      val tier = new Tier(400, Seq(3, 30))
      var compared = 0

      def same(a: RewritePlan, b: RewritePlan): Boolean = (a, b) match
        case (Rewrite(x), Rewrite(y)) => asBytes(x) == asBytes(y)
        case (NeedsSplit(x), NeedsSplit(y)) => asBytes(x) == asBytes(y)
        case _ => false

      for step <- 0 until 200 do
        val k = randomKey(rnd)
        val n = tier.nodes(tier.indexFor(k))

        locally:
          val v = randomValue(rnd)
          val single = scala.util.Try(planInsert(n.pointer, n.content, ordering, k, v, 400))
          val batched = scala.util.Try(planBatch(n.pointer, n.content, ordering, List(put(k, v)), 400))
          (single, batched) match
            case (scala.util.Success(p), scala.util.Success(Some(b))) => assert(same(p, b), s"step $step")
            case (scala.util.Failure(e1), scala.util.Failure(e2)) => assert(e1.getClass == e2.getClass, s"step $step")
            case other => fail(s"step $step: $other")
          compared += 1

        if n.content.entries.nonEmpty then
          val held = n.content.entries(rnd.nextInt(n.content.entries.size))._1
          (planDelete(n.content, held), planBatch(n.pointer, n.content, ordering, List(BatchOp.Remove(held)), 400)) match
            case (RemoveEntry(rem), Some(Rewrite(b))) => assert(asBytes(rem) == asBytes(b))
            case (ConsiderJoin(_), Some(Rewrite(Nil))) => assert(n.content.link.isDefined)
            case other => fail(s"step $step: $other")
        assert(planBatch(n.pointer, n.content, ordering, List(BatchOp.Remove(padded(130, 3))), 400).isEmpty)

        try tier.insert(k, randomValue(rnd))
        catch case _: NodeSizeExceeded => ()

      assert(compared == 200)

  test("the random runs decline joins and split to remove empty nodes"):
    assert(declinedJoins > 0, "no join was declined")
    assert(splitRemovals > 0, "no removal needed a split")
    assert(multiSplits > 0, "no batch split a node into more than two")
    assert(batchJoins > 0, "no batch joined an emptied node")

  test("an entry is rejected unless it fits with a link as long as one to its own key"):
    val ptr = pointer(1)
    val content = NodeContent(Key.AbsoluteMinimum, None, Nil)
    val k = key(1)
    val v = value(100)
    val needed = entrySize(k, v) + nodeOverhead(Key.AbsoluteMinimum, Some(NodeLink(k, ptr)))

    planInsert(ptr, content, ordering, k, v, needed) match
      case Rewrite(entries) => assert(entries.size == 1)
      case other => fail(s"Expected Rewrite, got $other")
    intercept[NodeSizeExceeded](planInsert(ptr, content, ordering, k, v, needed - 1))

  test("a split uses the nearest split point that fits when SplitPlan's choice does not"):
    // Sizes are the entries' encoded sizes. The new node's long minimum makes the balanced split overflow the left node
    val link = Some(NodeLink(padded(100, 9), pointer(9)))
    val entries = List(key(1) -> value(10), key(2) -> value(10), key(3) -> value(10), padded(100, 4) -> value(10),
      key(5) -> value(10))
    val size = encodedSize(entries, Key.AbsoluteMinimum, link)
    val limit = size - 20
    val split = planSplit(Key.AbsoluteMinimum, link, entries, pointer(10), limit)
    assert(split.left.nonEmpty && split.right.nonEmpty)
    assert(split.left ++ split.right == entries)
    assert(encodedSize(split.left, Key.AbsoluteMinimum, Some(NodeLink(split.newMinimum, pointer(10)))) <= limit)
    assert(encodedSize(split.right, split.newMinimum, link) <= limit)

  test("no split is found when the entries cannot share a node with the link"):
    val link = Some(NodeLink(padded(120, 9), pointer(2)))
    val big = List(key(0, 0) -> value(300), key(0, 1) -> value(300))
    intercept[NodeSizeExceeded](planSplit(Key.AbsoluteMinimum, link, big, pointer(3), 400))

  test("a join is made only when the joined node fits"):
    val left = NodeContent(padded(100, 1), Some(NodeLink(key(5), pointer(2))), Nil)
    val right = NodeContent(key(5), None, List(key(5, 0) -> value(60)))
    val joinedSize = encodedSize(right.entries, left.minimum, right.link)
    assert(planJoin(left, right, joinedSize).exists(j => j.minimum.bytes.toList == left.minimum.bytes.toList && j.entries == right.entries))
    assert(planJoin(left, right, joinedSize - 1).isEmpty)

  test("a delete plan"):
    val content = NodeContent(Key.AbsoluteMinimum, Some(NodeLink(key(9), pointer(2))), List(key(1) -> value(1), key(2) -> value(1)))
    assert(planDelete(content, key(7)) == NothingToDelete)
    planDelete(content, key(1)) match
      case RemoveEntry(remaining) => assert(remaining.map(_._1.bytes.toList) == List(List[Byte](2)))
      case other => fail(s"Expected RemoveEntry, got $other")
    val last = content.copy(entries = List(key(1) -> value(1)))
    planDelete(last, key(1)) match
      case ConsiderJoin(link) => assert(link.pointer == pointer(2))
      case other => fail(s"Expected ConsiderJoin, got $other")
    val tail = last.copy(link = None)
    planDelete(tail, key(1)) match
      case RemoveEntry(Nil) =>
      case other => fail(s"Expected RemoveEntry(Nil), got $other")

  test("a predecessor holding the shorter link fits but not the longer one, and then splits"):
    val shortLink = Some(NodeLink(key(5), pointer(1)))
    val longLink = Some(NodeLink(padded(120, 9), pointer(2)))
    val limit = 400
    val entries = Iterator.from(0).scanLeft(List[(Key, Value)]())((l, i) => l :+ (key(0, i) -> value(20)))
      .takeWhile(l => encodedSize(l, Key.AbsoluteMinimum, shortLink) <= limit).toList.last
    val content = NodeContent(Key.AbsoluteMinimum, shortLink, entries)

    assert(planRelink(content, shortLink, limit).isInstanceOf[Rewrite])
    planRelink(content, longLink, limit) match
      case NeedsSplit(all) =>
        val split = planSplit(Key.AbsoluteMinimum, longLink, all, pointer(3), limit)
        assert(encodedSize(split.right, split.newMinimum, longLink) <= limit)
      case other => fail(s"Expected NeedsSplit, got $other")

  test("a predecessor with a single entry that cannot hold the longer link is left alone"):
    val longLink = Some(NodeLink(padded(120, 9), pointer(2)))
    val content = NodeContent(Key.AbsoluteMinimum, Some(NodeLink(key(5), pointer(1))), List(key(0) -> value(300)))
    intercept[NodeSizeExceeded](planRelink(content, longLink, 400))

  test("splitAt rejects a right node whose longer minimum no longer fits"):
    val link = Some(NodeLink(key(9), pointer(2)))
    val entries = List(key(1) -> value(10), padded(100, 5) -> value(60), key(6) -> value(10))
    val content = NodeContent(key(1), link, entries)
    val splitKey = padded(100, 5)
    val (left, right) = planSplitAt(content, ordering, splitKey, inclusive = false, 1000)
    assert(left.map(_._1.bytes.toList) == List(List[Byte](1)))
    assert(right.size == 2)
    intercept[NodeSizeExceeded](planSplitAt(content, ordering, splitKey, inclusive = false, encodedSize(right, splitKey, link) - 1))
    val (left2, _) = planSplitAt(content, ordering, splitKey, inclusive = true, 1000)
    assert(left2.size == 2)

  // ---------------------------------------------------------------------------------------------------------------
  // Byte layout

  private def sameLink(a: Option[NodeLink], b: Option[NodeLink]): Boolean =
    (a, b) match
      case (None, None) => true
      case (Some(x), Some(y)) => x.minimum.bytes.toList == y.minimum.bytes.toList && x.pointer == y.pointer
      case _ => false

  test("encode/decode round trips entries, minimum and link"):
    val bigKey = Key(Array.tabulate[Byte](5000)(i => (i % 251).toByte))
    val cases: List[(List[(Key, Value)], Key, Option[NodeLink])] = List(
      (Nil, Key.AbsoluteMinimum, None),
      (Nil, Key("min"), Some(NodeLink(Key("next"), pointer(1)))),
      (List(key(1) -> Value(Array[Byte]())), Key.AbsoluteMinimum, None),
      (List(key(1) -> value(1), key(2) -> Value(Array[Byte]()), key(3) -> value(300)), key(1),
        Some(NodeLink(key(9), pointer(2)))),
      (List(bigKey -> value(200)), Key.AbsoluteMinimum, Some(NodeLink(bigKey, pointer(3)))),
      (List(Key.AbsoluteMinimum -> value(5)), Key.AbsoluteMinimum, None))

    cases.foreach: (entries, minimum, link) =>
      val bytes = encode(entries, minimum, link)
      val d = decode(bytes)
      assert(asBytes(d.entries) == asBytes(entries))
      assert(d.minimum.bytes.toList == minimum.bytes.toList)
      assert(sameLink(d.link, link))
      assert(encodedSize(entries, minimum, link) == bytes.length)

  test("an empty node with the absolute minimum and no link is one byte"):
    assert(encode(Nil, Key.AbsoluteMinimum, None).length == 1)

  test("encodedSize equals the encoded length and splits into overhead plus entry sizes"):
    val rnd = new Random(20261025L)
    def bytes(max: Int): Array[Byte] =
      val a = new Array[Byte](rnd.nextInt(max + 1))
      rnd.nextBytes(a)
      a

    (1 to 300).foreach: _ =>
      val entries = List.fill(rnd.nextInt(20))(Key(bytes(300)) -> Value(bytes(300)))
        .sortWith((a, b) => ordering.compare(a._1, b._1) < 0)
      val minimum = Key(bytes(200))
      val link = if rnd.nextBoolean() then Some(NodeLink(Key(bytes(200)), pointer(rnd.nextInt(1000)))) else None
      val size = encodedSize(entries, minimum, link)
      assert(size == encode(entries, minimum, link).length)
      assert(size == nodeOverhead(minimum, link) + entries.map((k, v) => entrySize(k, v)).sum)

  // ---------------------------------------------------------------------------------------------------------------
  // Scenarios moved from DataObjectNodeEncodingSuite

  test("a split that SplitPlan would leave with an oversized left node uses another split point"):
    // SplitPlan moves only key(3), leaving the big entry in a left node that now needs a link
    val original = List(key(1) -> value(10), key(2) -> value(900))
    val maxNodeSize = encodedSize(original, Key.AbsoluteMinimum, None) + 5
    val ptr = pointer(1)
    planInsert(ptr, NodeContent(Key.AbsoluteMinimum, None, original), ordering, key(3), value(10), maxNodeSize) match
      case NeedsSplit(all) =>
        val split = planSplit(Key.AbsoluteMinimum, None, all, pointer(2), maxNodeSize)
        assert(split.left.map(_._1.bytes.toList) == List(List[Byte](1)))
        assert(split.right.map(_._1.bytes.toList) == List(List[Byte](2), List[Byte](3)))
        assert(encodedSize(split.left, Key.AbsoluteMinimum, Some(NodeLink(key(2), pointer(2)))) <= maxNodeSize)
        assert(encodedSize(split.right, key(2), None) <= maxNodeSize)
      case other => fail(s"Expected NeedsSplit, got $other")

  test("the largest accepted entry still lets a later key above it split off into its own node"):
    val maxNodeSize = 1000
    val a = key(10)
    val b = key(20)
    val ptr = pointer(1)
    val empty = NodeContent(Key.AbsoluteMinimum, None, Nil)
    val reserve = nodeOverhead(Key.AbsoluteMinimum, Some(NodeLink(a, ptr)))
    val vlen = (0 to maxNodeSize).filter(v => entrySize(a, value(v)) <= maxNodeSize - reserve).max
    assert(vlen > 900)
    intercept[NodeSizeExceeded](planInsert(ptr, empty, ordering, a, value(vlen + 1), maxNodeSize))

    val full = planInsert(ptr, empty, ordering, a, value(vlen), maxNodeSize) match
      case Rewrite(entries) => NodeContent(Key.AbsoluteMinimum, None, entries)
      case other => fail(s"Expected Rewrite, got $other")
    planInsert(ptr, full, ordering, b, value(100), maxNodeSize) match
      case NeedsSplit(all) =>
        val split = planSplit(Key.AbsoluteMinimum, None, all, pointer(2), maxNodeSize)
        assert(split.left.map(_._1.bytes.toList) == List(a.bytes.toList))
        assert(split.right.map(_._1.bytes.toList) == List(b.bytes.toList))
        assert(encodedSize(split.left, Key.AbsoluteMinimum, Some(NodeLink(b, pointer(2)))) <= maxNodeSize)
      case other => fail(s"Expected NeedsSplit, got $other")

  test("an insert below a long-keyed big entry splits where both nodes fit"):
    val a = key(1)
    val s = key(2)
    val bigKey = Key(Array[Byte](3) ++ Array.fill[Byte](199)(7))
    val bigValue = value(300)
    val ptr = pointer(1)
    val nodeLink = Some(NodeLink(Key(Array.fill[Byte](20)(9)), pointer(5)))
    val content = NodeContent(Key.AbsoluteMinimum, nodeLink, List(a -> value(1), bigKey -> bigValue))
    // The big entry is exactly as large as the node accepts
    val maxNodeSize = math.max(nodeOverhead(Key.AbsoluteMinimum, nodeLink),
      nodeOverhead(Key.AbsoluteMinimum, Some(NodeLink(bigKey, ptr)))) + entrySize(bigKey, bigValue)
    // s overflows the node, but s and the big entry fit together in a node whose minimum is s
    val sv = (1 to 1000).map(value).find: v =>
      encodedSize(List(a -> value(1), s -> v, bigKey -> bigValue), Key.AbsoluteMinimum, nodeLink) > maxNodeSize &&
        encodedSize(List(s -> v, bigKey -> bigValue), s, nodeLink) <= maxNodeSize

    // SplitPlan alone would move only the big entry, into a node too large with the long key as its minimum
    assert(encodedSize(List(bigKey -> bigValue), bigKey, nodeLink) > maxNodeSize)
    planInsert(ptr, content, ordering, s, sv.get, maxNodeSize) match
      case NeedsSplit(all) =>
        val split = planSplit(Key.AbsoluteMinimum, nodeLink, all, pointer(2), maxNodeSize)
        assert(split.left.map(_._1.bytes.toList) == List(a.bytes.toList))
        assert(split.right.map(_._1.bytes.toList) == List(s.bytes.toList, bigKey.bytes.toList))
        assert(split.newMinimum.bytes.toList == s.bytes.toList)
        assert(encodedSize(split.left, Key.AbsoluteMinimum, Some(NodeLink(s, pointer(2)))) <= maxNodeSize)
        assert(encodedSize(split.right, s, nodeLink) <= maxNodeSize)
      case other => fail(s"Expected NeedsSplit, got $other")
