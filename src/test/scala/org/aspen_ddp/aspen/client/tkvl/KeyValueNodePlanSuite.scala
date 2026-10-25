package org.aspen_ddp.aspen.client.tkvl

import java.util.UUID

import org.aspen_ddp.aspen.client.KeyValueObjectState
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Delete, Insert, Key, KeyValueObjectPointer, ObjectId,
  ObjectRevision, SetMax, SetRight, Value}
import org.aspen_ddp.aspen.server.store.{KVObjectState, ValueState}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

/** Verifies the KeyValueListNode planning functions without a cluster. Plans are applied to an in-memory list of
  * nodes with `KVObjectState.applyOps` and measured with `KVObjectState.encodedSizeIDA`, which is how the stores
  * measure a slice, so a node over `maxNodeSize` here is a node a store would refuse.
  */
class KeyValueNodePlanSuite extends AnyFunSuite:
  import KeyValueListNode.*

  private val ordering = ByteArrayKeyOrdering

  /** A node as a store holds it */
  private case class Stored(pointer: KeyValueObjectPointer,
                            minimum: Key,
                            max: Option[Key],
                            right: Option[Value],
                            content: Map[Key, ValueState])

  private var declinedJoins = 0
  private var removedWithSplit = 0

  private class Tier(ida: IDA, val maxNodeSize: Int):
    private var nextId = 1
    var nodes: Vector[Stored] = Vector.empty

    def newPointer(): KeyValueObjectPointer =
      val n = nextId
      nextId += 1
      new KeyValueObjectPointer(ObjectId(new UUID(1, n)), PoolId(new UUID(2, 0)), new Array[Byte](40))

    nodes = Vector(Stored(newPointer(), Key.AbsoluteMinimum, None, None, Map()))

    def tailOf(n: Stored): Option[KeyValueListPointer] = n.right.map(v => KeyValueListPointer(v.bytes))

    def shapeOf(n: Stored): NodeShape =
      val contents = n.content.map((k, vs) => k -> KeyValueObjectState.ValueState(vs.value, vs.revision, vs.timestamp))
      val tail = tailOf(n)
      NodeShape(n.pointer, ida, ordering, n.minimum, tail.map(_.minimum), tail, contents)

    def sizeOf(n: Stored): Int = KVObjectState.encodedSizeIDA(ida, Some(n.minimum), n.max, None, n.right, n.content)

    def indexFor(key: Key): Int =
      nodes.lastIndexWhere(n => ordering.compare(n.minimum, key) <= 0)

    private def apply(n: Stored, ops: List[org.aspen_ddp.aspen.common.objects.KeyValueOperation]): Stored =
      val (_, max, _, right, content) = KVObjectState.applyOps(Some(n.minimum), n.max, None, n.right, n.content, ops)
      n.copy(max = max, right = right, content = content)

    private def applyPlan(index: Int, plan: ChangePlan): Unit =
      val n = nodes(index)
      plan match
        case InPlace(_, ops) => nodes = nodes.updated(index, apply(n, ops))

        case Split(_, newMinimum, newContent, newTail, leftOps) =>
          val newPtr = newPointer()
          val rightNode = Stored(newPtr, newMinimum, None, newTail,
            newContent.map((k, v) => k -> new ValueState(v, ObjectRevision.Null, HLCTimestamp.Zero, None)))
          val rightPtr = KeyValueListPointer(newMinimum, newPtr)
          val left = apply(n, SetMax(newMinimum) :: SetRight(rightPtr.toArray) :: leftOps)
          nodes = nodes.patch(index, Vector(left, rightNode), 1)

    /** Throws NodeSizeExceeded like the executors do */
    def insert(key: Key, value: Value): Unit =
      val index = indexFor(key)
      applyPlan(index, planInsert(shapeOf(nodes(index)), key, value, maxNodeSize, None))

    def rename(oldKey: Key, newKey: Key): Unit =
      val index = indexFor(oldKey)
      applyPlan(index, planRename(shapeOf(nodes(index)), oldKey, newKey, maxNodeSize))

    def delete(key: Key): Unit =
      val index = indexFor(key)
      val n = nodes(index)
      planDelete(shapeOf(n), key) match
        case NothingToDelete =>
        case RemoveKey(_) => nodes = nodes.updated(index, apply(n, List(Delete(key))))
        case ConsiderJoin(rp) =>
          val r = nodes(index + 1)
          assert(r.pointer == rp.pointer)
          val source = JoinSource(rp, r.max, tailOf(r),
            r.content.map((k, vs) => k -> KeyValueObjectState.ValueState(vs.value, vs.revision, vs.timestamp)))
          planJoin(shapeOf(n), key, source) match
            case None =>
              declinedJoins += 1
              nodes = nodes.updated(index, apply(n, List(Delete(key))))
            case Some(ops) => nodes = nodes.patch(index, Vector(apply(n, ops)), 2)
      removeEmptyNodes()

    /** What `TailRemovalFinalizationAction` does for every emptied node. A node whose predecessor cannot be relinked
      * is left in place.
      */
    def removeEmptyNodes(): Unit =
      nodes.filter(n => n.content.isEmpty && n.minimum != Key.AbsoluteMinimum).foreach(n => removeEmpty(n.pointer))

    private def removeEmpty(pointer: KeyValueObjectPointer): Unit =
      val index = nodes.indexWhere(_.pointer == pointer)
      assert(index > 0, "the first node is never removed")
      val pred = nodes(index - 1)
      try
        val plan = planRelink(shapeOf(pred), tailOf(nodes(index)), maxNodeSize)
        if plan.isInstanceOf[Split] then removedWithSplit += 1
        applyPlan(index - 1, plan)
        nodes = nodes.filterNot(_.pointer == pointer)
      catch
        case _: NodeSizeExceeded => stuck += pointer

    /** Empty nodes whose removal threw NodeSizeExceeded */
    var stuck: Set[KeyValueObjectPointer] = Set.empty

    def entries: List[(Key, Value)] =
      nodes.iterator.flatMap: n =>
        n.content.toList.sortWith((a, b) => ordering.compare(a._1, b._1) < 0).map((k, vs) => k -> vs.value)
      .toList

    /** What the stores and the tier's readers rely on */
    def checkInvariants(context: String): Unit =
      nodes.zipWithIndex.foreach: (n, i) =>
        assert(sizeOf(n) <= maxNodeSize, s"$context: node $i is ${sizeOf(n)} bytes, over $maxNodeSize")
        val upper = nodes.lift(i + 1).map(_.minimum)
        n.content.keys.foreach: k =>
          assert(ordering.compare(k, n.minimum) >= 0, s"$context: key below node $i's minimum")
          assert(upper.forall(u => ordering.compare(k, u) < 0), s"$context: key above node $i's range")
        assert(tailOf(n).map(_.minimum) == upper, s"$context: node $i's right pointer does not lead to node ${i + 1}")
      nodes.zipWithIndex.foreach: (n, i) =>
        assert(i == 0 || n.content.nonEmpty || stuck.contains(n.pointer), s"$context: node $i is empty")
      val keys = entries.map(_._1)
      assert(keys == keys.sortWith((a, b) => ordering.compare(a, b) < 0), s"$context: tier is out of order")

  private def key(bytes: Int*): Key = Key(bytes.map(_.toByte).toArray)

  private def padded(length: Int, fill: Int): Key = Key(Array.fill[Byte](length)(fill.toByte))

  private def value(length: Int): Value = Value(new Array[Byte](length))

  private def randomKey(rnd: Random): Key =
    // Few distinct leading bytes with widely varying lengths so that a short key can sort after a long one that
    // shares its prefix, giving nodes whose minimum is much longer than that of the node to their right
    val len = if rnd.nextInt(4) == 0 then 1 + rnd.nextInt(120) else 1 + rnd.nextInt(6)
    Key(Array.tabulate[Byte](len)(i => if i == len - 1 then rnd.nextInt(256).toByte else (1 + rnd.nextInt(3)).toByte))

  private def randomValue(rnd: Random): Value =
    value(if rnd.nextBoolean() then rnd.nextInt(8) else rnd.nextInt(250))

  private val idas: List[(String, IDA)] = List("Replication" -> Replication(3, 2), "ReedSolomon" -> ReedSolomon(5, 3, 4))

  for (idaName, ida) <- idas; limit <- List(400, 1024, 4096); seed <- 1 to 10 do
    test(s"random inserts, overwrites, renames and deletes keep every node within the limit ($idaName, limit $limit, seed $seed)"):
      val rnd = new Random(seed)
      val tier = new Tier(ida, limit)
      var model = Map.empty[List[Byte], List[Byte]]
      var applied = 0

      for step <- 0 until 300 do
        val k = randomKey(rnd)
        val v = randomValue(rnd)
        val roll = rnd.nextInt(10)
        val context = s"step $step"

        def tryChange(change: => Unit, expected: => Unit): Unit =
          val before = tier.entries.map((k, v) => k.bytes.toList -> v.bytes.toList)
          try
            change
            expected
            applied += 1
          catch
            case _: NodeSizeExceeded =>
              // A rejected change must leave the tier untouched
              assert(tier.entries.map((k, v) => k.bytes.toList -> v.bytes.toList) == before, context)

        if roll < 4 then
          // Mostly keys that exist, since random keys almost never collide
          val victim = if model.nonEmpty && rnd.nextInt(5) > 0 then Key(model.keys.drop(rnd.nextInt(model.size)).head.toArray) else k
          tier.delete(victim)
          model -= victim.bytes.toList
        else if roll < 5 && model.nonEmpty then
          // Rename within the node that holds the key, as the node-level operation requires
          val old = Key(model.keys.drop(rnd.nextInt(model.size)).head.toArray)
          val target = tier.indexFor(old)
          val newKey = randomKey(rnd)
          if tier.indexFor(newKey) == target && !model.contains(newKey.bytes.toList) then
            tryChange(tier.rename(old, newKey), { model = model - old.bytes.toList + (newKey.bytes.toList -> model(old.bytes.toList)) })
        else
          tryChange(tier.insert(k, v), { model += k.bytes.toList -> v.bytes.toList })

        tier.checkInvariants(context)

      assert(tier.entries.map((k, v) => k.bytes.toList -> v.bytes.toList).toMap == model)
      assert(applied > 50)
      assert(tier.nodes.size > 1 || limit > 1024)

  test("a split with a long new minimum leaves both nodes within the limit"):
    // Nine ~100 byte pairs with a short stored maximum and right pointer, then an insert that splits near a
    // 100 byte key. The new maximum and right pointer each grow by the key's length, which the pairs alone ignore.
    val tier = new Tier(Replication(3, 2), 1024)
    (1 to 8).foreach(i => tier.insert(key(1, i), value(60)))
    tier.insert(key(9), value(60))
    (1 to 3).foreach(i => tier.insert(padded(100, i), value(60)))
    tier.checkInvariants("long minimum")
    assert(tier.nodes.size > 1)

  test("a pair too large for any node is rejected"):
    val tier = new Tier(Replication(3, 2), 400)
    intercept[NodeSizeExceeded](tier.insert(key(1), value(400)))
    assert(tier.entries.isEmpty)

  test("a node that cannot be split into two fitting nodes is rejected and left alone"):
    // Two pairs that each fit alone but cannot share a node, and a long key that makes the left node's maximum
    // and right pointer too large to leave room for either
    val tier = new Tier(Replication(3, 2), 400)
    tier.insert(key(1), value(200))
    intercept[NodeSizeExceeded](tier.insert(padded(150, 2), value(200)))
    assert(tier.entries.map(_._1) == List(key(1)))
    tier.checkInvariants("rejected split")

  private def entry(v: Value) = new ValueState(v, ObjectRevision.Null, HLCTimestamp.Zero, None)

  private def linkTo(minimum: Key, pointer: KeyValueObjectPointer): Option[Value] =
    Some(Value(KeyValueListPointer(minimum, pointer).toArray))

  /** Pairs added to an empty node with the given bounds for as long as it stays within `limit` */
  private def filled(ida: IDA, limit: Int, minimum: Key, max: Option[Key], right: Option[Value], prefix: Int,
                     valueLength: Int): Map[Key, ValueState] =
    Iterator.from(0).scanLeft(Map[Key, ValueState]())((c, i) => c + (key(prefix, i) -> entry(value(valueLength))))
      .takeWhile(c => KVObjectState.encodedSizeIDA(ida, Some(minimum), max, None, right, c) <= limit).toList.last

  test("an emptied node whose join is declined is removed, with its predecessor pointing at the right neighbor"):
    // The emptied node's minimum is much longer than the right node's, so absorbing the right node would make it larger
    val ida = Replication(3, 2)
    val tier = new Tier(ida, 1024)
    val longMin = padded(120, 1)
    val rightMin = key(5)
    val headPtr = tier.nodes(0).pointer
    val leftPtr = tier.newPointer()
    val rightPtr = tier.newPointer()
    val rightContent = filled(ida, 1024, rightMin, None, None, 5, 40)
    val right = Stored(rightPtr, rightMin, None, None, rightContent)
    val left = Stored(leftPtr, longMin, Some(rightMin), linkTo(rightMin, rightPtr), Map(longMin -> entry(value(10))))
    val head = Stored(headPtr, Key.AbsoluteMinimum, Some(longMin), linkTo(longMin, leftPtr), Map(key(0) -> entry(value(10))))
    tier.nodes = Vector(head, left, right)
    assert(tier.sizeOf(right) <= 1024)

    tier.delete(longMin)

    tier.checkInvariants("removed")
    assert(tier.nodes.map(_.pointer) == Vector(headPtr, rightPtr))
    assert(tier.nodes(1).content.size == rightContent.size)
    assert(tier.tailOf(tier.nodes(0)).map(_.pointer) == Some(rightPtr))

  test("removing an emptied node splits the predecessor when the longer link no longer fits"):
    val ida = Replication(3, 2)
    val tier = new Tier(ida, 400)
    val emptyMin = key(5)
    val rightMin = padded(120, 9)
    val headPtr = tier.nodes(0).pointer
    val emptyPtr = tier.newPointer()
    val rightPtr = tier.newPointer()
    val headContent = filled(ida, 400, Key.AbsoluteMinimum, Some(emptyMin), linkTo(emptyMin, emptyPtr), 0, 20)
    val head = Stored(headPtr, Key.AbsoluteMinimum, Some(emptyMin), linkTo(emptyMin, emptyPtr), headContent)
    // Empty, as after a delete whose join was declined when the neighbor was another node with a shorter minimum
    val empty = Stored(emptyPtr, emptyMin, Some(rightMin), linkTo(rightMin, rightPtr), Map())
    val right = Stored(rightPtr, rightMin, None, None, Map(rightMin -> entry(value(1))))
    tier.nodes = Vector(head, empty, right)
    assert(headContent.size > 3)
    assert(KVObjectState.encodedSizeIDA(ida, Some(Key.AbsoluteMinimum), Some(rightMin), None, linkTo(rightMin, rightPtr),
      headContent) > 400)

    tier.removeEmptyNodes()

    tier.checkInvariants("split")
    assert(tier.nodes.size == 3)
    assert(tier.nodes.forall(_.content.nonEmpty))
    assert(!tier.nodes.exists(_.pointer == emptyPtr))
    assert(tier.nodes.last.pointer == rightPtr)
    assert(tier.entries.map(_._1.bytes.toList).toSet == (headContent.keys ++ Set(rightMin)).map(_.bytes.toList).toSet)

  test("an emptied node is left in place when its predecessor can neither hold the link nor split"):
    val ida = Replication(3, 2)
    val tier = new Tier(ida, 400)
    val emptyMin = key(5)
    val rightMin = padded(120, 9)
    val headPtr = tier.nodes(0).pointer
    val emptyPtr = tier.newPointer()
    val rightPtr = tier.newPointer()
    // A single pair that fits with the short link but not with the long one
    val pairLength = (1 to 400).findLast: n =>
      KVObjectState.encodedSizeIDA(ida, Some(Key.AbsoluteMinimum), Some(emptyMin), None, linkTo(emptyMin, emptyPtr),
        Map(key(0) -> entry(value(n)))) <= 400
    .get
    val head = Stored(headPtr, Key.AbsoluteMinimum, Some(emptyMin), linkTo(emptyMin, emptyPtr),
      Map(key(0) -> entry(value(pairLength))))
    // Empty, as after a delete whose join was declined when the neighbor was another node with a shorter minimum
    val empty = Stored(emptyPtr, emptyMin, Some(rightMin), linkTo(rightMin, rightPtr), Map())
    val right = Stored(rightPtr, rightMin, None, None, Map(rightMin -> entry(value(1))))
    tier.nodes = Vector(head, empty, right)

    tier.removeEmptyNodes()

    assert(tier.nodes.map(_.pointer) == Vector(headPtr, emptyPtr, rightPtr))
    assert(tier.stuck == Set(emptyPtr))
    tier.checkInvariants("left in place")

  test("a join whose result is no larger than the absorbed node is performed"):
    val tier = new Tier(Replication(3, 2), 1024)
    (1 to 14).foreach(i => tier.insert(key(i), value(100)))
    assert(tier.nodes.size > 1)
    val before = tier.nodes.size
    tier.entries.filter(_._1.bytes(0) <= tier.nodes(1).minimum.bytes(0) - 1).foreach(e => tier.delete(e._1))
    tier.checkInvariants("join")
    assert(tier.nodes.size < before)
    assert(tier.entries.size == tier.nodes.map(_.content.size).sum)

  test("insert requirements"):
    val ida = Replication(3, 2)
    val tier = new Tier(ida, 1024)
    tier.insert(key(1), value(10))
    val shape = tier.shapeOf(tier.nodes(0))
    intercept[org.aspen_ddp.aspen.common.objects.KeyAlreadyExists]:
      planInsert(shape, key(1), value(10), 1024, Some(Left(true)))
    planInsert(shape, key(2), value(10), 1024, Some(Left(true))) match
      case InPlace(reqs, ops) =>
        assert(reqs.map(_.getClass.getSimpleName) == List("WithinRange", "DoesNotExist"))
        assert(ops.map(_.getClass.getSimpleName) == List("Insert"))
      case other => fail(s"Expected InPlace, got $other")

  test("delete plans"):
    val tier = new Tier(Replication(3, 2), 1024)
    tier.insert(key(1), value(10))
    val shape = tier.shapeOf(tier.nodes(0))
    assert(planDelete(shape, key(9)) == NothingToDelete)
    assert(planDelete(shape, key(1)) == RemoveKey(key(1)))

  test("the random runs decline joins"):
    // Runs after the random tests above, which count what they exercise
    assert(declinedJoins > 0, "no join was declined")
