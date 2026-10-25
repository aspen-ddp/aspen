package org.aspen_ddp.aspen.client.tkvl

import java.util.UUID

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.ReedSolomon
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Key, ObjectId, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.Future
import scala.util.Random

class DataObjectNodeEncodingSuite extends IntegrationTestSuite:

  private val enc = DataObjectNodeEncoding
  private val ordering = ByteArrayKeyOrdering

  private def key(i: Int): Key = Key(Array[Byte](i.toByte))
  private def vbytes(i: Int, size: Int = 1): Value = Value(Array.fill[Byte](size)(i.toByte))

  private def fakePointer(n: Int): DataObjectPointer =
    DataObjectPointer(ObjectId(new UUID(7, n)), PoolId(new UUID(8, n)), Array[Byte](1, 2, 3))

  /** Entries as comparable byte lists: Key compares by content but Value does not */
  private def asBytes(entries: List[(Key, Value)]): List[(List[Byte], List[Byte])] =
    entries.map((k, v) => (k.bytes.toList, v.bytes.toList))

  private def sameLink(a: Option[NodeLink], b: Option[NodeLink]): Boolean = (a, b) match
    case (None, None) => true
    case (Some(x), Some(y)) => x.minimum == y.minimum && x.pointer == y.pointer &&
      x.pointer.toArray.toList == y.pointer.toArray.toList
    case _ => false

  private val noSplit: (Key, ObjectPointer) => Future[Unit] =
    (_, _) => Future.failed(new Exception("no split expected"))
  private val noJoin: (Key, ObjectPointer) => Future[Unit] =
    (_, _) => Future.failed(new Exception("no join expected"))

  private def allocator(): Future[ObjectAllocator] = client.getStoragePool(Radicle.poolId).map(_.allocator)

  /** Allocates and commits a node holding `entries`, then reads it back */
  private def makeNode(entries: List[(Key, Value)],
                       minimum: Key = Key.AbsoluteMinimum,
                       link: Option[NodeLink] = None): Future[DataObjectListNode] =
    for
      alloc <- allocator()
      tx = client.newTransaction()
      ptr <- alloc.allocateDataObject(DataBuffer(enc.encode(entries, minimum, link)))(using tx)
      _ <- tx.commit()
      node <- readNode(ptr, minimum)
    yield
      node

  private def readNode(ptr: ObjectPointer, minimum: Key = Key.AbsoluteMinimum): Future[DataObjectListNode] =
    enc.read(client, ptr, minimum, ordering, "test read").map(_.asInstanceOf[DataObjectListNode])

  // ---------------------------------------------------------------------------------------------------------------
  // Pure encoding

  test("encode/decode round trips entries, minimum and link"):
    val bigKey = Key(Array.tabulate[Byte](5000)(i => (i % 251).toByte))
    val cases: List[(List[(Key, Value)], Key, Option[NodeLink])] = List(
      (Nil, Key.AbsoluteMinimum, None),
      (Nil, Key("min"), Some(NodeLink(Key("next"), fakePointer(1)))),
      (List(key(1) -> Value(Array[Byte]())), Key.AbsoluteMinimum, None),
      (List(key(1) -> vbytes(1), key(2) -> Value(Array[Byte]()), key(3) -> vbytes(3, 300)), key(1),
        Some(NodeLink(key(9), fakePointer(2)))),
      (List(bigKey -> vbytes(4, 200)), Key.AbsoluteMinimum, Some(NodeLink(bigKey, fakePointer(3)))),
      (List(Key.AbsoluteMinimum -> vbytes(5)), Key.AbsoluteMinimum, None))

    cases.foreach: (entries, minimum, link) =>
      val bytes = enc.encode(entries, minimum, link)
      val d = enc.decode(bytes)
      assert(asBytes(d.entries) == asBytes(entries))
      assert(d.minimum == minimum)
      assert(sameLink(d.link, link))
      assert(enc.encodedSize(entries, minimum, link) == bytes.length)
    succeed

  test("an empty node with the absolute minimum and no link is one byte"):
    assert(enc.encode(Nil, Key.AbsoluteMinimum, None).length == 1)

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
      val link = if rnd.nextBoolean() then Some(NodeLink(Key(bytes(200)), fakePointer(rnd.nextInt()))) else None
      val size = enc.encodedSize(entries, minimum, link)
      assert(size == enc.encode(entries, minimum, link).length)
      assert(size == enc.nodeOverhead(minimum, link) + entries.map((k, v) => enc.entrySize(k, v)).sum)
    succeed

  test("NodeEncoding selects the DataObject encoding and it uses the DataObject node size limit"):
    val e = NodeEncoding(TreeKind.DataObjects)
    assert(e eq DataObjectNodeEncoding)
    assert(e.kind == TreeKind.DataObjects)
    assert(e.nodeSizeLimit(ReedSolomon(5, 3, 4), Some(4096)) ==
      NodeAllocator.nodeSizeLimit(ReedSolomon(5, 3, 4), Some(4096), TreeKind.DataObjects))

  // ---------------------------------------------------------------------------------------------------------------
  // Insert

  atest("allocate sorts entries and creates a node with the absolute minimum and no link"):
    for
      alloc <- allocator()
      tx = client.newTransaction()
      ptr <- enc.allocate(alloc, ordering, Map(key(3) -> vbytes(3), key(1) -> vbytes(1), key(2) -> vbytes(2)))(using tx)
      _ <- tx.commit()
      node <- readNode(ptr)
    yield
      assert(asBytes(node.entries) == asBytes(List(key(1) -> vbytes(1), key(2) -> vbytes(2), key(3) -> vbytes(3))))
      assert(node.minimum == Key.AbsoluteMinimum)
      assert(node.link.isEmpty)

  atest("insert under maxNodeSize rewrites the node"):
    val link = Some(NodeLink(key(50), fakePointer(1)))
    for
      n0 <- makeNode(List(key(1) -> vbytes(1), key(5) -> vbytes(5)), key(1), link)
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.insert(n0, key(3), vbytes(3), 4096, alloc, noSplit, None)(using tx)
      _ <- tx.commit()
      tx2 = client.newTransaction()
      n1 <- readNode(n0.pointer, key(1))
      _ <- enc.insert(n1, key(5), vbytes(9), 4096, alloc, noSplit, None)(using tx2)
      _ <- tx2.commit()
      n2 <- readNode(n0.pointer, key(1))
    yield
      assert(asBytes(n1.entries) == asBytes(List(key(1) -> vbytes(1), key(3) -> vbytes(3), key(5) -> vbytes(5))))
      assert(n1.revision == tx.revision)
      assert(sameLink(n1.link, link))
      assert(n1.minimum == key(1))
      assert(asBytes(n2.entries) == asBytes(List(key(1) -> vbytes(1), key(3) -> vbytes(3), key(5) -> vbytes(9))))

  atest("insert that overflows splits per SplitPlan and calls prepareForSplit with the new node"):
    val originalLink = Some(NodeLink(key(100), fakePointer(1)))
    val original = (1 to 6).toList.map(i => key(i * 10) -> vbytes(i, 100))
    val newKey = key(35)
    val newValue = vbytes(35, 100)
    val maxNodeSize = enc.encodedSize(original, Key.AbsoluteMinimum, originalLink) + 50
    val all = (newKey -> newValue :: original).sortWith((a, b) => ordering.compare(a._1, b._1) < 0)
    val expectedMoved = SplitPlan.keysToMove(all.map((k, v) => k -> enc.entrySize(k, v)))

    var split: List[(Key, ObjectPointer)] = Nil
    val onSplit: (Key, ObjectPointer) => Future[Unit] = (k, p) =>
      split = (k, p) :: split
      Future.unit

    for
      n0 <- makeNode(original, Key.AbsoluteMinimum, originalLink)
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.insert(n0, newKey, newValue, maxNodeSize, alloc, onSplit, None)(using tx)
      _ <- tx.commit()
      left <- readNode(n0.pointer)
      (newMin, newPtr) = split.head
      right <- readNode(newPtr, newMin)
      leftBytes <- client.read(n0.pointer.asInstanceOf[DataObjectPointer])
      rightBytes <- client.read(newPtr.asInstanceOf[DataObjectPointer])
    yield
      assert(split.size == 1)
      assert(newMin == expectedMoved.head)
      assert(right.entries.map(_._1) == expectedMoved)
      assert(right.minimum == newMin)
      assert(sameLink(right.link, originalLink))
      assert(sameLink(left.link, Some(NodeLink(newMin, newPtr))))
      assert(asBytes(left.entries ++ right.entries) == asBytes(all))
      assert(leftBytes.size <= maxNodeSize)
      assert(rightBytes.size <= maxNodeSize)

  /** Records each split's new minimum and node */
  private class SplitRecorder:
    var splits: List[(Key, ObjectPointer)] = Nil
    val onSplit: (Key, ObjectPointer) => Future[Unit] = (k, p) =>
      splits = (k, p) :: splits
      Future.unit

  /** Inserts and commits, then returns the left node and the split's new right node */
  private def insertAndSplit(n0: DataObjectListNode, k: Key, v: Value,
                             maxNodeSize: Int): Future[(DataObjectListNode, DataObjectListNode)] =
    val rec = new SplitRecorder
    for
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.insert(n0, k, v, maxNodeSize, alloc, rec.onSplit, None)(using tx)
      _ <- tx.commit()
      left <- readNode(n0.pointer)
      right <- readNode(rec.splits.head._2)
    yield
      assert(rec.splits.size == 1)
      (left, right)

  private def objectSize(node: ListNode): Future[Int] =
    client.read(node.pointer.asInstanceOf[DataObjectPointer]).map(_.size)

  atest("a split that SplitPlan would leave with an oversized left node uses another split point"):
    // SplitPlan moves only key(3), leaving the big entry in a left node that now needs a link
    val original = List(key(1) -> vbytes(1, 10), key(2) -> vbytes(2, 900))
    val maxNodeSize = enc.encodedSize(original, Key.AbsoluteMinimum, None) + 5
    for
      n0 <- makeNode(original)
      (left, right) <- insertAndSplit(n0, key(3), vbytes(3, 10), maxNodeSize)
      leftSize <- objectSize(left)
      rightSize <- objectSize(right)
    yield
      assert(left.entries.map(_._1) == List(key(1)))
      assert(right.entries.map(_._1) == List(key(2), key(3)))
      assert(sameLink(left.link, Some(NodeLink(key(2), right.pointer))))
      assert(leftSize <= maxNodeSize)
      assert(rightSize <= maxNodeSize)

  atest("the largest accepted entry still lets a later key above it split off into its own node"):
    val maxNodeSize = 1000
    val a = key(10)
    val b = key(20)
    for
      n0 <- makeNode(Nil)
      alloc <- allocator()
      // Room is kept for a link as long as one to the entry's own key
      reserve = enc.nodeOverhead(Key.AbsoluteMinimum, Some(NodeLink(a, n0.pointer)))
      vlen = (0 to maxNodeSize).filter(v => enc.entrySize(a, vbytes(1, v)) <= maxNodeSize - reserve).max
      tx0 = client.newTransaction()
      tooBig <- Future.delegate(enc.insert(n0, a, vbytes(1, vlen + 1), maxNodeSize, alloc, noSplit,
        None)(using tx0)).failed
      tx1 = client.newTransaction()
      _ <- enc.insert(n0, a, vbytes(1, vlen), maxNodeSize, alloc, noSplit, None)(using tx1)
      _ <- tx1.commit()
      n1 <- readNode(n0.pointer)
      (left, right) <- insertAndSplit(n1, b, vbytes(2, 100), maxNodeSize)
      leftSize <- objectSize(left)
    yield
      assert(vlen > 900)
      assert(tooBig.isInstanceOf[NodeSizeExceeded])
      assert(tx0.isEmpty)
      assert(left.entries.map(_._1) == List(a))
      assert(right.entries.map(_._1) == List(b))
      assert(leftSize <= maxNodeSize)

  atest("an insert below a long-keyed big entry splits where both nodes fit"):
    val a = key(1)
    val s = key(2)
    val bigKey = Key(Array[Byte](3) ++ Array.fill[Byte](199)(7))
    val bigValue = vbytes(3, 300)
    val nodeLink = Some(NodeLink(Key(Array.fill[Byte](20)(9)), fakePointer(1)))
    for
      n0 <- makeNode(List(a -> vbytes(1), bigKey -> bigValue), Key.AbsoluteMinimum, nodeLink)
      // The big entry is exactly as large as the node accepts
      maxNodeSize = math.max(enc.nodeOverhead(Key.AbsoluteMinimum, nodeLink),
        enc.nodeOverhead(Key.AbsoluteMinimum, Some(NodeLink(bigKey, n0.pointer)))) + enc.entrySize(bigKey, bigValue)
      // s overflows the node, but s and the big entry fit together in a node whose minimum is s
      sv = (1 to 1000).map(v => vbytes(2, v)).find: v =>
        enc.encodedSize(List(a -> vbytes(1), s -> v, bigKey -> bigValue), Key.AbsoluteMinimum, nodeLink) > maxNodeSize &&
          enc.encodedSize(List(s -> v, bigKey -> bigValue), s, nodeLink) <= maxNodeSize
      (left, right) <- insertAndSplit(n0, s, sv.get, maxNodeSize)
      leftSize <- objectSize(left)
      rightSize <- objectSize(right)
    yield
      // SplitPlan alone would move only the big entry, into a node too large with the long key as its minimum
      assert(enc.encodedSize(List(bigKey -> bigValue), bigKey, nodeLink) > maxNodeSize)
      assert(left.entries.map(_._1) == List(a))
      assert(right.entries.map(_._1) == List(s, bigKey))
      assert(right.minimum == s)
      assert(sameLink(right.link, nodeLink))
      assert(leftSize <= maxNodeSize)
      assert(rightSize <= maxNodeSize)

  atest("an insert that no split can hold throws NodeSizeExceeded with nothing staged"):
    // Built directly: a node this full could not be reached through insert
    val maxNodeSize = 1000
    val original = List(key(1) -> vbytes(1, 990))
    for
      n0 <- makeNode(original)
      alloc <- allocator()
      tx = client.newTransaction()
      err <- Future.delegate(enc.insert(n0, key(2), vbytes(2, 10), maxNodeSize, alloc, noSplit,
        None)(using tx)).failed
      n1 <- readNode(n0.pointer)
    yield
      assert(enc.encodedSize(original, Key.AbsoluteMinimum, None) <= maxNodeSize)
      assert(enc.encodedSize(original :+ (key(2) -> vbytes(2, 10)), Key.AbsoluteMinimum, None) > maxNodeSize)
      assert(err.isInstanceOf[NodeSizeExceeded])
      assert(tx.isEmpty)
      assert(n1.revision == n0.revision)

  atest("insert with a defined requirement throws UnsupportedRequirement"):
    for
      n0 <- makeNode(List(key(1) -> vbytes(1)))
      alloc <- allocator()
      tx = client.newTransaction()
      r1 <- Future.delegate(enc.insert(n0, key(2), vbytes(2), 4096, alloc, noSplit, Some(Left(true)))(using tx))
        .failed
      r2 <- Future.delegate(enc.insert(n0, key(2), vbytes(2), 4096, alloc, noSplit,
        Some(Right(n0.revision)))(using tx)).failed
    yield
      assert(r1.isInstanceOf[UnsupportedRequirement])
      assert(r2.isInstanceOf[UnsupportedRequirement])
      assert(tx.isEmpty)

  atest("an entry too large for the node limit throws NodeSizeExceeded with nothing staged"):
    val maxNodeSize = 200
    for
      n0 <- makeNode(List(key(1) -> vbytes(1)))
      alloc <- allocator()
      tx = client.newTransaction()
      err <- Future.delegate(enc.insert(n0, key(2), vbytes(2, maxNodeSize), maxNodeSize, alloc, noSplit,
        None)(using tx)).failed
      n1 <- readNode(n0.pointer)
    yield
      assert(err.isInstanceOf[NodeSizeExceeded])
      assert(tx.isEmpty)
      assert(n1.revision == n0.revision)
      assert(asBytes(n1.entries) == asBytes(n0.entries))

  // ---------------------------------------------------------------------------------------------------------------
  // Delete

  atest("deleting a non-last key rewrites the node"):
    val link = Some(NodeLink(key(50), fakePointer(1)))
    for
      n0 <- makeNode(List(key(1) -> vbytes(1), key(2) -> vbytes(2), key(3) -> vbytes(3)), key(1), link)
      tx = client.newTransaction()
      _ <- enc.delete(n0, key(2), 4096, noJoin)(using tx)
      _ <- tx.commit()
      n1 <- readNode(n0.pointer, key(1))
      tx2 = client.newTransaction()
      _ <- enc.delete(n1, key(99), 4096, noJoin)(using tx2)
    yield
      assert(asBytes(n1.entries) == asBytes(List(key(1) -> vbytes(1), key(3) -> vbytes(3))))
      assert(sameLink(n1.link, link))
      assert(tx2.isEmpty)

  atest("deleting the last key of a node with a right link joins the right node into it"):
    val rightLink = Some(NodeLink(key(90), fakePointer(1)))
    val rightEntries = List(key(50) -> vbytes(50), key(60) -> vbytes(60))

    var joined: List[(Key, ObjectPointer)] = Nil
    val onJoin: (Key, ObjectPointer) => Future[Unit] = (k, p) =>
      joined = (k, p) :: joined
      Future.unit

    for
      right <- makeNode(rightEntries, key(50), rightLink)
      left <- makeNode(List(key(1) -> vbytes(1)), Key.AbsoluteMinimum, Some(NodeLink(key(50), right.pointer)))
      tx = client.newTransaction()
      _ <- enc.delete(left, key(1), 4096, onJoin)(using tx)
      _ <- tx.commit()
      l1 <- readNode(left.pointer)
      oright <- client.readOptional(right.pointer.asInstanceOf[DataObjectPointer])
    yield
      assert(joined.size == 1)
      assert(joined.head._1 == key(50))
      assert(joined.head._2 == right.pointer)
      assert(asBytes(l1.entries) == asBytes(rightEntries))
      assert(l1.minimum == Key.AbsoluteMinimum)
      assert(sameLink(l1.link, rightLink))
      assert(oright.isEmpty)

  // The left minimum is much longer than the right one, so the joined node is larger than the right node
  private val longLeftMinimum = Key(Array.fill[Byte](60)(1))
  private val joinRightLink = Some(NodeLink(key(90), fakePointer(1)))
  private val joinRightEntries = List(key(50) -> vbytes(50), key(60) -> vbytes(60))
  private val joinedSize = enc.encodedSize(joinRightEntries, longLeftMinimum, joinRightLink)

  atest("a join larger than the right node but within maxNodeSize happens"):
    var joined: List[(Key, ObjectPointer)] = Nil
    val onJoin: (Key, ObjectPointer) => Future[Unit] = (k, p) =>
      joined = (k, p) :: joined
      Future.unit
    for
      right <- makeNode(joinRightEntries, key(50), joinRightLink)
      left <- makeNode(List(longLeftMinimum -> vbytes(1)), longLeftMinimum, Some(NodeLink(key(50), right.pointer)))
      tx = client.newTransaction()
      _ <- enc.delete(left, longLeftMinimum, joinedSize, onJoin)(using tx)
      _ <- tx.commit()
      l1 <- readNode(left.pointer)
      oright <- client.readOptional(right.pointer.asInstanceOf[DataObjectPointer])
    yield
      assert(joinedSize > enc.encodedSize(joinRightEntries, key(50), joinRightLink))
      assert(joined.map(_._2) == List(right.pointer))
      assert(asBytes(l1.entries) == asBytes(joinRightEntries))
      assert(l1.minimum == longLeftMinimum)
      assert(sameLink(l1.link, joinRightLink))
      assert(oright.isEmpty)

  atest("a join larger than maxNodeSize leaves the left node empty and linked instead"):
    val leftMinimum = longLeftMinimum
    val rightLink = joinRightLink
    val rightEntries = joinRightEntries
    for
      right <- makeNode(rightEntries, key(50), rightLink)
      rightLinkFromLeft = Some(NodeLink(key(50), right.pointer))
      left <- makeNode(List(leftMinimum -> vbytes(1)), leftMinimum, rightLinkFromLeft)
      tx = client.newTransaction()
      _ <- enc.delete(left, leftMinimum, joinedSize - 1, noJoin)(using tx)
      _ <- tx.commit()
      l1 <- readNode(left.pointer)
      r1 <- readNode(right.pointer)
    yield
      assert(l1.entries.isEmpty)
      assert(l1.minimum == leftMinimum)
      assert(sameLink(l1.link, rightLinkFromLeft))
      assert(r1.revision == right.revision)
      assert(asBytes(r1.entries) == asBytes(rightEntries))
      assert(sameLink(r1.link, rightLink))

  atest("a join aborts if the right node changes before it commits"):
    val rightEntries = List(key(50) -> vbytes(50))
    for
      right <- makeNode(rightEntries, key(50))
      left <- makeNode(List(key(1) -> vbytes(1)), Key.AbsoluteMinimum, Some(NodeLink(key(50), right.pointer)))
      alloc <- allocator()
      tx = client.newTransaction()
      // Modify the right node after the join has read and staged it
      _ <- enc.delete(left, key(1), 4096, (_, _) =>
        for
          r <- readNode(right.pointer, key(50))
          tx2 = client.newTransaction()
          _ <- enc.insert(r, key(70), vbytes(70), 4096, alloc, noSplit, None)(using tx2)
          _ <- tx2.commit()
        yield
          ())(using tx)
      joinResult <- tx.commit().map(_ => true).recover(_ => false)
      l1 <- readNode(left.pointer)
      r1 <- readNode(right.pointer, key(50))
    yield
      assert(!joinResult)
      assert(asBytes(l1.entries) == asBytes(List(key(1) -> vbytes(1))))
      assert(asBytes(r1.entries) == asBytes(rightEntries :+ (key(70) -> vbytes(70))))
      assert(r1.refcount == right.refcount)

  atest("deleting the last key of the last node leaves an empty node"):
    for
      n0 <- makeNode(List(key(1) -> vbytes(1)))
      tx = client.newTransaction()
      _ <- enc.delete(n0, key(1), 4096, noJoin)(using tx)
      _ <- tx.commit()
      n1 <- readNode(n0.pointer)
    yield
      assert(n1.entries.isEmpty)
      assert(n1.link.isEmpty)

  // ---------------------------------------------------------------------------------------------------------------
  // splitAt

  atest("splitAt keeps lower keys in place and returns a two-node list holding the rest"):
    val originalLink = Some(NodeLink(key(100), fakePointer(1)))
    val down = fakePointer(2)
    val original = (1 to 6).toList.map(i => key(i) -> vbytes(i))
    for
      n0 <- makeNode(original, Key.AbsoluteMinimum, originalLink)
      alloc <- allocator()
      tx = client.newTransaction()
      head <- enc.splitAt(n0, key(4), false, 4096, Some(down), alloc)(using tx)
      _ <- tx.commit()
      n1 <- readNode(n0.pointer)
      newLeft <- readNode(head.pointer)
      newRight <- readNode(newLeft.link.get.pointer, key(4))
    yield
      assert(head.minimum == Key.AbsoluteMinimum)
      assert(asBytes(n1.entries) == asBytes(original.take(3)))
      assert(n1.link.isEmpty)
      assert(asBytes(newLeft.entries) == asBytes(List(Key.AbsoluteMinimum -> Value(down.toArray))))
      assert(newLeft.minimum == Key.AbsoluteMinimum)
      assert(newLeft.link.get.minimum == key(4))
      assert(asBytes(newRight.entries) == asBytes(original.drop(3)))
      assert(newRight.minimum == key(4))
      assert(sameLink(newRight.link, originalLink))

  atest("after splitAt the left node has no maximum and accepts keys at or above the split key"):
    val original = (1 to 6).toList.map(i => key(i) -> vbytes(i))
    for
      n0 <- makeNode(original)
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.splitAt(n0, key(4), false, 4096, None, alloc)(using tx)
      _ <- tx.commit()
      n1 <- readNode(n0.pointer)
      tx2 = client.newTransaction()
      _ <- enc.insert(n1, key(9), vbytes(9), 4096, alloc, noSplit, None)(using tx2)
      _ <- tx2.commit()
      n2 <- readNode(n0.pointer)
    yield
      assert(n1.maximum.isEmpty)
      assert(n1.keyInRange(key(9)))
      assert(asBytes(n2.entries) == asBytes(original.take(3) :+ (key(9) -> vbytes(9))))

  atest("inclusive splitAt without a down pointer keeps the split key and leaves the new head empty"):
    val original = (1 to 6).toList.map(i => key(i) -> vbytes(i))
    for
      n0 <- makeNode(original)
      alloc <- allocator()
      tx = client.newTransaction()
      head <- enc.splitAt(n0, key(4), true, 4096, None, alloc)(using tx)
      _ <- tx.commit()
      n1 <- readNode(n0.pointer)
      newLeft <- readNode(head.pointer)
      newRight <- readNode(newLeft.link.get.pointer, key(4))
    yield
      assert(asBytes(n1.entries) == asBytes(original.take(4)))
      assert(newLeft.entries.isEmpty)
      assert(asBytes(newRight.entries) == asBytes(original.drop(4)))
      assert(newRight.link.isEmpty)

  // A node whose minimum is the empty absolute minimum splits at a long key: the right node's minimum is that key, so
  // the right node is larger than the node it came from
  private val longSplitKey = Key(Array.fill[Byte](100)(7))

  atest("splitAt allows a right node that exactly fits maxNodeSize"):
    val original = List(key(1) -> vbytes(1, 10), key(8) -> vbytes(8, 10))
    val right = original.drop(1)
    val maxNodeSize = enc.encodedSize(right, longSplitKey, None)
    for
      n0 <- makeNode(original)
      alloc <- allocator()
      tx = client.newTransaction()
      head <- enc.splitAt(n0, longSplitKey, false, maxNodeSize, None, alloc)(using tx)
      _ <- tx.commit()
      newLeft <- readNode(head.pointer)
      newRight <- readNode(newLeft.link.get.pointer, longSplitKey)
    yield
      assert(enc.encodedSize(original, Key.AbsoluteMinimum, None) < maxNodeSize)
      assert(asBytes(newRight.entries) == asBytes(right))

  atest("splitAt of a right node past maxNodeSize throws NodeSizeExceeded with nothing staged"):
    val original = List(key(1) -> vbytes(1, 10), key(8) -> vbytes(8, 10))
    val maxNodeSize = enc.encodedSize(original.drop(1), longSplitKey, None) - 1
    for
      n0 <- makeNode(original)
      alloc <- allocator()
      tx = client.newTransaction()
      err <- Future.delegate(enc.splitAt(n0, longSplitKey, false, maxNodeSize, None, alloc)(using tx)).failed
      n1 <- readNode(n0.pointer)
    yield
      assert(enc.encodedSize(original, Key.AbsoluteMinimum, None) <= maxNodeSize)
      assert(err.isInstanceOf[NodeSizeExceeded])
      assert(tx.isEmpty)
      assert(n1.revision == n0.revision)

  // ---------------------------------------------------------------------------------------------------------------
  // deleteList

  /** A three-node list: head (absolute minimum) -> key(10) -> key(20) */
  private def threeNodeList(): Future[List[DataObjectListNode]] =
    for
      c <- makeNode(List(key(20) -> vbytes(20), key(21) -> vbytes(21)), key(20))
      b <- makeNode(List(key(10) -> vbytes(10)), key(10), Some(NodeLink(key(20), c.pointer)))
      a <- makeNode(List(key(1) -> vbytes(1), key(2) -> vbytes(2)), Key.AbsoluteMinimum,
        Some(NodeLink(key(10), b.pointer)))
    yield
      List(a, b, c)

  atest("deleteList visits every entry and deletes every node"):
    var visited: List[(Key, Value)] = Nil
    val onEntry: (Key, Value) => Future[Unit] = (k, v) =>
      synchronized { visited = (k, v) :: visited }
      Future.unit
    for
      nodes <- threeNodeList()
      _ <- enc.deleteList(client, NodeLink(Key.AbsoluteMinimum, nodes.head.pointer), ordering, Some(onEntry))
      remaining <- Future.sequence(nodes.map(n => client.readOptional(n.pointer.asInstanceOf[DataObjectPointer])))
    yield
      assert(asBytes(visited).toSet == asBytes(nodes.flatMap(_.entries)).toSet)
      assert(visited.size == 5)
      assert(remaining.forall(_.isEmpty))

  atest("deleteList without a callback deletes every node and tolerates an already deleted list"):
    for
      nodes <- threeNodeList()
      head = NodeLink(Key.AbsoluteMinimum, nodes.head.pointer)
      _ <- enc.deleteList(client, head, ordering, None)
      remaining <- Future.sequence(nodes.map(n => client.readOptional(n.pointer.asInstanceOf[DataObjectPointer])))
      _ <- enc.deleteList(client, head, ordering, None)
    yield
      assert(remaining.forall(_.isEmpty))

  // ---------------------------------------------------------------------------------------------------------------
  // Same-node guard

  private def alreadyModified(err: Throwable, node: ListNode): Boolean = err match
    case NodeAlreadyModified(p) => p == node.pointer
    case _ => false

  atest("a second insert into a node already written in the transaction fails and leaves the first staged"):
    for
      n0 <- makeNode(List(key(1) -> vbytes(1)))
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.insert(n0, key(2), vbytes(2), 4096, alloc, noSplit, None)(using tx)
      err <- Future.delegate(enc.insert(n0, key(3), vbytes(3), 4096, alloc, noSplit, None)(using tx)).failed
      _ <- tx.commit()
      n1 <- readNode(n0.pointer)
    yield
      assert(alreadyModified(err, n0))
      assert(asBytes(n1.entries) == asBytes(List(key(1) -> vbytes(1), key(2) -> vbytes(2))))

  atest("a delete from a node already written in the transaction fails"):
    for
      n0 <- makeNode(List(key(1) -> vbytes(1), key(2) -> vbytes(2)))
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.insert(n0, key(3), vbytes(3), 4096, alloc, noSplit, None)(using tx)
      err <- Future.delegate(enc.delete(n0, key(1), 4096, noJoin)(using tx)).failed
    yield
      assert(alreadyModified(err, n0))

  atest("a split of a node already written in the transaction aborts it, since its new node is already staged"):
    val original = List(key(1) -> vbytes(1, 100), key(2) -> vbytes(2, 100))
    // Room for the two original entries plus a link, but not for a third entry
    val maxNodeSize = enc.encodedSize(original, Key.AbsoluteMinimum, None) + 80
    assert(enc.encodedSize(original :+ (key(3) -> vbytes(3, 100)), Key.AbsoluteMinimum, None) > maxNodeSize)
    for
      n0 <- makeNode(original)
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.delete(n0, key(2), 4096, noJoin)(using tx)
      err <- Future.delegate(enc.insert(n0, key(3), vbytes(3, 100), maxNodeSize, alloc, noSplit, None)(using tx))
        .failed
      txErr <- tx.result.failed
      n1 <- readNode(n0.pointer)
    yield
      assert(alreadyModified(err, n0))
      assert(alreadyModified(txErr, n0))
      assert(n1.revision == n0.revision)

  atest("a join whose left node is already written in the transaction fails"):
    for
      right <- makeNode(List(key(50) -> vbytes(50)), key(50))
      left <- makeNode(List(key(1) -> vbytes(1)), Key.AbsoluteMinimum, Some(NodeLink(key(50), right.pointer)))
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.insert(left, key(2), vbytes(2), 4096, alloc, noSplit, None)(using tx)
      err <- Future.delegate(enc.delete(left, key(1), 4096, noJoin)(using tx)).failed
    yield
      assert(alreadyModified(err, left))

  atest("a join whose right node is already written in the transaction aborts it"):
    for
      right <- makeNode(List(key(50) -> vbytes(50)), key(50))
      left <- makeNode(List(key(1) -> vbytes(1)), Key.AbsoluteMinimum, Some(NodeLink(key(50), right.pointer)))
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.insert(right, key(60), vbytes(60), 4096, alloc, noSplit, None)(using tx)
      err <- Future.delegate(enc.delete(left, key(1), 4096, noJoin)(using tx)).failed
      txErr <- tx.result.failed
      l1 <- readNode(left.pointer)
    yield
      assert(alreadyModified(err, right))
      assert(alreadyModified(txErr, right))
      assert(l1.revision == left.revision)

  atest("writing a node a join in the same transaction absorbed fails"):
    var joined = false
    val onJoin: (Key, ObjectPointer) => Future[Unit] = (_, _) =>
      joined = true
      Future.unit
    for
      right <- makeNode(List(key(50) -> vbytes(50)), key(50))
      left <- makeNode(List(key(1) -> vbytes(1)), Key.AbsoluteMinimum, Some(NodeLink(key(50), right.pointer)))
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.delete(left, key(1), 4096, onJoin)(using tx)
      err <- Future.delegate(enc.insert(right, key(60), vbytes(60), 4096, alloc, noSplit, None)(using tx)).failed
    yield
      assert(joined)
      assert(alreadyModified(err, right))

  atest("splitAt of a node already written in the transaction fails before allocating"):
    val original = (1 to 6).toList.map(i => key(i) -> vbytes(i))
    for
      n0 <- makeNode(original)
      alloc <- allocator()
      tx = client.newTransaction()
      _ <- enc.delete(n0, key(1), 4096, noJoin)(using tx)
      err <- Future.delegate(enc.splitAt(n0, key(4), false, 4096, None, alloc)(using tx)).failed
      _ <- tx.commit()
      n1 <- readNode(n0.pointer)
    yield
      assert(alreadyModified(err, n0))
      assert(asBytes(n1.entries) == asBytes(original.drop(1)))
