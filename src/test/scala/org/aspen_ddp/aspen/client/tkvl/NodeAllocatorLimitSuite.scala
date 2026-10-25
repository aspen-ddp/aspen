package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, ObjectAllocatorId, PoolObjectAllocatorId, Transaction}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.{RandomAccessReedSolomon, IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Key, KeyValueObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.server.store.{KVObjectState, ValueState}
import org.scalatest.compatible.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object NodeAllocatorLimitSuite:
  val sliceLimit: Int = 4096

  /** Resolves `f` to the NodeSizeExceedsAllocatorLimit it fails with, or fails the test */
  def limitError(f: Future[Any])(using ExecutionContext): Future[NodeSizeExceedsAllocatorLimit] =
    f.transform:
      case Failure(e: NodeSizeExceedsAllocatorLimit) => Success(e)
      case other => Failure(new Exception(s"Expected NodeSizeExceedsAllocatorLimit, got $other"))

  /** An allocator whose reported limit can be changed, standing in for a group whose limit shrank or whose
    * snapshot of a member is stale. Objects are created through `delegate`.
    */
  class StubAllocator(val delegate: ObjectAllocator, var limit: Option[Int]) extends ObjectAllocator:
    val client: AspenClient = delegate.client
    val allocatorId: ObjectAllocatorId = delegate.allocatorId
    def maxObjectSize: Option[Int] = limit
    protected[client] def createDataObjectPointer()(using t: Transaction): Future[DataObjectPointer] =
      delegate.createDataObjectPointer()
    protected[client] def createKeyValueObjectPointer()(using t: Transaction): Future[KeyValueObjectPointer] =
      delegate.createKeyValueObjectPointer()

  /** A NodeAllocator whose single tier allocates through `stub` */
  class StubNodeAllocator(client: AspenClient, stub: StubAllocator, maxNodeSize: Int)
    extends NodeAllocator(client, List((stub.allocatorId, maxNodeSize))):
    override protected def lookupAllocator(allocatorId: ObjectAllocatorId): Future[ObjectAllocator] =
      Future.successful(stub)


/** Fills a single node to the node code's own limit with tiny entries and checks every insert commits (the
  * stores vote abort on any slice above `sliceLimit`) and that the stored slice stays within the limit.
  */
trait NodeFillBehaviour:
  this: IntegrationTestSuite =>

  import NodeAllocatorLimitSuite.sliceLimit

  override def testPoolMaxSliceSize: Option[Int] = Some(sliceLimit)
  override def testStoreMaxSliceSize: Option[Long] = Some(sliceLimit.toLong)

  def fillNode(minKey: Key, tailKey: Key, entryKeyPrefix: Array[Byte]): Future[Assertion] =
    given ExecutionContext = executionContext

    val nodeAllocator = new NodeAllocator(client, List((PoolObjectAllocatorId(Radicle.poolId), sliceLimit)))
    val tail = KeyValueListPointer(tailKey, radicle)

    def entryKey(i: Int): Key = Key(entryKeyPrefix ++ Array[Byte]((i >> 8).toByte, i.toByte))

    /** Inserts entry i; true if it committed in place, false if the node code chose to split instead */
    def insertOnce(alloc: ObjectAllocator, ptr: KeyValueObjectPointer, i: Int): Future[Boolean] =
      var split = false
      val tx = client.newTransaction()
      for
        kvos <- client.read(ptr)
        node = KeyValueListNode(client, KeyValueListPointer(minKey, ptr), ByteArrayKeyOrdering, kvos)
        _ <- node.insert(entryKey(i), Value(Array[Byte]()), sliceLimit, alloc,
          (_, _) => { split = true; Future.unit })(using tx)
        committed <-
          if split then
            tx.abort(new Exception("split not wanted"))
            Future.successful(false)
          else
            tx.commit().map(_ => true)
      yield committed

    def fill(alloc: ObjectAllocator, ptr: KeyValueObjectPointer, i: Int): Future[Int] =
      insertOnce(alloc, ptr, i).flatMap: inPlace =>
        if inPlace then
          fill(alloc, ptr, i + 1)
        else
          Future.successful(i)

    for
      _ <- nodeAllocator.validate()
      alloc <- nodeAllocator.getAllocatorForTier(0)
      tx0 = client.newTransaction()
      kvos0 <- client.read(radicle)
      _ = tx0.bumpVersion(radicle, kvos0.revision)
      ptr <- alloc.allocateKeyValueObject(Map(), Some(minKey), Some(tailKey), None, Some(Value(tail.toArray)))(using tx0)
      _ <- tx0.commit()
      inserted <- fill(alloc, ptr, 0)
      kvos <- client.read(ptr)
    yield
      val contents = kvos.contents.map((k, vs) => k -> new ValueState(vs.value, vs.revision, vs.timestamp, None))
      val storedSlice = KVObjectState.encodedSizeIDA(kvos.ida, kvos.minimum, kvos.maximum, kvos.left, kvos.right,
        contents)
      inserted should be > 1
      kvos.contents.size should be (inserted)
      storedSlice should be <= sliceLimit


class NodeAllocatorLimitSuite extends IntegrationTestSuite with NodeFillBehaviour:
  import NodeAllocatorLimitSuite.*

  private val poolTier = PoolObjectAllocatorId(Radicle.poolId)

  private def allocator(sizes: Int*): NodeAllocator =
    new NodeAllocator(client, sizes.map(sz => (poolTier, sz)).toList)

  atest("a tier whose maxNodeSize equals the allocator limit validates"):
    allocator(sliceLimit).validate().map(_ => succeed)

  atest("a tier one byte above the limit fails naming the tier, requested size and limit"):
    limitError(allocator(sliceLimit, sliceLimit + 1).validate()).map: err =>
      err.tier should be (1)
      err.requested should be (sliceLimit + 1)
      err.limit should be (sliceLimit)

  atest("Root.create rejects a node size above the limit"):
    given tx: Transaction = client.newTransaction()
    for
      empty <- limitError(Root.create(client, ByteArrayKeyOrdering, allocator(sliceLimit + 1)))
      nonEmpty <- limitError(Root.create(client, ByteArrayKeyOrdering, allocator(sliceLimit, sliceLimit + 1),
        Map(Key(Array[Byte](1)) -> Value(Array[Byte](2)))))
    yield
      empty.tier should be (0)
      nonEmpty.tier should be (1)

  atest("Root.create accepts a node size at the limit"):
    given tx: Transaction = client.newTransaction()
    Root.create(client, ByteArrayKeyOrdering, allocator(sliceLimit)).map: root =>
      root.nodeAllocator.getMaxNodeSize(0) should be (sliceLimit)

  atest("a decoded root with a bad node size fails when it allocates a node"):
    val decoded = Root(client, Root(0, ByteArrayKeyOrdering, None, allocator(sliceLimit, sliceLimit + 1)).encode())
    for
      v <- limitError(decoded.nodeAllocator.validate())
      t0 <- decoded.nodeAllocator.getAllocatorForTier(0)
      t1 <- limitError(decoded.nodeAllocator.getAllocatorForTier(1))
      t3 <- limitError(decoded.nodeAllocator.getAllocatorForTier(3))
    yield
      v.tier should be (1)
      t1.tier should be (1)
      // Tiers above the configured ones reuse the last tier's configuration and are checked as themselves
      t3.tier should be (3)
      t3.requested should be (sliceLimit + 1)

  atest("node allocation re-checks the allocator's current limit"):
    for
      pool <- client.getStoragePool(Radicle.poolId)
      stub = new StubAllocator(pool.allocator, Some(sliceLimit))
      na = new StubNodeAllocator(client, stub, sliceLimit)
      _ <- na.validate()
      _ = stub.limit = Some(1024)
      err <- limitError(na.getAllocatorForTier(0))
    yield
      err.tier should be (0)
      err.requested should be (sliceLimit)
      err.limit should be (1024)

  atest("an allocator reporting a stale limit fails where the node lands"):
    given tx: Transaction = client.newTransaction()
    for
      pool <- client.getStoragePool(Radicle.poolId)
      stub = new StubAllocator(pool.allocator, None)
      na = new StubNodeAllocator(client, stub, sliceLimit * 2)
      _ <- na.validate()
      alloc <- na.getAllocatorForTier(0)
      err <- limitError(alloc.allocateKeyValueObject(Map()))
    yield
      err.tier should be (0)
      err.requested should be (sliceLimit * 2)
      err.limit should be (sliceLimit)

  atest("a split finalization whose upper tier exceeds its limit still completes"):
    val treeKey = Key(Array[Byte](9))
    // Tier 0 is valid, tier 1 is not. setNewRoot does not validate, standing in for a root written before the
    // guard or decoded from storage.
    val na = allocator(sliceLimit, sliceLimit + 1)
    for
      pool <- client.getStoragePool(Radicle.poolId)
      tx0 = client.newTransaction()
      host <- pool.allocator.allocateKeyValueObject(Map())(using tx0)
      rootNode <- pool.allocator.allocateKeyValueObject(Map())(using tx0)
      newNode <- pool.allocator.allocateKeyValueObject(Map())(using tx0)
      kvos0 <- client.read(radicle)
      _ = tx0.bumpVersion(radicle, kvos0.revision)
      _ <- tx0.commit()
      tx1 = client.newTransaction()
      frm = KVObjectRootManager.setNewRoot(client, host, treeKey, true,
        Root(0, ByteArrayKeyOrdering, Some(rootNode), na))(using tx1)
      _ <- tx1.commit()
      rootManager <- frm
      action = new SplitFinalizationAction(client, null, rootManager, 1, Key(Array[Byte](5)), newNode)
      _ = action.execute()
      // Creating the tier-1 root fails NodeSizeExceedsAllocatorLimit; the action must still complete successfully
      _ <- action.complete
      root <- rootManager.getRoot()
    yield
      root.tier should be (0)

  atest("a node filled to the node code's limit with tiny entries and long bounds fits in a slice"):
    fillNode(Key(Array.fill[Byte](200)(1)), Key(Array.fill[Byte](200)(0x7f)), Array[Byte](2))


class NodeAllocatorUnlimitedSuite extends IntegrationTestSuite:
  atest("an unlimited allocator accepts any node size"):
    new NodeAllocator(client, List((PoolObjectAllocatorId(Radicle.poolId), Int.MaxValue))).validate().map(_ => succeed)


/** RandomAccessReedSolomon slices hold whole cells, so a short right pointer occupies a full cell of every slice */
class NodeAllocatorRandomAccessFillSuite extends IntegrationTestSuite with NodeFillBehaviour:
  override def testIda: IDA = RandomAccessReedSolomon(3, 2, 2, 256)

  atest("a node filled to the node code's limit fits in an random-access reed-solomon slice"):
    fillNode(Key(Array[Byte](1)), Key(Array[Byte](0x7f)), Array[Byte](2))


/** The node code's accounting against the store's measure, for every IDA, without a network */
class NodeOverheadSuite extends AnyFunSuite with Matchers:

  private val ptr = KeyValueObjectPointer(org.aspen_ddp.aspen.common.objects.ObjectId(new UUID(1, 2)),
    org.aspen_ddp.aspen.common.pool.PoolId(new UUID(3, 4)))

  private val idas: List[IDA] = List(Replication(3, 2), ReedSolomon(5, 3, 4), RandomAccessReedSolomon(3, 2, 2, 256))

  test("a node filled to maxNodeSize less the node overhead never exceeds maxNodeSize as the store measures it"):
    for
      ida <- idas
      boundLen <- List(0, 1, 127, 128, 300)
      withTail <- List(false, true)
      valueLen <- List(0, 1, 100)
    do
      val maxNodeSize = 1 << 16
      val minimum = Key(Array.fill[Byte](boundLen)(1))
      val tail = if withTail then Some(KeyValueListPointer(Key(Array.fill[Byte](boundLen)(0x7f)), ptr)) else None
      val value = Value(Array.fill[Byte](valueLen)(5))
      var contents = Map[Key, ValueState]()
      var used = 0
      var i = 0
      var full = false
      while !full do
        val key = Key(Array[Byte](2, (i >> 16).toByte, (i >> 8).toByte, i.toByte))
        val pair = KVObjectState.idaEncodedPairSize(ida, key, value)
        val capacity = maxNodeSize - KeyValueListNode.nodeOverhead(ida, minimum, tail.map(_.minimum), tail,
          contents.size)
        if used + pair < capacity then
          contents += key -> new ValueState(value, ObjectRevision.Null, HLCTimestamp.Zero, None)
          used += pair
          i += 1
        else
          full = true
      val stored = KVObjectState.encodedSizeIDA(ida, Some(minimum), tail.map(_.minimum), None,
        tail.map(p => Value(p.toArray)), contents)
      withClue(s"$ida bounds=$boundLen tail=$withTail value=$valueLen entries=${contents.size}: "):
        stored should be <= maxNodeSize
        stored should be > maxNodeSize - 2 * KVObjectState.idaEncodedPairSize(ida, Key(Array[Byte](2, 0, 0, 0)), value)
