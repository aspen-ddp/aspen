package org.aspen_ddp.aspen.client.tkvl

import java.util.UUID

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, Key, KeyOrdering, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.pool.PoolId

class TierCacheSuite extends IntegrationTestSuite:

  private val poolId = PoolId(new UUID(0, 5))

  private class StubNode(val pointer: ObjectPointer) extends ListNode:
    def minimum: Key = Key.AbsoluteMinimum
    def maximum: Option[Key] = None
    def link: Option[NodeLink] = None
    def ordering: KeyOrdering = ByteArrayKeyOrdering
    def revision: ObjectRevision = ObjectRevision.Null
    def refcount: ObjectRefcount = ObjectRefcount(0, 1)
    def entries: List[(Key, Value)] = Nil
    def get(k: Key): Option[Value] = None
    def size: Int = 0
    def keyInRange(k: Key): Boolean = true

  private def stub(i: Int): StubNode =
    new StubNode(KeyValueObjectPointer(ObjectId(new UUID(1, i)), poolId))

  private def rootState(tier: Int, node: Option[ListNode]): RootState =
    RootState(Root(tier, ByteArrayKeyOrdering, None, new NodeAllocator(client, Nil)), ObjectRevision.Null, node)

  test("NoCache stores nothing"):
    val n = stub(1)
    NoCache.put(1, n, NoCache.generation)
    NoCache.putRoot(rootState(1, Some(n)), NoCache.generation)
    assert(NoCache.node(1, n.pointer.id).isEmpty)
    assert(NoCache.root.isEmpty)

  test("LruTierCache ignores tier 0"):
    val c = new LruTierCache(5)
    val n = stub(1)
    c.put(0, n, c.generation)
    assert(c.node(0, n.pointer.id).isEmpty)
    c.put(1, n, c.generation)
    assert(c.node(1, n.pointer.id).contains(n))

  test("LruTierCache caches only tier >= 1 roots"):
    val c = new LruTierCache(5)
    val n = stub(1)
    c.putRoot(rootState(0, Some(n)), c.generation)
    assert(c.root.isEmpty)
    val rs = rootState(1, Some(n))
    c.putRoot(rs, c.generation)
    assert(c.root.contains(rs))
    c.putRoot(rootState(0, Some(n)), c.generation)
    assert(c.root.isEmpty)

  test("LruTierCache does not cache a root without a node"):
    val c = new LruTierCache(5)
    c.putRoot(rootState(1, None), c.generation)
    assert(c.root.isEmpty)

  test("LruTierCache bounds each tier"):
    val c = new LruTierCache(5)
    val nodes = (1 to 6).map(stub)
    nodes.foreach(c.put(1, _, c.generation))
    assert(nodes.count(n => c.node(1, n.pointer.id).isDefined) <= 5)
    val other = stub(10)
    c.put(2, other, c.generation)
    assert(c.node(2, other.pointer.id).contains(other))

  test("stale evicts the node and the root it backs"):
    val c = new LruTierCache(5)
    val rootNode = stub(1)
    val other = stub(2)
    c.put(1, rootNode, c.generation)
    c.put(1, other, c.generation)
    c.putRoot(rootState(1, Some(rootNode)), c.generation)
    c.stale(1, other)
    assert(c.node(1, other.pointer.id).isEmpty)
    assert(c.root.isDefined)
    c.stale(1, rootNode)
    assert(c.node(1, rootNode.pointer.id).isEmpty)
    assert(c.root.isEmpty)

  test("clear empties every tier and the root"):
    val c = new LruTierCache(5)
    val a = stub(1)
    val b = stub(2)
    c.put(1, a, c.generation)
    c.put(2, b, c.generation)
    c.putRoot(rootState(2, Some(b)), c.generation)
    c.clear()
    assert(c.node(1, a.pointer.id).isEmpty)
    assert(c.node(2, b.pointer.id).isEmpty)
    assert(c.root.isEmpty)

  test("a fill whose read started before a clear is ignored"):
    val c = new LruTierCache(5)
    val n = stub(1)
    val before = c.generation
    c.clear()
    c.put(1, n, before)
    c.putRoot(rootState(1, Some(n)), before)
    assert(c.node(1, n.pointer.id).isEmpty)
    assert(c.root.isEmpty)
    c.put(1, n, c.generation)
    val rs = rootState(1, Some(n))
    c.putRoot(rs, c.generation)
    assert(c.node(1, n.pointer.id).contains(n))
    assert(c.root.contains(rs))
