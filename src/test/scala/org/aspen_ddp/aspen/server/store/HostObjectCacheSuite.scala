package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Key, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.cache.HostObjectCache
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

object HostObjectCacheSuite {

  val Overhead: Long = HostObjectCache.EntryOverhead

  val store0: StoreId = StoreId(PoolId(new UUID(0, 9)), 0)
  val store1: StoreId = StoreId(PoolId(new UUID(0, 9)), 1)

  def mkobj(n: Int, dataSize: Int = 0): ObjectState = new ObjectState(
    ObjectId(new UUID(0, n)),
    Metadata(ObjectRevision(TransactionId(new UUID(0, 2))), ObjectRefcount(1, 1), HLCTimestamp(1)),
    ObjectType.Data,
    DataBuffer(new Array[Byte](dataSize))
  )

  def mkobjs(): (ObjectState, ObjectState, ObjectState, ObjectState) =
    (mkobj(1), mkobj(2), mkobj(3), mkobj(4))
}

class HostObjectCacheSuite extends AnyFunSuite with Matchers {
  import HostObjectCacheSuite._

  test("Max size") {
    val c = new HostObjectCache(3 * Overhead).forStore(store0)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1) shouldBe empty
    c.insert(o2) shouldBe empty
    c.insert(o3) shouldBe empty
    c.insert(o4) should be(List(o1))
  }

  test("Get increases priority") {
    val c = new HostObjectCache(3 * Overhead).forStore(store0)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1) shouldBe empty
    c.insert(o2) shouldBe empty
    c.insert(o3) shouldBe empty

    assert(c.get(o1.objectId).contains(o1))
    assert(c.get(o2.objectId).contains(o2))

    c.insert(o4) should be(List(o3))
  }

  test("Two element increases priority") {
    val c = new HostObjectCache(3 * Overhead).forStore(store0)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1) shouldBe empty
    c.insert(o2) shouldBe empty

    assert(c.get(o1.objectId).contains(o1))
    c.insert(o3) shouldBe empty
    assert(c.get(o2.objectId).contains(o2))

    // Access order is o1, o3, o2 (oldest first) so o1 is the eviction target
    c.insert(o4) should be(List(o1))
  }

  test("Skip locked transactions") {
    val c = new HostObjectCache(3 * Overhead).forStore(store0)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1) shouldBe empty
    c.insert(o2) shouldBe empty
    c.insert(o3) shouldBe empty

    o1.transactionReferences = 1
    o2.transactionReferences = 2

    c.insert(o4) should be(List(o3))
  }

  test("Evicts as many objects as needed to fit a large one") {
    val hc = new HostObjectCache(4 * Overhead)
    val c = hc.forStore(store0)
    val (o1, o2, o3, _) = mkobjs()

    c.insert(o1) shouldBe empty
    c.insert(o2) shouldBe empty
    c.insert(o3) shouldBe empty

    c.insert(mkobj(5, dataSize = 2 * Overhead.toInt)) should be(List(o1, o2))
    hc.sizeInBytes should be(4 * Overhead)
  }

  test("Exceeds capacity rather than evict objects referenced by transactions") {
    val hc = new HostObjectCache(2 * Overhead)
    val c = hc.forStore(store0)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1) shouldBe empty
    c.insert(o2) shouldBe empty
    o1.transactionReferences = 1
    o2.transactionReferences = 1

    c.insert(o3) shouldBe empty
    hc.sizeInBytes should be(3 * Overhead)

    // Once released, the excess is reclaimed by the next insert
    o1.transactionReferences = 0
    o2.transactionReferences = 0
    c.insert(o4) should be(List(o1, o2))
    hc.sizeInBytes should be(2 * Overhead)
  }

  test("An object larger than the capacity is retained until the next insert") {
    val hc = new HostObjectCache(2 * Overhead)
    val c = hc.forStore(store0)
    val big = mkobj(1, dataSize = 4 * Overhead.toInt)

    c.insert(big) shouldBe empty
    c.get(big.objectId) should be(Some(big))

    c.insert(mkobj(2)) should be(List(big))
  }

  test("Re-inserting an object re-measures it") {
    val hc = new HostObjectCache(100 * Overhead)
    val c = hc.forStore(store0)
    val o1 = mkobj(1)

    c.insert(o1)
    hc.sizeInBytes should be(Overhead)

    o1.data = DataBuffer(new Array[Byte](1000))
    c.insert(o1)
    hc.sizeInBytes should be(Overhead + 1000)
    hc.entryCount should be(1)

    c.remove(o1.objectId)
    hc.sizeInBytes should be(0)
  }

  test("Key-Value objects count their decoded content") {
    val o = mkobj(1)
    val kv = new ObjectState(o.objectId, o.metadata, ObjectType.KeyValue, DataBuffer.Empty)
    val emptySize = HostObjectCache.estimateSize(kv)

    kv.kvState.get.content += Key(Array[Byte](1, 2, 3)) ->
      new ValueState(Value(new Array[Byte](10)), o.metadata.revision, HLCTimestamp(1), None)

    HostObjectCache.estimateSize(kv) should be(emptySize + 3 + 10 + HostObjectCache.KeyValueEntryOverhead)
  }

  test("Stores share one budget and recency order but not objects") {
    val hc = new HostObjectCache(3 * Overhead)
    val c0 = hc.forStore(store0)
    val c1 = hc.forStore(store1)

    // Slices of the same object held by two stores of one pool
    val s0 = mkobj(1)
    val s1 = mkobj(1)

    c0.insert(s0) shouldBe empty
    c1.insert(s1) shouldBe empty
    c0.get(s0.objectId) should be(Some(s0))
    c1.get(s1.objectId) should be(Some(s1))

    val o2 = mkobj(2)
    c1.insert(o2) shouldBe empty

    // Store 1's insert evicts store 0's least recently used object
    c1.insert(mkobj(3)) should be(List(s0))
    c0.get(s0.objectId) shouldBe None
    c1.get(s1.objectId) should be(Some(s1))
  }

  test("Clearing a store drops only its objects") {
    val hc = new HostObjectCache(10 * Overhead)
    val c0 = hc.forStore(store0)
    val c1 = hc.forStore(store1)
    val (o1, o2, o3, _) = mkobjs()

    c0.insert(o1)
    c0.insert(o2)
    c1.insert(o3)

    c0.clear()
    c0.get(o1.objectId) shouldBe None
    c0.get(o2.objectId) shouldBe None
    c1.get(o3.objectId) should be(Some(o3))
    hc.sizeInBytes should be(Overhead)
  }

  test("A new view of a store starts empty") {
    val hc = new HostObjectCache(10 * Overhead)
    val o1 = mkobj(1)

    hc.forStore(store0).insert(o1)
    hc.forStore(store0).get(o1.objectId) shouldBe None
    hc.sizeInBytes should be(0)
  }
}
