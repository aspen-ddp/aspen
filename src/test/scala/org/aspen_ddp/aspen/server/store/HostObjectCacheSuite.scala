package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{Key, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.cache.{HostObjectCache, StoreObjectCache}
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

  private var now = 0L
  private val clock = () => { now += 1; now }

  private def mk(capacity: Long): (HostObjectCache, StoreObjectCache) =
    val hc = new HostObjectCache(capacity, clock)
    (hc, hc.forStore(store0))

  test("evicts least recently used first") {
    val (hc, c) = mk(3 * Overhead)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1); c.insert(o2); c.insert(o3)
    c.get(o1.objectId) should be(Some(o1))
    c.insert(o4)

    c.get(o2.objectId) shouldBe None
    c.get(o1.objectId) should be(Some(o1))
    c.get(o3.objectId) should be(Some(o3))
    c.get(o4.objectId) should be(Some(o4))
    hc.sizeInBytes should be(3 * Overhead)
    c.sizeInBytes should be(3 * Overhead)
    c.entryCount should be(3)
  }

  test("evicts as many as needed to fit a large one") {
    val (hc, c) = mk(4 * Overhead)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1); c.insert(o2); c.insert(o3); c.insert(o4)
    val big = mkobj(5, dataSize = 2 * Overhead.toInt)
    c.insert(big)

    c.get(o1.objectId) shouldBe None
    c.get(o2.objectId) shouldBe None
    c.get(o3.objectId) shouldBe None
    c.get(o4.objectId) should be(Some(o4))
    c.get(big.objectId) should be(Some(big))
    hc.sizeInBytes should be(4 * Overhead)
  }

  test("an object larger than the capacity is kept until the next insert") {
    val (hc, c) = mk(2 * Overhead)
    val big = mkobj(1, dataSize = 4 * Overhead.toInt)

    c.insert(big)
    c.get(big.objectId) should be(Some(big))
    hc.sizeInBytes should be(5 * Overhead)

    val o2 = mkobj(2)
    c.insert(o2)
    c.get(big.objectId) shouldBe None
    c.get(o2.objectId) should be(Some(o2))
    hc.sizeInBytes should be(Overhead)
  }

  test("re-inserting re-measures") {
    val (hc, c) = mk(100 * Overhead)
    val o1 = mkobj(1)

    c.insert(o1)
    hc.sizeInBytes should be(Overhead)

    o1.data = DataBuffer(new Array[Byte](1000))
    c.insert(o1)
    hc.sizeInBytes should be(Overhead + 1000)
    c.entryCount should be(1)

    c.remove(o1.objectId)
    hc.sizeInBytes should be(0)
    c.entryCount should be(0)
  }

  test("key-value objects count their decoded content") {
    val o = mkobj(1)
    val kv = new ObjectState(o.objectId, o.metadata, ObjectType.KeyValue, DataBuffer.Empty)
    val emptySize = HostObjectCache.estimateSize(kv)

    kv.kvState.get.content += Key(Array[Byte](1, 2, 3)) ->
      new ValueState(Value(new Array[Byte](10)), o.metadata.revision, HLCTimestamp(1), None)

    HostObjectCache.estimateSize(kv) should be(emptySize + 3 + 10 + HostObjectCache.KeyValueEntryOverhead)
  }

  test("pinned states are counted but never evicted") {
    val (hc, c) = mk(2 * Overhead)
    val (o1, o2, o3, _) = mkobjs()

    c.insert(o1)
    c.pin(o1)
    c.pinnedCount should be(1)
    c.entryCount should be(0)
    c.insert(o2)
    c.insert(o3)

    c.get(o1.objectId) should be(Some(o1))
    c.get(o3.objectId) should be(Some(o3))
    c.get(o2.objectId) shouldBe None
    hc.sizeInBytes should be(2 * Overhead)
  }

  test("unpinning returns a cached state at the most recently used end") {
    val (_, c) = mk(3 * Overhead)
    val (o1, o2, o3, o4) = mkobjs()

    c.insert(o1); c.insert(o2); c.insert(o3)
    c.pin(o1)
    c.unpin(o1)
    c.pinnedCount should be(0)
    c.insert(o4)

    c.get(o2.objectId) shouldBe None
    c.get(o1.objectId) should be(Some(o1))
  }

  test("unpinning an uncached state stops counting it") {
    val (hc, c) = mk(10 * Overhead)
    val x = mkobj(1)

    c.pin(x)
    hc.sizeInBytes should be(Overhead)
    c.sizeInBytes should be(Overhead)
    c.get(x.objectId) shouldBe None
    c.unpin(x)
    hc.sizeInBytes should be(0)
    c.get(x.objectId) shouldBe None
  }

  test("unpinning a discarded state stops counting it") {
    val (hc, c) = mk(10 * Overhead)
    val a = mkobj(1)

    c.insert(a)
    c.pin(a)
    a.reloadRequired = true
    c.unpin(a)

    c.get(a.objectId) shouldBe None
    hc.sizeInBytes should be(0)
  }

  test("pinning twice is a no-op") {
    val (hc, c) = mk(10 * Overhead)
    val a = mkobj(1)

    c.pin(a)
    c.pin(a)
    hc.sizeInBytes should be(Overhead)
    c.pinnedCount should be(1)
  }

  test("inserting a pinned state marks it cached") {
    val (hc, c) = mk(10 * Overhead)
    val a = mkobj(1)

    c.pin(a)
    a.data = DataBuffer(new Array[Byte](100))
    c.insert(a)
    hc.sizeInBytes should be(Overhead + 100)
    c.entryCount should be(0)
    c.get(a.objectId) should be(Some(a))
    c.unpin(a)
    c.entryCount should be(1)
    hc.sizeInBytes should be(Overhead + 100)
  }

  test("stores share one host total but not objects") {
    val hc = new HostObjectCache(10 * Overhead, clock)
    val c0 = hc.forStore(store0)
    val c1 = hc.forStore(store1)
    val s0 = mkobj(1)
    val s1 = mkobj(1)

    c0.insert(s0)
    c1.insert(s1)
    c0.get(s0.objectId).get should be theSameInstanceAs s0
    c1.get(s1.objectId).get should be theSameInstanceAs s1
    hc.sizeInBytes should be(2 * Overhead)
    c0.sizeInBytes should be(Overhead)
  }

  test("clearing a view uncounts it and ends it") {
    val hc = new HostObjectCache(10 * Overhead, clock)
    val c0 = hc.forStore(store0)
    val c1 = hc.forStore(store1)
    val (o1, o2, o3, o4) = mkobjs()

    c0.insert(o1)
    c0.insert(o2)
    c0.pin(o3)
    c1.insert(o4)
    hc.sizeInBytes should be(4 * Overhead)

    c0.clear()
    hc.sizeInBytes should be(Overhead)
    c0.get(o1.objectId) shouldBe None
    c0.insert(o1)
    c0.pin(o2)
    c0.get(o1.objectId) shouldBe None
    hc.sizeInBytes should be(Overhead)
    c1.get(o4.objectId) should be(Some(o4))
  }

  test("a new view replacing an uncleared one uncounts the old one") {
    val hc = new HostObjectCache(10 * Overhead, clock)
    val old = hc.forStore(store0)
    val other = hc.forStore(store1)
    val (o1, o2, o3, _) = mkobjs()

    old.insert(o1)
    old.insert(o2)
    other.insert(o3)

    val fresh = hc.forStore(store0)
    hc.sizeInBytes should be(Overhead)
    fresh.get(o1.objectId) shouldBe None
    old.get(o1.objectId) shouldBe None
    old.insert(o1)
    hc.sizeInBytes should be(Overhead)

    // Clearing the replaced view later must not uncount again
    old.clear()
    hc.sizeInBytes should be(Overhead)
    fresh.insert(o1)
    hc.sizeInBytes should be(2 * Overhead)
  }
}
