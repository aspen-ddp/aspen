package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.{GroupObjectAllocatorId, ObjectAllocatorId, PoolObjectAllocatorId}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.eventlog.EventLogState.*
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

class EventLogStateSuite extends AnyFunSuite:

  test("keys are distinct single bytes") {
    val keys = Seq(IncomingKey, LoggedKey, TypeIdsKey, ObserversKey, AllocatorKey, TailsKey, TailFloorKey)
    assert(keys.map(_.bytes.toSeq).distinct.size == keys.size)
    keys.foreach(k => assert(k.bytes.length == 1))
  }

  test("TypeIds round trip, empty and 300 entries, unsigned ids") {
    assert(decodeTypeIds(encodeTypeIds(Map.empty)) == Map.empty)
    val ids: TypeIds = (0 until 300).map(i => UUID.randomUUID() -> (i * 200).toShort).toMap
    assert(ids.values.exists(_ < 0))
    assert(decodeTypeIds(encodeTypeIds(ids)) == ids)
  }

  test("Observers round trip, empty and non-empty config") {
    assert(decodeObservers(encodeObservers(Nil)) == Nil)
    val obs = List(ObserverConfig(UUID.randomUUID(), Array.emptyByteArray),
      ObserverConfig(UUID.randomUUID(), Array[Byte](1, 2, 3)))
    assert(decodeObservers(encodeObservers(obs)) == obs)
    assert(ObserverConfig(obs.head.factoryType, Array[Byte](1)) != ObserverConfig(obs.head.factoryType, Array[Byte](2)))
  }

  test("Allocator round trip for pool and group") {
    val a: ObjectAllocatorId = PoolObjectAllocatorId(PoolId(UUID.randomUUID()))
    val g: ObjectAllocatorId = GroupObjectAllocatorId(AllocationGroupId(UUID.randomUUID()))
    assert(encodeAllocator(a).length == 17)
    assert(decodeAllocator(encodeAllocator(a)) == a)
    assert(decodeAllocator(encodeAllocator(g)) == g)
  }

  test("Tails round trip") {
    val pool = PoolId(UUID.randomUUID())
    val ptrs = Vector.tabulate(TailCount)(i =>
      DataObjectPointer(ObjectId(UUID.randomUUID()), pool, Array.fill[Byte](i)(i.toByte)))
    val t = Tails(ptrs, 7, GroupObjectAllocatorId(AllocationGroupId(UUID.randomUUID())))
    assert(decodeTails(encodeTails(t)) == t)
  }

  test("Tails whose current index names no tail fail to decode") {
    val pool = PoolId(UUID.randomUUID())
    val ptrs = Vector.tabulate(TailCount)(_ => DataObjectPointer(ObjectId(UUID.randomUUID()), pool, Array.emptyByteArray))
    val alloc = PoolObjectAllocatorId(pool)
    List(-1, TailCount).foreach { c =>
      intercept[IllegalArgumentException](decodeTails(encodeTails(Tails(ptrs, c, alloc))))
    }
  }

  test("TailFloor round trip") {
    List(Long.MinValue, -1L, 0L, Long.MaxValue).foreach { l =>
      assert(decodeTailFloor(encodeTailFloor(HLCTimestamp(l))) == HLCTimestamp(l))
    }
  }

  test("messages round trip") {
    assert(EventLogMessage.decode(EventLogMessage.encode(Nudge)) == Nudge)
    assert(EventLogMessage.decode(EventLogMessage.encode(ReloadObservers)) == ReloadObservers)
    intercept[IllegalArgumentException](EventLogMessage.decode(Array.emptyByteArray))
  }
