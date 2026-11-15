package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class UnackedCommitEntrySuite extends AnyFunSuite with Matchers:

  private def pointer(i: Int): DataObjectPointer =
    DataObjectPointer(ObjectId(UUID.randomUUID()), Radicle.poolId, Array[Byte](9, i.toByte))

  test("an entry round-trips its incarnation, and an unknown one decodes as None"):
    List(Some(StoreIncarnation.random()), None).foreach: inc =>
      val e = UnackedCommitEntry(HLCTimestamp(1234L), inc, List(pointer(1), pointer(2)))
      UnackedCommitEntry.decode(e.encode) shouldBe e
      e.encode.length shouldBe e.encodedSize

  test("every chunk of a split entry carries the entry's incarnation and fits the limit"):
    val ptrs = List.tabulate(50)(pointer)
    val e = UnackedCommitEntry(HLCTimestamp(1234L), Some(StoreIncarnation.random()), ptrs)
    val limit = UnackedCommitEntry.maxEncodedSize(4096)
    val chunks = e.chunks(limit)
    chunks.size should be > 1
    chunks.foreach: c =>
      c.incarnation shouldBe e.incarnation
      c.encodedSize should be <= limit
    chunks.flatMap(_.objects) shouldBe ptrs

  test("a store's default incarnation is stable and differs between stores"):
    val pool = PoolId(UUID.randomUUID())
    val a = StoreId(pool, 0)
    val b = StoreId(pool, 1)
    StoreIncarnation.default(a) shouldBe StoreIncarnation.default(StoreId(pool, 0))
    StoreIncarnation.default(a) should not be StoreIncarnation.default(b)
    StoreIncarnation.default(a).uuid should not be new UUID(0L, 0L)
    StoreIncarnation.random() should not be StoreIncarnation.random()
