package org.aspen_ddp.aspen.server.store.backend

import java.util.UUID

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.{AppendDelta, XorRegion}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, KeyValueUpdate, RefcountUpdate,
  TransactionId, VersionBump}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import UpdateForm.*

class UpdateFormSuite extends AnyFunSuite with Matchers:
  private val oid = ObjectId(new UUID(0, 1))
  private val ptr = DataObjectPointer(oid, PoolId(new UUID(0, 0)), Array[Byte]())
  private val kvPtr = KeyValueObjectPointer(oid, PoolId(new UUID(0, 0)), Array[Byte]())
  private val rev = ObjectRevision(TransactionId(new UUID(0, 3)))

  private def bytes(n: Int) = new Array[Byte](n)
  private def updates(d: AppendDelta) = Map(oid -> d.encode)

  test("allocation is Full"):
    UpdateForm.of(oid, List(DataUpdate(ptr, rev, DataUpdateOperation.Append)), Map(oid -> DataBuffer(bytes(4))),
      allocating = true) shouldBe Full

  test("overwrite is Full"):
    UpdateForm.of(oid, List(DataUpdate(ptr, rev, DataUpdateOperation.Overwrite)), Map(oid -> DataBuffer(bytes(4))),
      allocating = false) shouldBe Full

  test("append spans include the zero-extended area"):
    val d = AppendDelta(10, 30, List(XorRegion(20, bytes(5))), None)
    UpdateForm.of(oid, List(DataUpdate(ptr, rev, DataUpdateOperation.Append)), updates(d), allocating = false) shouldBe
      Ranges(10, List((10, 20)))

  test("write-range with no growth keeps region spans"):
    val d = AppendDelta(100, 100, List(XorRegion(4, bytes(2)), XorRegion(50, bytes(8))), None)
    UpdateForm.of(oid, List(DataUpdate(ptr, rev, DataUpdateOperation.WriteRange)), updates(d), allocating = false) shouldBe
      Ranges(100, List((4, 2), (50, 8)))

  test("adjacent and overlapping spans merge"):
    val d = AppendDelta(20, 40, List(XorRegion(2, bytes(3)), XorRegion(5, bytes(3)), XorRegion(30, bytes(2))), None)
    UpdateForm.of(oid, List(DataUpdate(ptr, rev, DataUpdateOperation.WriteRange)), updates(d), allocating = false) shouldBe
      Ranges(20, List((2, 6), (20, 20)))

  test("key-value update is Full"):
    UpdateForm.of(oid, List(KeyValueUpdate(kvPtr, Some(rev), None, Nil)), Map(), allocating = false) shouldBe Full

  test("refcount-only is MetadataOnly"):
    UpdateForm.of(oid, List(RefcountUpdate(ptr, ObjectRefcount(1, 1), ObjectRefcount(1, 2))), Map(),
      allocating = false) shouldBe MetadataOnly

  test("version bump is MetadataOnly"):
    UpdateForm.of(oid, List(VersionBump(ptr, rev)), Map(), allocating = false) shouldBe MetadataOnly
