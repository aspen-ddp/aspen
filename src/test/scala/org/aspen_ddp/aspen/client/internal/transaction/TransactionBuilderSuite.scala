package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.{AppendNotSupported, AppendPosition, ConflictingRequirements, MultipleDataUpdatesToObject}
import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.common.ida.{AppendDelta, AppendingReedSolomon, IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class TransactionBuilderSuite extends AnyFunSuite with Matchers {

  private val oid1 = ObjectId(new UUID(0, 1))
  private val tx1 = TransactionId(new UUID(0, 2))
  private val rev1 = ObjectRevision(TransactionId(new UUID(0, 3)))
  private val p1 = DataObjectPointer(oid1, PoolId(new UUID(0, 0)), Array[Byte]())

  private def mkBuilder(): TransactionBuilder =
    new TransactionBuilder(tx1, (_, _) => 0.toByte, ClientId(new UUID(0, 4)))

  private val rs = AppendingReedSolomon(5, 3, 4, 8)
  private val L = 30
  private val pos = AppendPosition(p1, rev1, L)
  private val appended = Array.tabulate[Byte](50)(i => (i * 7 + 1).toByte)

  private def build(bldr: TransactionBuilder, ida: IDA) =
    bldr.buildTranaction(OpportunisticRebuildManager.None, Map(p1.poolId -> ida), Map(p1.poolId -> None))

  test("append adds an Append DataUpdate with the position's revision") {
    val bldr = mkBuilder()
    bldr.append(pos, DataBuffer(appended))
    assert(!bldr.isEmpty)

    val (txd, _, _) = build(bldr, rs)

    txd.requirements should contain (DataUpdate(p1, rev1, DataUpdateOperation.Append))
  }

  test("append sends each store its own encoded delta") {
    val bldr = mkBuilder()
    bldr.append(pos, DataBuffer(appended))

    val (_, updates, _) = build(bldr, rs)

    val expected = rs.appendDeltas(L, appended)
    updates.size should be (rs.width)
    (0 until rs.width).foreach: i =>
      val td = updates(StoreId(p1.poolId, i.toByte))
      td.localUpdates.size should be (1)
      td.localUpdates.head.objectId should be (oid1)
      AppendDelta.decode(td.localUpdates.head.data) should be (expected(i))
  }

  test("append and overwrite of the same object conflict") {
    val bldr = mkBuilder()
    bldr.append(pos, DataBuffer(appended))

    intercept[MultipleDataUpdatesToObject]:
      bldr.overwrite(p1, rev1, DataBuffer(Array[Byte](1)))

    val b2 = mkBuilder()
    b2.overwrite(p1, rev1, DataBuffer(Array[Byte](1)))
    intercept[MultipleDataUpdatesToObject]:
      b2.append(pos, DataBuffer(appended))

    val b3 = mkBuilder()
    b3.lockRevision(p1, rev1)
    intercept[ConflictingRequirements]:
      b3.append(pos, DataBuffer(appended))
  }

  test("building an append for a classic ReedSolomon pool throws AppendNotSupported") {
    val bldr = mkBuilder()
    val classic = ReedSolomon(5, 3, 4)
    bldr.append(pos, DataBuffer(appended))

    val err = intercept[AppendNotSupported]:
      build(bldr, classic)

    err.objectPointer should be (p1)
    err.ida should be (classic)
  }

  test("overwrite records an update") {
    val bldr = mkBuilder()

    assert(bldr.isEmpty)

    bldr.overwrite(p1, rev1, DataBuffer(Array[Byte](1, 2, 3)))

    assert(!bldr.isEmpty)
  }
}
