package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.{AppendPosition, DataObjectState, ObjectSizeLimitExceeded, RangedWrite, RangedWriteState}
import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.common.ida.{AppendingReedSolomon, IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, KeyValueObjectPointer, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class TransactionMaxSizeSuite extends AnyFunSuite with Matchers {

  private val pool = PoolId(new UUID(0, 0))
  private val dp = DataObjectPointer(ObjectId(new UUID(0, 1)), pool, Array[Byte]())
  private val kp = KeyValueObjectPointer(ObjectId(new UUID(0, 5)), pool, Array[Byte]())
  private val tx1 = TransactionId(new UUID(0, 2))
  private val rev1 = ObjectRevision(TransactionId(new UUID(0, 3)))

  private val maxSliceSize = 64L
  private val idas: List[IDA] = List(Replication(3, 2), ReedSolomon(5, 3, 4), AppendingReedSolomon(5, 3, 4, 8))
  private val appendable: List[IDA] = List(idas.head, idas.last)

  private def mkBuilder(): TransactionBuilder =
    new TransactionBuilder(tx1, (_, _) => 0.toByte, ClientId(new UUID(0, 4)))

  private def build(bldr: TransactionBuilder, ida: IDA, max: Option[Int]) =
    bldr.buildTranaction(OpportunisticRebuildManager.None, Map(pool -> ida), Map(pool -> max))

  private def state(ida: IDA, length: Int): RangedWriteState =
    RangedWriteState.fromObject(DataObjectState(dp, rev1, ObjectRefcount(0, 1), HLCTimestamp.now, HLCTimestamp.now,
      ida, 0, DataBuffer(new Array[Byte](length))))

  private def expectExceeded(ptr: org.aspen_ddp.aspen.common.objects.ObjectPointer, max: Int, newLength: Long)(f: => Unit): Unit =
    val err = intercept[ObjectSizeLimitExceeded](f)
    err.objectPointer should be (ptr)
    err.newLength should be (newLength)
    err.maxObjectSize should be (max.toLong)

  test("overwrite of exactly maxObjectSize builds; one more byte throws") {
    idas.foreach: ida =>
      val max = ida.maxObjectSize(maxSliceSize)
      val b1 = mkBuilder()
      b1.overwrite(dp, rev1, DataBuffer(new Array[Byte](max)))
      build(b1, ida, Some(max))

      val b2 = mkBuilder()
      b2.overwrite(dp, rev1, DataBuffer(new Array[Byte](max + 1)))
      expectExceeded(dp, max, max + 1L)(build(b2, ida, Some(max)))
  }

  test("append growing to exactly maxObjectSize builds; one more byte throws") {
    appendable.foreach: ida =>
      val max = ida.maxObjectSize(maxSliceSize)
      val b1 = mkBuilder()
      b1.append(AppendPosition(dp, rev1, 10), DataBuffer(new Array[Byte](max - 10)))
      build(b1, ida, Some(max))

      val b2 = mkBuilder()
      b2.append(AppendPosition(dp, rev1, 10), DataBuffer(new Array[Byte](max - 9)))
      expectExceeded(dp, max, max + 1L)(build(b2, ida, Some(max)))
  }

  test("ranged write ending exactly at maxObjectSize builds; one byte over throws") {
    appendable.foreach: ida =>
      val max = ida.maxObjectSize(maxSliceSize)
      val b1 = mkBuilder()
      b1.rangedWrites(state(ida, 10), List(RangedWrite(max - 4, new Array[Byte](4))), true)
      build(b1, ida, Some(max))

      val b2 = mkBuilder()
      b2.rangedWrites(state(ida, 10), List(RangedWrite(max - 3, new Array[Byte](4))), true)
      expectExceeded(dp, max, max + 1L)(build(b2, ida, Some(max)))
  }

  test("data object allocation with initial content is limited") {
    idas.foreach: ida =>
      val max = ida.maxObjectSize(maxSliceSize)
      def allocate(len: Int): TransactionBuilder =
        val b = mkBuilder()
        b.addAllocatingObject(dp)
        b.overwrite(dp, ObjectRevision.Allocating, DataBuffer(new Array[Byte](len)))
        b.setRefcount(dp, ObjectRefcount.Allocating, ObjectRefcount(0, 1))
        b

      build(allocate(max), ida, Some(max))
      expectExceeded(dp, max, max + 1L)(build(allocate(max + 1), ida, Some(max)))
  }

  test("key-value allocation with initial content is limited by key and value bytes") {
    idas.foreach: ida =>
      val max = ida.maxObjectSize(maxSliceSize)
      def allocate(len: Int): TransactionBuilder =
        val b = mkBuilder()
        b.addAllocatingObject(kp)
        b.update(kp, None, None, Nil, List(Insert(Key(new Array[Byte](4)), new Array[Byte](len - 4))))
        b

      build(allocate(max), ida, Some(max))
      expectExceeded(kp, max, max + 1L)(build(allocate(max + 1), ida, Some(max)))
  }

  test("a pool without a limit still enforces the IDA's own bound") {
    val ars = AppendingReedSolomon(5, 3, 4, 8)
    val ceiling = ars.maxObjectSize(Long.MaxValue)
    ceiling should be (Int.MaxValue - ars.rowBytes)

    val b1 = mkBuilder()
    b1.rangedWrites(state(ars, 10), List(RangedWrite(ceiling, new Array[Byte](1))), true)
    expectExceeded(dp, ceiling, ceiling + 1L)(build(b1, ars, None))

    val b2 = mkBuilder()
    b2.overwrite(dp, rev1, DataBuffer(new Array[Byte](100)))
    build(b2, ars, None)
  }
}
