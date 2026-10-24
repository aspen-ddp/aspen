package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.{ObjectSizeLimitExceeded, Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.objects.DataObjectPointer

import scala.concurrent.Future
import scala.language.implicitConversions

/** Shared helpers for the max slice size suites. The bootstrap pool is the only pool the harness can run stores
  * for, so each suite configures the bootstrap pool's maxSliceSize and its stores' effective limits directly.
  */
trait MaxSliceSizeHelpers:
  this: IntegrationTestSuite =>

  def content(n: Int): Array[Byte] = Array.tabulate[Byte](n)(i => (i % 251).toByte)

  def allocate(n: Int): Future[DataObjectPointer] =
    given tx: Transaction = client.newTransaction()
    for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(DataBuffer(content(n)))
      _ <- tx.commit()
    yield dp

  /** Overwrites `dp` with `n` bytes and returns the commit failure, if any */
  def overwrite(dp: DataObjectPointer, n: Int): Future[Option[Throwable]] =
    for
      state <- client.read(dp)
      tx = client.newTransaction()
      _ = tx.overwrite(dp, state.revision, DataBuffer(content(n)))
      r <- tx.commit().map(_ => None).recover { case e => Some(e) }
    yield r


/** Pool and stores share one limit: maxObjectSize bytes commit, and one byte more is rejected by the client. */
class MaxSliceSizePoolIntegrationSuite extends IntegrationTestSuite with MaxSliceSizeHelpers:
  val sliceLimit: Int = 16384

  override def testIda: IDA = ReedSolomon(5, 3, 4)
  override def testPoolMaxSliceSize: Option[Int] = Some(sliceLimit)
  override def testStoreMaxSliceSize: Option[Long] = Some(sliceLimit.toLong)

  atest("an object of exactly maxObjectSize bytes commits and one byte more is rejected by the client"):
    for
      pool <- client.getStoragePool(Radicle.poolId)
      maxObjectSize = pool.maxObjectSize.get
      _ = maxObjectSize should be (testIda.maxObjectSize(sliceLimit.toLong))
      dp <- allocate(16)
      ok <- overwrite(dp, maxObjectSize)
      stored <- client.read(dp)
      tooBig <- overwrite(dp, maxObjectSize + 1)
      after <- client.read(dp)
    yield
      ok should be (None)
      stored.data.size should be (maxObjectSize)
      tooBig match
        case Some(ObjectSizeLimitExceeded(_, newLength, limit)) =>
          newLength should be (maxObjectSize + 1L)
          limit should be (maxObjectSize.toLong)
        case other => fail(s"Expected ObjectSizeLimitExceeded, got $other")
      after.revision should be (stored.revision)


/** The stores' effective limit (min of pool and backend limits) is below the pool's, so the client's check, which
  * only knows the pool limit, lets through updates that the stores must reject by voting to abort.
  */
class MaxSliceSizeStoreIntegrationSuite extends IntegrationTestSuite with MaxSliceSizeHelpers:
  val poolSliceLimit: Int = 32768
  val storeSliceLimit: Long = 16384

  override def testIda: IDA = ReedSolomon(5, 3, 4)
  override def testPoolMaxSliceSize: Option[Int] = Some(poolSliceLimit)
  override def testStoreMaxSliceSize: Option[Long] = Some(storeSliceLimit)

  atest("stores vote abort when a slice would exceed their limit even though the client check passes"):
    val storeMaxObjectSize = testIda.maxObjectSize(storeSliceLimit)
    for
      pool <- client.getStoragePool(Radicle.poolId)
      _ = pool.maxObjectSize.get should be > (storeMaxObjectSize + 1)
      dp <- allocate(16)
      ok <- overwrite(dp, storeMaxObjectSize)
      stored <- client.read(dp)
      tooBig <- overwrite(dp, storeMaxObjectSize + 1)
      after <- client.read(dp)
    yield
      ok should be (None)
      stored.data.size should be (storeMaxObjectSize)
      tooBig match
        case Some(_: TransactionAborted) =>
        case other => fail(s"Expected TransactionAborted, got $other")
      after.revision should be (stored.revision)
      after.data.size should be (storeMaxObjectSize)
