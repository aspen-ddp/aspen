package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.{ObjectSizeLimitExceeded, Transaction}
import org.aspen_ddp.aspen.client.internal.transaction.UncheckedTransaction
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


/** The pool's backend limit is below the pool's own limit. The stores enforce the smaller one, and so does the
  * client, because the pool's maxObjectSize is derived from that same effective limit.
  */
class MaxSliceSizeStoreIntegrationSuite extends IntegrationTestSuite with MaxSliceSizeHelpers:
  val poolSliceLimit: Int = 32768
  val backendSliceLimit: Int = 16384

  override def testIda: IDA = ReedSolomon(5, 3, 4)
  override def testPoolMaxSliceSize: Option[Int] = Some(poolSliceLimit)
  override def testBackendMaxSliceSize: Option[Int] = Some(backendSliceLimit)

  private def effectiveMaxObjectSize: Int = testIda.maxObjectSize(backendSliceLimit.toLong)

  atest("the client derives maxObjectSize from the backend limit when it is below the pool's"):
    for
      pool <- client.getStoragePool(Radicle.poolId)
      _ = pool.maxSliceSize should be (Some(backendSliceLimit))
      _ = pool.maxObjectSize should be (Some(effectiveMaxObjectSize))
      dp <- allocate(16)
      ok <- overwrite(dp, effectiveMaxObjectSize)
      stored <- client.read(dp)
      tooBig <- overwrite(dp, effectiveMaxObjectSize + 1)
      after <- client.read(dp)
    yield
      ok should be (None)
      stored.data.size should be (effectiveMaxObjectSize)
      tooBig match
        case Some(ObjectSizeLimitExceeded(_, newLength, limit)) =>
          newLength should be (effectiveMaxObjectSize + 1L)
          limit should be (effectiveMaxObjectSize.toLong)
        case other => fail(s"Expected ObjectSizeLimitExceeded, got $other")
      after.revision should be (stored.revision)

  atest("stores vote abort for an oversized slice from a client that skips the size check"):
    for
      dp <- allocate(16)
      initial <- client.read(dp)
      atLimit <- UncheckedTransaction.run(client): bldr =>
        bldr.overwrite(dp, initial.revision, DataBuffer(content(effectiveMaxObjectSize)))
      before <- client.read(dp)
      overLimit <- UncheckedTransaction.run(client): bldr =>
        bldr.overwrite(dp, before.revision, DataBuffer(content(effectiveMaxObjectSize + 1)))
      after <- client.read(dp)
    yield
      atLimit should be (true)
      before.data.size should be (effectiveMaxObjectSize)
      overLimit should be (false)
      after.revision should be (before.revision)
      after.data.size should be (effectiveMaxObjectSize)
