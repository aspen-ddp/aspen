package org.aspen_ddp.aspen.vdisk

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.ObjectAllocator
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.{IDA, RandomAccessReedSolomon}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.*

class DiskHandleSuite extends IntegrationTestSuite:

  override def testIda: IDA = RandomAccessReedSolomon(5, 3, 4, 64)
  override def testPoolMaxSliceSize: Option[Int] =
    Some(RandomAccessReedSolomon(5, 3, 4, 64).minSliceSizeFor(16384).toInt)

  private val DiskSize = 65536

  /** A window long enough that writeback never runs before the test flushes */
  private val Held = VDiskConfig(coalesceWindow = 10.seconds)

  private val handles = ListBuffer[DiskHandle]()
  private val disks = ListBuffer[VirtualDisk]()

  override def subFixtureTeardown(): Unit =
    handles.foreach(_.close())
    handles.clear()
    disks.foreach(_.close())
    disks.clear()

  private def createDisk(name: String = "d"): Future[DiskInfo] =
    client.getStoragePool(Radicle.poolId).flatMap: pool =>
      DiskCatalog.create(client, name, new PoolObjectAllocator(client, pool), DiskSize)

  private def open(config: VDiskConfig = VDiskConfig()): Future[DiskHandle] =
    DiskHandle.open(client, "d", config).map: h =>
      handles += h
      h

  private def epoch(info: DiskInfo): Future[Long] =
    client.read(info.fencePointer).map(s => FenceState.fromArray(s.data.getByteArray).epoch)

  private def pattern(len: Int, seed: Int): Array[Byte] =
    Array.tabulate(len)(i => ((i * 7 + seed * 13) % 251 + 1).toByte)

  atest("open does not take ownership"):
    for
      _ <- createDisk()
      d <- VirtualDisk.acquire(client, "d")
      _ = disks += d
      h <- open()
      _ <- d.write(0, pattern(4096, 1), fua = false)
      _ <- d.flush()
      r <- h.read(0, 4096).failed
    yield
      h.lockState should be (LockState.Unlocked)
      r should be (VDiskError.NotOwner)

  atest("acquire enables I/O"):
    val data = pattern(4096, 2)
    for
      _ <- createDisk()
      h <- open()
      _ <- h.acquire()
      _ <- h.write(4096, data, fua = false)
      r <- h.read(4096, 4096)
    yield
      h.lockState should be (LockState.Locked)
      r should be (data)

  atest("acquire while locked keeps the same owner"):
    val data = pattern(4096, 3)
    for
      info <- createDisk()
      h <- open(Held)
      _ <- h.acquire()
      _ <- h.write(0, data, fua = false)
      _ <- h.acquire()
      r <- h.read(0, 4096)
      e <- epoch(info)
    yield
      r should be (data)
      e should be (1L)

  atest("concurrent acquires share one takeover"):
    for
      info <- createDisk()
      h <- open()
      _ <- Future.sequence(List(h.acquire(), h.acquire()))
      e <- epoch(info)
    yield
      h.lockState should be (LockState.Locked)
      e should be (1L)

  atest("a second handle fences the first"):
    for
      _ <- createDisk()
      h1 <- open()
      h2 <- open()
      _ <- h1.acquire()
      _ <- h2.acquire()
      _ <- h1.write(0, pattern(4096, 4), fua = false)
      r <- h1.flush().failed
      fencedState = h1.lockState
      _ <- h1.acquire()
      // A fence is only discovered by a commit, so h2 learns of h1's takeover when it flushes
      _ <- h2.write(4096, pattern(4096, 8), fua = false)
      r2 <- h2.flush().failed
    yield
      r should be (VDiskError.Fenced)
      r2 should be (VDiskError.Fenced)
      fencedState should be (LockState.Fenced)
      h1.lockState should be (LockState.Locked)
      h2.lockState should be (LockState.Fenced)

  atest("release flushes"):
    val data = pattern(4096, 5)
    for
      _ <- createDisk()
      h1 <- open(Held)
      h2 <- open()
      _ <- h1.acquire()
      _ <- h1.write(0, data, fua = false)
      _ <- h1.release()
      _ <- h2.acquire()
      r <- h2.read(0, 4096)
    yield
      h1.lockState should be (LockState.Unlocked)
      r should be (data)

  atest("close does not flush"):
    val a = pattern(4096, 6)
    val b = pattern(4096, 7)
    for
      _ <- createDisk()
      h1 <- open(Held)
      h2 <- open()
      _ <- h1.acquire()
      _ <- h1.write(0, a, fua = false)
      _ <- h1.flush()
      _ <- h1.write(0, b, fua = false)
      _ = h1.close()
      _ <- h2.acquire()
      r <- h2.read(0, 4096)
    yield
      h1.lockState should be (LockState.Unlocked)
      r should be (a)

  atest("grow to the current size is a no-op; larger grows"):
    for
      _ <- createDisk()
      h <- open()
      _ <- h.acquire()
      _ <- h.grow(DiskSize)
      same = h.geometry.size
      _ <- h.grow(131072)
      bad <- h.grow(32768).failed
    yield
      same should be (DiskSize.toLong)
      h.geometry.size should be (131072L)
      bad shouldBe a [VDiskError.InvalidGeometry]

  atest("open of a missing disk fails DiskNotFound"):
    for
      r <- DiskHandle.open(client, "nope").failed
    yield
      r should be (VDiskError.DiskNotFound("nope"))
