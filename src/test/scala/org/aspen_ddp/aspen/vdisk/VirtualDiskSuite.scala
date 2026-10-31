package org.aspen_ddp.aspen.vdisk

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.ObjectAllocator
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.{IDA, RandomAccessReedSolomon}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.*

class VirtualDiskSuite extends IntegrationTestSuite:

  override def testIda: IDA = RandomAccessReedSolomon(5, 3, 4, 64)
  override def testPoolMaxSliceSize: Option[Int] =
    Some(RandomAccessReedSolomon(5, 3, 4, 64).minSliceSizeFor(16384).toInt)

  private val DiskSize = 65536
  private val BlockSize = 16384

  /** A window long enough that writeback never runs before the test flushes */
  private val Held = VDiskConfig(coalesceWindow = 10.seconds)
  private val Eager = VDiskConfig(coalesceWindow = 0.millis)

  private val opened = ListBuffer[VirtualDisk]()

  override def subFixtureTeardown(): Unit =
    opened.foreach(_.close())
    opened.clear()

  private def allocator(): Future[ObjectAllocator] =
    client.getStoragePool(Radicle.poolId).map(pool => new PoolObjectAllocator(client, pool))

  private def createDisk(name: String = "d"): Future[DiskInfo] =
    allocator().flatMap(alloc => DiskCatalog.create(client, name, alloc, DiskSize))

  private def acquire(config: VDiskConfig = VDiskConfig(), name: String = "d"): Future[VirtualDisk] =
    VirtualDisk.acquire(client, name, config).map: d =>
      opened += d
      d

  /** Non-zero bytes, distinct per seed, so a lost or misplaced write never reads back as zeros */
  private def pattern(len: Int, seed: Int): Array[Byte] = Array.tabulate(len)(i => ((i * 7 + seed * 13) % 251 + 1).toByte)

  private def applyTo(model: Array[Byte], offset: Int, data: Array[Byte]): Unit =
    System.arraycopy(data, 0, model, offset, data.length)

  atest("write then read returns the data before any flush"):
    val data = pattern(8192, 1)
    for
      _ <- createDisk()
      d <- acquire(Held)
      _ <- d.write(4096, data, fua = false)
      r <- d.read(4096, 8192)
      partial <- d.read(8192, 4096)
    yield
      r should be (data)
      partial should be (data.slice(4096, 8192))
      d.dirtyBytes should be (8192L)

  atest("flush makes data visible to a new owner"):
    val data = pattern(8192, 2)
    val model = new Array[Byte](DiskSize)
    applyTo(model, 4096, data)
    for
      _ <- createDisk()
      d1 <- acquire(Held)
      _ <- d1.write(4096, data, fua = false)
      _ <- d1.flush()
      d2 <- acquire()
      r <- d2.read(0, DiskSize)
    yield
      d1.dirtyBytes should be (0L)
      r should be (model)

  atest("fua write is durable without flush"):
    val data = pattern(4096, 3)
    for
      _ <- createDisk()
      d1 <- acquire(Held)
      _ <- d1.write(20480, data, fua = true)
      d2 <- acquire()
      r <- d2.read(20480, 4096)
    yield
      r should be (data)

  atest("unwritten ranges read as zeros"):
    val data = pattern(4096, 4)
    for
      _ <- createDisk()
      d1 <- acquire(Held)
      fresh <- d1.read(0, DiskSize)
      // Block 1's object is allocated holding [0, 8192), so its tail lies beyond the object's length
      _ <- d1.write(BlockSize + 4096, data, fua = false)
      _ <- d1.flush()
      d2 <- acquire()
      block0 <- d2.read(0, BlockSize)
      block1 <- d2.read(BlockSize, BlockSize)
      tail <- d2.read(BlockSize + 8192, 8192)
    yield
      fresh should be (new Array[Byte](DiskSize))
      block0 should be (new Array[Byte](BlockSize))
      block1.slice(0, 4096) should be (new Array[Byte](4096))
      block1.slice(4096, 8192) should be (data)
      block1.slice(8192, BlockSize) should be (new Array[Byte](8192))
      tail should be (new Array[Byte](8192))

  atest("write spanning two block objects round-trips"):
    val data = pattern(8192, 5)
    val model = new Array[Byte](DiskSize)
    applyTo(model, 12288, data)
    for
      _ <- createDisk()
      d1 <- acquire(Held)
      _ <- d1.write(12288, data, fua = false)
      before <- d1.read(12288, 8192)
      _ <- d1.flush()
      after <- d1.read(8192, 16384)
      d2 <- acquire()
      r <- d2.read(0, DiskSize)
    yield
      before should be (data)
      after should be (model.slice(8192, 8192 + 16384))
      r should be (model)

  atest("overlapping writes during an in-flight commit keep the newest bytes"):
    val rnd = new scala.util.Random(42)
    val model = new Array[Byte](DiskSize)
    // Writes are issued together without awaiting, over the first two blocks. With both blocks looked up and no
    // coalescing window, the first write to each starts its commit before returning, so the rest land on a block
    // whose commit is in flight
    def burst(d: VirtualDisk, seedBase: Int): List[Future[Unit]] =
      (1 to 20).toList.map: i =>
        val offset = rnd.nextInt(8) * 4096
        val len = math.min((1 + rnd.nextInt(3)) * 4096, 2 * BlockSize - offset)
        val data = pattern(len, seedBase + i)
        applyTo(model, offset, data)
        d.write(offset, data, fua = false)
    for
      _ <- createDisk()
      d1 <- acquire(Eager)
      _ <- d1.read(0, 2 * BlockSize)
      // First against the map transactions that create the blocks, then against data transactions
      first = burst(d1, 0)
      duringMap = d1.stats.writesDuringCommit
      _ <- d1.flush()
      second = burst(d1, 100)
      _ <- d1.flush()
      _ <- Future.sequence(first ++ second)
      own <- d1.read(0, DiskSize)
      d2 <- acquire()
      r <- d2.read(0, DiskSize)
    yield
      duringMap should be > 0L
      d1.stats.writesDuringCommit should be > duringMap
      own should be (model)
      r should be (model)

  atest("writes to many blocks commit concurrently"):
    val model = new Array[Byte](DiskSize)
    for
      _ <- createDisk()
      d1 <- acquire(Held)
      // Reading first looks every block up, so the flush finds all four ready for one map transaction
      _ <- d1.read(0, DiskSize)
      _ <- Future.sequence((0 until 4).map: b =>
        val data = pattern(8192, b)
        applyTo(model, b * BlockSize, data)
        d1.write(b * BlockSize, data, fua = false))
      _ <- d1.flush()
      // Every block is mapped now, so these are four independent data transactions
      _ <- Future.sequence((0 until 4).map: b =>
        val data = pattern(4096, 10 + b)
        applyTo(model, b * BlockSize + 4096, data)
        d1.write(b * BlockSize + 4096, data, fua = false))
      _ <- d1.flush()
      d2 <- acquire()
      r <- d2.read(0, DiskSize)
    yield
      d1.stats.mapTxCount should be (1L)
      d1.stats.maxConcurrentDataTx should be >= 2
      r should be (model)

  atest("read during first-write allocation returns written bytes"):
    val data = pattern(4096, 6)
    for
      _ <- createDisk()
      d1 <- acquire(Eager)
      // Once the block is known to be absent, the write starts its map transaction before returning
      _ <- d1.read(8192, 4096)
      w = d1.write(8192, data, fua = false)
      inFlight = d1.mapTxInFlight
      r <- d1.read(8192, 4096)
      _ <- w
      _ <- d1.flush()
    yield
      inFlight should be (true)
      r should be (data)

  atest("unaligned and zero-length requests"):
    for
      _ <- createDisk()
      d1 <- acquire(Held)
      w1 <- d1.write(100, new Array[Byte](4096), fua = false).failed
      w2 <- d1.write(0, new Array[Byte](100), fua = false).failed
      r1 <- d1.read(100, 4096).failed
      empty <- d1.read(0, 0)
      _ <- d1.write(0, Array.emptyByteArray, fua = true)
      _ <- d1.flush()
    yield
      w1 shouldBe a [VDiskError.InvalidRequest]
      w2 shouldBe a [VDiskError.InvalidRequest]
      r1 shouldBe a [VDiskError.InvalidRequest]
      empty.length should be (0)
      d1.dirtyBytes should be (0L)

  atest("out-of-range requests fail without staging"):
    for
      _ <- createDisk()
      d1 <- acquire(Held)
      w <- d1.write(61440, new Array[Byte](8192), fua = false).failed
      r <- d1.read(61440, 8192).failed
      dirty = d1.dirtyBytes
      _ <- d1.flush()
    yield
      w shouldBe a [VDiskError.OutOfRange]
      r shouldBe a [VDiskError.OutOfRange]
      dirty should be (0L)
      d1.stats.mapTxCount should be (0L)

  atest("second write to a mapped block uses ranged writes"):
    val first = pattern(8192, 7)
    val second = pattern(4096, 8)
    val third = pattern(4096, 9)
    val model = new Array[Byte](DiskSize)
    applyTo(model, 0, first)
    applyTo(model, 4096, second)
    applyTo(model, 12288, third)
    for
      _ <- createDisk()
      d1 <- acquire(Held)
      _ <- d1.write(0, first, fua = false)
      _ <- d1.flush()
      _ <- d1.write(4096, second, fua = false)
      _ <- d1.flush()
      // A new owner knows nothing of the block's revision or length, so it reads the pre-image first. The write
      // also extends the object past its current 8192 bytes
      d2 <- acquire(Held)
      _ <- d2.write(12288, third, fua = false)
      _ <- d2.flush()
      d3 <- acquire()
      r <- d3.read(0, DiskSize)
    yield
      d1.stats.mapTxCount should be (1L)
      d1.stats.rangedTxCount should be (1L)
      d1.stats.overwriteTxCount should be (0L)
      // The first owner's clean cache holds the pre-image, the new owner has to read it
      d1.stats.preImageReads should be (0L)
      d2.stats.rangedTxCount should be (1L)
      d2.stats.preImageReads should be (1L)
      r should be (model)

  atest("a write covering a whole mapped block overwrites it"):
    val first = pattern(4096, 11)
    val whole = pattern(BlockSize, 12)
    val again = pattern(BlockSize, 13)
    for
      _ <- createDisk()
      d1 <- acquire(Held)
      _ <- d1.write(BlockSize, first, fua = false)
      _ <- d1.flush()
      _ <- d1.write(BlockSize, whole, fua = false)
      _ <- d1.flush()
      // A new owner learns the revision before overwriting
      d2 <- acquire(Held)
      _ <- d2.write(BlockSize, again, fua = true)
      d3 <- acquire()
      r <- d3.read(BlockSize, BlockSize)
    yield
      d1.stats.overwriteTxCount should be (1L)
      d1.stats.rangedTxCount should be (0L)
      d2.stats.overwriteTxCount should be (1L)
      r should be (again)
