package org.aspen_ddp.aspen.vdisk

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{DataObjectRangeState, ObjectAllocator}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.objects.ObjectRevision
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.ida.{IDA, RandomAccessReedSolomon}

import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*

class VirtualDiskFailureSuite extends IntegrationTestSuite:

  private type RangeReader = VirtualDisk.RangeReader

  override def testIda: IDA = RandomAccessReedSolomon(5, 3, 4, 64)
  override def testPoolMaxSliceSize: Option[Int] =
    Some(RandomAccessReedSolomon(5, 3, 4, 64).minSliceSizeFor(16384).toInt)

  private val DiskSize = 65536
  private val BlockSize = 16384
  private val Sector = 4096

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

  /** A commit gate that holds each commit until its own promise is completed, and reports each start */
  private final class Gates(n: Int):
    val started: Vector[Promise[Unit]] = Vector.fill(n)(Promise[Unit]())
    val release: Vector[Promise[Unit]] = Vector.fill(n)(Promise[Unit]())
    private var next = 0
    val gate: () => Future[Unit] = () =>
      val i = synchronized:
        val i = next
        next += 1
        i
      if i < n then
        started(i).success(())
        release(i).future
      else
        Future.unit

  /** A gate that holds only its call number `held` (from 0) until `release` completes, reporting when it is reached */
  private final class HoldCall(held: Int):
    val started: Promise[Unit] = Promise[Unit]()
    val release: Promise[Unit] = Promise[Unit]()
    private val calls = new java.util.concurrent.atomic.AtomicInteger(0)
    val gate: () => Future[Unit] = () =>
      if calls.getAndIncrement() == held then
        started.success(())
        release.future
      else
        Future.unit

  // ---- Fencing ----

  atest("a takeover fences the old owner"):
    val x = pattern(Sector, 1)
    val y = pattern(Sector, 2)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, x, fua = false)
      _ <- da.flush()
      b <- acquire(Held)
      _ <- da.write(0, y, fua = false)
      err <- da.flush().failed
      later <- da.write(Sector, y, fua = false).failed
      readErr <- da.read(0, Sector).failed
      r <- b.read(0, Sector)
    yield
      err should be (VDiskError.Fenced)
      da.state should be (DiskState.Fenced)
      later should be (VDiskError.Fenced)
      readErr should be (VDiskError.Fenced)
      da.dirtyBytes should be (0L)
      r should be (x)

  atest("a fenced owner's unflushed writes never appear after takeover"):
    val x = pattern(Sector, 3)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(2 * BlockSize, x, fua = false)
      b <- acquire(Held)
      err <- da.flush().failed
      r <- b.read(2 * BlockSize, BlockSize)
    yield
      err should be (VDiskError.Fenced)
      da.state should be (DiskState.Fenced)
      r should be (new Array[Byte](BlockSize))

  atest("crash-only close loses only unflushed data"):
    val x = pattern(BlockSize, 4)
    val y = pattern(2 * Sector, 5)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, x, fua = false)
      _ <- da.flush()
      _ <- da.write(Sector, y, fua = false)
      _ = da.close()
      b <- acquire()
      r <- b.read(0, BlockSize)
    yield
      da.state should be (DiskState.Fenced)
      r.slice(0, Sector) should be (x.slice(0, Sector))
      (0 until 2).foreach: i =>
        val s = Sector * (1 + i)
        val got = r.slice(s, s + Sector)
        assert(got.sameElements(y.slice(i * Sector, (i + 1) * Sector)) || got.sameElements(x.slice(s, s + Sector)))
      r.slice(3 * Sector, BlockSize) should be (x.slice(3 * Sector, BlockSize))

  // ---- Commit-failure resolution ----

  atest("a data commit that actually committed is recognized"):
    val x = pattern(BlockSize, 6)
    val y = pattern(Sector, 7)
    val model = x.clone()
    applyTo(model, Sector, y)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, x, fua = false)
      _ <- da.flush()
      _ = da.failNextCommitAfterSuccess = true
      _ <- da.write(Sector, y, fua = false)
      _ <- da.flush()
      own <- da.read(0, BlockSize)
      b <- acquire()
      r <- b.read(0, BlockSize)
    yield
      da.failNextCommitAfterSuccess should be (false)
      da.state should be (DiskState.Serving)
      da.stats.recoveredCommits should be (1L)
      own should be (model)
      r should be (model)

  atest("map transaction KeyAlreadyExists adopts the existing pointer"):
    val x = pattern(Sector, 8)
    val y = pattern(Sector, 9)
    val model = new Array[Byte](BlockSize)
    applyTo(model, 0, x)
    applyTo(model, Sector, y)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ = da.failNextCommitAfterSuccess = true
      _ <- da.write(BlockSize, x, fua = false)
      _ <- da.flush()
      adopted = da.stats.adoptedBlocks
      _ <- da.write(BlockSize + Sector, y, fua = false)
      _ <- da.flush()
      own <- da.read(BlockSize, BlockSize)
      b <- acquire()
      r <- b.read(BlockSize, BlockSize)
    yield
      da.failNextCommitAfterSuccess should be (false)
      adopted should be (1L)
      da.state should be (DiskState.Serving)
      own should be (model)
      r should be (model)

  atest("a map KeyAlreadyExists with no attempt sent checks the fence before adopting"):
    val x = pattern(Sector, 80)
    val y = pattern(Sector, 81)
    for
      _ <- createDisk()
      da <- acquire(Held)
      // da learns the block is absent, then another owner takes the disk and creates it
      _ <- da.read(BlockSize, Sector)
      b <- acquire(Held)
      _ <- b.write(BlockSize, x, fua = false)
      _ <- b.flush()
      _ <- da.write(BlockSize, y, fua = false)
      err <- da.flush().failed
      r <- b.read(BlockSize, Sector)
    yield
      err should be (VDiskError.Fenced)
      da.state should be (DiskState.Fenced)
      da.stats.adoptedBlocks should be (0L)
      r should be (x)

  atest("an unmap whose map transaction actually committed is recognized"):
    val x = pattern(BlockSize, 82)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(BlockSize, x, fua = false)
      _ <- da.flush()
      ptr = da.blockPointer(1).get
      _ = da.failNextCommitAfterSuccess = true
      _ <- da.unmap(List((BlockSize.toLong, BlockSize.toLong)))
      _ <- da.flush()
      gone <- client.readMetadata(ptr).failed
      b <- acquire()
      mapped <- b.lookupBlock(1)
      r <- b.read(BlockSize, BlockSize)
    yield
      da.failNextCommitAfterSuccess should be (false)
      da.state should be (DiskState.Serving)
      gone shouldBe an [org.aspen_ddp.aspen.client.InvalidObject]
      mapped should be (None)
      r should be (new Array[Byte](BlockSize))

  atest("a replacing map transaction that actually committed adopts the new object"):
    val x = pattern(BlockSize, 83)
    val y = pattern(Sector, 84)
    val z = pattern(Sector, 85)
    val w = pattern(Sector, 86)
    val model = new Array[Byte](BlockSize)
    applyTo(model, Sector, y)
    applyTo(model, 2 * Sector, z)
    val gates = Gates(2)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(BlockSize, x, fua = false)
      _ <- da.flush()
      old = da.blockPointer(1).get
      // A map transaction creating block 2 is held, so block 1's deletion waits and its next write joins it
      _ <- da.read(2 * BlockSize, Sector)
      _ = da.commitGate = gates.gate
      _ <- da.write(2 * BlockSize, w, fua = false)
      creating = da.flush()
      _ <- gates.started(0).future
      _ <- da.unmap(List((BlockSize.toLong, BlockSize.toLong)))
      _ <- da.write(BlockSize + Sector, y, fua = false)
      replaced = da.flush()
      _ = gates.release(0).success(())
      // The replacing transaction is the next one. It commits and reports failure
      _ <- gates.started(1).future
      _ = da.failNextCommitAfterSuccess = true
      _ = gates.release(1).success(())
      _ <- creating
      _ <- replaced
      adopted = da.stats.adoptedBlocks
      _ <- da.write(BlockSize + 2 * Sector, z, fua = false)
      _ <- da.flush()
      gone <- client.readMetadata(old).failed
      own <- da.read(BlockSize, BlockSize)
      b <- acquire()
      r <- b.read(BlockSize, BlockSize)
    yield
      da.failNextCommitAfterSuccess should be (false)
      da.state should be (DiskState.Serving)
      adopted should be (1L)
      gone shouldBe an [org.aspen_ddp.aspen.client.InvalidObject]
      own should be (model)
      r should be (model)

  // ---- UNMAP during commits ----

  atest("a write after unmapping a block of a running map batch is kept"):
    val x = pattern(BlockSize, 90)
    val first = pattern(Sector, 91)
    val w = pattern(Sector, 92)
    val z = pattern(Sector, 93)
    val model = new Array[Byte](BlockSize)
    applyTo(model, 2 * Sector, w)
    val creating = HoldCall(0)
    val lookup = HoldCall(0)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(BlockSize, x, fua = false)
      _ <- da.flush()
      _ <- da.read(2 * BlockSize, 2 * BlockSize)
      // A map transaction creating block 3 is held, so block 1's deletion and block 2's creation form the next batch
      _ = da.commitGate = creating.gate
      _ <- da.write(3 * BlockSize, z, fua = false)
      created = da.flush()
      _ <- creating.started.future
      _ <- da.unmap(List((BlockSize.toLong, BlockSize.toLong)))
      _ <- da.write(2 * BlockSize, first, fua = false)
      flushed = da.flush()
      // That batch is held while it looks up block 1's object, after its creates have passed the unmap check
      _ = da.deleteLookupGate = lookup.gate
      _ = creating.release.success(())
      _ <- lookup.started.future
      _ <- da.unmap(List((2L * BlockSize, BlockSize.toLong)))
      _ <- da.write(2 * BlockSize + 2 * Sector, w, fua = false)
      _ = lookup.release.success(())
      _ <- created
      _ <- flushed
      _ <- da.flush()
      own <- da.read(2 * BlockSize, BlockSize)
      b <- acquire()
      r <- b.read(2 * BlockSize, BlockSize)
    yield
      da.state should be (DiskState.Serving)
      own should be (model)
      r should be (model)

  atest("an unmap between a map attempt of unknown outcome and its retry deletes what it created"):
    val x = pattern(BlockSize, 94)
    val y = pattern(Sector, 95)
    val model = new Array[Byte](BlockSize)
    applyTo(model, Sector, y)
    // Call 0 is the first attempt, which commits and reports failure; call 1 is its retry
    val retry = HoldCall(1)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.read(BlockSize, Sector)
      _ = da.attemptGate = retry.gate
      _ = da.failNextCommitAfterSuccess = true
      _ <- da.write(BlockSize, x, fua = false)
      flushed = da.flush()
      _ <- retry.started.future
      _ <- da.unmap(List((BlockSize.toLong, BlockSize.toLong)))
      _ <- da.write(BlockSize + Sector, y, fua = false)
      _ = retry.release.success(())
      _ <- flushed
      _ <- da.flush()
      own <- da.read(BlockSize, BlockSize)
      b <- acquire()
      r <- b.read(BlockSize, BlockSize)
    yield
      da.failNextCommitAfterSuccess should be (false)
      da.state should be (DiskState.Serving)
      // The object the first attempt created was replaced, so it is not leaked
      da.stats.deletedObjects should be (1L)
      own should be (model)
      r should be (model)

  atest("an unmap between a data attempt of unknown outcome and its retry ends the commit"):
    val x = pattern(BlockSize, 96)
    val y = pattern(Sector, 97)
    val z = pattern(Sector, 98)
    val model = new Array[Byte](BlockSize)
    applyTo(model, 2 * Sector, z)
    val retry = HoldCall(1)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(BlockSize, x, fua = false)
      _ <- da.flush()
      ptr = da.blockPointer(1).get
      _ = da.attemptGate = retry.gate
      _ = da.failNextCommitAfterSuccess = true
      _ <- da.write(BlockSize + Sector, y, fua = false)
      flushed = da.flush()
      _ <- retry.started.future
      _ <- da.unmap(List((BlockSize.toLong, BlockSize.toLong)))
      _ <- da.write(BlockSize + 2 * Sector, z, fua = false)
      _ = retry.release.success(())
      _ <- flushed
      _ <- da.flush()
      gone <- client.readMetadata(ptr).failed
      own <- da.read(BlockSize, BlockSize)
      b <- acquire()
      r <- b.read(BlockSize, BlockSize)
    yield
      da.failNextCommitAfterSuccess should be (false)
      da.state should be (DiskState.Serving)
      da.stats.recoveredCommits should be (0L)
      gone shouldBe an [org.aspen_ddp.aspen.client.InvalidObject]
      own should be (model)
      r should be (model)

  atest("a foreign write to a block object fails the disk"):
    val x = pattern(BlockSize, 10)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, x, fua = false)
      _ <- da.flush()
      ptr = da.blockPointer(0).get
      md <- client.readMetadata(ptr)
      _ <- client.transact: tx =>
        tx.overwrite(ptr, md.revision, DataBuffer(pattern(BlockSize, 11)))
        Future.unit
      _ <- da.write(Sector, pattern(Sector, 12), fua = false)
      err <- da.flush().failed
      readErr <- da.read(0, Sector).failed
    yield
      err shouldBe a [VDiskError.DiskFailed]
      da.state shouldBe a [DiskState.Failed]
      readErr shouldBe a [VDiskError.DiskFailed]

  atest("a commit failing against a learned revision relearns it and retries"):
    val v1 = pattern(BlockSize, 13)
    val v2 = pattern(Sector, 14)
    val model = v1.clone()
    applyTo(model, Sector, v2)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, v1, fua = false)
      _ <- da.flush()
      // A new owner's first read comes from a lagging store, so it learns a wrong revision and caches wrong bytes
      b <- acquire(Held)
      _ = b.rangeReader = staleOnce(pattern(BlockSize, 96))
      _ <- b.read(Sector, Sector)
      // The cached bytes cover the write, so the commit uses them and the learned revision, and fails
      _ <- b.write(Sector, v2, fua = false)
      _ <- b.flush()
      own <- b.read(0, BlockSize)
      c <- acquire()
      r <- c.read(0, BlockSize)
    yield
      b.state should be (DiskState.Serving)
      b.stats.preImageReads should be (1L)
      own should be (model)
      r should be (model)

  atest("a synchronous throw in a launched commit body fails waiters with DiskFailed"):
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, pattern(Sector, 15), fua = false)
      _ <- da.flush()
      _ = da.commitGate = () => throw new RuntimeException("injected")
      err <- da.write(0, pattern(Sector, 16), fua = true).failed
    yield
      err shouldBe a [VDiskError.DiskFailed]
      da.state shouldBe a [DiskState.Failed]

  // ---- Waiters and close ----

  atest("a FUA write during an in-flight commit on the same block waits for its own commit"):
    val x = pattern(Sector, 17)
    val y = pattern(Sector, 18)
    val gates = Gates(2)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, pattern(BlockSize, 19), fua = false)
      _ <- da.flush()
      _ = da.commitGate = gates.gate
      _ <- da.write(0, x, fua = false)
      first = da.flush()
      _ <- gates.started(0).future
      fua = da.write(Sector, y, fua = true)
      _ = gates.release(0).success(())
      _ <- first
      // The second commit, which covers the FUA write, has started and is held
      _ <- gates.started(1).future
      pending = fua.isCompleted
      _ = gates.release(1).success(())
      _ <- fua
      b <- acquire()
      r <- b.read(0, 2 * Sector)
    yield
      pending should be (false)
      r should be (x ++ y)

  atest("close fails pending FLUSH and FUA with Fenced"):
    val gates = Gates(1)
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, pattern(Sector, 20), fua = false)
      _ <- da.flush()
      _ = da.commitGate = gates.gate
      fua = da.write(0, pattern(Sector, 21), fua = true)
      _ <- gates.started(0).future
      _ <- da.write(Sector, pattern(Sector, 22), fua = false)
      flushed = da.flush()
      _ = da.close()
      fuaErr <- fua.failed
      flushErr <- flushed.failed
      _ = gates.release(0).success(())
    yield
      fuaErr should be (VDiskError.Fenced)
      flushErr should be (VDiskError.Fenced)
      da.state should be (DiskState.Fenced)

  atest("close fails an in-progress read with Fenced"):
    val entered = Promise[Unit]()
    val release = Promise[Unit]()
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, pattern(BlockSize, 23), fua = false)
      _ <- da.flush()
      _ = da.dropClean(0)
      _ = da.rangeReader = (ptr, ranges) =>
        entered.trySuccess(())
        release.future.flatMap(_ => client.readRanges(ptr, ranges))
      read = da.read(0, Sector)
      _ <- entered.future
      _ = da.close()
      err <- read.failed
      _ = release.success(())
    yield
      err should be (VDiskError.Fenced)

  atest("a failed fetch surfaces as a VDiskError"):
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, pattern(BlockSize, 24), fua = false)
      _ <- da.flush()
      _ = da.dropClean(0)
      _ = da.rangeReader = (_, _) => Future.failed(new RuntimeException("injected"))
      err <- da.read(0, Sector).failed
    yield
      err shouldBe a [VDiskError.DiskFailed]

  // ---- Reads racing commits ----

  atest("a data commit completing during a fetch returns correct bytes"):
    val v1 = pattern(BlockSize, 25)
    val v2 = pattern(Sector, 26)
    val entered = Promise[Unit]()
    val release = Promise[Unit]()
    for
      _ <- createDisk()
      da <- acquire(Held)
      _ <- da.write(0, v1, fua = false)
      _ <- da.flush()
      _ = da.dropClean(0)
      _ <- da.write(0, v2, fua = false)
      // The fetch reads the pre-commit revision, then is held while the commit completes
      _ = da.rangeReader = (ptr, ranges) =>
        client.readRanges(ptr, ranges).flatMap: states =>
          entered.trySuccess(())
          release.future.map(_ => states)
      read = da.read(0, 2 * Sector)
      _ <- entered.future
      _ <- da.flush()
      _ = release.success(())
      r <- read
      // The fetched bytes are at the replaced revision, so only the commit's bytes are cached
      cached = da.cleanBytes(0)
      again <- da.read(0, 2 * Sector)
    yield
      r should be (v2 ++ v1.slice(Sector, 2 * Sector))
      cached should be (Sector.toLong)
      again should be (r)

  atest("a map transaction committing during a read returns correct bytes"):
    val x = pattern(Sector, 27)
    val z = pattern(Sector, 28)
    val gates = Gates(1)
    val readHeld = Promise[Unit]()
    val readEntered = Promise[Unit]()
    for
      _ <- createDisk()
      da <- acquire(Eager)
      // The block is looked up and absent, so the write's map transaction starts at once and is held
      _ <- da.read(BlockSize, BlockSize)
      _ = da.commitGate = gates.gate
      written = da.write(BlockSize, x, fua = false).flatMap(_ => da.write(BlockSize + 2 * Sector, z, fua = false))
      _ <- written
      _ <- gates.started(0).future
      // The read's overlay holds x and z. It then waits while the map transaction commits
      _ = da.readGate = () =>
        readEntered.trySuccess(())
        readHeld.future
      read = da.read(BlockSize, 3 * Sector)
      _ <- readEntered.future
      _ = da.readGate = () => Future.unit
      _ = gates.release(0).success(())
      _ <- da.flush()
      _ = readHeld.success(())
      r <- read
      b <- acquire()
      r2 <- b.read(BlockSize, 3 * Sector)
    yield
      r should be (x ++ new Array[Byte](Sector) ++ z)
      r2 should be (r)

  /** A reader whose first call returns `old`'s bytes at an unknown revision, as a lagging store would */
  private def staleOnce(old: Array[Byte]): RangeReader =
    val first = new java.util.concurrent.atomic.AtomicBoolean(true)
    (ptr, ranges) =>
      client.readRanges(ptr, ranges).map: states =>
        if first.getAndSet(false) then
          val oldRevision = ObjectRevision(TransactionId(UUID.randomUUID()))
          states.map: s =>
            new DataObjectRangeState(s.pointer, oldRevision, s.refcount, s.timestamp, s.readTimestamp, s.ida,
              s.offset, DataBuffer(old.slice(s.offset, s.offset + s.data.size)), s.objectLength)
        else
          states
