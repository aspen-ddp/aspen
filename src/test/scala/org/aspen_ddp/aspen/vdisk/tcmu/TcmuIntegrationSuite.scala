package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.{IDA, RandomAccessReedSolomon}
import org.aspen_ddp.aspen.vdisk.*
import org.aspen_ddp.aspen.vdisk.tcmu.FakeKernel.{read10, write10}
import org.aspen_ddp.aspen.vdisk.tcmu.scsi.ScsiStatus
import org.scalatest.Assertion

import java.nio.ByteBuffer
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal

/** The whole native TCMU stack: a fake kernel ring, TcmuService, TcmuDevice and ScsiCommands, over a real
  * DiskHandle on the in-process Aspen test system.
  *
  * Everything Aspen does runs on the suite's serial execution context, so nothing here blocks on it: completions
  * are polled for on a separate thread and delivered through a Future.
  */
class TcmuIntegrationSuite extends IntegrationTestSuite:

  import TcmuIntegrationSuite.*

  override def testIda: IDA = RandomAccessReedSolomon(5, 3, 4, 64)
  override def testPoolMaxSliceSize: Option[Int] =
    Some(RandomAccessReedSolomon(5, 3, 4, 64).minSliceSizeFor(16384).toInt)

  private val Mi = 1024L * 1024
  private val DiskSize = 8 * Mi
  private val Bs = 4096
  private val Minor = 3

  /** A window long enough that writeback never runs before the test flushes */
  private val Held = VDiskConfig(coalesceWindow = 10.seconds)

  private val handles = ListBuffer[DiskHandle]()
  private var kernel: Option[FakeTcmuKernel] = None

  override def subFixtureTeardown(): Unit =
    // Every device was removed (stopping its ring thread) at the end of its test's Future chain
    kernel.foreach(_.cleanup())
    kernel = None
    synchronized(handles.toList).foreach(h => Try(h.close()))
    synchronized(handles.clear())

  private def createDisk(): Future[DiskInfo] =
    client.getStoragePool(Radicle.poolId).flatMap: pool =>
      DiskCatalog.create(client, "d", new PoolObjectAllocator(client, pool), DiskSize)

  private def openHandle(name: String): Future[DiskHandle] =
    DiskHandle.open(client, name, Held).map: h =>
      synchronized(handles += h)
      h

  private final class Fx(val kernel: FakeTcmuKernel, val service: TcmuService):
    def ring: FakeKernel = kernel.kernel(Minor)
    /** The handle TcmuService opened for the device */
    def served: DiskHandle = synchronized(handles.head)

  /** Creates disk `d`, a fake kernel device of `kernelSize` for it and the service; adds the device, runs `body`,
    * then removes every device, whatever `body`'s outcome.
    */
  private def withDevice(kernelSize: Long = DiskSize, beforeAdd: => Future[Unit] = Future.unit)(
      body: Fx => Future[Assertion]): Future[Assertion] =
    val k = FakeTcmuKernel()
    kernel = Some(k)
    val service = TcmuService(k, name => openHandle(name))
    val fx = Fx(k, service)
    val result =
      for
        _ <- createDisk()
        _ <- beforeAdd
        _ = k.addDevice(Minor, "dev1", "aspen/d", kernelSize)
        _ <- service.added(Minor, reopen = false)
        r <- body(fx)
      yield r
    result.transformWith: r =>
      Future.sequence(service.devices.keys.map(service.removed)).transform(_ => r)

  private def pattern(len: Int, seed: Int): Array[Byte] =
    Array.tabulate(len)(i => ((i * 7 + seed * 13) % 251 + 1).toByte)

  /** Submits one command and completes with its completion, failing after a deadline. */
  private def run(k: FakeKernel, cdb: Array[Byte], data: Array[Byte] = Array.empty,
                  readLen: Int = 0): Future[FakeCompletion] =
    val id = k.submit(cdb, data, readLen)
    val p = Promise[FakeCompletion]()
    val deadline = System.nanoTime() + CompletionTimeout.toNanos
    // One-shot polls, each scheduling the next only while unanswered: once p is completed nothing reads this
    // ring's completions on its behalf, so a later command's completion cannot be consumed here
    def poll(): Unit =
      try
        k.completions() match
          case Nil =>
            if System.nanoTime() > deadline then
              p.failure(AssertionError(s"no completion for command $id within $CompletionTimeout"))
            else
              poller.schedule((() => poll()): Runnable, 200, TimeUnit.MICROSECONDS): Unit
          case List(c) if c.cmdId == id => p.success(c)
          case other => p.failure(AssertionError(s"unexpected completions for command $id: $other"))
      catch case NonFatal(e) => p.tryFailure(e): Unit
    poller.execute(() => poll())
    p.future

  private def good(c: FakeCompletion): Assertion =
    withClue(f"sense key ${c.sense(2) & 0x0f}%x ASC/ASCQ ${c.sense(12) & 0xff}%02x/${c.sense(13) & 0xff}%02x: "):
      c.status should be (ScsiStatus.Good)

  private def checkCondition(c: FakeCompletion, key: Int, asc: Int, ascq: Int): Assertion =
    c.status should be (ScsiStatus.CheckCondition)
    (c.sense(2) & 0x0f, c.sense(12) & 0xff, c.sense(13) & 0xff) should be ((key, asc, ascq))

  private def readCapacity(k: FakeKernel): Future[Long] =
    val cdb = new Array[Byte](10)
    cdb(0) = 0x25
    run(k, cdb, readLen = 8).map: c =>
      good(c)
      val b = ByteBuffer.wrap(c.data)
      (Integer.toUnsignedLong(b.getInt(0)) + 1) * b.getInt(4)

  private def write16(lba: Long, blocks: Int, fua: Boolean): Array[Byte] =
    val b = ByteBuffer.allocate(16)
    b.put(0, 0x8a.toByte)
    b.put(1, (if fua then 0x08 else 0).toByte)
    b.putLong(2, lba)
    b.putInt(10, blocks)
    b.array()

  private val SyncCache10: Array[Byte] =
    val c = new Array[Byte](10)
    c(0) = 0x35
    c

  private val TestUnitReady: Array[Byte] = new Array[Byte](6)

  /** An UNMAP CDB and its parameter list with one block descriptor */
  private def unmap(lba: Long, blocks: Int): (Array[Byte], Array[Byte]) =
    val cdb = new Array[Byte](10)
    cdb(0) = 0x42
    cdb(8) = 24
    val par = ByteBuffer.allocate(24)
    par.putShort(0, 22)
    par.putShort(2, 16)
    par.putLong(8, lba)
    par.putInt(16, blocks)
    (cdb, par.array())

  /** Opens a new handle on `d`, which fences every earlier owner */
  private def takeOver(): Future[DiskHandle] =
    for
      h <- openHandle("d")
      _ <- h.acquire()
    yield h

  atest("write then read through the ring"):
    val data = pattern(64 * 1024, 1)
    val blocks = data.length / Bs
    withDevice(): fx =>
      for
        w <- run(fx.ring, write10(16, blocks), data)
        r <- run(fx.ring, read10(16, blocks), readLen = data.length)
      yield
        good(w)
        good(r)
        r.readLen should be (Some(data.length))
        java.util.Arrays.equals(r.data, data) should be (true)

  atest("FUA write is durable after takeover"):
    val data = pattern(2 * Bs, 2)
    withDevice(): fx =>
      for
        w <- run(fx.ring, write16(3, 2, fua = true), data)
        _ = fx.served.close()
        h <- takeOver()
        r <- h.read(3L * Bs, data.length)
      yield
        good(w)
        r.toList should be (data.toList)

  atest("SYNCHRONIZE CACHE makes earlier writes durable"):
    val a = pattern(Bs, 3)
    val b = pattern(2 * Bs, 4)
    val bLba = (Mi / Bs).toInt
    withDevice(): fx =>
      for
        w1 <- run(fx.ring, write10(0, 1), a)
        w2 <- run(fx.ring, write10(bLba, 2), b)
        f <- run(fx.ring, SyncCache10)
        _ = fx.served.close()
        h <- takeOver()
        r1 <- h.read(0, a.length)
        r2 <- h.read(bLba.toLong * Bs, b.length)
      yield
        List(w1, w2, f).foreach(good)
        r1.toList should be (a.toList)
        r2.toList should be (b.toList)

  atest("UNMAP reads back zeros"):
    withDevice(): fx =>
      val objectBlocks = fx.served.geometry.blockObjectSize / Bs
      // Two block objects, the first of which is then unmapped whole
      val data = pattern(2 * objectBlocks * Bs, 5)
      val (cdb, par) = unmap(0, objectBlocks)
      for
        w <- run(fx.ring, write10(0, 2 * objectBlocks), data)
        u <- run(fx.ring, cdb, par)
        r <- run(fx.ring, read10(0, 2 * objectBlocks), readLen = data.length)
      yield
        good(w)
        good(u)
        good(r)
        val half = objectBlocks * Bs
        r.data.take(half).forall(_ == 0) should be (true)
        r.data.drop(half).toList should be (data.drop(half).toList)

  atest("grow before targetcli resize still opens"):
    withDevice(beforeAdd = DiskCatalog.grow(client, "d", 2 * DiskSize).map(_ => ())): fx =>
      for
        before <- readCapacity(fx.ring)
        status <- fx.service.onEvent(TcmuEvent.Reconfig("aspen/d", Minor, 1, ReconfigChange.Size(2 * DiskSize)))
        ua <- run(fx.ring, TestUnitReady)
        after <- readCapacity(fx.ring)
      yield
        before should be (DiskSize)
        status should be (0)
        checkCondition(ua, 6, 0x2a, 0x09)
        after should be (2 * DiskSize)

  atest("add with a larger targetcli size grows the Aspen disk"):
    val data = pattern(Bs, 6)
    val lba = (12 * Mi / Bs).toInt
    withDevice(kernelSize = 2 * DiskSize): fx =>
      for
        info <- DiskCatalog.load(client, "d")
        capacity <- readCapacity(fx.ring)
        w <- run(fx.ring, write10(lba, 1), data)
        r <- run(fx.ring, read10(lba, 1), readLen = Bs)
      yield
        info.geometry.size should be (2 * DiskSize)
        capacity should be (16 * Mi)
        good(w)
        good(r)
        r.data.toList should be (data.toList)

  atest("a fenced owner returns NOT READY 0x040B"):
    val data = pattern(Bs, 7)
    withDevice(): fx =>
      for
        _ <- takeOver()
        // Write-back: the write completes from memory, and the fence is found when it is committed
        w <- run(fx.ring, write10(0, 1), data)
        f <- run(fx.ring, SyncCache10)
      yield
        good(w)
        checkCondition(f, 2, 0x04, 0x0b)

object TcmuIntegrationSuite:
  private val CompletionTimeout = 10.seconds

  /** Polls fake rings for completions, off the suite's serial execution context */
  private lazy val poller: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(r => Thread.ofPlatform().name("tcmu-it-poll").daemon(true).unstarted(r))
