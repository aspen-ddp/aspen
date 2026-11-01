package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.{DiskGeometry, LockableBlockDevice, VDiskError}
import org.aspen_ddp.aspen.vdisk.tcmu.FakeKernel.read10
import org.aspen_ddp.aspen.vdisk.tcmu.ring.RingLayout.MbVersion
import org.aspen_ddp.aspen.vdisk.tcmu.scsi.ScsiStatus
import org.aspen_ddp.aspen.vdisk.tcmu.sys.{Linux, SysError}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.lang.foreign.ValueLayout.JAVA_SHORT
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.locks.LockSupport
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.Try

class TcmuServiceSuite extends AnyFunSuite with Matchers:

  private given ExecutionContext = ExecutionContext.global

  private val Bs = 4096
  private val Mi = 1024L * 1024

  private final class Fixture(val kernel: FakeTcmuKernel, val service: TcmuService,
                              val disks: TrieMap[String, FakeBlockDevice]):
    def disk(name: String, auto: Boolean = true,
             geo: DiskGeometry = DiskGeometry(8 * Mi, Bs, 16384)): FakeBlockDevice =
      val d = FakeBlockDevice(geo, auto)
      disks(name) = d
      d

  private def withService(config: TcmuServiceConfig = TcmuServiceConfig())(f: Fixture => Unit): Unit =
    val kernel = FakeTcmuKernel()
    val disks = TrieMap[String, FakeBlockDevice]()
    val open: String => Future[LockableBlockDevice] = name =>
      disks.get(name) match
        case Some(d) => Future.successful(d)
        case None => Future.failed(VDiskError.DiskNotFound(name))
    val service = TcmuService(kernel, open, config)
    try f(Fixture(kernel, service, disks))
    finally
      for d <- disks.values do d.auto = true
      for minor <- service.devices.keys do Try(Await.result(service.removed(minor), 5.seconds))
      kernel.cleanup()

  private def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  private def status(f: Future[Unit]): Int =
    Await.result(f.map(_ => 0).recover { case TcmuService.LifecycleError(errno, _) => -errno }, 5.seconds)

  private def eventually(what: String)(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + 5.seconds.toNanos
    while !cond do
      if System.nanoTime() > deadline then fail(s"timed out waiting for $what")
      LockSupport.parkNanos(200_000)

  private def completion(k: FakeKernel): FakeCompletion =
    var got = List.empty[FakeCompletion]
    eventually("a completion"):
      got = got ++ k.completions()
      got.nonEmpty
    got.size should be (1)
    got.head

  private def readCapacity(k: FakeKernel): Long =
    val cdb = new Array[Byte](10)
    cdb(0) = 0x25
    k.submit(cdb, readLen = 8)
    val c = completion(k)
    c.status should be (ScsiStatus.Good)
    val b = java.nio.ByteBuffer.wrap(c.data)
    val lastLba = Integer.toUnsignedLong(b.getInt(0))
    val blockLen = b.getInt(4)
    (lastLba + 1) * blockLen

  /** Captures warn lines, from any thread, while `body` runs. */
  private def warnings(body: => Unit): List[String] =
    val lines = ConcurrentLinkedQueue[String]()
    val handler = scribe.handler.LogHandler(scribe.Level.Warn): r =>
      lines.add(r.logOutput.plainText): Unit
    scribe.Logger.root.withHandler(handler).replace()
    try body
    finally scribe.Logger.root.withoutHandler(handler).replace()
    lines.asScala.toList

  private def readServed(fx: Fixture, minor: Int, dev: FakeBlockDevice): Unit =
    val k = fx.kernel.kernel(minor)
    k.submit(read10(1, 1), readLen = Bs)
    completion(k).status should be (ScsiStatus.Good)
    dev.names should contain ("read")

  test("added opens, acquires and serves"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (0)
      fx.service.devices should be (Map(3 -> "vd1"))
      readServed(fx, 3, dev)
      dev.names.take(2) should be (List("acquire", "read"))
      fx.kernel.events should not contain ("reset_ring 1")

  test("added with reopen resets the ring before serving"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      val k = fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      // A command left on the ring by a previous run: the reset makes the kernel forget it
      k.submit(read10(5, 1), readLen = Bs)
      status(fx.service.added(3, reopen = true)) should be (0)
      fx.kernel.events should be (List("block_dev 1", "reset_ring 1", "block_dev 0", "openRing 3"))
      readServed(fx, 3, dev)
      dev.logged.filter(_._1 == "read") should be (List(("read", List(Bs.toLong, Bs))))

  test("a failing ring reset on reopen is logged and the device still serves"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      os.remove(fx.kernel.core / "user_1" / "dev1" / "action" / "reset_ring")
      val lines = warnings:
        status(fx.service.added(3, reopen = true)) should be (0)
      lines.exists(l => l.contains("Could not reset ring") && l.contains("dev1")) should be (true)
      readServed(fx, 3, dev)

  test("unknown disk is -ENOENT and nothing is left open"):
    withService(): fx =>
      fx.kernel.addDevice(3, "dev1", "aspen/nope", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (-Linux.ENOENT)
      fx.service.devices should be (empty)
      fx.kernel.wasOpened(3) should be (false)

  test("a non-aspen subtype is -ENODEV"):
    withService(): fx =>
      fx.disk("vd1")
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi, subtype = "file")
      status(fx.service.added(3, reopen = false)) should be (-Linux.ENODEV)

  test("kernel size larger than the Aspen size grows the disk before serving"):
    withService(): fx =>
      val dev = fx.disk("vdgrow")
      fx.kernel.addDevice(3, "dev1", "aspen/vdgrow", 16 * Mi)
      val lines = warnings:
        status(fx.service.added(3, reopen = false)) should be (0)
      lines.exists(l => l.contains(s"growing Aspen disk vdgrow from ${8 * Mi} to ${16 * Mi} to match the " +
        "targetcli size")) should be (true)
      readServed(fx, 3, dev)
      dev.logged.take(3) should be (List(("acquire", Nil), ("grow", List(16 * Mi)), ("read", List(Bs.toLong, Bs))))
      readCapacity(fx.kernel.kernel(3)) should be (16 * Mi)

  test("serving starts only after the grow on add commits"):
    withService(): fx =>
      val dev = fx.disk("vd1", auto = false)
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 16 * Mi)
      val f = fx.service.added(3, reopen = false)
      await(dev.call(0)).succeed()
      val grow = await(dev.call(1))
      grow.name should be ("grow")
      fx.kernel.wasOpened(3) should be (false)
      f.isCompleted should be (false)
      grow.succeed()
      status(f) should be (0)
      fx.kernel.wasOpened(3) should be (true)

  test("a failed grow on add is -EIO and the device is closed"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      dev.failures = Map("grow" -> VDiskError.DiskFailed("injected"))
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 16 * Mi)
      status(fx.service.added(3, reopen = false)) should be (-Linux.EIO)
      dev.closes should be (1)
      fx.kernel.wasOpened(3) should be (false)
      fx.service.devices should be (empty)

  test("a grow on add rejected as invalid is -EINVAL"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      dev.failures = Map("grow" -> VDiskError.InvalidGeometry("injected"))
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 16 * Mi)
      status(fx.service.added(3, reopen = false)) should be (-Linux.EINVAL)
      dev.closes should be (1)

  test("kernel size smaller than the Aspen size serves the kernel size"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 4 * Mi)
      status(fx.service.added(3, reopen = false)) should be (0)
      readCapacity(fx.kernel.kernel(3)) should be (4 * Mi)
      dev.names should not contain ("grow")

  test("block size mismatch is -EINVAL"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi, blockSize = 512)
      status(fx.service.added(3, reopen = false)) should be (-Linux.EINVAL)
      dev.closes should be (1)
      dev.names should be (empty)

  test("a block object size that is not a multiple of the block size is -EINVAL"):
    withService(): fx =>
      val dev = fx.disk("vd1", geo = DiskGeometry(8 * Mi, Bs, 6000))
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (-Linux.EINVAL)
      dev.closes should be (1)

  test("a kernel size that is not a multiple of the block size is -EINVAL"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi + 512)
      status(fx.service.added(3, reopen = false)) should be (-Linux.EINVAL)
      dev.closes should be (1)

  test("the old @socket cfgstring is -EINVAL"):
    withService(): fx =>
      fx.disk("vd1")
      fx.kernel.addDevice(3, "dev1", "aspen/vd1@/run/aspen.sock", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (-Linux.EINVAL)
      fx.service.devices should be (empty)

  test("acquire that never completes times out"):
    withService(TcmuServiceConfig(acquireTimeout = 200.millis)): fx =>
      val dev = fx.disk("vd1", auto = false)
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (-Linux.ETIMEDOUT)
      dev.closes should be (1)
      fx.kernel.wasOpened(3) should be (false)
      fx.service.devices should be (empty)

  test("a failed openRing closes the device"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      fx.kernel.failOpenRing = Set(3)
      status(fx.service.added(3, reopen = false)) should be (-Linux.EIO)
      dev.closes should be (1)
      fx.service.devices should be (empty)

  test("an invalid ring is -EINVAL and is unmapped"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      val k = fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      k.segment.set(JAVA_SHORT, MbVersion.toLong, 99.toShort)
      status(fx.service.added(3, reopen = false)) should be (-Linux.EINVAL)
      dev.closes should be (1)
      fx.kernel.wasOpened(3) should be (true)
      fx.kernel.isMapped(3) should be (false)

  test("removed stops serving and releases"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      val k = fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (0)
      readServed(fx, 3, dev)
      status(fx.service.removed(3)) should be (0)
      fx.service.devices should be (empty)
      dev.names.last should be ("release")
      fx.kernel.isMapped(3) should be (false)
      k.segment.scope().isAlive should be (false)

  test("removed replies 0 when release fails"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      dev.failures = Map("release" -> VDiskError.DiskFailed("injected"))
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (0)
      await(fx.service.onEvent(TcmuEvent.Removed("aspen/vd1", 3, 1))) should be (0)
      fx.kernel.isMapped(3) should be (false)

  test("removed during added waits for the add and leaves nothing open"):
    withService(): fx =>
      val dev = fx.disk("vd1", auto = false)
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      val add = fx.service.added(3, reopen = false)
      val acquire = await(dev.call(0))
      acquire.name should be ("acquire")
      val remove = fx.service.removed(3)
      remove.isCompleted should be (false)
      acquire.succeed()
      status(add) should be (0)
      val release = await(dev.call(1))
      release.name should be ("release")
      release.succeed()
      status(remove) should be (0)
      fx.service.devices should be (empty)
      fx.kernel.isMapped(3) should be (false)

  test("removed during an add that fails leaves nothing open"):
    withService(TcmuServiceConfig(acquireTimeout = 200.millis)): fx =>
      val dev = fx.disk("vd1", auto = false)
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      val add = fx.service.added(3, reopen = false)
      await(dev.call(0))
      val remove = fx.service.removed(3)
      status(add) should be (-Linux.ETIMEDOUT)
      status(remove) should be (0)
      dev.closes should be (1)
      fx.kernel.wasOpened(3) should be (false)

  test("reconfig size grows then raises the unit attention"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      val k = fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (0)
      await(fx.service.onEvent(TcmuEvent.Reconfig("aspen/vd1", 3, 1, ReconfigChange.Size(16 * Mi)))) should be (0)
      dev.logged.last should be (("grow", List(16 * Mi)))
      k.submit(new Array[Byte](6))
      val ua = completion(k)
      ua.status should be (ScsiStatus.CheckCondition)
      (ua.sense(2) & 0x0f) should be (6)
      (ua.sense(12) & 0xff, ua.sense(13) & 0xff) should be ((0x2a, 0x09))
      readCapacity(k) should be (16 * Mi)

  test("reconfig size within the Aspen size resizes without growing"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      val k = fx.kernel.addDevice(3, "dev1", "aspen/vd1", 4 * Mi)
      status(fx.service.added(3, reopen = false)) should be (0)
      status(fx.service.reconfig(3, ReconfigChange.Size(8 * Mi))) should be (0)
      dev.names should not contain ("grow")
      k.submit(new Array[Byte](6))
      completion(k).status should be (ScsiStatus.CheckCondition)
      readCapacity(k) should be (8 * Mi)

  test("reconfig shrink, write cache and config are -EINVAL"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (0)
      status(fx.service.reconfig(3, ReconfigChange.Size(4 * Mi))) should be (-Linux.EINVAL)
      status(fx.service.reconfig(3, ReconfigChange.Size(8 * Mi + 512))) should be (-Linux.EINVAL)
      status(fx.service.reconfig(3, ReconfigChange.WriteCache(true))) should be (-Linux.EINVAL)
      status(fx.service.reconfig(3, ReconfigChange.Config("aspen/other"))) should be (-Linux.EINVAL)
      status(fx.service.reconfig(3, ReconfigChange.Unknown)) should be (-Linux.EINVAL)
      dev.names should not contain ("grow")
      fx.service.devices should be (Map(3 -> "vd1"))

  test("a failed reconfig grow is -EIO"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      dev.failures = Map("grow" -> VDiskError.DiskFailed("injected"))
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (0)
      status(fx.service.reconfig(3, ReconfigChange.Size(16 * Mi))) should be (-Linux.EIO)

  test("reconfig for an unknown minor is -ENODEV"):
    withService(): fx =>
      status(fx.service.reconfig(9, ReconfigChange.Size(16 * Mi))) should be (-Linux.ENODEV)

  test("reconfig during added waits for the add"):
    withService(): fx =>
      val dev = fx.disk("vd1", auto = false)
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      val add = fx.service.added(3, reopen = false)
      val acquire = await(dev.call(0))
      val reconfig = fx.service.reconfig(3, ReconfigChange.Size(16 * Mi))
      reconfig.isCompleted should be (false)
      acquire.succeed()
      status(add) should be (0)
      val grow = await(dev.call(1))
      grow.name should be ("grow")
      grow.succeed()
      status(reconfig) should be (0)

  test("a second added for the same uio device replies 0 and changes nothing"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      await(fx.service.onEvent(TcmuEvent.Added("aspen/vd1", 3, 1))) should be (0)
      await(fx.service.onEvent(TcmuEvent.Added("aspen/vd1", 3, 1))) should be (0)
      fx.service.devices should be (Map(3 -> "vd1"))
      fx.kernel.events.count(_ == "openRing 3") should be (1)
      dev.names.count(_ == "acquire") should be (1)
      dev.closes should be (0)
      readServed(fx, 3, dev)

  test("a device created during the startup scan is added once: the scan's add, then its event replies 0"):
    withService(): fx =>
      val dev = fx.disk("vd1")
      // The kernel registers uio3 before multicasting ADDED, so the scan finds it and the event is handled after
      fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      await(fx.service.startupScan())
      await(fx.service.onEvent(TcmuEvent.Added("aspen/vd1", 3, 1))) should be (0)
      fx.service.devices should be (Map(3 -> "vd1"))
      fx.kernel.events.count(_ == "openRing 3") should be (1)
      readServed(fx, 3, dev)

  test("an added for a served minor under a different uio name replaces the stale entry"):
    withService(): fx =>
      val old = fx.disk("vd1")
      val oldRing = fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      status(fx.service.added(3, reopen = false)) should be (0)
      // The kernel unregistered dev1 without a remove (a failed add reply) and reused the minor for dev2
      val dev = fx.disk("vd2")
      fx.kernel.addDevice(3, "dev2", "aspen/vd2", 8 * Mi)
      val lines = warnings:
        await(fx.service.onEvent(TcmuEvent.Added("aspen/vd2", 3, 2))) should be (0)
      lines.exists(l => l.contains("stale") && l.contains("dev1")) should be (true)
      fx.service.devices should be (Map(3 -> "vd2"))
      old.closes should be (1)
      old.names should not contain ("release")
      oldRing.segment.scope().isAlive should be (false)
      readServed(fx, 3, dev)

  test("an open that never completes times out and leaves nothing open"):
    val kernel = FakeTcmuKernel()
    val late = scala.concurrent.Promise[LockableBlockDevice]()
    val service = TcmuService(kernel, _ => late.future, TcmuServiceConfig(acquireTimeout = 200.millis))
    try
      kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi)
      status(service.added(3, reopen = false)) should be (-Linux.ETIMEDOUT)
      service.devices should be (empty)
      kernel.wasOpened(3) should be (false)
      // A disk that opens after the timeout is closed, not leaked
      val dev = FakeBlockDevice()
      late.success(dev)
      eventually("the late disk closed")(dev.closes == 1)
      dev.names should be (empty)
    finally kernel.cleanup()

  test("startup scan adds every aspen device and survives one failure"):
    withService(): fx =>
      val dev = fx.disk("vd0")
      fx.kernel.addDevice(0, "dev0", "aspen/vd0", 8 * Mi)
      fx.kernel.addDevice(1, "dev1", "aspen/missing", 8 * Mi)
      fx.kernel.addDevice(2, "dev2", "file/x", 8 * Mi, subtype = "file")
      await(fx.service.startupScan())
      fx.service.devices should be (Map(0 -> "vd0"))
      fx.kernel.events should contain ("reset_ring 1")
      fx.kernel.wasOpened(1) should be (false)
      fx.kernel.wasOpened(2) should be (false)
      readServed(fx, 0, dev)

  test("the unit serial is the WWN when present"):
    withService(): fx =>
      fx.disk("vd1")
      val k = fx.kernel.addDevice(3, "dev1", "aspen/vd1", 8 * Mi, serial = Some("serial-42"))
      status(fx.service.added(3, reopen = false)) should be (0)
      // INQUIRY, VPD page 0x80 (unit serial number)
      k.submit(Array[Byte](0x12, 1, 0x80.toByte, 0, 0xff.toByte, 0), readLen = 255)
      val c = completion(k)
      c.status should be (ScsiStatus.Good)
      String(c.data, java.nio.charset.StandardCharsets.US_ASCII) should include ("serial-42")

  test("LinuxTcmuKernel maps the uio device shared and the closer unmaps once"):
    val root = os.temp.dir(prefix = "linux-tcmu")
    try
      os.makeDir.all(root / "dev")
      os.write(root / "dev" / "uio7", new Array[Byte](8192))
      val kernel = LinuxTcmuKernel(Configfs(root / "core", root / "uio", root / "dev"))
      val (seg, channel, closer) = kernel.openRing(7, 8192)
      seg.byteSize() should be (8192L)
      seg.set(JAVA_SHORT, 0L, 0x1234.toShort)
      closer()
      closer()
      seg.scope().isAlive should be (false)
      val b = java.nio.ByteBuffer.wrap(os.read.bytes(root / "dev" / "uio7")).order(java.nio.ByteOrder.nativeOrder())
      b.getShort(0) should be (0x1234.toShort)
      channel.kick()
      intercept[InterruptedException](channel.await())
      intercept[SysError](kernel.openRing(8, 8192)).errno should be (Linux.ENOENT)
    finally os.remove.all(root)
