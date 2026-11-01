package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.tcmu.FakeKernel.read10
import org.aspen_ddp.aspen.vdisk.tcmu.ring.Ring
import org.aspen_ddp.aspen.vdisk.tcmu.ring.RingLayout.*
import org.aspen_ddp.aspen.vdisk.tcmu.scsi.{ScsiCommands, ScsiHandler, ScsiResult, ScsiStatus}
import org.aspen_ddp.aspen.vdisk.tcmu.sys.Libc
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scribe.Level

import java.lang.foreign.Arena
import java.lang.foreign.ValueLayout.{JAVA_BYTE, JAVA_SHORT}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.LockSupport
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.Try

class TcmuDeviceSuite extends AnyFunSuite with Matchers:

  private val Bs = 4096
  private val settings = DeviceSettings(8L * 1024 * 1024, Bs, 1024, 16384, "vd1", "wwn")

  /** A log sink that records every line. */
  private final class Sink extends ((Level, String) => Unit):
    private val lines = ConcurrentLinkedQueue[(Level, String)]()
    def apply(level: Level, message: String): Unit = lines.add((level, message)): Unit
    def at(level: Level): List[String] = lines.asScala.toList.filter(_._1 == level).map(_._2)

  /** Runs callbacks inline and records anything that escapes them. */
  private final class RecordingContext extends ExecutionContext:
    val escaped = ConcurrentLinkedQueue[Throwable]()
    def execute(r: Runnable): Unit =
      try r.run()
      catch case e: Throwable => escaped.add(e): Unit
    def reportFailure(t: Throwable): Unit = escaped.add(t): Unit

  private final class Control(kernel: FakeKernel) extends RingControl:
    val resets = AtomicInteger()
    @volatile var onReset: () => Unit = () => kernel.resetRing()
    def resetRing(): Unit =
      resets.incrementAndGet()
      onReset()

  private final class Fixture(
      val kernel: FakeKernel,
      val dev: FakeBlockDevice,
      val control: Control,
      val sink: Sink,
      val clock: AtomicLong,
      val failures: FailureLog,
      val context: RecordingContext,
      val device: TcmuDevice)

  private def withDevice(auto: Boolean = true, handler: Option[ScsiHandler] = None)(f: Fixture => Unit): Unit =
    val kernel = FakeKernel()
    val dev = FakeBlockDevice(auto = auto)
    val control = Control(kernel)
    val sink = Sink()
    val clock = AtomicLong(0L)
    val failures = FailureLog("vd1", clock = () => clock.get, log = sink)
    val context = RecordingContext()
    val ring = Ring.open(kernel.segment).fold(e => fail(e), identity)
    val commands = handler.getOrElse(ScsiCommands(dev, settings)(using ExecutionContext.global))
    val device = TcmuDevice("vd1", ring, kernel.channel, control, commands, failures, context)
    device.start()
    try
      f(Fixture(kernel, dev, control, sink, clock, failures, context, device))
      context.escaped.asScala.toList should be (empty)
    finally
      device.stop()
      Try(kernel.close())

  private def eventually(what: String)(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + 5.seconds.toNanos
    while !cond do
      if System.nanoTime() > deadline then fail(s"timed out waiting for $what")
      LockSupport.parkNanos(200_000)

  private def completions(k: FakeKernel, n: Int): List[FakeCompletion] =
    var got = List.empty[FakeCompletion]
    eventually(s"$n completions"):
      got = got ++ k.completions()
      got.size >= n
    got.size should be (n)
    got

  private def senseKey(c: FakeCompletion): Int = c.sense(2) & 0x0f

  private def expected(offset: Long, length: Int): List[Byte] =
    List.tabulate(length)(i => ((offset + i) % 251).toByte)

  /** A CMD entry whose only iovec points into the command ring, outside the data area. */
  private def badIovecEntry(k: FakeKernel): Array[Byte] =
    k.commandEntry(k.allocId(), read10(0, 1), Seq((k.cmdrOff.toLong, Bs.toLong)))

  private def stub(f: (Array[Byte], () => Array[Byte], Long) => Future[(ScsiResult, Array[Byte])]): ScsiHandler =
    new ScsiHandler:
      def handle(cdb: Array[Byte], dataOut: () => Array[Byte], dataInCapacity: Long)
          : Future[(ScsiResult, Array[Byte])] = f(cdb, dataOut, dataInCapacity)

  test("a read completes with data and a kick"):
    withDevice(): fx =>
      val id = fx.kernel.submit(read10(2, 1), readLen = Bs)
      val List(c) = completions(fx.kernel, 1): @unchecked
      c.cmdId should be (id)
      c.status should be (ScsiStatus.Good)
      c.readLen should be (Some(Bs))
      c.data.toList should be (expected(2L * Bs, Bs))
      eventually("a kick")(fx.kernel.channel.kicks == 1)
      fx.device.processedCount should be (1L)

  test("out-of-order device completions are all delivered once"):
    withDevice(auto = false): fx =>
      val ids = (0 until 3).map(i => fx.kernel.submit(read10(i, 1), readLen = Bs)).toList
      Await.result(fx.dev.call(2), 5.seconds)
      val calls = fx.dev.calls
      def release(i: Int): Unit =
        val off = calls(i).args.head.asInstanceOf[Long]
        calls(i).succeed(expected(off, Bs).toArray)
      release(2)
      release(0)
      release(1)
      val got = completions(fx.kernel, 3)
      got.map(_.cmdId).toSet should be (ids.toSet)
      got.map(_.cmdId).distinct.size should be (3)
      got.foreach(_.status should be (ScsiStatus.Good))
      for c <- got do
        c.data.toList should be (expected(ids.indexOf(c.cmdId).toLong * Bs, Bs))
      eventually("three kicks")(fx.kernel.channel.kicks == 3)
      fx.kernel.completions() should be (empty)
      fx.failures.counts.get("stale-completion") should be (None)

  test("malformed entry fails with HARDWARE ERROR and the device keeps serving"):
    withDevice(): fx =>
      fx.kernel.submitRaw(badIovecEntry(fx.kernel))
      val good = fx.kernel.submit(read10(0, 1), readLen = Bs)
      val List(bad, ok) = completions(fx.kernel, 2): @unchecked
      bad.status should be (ScsiStatus.CheckCondition)
      senseKey(bad) should be (4)
      ok.cmdId should be (good)
      ok.status should be (ScsiStatus.Good)
      fx.failures.counts("malformed-entry") should be (1L)

  test("a malformed TMR entry is reported and never completed"):
    withDevice(): fx =>
      val len = align8(TmrCmdIds + 2)
      val buf = java.nio.ByteBuffer.allocate(len).order(java.nio.ByteOrder.nativeOrder())
      buf.putInt(EntLenOp, len | OpTmr)
      buf.putInt(TmrCmdCnt, 100)
      fx.kernel.submitRaw(buf.array())
      val good = fx.kernel.submit(read10(0, 1), readLen = Bs)
      val List(ok) = completions(fx.kernel, 1): @unchecked
      ok.cmdId should be (good)
      fx.failures.counts("malformed-entry") should be (1L)
      fx.failures.counts.get("stale-completion") should be (None)

  test("a TMR is consumed without a completion"):
    withDevice(): fx =>
      fx.kernel.submitTmr(1, Seq(5))
      val good = fx.kernel.submit(read10(0, 1), readLen = Bs)
      val List(ok) = completions(fx.kernel, 1): @unchecked
      ok.cmdId should be (good)
      fx.device.processedCount should be (2L)
      fx.failures.counts should be (empty)

  test("repeated failures are rate limited"):
    withDevice(): fx =>
      for _ <- 1 to 5 do fx.kernel.submitRaw(badIovecEntry(fx.kernel))
      completions(fx.kernel, 5).foreach(senseKey(_) should be (4))
      fx.failures.counts("malformed-entry") should be (5L)
      val errors = fx.sink.at(Level.Error).filter(_.contains("malformed-entry"))
      errors.size should be (1)
      errors.head should include ("cmd_head=")
      errors.head should include ("iovecs=[")
      fx.sink.at(Level.Warn).filter(_.contains("malformed-entry")) should be (empty)

      fx.clock.addAndGet(61.seconds.toNanos)
      fx.kernel.submitRaw(badIovecEntry(fx.kernel))
      completions(fx.kernel, 1)
      val warns = fx.sink.at(Level.Warn).filter(_.contains("malformed-entry"))
      warns.size should be (1)
      warns.head should include ("5 more malformed-entry since")
      warns.head should not include ("cmd_head=")
      fx.sink.at(Level.Error).count(_.contains("malformed-entry")) should be (1)
      fx.failures.counts("malformed-entry") should be (6L)

  test("ring corruption resets the ring and serving resumes"):
    withDevice(): fx =>
      fx.kernel.setCmdHead(3)
      fx.kernel.channel.signal()
      eventually("a ring reset")(fx.control.resets.get == 1)
      eventually("the report")(fx.failures.counts.get("ring-corrupt").contains(1L))
      val id = fx.kernel.submit(read10(1, 1), readLen = Bs)
      val List(c) = completions(fx.kernel, 1): @unchecked
      c.cmdId should be (id)
      c.status should be (ScsiStatus.Good)
      c.data.toList should be (expected(Bs.toLong, Bs))
      fx.control.resets.get should be (1)

  test("a completion for a command from before a ring reset is dropped with a warn"):
    withDevice(auto = false): fx =>
      fx.kernel.submit(read10(0, 1), readLen = Bs)
      val held = Await.result(fx.dev.call(0), 5.seconds)
      fx.kernel.setCmdHead(3)
      fx.kernel.channel.signal()
      eventually("a ring reset")(fx.control.resets.get == 1)
      held.succeed(expected(0, Bs).toArray)
      eventually("the warn")(fx.sink.at(Level.Warn).exists(_.contains("ring reset")))
      fx.failures.counts.get("stale-completion") should be (None)
      fx.kernel.completions() should be (empty)
      val id = fx.kernel.submit(read10(1, 1), readLen = Bs)
      Await.result(fx.dev.call(1), 5.seconds).succeed(expected(Bs.toLong, Bs).toArray)
      val List(c) = completions(fx.kernel, 1): @unchecked
      c.cmdId should be (id)
      c.status should be (ScsiStatus.Good)

  test("a failed resync is reported and retried on the next notification"):
    withDevice(): fx =>
      fx.control.onReset = () =>
        fx.kernel.resetRing()
        fx.kernel.segment.set(JAVA_SHORT, MbVersion.toLong, 1.toShort)
      fx.kernel.setCmdHead(3)
      fx.kernel.channel.signal()
      eventually("the resync failure")(fx.failures.counts.get("resync-failed").contains(1L))
      fx.control.onReset = () => fx.kernel.resetRing()
      fx.kernel.channel.signal()
      eventually("a second reset")(fx.control.resets.get == 2)
      val id = fx.kernel.submit(read10(0, 1), readLen = Bs)
      val List(c) = completions(fx.kernel, 1): @unchecked
      c.cmdId should be (id)
      c.status should be (ScsiStatus.Good)
      fx.failures.counts("ring-corrupt") should be (1L)

  test("returned data larger than the iovecs is HARDWARE ERROR"):
    val h = stub((_, _, cap) => Future.successful((ScsiResult.good(cap.toInt + 1), new Array[Byte](cap.toInt + 1))))
    withDevice(handler = Some(h)): fx =>
      fx.kernel.submit(read10(0, 1), readLen = Bs)
      val List(c) = completions(fx.kernel, 1): @unchecked
      c.status should be (ScsiStatus.CheckCondition)
      senseKey(c) should be (4)
      c.data.toList should be (List.fill(Bs)(0.toByte))
      fx.failures.counts("data-overflow") should be (1L)

  test("a handler that throws or fails gets HARDWARE ERROR and the device keeps serving"):
    val n = AtomicInteger()
    val h = stub: (cdb, _, cap) =>
      n.incrementAndGet() match
        case 1 => throw LinkageError("sync")
        case 2 => Future.failed(InterruptedException("async"))
        case _ => Future.successful((ScsiResult.good(cap.toInt), new Array[Byte](cap.toInt)))
    withDevice(handler = Some(h)): fx =>
      val ids = (0 until 3).map(i => fx.kernel.submit(read10(i, 1), readLen = Bs))
      val got = completions(fx.kernel, 3).map(c => c.cmdId -> c).toMap
      got(ids(0)).status should be (ScsiStatus.CheckCondition)
      senseKey(got(ids(0))) should be (4)
      got(ids(1)).status should be (ScsiStatus.CheckCondition)
      senseKey(got(ids(1))) should be (4)
      got(ids(2)).status should be (ScsiStatus.Good)
      fx.failures.counts("command-error") should be (2L)

  test("completion after stop is dropped"):
    withDevice(auto = false): fx =>
      fx.kernel.submit(read10(0, 1), readLen = Bs)
      val held = Await.result(fx.dev.call(0), 5.seconds)
      fx.device.stop()
      fx.kernel.close()
      held.succeed(expected(0, Bs).toArray)
      eventually("the warn")(fx.sink.at(Level.Warn).exists(_.contains("after stop")))
      fx.kernel.channel.kicks should be (0)

  test("stop wakes a blocked loop"):
    withDevice(): fx =>
      val start = System.nanoTime()
      Await.result(Future(fx.device.stop())(using ExecutionContext.global), 1.second)
      (System.nanoTime() - start) should be < 1.second.toNanos

  // ---- UioChannel, over a pipe standing in for the UIO fd ----

  private def withPipe(f: (UioChannel, Int) => Unit): Unit =
    val (r, w) = Libc.pipe()
    val ch = UioChannel(r)
    try f(ch, w)
    finally
      ch.close()
      Libc.close(w)

  private def writeInt(fd: Int): Unit =
    val arena = Arena.ofConfined()
    try
      val buf = arena.allocate(4)
      buf.set(JAVA_BYTE, 0, 1.toByte)
      Libc.write(fd, buf)
    finally arena.close()

  test("UioChannel await returns on a notification"):
    withPipe: (ch, w) =>
      writeInt(w)
      ch.await()

  test("UioChannel close wakes a blocked await, and await after close throws"):
    withPipe: (ch, _) =>
      val entered = scala.concurrent.Promise[Unit]()
      val waiting = Future {
        entered.success(())
        try
          ch.await()
          None
        catch case e: InterruptedException => Some(e)
      }(using ExecutionContext.global)
      Await.result(entered.future, 1.second)
      ch.close()
      Await.result(waiting, 1.second) shouldBe defined
      intercept[InterruptedException](ch.await())
      ch.kick()
