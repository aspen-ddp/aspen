package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, LogCapture, TestNetwork}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.server.Host

import java.io.IOException
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** A LockstepThreading whose DeviceIos stamp their observer and probe with `clock` and probe with `write` */
private final class ProbeSeamThreading(ec: ExecutionContext, clock: AtomicLong, write: os.Path => Unit)
  extends LockstepThreading(ec):
  override def newDeviceIo(path: os.Path): DeviceIo =
    new LockstepDeviceIo(path, this, DeviceIo.Config(), () => clock.get, write)

/** DeviceHealthJudge (device hang detection, spec section 5.2-5.3, 5.5) over an injected device whose probe has a
 *  manual clock and an injected write. Each pass is a posted `Host.CheckDeviceHealth`, drained on the control thread
 *  (lockstep). The probe thread is real: the one wait on it, for a probe to return, polls to a deadline */
class DeviceHealthJudgeSuite extends IntegrationTestSuite with ControlFixture:
  private val deviceId = StorageDeviceId(UUID.fromString("77777777-0000-0000-0000-000000000001"))
  private val threshold = HealthProbe.Config().hangThreshold

  /** A host with one injected device under `write`, its clock, and the probe's start time once armed */
  private final class Rig(write: os.Path => Unit):
    val clock = new AtomicLong(0)
    val host: Host = newHost(newHostDir(), threadingOf = ec => new ProbeSeamThreading(ec, clock, write))
    val dev: os.Path = host.control.rootDir / os.up / "judged-device"
    os.makeDir.all(dev)
    var sds: LocalStorageDeviceState = inject()
    val transitions = new ConcurrentLinkedQueue[(LocalStorageDeviceState, DeviceHealth)]()
    host.control.health.onTransition((s, h) => transitions.add(s -> h))

    def inject(): LocalStorageDeviceState =
      val s = new LocalStorageDeviceState(deviceId, dev, dev / "dummy.file", new TestNetwork.TestCRL(),
        host.control.deviceIos.forPath(dev))
      injectLoadedDevice(host, s)
      s

    def probe: HealthProbe = sds.io.probe

    /** One judge pass, as the periodic trigger runs it: a posted event handled on the control thread */
    def pass(): Unit =
      host.control.post(Host.CheckDeviceHealth())
      host.testingOnlyHandleEvents()

    def advance(d: FiniteDuration): Unit = clock.addAndGet(d.toNanos)

  /** Lines the judge logs at info or above while `f` runs */
  private def judgeLog(f: => Unit): List[(scribe.Level, String)] =
    val lines = new ConcurrentLinkedQueue[(scribe.Level, String)]()
    val handler = scribe.handler.LogHandler(scribe.Level.Info)(r => lines.add(r.level -> r.logOutput.plainText))
    LogCapture.during(classOf[DeviceHealthJudge].getName, handler)(f)
    lines.asScala.toList

  private def warns(log: List[(scribe.Level, String)]) = log.filter(_._1 == scribe.Level.Warn)

  /** Completes once `cond` holds, failing after 5 s of real time: for the probe's own thread */
  private def eventually(what: String)(cond: => Boolean): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + 5.seconds.toNanos
    def loop(): Future[Unit] =
      if cond then Future.unit
      else if System.nanoTime() > deadline then Future.failed(new AssertionError(s"Timed out waiting for $what"))
      else ThreadedHungDeviceSuite.delay(1).flatMap(_ => loop())
    loop()

  /** Runs `f` with the probe stuck in its write (armed, in flight), then lets it return and disarms it */
  private def withStuckProbe(f: (Rig, CountDownLatch) => Future[org.scalatest.compatible.Assertion])
      : Future[org.scalatest.compatible.Assertion] =
    given ExecutionContext = executionContext
    val latch = new CountDownLatch(1)
    val rig = new Rig(_ => latch.await())
    rig.probe.arm()
    eventually("the probe in flight")(rig.probe.inFlightAge.isDefined)
      .flatMap(_ => f(rig, latch))
      .andThen: _ =>
        rig.probe.disarm()
        latch.countDown()

  atest("healthy below the threshold"):
    withStuckProbe: (rig, _) =>
      rig.advance(threshold - 1.second)
      val log = judgeLog(rig.pass())
      rig.sds.health should be(DeviceHealth.Initial)
      rig.transitions.size should be(0)
      warns(log) should be(Nil)

  atest("hung above the threshold, once"):
    withStuckProbe: (rig, _) =>
      rig.advance(threshold + 1.second)
      val first = judgeLog(rig.pass())
      rig.sds.health.status should be(StorageDeviceState.HealthStatus.Hung)
      rig.sds.health.since should be > HLCTimestamp.Zero
      rig.sds.health.evidence should include(HealthProbe.Kind)
      rig.transitions.asScala.toList should be(List(rig.sds -> rig.sds.health))
      warns(first).size should be(1)
      first.size should be(1)
      val before = rig.sds.health
      val second = judgeLog(rig.pass())
      second should be(Nil)
      rig.transitions.size should be(1)
      rig.sds.health should be(before)

  atest("back to healthy when the probe returns"):
    given ExecutionContext = executionContext
    withStuckProbe: (rig, latch) =>
      rig.advance(threshold + 1.second)
      rig.pass()
      rig.sds.health.status should be(StorageDeviceState.HealthStatus.Hung)
      latch.countDown()
      eventually("the probe to return")(rig.probe.inFlightAge.isEmpty).map: _ =>
        val log = judgeLog(rig.pass())
        rig.sds.health.status should be(StorageDeviceState.HealthStatus.Healthy)
        rig.sds.health.since should be > HLCTimestamp.Zero
        log.map(_._1) should be(List(scribe.Level.Info))
        log.head._2 should include("hung for")
        rig.transitions.size should be(2)

  atest("a probe error alone stays healthy"):
    given ExecutionContext = executionContext
    val rig = new Rig(_ => throw new IOException("injected EIO"))
    rig.probe.arm()
    eventually("the probe's error")(rig.probe.lastResult.exists(_.error.isDefined)).map: _ =>
      try
        val log = judgeLog:
          rig.pass()
          rig.pass()
        rig.sds.health should be(DeviceHealth.Initial)
        rig.transitions.size should be(0)
        log.size should be(1)
        log.head._2 should include("injected EIO")
      finally rig.probe.disarm()

  atest("an old non-probe operation does not trip hung"):
    val rig = new Rig(_ => ())
    rig.sds.io.observer.started(JobKind.Delete.label, None, 0L)
    rig.advance(1.hour)
    val log = judgeLog(rig.pass())
    rig.sds.health should be(DeviceHealth.Initial)
    log should be(Nil)
    Future.successful(succeed)

  atest("a reload inherits a stuck probe and is hung at once"):
    withStuckProbe: (rig, _) =>
      rig.advance(threshold + 1.second)
      rig.host.control.shutdown.unloadDevice(deviceId)
      rig.sds = rig.inject()
      rig.probe.arm()
      rig.sds.health should be(DeviceHealth.Initial)
      rig.pass()
      rig.sds.health.status should be(StorageDeviceState.HealthStatus.Hung)

  test("evidence is capped at 1024"):
    val t = new AtomicLong(0)
    val observer = new DeviceIoObserver(() => t.get)
    (0 until 200).foreach(i => observer.started(s"SyntheticKind$i", None, 0L))
    t.addAndGet(1.minute.toNanos)
    val probe = new HealthProbe(os.Path("/tmp/judge-evidence"), HealthProbe.Config(), observer, () => "unused",
      clock = () => t.get)
    val evidence = DeviceHealthJudge.evidence(probe, observer, readsOverBound = true)
    evidence.length should be <= DeviceHealthJudge.MaxEvidence
    evidence should include("SyntheticKind")
