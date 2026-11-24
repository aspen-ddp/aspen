package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.{IntegrationTestSuite, LogCapture, TestNetwork}
import org.aspen_ddp.aspen.client.StopRetrying
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.server.Host
import org.aspen_ddp.aspen.server.host.DeviceHealthPublisher.Outcome
import org.aspen_ddp.aspen.server.usage.StorageDeviceUsageManager

import java.io.IOException
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

/** The judge's health publish (device hang detection, spec section 5.3): with a fake publisher the test completes by
 *  hand, and with the real ClientDeviceHealthPublisher against the test network's device records. Each pass is a
 *  posted `Host.CheckDeviceHealth` drained on the control thread (lockstep), and so is each publish's outcome */
class DeviceHealthPublishSuite extends IntegrationTestSuite with ControlFixture:
  private val threshold = HealthProbe.Config().hangThreshold
  private val Hung = StorageDeviceState.HealthStatus.Hung
  private val Healthy = StorageDeviceState.HealthStatus.Healthy

  /** Records each publish, completed by the test */
  private final class FakePublisher extends DeviceHealthPublisher:
    val calls = new ConcurrentLinkedQueue[(Promise[Outcome], () => DeviceHealth)]()
    def publish(id: StorageDeviceId, current: () => Option[DeviceHealth]): Future[Outcome] =
      val p = Promise[Outcome]()
      calls.add(p -> (() => current().get))
      p.future
    def call(i: Int): (Promise[Outcome], () => DeviceHealth) = calls.asScala.toList(i)

  /** Holds the first publish until `release` completes, then runs it, and every later one at once, through `to` */
  private final class HoldFirst(to: DeviceHealthPublisher) extends DeviceHealthPublisher:
    val release = Promise[Unit]()
    @volatile private var first = true
    @volatile var firstDone: Option[Future[Outcome]] = None
    def publish(id: StorageDeviceId, current: () => Option[DeviceHealth]): Future[Outcome] =
      if first then
        first = false
        val f = release.future.flatMap(_ => to.publish(id, current))(using executionContext)
        firstDone = Some(f)
        f
      else to.publish(id, current)

  /** A host with one injected device `deviceId` whose probe writes with `write`, under a manual clock */
  private final class PublishRig(val deviceId: StorageDeviceId, write: os.Path => Unit,
                          publisher: Option[DeviceHealthPublisher] = None):
    val clock = new AtomicLong(0)
    val host: Host = newHost(newHostDir(), threadingOf = ec => new ProbeSeamThreading(ec, clock, write))
    publisher.foreach(host.control.healthPublisher = _)
    val dev: os.Path = host.control.rootDir / os.up / "published-device"
    os.makeDir.all(dev)
    var sds: LocalStorageDeviceState = inject()

    private def inject(): LocalStorageDeviceState =
      val s = new LocalStorageDeviceState(deviceId, dev, dev / "dummy.file", new TestNetwork.TestCRL(),
        host.control.deviceIos.forPath(dev))
      injectLoadedDevice(host, s)
      s

    /** Unloads the device and loads a fresh state for it, on the same path */
    def reload(): Unit =
      host.control.shutdown.unloadDevice(deviceId)
      sds = inject()

    def probe: HealthProbe = sds.io.probe

    def pass(): Unit =
      host.control.post(Host.CheckDeviceHealth())
      host.testingOnlyHandleEvents()

    /** Applies the publish outcomes already posted */
    def drain(): Unit = host.testingOnlyHandleEvents()

    def advance(d: FiniteDuration): Unit = clock.addAndGet(d.toNanos)

    /** One pass, then drains until its publish's outcome has been applied */
    def passAndSettle(): Future[Unit] =
      pass()
      settle()

    def settle(): Future[Unit] =
      given ExecutionContext = executionContext
      val deadline = System.nanoTime() + 5.seconds.toNanos
      def loop(): Future[Unit] =
        if !sds.healthPublishing then Future.unit
        else if System.nanoTime() > deadline then Future.failed(new AssertionError("Timed out waiting for the publish"))
        else
          for
            _ <- waitForTransactionsToComplete()
            _ <- Future(drain())
            _ <- loop()
          yield ()
      loop()

  /** Lines the judge logs at info or above about the device while `f` runs (suites log through it in parallel) */
  private def judgeLog(deviceId: StorageDeviceId)(f: => Unit): List[(scribe.Level, String)] =
    val lines = new ConcurrentLinkedQueue[(scribe.Level, String)]()
    val handler = scribe.handler.LogHandler(scribe.Level.Info): r =>
      val text = r.logOutput.plainText
      if text.contains(deviceId.toString) then lines.add(r.level -> text)
    LogCapture.during(classOf[DeviceHealthJudge].getName, handler)(f)
    lines.asScala.toList

  private def eventually(what: String)(cond: => Boolean): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + 5.seconds.toNanos
    def loop(): Future[Unit] =
      if cond then Future.unit
      else if System.nanoTime() > deadline then Future.failed(new AssertionError(s"Timed out waiting for $what"))
      else ThreadedHungDeviceSuite.delay(1).flatMap(_ => loop())
    loop()

  /** Runs `f` over a rig whose probe is stuck in its write past the threshold, then lets it return and disarms it */
  private def withHungRig(deviceId: StorageDeviceId, publisher: Option[DeviceHealthPublisher] = None)
                         (f: (PublishRig, CountDownLatch) => Future[org.scalatest.compatible.Assertion])
      : Future[org.scalatest.compatible.Assertion] =
    given ExecutionContext = executionContext
    val latch = new CountDownLatch(1)
    val rig = new PublishRig(deviceId, _ => latch.await(), publisher)
    rig.probe.arm()
    eventually("the probe in flight")(rig.probe.inFlightAge.isDefined)
      .flatMap: _ =>
        rig.advance(threshold + 1.second)
        f(rig, latch)
      .andThen: _ =>
        rig.probe.disarm()
        latch.countDown()

  private def freshId(): StorageDeviceId = StorageDeviceId(UUID.randomUUID())

  /** A record for `id` owned by this host (TestNetwork.createDevice) */
  private def newRecord(): Future[StorageDeviceId] =
    given ExecutionContext = executionContext
    val id = freshId()
    net.createDevice(id).map(_ => id)

  private def revisionOf(id: StorageDeviceId) =
    given ExecutionContext = executionContext
    for
      ptr <- client.getStorageDevicePointer(id)
      kvos <- client.read(ptr)
    yield kvos.contents(StorageDeviceState.StateKey).revision

  private def stateOf(id: StorageDeviceId): Future[StorageDeviceState] = client.getStorageDeviceState(id)

  /** A read-modify-write of the record under a KeyRevision, as every other writer of it does */
  private def writeRecord(id: StorageDeviceId)(f: StorageDeviceState => StorageDeviceState): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      for
        ptr <- client.getStorageDevicePointer(id)
        kvos <- client.read(ptr)
      yield
        val reqs = List(KeyRevision(StorageDeviceState.StateKey, kvos.contents(StorageDeviceState.StateKey).revision))
        tx.update(ptr, None, None, reqs,
          List(Insert(StorageDeviceState.StateKey, f(StorageDeviceState(kvos)).encode())))

  // ---- With a fake publisher ----

  atest("a transition is published and a failure is retried next pass, warned once per streak"):
    given ExecutionContext = executionContext
    val fake = new FakePublisher
    withHungRig(freshId(), Some(fake)): (rig, latch) =>
      def fail(i: Int): List[(scribe.Level, String)] = judgeLog(rig.deviceId):
        fake.call(i)._1.failure(new IOException(s"injected publish failure $i"))
        rig.drain()
      rig.pass()
      rig.sds.health.status should be(Hung)
      fake.calls.size should be(1)
      val first = fail(0)
      first.map(_._1) should be(List(scribe.Level.Warn))
      first.head._2 should include("injected publish failure 0")
      rig.sds.healthPublishing should be(false)
      rig.sds.publishedHealth should be(None)

      // The streak goes on: retried, and not warned again
      rig.pass()
      fake.calls.size should be(2)
      fail(1) should be(Nil)
      rig.sds.publishedHealth should be(None)

      rig.pass()
      fake.calls.size should be(3)
      val (p, current) = fake.call(2)
      current().status should be(Hung)
      p.success(Outcome.Written(current()))
      rig.drain()
      rig.sds.publishedHealth should be(Some(rig.sds.health))
      rig.pass()
      fake.calls.size should be(3)

      // After a success, the next failure starts a new streak: warned again
      latch.countDown()
      eventually("the probe to return")(rig.probe.inFlightAge.isEmpty).map: _ =>
        judgeLog(rig.deviceId)(rig.pass())
        rig.sds.health.status should be(Healthy)
        fake.calls.size should be(4)
        fail(3).map(_._1) should be(List(scribe.Level.Warn))

  atest("a transition during a publish ends with the newer value"):
    given ExecutionContext = executionContext
    val fake = new FakePublisher
    withHungRig(freshId(), Some(fake)): (rig, latch) =>
      rig.pass()
      val (held, current) = fake.call(0)
      val old = current()
      old.status should be(Hung)
      latch.countDown()
      eventually("the probe to return")(rig.probe.inFlightAge.isEmpty).map: _ =>
        judgeLog(rig.deviceId)(rig.pass())
        rig.sds.health.status should be(Healthy)
        fake.calls.size should be(1)
        held.success(Outcome.Written(old))
        rig.drain()
        rig.sds.publishedHealth should be(Some(old))

        rig.pass()
        fake.calls.size should be(2)
        val (p, next) = fake.call(1)
        next().status should be(Healthy)
        p.success(Outcome.Written(next()))
        rig.drain()
        rig.sds.publishedHealth should be(Some(rig.sds.health))
        rig.sds.health.status should be(Healthy)

  atest("no publish while one is in flight"):
    val fake = new FakePublisher
    val rig = new PublishRig(freshId(), _ => (), Some(fake))
    rig.pass()
    rig.pass()
    fake.calls.size should be(1)
    rig.sds.healthPublishing should be(true)
    Future.successful(succeed)

  // ---- With the real client ----

  atest("Hung then Healthy are written with since and evidence"):
    given ExecutionContext = executionContext
    newRecord().flatMap: id =>
      withHungRig(id): (rig, latch) =>
        for
          _ <- rig.passAndSettle()
          hung = rig.sds.health
          s1 <- stateOf(id)
          _ = hung.status should be(Hung)
          _ = s1.health should be(Hung)
          _ = s1.healthSince should be(hung.since)
          _ = s1.healthEvidence should be(hung.evidence)
          _ = s1.healthEvidence should include(HealthProbe.Kind)
          _ = rig.sds.publishedHealth should be(Some(hung))
          _ = latch.countDown()
          _ <- eventually("the probe to return")(rig.probe.inFlightAge.isEmpty)
          _ <- Future(judgeLog(rig.deviceId)(rig.pass()))
          _ <- rig.settle()
          s2 <- stateOf(id)
        yield
          rig.sds.health.status should be(Healthy)
          s2.health should be(Healthy)
          s2.healthSince should be(rig.sds.health.since)
          s2.healthSince should be > hung.since
          s2.healthEvidence should be(rig.sds.health.evidence)
          rig.sds.publishedHealth should be(Some(rig.sds.health))

  atest("a healthy record at load is not written"):
    given ExecutionContext = executionContext
    for
      id <- newRecord()
      before <- revisionOf(id)
      rig = new PublishRig(id, _ => ())
      _ <- rig.passAndSettle()
      after <- revisionOf(id)
    yield
      after should be(before)
      rig.sds.publishedHealth should be(Some(DeviceHealth.Initial))

  atest("a stale Hung record is cleared after load"):
    given ExecutionContext = executionContext
    for
      id <- newRecord()
      _ <- writeRecord(id)(_.copy(health = Hung, healthSince = HLCTimestamp.now, healthEvidence = "probe in flight 3m"))
      rig = new PublishRig(id, _ => ())
      _ <- rig.passAndSettle()
      state <- stateOf(id)
    yield
      state.health should be(Healthy)
      state.healthSince should be > HLCTimestamp.Zero
      state.healthEvidence should be("")
      rig.sds.publishedHealth should be(Some(DeviceHealth.Initial))

  atest("nothing is written to a Failed record"):
    given ExecutionContext = executionContext
    newRecord().flatMap: id =>
      writeRecord(id)(_.copy(status = StorageDeviceState.DeviceStatus.Failed)).flatMap: _ =>
        withHungRig(id): (rig, _) =>
          for
            before <- revisionOf(id)
            _ <- rig.passAndSettle()
            after <- revisionOf(id)
          yield
            rig.sds.health.status should be(Hung)
            after should be(before)
            rig.sds.publishedHealth should be(Some(rig.sds.health))

  atest("nothing is written to a record naming another host"):
    given ExecutionContext = executionContext
    newRecord().flatMap: id =>
      writeRecord(id)(_.setHost(HostId(new UUID(7, 7)))).flatMap: _ =>
        withHungRig(id): (rig, _) =>
          for
            before <- revisionOf(id)
            _ <- rig.passAndSettle()
            after <- revisionOf(id)
          yield
            after should be(before)
            rig.sds.publishedHealth should be(Some(rig.sds.health))

  atest("a publish of an unloaded state writes nothing over the reloaded state's"):
    given ExecutionContext = executionContext
    newRecord().flatMap: id =>
      // The ControlFixture host is the bootstrap host, the record's owner
      val hold = new HoldFirst(new ClientDeviceHealthPublisher(client, HostId.BootstrapHostId))
      withHungRig(id, Some(hold)): (rig, latch) =>
        rig.pass()
        val old = rig.sds
        old.health.status should be(Hung)
        old.healthPublishing should be(true)
        latch.countDown()
        for
          _ <- eventually("the probe to return")(rig.probe.inFlightAge.isEmpty)
          before <- revisionOf(id)
          _ = rig.reload()
          _ <- rig.passAndSettle()
          _ = rig.sds.health should be(DeviceHealth.Initial)
          _ = rig.sds.publishedHealth should be(Some(DeviceHealth.Initial))
          _ = hold.release.success(())
          outcome <- hold.firstDone.get
          _ <- waitForTransactionsToComplete()
          _ = rig.drain()
          after <- revisionOf(id)
          state <- stateOf(id)
        yield
          outcome should be(Outcome.Skipped)
          after should be(before)
          state.health should be(Healthy)
          old.publishedHealth should be(None)

  atest("usage and store-entry writes keep the health fields, and health keeps theirs"):
    given ExecutionContext = executionContext
    val storeId = StoreId(PoolId(new UUID(3, 3)), 0)
    newRecord().flatMap: id =>
      withHungRig(id): (rig, _) =>
        rig.pass()
        rig.sds.healthPublishing should be(true)
        val usage = new StorageDeviceUsageManager(client).updateDeviceUsage(id, 5000L, 100000L)
        val entry = writeRecord(id)(_.setStoreEntry(storeId, StorageDeviceState.StoreStatus.Active, None))
        for
          _ <- usage
          _ <- entry
          _ <- rig.settle()
          state <- stateOf(id)
        yield
          state.health should be(Hung)
          state.healthSince should be(rig.sds.health.since)
          state.healthEvidence should be(rig.sds.health.evidence)
          state.currentUsage should be(5000L)
          state.totalSize should be(100000L)
          state.stores.get(storeId).map(_.status) should be(Some(StorageDeviceState.StoreStatus.Active))
          rig.sds.publishedHealth should be(Some(rig.sds.health))
