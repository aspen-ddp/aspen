package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId,
                                            StorageDeviceSetState}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, HOURS, MILLISECONDS, MINUTES}

class RebalancingServiceSuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] = List(RebalancingDurableService)

  private def makeExecutor(): SimpleDurableServiceExecutor =
    new SimpleDurableServiceExecutor(
      client, HostId.BootstrapHostId, client.backgroundTaskManager,
      leaseDuration        = Duration(2000, MILLISECONDS),
      renewalInterval      = Duration(200, MILLISECONDS),
      minScanInterval      = Duration(40, MILLISECONDS),
      maxScanInterval      = Duration(80, MILLISECONDS),
      claimDelayPerService = Duration(0, MILLISECONDS))

  atest("rebalanceStorageDeviceSet drains pendingTransfers and empties ActiveRebalancingTasks"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    // The RebalancingDurableService entry is created by Bootstrap (see TestNetwork), so the
    // executor only needs to scan and claim it — no explicit registration here.
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      _ <- RebalancingDurableService.rebalanceStorageDeviceSet(client, setId)
      _ <- driveUntilActiveEmpty(setId, Duration(20000, MILLISECONDS))
      finalSet <- client.getStorageDeviceSetState(setId)
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      finalSet.pendingTransfers shouldBe empty

  /** Look up the service's KV state object via the services TKVL. */
  private def readServiceStatePointer(): Future[KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    tkvl.get(Key(RebalancingDurableService.ServiceUUID)).map:
      case Some(vs) => ServiceEntry.decode(vs.value.bytes).statePointer
      case None => throw new IllegalStateException("RebalancingDurableService is not registered")

  /** Read the service's ActiveRebalancingTasks. */
  private def readActiveTasks(): Future[RebalancingServiceState.ActiveTasks] =
    given ExecutionContext = executionContext
    for
      ptr <- readServiceStatePointer()
      kvos <- client.read(ptr)
    yield RebalancingServiceState.decodeActiveTasks(
            kvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)

  private def readLastAutoRebalance(): Future[HLCTimestamp] =
    given ExecutionContext = executionContext
    for
      ptr <- readServiceStatePointer()
      kvos <- client.read(ptr)
    yield RebalancingServiceState.decodeLastAutoRebalance(
            kvos.contents(RebalancingServiceState.LastAutoRebalanceKey).value.bytes)

  /** Overwrite the last-sweep timestamp so that a sweep is, or is not, due. There is no public
   *  API for this: only a completed sweep writes it in production. */
  private def writeLastAutoRebalance(ts: HLCTimestamp): Future[Unit] =
    given ExecutionContext = executionContext
    readServiceStatePointer().flatMap: ptr =>
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        client.read(ptr).map: kvos =>
          val reqs = KeyValueUpdate.KeyRevision(
                       RebalancingServiceState.LastAutoRebalanceKey,
                       kvos.contents(RebalancingServiceState.LastAutoRebalanceKey).revision) :: Nil
          val ops = Insert(RebalancingServiceState.LastAutoRebalanceKey,
                      RebalancingServiceState.encodeLastAutoRebalance(ts)) :: Nil
          tx.update(ptr, None, None, reqs, ops)

  /** Poll `cond` until it holds or `timeout` elapses. Fails the test on timeout. */
  private def awaitUntil(what: String, timeout: Duration = Duration(20000, MILLISECONDS))
                        (cond: => Future[Boolean]): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      cond.flatMap: ok =>
        if ok then Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(s"timed out waiting for: $what"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(Duration(100, MILLISECONDS)):
            p.success(())
          p.future.flatMap(_ => loop())
    loop()

  /** Let the service poll several times, then report whether anything was enrolled. Used for
   *  the negative cases, where the assertion is that no sweep happened. */
  private def nothingEnrolledAfterSeveralPolls(): Future[Boolean] =
    given ExecutionContext = executionContext
    val p = Promise[Unit]()
    client.backgroundTaskManager.schedule(Duration(2000, MILLISECONDS)):
      p.success(())
    p.future.flatMap(_ => readActiveTasks()).map(_.isEmpty)

  /** Repeatedly complete in-flight transfers and wait until the service removes the task from
   *  ActiveRebalancingTasks (which happens only after the task drains pendingTransfers and
   *  completes). Fails on timeout. */
  private def driveUntilActiveEmpty(setId: StorageDeviceSetId, timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      readActiveTasks().flatMap: active =>
        if active.isEmpty then Future.unit
        else if System.nanoTime() > deadline then
          client.getStorageDeviceSetState(setId).flatMap: s =>
            Future.failed(new AssertionError(
              s"active tasks did not empty; active=$active pending=${s.pendingTransfers}"))
        else
          net.completeInFlightTransfers(setId).flatMap: _ =>
            val p = Promise[Unit]()
            client.backgroundTaskManager.schedule(Duration(100, MILLISECONDS)):
              p.success(())
            p.future.flatMap(_ => loop())
    loop()

  atest("setAutoRebalancePeriod persists the period and getAutoRebalancePeriod reads it back"):
    given ExecutionContext = executionContext
    for
      initial  <- RebalancingDurableService.getAutoRebalancePeriod(client)
      _        <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(4, HOURS))
      updated  <- RebalancingDurableService.getAutoRebalancePeriod(client)
      _        <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration.Zero)
      disabled <- RebalancingDurableService.getAutoRebalancePeriod(client)
    yield
      initial shouldBe Duration(8, HOURS)
      updated shouldBe Duration(240, MINUTES)
      disabled shouldBe Duration(0, MINUTES)

  atest("the sweep enrolls an unbalanced level-0 set once the period has elapsed"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(1, MINUTES))
      _ <- writeLastAutoRebalance(HLCTimestamp.Zero)
      _ <- awaitUntil("the sweep to enroll the set")(readActiveTasks().map(_.exists(_._1 == setId)))
      _ <- driveUntilActiveEmpty(setId, Duration(20000, MILLISECONDS))
      lastSweep <- readLastAutoRebalance()
      finalSet <- client.getStorageDeviceSetState(setId)
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      finalSet.pendingTransfers shouldBe empty
      // recordSweep stamped it; asserted as "not Zero" rather than an ordering comparison,
      // since HLCTimestamp is an AnyVal and ScalaTest's `be >` needs an Ordering in scope.
      lastSweep should not be HLCTimestamp.Zero

  atest("a zero period disables the sweep even when a set is unbalanced"):
    given ExecutionContext = executionContext
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration.Zero)
      _ <- writeLastAutoRebalance(HLCTimestamp.Zero)
      quiet <- nothingEnrolledAfterSeveralPolls()
      lastSweep <- readLastAutoRebalance()
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      quiet shouldBe true
      lastSweep shouldBe HLCTimestamp.Zero

  atest("a recent sweep defers the next one until the period elapses"):
    given ExecutionContext = executionContext
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(8, HOURS))
      _ <- writeLastAutoRebalance(HLCTimestamp.now)
      quiet <- nothingEnrolledAfterSeveralPolls()
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      quiet shouldBe true

  atest("the sweep skips a level-1 set instead of failing on it"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      // A level-1 set holds sets, not devices. getStateForRebalancePlanning throws on it, so
      // the sweep must filter it out before planning; if it does not, the level-0 set below
      // is never reached and the awaitUntil times out.
      upper <- client.createStorageDeviceSet("upper", 1, None)
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(1, MINUTES))
      _ <- writeLastAutoRebalance(HLCTimestamp.Zero)
      _ <- awaitUntil("the sweep to enroll the level-0 set")(
             readActiveTasks().map(_.exists(_._1 == setId)))
      active <- readActiveTasks()
      _ <- driveUntilActiveEmpty(setId, Duration(20000, MILLISECONDS))
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      active.map(_._1) should not contain upper

  atest("a set already being rebalanced is not enrolled twice by the sweep"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      // Enroll by hand first, so the set is in ActiveTasks (and shortly has non-empty
      // pendingTransfers) before the sweep ever looks at it.
      _ <- RebalancingDurableService.rebalanceStorageDeviceSet(client, setId)
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(1, MINUTES))
      _ <- writeLastAutoRebalance(HLCTimestamp.Zero)
      duringSweep <- readActiveTasks()
      _ <- driveUntilActiveEmpty(setId, Duration(20000, MILLISECONDS))
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      duringSweep.count(_._1 == setId) shouldBe 1

  atest("a set whose planning fails does not abort the sweep"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      broken <- client.createStorageDeviceSet("broken", 0, None)
      _ <- addBogusMemberDevice(broken)
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(1, MINUTES))
      _ <- writeLastAutoRebalance(HLCTimestamp.Zero)
      // listStorageDeviceSets() gives no ordering guarantee, so this asserts the property that
      // matters either way round: the healthy set is reached and the sweep is recorded.
      _ <- awaitUntil("the healthy set to be enrolled despite the broken one")(
             readActiveTasks().map(_.exists(_._1 == setId)))
      _ <- driveUntilActiveEmpty(setId, Duration(20000, MILLISECONDS))
      lastSweep <- readLastAutoRebalance()
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      lastSweep should not be HLCTimestamp.Zero

  /** Append a member device id that has no StorageDeviceState object, so that
   *  getStateForRebalancePlanning fails for this set. */
  private def addBogusMemberDevice(setId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = executionContext
    val bogus = StorageDeviceId(UUID.randomUUID())
    client.getStorageDeviceSetPointer(setId).flatMap: setPtr =>
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        client.read(setPtr).map: dos =>
          val current = StorageDeviceSetState(dos)
          val updated = current.copy(memberDevices = bogus :: current.memberDevices)
          tx.overwrite(setPtr, dos.revision, DataBuffer(updated.toBytes))
