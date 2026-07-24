package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ExecuteSystemTask
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class SystemTaskExecutorServiceSuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] =
    List(SystemTaskExecutorService, CountingSystemTask)

  private def makeExecutor(): SimpleDurableServiceExecutor =
    new SimpleDurableServiceExecutor(
      client, HostId.BootstrapHostId, client.backgroundTaskManager,
      leaseDuration        = Duration(2000, MILLISECONDS),
      renewalInterval      = Duration(200, MILLISECONDS),
      minScanInterval      = Duration(40, MILLISECONDS),
      maxScanInterval      = Duration(80, MILLISECONDS),
      claimDelayPerService = Duration(0, MILLISECONDS))

  private def serviceStatePtr(): Future[org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map: vs =>
      ServiceEntry.decode(vs.get.value.bytes).statePointer

  private def sleep(d: Duration): Future[Unit] =
    val p = Promise[Unit]()
    client.backgroundTaskManager.schedule(d)(p.success(()))
    p.future

  atest("service dispatches an enrolled task and a runner completes it"):
    given ExecutionContext = executionContext
    SystemTaskExecutorService.reconcilePeriod = Duration(100, MILLISECONDS)
    SystemTaskExecutorService.monitorPeriod   = Duration(100, MILLISECONDS)
    SystemTaskExecutorService.firstCheckDelay = Duration(200, MILLISECONDS)
    SystemTaskExecutorService.stallTimeout    = Duration(1000, MILLISECONDS)
    val exec = makeExecutor()
    for
      statePtr <- serviceStatePtr()
      runner = new SystemTaskRunner(client, statePtr, HostId.BootstrapHostId)
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(2))
      _ <- driveUntilEmpty(statePtr, runner, Duration(20000, MILLISECONDS))
      finalScan <- SystemTaskServiceState.scan(client, statePtr)
    yield
      exec.shutdown()
      runner.shutdown()
      resetTuning()
      finalScan shouldBe empty

  atest("service reassigns a task whose heartbeat stalls (no runner)"):
    given ExecutionContext = executionContext
    SystemTaskExecutorService.reconcilePeriod = Duration(100, MILLISECONDS)
    SystemTaskExecutorService.monitorPeriod   = Duration(100, MILLISECONDS)
    SystemTaskExecutorService.firstCheckDelay = Duration(150, MILLISECONDS)
    SystemTaskExecutorService.stallTimeout    = Duration(300, MILLISECONDS)
    val exec = makeExecutor()
    for
      statePtr <- serviceStatePtr()
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(5))
      dispatches <- countDispatchesFor(Duration(3000, MILLISECONDS))
    yield
      exec.shutdown()
      resetTuning()
      dispatches should be >= 2

  private def resetTuning(): Unit =
    SystemTaskExecutorService.reconcilePeriod = SystemTaskExecutorService.DefaultReconcilePeriod
    SystemTaskExecutorService.monitorPeriod   = SystemTaskExecutorService.DefaultMonitorPeriod
    SystemTaskExecutorService.firstCheckDelay = SystemTaskExecutorService.DefaultFirstCheckDelay
    SystemTaskExecutorService.stallTimeout    = SystemTaskExecutorService.DefaultStallTimeout

  /** Poll: deliver any captured ExecuteSystemTask messages to the runner, then check emptiness. */
  private def driveUntilEmpty(statePtr: org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer,
                              runner: SystemTaskRunner,
                              timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      net.takeCapturedHostMessages().foreach:
        case m: ExecuteSystemTask => runner.receive(m)
        case _ => ()
      SystemTaskServiceState.scan(client, statePtr).flatMap: active =>
        if active.isEmpty then Future.unit
        else if System.nanoTime() > deadline then Future.failed(new AssertionError(s"not empty: $active"))
        else sleep(Duration(50, MILLISECONDS)).flatMap(_ => loop())
    loop()

  /** Count distinct ExecuteSystemTask dispatch messages captured over `window`. */
  private def countDispatchesFor(window: Duration): Future[Int] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + window.toNanos
    def loop(count: Int): Future[Int] =
      val n = count + net.takeCapturedHostMessages().count(_.isInstanceOf[ExecuteSystemTask])
      if System.nanoTime() > deadline then Future.successful(n)
      else sleep(Duration(50, MILLISECONDS)).flatMap(_ => loop(n))
    loop(0)
