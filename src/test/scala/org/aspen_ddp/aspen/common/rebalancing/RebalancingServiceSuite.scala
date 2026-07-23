package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceSetId}
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

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
    val exec = makeExecutor()
    for
      _ <- RebalancingDurableService.register(exec)
      _ <- net.createSecondDevice()
      _ <- RebalancingDurableService.rebalanceStorageDeviceSet(client, setId)
      _ <- driveUntilActiveEmpty(setId, Duration(20000, MILLISECONDS))
      finalSet <- client.getStorageDeviceSetState(setId)
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      finalSet.pendingTransfers shouldBe empty

  /** Read the service's ActiveRebalancingTasks via the services TKVL. */
  private def readActiveTasks(): Future[List[(StorageDeviceSetId, org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer)]] =
    given ExecutionContext = executionContext
    val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    tkvl.get(Key(RebalancingDurableService.ServiceUUID)).flatMap:
      case Some(vs) =>
        val statePtr = ServiceEntry.decode(vs.value.bytes).statePointer
        client.read(statePtr).map: kvos =>
          RebalancingServiceState.decodeActiveTasks(
            kvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
      case None => Future.successful(Nil)

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
