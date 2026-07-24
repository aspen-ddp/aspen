package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ExecuteSystemTask
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.compute.ServiceEntry

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class StoreManagerSystemTaskSuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] = List(CountingSystemTask)

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

  atest("StoreManager runs a task delivered via ExecuteSystemTask"):
    given ExecutionContext = executionContext
    for
      statePtr <- serviceStatePtr()
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(1))
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
      (taskId, taskStatePtr) = enrolled.head
      // Deliver the execute message straight to the StoreManager, as the network would.
      _ = net.smgr.receiveHostMessage(
            ExecuteSystemTask(HostId.BootstrapHostId, client.clientId, taskId, taskStatePtr))
      _ = net.handleEvents()
      _ <- driveEmpty(statePtr, Duration(20000, MILLISECONDS))
      finalScan <- SystemTaskServiceState.scan(client, statePtr)
    yield
      finalScan shouldBe empty

  private def driveEmpty(statePtr: org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer,
                         timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      net.handleEvents()
      SystemTaskServiceState.scan(client, statePtr).flatMap: active =>
        if active.isEmpty then Future.unit
        else if System.nanoTime() > deadline then Future.failed(new AssertionError(s"not empty: $active"))
        else sleep(Duration(50, MILLISECONDS)).flatMap(_ => loop())
    loop()
