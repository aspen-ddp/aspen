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

class SystemTaskRunnerSuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] = List(CountingSystemTask)

  private def serviceStatePtr(): Future[org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map: vs =>
      ServiceEntry.decode(vs.get.value.bytes).statePointer

  private def pollUntil(cond: () => Future[Boolean], timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      cond().flatMap: ok =>
        if ok then Future.unit
        else if System.nanoTime() > deadline then Future.failed(new AssertionError("timed out"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(Duration(50, MILLISECONDS))(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  atest("runner executes an assigned task to completion and removes it from the registry"):
    given ExecutionContext = executionContext
    for
      statePtr <- serviceStatePtr()
      // Enroll a task that completes after 2 bumps.
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(2))
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
      (taskId, taskStatePtr) = enrolled.head
      runner = new SystemTaskRunner(client, statePtr, HostId.BootstrapHostId)
      _ = runner.receive(ExecuteSystemTask(HostId.BootstrapHostId, client.clientId, taskId, taskStatePtr))
      _ <- pollUntil(() => SystemTaskServiceState.scan(client, statePtr).map(_.isEmpty),
                     Duration(20000, MILLISECONDS))
      finalScan <- SystemTaskServiceState.scan(client, statePtr)
    yield
      runner.shutdown()
      finalScan shouldBe empty
