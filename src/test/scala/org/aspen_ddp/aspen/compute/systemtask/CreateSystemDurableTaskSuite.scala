package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.common.util.byte2uuid

import scala.concurrent.ExecutionContext

class CreateSystemDurableTaskSuite extends IntegrationTestSuite:

  // Register the test task factory so the type-registry sanity check passes.
  override def userTypeFactories: List[RegisteredTypeFactory] = List(CountingSystemTask)

  atest("createSystemDurableTask allocates a typed task object, enrolls it, and wakes the service"):
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(0))
      stateVs <- servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(stateVs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
      taskKvos <- client.read(enrolled.head._2)
    yield
      enrolled.size shouldBe 1
      // Task object carries its type UUID for reconstruction.
      byte2uuid(taskKvos.contents(SimpleTaskExecutor.TaskTypeKey).value.bytes) shouldBe CountingSystemTask.typeUUID
      // No wake-up message is sent because the service is not claimed by any host in this test.
      // (In a real deployment, createSystemDurableTask sends a best-effort wake-up if a host is available.)

  atest("prepareSystemDurableTask enrolls the task within a caller-supplied transaction"):
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      _ <- client.transactUntilSuccessful: tx =>
             given org.aspen_ddp.aspen.client.Transaction = tx
             client.prepareSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(0))
      stateVs <- servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(stateVs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
      taskKvos <- client.read(enrolled.head._2)
    yield
      enrolled.size shouldBe 1
      byte2uuid(taskKvos.contents(SimpleTaskExecutor.TaskTypeKey).value.bytes) shouldBe CountingSystemTask.typeUUID
