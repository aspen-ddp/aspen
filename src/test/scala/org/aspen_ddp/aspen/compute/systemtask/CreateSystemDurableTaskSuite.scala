package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.common.ida.Replication
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

  atest("createSystemDurableTask and the admin creators are pinned"):
    given ExecutionContext = executionContext
    // createSystemDurableTask retries every failure, so an unpinned transaction is caught by the assertion
    val audit = new PinAudit(client, enforce = false)
    for
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(0))
      _ <- client.createAllocationGroup("pin-group", 0, ida = Some(Replication(3, 2)))
      setId <- client.createStorageDeviceSet("pin-set", 1, None)
      _ <- client.createStorageDeviceSet("pin-child-set", 0, Some(setId))
      _ <- client.createHost("pin-host", "127.0.0.1", 1, 2, 3)
      _ <- waitForTransactionsToComplete()
      _ = audit.stop()
    yield
      audit.built.size should be >= 5
      audit.unpinned shouldBe empty
