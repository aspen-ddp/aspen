package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{Key, Value}
import org.aspen_ddp.aspen.compute.ServiceEntry

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class SystemTaskServiceStateSuite extends IntegrationTestSuite:

  private def servicePtr(): Future[org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map: vs =>
      ServiceEntry.decode(vs.get.value.bytes).statePointer

  atest("enroll then scan returns the task; remove empties the tree"):
    given ExecutionContext = executionContext
    val taskId = UUID.randomUUID()
    for
      statePtr <- servicePtr()
      // Allocate a dummy task-state object to enroll.
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool)
      taskStatePtr <- client.transactUntilSuccessful: tx =>
                        given Transaction = tx
                        allocator.allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](7))))
      _ <- SystemTaskServiceState.enroll(client, statePtr, taskId, taskStatePtr)
      afterEnroll <- SystemTaskServiceState.scan(client, statePtr)
      _ <- SystemTaskServiceState.remove(client, statePtr, taskId)
      afterRemove <- SystemTaskServiceState.scan(client, statePtr)
    yield
      afterEnroll.map(_._1) should contain (taskId)
      afterEnroll.find(_._1 == taskId).map(_._2.id) shouldBe Some(taskStatePtr.id)
      afterRemove.map(_._1) should not contain taskId

  atest("enrollInTx enrolls within a caller-supplied transaction"):
    given ExecutionContext = executionContext
    val taskId = UUID.randomUUID()
    for
      statePtr <- servicePtr()
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool)
      // Allocate the task-state object and enroll it in a single transaction.
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             for
               taskStatePtr <- allocator.allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](9))))
               _ <- SystemTaskServiceState.enrollInTx(client, statePtr, taskId, taskStatePtr)
             yield ()
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield
      enrolled.map(_._1) should contain (taskId)
