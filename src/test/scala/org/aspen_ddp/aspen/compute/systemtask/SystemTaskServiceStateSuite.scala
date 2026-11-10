package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{Key, Value}
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.common.util.uuid2byte

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
                        tx.replaySafe("test")
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
             tx.replaySafe("test")
             for
               taskStatePtr <- allocator.allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](9))))
               _ <- SystemTaskServiceState.enrollInTx(client, statePtr, taskId, taskStatePtr)
             yield ()
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield
      enrolled.map(_._1) should contain (taskId)

  atest("enroll, enrollInTx and remove are each pinned"):
    given ExecutionContext = executionContext
    val taskId = UUID.randomUUID()
    val otherId = UUID.randomUUID()
    for
      statePtr <- servicePtr()
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool)
      taskStatePtr <- client.transactUntilSuccessful: tx =>
                        given Transaction = tx
                        tx.replaySafe("test")
                        allocator.allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](7))))
      _ <- waitForTransactionsToComplete()
      // enroll and remove retry every failure, so an unpinned transaction is caught by the assertion, not enforced
      audit = new PinAudit(client)
      _ <- SystemTaskServiceState.enroll(client, statePtr, taskId, taskStatePtr)
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             tx.replaySafe("test")
             for
               p <- allocator.allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](9))))
               _ <- SystemTaskServiceState.enrollInTx(client, statePtr, otherId, p, pinned = true)
             yield ()
      _ <- SystemTaskServiceState.remove(client, statePtr, taskId)
      _ <- waitForTransactionsToComplete()
      _ = audit.stop()
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield
      audit.built.size should be >= 3
      audit.unpinned shouldBe empty
      // enroll pins on the task tree node it inserts into, never by bumping the task state object
      val enrollTx = audit.built.find(_.kvOps.values.flatten.exists:
        case i: org.aspen_ddp.aspen.common.objects.Insert => i.key == Key(uuid2byte(taskId))
        case _ => false).get
      enrollTx.description.requirements.exists:
        case kvu: org.aspen_ddp.aspen.common.transaction.KeyValueUpdate => kvu.requiredRevision.isDefined
        case _ => false
      .shouldBe(true)
      audit.built.flatMap(_.description.requirements).exists:
        case vb: org.aspen_ddp.aspen.common.transaction.VersionBump => vb.objectPointer.id == taskStatePtr.id
        case _ => false
      .shouldBe(false)
      enrolled.map(_._1) should contain (otherId)
      enrolled.map(_._1) should not contain taskId

  // The FSIF index release path (deferAllocationRelease) enrolls without the node pin, so those transactions do not
  // serialize on the system-task tree node; their deletion's refcount update is their pin
  atest("an unpinned enrollInTx carries no node revision and is pinned by the transaction's refcount update"):
    given ExecutionContext = executionContext
    val taskId = UUID.randomUUID()
    for
      statePtr <- servicePtr()
      // One enrollment first, so the second goes into an existing node rather than the tree's initial node
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             tx.replaySafe("test")
             client.getStoragePool(Radicle.poolId).flatMap: pool =>
               new PoolObjectAllocator(client, pool).allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](1))))
                 .flatMap(p => SystemTaskServiceState.enrollInTx(client, statePtr, UUID.randomUUID(), p, pinned = true))
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool)
      doomed <- client.transactUntilSuccessful: tx =>
                  given Transaction = tx
                  tx.replaySafe("test")
                  allocator.allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](2))))
      _ <- waitForTransactionsToComplete()
      doomedState <- client.read(doomed)
      audit = new PinAudit(client)
      tx = client.newTransaction()
      _ = tx.setRefcount(doomed, doomedState.refcount, doomedState.refcount.setCount(0))
      taskStatePtr <- allocator.allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](3))))(using tx)
      _ <- SystemTaskServiceState.enrollInTx(client, statePtr, taskId, taskStatePtr)(using tx)
      _ <- tx.commit()
      _ <- waitForTransactionsToComplete()
      _ = audit.stop()
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield
      enrolled.map(_._1) should contain (taskId)
      val built = audit.built.find(_.description.transactionId == tx.id).get
      built.hasPin shouldBe true
      built.replaySafe shouldBe false
      val enrollUpdate = built.description.requirements.collect:
        case kvu: org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
          if built.kvOps.getOrElse(kvu.objectPointer.id, Nil).exists:
               case i: org.aspen_ddp.aspen.common.objects.Insert => i.key == Key(uuid2byte(taskId))
               case _ => false
        => kvu
      enrollUpdate.size shouldBe 1
      enrollUpdate.head.requiredRevision shouldBe None
