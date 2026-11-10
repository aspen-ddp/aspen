package org.aspen_ddp.aspen.compute

import java.lang.reflect.{InvocationTargetException, Proxy}
import java.util.UUID

import org.aspen_ddp.aspen.{IntegrationTestSuite, PinAudit}
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, RegisteredTypeFactory, TypeRegistry}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor

import scala.concurrent.{ExecutionContext, Future, Promise}

object SimpleTaskExecutorSuite:
  /** A task that never completes, so its state stays pending in the executor object */
  object PendingTaskFactory extends DurableTaskFactory:
    val typeUUID: UUID = UUID.fromString("5d3e7a10-9b2c-4f68-8a1d-0c4e6b7f2a93")
    val MarkerKey: Key = Key(7)

    def createTask(client: AspenClient,
                   pointer: DurableTaskPointer,
                   revision: ObjectRevision,
                   state: Map[Key, KeyValueObjectState.ValueState],
                   taskExecutor: TaskExecutor): DurableTask =
      new DurableTask:
        val taskPointer: DurableTaskPointer = pointer
        val completed: Future[Option[AnyRef]] = Promise[Option[AnyRef]]().future

  /** A task that completes at once, so the executor deallocates its state object for reuse */
  object CompletingTaskFactory extends DurableTaskFactory:
    val typeUUID: UUID = UUID.fromString("b1c7d0e2-4f3a-4b9e-9d6c-2a8f5e1c7b34")

    def createTask(client: AspenClient,
                   pointer: DurableTaskPointer,
                   revision: ObjectRevision,
                   state: Map[Key, KeyValueObjectState.ValueState],
                   taskExecutor: TaskExecutor): DurableTask =
      new DurableTask:
        val taskPointer: DurableTaskPointer = pointer
        val completed: Future[Option[AnyRef]] = Future.successful(None)

class SimpleTaskExecutorSuite extends IntegrationTestSuite:
  import SimpleTaskExecutorSuite.*

  override def userTypeFactories: List[RegisteredTypeFactory] = List(PendingTaskFactory, CompletingTaskFactory)

  /** The test client with a registry that lacks PendingTaskFactory. A second TClient cannot be built (replies
   *  reach only the TestNetwork's own client), so this forwards everything else to it.
   */
  private def clientWithoutFactory(): AspenClient =
    val registry = TypeRegistry(
      org.aspen_ddp.aspen.common.TypeFactories.factories,
      org.aspen_ddp.aspen.client.TypeFactories.factories,
      org.aspen_ddp.aspen.server.TypeFactories.factories)
    Proxy.newProxyInstance(getClass.getClassLoader, Array(classOf[AspenClient]), (_, method, args) =>
      if method.getName == "typeRegistry" && method.getParameterCount == 0 then
        registry
      else
        try method.invoke(client, (if args == null then Array.empty[AnyRef] else args)*)
        catch case e: InvocationTargetException => throw e.getCause
    ).asInstanceOf[AspenClient]

  atest("a task of an unregistered type is neither resumed nor reused"):
    given ExecutionContext = executionContext
    for
      pool <- client.getStoragePool(radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool)
      kvos <- client.read(radicle)
      tx0 = client.newTransaction()
      executorRoot <- allocator.allocateKeyValueObject(Map())(using tx0)
      _ = tx0.bumpVersion(radicle, kvos.revision)
      _ <- tx0.commit()

      executor <- SimpleTaskExecutor(client, allocator, executorRoot)
      kvos2 <- client.read(radicle)
      tx1 = client.newTransaction()
      _ <- executor.prepareTask(PendingTaskFactory, List(PendingTaskFactory.MarkerKey -> Array[Byte](1, 2, 3)))(using tx1)
      _ = tx1.bumpVersion(radicle, kvos2.revision)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      rootAfterEnroll <- client.read(executorRoot)
      pendingPtr = DurableTaskPointer(org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer(
        rootAfterEnroll.contents.values.head.value.bytes))
      pendingBefore <- client.read(pendingPtr.kvPointer)

      // A restarted process whose registry lacks the type
      executor2 <- SimpleTaskExecutor(clientWithoutFactory(), allocator, executorRoot)
      _ <- client.read(pendingPtr.kvPointer) // lets the constructor's reads settle

      kvos3 <- client.read(radicle)
      tx2 = client.newTransaction()
      _ <- executor2.prepareTask(PendingTaskFactory, List(PendingTaskFactory.MarkerKey -> Array[Byte](9)))(using tx2)
      _ = tx2.bumpVersion(radicle, kvos3.revision)
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()

      rootAfter <- client.read(executorRoot)
      pendingAfter <- client.read(pendingPtr.kvPointer)
    yield
      rootAfter.contents.size should be (2)
      pendingAfter.revision should be (pendingBefore.revision)
      pendingAfter.contents(PendingTaskFactory.MarkerKey).value.bytes.toList should be (List[Byte](1, 2, 3))

  /** Polls until the task object holds nothing, as deallocateTask leaves it, failing after a 30 s deadline */
  private def awaitDeallocated(ptr: KeyValueObjectPointer,
                               until: Long = System.nanoTime() + 30_000_000_000L): Future[Unit] =
    given ExecutionContext = executionContext
    client.read(ptr).flatMap: kvos =>
      if kvos.contents.isEmpty then Future.unit
      else if System.nanoTime() > until then Future.failed(new AssertionError(s"task ${ptr.id} was never deallocated"))
      else
        Future(Thread.sleep(10))(using ExecutionContext.global)
          .flatMap(_ => waitForTransactionsToComplete())
          .flatMap(_ => awaitDeallocated(ptr, until))

  atest("allocating, preparing, deallocating and reusing a task object are each pinned"):
    given ExecutionContext = executionContext
    for
      pool <- client.getStoragePool(radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool)
      tx0 = client.newTransaction()
      kvos <- client.read(radicle)
      executorRoot <- allocator.allocateKeyValueObject(Map())(using tx0)
      _ = tx0.bumpVersion(radicle, kvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()
      executor <- SimpleTaskExecutor(client, allocator, executorRoot)

      audit = new PinAudit(client)
      // Only the task's own writes: prepareTask must pin the transaction by itself
      tx1 = client.newTransaction()
      _ <- executor.prepareTask(CompletingTaskFactory, Nil)(using tx1)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()
      root <- client.read(executorRoot)
      taskPtr = KeyValueObjectPointer(root.contents.values.head.value.bytes)
      _ <- awaitDeallocated(taskPtr)
      deallocated <- client.read(taskPtr)

      tx2 = client.newTransaction()
      _ <- executor.prepareTask(CompletingTaskFactory, Nil)(using tx2)
      _ <- tx2.commit()
      _ <- waitForTransactionsToComplete()
      _ = audit.stop()
      rootAfter <- client.read(executorRoot)
    yield
      audit.unpinned shouldBe empty
      // allocateTask's update of the executor object requires its revision
      audit.touching(executorRoot).exists(_.description.requirements.exists:
        case kvu: KeyValueUpdate => kvu.objectPointer.id == executorRoot.id && kvu.requiredRevision.isDefined
        case _ => false) shouldBe true
      // The reused task object is written at the revision its deallocation left
      audit.built.find(_.description.transactionId == tx2.id).get.description.requirements.exists:
        case kvu: KeyValueUpdate => kvu.objectPointer.id == taskPtr.id && kvu.requiredRevision == Some(deallocated.revision)
        case _ => false
      .shouldBe(true)
      rootAfter.contents.size shouldBe 1
