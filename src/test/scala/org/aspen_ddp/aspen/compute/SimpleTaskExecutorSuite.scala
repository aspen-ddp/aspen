package org.aspen_ddp.aspen.compute

import java.lang.reflect.{InvocationTargetException, Proxy}
import java.util.UUID

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, RegisteredTypeFactory, TypeRegistry}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}
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

class SimpleTaskExecutorSuite extends IntegrationTestSuite:
  import SimpleTaskExecutorSuite.*

  override def userTypeFactories: List[RegisteredTypeFactory] = List(PendingTaskFactory)

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
