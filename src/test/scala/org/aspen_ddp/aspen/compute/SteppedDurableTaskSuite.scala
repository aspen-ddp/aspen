package org.aspen_ddp.aspen.compute

import java.nio.charset.StandardCharsets
import java.util.UUID

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.compute.{TaskExecutor, TaskStopped}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

object TestSteppedTask extends DurableTaskFactory:
  val typeUUID: UUID = UUID.fromString("A1B2C3D4-E5F6-7890-ABCD-EF1234567890")

  private val DataPointerKey = Key(2)

  @volatile private var delayStep: Boolean = false

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =

    val dataPointer = org.aspen_ddp.aspen.common.objects.DataObjectPointer(state(DataPointerKey).value.bytes)
    new TestSteppedTask(pointer, client, dataPointer)

  def prepareTask(executor: TaskExecutor,
                  dataPointer: org.aspen_ddp.aspen.common.objects.DataObjectPointer)
                 (using tx: Transaction): Future[Future[Option[AnyRef]]] =
    val (stepKey, stepValue) = SteppedDurableTask.getInitialContent(
      Map("result" -> "none".getBytes(StandardCharsets.UTF_8))
    )
    val istate = List(
      stepKey -> stepValue.bytes,
      DataPointerKey -> dataPointer.toArray
    )
    executor.prepareTask(TestSteppedTask, istate)

  def setDelayStep(delay: Boolean): Unit =
    delayStep = delay

class TestSteppedTask(
  taskPointer: DurableTaskPointer,
  client: AspenClient,
  dataPointer: org.aspen_ddp.aspen.common.objects.DataObjectPointer
) extends SteppedDurableTask(taskPointer, client):

  override def resultFromState(state: Map[String, Array[Byte]]): Option[AnyRef] =
    Some(new String(state("result"), StandardCharsets.UTF_8))

  def step0(tx: Transaction, state: Map[String, Array[Byte]], stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    val delay = if TestSteppedTask.delayStep then
      Future:
        Thread.sleep(500)
    else
      Future.successful(())

    delay.flatMap: _ =>
      client.read(dataPointer).map: dos =>
        val content = new String(dos.data.getByteArray, StandardCharsets.UTF_8)
        assert(content == "initial", s"Step 0: expected 'initial' but got '$content'")
        tx.overwrite(dataPointer, dos.revision, DataBuffer("step1".getBytes(StandardCharsets.UTF_8)))
        state
          .updated("step0_seen", content.getBytes(StandardCharsets.UTF_8))
          .updated("result", "step0_done".getBytes(StandardCharsets.UTF_8))

  def step1(tx: Transaction, state: Map[String, Array[Byte]], stepRevision: ObjectRevision): Future[Map[String, Array[Byte]]] =
    client.read(dataPointer).map: dos =>
      val content = new String(dos.data.getByteArray, StandardCharsets.UTF_8)
      assert(content == "step1", s"Step 1: expected 'step1' but got '$content'")
      val step0Seen = new String(state("step0_seen"), StandardCharsets.UTF_8)
      assert(step0Seen == "initial", s"Step 1: state 'step0_seen' expected 'initial' but got '$step0Seen'")
      tx.overwrite(dataPointer, dos.revision, DataBuffer("step2".getBytes(StandardCharsets.UTF_8)))
      state.updated("result", "step1_done".getBytes(StandardCharsets.UTF_8))

  val steps: Array[(Transaction, Map[String, Array[Byte]], ObjectRevision) => Future[Map[String, Array[Byte]]]] =
    Array(step0, step1)

class SteppedDurableTaskSuite extends IntegrationTestSuite:

  atest("Two-step task updates data object and state correctly"):
    given ExecutionContext = executionContext

    for
      kvos <- client.read(radicle)
      rootPool <- client.getStoragePool(kvos.pointer.poolId)
      allocator = new PoolObjectAllocator(client, rootPool)

      // Allocate executor root and data object in one transaction
      tx0 = client.newTransaction()
      executorRoot <- allocator.allocateKeyValueObject(Map())(using tx0)
      dataPtr <- allocator.allocateDataObject(
        DataBuffer("initial".getBytes(StandardCharsets.UTF_8)))(using tx0)
      _ = tx0.bumpVersion(radicle, kvos.revision)
      _ <- tx0.commit()

      // Create executor from the allocated root
      executor <- SimpleTaskExecutor(client, allocator, executorRoot)

      // Prepare and run the stepped task
      kvos2 <- client.read(radicle)
      tx1 = client.newTransaction()
      taskFuture <- TestSteppedTask.prepareTask(executor, dataPtr)(using tx1)
      _ = tx1.bumpVersion(radicle, kvos2.revision)
      _ <- tx1.commit()
      result <- taskFuture

      // Verify final data object content
      finalDos <- client.read(dataPtr)
      finalContent = new String(finalDos.data.getByteArray, StandardCharsets.UTF_8)
    yield
      finalContent should be("step2")
      result should be(Some("step1_done"))

  atest("stopping a running stepped task completes with TaskStopped"):
    given ExecutionContext = executionContext

    for
      kvos <- client.read(radicle)
      rootPool <- client.getStoragePool(kvos.pointer.poolId)
      allocator = new PoolObjectAllocator(client, rootPool)

      tx0 = client.newTransaction()
      executorRoot <- allocator.allocateKeyValueObject(Map())(using tx0)
      dataPtr <- allocator.allocateDataObject(
        DataBuffer("initial".getBytes(StandardCharsets.UTF_8)))(using tx0)
      _ = tx0.bumpVersion(radicle, kvos.revision)
      _ <- tx0.commit()

      executor <- SimpleTaskExecutor(client, allocator, executorRoot)

      // Enable delay in step0 to ensure task is running when we call shutdown
      _ = TestSteppedTask.setDelayStep(true)

      kvos2 <- client.read(radicle)
      tx1 = client.newTransaction()
      outerTaskFuture <- TestSteppedTask.prepareTask(executor, dataPtr)(using tx1)
      _ = tx1.bumpVersion(radicle, kvos2.revision)
      _ <- tx1.commit()

      // Schedule shutdown after task starts executing
      shutdownPromise = Promise[Unit]()
      _ = client.backgroundTaskManager.schedule(Duration(250, MILLISECONDS)):
        executor.shutdown()
        shutdownPromise.success(())

      _ <- shutdownPromise.future

      // The task should fail with TaskStopped
      result <- outerTaskFuture.failed
    yield
      TestSteppedTask.setDelayStep(false)  // Reset for other tests
      result shouldBe a[TaskStopped]

class DurableTaskStopSuite extends IntegrationTestSuite:

  atest("stop() sets isStopped and calls onStop()"):
    given ExecutionContext = executionContext

    val onStopCalled = Promise[Unit]()

    val task = new DurableTask:
      val taskPointer: DurableTaskPointer =
        DurableTaskPointer(KeyValueObjectPointer(radicle.toArray))
      val completed: Future[Option[AnyRef]] = Future.successful(None)
      override protected def onStop(): Unit = onStopCalled.trySuccess(())

    task.stop()
    onStopCalled.future.map(_ => succeed)
