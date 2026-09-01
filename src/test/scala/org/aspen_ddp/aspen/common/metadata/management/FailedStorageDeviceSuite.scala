package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{Radicle, TypeFactories}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, fixed_ids}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.compute.{DurableTaskFactory, DurableTaskPointer, ServiceEntry}
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

class FailedStorageDeviceSuite extends IntegrationTestSuite:

  atest("FailedStorageDeviceDurableTask is registered in the client type registry"):
    given ExecutionContext = executionContext
    val factory = client.typeRegistry.getType[DurableTaskFactory](
      FailedStorageDeviceDurableTask.typeUUID)
    factory should be(Some(FailedStorageDeviceDurableTask))
    TypeFactories.factories should contain(FailedStorageDeviceDurableTask)

  /** The task state objects currently enrolled with the system task executor. */
  protected def enrolledTasks(): Future[List[(java.util.UUID, KeyValueObjectPointer)]] =
    given ExecutionContext = executionContext
    val tkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      vs <- tkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(vs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield enrolled

  atest("failStorageDevice enrolls exactly one task"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      enrolled <- enrolledTasks()
    yield
      enrolled.size should be(1)

  atest("failStorageDevice fails with NoSuchElementException for an unknown device"):
    given ExecutionContext = executionContext
    recoverToSucceededIf[NoSuchElementException](
      client.failStorageDevice(StorageDeviceId(java.util.UUID.randomUUID())))

  // ---- Driving the task -----------------------------------------------------------

  private val fastPoll = Duration(50, MILLISECONDS)

  /** Build a task instance over the state object failStorageDevice enrolled. */
  protected def taskForEnrolled(deviceId: StorageDeviceId,
                                setId: StorageDeviceSetId): Future[FailedStorageDeviceDurableTask] =
    given ExecutionContext = executionContext
    enrolledTasks().map: enrolled =>
      enrolled.size should be(1)
      new FailedStorageDeviceDurableTask(
        DurableTaskPointer(enrolled.head._2), client, deviceId, setId, fastPoll)

  /** Poll until `deviceId` reads as tombstoned, or fail on timeout. A bare wait on the task's
   *  completion would hang the suite rather than fail if the loop stopped making progress. */
  protected def awaitTombstone(deviceId: StorageDeviceId, timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      client.getStorageDeviceState(deviceId).flatMap: state =>
        if state.isFailed then
          Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(s"device ${deviceId.uuid} was not tombstoned"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(fastPoll)(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  /** Race a future against a timeout, failing with an explicit assertion if the timeout wins. */
  protected def withTimeout[T](fut: Future[T], timeout: Duration, what: String): Future[T] =
    given ExecutionContext = executionContext
    val p = Promise[T]()
    client.backgroundTaskManager.schedule(timeout):
      if !p.isCompleted then
        p.failure(new AssertionError(s"$what did not complete within ${timeout.toMillis} ms"))
    fut.onComplete(p.tryComplete)
    p.future

  /** Wait without occupying a pool thread. Thread.sleep here would hold a thread the task under
   *  test needs to make the very progress the test is watching for. */
  protected def delay(d: Duration): Future[Unit] =
    val p = Promise[Unit]()
    client.backgroundTaskManager.schedule(d)(p.success(()))
    p.future

  atest("step 1 zeroes both ids and removes the device from its set and host"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ <- taskForEnrolled(net.secondDeviceId, setId)
      _ <- awaitTombstone(net.secondDeviceId, Duration(30000, MILLISECONDS))

      state <- client.getStorageDeviceState(net.secondDeviceId)
      set <- client.getStorageDeviceSetState(setId)
      host <- client.getHostState(HostId.BootstrapHostId)
    yield
      state.hostId should be(fixed_ids.FailedHostId)
      state.storageDeviceId should be(fixed_ids.FailedStorageDeviceId)
      state.isFailed should be(true)
      set.memberDevices should not contain net.secondDeviceId
      host.storageDevices should not contain net.secondDeviceId

  atest("step 1 against an already-tombstoned device changes nothing"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    val timeout = Duration(30000, MILLISECONDS)
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ <- taskForEnrolled(net.secondDeviceId, setId)
      _ <- awaitTombstone(net.secondDeviceId, timeout)
      _ <- waitForTransactionsToComplete()
      before <- client.getStorageDeviceState(net.secondDeviceId)

      // Re-running tombstone() against already-tombstoned state must complete and change nothing.
      task <- taskForEnrolled(net.secondDeviceId, setId)
      completed <- withTimeout(task.tombstone(), timeout, "tombstone() on already-tombstoned device")
      _ <- waitForTransactionsToComplete()
      after <- client.getStorageDeviceState(net.secondDeviceId)
      set <- client.getStorageDeviceSetState(setId)
    yield
      after should be(before)
      set.memberDevices should not contain net.secondDeviceId

  atest("failStorageDevice refuses a device that is already tombstoned"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      _ <- taskForEnrolled(net.secondDeviceId, setId)
      _ <- awaitTombstone(net.secondDeviceId, Duration(30000, MILLISECONDS))
      _ <- waitForTransactionsToComplete()

      // A second enrollment attempt sees the tombstoned device and rejects it.
      result <- recoverToExceptionIf[AspenClient.DeviceAlreadyFailed](
        client.failStorageDevice(net.secondDeviceId))
    yield
      result.getMessage should include("already been declared failed")

  atest("a failing pass does not wedge the task"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    val bogusId = StorageDeviceId(java.util.UUID.randomUUID())
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()

      // Build a task instance over the enrolled state but with a bogus device id, so
      // drive()'s getStorageDeviceState fails.
      task <- taskForEnrolled(bogusId, setId)
      _ <- waitForTransactionsToComplete()

      // Wait several poll periods. The task should survive: neither completed nor failed,
      // still rescheduling.
      _ <- delay(fastPoll * 5)
    yield
      task.completed.isCompleted should be(false)
