package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.compute.{DurableTaskPointer, ServiceEntry}
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS}

/** Shared test harness for driving FailedStorageDeviceDurableTask instances.
 *
 *  Provides helpers for enrolling tasks and racing futures against timeouts. Factored out so
 *  both FailedStorageDeviceSuite and RebuildEndToEndSuite can drive the producer half without
 *  duplication.
 */
trait FailedStorageDeviceTestHarness:
  this: IntegrationTestSuite =>

  protected val fastPoll = Duration(50, MILLISECONDS)

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

  /** Build a task instance over the state object failStorageDevice enrolled. */
  protected def taskForEnrolled(deviceId: StorageDeviceId): Future[FailedStorageDeviceDurableTask] =
    taskForEnrolled(deviceId, fastPoll)

  protected def taskForEnrolled(deviceId: StorageDeviceId,
                                pollPeriod: Duration): Future[FailedStorageDeviceDurableTask] =
    given ExecutionContext = executionContext
    enrolledTasks().map: enrolled =>
      enrolled.size should be(1)
      new FailedStorageDeviceDurableTask(
        DurableTaskPointer(enrolled.head._2), client, deviceId, pollPeriod)

  /** Race a future against a timeout, failing with an explicit assertion if the timeout wins. */
  protected def withTimeout[T](fut: Future[T], timeout: Duration, what: String): Future[T] =
    given ExecutionContext = executionContext
    val p = Promise[T]()
    client.backgroundTaskManager.schedule(timeout):
      if !p.isCompleted then
        p.failure(new AssertionError(s"$what did not complete within ${timeout.toMillis} ms"))
    fut.onComplete(p.tryComplete)
    p.future
