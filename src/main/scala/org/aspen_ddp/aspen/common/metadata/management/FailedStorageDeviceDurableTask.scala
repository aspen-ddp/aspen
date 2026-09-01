package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.util.{byte2uuid, ignoreExtraCallsWhileRunning, uuid2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor, TaskStopped}
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object FailedStorageDeviceDurableTask extends DurableTaskFactory:

  val typeUUID: UUID = UUID.fromString("7b1f4d62-0c58-4a97-8e35-6a2d91cf4b08")

  /** The failed device. Below DurableTask.ReservedFromKeyId and distinct from
   *  SimpleTaskExecutor.TaskTypeKey, which is where prepareSystemDurableTask writes the type
   *  UUID. */
  private[aspen] val DeviceIdKey: Key = Key(Array[Byte](0))

  /** The set that contained the device, captured at enrollment because step 1 destroys the
   *  device's own record of it. */
  private[aspen] val SetIdKey: Key = Key(Array[Byte](1))

  val DefaultPollPeriod: Duration = Duration(2, MINUTES)

  /** Overridable poll period (test seam; mirrors MigratePoolToSetDurableTask.pollPeriod). */
  @volatile var pollPeriod: Duration = DefaultPollPeriod

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    val deviceId = StorageDeviceId(byte2uuid(state(DeviceIdKey).value.bytes))
    val setId = StorageDeviceSetId(byte2uuid(state(SetIdKey).value.bytes))
    new FailedStorageDeviceDurableTask(pointer, client, deviceId, setId, pollPeriod)

  /** Stage enrollment of a failure task for `deviceId` inside the caller's transaction. */
  def prepareSystemTask(client: AspenClient,
                        deviceId: StorageDeviceId,
                        setId: StorageDeviceSetId)(using tx: Transaction): Future[Unit] =
    client.prepareSystemDurableTask(typeUUID, Map(
      DeviceIdKey -> uuid2byte(deviceId.uuid),
      SetIdKey -> uuid2byte(setId.uuid)))


/** Tombstones a failed storage device and drains its stores onto live devices, one at a time.
 *
 *  Two steps, each a single transaction and each idempotent by re-reading inside that
 *  transaction. Step 1 removes the device from its set and its host and zeroes both ids; step 2
 *  moves one store per pass onto a device selected from that store's pool's set, marked
 *  Rebuilding. The task completes when the tombstone's store map is empty.
 *
 *  Progress needs no record of its own: the tombstone's own store map is the work list, and every
 *  transaction that moves a store also removes it from that map. Crash recovery is therefore
 *  free.
 *
 *  Nothing here reconstructs data. A Rebuilding entry is a message to the StoreManager on the
 *  destination host, which does the actual walk. The two halves never talk and either may crash
 *  and restart independently.
 */
class FailedStorageDeviceDurableTask(
    val taskPointer: DurableTaskPointer,
    client: AspenClient,
    deviceId: StorageDeviceId,
    setId: StorageDeviceSetId,
    pollPeriod: Duration
) extends DurableTask with Logging:

  private given ExecutionContext = client.clientContext

  private val promise = Promise[Option[AnyRef]]()
  def completed: Future[Option[AnyRef]] = promise.future

  private var pollTask: ScheduledTask = NoTask

  /** Single-flight guard: a tick arriving while a pass is still running is dropped.
   *
   *  processNext MUST never return a failed Future, nor throw: ignoreExtraCallsWhileRunning
   *  clears its flag from `Future.foreach`, which does not run on failure, and a synchronous
   *  throw escapes before the flag is ever cleared. Either would wedge the wrapper permanently. */
  private val run: () => Unit = ignoreExtraCallsWhileRunning(processNext())

  // Begin driving immediately.
  run()

  override protected def onStop(): Unit = synchronized:
    pollTask.cancel()
    if !promise.isCompleted then promise.failure(new TaskStopped)

  private def scheduleRecheck(): Unit = synchronized:
    if !isStopped then
      pollTask.cancel()
      pollTask = client.backgroundTaskManager.schedule(pollPeriod):
        run()

  private def finishOk(): Unit = synchronized:
    if !promise.isCompleted then promise.success(None)

  private def processNext(): Future[Unit] =
    if isStopped then
      synchronized { if !promise.isCompleted then promise.failure(new TaskStopped) }
      Future.unit
    else
      // try/catch as well as recover: a synchronous throw out of drive() would leave the
      // single-flight flag set forever.
      val pass =
        try drive()
        catch
          case err: Throwable => Future.failed(err)

      // transformWith to a constant Future.unit rather than recover: a `recover` body that
      // itself threw would fail the returned future and wedge the flag just the same.
      pass.transformWith: outcome =>
        try
          outcome match
            case Failure(err) =>
              logger.warn(s"Failed device ${deviceId.uuid}: transient error, will retry: $err")
              scheduleRecheck()

            case Success(_) => ()
        catch
          case _: Throwable => () // nothing left to report with; the next tick is the recovery

        Future.unit

  /** One pass. Filled in by the tombstone step (Task 8) and the drain step (Task 9). */
  private def drive(): Future[Unit] =
    Future.successful(finishOk())
