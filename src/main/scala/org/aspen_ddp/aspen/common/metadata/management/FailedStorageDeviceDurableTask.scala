package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{HostState, StorageDeviceId, StorageDeviceSetId, StorageDeviceSetState, StorageDeviceState, fixed_ids}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
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
    client.getStorageDeviceState(deviceId).flatMap: state =>
      if state.isFailed then
        // Step 1 is already done. The drain (step 2) goes here in the next task.
        Future.successful(finishOk())
      else
        tombstone()

  /** Step 1. One transaction: the device leaves its set, leaves its owning host, and has both
   *  ids zeroed.
   *
   *  Everything is re-read inside the transaction and every removal is self-healing, so a
   *  partially-applied step, a concurrent second invocation, and a plain retry all converge on
   *  the same final state. Re-drives rather than finishing: the drain has work to do.
   *
   *  Widened to private[management] for the idempotency test.
   */
  private[management] def tombstone(): Future[Unit] =
    val done = client.transactUntilSuccessful: tx =>
      for
        devPtr <- client.getStorageDevicePointer(deviceId)
        devKvos <- client.read(devPtr, "fail storage device")
        state = StorageDeviceState(devKvos)
        // Re-read inside the transaction: a concurrent instance may have tombstoned the device
        // since this pass began. Retrying would be permanent, not transient -- the zeroed hostId
        // has no entry in the host tree, so getHostPointer below throws NoSuchElementException on
        // every attempt, transactUntilSuccessful loops forever, and this future never completes.
        // A future that never completes wedges the single-flight flag exactly as a failed one does.
        _ = if state.isFailed then throw StopRetrying(AspenClient.DeviceAlreadyFailed(deviceId))
        setPtr <- client.getStorageDeviceSetPointer(setId)
        setDos <- client.read(setPtr, "fail storage device")
        hostPtr <- client.getHostPointer(state.hostId)
        hostKvos <- client.read(hostPtr, "fail storage device")
      yield
        // Set: drop the device (self-healing if absent).
        val set = StorageDeviceSetState(setDos)
        val newSet = set.copy(memberDevices = set.memberDevices.filter(_ != deviceId))
        tx.overwrite(setPtr, setDos.revision, DataBuffer(newSet.toBytes))

        // Host: drop the device (self-healing if absent).
        val host = HostState(hostKvos)
        val newHost = host.removeStorageDevice(deviceId)
        tx.update(hostPtr, None, None,
          List(KeyRevision(HostState.StateKey, hostKvos.contents(HostState.StateKey).revision)),
          List(Insert(HostState.StateKey, newHost.encode())))

        // Device: zero both ids. Everything else -- the store map above all -- is preserved:
        // it is the drain's work list.
        val tombstoned = state.copy(hostId = fixed_ids.FailedHostId,
                                    storageDeviceId = fixed_ids.FailedStorageDeviceId)
        tx.update(devPtr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey,
            devKvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, tombstoned.encode())))

    done.transformWith:
      case Success(_) =>
        // scheduleRecheck, not run(): run()'s single-flight flag is still held by this pass, so
        // a direct call would be dropped.
        scheduleRecheck()
        Future.unit

      case Failure(_: AspenClient.DeviceAlreadyFailed) =>
        // Benign convergence: a concurrent instance tombstoned the device since this pass began.
        // Re-drive to move forward.
        logger.info(s"Failed device ${deviceId.uuid}: already tombstoned by a concurrent instance")
        scheduleRecheck()
        Future.unit

      case Failure(err) =>
        logger.warn(s"Failed device ${deviceId.uuid}: tombstone transaction failed: $err")
        scheduleRecheck()
        Future.unit
