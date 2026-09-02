package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.client.{AspenClient, FatalReadError, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostState, StorageDeviceId, StorageDeviceSetState, StorageDeviceState, StoragePoolState, fixed_ids}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.objects.{Insert, Key, ObjectRevision}
import org.aspen_ddp.aspen.common.store.StoreId
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

  val DefaultPollPeriod: Duration = Duration(2, MINUTES)

  /** Overridable poll period (test seam; mirrors MigratePoolToSetDurableTask.pollPeriod). */
  @volatile var pollPeriod: Duration = DefaultPollPeriod

  /** The store this pass chose is no longer on the tombstone -- another pass, or a concurrent
   *  invocation, moved it. Not an error: the pass is abandoned and the next poll picks up
   *  whatever is left. */
  class StoreAlreadyMoved(storeId: StoreId)
      extends Throwable(s"store $storeId is no longer recorded on the failed device")

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    val deviceId = StorageDeviceId(byte2uuid(state(DeviceIdKey).value.bytes))
    new FailedStorageDeviceDurableTask(pointer, client, deviceId, pollPeriod)

  /** Stage enrollment of a failure task for `deviceId` inside the caller's transaction. */
  def prepareSystemTask(client: AspenClient,
                        deviceId: StorageDeviceId)(using tx: Transaction): Future[Unit] =
    client.prepareSystemDurableTask(typeUUID, Map(
      DeviceIdKey -> uuid2byte(deviceId.uuid)))


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
    pollPeriod: Duration
) extends DurableTask with Logging:

  import FailedStorageDeviceDurableTask.StoreAlreadyMoved

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

  /** Test hook: entries into drive(), counted before anything in the pass can fail.
   *
   *  A wedged single-flight flag is indistinguishable from a healthy poll loop by every other
   *  observable this class has -- the promise stays incomplete either way, and the logs are
   *  silent either way. Only the pass count separates them, which is why this seam exists.
   *
   *  Not atomic, and does not need to be: entries into drive() are serialized by the
   *  single-flight wrapper. @volatile is for the reading test thread. */
  @volatile private[management] var testDriveCount: Int = 0

  /** One pass: tombstone the device (step 1) or move one of its stores (step 2). */
  private def drive(): Future[Unit] =
    testDriveCount += 1
    client.getStorageDeviceState(deviceId).flatMap: state =>
      if state.isFailed then
        drain(state)
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
    def onFail(err: Throwable): Future[Unit] = err match
      case e: FatalReadError => throw StopRetrying(e)
      case _ => Future.unit

    val done = client.transactUntilSuccessfulWithRecovery(onFail): tx =>
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
        // The set comes from the device's own record, read fresh here rather than captured at
        // enrollment: step 1 preserves storageDeviceSet (it zeroes only the two ids), and an
        // operator may have run move-device-to-set between enrollment and this pass. A captured
        // set id would filter the device out of a set it had already left and leave it in its
        // new set's memberDevices with both ids zeroed, permanently.
        setPtr <- client.getStorageDeviceSetPointer(state.storageDeviceSet)
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

  /** The tombstone's own store map is the work list. Sorted so passes are deterministic and a
   *  resumed task picks the same next store a crashed one would have. */
  private def nextStore(state: StorageDeviceState): Option[StoreId] =
    state.stores.keys.toList.sortBy(sid => (sid.poolId.uuid, sid.poolIndex)).headOption

  /** Step 2. One store per pass.
   *
   *  Selection happens BEFORE the pool is repointed, and against the pool's own set rather than
   *  the failed device's former set. The former is required: selectDeviceForRebuild derives the
   *  failed device from `poolState.stores(poolIndex)`, so a pool already repointed would
   *  hard-exclude the wrong device. The latter means a store lost during a pool migration is
   *  rebuilt directly into the migration's target rather than into the set the pool is leaving.
   */
  private def drain(state: StorageDeviceState): Future[Unit] = nextStore(state) match
    case None =>
      Future.successful(finishOk())

    case Some(storeId) =>
      val moved = for
        poolState <- client.getStoragePoolState(storeId.poolId)
        set <- client.getStorageDeviceSetState(poolState.storageDeviceSet)
        destinationId <- set.selectDeviceForRebuild(storeId.poolId, storeId.poolIndex, client)
        nudge <- moveStore(storeId, destinationId)
      yield nudge

      moved.transformWith:
        case Success(nudge) =>
          // A best-effort wake-up so the destination host starts promptly rather than waiting
          // out Main.CheckStorageDevicesPeriod. The poll is the guarantee; this is the
          // optimization. None when the pass merely disowned an entry: nothing was written to
          // any destination, so there is nothing to wake.
          nudge.foreach(client.sendBestEffortHostMessage)
          scheduleRecheck()
          Future.unit

        case Failure(err) =>
          // AllocationError (no device with room), a lost transaction race, a read failure: all
          // transient from this task's point of view. The store stays on the tombstone and the
          // next poll retries, so capacity appearing later is enough to recover.
          logger.warn(s"Failed device ${deviceId.uuid}: could not place $storeId: $err")
          scheduleRecheck()
          Future.unit

  /** The one transaction that moves a single store off the tombstone. Returns the nudge to send
   *  once it has committed -- built inside so a retried attempt cannot double-send, and sent by
   *  the caller rather than from tx.result so the returned Future actually waits for it.
   *
   *  Returns None when the failed device turns out not to own the store: the entry is dropped
   *  from the tombstone's work list and nothing else is touched, so there is no destination to
   *  nudge. */
  private def moveStore(storeId: StoreId,
                        destinationId: StorageDeviceId): Future[Option[CheckStorageDevice]] =

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: IndexOutOfBoundsException => throw StopRetrying(e)
      case e: AspenClient.DeviceFailed => throw StopRetrying(e)
      case e: StoreAlreadyMoved => throw StopRetrying(e)
      case e: FatalReadError => throw StopRetrying(e)
      case _ => Future.unit

    client.transactUntilSuccessfulWithRecovery(onFail): tx =>
      given Transaction = tx

      for
        devPtr <- client.getStorageDevicePointer(deviceId)
        devKvos <- client.read(devPtr, "failed device drain")
        tombstoneState = StorageDeviceState(devKvos)
        dstPtr <- client.getStorageDevicePointer(destinationId)
        dstKvos <- client.read(dstPtr, "failed device drain")
        dstState = StorageDeviceState(dstKvos)
        poolPtr <- client.getStoragePoolPointer(storeId.poolId)
        poolKvos <- client.read(poolPtr, "failed device drain")
        poolCfg = StoragePoolState(poolKvos)
        _ =
          if !tombstoneState.stores.contains(storeId) then
            // Another pass, or a concurrent fail-storage-device, already moved it.
            throw new StoreAlreadyMoved(storeId)

        // An entry on the failed device does not by itself mean the failed device owns the
        // store, and rebuilding a store it does not own repoints the pool away from the live
        // copy and orphans it. Two ways that happens:
        //
        //   - TransferringIn: the entry is the receiving half of a store transfer. The source
        //     owns the store until the transfer completes, which is why transferStore leaves the
        //     pool naming the source, and why rebalancing's ownedStores (common/rebalancing/
        //     State.scala) filters TransferringIn out of its accounting.
        //   - the pool no longer names this device for that index, whatever repointed it.
        //
        // Either way the entry is stale bookkeeping, so drop it and touch nothing else. Dropping
        // rather than filtering it out of nextStore is deliberate: the tombstone's store map is
        // simultaneously the work list and the completion condition, so an entry left in place
        // would keep the map non-empty and strand the task forever.
        disowned =
          tombstoneState.stores.get(storeId).exists(
            _.status == StorageDeviceState.StoreStatus.TransferringIn) ||
          poolCfg.stores(storeId.poolIndex).storageDeviceId != deviceId

        _ =
          if !disowned && dstState.isFailed then
            // Structurally unreachable once step 1 has removed the tombstone from its set, but
            // the pool state driving selection can be stale.
            throw AspenClient.DeviceFailed(destinationId)

        _ <-
          if disowned then
            Future.unit
          else
            // Pool: repoint now, at the start of the rebuild rather than at its end. Reads of a
            // rebuilding store fail until it is reconstructed, but that is equally true of a
            // store on a dead device, and the pool must stop naming the dead device before
            // anything can route around it. The rebalancer already excludes non-Active stores
            // from movement and from the write-threshold count.
            //
            // CRITICAL ORDERING: this mutation must precede prepRadicleUpdate. prepRadicleUpdate
            // builds poolHosts from poolCfg.stores, so if the mutation happens after (in the
            // yield block), it reads stale state: the failed device's old host lands in
            // hostsList while storeMap remaps its only bootstrap store away, and
            // generateBootstrapConfig's require(storesOnHost.nonEmpty) throws
            // IllegalArgumentException — permanent, infinite loop unless StopRetrying catches
            // it. Invisible in single-host TestNetwork; no test protects this ordering.
            poolCfg.stores(storeId.poolIndex) =
              StoragePoolState.StoreEntry(dstState.hostId, destinationId)
            BootstrapConfig.prepRadicleUpdate(client, storeId, poolCfg, dstState.hostId)
      yield
        // Tombstone: loses the store. This is also the progress record -- an empty store map is
        // what completes the task. Staged on both paths: a disowned entry is dropped and nothing
        // else is written.
        val newTombstone = tombstoneState.removeStore(storeId)
        tx.update(devPtr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey,
            devKvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, newTombstone.encode())))

        if disowned then
          logger.info(s"Failed device ${deviceId.uuid}: dropping $storeId, which it does not own")
          None
        else
          // Destination: gains a Rebuilding entry. This is the entire message to the consumer.
          val newDst = dstState.setStoreEntry(
            storeId, StorageDeviceState.StoreStatus.Rebuilding, None)
          tx.update(dstPtr, None, None,
            List(KeyRevision(StorageDeviceState.StateKey,
              dstKvos.contents(StorageDeviceState.StateKey).revision)),
            List(Insert(StorageDeviceState.StateKey, newDst.encode())))

          // Pool config update with the mutation already applied above.
          tx.update(poolPtr, None, None,
            List(KeyRevision(StoragePoolState.ConfigKey,
              poolKvos.contents(StoragePoolState.ConfigKey).revision)),
            List(Insert(StoragePoolState.ConfigKey, poolCfg.encode())))

          Some(CheckStorageDevice(dstState.hostId, client.clientId, destinationId))
