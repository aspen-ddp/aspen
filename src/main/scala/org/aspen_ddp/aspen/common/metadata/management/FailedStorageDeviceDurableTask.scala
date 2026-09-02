package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.client.{AspenClient, FatalReadError, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostState, StorageDeviceId, StorageDeviceSetState, StorageDeviceState, StoragePoolState, fixed_ids}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectRevision}
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

  /** True when the entry on the failed device is stale bookkeeping rather than ownership of the
   *  store. Rebuilding a store the failed device does not own repoints the pool away from the
   *  live copy and orphans it. Two ways that happens:
   *
   *    - TransferringIn: the entry is the receiving half of a store transfer. The source owns
   *      the store until the transfer completes, which is why transferStore leaves the pool
   *      naming the source, and why rebalancing's ownedStores (common/rebalancing/State.scala)
   *      filters TransferringIn out of its accounting.
   *    - the pool no longer names this device for that index, whatever repointed it.
   *
   *  One function, called from both `drain` (before a destination is selected) and `moveStore`
   *  (inside the transaction, where it is authoritative), so the two cannot drift apart.
   *
   *  Compares against the constructor's `deviceId`, never `tombstoneState.storageDeviceId`,
   *  which step 1 has zeroed.
   */
  private def isDisowned(tombstoneState: StorageDeviceState,
                         poolCfg: StoragePoolState,
                         storeId: StoreId): Boolean =
    tombstoneState.stores.get(storeId).exists(
      _.status == StorageDeviceState.StoreStatus.TransferringIn) ||
    poolCfg.stores(storeId.poolIndex).storageDeviceId != deviceId

  /** The destination reads the rebuild path needs and the disown path does not. Bundled so the
   *  transaction can skip them entirely when there is no destination. */
  private case class Destination(id: StorageDeviceId,
                                 ptr: KeyValueObjectPointer,
                                 kvos: KeyValueObjectState,
                                 state: StorageDeviceState)

  /** The mirror of Destination: the reads the disown path needs and the rebuild path does not.
   *
   *  `id` is carried separately from `state.storageDeviceId` because the two disagree when the
   *  source has itself been tombstoned: that zeroes the copy inside the state, while this one
   *  comes from the tombstone's own entry and stays addressable.
   */
  private case class TransferSource(id: StorageDeviceId,
                                    ptr: KeyValueObjectPointer,
                                    kvos: KeyValueObjectState,
                                    state: StorageDeviceState)

  /** The live device shipping a store to this tombstone, when that is what the entry is.
   *
   *  Only a TransferringIn entry has one -- the other way an entry is disowned is the pool having
   *  been repointed elsewhere, which says nothing about who is transferring what. Reads the id
   *  out of the entry rather than out of the pool: they agree today, but the entry is the record
   *  the source's own TransferringOut entry is paired with, and it is that pairing the restore
   *  checks.
   */
  private def transferSourceId(tombstoneState: StorageDeviceState,
                               storeId: StoreId): Option[StorageDeviceId] =
    tombstoneState.stores.get(storeId)
      .filter(_.status == StorageDeviceState.StoreStatus.TransferringIn)
      .flatMap(_.transferDevice)

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
        // Ownership is decided BEFORE a destination is selected, because a disowned entry is
        // going to be dropped without touching the pool and selection for one can fail outright.
        // Concretely: a TransferringIn whose pool entry names the only other device in the set
        // has no legal candidate, since that device holds the live data and is hard-excluded.
        // nextStore returns the head of a sorted list, so an entry selection cannot place is
        // returned by every subsequent pass and blocks every store ordered after it -- the whole
        // drain stalls and `completed` never fires. Not a wedge (the pass completes and the next
        // poll runs), but permanent all the same.
        //
        // This read is outside the transaction, which is sound in one direction only, and that
        // is the direction that matters. If it says "owned" and the store is disowned by the
        // time the transaction runs, moveStore's own check -- the authoritative one -- catches
        // it. The converse, a store becoming owned by a tombstoned device between the two reads,
        // cannot happen -- but only because every path that could place one refuses:
        // StorageDeviceSetState.moveDevice, AspenClient.createNewStoragePool,
        // AspenClient.transferStore and BaseAspenClient.createStoragePool's stageDeviceUpdate all
        // throw DeviceFailed, and StoreManager.updateStateForTransferredStore -- a transfer whose
        // destination was tombstoned mid-flight, the one path that reaches this state without any
        // operator asking for it -- restores the source instead of repointing the pool. That last
        // one was missing until the round-3 fix, and while it was missing this paragraph was
        // false.
        odestination <-
          if isDisowned(state, poolState, storeId) then
            Future.successful(None)
          else
            for
              set <- client.getStorageDeviceSetState(poolState.storageDeviceSet)
              destinationId <- set.selectDeviceForRebuild(storeId.poolId, storeId.poolIndex, client)
            yield Some(destinationId)
        nudge <- moveStore(storeId, odestination)
      yield nudge

      moved.transformWith:
        case Success(nudge) =>
          // A best-effort wake-up so the host that gained work starts promptly rather than
          // waiting out Main.CheckStorageDevicesPeriod. The poll is the guarantee; this is the
          // optimization. On the rebuild path that host holds the destination; on the disown
          // path it holds the restored transfer source. None only when the pass wrote nothing
          // outside the tombstone itself, so there is nothing to wake.
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
   *  When the failed device turns out not to own the store the entry is simply dropped from the
   *  tombstone's work list. That still yields a nudge if the drop restored an abandoned transfer
   *  source, since that source's host has work to do; None means nothing outside the tombstone
   *  was written.
   *
   *  `odestinationId` is None exactly when `drain` already decided the store is disowned and so
   *  spent no selection on it. */
  private def moveStore(storeId: StoreId,
                        odestinationId: Option[StorageDeviceId]): Future[Option[CheckStorageDevice]] =

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: IndexOutOfBoundsException => throw StopRetrying(e)
      // generateBootstrapConfig's two `require`s raise this. Both are believed unfalsifiable
      // from here, but a permanent error with no route to StopRetrying is an infinite 60s retry
      // loop and a never-completing future. One line to fail loudly instead.
      case e: IllegalArgumentException => throw StopRetrying(e)
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
        poolPtr <- client.getStoragePoolPointer(storeId.poolId)
        poolKvos <- client.read(poolPtr, "failed device drain")
        poolCfg = StoragePoolState(poolKvos)
        _ =
          if !tombstoneState.stores.contains(storeId) then
            // Another pass, or a concurrent fail-storage-device, already moved it.
            throw new StoreAlreadyMoved(storeId)

        // The authoritative ownership decision. drain applied the same predicate to a read taken
        // outside the transaction, to avoid spending a selection on an entry it is only going to
        // drop; this one is under the KeyRevision that commits.
        //
        // A disowned entry is stale bookkeeping, so drop it and touch nothing else. Dropping
        // rather than filtering it out of nextStore is deliberate: the tombstone's store map is
        // simultaneously the work list and the completion condition, so an entry left in place
        // would keep the map non-empty and strand the task forever.
        disowned = isDisowned(tombstoneState, poolCfg, storeId)

        // Read the destination only on the rebuild path. A disowned entry has no destination --
        // drain selected none -- and needs none.
        odst <-
          if disowned then
            Future.successful(None)
          else
            odestinationId match
              case None =>
                // drain saw a disowned entry, this read does not. The transition requires the
                // store becoming owned by a tombstoned device between the two reads, which is
                // unreachable only because every path that could do it refuses -- see the
                // paragraph above isDisowned's call in drain, and note that one of those guards
                // (updateStateForTransferredStore) did not exist before round 3, so this branch
                // was reachable then. It is cheap either way: the future fails, drain logs and
                // reschedules, and the next pass rebuilds normally at the cost of one wasted
                // pass. Fail loudly rather than retry a transaction with nothing to write to.
                throw StopRetrying(new IllegalStateException(
                  s"$storeId became owned by tombstoned device ${deviceId.uuid} mid-pass"))
              case Some(destinationId) =>
                for
                  dstPtr <- client.getStorageDevicePointer(destinationId)
                  dstKvos <- client.read(dstPtr, "failed device drain")
                yield
                  Some(Destination(destinationId, dstPtr, dstKvos, StorageDeviceState(dstKvos)))

        // The mirror of the destination read above. Disowning a TransferringIn entry leaves the
        // live source holding StoreEntry(TransferringOut, Some(this device)) with nothing left to
        // clear it: startStoreTransferOut only runs on a StartStoreTransfer from this device,
        // which is dead, and updateStateForTransferredStore only runs if the transfer completes.
        // The entry would sit there permanently, and permanently is enough to matter -- it holds
        // MigratePoolToSetDurableTask's inFlight true so the pool never leaves InProgress, and
        // SetRebalanceDurableTask's classify at Phase.InFlight so the entry never leaves
        // pendingTransfers.
        osrc <-
          if !disowned then
            Future.successful(None)
          else
            transferSourceId(tombstoneState, storeId) match
              case None => Future.successful(None)
              case Some(srcId) =>
                for
                  srcPtr <- client.getStorageDevicePointer(srcId)
                  srcKvos <- client.read(srcPtr, "failed device drain")
                yield
                  Some(TransferSource(srcId, srcPtr, srcKvos, StorageDeviceState(srcKvos)))

        _ =
          odst.foreach: dst =>
            if dst.state.isFailed then
              // Structurally unreachable once step 1 has removed the tombstone from its set, but
              // the pool state driving selection can be stale.
              throw AspenClient.DeviceFailed(dst.id)

        _ <-
          odst match
            case None => Future.unit
            case Some(dst) =>
              // Pool: repoint now, at the start of the rebuild rather than at its end. Reads of a
              // rebuilding store fail until it is reconstructed, but that is equally true of a
              // store on a dead device, and the pool must stop naming the dead device before
              // anything can route around it. The rebalancer already excludes non-Active stores
              // from movement and from the write-threshold count.
              //
              // ORDERING: this mutation precedes prepRadicleUpdate, which builds poolHosts from
              // poolCfg.stores. prepRadicleUpdate now filters its host list to the hosts its
              // store map references, so the other order no longer trips
              // generateBootstrapConfig's require(storesOnHost.nonEmpty) — but this order is the
              // honest one and keeps the two statements describing the same pool.
              poolCfg.stores(storeId.poolIndex) =
                StoragePoolState.StoreEntry(dst.state.hostId, dst.id)
              BootstrapConfig.prepRadicleUpdate(client, storeId, poolCfg, dst.state.hostId)
      yield
        // Tombstone: loses the store. This is also the progress record -- an empty store map is
        // what completes the task. Staged on both paths: a disowned entry is dropped and nothing
        // else is written.
        val newTombstone = tombstoneState.removeStore(storeId)
        tx.update(devPtr, None, None,
          List(KeyRevision(StorageDeviceState.StateKey,
            devKvos.contents(StorageDeviceState.StateKey).revision)),
          List(Insert(StorageDeviceState.StateKey, newTombstone.encode())))

        odst match
          case None =>
            logger.info(s"Failed device ${deviceId.uuid}: dropping $storeId, which it does not own")

            // Put the abandoned transfer's source back to Active in the same transaction that
            // drops this half of it. Compared against the constructor's deviceId, never
            // tombstoneState.storageDeviceId, which step 1 has zeroed.
            //
            // None when the source's own entry no longer names this device -- the transfer
            // completed and StoreManager.updateStateForTransferredStore restored it first, or a
            // concurrent pass did. Both writes carry a KeyRevision on the source's StateKey, so
            // the loser re-reads and this guard turns the re-read into a no-op.
            osrc.flatMap: src =>
              src.state.restoreAbandonedTransferSource(storeId, deviceId).map: restored =>
                logger.info(s"Failed device ${deviceId.uuid}: restoring $storeId to Active on " +
                            s"transfer source ${restored.storageDeviceId}")
                tx.update(src.ptr, None, None,
                  List(KeyRevision(StorageDeviceState.StateKey,
                    src.kvos.contents(StorageDeviceState.StateKey).revision)),
                  List(Insert(StorageDeviceState.StateKey, restored.encode())))

                // The restore is metadata only. The source's own copy is still offline behind
                // the transfer-out marker startStoreTransferOut wrote, and the pool never
                // stopped naming the source, so until that host reinstates it every message for
                // the slice is dropped. Its poll is the guarantee; this shortens the outage from
                // up to Main.CheckStorageDevicesPeriod to the next event loop pass.
                //
                // The device id is the one the tombstone's own entry named, not
                // src.state.storageDeviceId, which reads as zero if the source has since been
                // tombstoned itself. A zeroed host id there just means the send finds nobody,
                // which is what best-effort is for.
                CheckStorageDevice(src.state.hostId, client.clientId, src.id)

          case Some(dst) =>
            // Destination: gains a Rebuilding entry. This is the entire message to the consumer.
            val newDst = dst.state.setStoreEntry(
              storeId, StorageDeviceState.StoreStatus.Rebuilding, None)
            tx.update(dst.ptr, None, None,
              List(KeyRevision(StorageDeviceState.StateKey,
                dst.kvos.contents(StorageDeviceState.StateKey).revision)),
              List(Insert(StorageDeviceState.StateKey, newDst.encode())))

            // Pool config update with the mutation already applied above.
            tx.update(poolPtr, None, None,
              List(KeyRevision(StoragePoolState.ConfigKey,
                poolKvos.contents(StoragePoolState.ConfigKey).revision)),
              List(Insert(StoragePoolState.ConfigKey, poolCfg.encode())))

            Some(CheckStorageDevice(dst.state.hostId, client.clientId, dst.id))
