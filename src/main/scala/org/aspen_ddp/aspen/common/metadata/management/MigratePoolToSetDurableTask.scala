package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.rebalancing.{Plan, State, TransferSafety}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.util.{byte2long, byte2uuid, ignoreExtraCallsWhileRunning, runBoundedParallel, uuid2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor, TaskStopped}
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object MigratePoolToSetDurableTask extends DurableTaskFactory:

  val typeUUID: UUID = UUID.fromString("3c6e9a14-8b7d-4f02-a5e1-9d0b2c48f736")

  /** The task's only state: the pool being migrated. Below DurableTask.ReservedFromKeyId and
   *  distinct from SimpleTaskExecutor.TaskTypeKey, which is where prepareSystemDurableTask
   *  writes the type UUID. The target set is not stored here -- it is re-read from pool state
   *  every pass, which is what makes retargeting free. */
  private[aspen] val PoolIdKey: Key = Key(Array[Byte](0))

  val DefaultPollPeriod: Duration = Duration(2, MINUTES)

  /** Overridable poll period (test seam; mirrors RebalancingDurableService.pollPeriod). */
  @volatile var pollPeriod: Duration = DefaultPollPeriod

  /** The recorded migration changed under us (retargeted, or already completed by another
   *  instance). Not an error: the completion write is simply skipped. */
  class MigrationSuperseded(poolId: PoolId)
      extends Throwable(s"migration of pool ${poolId.uuid} was retargeted or already completed")

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    val poolId = PoolId(byte2uuid(state(PoolIdKey).value.bytes))
    new MigratePoolToSetDurableTask(pointer, client, poolId, pollPeriod)

  /** Stage enrollment of a migration task for `poolId` inside the caller's transaction. */
  def prepareSystemTask(client: AspenClient, poolId: PoolId)(using tx: Transaction): Future[Unit] =
    client.prepareSystemDurableTask(typeUUID, Map(PoolIdKey -> uuid2byte(poolId.uuid)))


/** Drives one pool's migration onto its target StorageDeviceSet, serially: one store transfer
 *  in flight at a time.
 *
 *  Progress needs no record of its own. StoreManager rewrites `poolState.stores(poolIndex)` to
 *  the new (host, device) in the same transaction that completes a transfer, so a store is
 *  migrated iff its device is a member of the target set. Crash recovery is therefore free: a
 *  fresh instance re-reads the pool and picks up exactly where the last one left off.
 *
 *  The task holds only the pool id; the target set is re-read every pass, so a retarget is
 *  picked up on the next tick with no task-side change at all.
 */
class MigratePoolToSetDurableTask(
    val taskPointer: DurableTaskPointer,
    client: AspenClient,
    poolId: PoolId,
    pollPeriod: Duration
) extends DurableTask with Logging:

  import MigratePoolToSetDurableTask.MigrationSuperseded

  private given ExecutionContext = client.clientContext

  private val promise = Promise[Option[AnyRef]]()
  def completed: Future[Option[AnyRef]] = promise.future

  private var pollTask: ScheduledTask = NoTask

  /** Single-flight guard: a tick arriving while a pass is still running is dropped
   *  (TODO.txt's "protect against pileups", on the async-body side; the cancel-then-schedule
   *  in scheduleRecheck covers the timer side).
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
      // try/catch as well as recover: see the run() comment. A synchronous throw out of
      // driveMigration would leave the single-flight flag set forever.
      val pass =
        try driveMigration()
        catch
          case err: Throwable => Future.failed(err)

      pass.recover:
        case err =>
          logger.warn(s"Pool migration ${poolId.uuid}: transient error, will retry: $err")
          scheduleRecheck()

  private def driveMigration(): Future[Unit] =
    for
      poolPtr <- client.getStoragePoolPointer(poolId)
      poolKvos <- client.read(poolPtr, "pool migration")
      poolState = StoragePoolState(poolKvos)
      result <- poolState.migration match
        // Nothing to do, and a re-dispatched task after completion is a no-op.
        case None =>
          Future.successful(finishOk())

        case Some(m) if m.status == StoragePoolState.MigrationStatus.Complete =>
          Future.successful(finishOk())

        case Some(m) =>
          for
            targetSet <- client.getStorageDeviceSetState(m.targetSet)
            members <- targetSet.collectMemberDevices(client)
            r <- step(poolPtr, poolKvos, poolState, m, targetSet, members)
          yield r
    yield result

  private def step(poolPtr: KeyValueObjectPointer,
                   poolKvos: KeyValueObjectState,
                   poolState: StoragePoolState,
                   migration: StoragePoolState.Migration,
                   targetSet: StorageDeviceSetState,
                   targetMembers: Set[StorageDeviceId]): Future[Unit] =

    val remaining: List[Byte] = poolState.stores.indices
      .filterNot(i => targetMembers.contains(poolState.stores(i).storageDeviceId))
      .map(_.toByte)
      .toList

    if remaining.isEmpty then
      markComplete(poolPtr, migration)
    else
      val storeDevices = poolState.stores.map(_.storageDeviceId).distinct.toSeq
      runBoundedParallel(storeDevices, State.DefaultMaxConcurrentReads)(client.getStorageDeviceState)
        .flatMap: deviceStates =>
          // One transfer at a time. The pool's device of record stays the source until the
          // transfer completes, so TransferringOut there is the signal; TransferringIn is
          // checked too in case a device serves as both.
          val inFlight = deviceStates.exists: ds =>
            ds.stores.exists: (sid, entry) =>
              sid.poolId == poolId &&
                (entry.status == StorageDeviceState.StoreStatus.TransferringIn ||
                 entry.status == StorageDeviceState.StoreStatus.TransferringOut)

          if inFlight then
            scheduleRecheck()
            Future.unit
          else
            tryStores(poolKvos, poolState, targetSet, remaining)

  /** Try each remaining store in poolIndex order until one transfer starts. */
  private def tryStores(poolKvos: KeyValueObjectState,
                        poolState: StoragePoolState,
                        targetSet: StorageDeviceSetState,
                        remaining: List[Byte]): Future[Unit] =

    val poolDevices = poolState.stores.map(_.storageDeviceId).toSet

    def attempt(candidates: List[Byte]): Future[Unit] = candidates match
      case Nil =>
        scheduleRecheck()
        Future.unit

      case poolIndex :: tail =>
        val storeId = StoreId(poolId, poolIndex)
        val sourceDevice = poolState.stores(poolIndex).storageDeviceId
        val requiredSize = poolKvos.contents
          .get(StoragePoolState.getStoreUsageKey(poolIndex))
          .map(vs => byte2long(vs.value.bytes))
          .getOrElse(0L)

        val started = for
          destination <- targetSet.selectDeviceForStore(requiredSize, poolDevices, client)
          ok <-
            if destination == sourceDevice then
              // Structurally unreachable -- selection only returns target-set devices and this
              // store's device is not one -- but the pool state driving that reasoning can be
              // stale, and transferStore would throw InvalidDestination. Skip instead.
              Future.successful(false)
            else
              State.getStateForPoolMigration(client, poolId, destination).flatMap: planning =>
                if TransferSafety.isSafe(planning, client.offlineHosts(),
                     Plan.Transfer(storeId, sourceDevice, destination)) then
                  client.transferStore(storeId, destination).map(_ => true)
                else
                  Future.successful(false)
        yield ok

        started.transformWith:
          case Success(true) =>
            scheduleRecheck()
            Future.unit

          case Success(false) =>
            attempt(tail)

          case Failure(err) =>
            // AllocationError (no device with room), a lost transferStore race, a read failure:
            // all transient from this task's point of view. Try the next store.
            logger.warn(s"Pool migration ${poolId.uuid}: could not start $storeId: $err")
            attempt(tail)

    attempt(remaining)

  /** Record the migration as Complete, then finish.
   *
   *  Re-reads inside the transaction and writes only if the migration we acted on is still the
   *  one recorded, which makes this idempotent and keeps it from clobbering a retarget that
   *  landed between passes. Throwing rather than staging nothing also avoids committing an
   *  empty transaction.
   */
  private def markComplete(poolPtr: KeyValueObjectPointer,
                           migration: StoragePoolState.Migration): Future[Unit] =

    def onFail(err: Throwable): Future[Unit] = err match
      case e: MigrationSuperseded => throw StopRetrying(e)

    val done = client.transactUntilSuccessfulWithRecovery(onFail): tx =>
      client.read(poolPtr, "pool migration completion").map: kvos =>
        val current = StoragePoolState(kvos)
        if !current.migration.contains(migration) then
          throw new MigrationSuperseded(poolId)

        val updated = current.copy(migration =
          Some(migration.copy(status = StoragePoolState.MigrationStatus.Complete)))
        val reqs = List(KeyRevision(StoragePoolState.ConfigKey,
          kvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, updated.encode()))
        tx.update(poolPtr, None, None, reqs, ops)

    done.transformWith:
      case Success(_) =>
        finishOk()
        Future.unit

      case Failure(_: MigrationSuperseded) =>
        // Retargeted or already complete. Re-drive rather than finish -- a retarget means there
        // is more work. scheduleRecheck, not run(), because run()'s single-flight flag is still
        // held by this very pass and the call would be dropped.
        scheduleRecheck()
        Future.unit

      case Failure(err) =>
        logger.warn(s"Pool migration ${poolId.uuid}: completion transaction failed: $err")
        scheduleRecheck()
        Future.unit
