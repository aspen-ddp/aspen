package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.rebalancing.{StartResult, State, TransferBackoff, TransferStarter}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.util.{boundedSingleFlight, byte2long, byte2uuid, runBoundedParallel, uuid2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor, TaskStopped}
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}

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

  /** Backoff for starts that were RolledBack or failed (see TransferBackoff). Same values as
   *  SetRebalanceDurableTask. */
  val StartBackoffBase: FiniteDuration = Duration(1, SECONDS)
  val StartBackoffMax: FiniteDuration = Duration(2, MINUTES)
  val StartBackoffJitter: Double = 0.25

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


/** Drives one pool's migration onto its target StorageDeviceSet. Several of the pool's store
 *  transfers may be in flight at once, as many as `TransferSafety` admits; each pass tries every
 *  store that is neither migrated nor already transferring, and anything not admitted now is
 *  revisited on the next poll.
 *
 *  Progress needs no record of its own. Host rewrites `poolState.stores(poolIndex)` to
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
    pollPeriod: Duration,
    rng: Random = new Random()  // Destination-selection randomness; a test seam.
) extends DurableTask with Logging:

  import MigratePoolToSetDurableTask.MigrationSuperseded

  private given ExecutionContext = client.clientContext

  private val promise = Promise[Option[AnyRef]]()
  def completed: Future[Option[AnyRef]] = promise.future

  private var pollTask: ScheduledTask = NoTask

  // In memory only, which is fine for a crash-only task: a restarted instance merely retries a
  // little sooner.
  private val backoff = new TransferBackoff(() => System.currentTimeMillis(),
    MigratePoolToSetDurableTask.StartBackoffBase, MigratePoolToSetDurableTask.StartBackoffMax, new Random(), MigratePoolToSetDurableTask.StartBackoffJitter)

  private val passes = new java.util.concurrent.atomic.AtomicInteger(0)

  private def countedPass(): Future[Unit] =
    processNext().andThen(_ => passes.incrementAndGet())

  /** Testing hook: how many polling passes have finished, for waiting on "N more passes ran"
   *  instead of sleeping. */
  private[management] def testingOnlyCompletedPasses: Int = passes.get

  /** Testing hook: whether the store's start backoff has elapsed. */
  private[management] def testingOnlyBackoffReady(storeId: StoreId): Boolean = backoff.ready(storeId)

  /** Single-flight guard: a tick arriving while a pass is still running is dropped
   *  (TODO.txt's "protect against pileups", on the async-body side; the cancel-then-schedule
   *  in scheduleRecheck covers the timer side).
   *
   *  A failed Future or a synchronous throw out of processNext releases the guard rather than
   *  wedging it, so neither is a correctness hazard here any more -- but both still cost a poll
   *  period of progress, so processNext should continue to handle its own errors. The remaining
   *  wedge mode is a Future that never completes: the guard reports it (see the pool id in the
   *  name) but cannot break it, because it cannot distinguish it from a slow pass. */
  private val guardedRun: () => Unit =
    boundedSingleFlight(s"migrate-pool-task ${poolId.uuid}")(countedPass())

  /** Every invocation, including one the guard drops, arms the next tick first. Arming at the
   *  end of a pass instead loses the wakeup whenever a tick fires between that arming and the
   *  guard's release: the tick is dropped and nothing is left to re-arm, so the task stalls
   *  for good. */
  private def run(): Unit =
    scheduleRecheck()
    guardedRun()

  // Begin driving immediately.
  run()

  override protected def onStop(): Unit = synchronized:
    pollTask.cancel()
    if !promise.isCompleted then promise.failure(new TaskStopped)

  private def scheduleRecheck(): Unit = synchronized:
    if !isStopped && !promise.isCompleted then
      pollTask.cancel()
      pollTask = client.backgroundTaskManager.schedule(pollPeriod):
        run()

  private def finishOk(): Unit = synchronized:
    if !promise.isCompleted then
      pollTask.cancel()
      promise.success(None)

  private def processNext(): Future[Unit] =
    if isStopped then
      synchronized { if !promise.isCompleted then promise.failure(new TaskStopped) }
      Future.unit
    else
      // try/catch as well as recover so a synchronous throw out of driveMigration reaches the
      // same handler as an async one. The single-flight guard releases either way.
      val pass =
        try driveMigration()
        catch
          case err: Throwable => Future.failed(err)

      // transformWith to a constant Future.unit rather than recover: a `recover` body that
      // itself threw (logger.warn) would fail the
      // returned future, and a failed future here is reported as a task failure by the guard
      // even though the pass was handled. Nothing below can throw or fail.
      pass.transformWith: outcome =>
        try
          outcome match
            case Failure(err) =>
              logger.warn(s"Pool migration ${poolId.uuid}: transient error, will retry: $err")

            case Success(_) => ()
        catch
          case _: Throwable => () // nothing left to report with; the next tick is the recovery

        Future.unit

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
          // Stores whose transfer is already in flight are waited for, not restarted. The pool's
          // device of record stays the source until the transfer completes, so TransferringOut
          // there is the signal; TransferringIn is checked too in case a device serves as both.
          val inFlight: Set[StoreId] = deviceStates.iterator.flatMap: ds =>
            ds.stores.collect:
              case (sid, entry) if sid.poolId == poolId &&
                  (entry.status == StorageDeviceState.StoreStatus.TransferringIn ||
                   entry.status == StorageDeviceState.StoreStatus.TransferringOut) => sid
          .toSet

          // Destinations of this pool's in-flight transfers: the TransferringOut entry on the
          // device of record names it. They are soft-excluded from selection along with the
          // devices of record, so concurrent transfers do not pile replicas onto one device.
          val inFlightDestinations: Set[StorageDeviceId] = deviceStates.iterator.flatMap: ds =>
            ds.stores.collect:
              case (sid, StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.TransferringOut,
                                                       Some(dst), _)) if sid.poolId == poolId => dst
          .toSet

          val candidates = remaining.filter: i =>
            val sid = StoreId(poolId, i)
            !inFlight.contains(sid) && backoff.ready(sid)

          tryStores(poolKvos, poolState, targetSet, inFlightDestinations, candidates)

  /** Try to start a transfer for every candidate store, in poolIndex order. Sequential, not
   *  parallel: each start re-reads the pool's state, so it sees the transfers this pass has
   *  already started. Never fails. */
  private def tryStores(poolKvos: KeyValueObjectState,
                        poolState: StoragePoolState,
                        targetSet: StorageDeviceSetState,
                        inFlightDestinations: Set[StorageDeviceId],
                        candidates: List[Byte]): Future[Unit] =

    val poolDevices = poolState.stores.map(_.storageDeviceId).toSet

    // `exclude` grows with each destination started in this pass. It is a SOFT exclude: when no
    // other device has room, selection still falls back to one of these.
    def attempt(candidates: List[Byte], exclude: Set[StorageDeviceId]): Future[Unit] = candidates match
      case Nil =>
        Future.unit

      case poolIndex :: tail =>
        val storeId = StoreId(poolId, poolIndex)
        val sourceDevice = poolState.stores(poolIndex).storageDeviceId
        val requiredSize = poolKvos.contents
          .get(StoragePoolState.getStoreUsageKey(poolIndex))
          .map(vs => byte2long(vs.value.bytes))
          .getOrElse(0L)

        // The destination is selected afresh on every attempt, so a store whose transfer was
        // aborted is simply restarted toward whatever the target set offers now.
        val selected =
          try targetSet.selectDeviceForStore(requiredSize, exclude, client, rng)
          catch
            case err: Throwable => Future.failed(err)

        selected.transformWith:
          case Failure(err) =>
            // AllocationError (no device with room) or a read failure: transient from this
            // task's point of view. Try the next store.
            logger.warn(s"Pool migration ${poolId.uuid}: no destination for $storeId: $err")
            backoff.failed(storeId)
            attempt(tail, exclude)

          case Success(destination) if destination == sourceDevice =>
            // Structurally unreachable -- selection only returns target-set devices and this
            // store's device is not one -- but the pool state driving that reasoning can be
            // stale, and transferStore would throw InvalidDestination. Skip instead.
            attempt(tail, exclude)

          case Success(destination) =>
            // KNOWN RACE -- the admission check inside TransferStarter.start and the start itself
            // are not atomic, so a competing initiator (a set rebalance, another migration pass)
            // can pass the same check and together take the pool below the floor. TransferStarter
            // detects that after the start commits and rolls this transfer back (RolledBack),
            // which is why RolledBack backs off here. See the TransferStarter and TransferSafety
            // scaladoc.
            TransferStarter.start(client, storeId, destination).transformWith:
              case Success(StartResult.Started) =>
                backoff.succeeded(storeId)
                attempt(tail, exclude + destination)

              case Success(StartResult.RolledBack) =>
                logger.info(s"Pool migration ${poolId.uuid}: start of $storeId was rolled back; " +
                  "backing off")
                backoff.failed(storeId)
                attempt(tail, exclude)

              case Success(StartResult.Rejected) =>
                // Not admissible right now; the next poll re-evaluates.
                attempt(tail, exclude)

              case Failure(err) =>
                // Ambiguous: transferStore failed (a lost race, a read failure), or the
                // rollback's abort failed and left the transfer started. Either way the next
                // poll sees the true state (a started transfer is skipped as in flight), so
                // back off and try the next store.
                logger.warn(s"Pool migration ${poolId.uuid}: could not start $storeId: $err")
                backoff.failed(storeId)
                attempt(tail, exclude)

    attempt(candidates, poolDevices ++ inFlightDestinations)

  /** Record the migration as Complete, then finish.
   *
   *  Re-reads inside the transaction and writes only if the migration we acted on is still the
   *  one recorded, which makes this idempotent and keeps it from clobbering a retarget that
   *  landed between passes. Throwing rather than staging nothing also avoids committing an
   *  empty transaction.
   */
  private def markComplete(poolPtr: KeyValueObjectPointer,
                           migration: StoragePoolState.Migration): Future[Unit] =

    val done = client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
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
        // is more work; the poll armed by run() picks it up.
        Future.unit

      case Failure(err) =>
        logger.warn(s"Pool migration ${poolId.uuid}: completion transaction failed: $err")
        Future.unit
