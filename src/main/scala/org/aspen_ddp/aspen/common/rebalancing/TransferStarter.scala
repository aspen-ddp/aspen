package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetState}
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

/** Outcome of `TransferStarter.start`. */
enum StartResult:
  /** The admission rule failed on fresh state; nothing was started. */
  case Rejected
  /** The destination lacks room for the store beside what it is already receiving; nothing was
   *  started. Unlike `Rejected` this will not clear on its own soon, so the caller should pick
   *  another destination. */
  case NoRoom
  /** The transfer was started and the pool is still at or above the floor. Also returned when the
   *  pool was found below the floor but the rollback found nothing to undo, because the transfer
   *  had already been aborted or completed by someone else. */
  case Started
  /** The transfer was started, the pool was then found below the floor (a competing initiator
   *  started another transfer in the same pool), and the transfer was aborted again. The caller
   *  must back off before retrying. */
  case RolledBack

/** The single way every initiator (rebalance, migration) starts a store transfer. */
object TransferStarter:

  /** Start moving `storeId` to `destination`, subject to `TransferSafety`.
   *
   *  KNOWN RACE -- READ BEFORE CALLING. The admission check and the start are NOT atomic:
   *  `transferStore` revision-checks only the source and destination device objects, not the other
   *  stores of the pool, so two initiators can both pass `TransferSafety.isSafe` against the same
   *  state and together take the pool below the floor. This method is therefore best-effort at the
   *  instant of the start. It contains the race as follows:
   *
   *    1. Re-read the pool's state immediately before the start and check `isSafe` against it and
   *       the current `offlineHosts()`. Fails -> `Rejected`, nothing started. Then check that the
   *       store fits on the destination beside the stores it is already committed to receive
   *       (`StorageDeviceSetState.availableSpace`). Fails -> `NoRoom`, nothing started. The fit
   *       check races competing starts the same way admission does, but it is not verified
   *       afterwards: an overcommitted destination fails a transfer, it does not lose data.
   *    2. `transferStore`.
   *    3. Re-read the pool's state after it commits. If `TransferSafety.isBelowFloor` (the same
   *       clamped floor as admission, never the raw write threshold), abort the transfer just
   *       started with `abortStoreTransfer` -> `RolledBack`. Otherwise -> `Started`.
   *
   *  The pool is therefore below the floor only for the brief window between a competing start
   *  and its rollback. Callers MUST back off (see `TransferBackoff`) on `RolledBack`; retrying
   *  immediately can keep two initiators rolling each other back.
   *
   *  A failure of `transferStore` or of the rollback's `abortStoreTransfer` is returned as a failed
   *  future; callers decide how to retry. The failure is AMBIGUOUS: it does not say whether the
   *  transfer was started. A failed `transferStore` leaves nothing started, but a failed rollback
   *  leaves the transfer started and the pool possibly below the floor. Callers must not assume
   *  either; they should back off and re-read the transfer's real state (as the rebalance and
   *  migration polls' classify does) before acting again.
   */
  def start(client: AspenClient,
            storeId: StoreId,
            destination: StorageDeviceId): Future[StartResult] =
    start(client, storeId, destination, () => Future.unit)

  /** `start` with a hook run between the admission check and `transferStore`, so tests can
   *  reproduce the race deterministically. */
  private[rebalancing] def start(client: AspenClient,
                                 storeId: StoreId,
                                 destination: StorageDeviceId,
                                 beforeTransfer: () => Future[Unit]): Future[StartResult] =
    given ExecutionContext = client.clientContext

    def readState(): Future[State.PlanningState] =
      State.getStateForPoolMigration(client, storeId.poolId, destination)

    def verify(): Future[StartResult] =
      readState().flatMap: after =>
        if TransferSafety.isBelowFloor(after, client.offlineHosts(), storeId.poolId) then
          client.abortStoreTransfer(storeId).map:
            case true => StartResult.RolledBack
            // Someone else aborted or completed the transfer first, so there is no rollback of
            // ours to back off from. The caller's next classify reads the true state.
            case false => StartResult.Started
        else
          Future.successful(StartResult.Started)

    readState().flatMap: before =>
      val source = before.devices.values.find(_.stores.contains(storeId)).map(_.deviceId)
      val admitted = source.exists: src =>
        TransferSafety.isSafe(before, client.offlineHosts(), Plan.Transfer(storeId, src, destination))
      if !admitted then
        Future.successful(StartResult.Rejected)
      else
        // The store's own size is unknown only if its pool records none, in which case nothing
        // can be checked. Planning makes the same 0-byte assumption.
        val size = before.pools.get(storeId.poolId).flatMap(_.stores.get(storeId)).fold(0L)(_.currentSize)
        for
          dst <- client.getStorageDeviceState(destination)
          free <- StorageDeviceSetState.availableSpace(client, dst, excluding = Set(storeId))
          result <-
            if size > free then
              Future.successful(StartResult.NoRoom)
            else
              for
                _ <- beforeTransfer()
                _ <- client.transferStore(storeId, destination)
                result <- verify()
              yield result
        yield result

/** Per-store exponential backoff for starts that were `RolledBack` (or otherwise failed).
 *
 *  In-memory and pure apart from the injected clock. The tasks that use it are poll-driven, so
 *  `ready` is consulted on each poll rather than scheduling a timer. After the n-th consecutive
 *  `failed`, the store is not ready for `min(base * 2^(n-1), max)`, shortened by a random fraction
 *  of up to `jitter`; `succeeded` forgets it.
 *
 *  @param now current time in milliseconds
 */
class TransferBackoff(now: () => Long,
                      base: FiniteDuration,
                      max: FiniteDuration,
                      rng: Random = new Random(),
                      jitter: Double = 0.0):
  require(jitter >= 0.0 && jitter <= 1.0, s"jitter must be within [0, 1], got $jitter")

  private case class Entry(failures: Int, readyAt: Long)

  private var entries: Map[StoreId, Entry] = Map.empty

  def ready(storeId: StoreId): Boolean = synchronized:
    entries.get(storeId).forall(e => now() >= e.readyAt)

  def failed(storeId: StoreId): Unit = synchronized:
    val failures = entries.get(storeId).map(_.failures).getOrElse(0) + 1
    entries += storeId -> Entry(failures, now() + delayMillis(failures))

  def succeeded(storeId: StoreId): Unit = synchronized:
    entries -= storeId

  private def delayMillis(failures: Int): Long =
    val maxMs = max.toMillis
    // Cap the shift so the doubling cannot overflow; anything that large is clamped to max anyway.
    val shift = math.min(failures - 1, 30)
    val delay = math.min(base.toMillis << shift, maxMs)
    // Shortened by up to `jitter` of itself, never lengthened, so max stays a hard bound. Without
    // it, initiators that collided are ready again at the same instant and collide again.
    delay - (delay * jitter * rng.nextDouble()).toLong
