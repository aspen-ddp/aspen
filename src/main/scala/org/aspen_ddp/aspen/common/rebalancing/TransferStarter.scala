package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

/** Outcome of `TransferStarter.start`. */
enum StartResult:
  /** The admission rule failed on fresh state; nothing was started. */
  case Rejected
  /** The transfer was started and the pool is still at or above the floor. */
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
   *       the current `offlineHosts()`. Fails -> `Rejected`, nothing started.
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
   *  future; callers decide how to retry.
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
          client.abortStoreTransfer(storeId).map(_ => StartResult.RolledBack)
        else
          Future.successful(StartResult.Started)

    readState().flatMap: before =>
      val source = before.devices.values.find(_.stores.contains(storeId)).map(_.deviceId)
      val admitted = source.exists: src =>
        TransferSafety.isSafe(before, client.offlineHosts(), Plan.Transfer(storeId, src, destination))
      if !admitted then
        Future.successful(StartResult.Rejected)
      else
        for
          _ <- beforeTransfer()
          _ <- client.transferStore(storeId, destination)
          result <- verify()
        yield result

/** Per-store exponential backoff for starts that were `RolledBack` (or otherwise failed).
 *
 *  In-memory and pure apart from the injected clock. The tasks that use it are poll-driven, so
 *  `ready` is consulted on each poll rather than scheduling a timer. After the n-th consecutive
 *  `failed`, the store is not ready for `min(base * 2^(n-1), max)`; `succeeded` forgets it.
 *
 *  @param now current time in milliseconds
 */
class TransferBackoff(now: () => Long, base: Duration, max: Duration):

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
    math.min(base.toMillis << shift, maxMs)
