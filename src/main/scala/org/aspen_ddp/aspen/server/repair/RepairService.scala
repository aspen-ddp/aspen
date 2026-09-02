package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, runBoundedParallel}
import scribe.Logging

import scala.concurrent.duration.{Duration, FiniteDuration, HOURS, SECONDS}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/** Drives continual repair of every store a host owns.
  *
  * A single host-wide tick decides which stores are due, then scans the due set through a
  * bounded-parallelism gate. Each store paces itself: quiet stores back off toward the cap,
  * stores with entries snap back to the floor. That keeps the steady-state cost of a host with
  * thousands of quiet stores proportional to the cap rather than to the store count, while a
  * store that is actually behind is revisited promptly.
  *
  * @param clock monotonic milliseconds; injectable so pacing is testable without sleeping
  */
class RepairService(client: AspenClient,
                    hostId: HostId,
                    target: RepairTarget,
                    backgroundTasks: BackgroundTaskManager,
                    clock: () => Long = () => System.nanoTime() / 1000000L)
                   (using ec: ExecutionContext) extends Logging:

  import RepairService.*

  private val repairer = StoreRepairer(client, target)
  private val rand = Random()

  private var scanStates: Map[StoreId, ScanState] = Map()
  private var policyCache: Map[PoolId, RepairPolicy] = Map()
  private var limits: HostRepairLimits = HostRepairLimits.Default

  private val sweepTask = backgroundTasks.scheduleNonConcurrentPollingTask(
    "repair-sweep", TickPeriod, callNow = false, stallAfter = StallAfter)(sweep())

  def cancel(): Unit = sweepTask.cancel()

  /** Overridable so tests can count concurrency without doing I/O. */
  protected def scanStore(storeId: StoreId, policy: RepairPolicy): Future[ScanResult] =
    repairer.scan(storeId, policy)

  private[repair] def testingOnlyScanStates: Map[StoreId, ScanState] = synchronized { scanStates }

  private[repair] def sweep(): Future[Unit] =
    val now = clock()
    val repairable = target.repairableStoreIds
    val pools = repairable.map(_.poolId).distinct

    for
      _ <- refreshLimits()
      _ <- refreshPolicies(pools)
      due = selectDue(repairable, now)
      _ <- runBoundedParallel(due, limitOf.maxConcurrentStoreScans)(scanOne)
    yield ()

  private def limitOf: HostRepairLimits = synchronized { limits }

  private def policyOf(poolId: PoolId): RepairPolicy =
    synchronized { policyCache.getOrElse(poolId, RepairPolicy.Default) }

  /** Policy reads are advisory and must never gate repair -- this is the service that has to
    * keep working while the system is unhealthy. A pool whose metadata cannot be read keeps its
    * last-known-good policy, or the default if it never had one.
    */
  private def refreshPolicies(pools: List[PoolId]): Future[Unit] =
    val f = runBoundedParallel(pools, limitOf.maxConcurrentStoreScans): poolId =>
      val fp = RepairPolicy.read(client, poolId).map: policy =>
        synchronized:
          policyCache = policyCache + (poolId -> policy)
      fp.recover:
        case t: Throwable =>
          logger.debug(s"Repair policy read failed for pool $poolId; keeping " +
                       s"${policyOf(poolId)}: $t")
    f.map(_ => ())

  private def refreshLimits(): Future[Unit] =
    val fl = HostRepairLimits.read(client, hostId).map: l =>
      synchronized:
        limits = l
    fl.recover:
      case t: Throwable =>
        logger.debug(s"Repair limits read failed for host $hostId; keeping ${limitOf}: $t")

  /** Drops states for departed stores, admits new ones, and returns the due set.
    *
    * A newly admitted store is deliberately not due on the tick that admits it: admit() places
    * it at a random point inside the first floor interval so a host restart does not scan every
    * store at once.
    */
  private def selectDue(repairable: List[StoreId], now: Long): List[StoreId] = synchronized {
    val live = repairable.toSet
    scanStates = scanStates.filter((storeId, _) => live.contains(storeId))

    repairable.filter: storeId =>
      scanStates.get(storeId) match
        case Some(state) => state.nextDue <= now
        case None =>
          scanStates = scanStates + (storeId -> ScanState.admit(now, policyOf(storeId.poolId), rand))
          false
  }

  /** runBoundedParallel fails fast, so every store's scan is recovered here: one bad store must
    * not abort the sweep for the rest.
    */
  private def scanOne(storeId: StoreId): Future[Unit] =
    val policy = policyOf(storeId.poolId)

    val fFound = scanStore(storeId, policy).map: result =>
      if result.seen > 0 then
        logger.info(s"Repair scan of $storeId: ${result.seen} seen, ${result.repaired} " +
                    s"repaired, ${result.deferred} deferred")
      result.foundEntries

    val recovered = fFound.recover:
      case t: Throwable =>
        logger.warn(s"Repair scan of $storeId failed: $t")
        false

    recovered.map: foundEntries =>
      synchronized:
        scanStates.get(storeId).foreach: prev =>
          scanStates = scanStates +
            (storeId -> ScanState.advance(prev, clock(), foundEntries, policy, rand))

object RepairService:

  /** schedulePeriodic fixes its period at construction and pool policies differ, so the tick is
    * a constant rather than a per-pool value. A pool whose floor is set below this is rounded up
    * to it; floors above it work exactly as configured, which is the direction that matters,
    * since the knob exists to slow repair down.
    */
  val TickPeriod: Duration = Duration(30, SECONDS)

  /** A sweep of a thousand stores through a concurrency cap of four is legitimately slow. A
    * stall report is an operator's cue that something is wrong, so it must not fire for merely
    * slow work.
    */
  val StallAfter: FiniteDuration = FiniteDuration(6, HOURS)
