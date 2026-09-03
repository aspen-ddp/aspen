package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, runBoundedParallel}
import scribe.Logging

import scala.concurrent.duration.{Duration, FiniteDuration, HOURS, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

case class ScanTimedOut(storeId: StoreId, after: Duration)
  extends Exception(s"Repair scan of $storeId did not complete within $after")

case class MetadataReadTimedOut(what: String, after: Duration)
  extends Exception(s"Repair metadata read for $what did not complete within $after")

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
                    private val backgroundTasks: BackgroundTaskManager,
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

  /** Overridable so the deadline can be tested without waiting for it. */
  protected def scanDeadline: FiniteDuration = ScanDeadline

  /** Overridable so the metadata deadline can be tested without waiting for it. */
  protected def metadataDeadline: FiniteDuration = MetadataDeadline

  /** Bounds a future that would otherwise never settle.
    *
    * Aspen reads of an unavailable object retry indefinitely rather than failing, so any read on
    * the sweep's critical path can stay outstanding forever. Unbounded, that wedges the sweep,
    * and the single-flight guard then suppresses every later tick -- one sick pool would stop
    * repair for every healthy store on the host. The abandoned read continues in the background;
    * the timeout is reported through the caller's normal recovery path.
    */
  private def withDeadline[T](deadline: FiniteDuration, timeoutError: => Throwable)
                             (f: Future[T]): Future[T] =
    val p = Promise[T]()

    val timer = backgroundTasks.schedule(deadline):
      p.tryFailure(timeoutError)

    f.onComplete: t =>
      timer.cancel()
      p.tryComplete(t)

    p.future

  private[repair] def testingOnlyScanStates: Map[StoreId, ScanState] = synchronized { scanStates }

  /** Policies are refreshed for the due set rather than for every repairable store, so per-tick
    * metadata cost tracks the work actually being done rather than the host's pool inventory. A
    * store admitted on this tick is therefore placed by ScanState.admit using whatever policy is
    * already cached -- the default floor for a pool never seen before. That is corrected on the
    * store's next tick, and admission places it at a random offset inside the first interval
    * anyway.
    */
  private[repair] def sweep(): Future[Unit] =
    val now = clock()
    val repairable = target.repairableStoreIds

    for
      _ <- refreshLimits()
      due = selectDue(repairable, now)
      _ <- refreshPolicies(due.map(_.poolId).distinct)
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
      val fp = Future.unit.flatMap(_ =>
        withDeadline(metadataDeadline,
                     MetadataReadTimedOut(s"policy of pool $poolId", metadataDeadline)):
          RepairPolicy.read(client, poolId)
      ).map: policy =>
        synchronized:
          policyCache = policyCache + (poolId -> policy)
      fp.recover:
        case t: Throwable =>
          logger.debug(s"Repair policy read failed for pool $poolId; keeping " +
                       s"${policyOf(poolId)}: $t")
    f.map(_ => ())

  private def refreshLimits(): Future[Unit] =
    val fl = Future.unit.flatMap(_ =>
      withDeadline(metadataDeadline,
                   MetadataReadTimedOut(s"limits of host $hostId", metadataDeadline)):
        HostRepairLimits.read(client, hostId)
    ).map: l =>
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

    val fFound = Future.unit
      .flatMap(_ => withDeadline(scanDeadline, ScanTimedOut(storeId, scanDeadline)):
        scanStore(storeId, policy))
      .map: result =>
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

  /** Generous relative to a healthy scan -- which is milliseconds -- but bounded well under the
    * scanIntervalCap, so a store that times out repeatedly does not spend most of its cycle hung
    * holding one of the few concurrent scan slots. A deadline close to the cap would let a
    * handful of stores in an unreachable pool collapse host-wide scan throughput and eventually
    * trip StallAfter under exactly the conditions this service exists to tolerate. An interrupted
    * scan loses no completed work: every repaired entry is deleted as it goes, and the next scan
    * resumes from the tree head.
    */
  val ScanDeadline: FiniteDuration = FiniteDuration(90, SECONDS)

  /** Metadata reads are advisory -- the fallback is a cached or default policy, which is cheap
    * and correct -- so they get a far tighter bound than a scan. Long enough to ride out ordinary
    * latency, short enough that an unreadable pool costs the sweep seconds rather than stopping it.
    */
  val MetadataDeadline: FiniteDuration = FiniteDuration(15, SECONDS)
