package org.aspen_ddp.aspen.compute.impl

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.util.{boundedSingleFlight, byte2uuid}
import org.aspen_ddp.aspen.compute.{DurableService, DurableServiceExecutor, DurableServiceFactory, ServiceEntry}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

/** Runs DurableServices claimed from the services TKVL.
 *
 *  The DurableService scaladoc describes claiming, leases and the scanning strategy. In short: each host scales
 *  its scan window by `hostCount` so that, across the cluster, a scan happens roughly every
 *  `clusterScanInterval`, keeping the load on the Radicle and the TKVL independent of cluster size.
 *
 *  @param hostCount approximate number of hosts in the system; 0 when unknown, which selects the unscaled window.
 *                   Read at construction and again every `hostCountRecheckInterval`.
 */
class SimpleDurableServiceExecutor(
  val client: AspenClient,
  val hostId: HostId,
  val backgroundTasks: BackgroundTaskManager,
  val leaseDuration: Duration        = Duration(15, SECONDS),
  val renewalInterval: Duration      = Duration(5, SECONDS),
  val minScanInterval: Duration      = Duration(10, SECONDS),
  val maxScanInterval: Duration      = Duration(30, SECONDS),
  val claimDelayPerService: Duration = Duration(500, MILLISECONDS),
  val hostCount: () => Int           = () => 0,
  val clusterScanInterval: Duration  = Duration(15, SECONDS),
  val hostCountRecheckInterval: Duration = Duration(1, HOURS)
) extends DurableServiceExecutor:

  given ExecutionContext = client.clientContext

  // serviceUUID -> (running instance, renewal timer)
  private var ownedServices: Map[UUID, (DurableService, ScheduledTask)] = Map.empty

  // One-shot task for the next scan (rescheduled after each scan completes)
  private var scanTask: ScheduledTask = NoTask

  // Bumped by every scheduleScan. A scan task runs only if its generation is still current, so a task that
  // a rescheduling cancel() raced with cannot start a second scan chain.
  private var scanGeneration: Long = 0

  // System.nanoTime at which the pending scan task fires
  private var nextScanAt: Long = 0

  private var currentHostCount: Int = readHostCount()

  @volatile private var isShutdown = false

  val servicesTkvl: TieredKeyValueList =
    TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))

  // Start the first scan on construction
  scheduleScan()

  private val hostCountTask: ScheduledTask = backgroundTasks.schedulePeriodic(hostCountRecheckInterval):
    recheckHostCount()

  /** Cancels all timers and shuts down all owned services. */
  def shutdown(): Unit = synchronized:
    isShutdown = true
    scanTask.cancel()
    hostCountTask.cancel()
    ownedServices.foreach: (_, pair) =>
      val (service, renewalTimer) = pair
      renewalTimer.cancel()
      service.shutdown()
    ownedServices = Map.empty

  override def deliverMessage(msg: ServiceMessage): Unit = synchronized:
    ownedServices.get(msg.serviceUUID).foreach: (service, _) =>
      service.receiveMessage(msg)

  private def readHostCount(): Int =
    try hostCount()
    catch
      case NonFatal(_) => 0

  /** Picks up a changed host count, which the next scheduleScan uses. The pending scan is re-armed only when it is
   *  due later than the new window allows, as after the cluster shrank. Re-arming on every change would let a
   *  count that drifts each hour keep pushing back a scan whose window starts more than an hour out. */
  private def recheckHostCount(): Unit =
    val n = readHostCount()
    synchronized:
      if n != currentHostCount then
        currentHostCount = n
        val (_, hi) = scanWindowFor(n)
        if nextScanAt - System.nanoTime() > hi.toNanos then
          scanTask.cancel()
          scheduleScan()

  private def scanWindowFor(n: Int): (FiniteDuration, FiniteDuration) =
    SimpleDurableServiceExecutor.scanWindow(n, minScanInterval, maxScanInterval, clusterScanInterval)

  protected def scheduleScan(): Unit =
    synchronized:
      // A scan that was in flight when shutdown() ran finishes by calling this. Without the
      // check it would re-arm itself and scan for the life of the process.
      if !isShutdown then
        val (lo, hi) = scanWindowFor(currentHostCount)
        val rangeMillis = hi.toMillis - lo.toMillis
        val delayMillis = lo.toMillis + ThreadLocalRandom.current().nextLong(math.max(1L, rangeMillis))
        nextScanAt = System.nanoTime() + delayMillis * 1000000L
        scanGeneration += 1
        val generation = scanGeneration
        scanTask = backgroundTasks.schedule(Duration(delayMillis, MILLISECONDS)):
          if synchronized(generation == scanGeneration) then
            doScan()

  private val scanFn: () => Unit = boundedSingleFlight("durable-service-scan"):
    val entries = ListBuffer[(Key, ServiceEntry, ObjectRevision)]()

    servicesTkvl.foreach: (_, key, vs) =>
      entries += ((key, ServiceEntry.decode(vs.value.bytes), vs.revision))
      Future.unit
    .map: _ =>
      val tkvlUUIDs   = entries.map((key, _, _) => byte2uuid(key.bytes)).toSet
      val currentOwned = synchronized { ownedServices.keySet.toSet }

      // Unregistration check: shutdown any owned service that disappeared from the TKVL
      synchronized:
        val removed = currentOwned.filter(svcUUID => !tkvlUUIDs.contains(svcUUID))
        removed.foreach: svcUUID =>
          ownedServices.get(svcUUID).foreach: (service, timer) =>
            timer.cancel()
            service.shutdown()
          ownedServices -= svcUUID

      val candidates = entries.filter: (key, entry, _) =>
        val svcUUID = byte2uuid(key.bytes)
        (!entry.isClaimed || entry.isExpired) && !currentOwned.contains(svcUUID)

      candidates.foreach: (key, entry, revision) =>
        val delay =
          val n = synchronized(ownedServices.size)
          if n == 0 || claimDelayPerService == Duration.Zero then Duration.Zero
          else Duration(ThreadLocalRandom.current().nextLong(n * claimDelayPerService.toMillis), MILLISECONDS)
        backgroundTasks.schedule(delay):
          attemptClaim(key, entry.typeUUID, entry.statePointer, revision)

      scheduleScan()

  protected def doScan(): Unit = scanFn()

  private def attemptClaim(
    serviceKey: Key,
    typeUUID: UUID,
    statePointer: KeyValueObjectPointer,
    oldRevision: ObjectRevision
  ): Unit =
    client.typeRegistry.getType[DurableServiceFactory](typeUUID) match
      case None =>
        // No factory registered for this type on this host — skip silently
        ()
      case Some(factory) =>
        val newExpiry = HLCTimestamp(HLCTimestamp.now.asLong + (leaseDuration.toMillis << 16))
        val newEntry  = ServiceEntry(typeUUID, hostId.uuid, newExpiry, statePointer)
        // Single attempt: if another host has already claimed, the revision check will
        // reject the transaction and we simply drop this attempt. The next scan cycle
        // will detect whoever won.
        client.transact: tx =>
          given Transaction = tx
          servicesTkvl.set(serviceKey, Value(newEntry.encode()), Some(Right(oldRevision)))(using tx)
        .foreach: _ =>
          if !isShutdown then
            client.read(statePointer).foreach: state =>
              val svcUUID = byte2uuid(serviceKey.bytes)
              val service = factory.createService(client, statePointer, state)
              val timer   = startRenewalTimer(svcUUID)
              synchronized:
                if !isShutdown then
                  ownedServices += svcUUID -> (service, timer)
                else
                  timer.cancel()
                  service.shutdown()

  protected def startRenewalTimer(serviceUUID: UUID): ScheduledTask =
    // Single flight: a tick must not start a second renewal while the previous one is still
    // retrying, or the chains pile up, collide on the one services key, and keep each other
    // failing. stallAfter is the lease because a renewal outliving the lease has lost it.
    val renew = boundedSingleFlight("durable-service-renewal", stallAfter = FiniteDuration(leaseDuration.toMillis, MILLISECONDS)):
      renewLease(serviceUUID)
    backgroundTasks.schedulePeriodic(renewalInterval):
      renew()

  /** Renew one service's lease, retrying a failed attempt after a short jittered delay until it
   *  commits, the lease is found lost, or this host stops owning the service. Every attempt
   *  starts from a fresh read, so ownership changes and an expired lease end the retrying. */
  private def renewLease(serviceUUID: UUID): Future[Unit] =
    val serviceKey = Key(serviceUUID)

    def stillOwned: Boolean = synchronized(!isShutdown && ownedServices.contains(serviceUUID))

    def retryLater(): Future[Unit] =
      val p = Promise[Unit]()
      val delayMillis = 10 + ThreadLocalRandom.current().nextLong(math.max(1L, renewalInterval.toMillis / 4))
      backgroundTasks.schedule(Duration(delayMillis, MILLISECONDS)):
        p.success(())
      p.future.flatMap(_ => attempt())

    def attempt(): Future[Unit] =
      if !stillOwned then
        Future.unit
      else
        servicesTkvl.get(serviceKey).flatMap:
          case None =>
            // Service was unregistered; scan will detect and clean up ownedServices
            Future.unit
          case Some(vs) =>
            val entry = ServiceEntry.decode(vs.value.bytes)
            val now   = HLCTimestamp.now

            if entry.hostId != hostId.uuid || entry.leaseExpiry < now then
              handleLeaseLoss(serviceUUID)
              Future.unit
            else
              val newExpiry = HLCTimestamp(now.asLong + (leaseDuration.toMillis << 16))
              val newEntry  = entry.copy(leaseExpiry = newExpiry)
              client.transact: tx =>
                given Transaction = tx
                servicesTkvl.set(serviceKey, Value(newEntry.encode()), Some(Right(vs.revision)))(using tx)
              .recoverWith:
                case _ => retryLater()
        .recoverWith:
          case _ => retryLater()

    attempt()

  private def handleLeaseLoss(serviceUUID: UUID): Unit = synchronized:
    ownedServices.get(serviceUUID).foreach: (service, timer) =>
      timer.cancel()
      service.shutdown()
    ownedServices -= serviceUUID

  override def registerService(
    typeUUID: UUID,
    serviceUUID: UUID,
    initialState: Map[Key, Array[Byte]]
  ): Future[Unit] =
    val serviceKey = Key(serviceUUID)

    client.retryStrategy.retryUntilSuccessful:
      servicesTkvl.get(serviceKey).flatMap:
        case Some(_) => Future.unit  // already registered — idempotent
        case None =>
          client.transact: tx =>
            given Transaction = tx
            for
              pool      <- client.getStoragePool(Radicle.poolId)
              allocator  = new PoolObjectAllocator(client, pool)
              statePtr  <- allocator.allocateKeyValueObject(initialState.map((k, v) => k -> Value(v)))
              entry      = ServiceEntry(typeUUID, ServiceEntry.UnclaimedHostId, HLCTimestamp.Zero, statePtr)
              _         <- servicesTkvl.set(serviceKey, Value(entry.encode()), Some(Left(true)))
            yield ()

  override def unregisterService(serviceUUID: UUID): Future[Unit] =
    // If this host owns the service, shut it down immediately (before the transaction)
    synchronized:
      ownedServices.get(serviceUUID).foreach: (service, timer) =>
        timer.cancel()
        service.shutdown()
      ownedServices -= serviceUUID

    val serviceKey = Key(serviceUUID)

    servicesTkvl.get(serviceKey).flatMap:
      case None => Future.unit  // already gone
      case Some(vs) =>
        val entry = ServiceEntry.decode(vs.value.bytes)
        client.read(entry.statePointer).flatMap: stateKvos =>
          client.retryStrategy.retryUntilSuccessful:
            client.transact: tx =>
              given Transaction = tx
              for _ <- servicesTkvl.delete(serviceKey)(using tx) yield
                tx.setRefcount(entry.statePointer, stateKvos.refcount, stateKvos.refcount.decrement())

object SimpleDurableServiceExecutor:

  /** The window each host draws its next scan delay from, uniformly.
   *
   *  With n hosts each scanning on average every M, the cluster scans on average every M/n. M is set to
   *  n * clusterScanInterval and the window to [M/2, 3M/2], floored at [minScan, maxScan] so small clusters, and
   *  a host count of 0 (unknown), keep the unscaled window.
   */
  def scanWindow(hostCount: Int, minScan: Duration, maxScan: Duration,
                 clusterScanInterval: Duration): (FiniteDuration, FiniteDuration) =
    val meanMillis = math.max(0, hostCount).toLong * clusterScanInterval.toMillis
    val lo = math.max(minScan.toMillis, meanMillis / 2)
    val hi = math.max(maxScan.toMillis, meanMillis * 3 / 2)
    (FiniteDuration(lo, MILLISECONDS), FiniteDuration(hi, MILLISECONDS))
