package org.aspen_ddp.aspen.compute.impl

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.util.{byte2uuid, ignoreExtraCallsWhileRunning}
import org.aspen_ddp.aspen.compute.{DurableService, DurableServiceExecutor, DurableServiceFactory, ServiceEntry}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class SimpleDurableServiceExecutor(
  val client: AspenClient,
  val hostId: HostId,
  val backgroundTasks: BackgroundTaskManager,
  val leaseDuration: Duration        = Duration(15, SECONDS),
  val renewalInterval: Duration      = Duration(5, SECONDS),
  val minScanInterval: Duration      = Duration(10, SECONDS),
  val maxScanInterval: Duration      = Duration(30, SECONDS),
  val claimDelayPerService: Duration = Duration(500, MILLISECONDS)
) extends DurableServiceExecutor:

  given ExecutionContext = client.clientContext

  // serviceUUID -> (running instance, renewal timer)
  private var ownedServices: Map[UUID, (DurableService, ScheduledTask)] = Map.empty

  // One-shot task for the next scan (rescheduled after each scan completes)
  private var scanTask: ScheduledTask = NoTask

  val servicesTkvl: TieredKeyValueList =
    TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))

  // Start the first scan on construction
  scheduleScan()

  /** Cancels all timers and shuts down all owned services. */
  def shutdown(): Unit = synchronized:
    scanTask.cancel()
    ownedServices.foreach: (_, pair) =>
      val (service, renewalTimer) = pair
      renewalTimer.cancel()
      service.shutdown()
    ownedServices = Map.empty

  protected def scheduleScan(): Unit =
    val rangeMillis = maxScanInterval.toMillis - minScanInterval.toMillis
    val delayMillis = minScanInterval.toMillis + ThreadLocalRandom.current().nextLong(rangeMillis)
    synchronized:
      scanTask = backgroundTasks.schedule(Duration(delayMillis, MILLISECONDS)):
        doScan()

  private val scanFn: () => Unit = ignoreExtraCallsWhileRunning:
    val entries = ListBuffer[(Key, ServiceEntry, ObjectRevision)]()

    servicesTkvl.foreach: (_, key, vs) =>
      entries += ((key, ServiceEntry.decode(vs.value.bytes), vs.revision))
      Future.unit
    .map: _ =>
      val currentOwned = synchronized { ownedServices.keySet.toSet }

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
          client.read(statePointer).foreach: state =>
            val svcUUID = byte2uuid(serviceKey.bytes)
            val service = factory.createService(client, statePointer, state)
            val timer   = startRenewalTimer(svcUUID)
            synchronized:
              ownedServices += svcUUID -> (service, timer)

  protected def startRenewalTimer(serviceUUID: UUID): ScheduledTask =
    backgroundTasks.schedulePeriodic(renewalInterval):
      doRenewal(serviceUUID)

  protected def doRenewal(serviceUUID: UUID): Unit = ()  // implementation added in Task 7

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

  override def unregisterService(serviceUUID: UUID): Future[Unit] = Future.unit  // implementation added in Task 9
