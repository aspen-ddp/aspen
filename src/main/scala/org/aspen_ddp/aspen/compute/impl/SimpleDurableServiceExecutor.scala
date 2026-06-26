package org.aspen_ddp.aspen.compute.impl

import org.aspen_ddp.aspen.client.{AspenClient, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, Value}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.util.ignoreExtraCallsWhileRunning
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

  protected def doScan(): Unit = ()  // implementation added in Task 6

  protected def startRenewalTimer(serviceUUID: UUID): ScheduledTask =
    backgroundTasks.schedulePeriodic(renewalInterval):
      doRenewal(serviceUUID)

  protected def doRenewal(serviceUUID: UUID): Unit = ()  // implementation added in Task 7

  override def registerService(
    typeUUID: UUID,
    serviceUUID: UUID,
    initialState: Map[Key, Array[Byte]]
  ): Future[Unit] = Future.unit  // implementation added in Task 5

  override def unregisterService(serviceUUID: UUID): Future[Unit] = Future.unit  // implementation added in Task 9
