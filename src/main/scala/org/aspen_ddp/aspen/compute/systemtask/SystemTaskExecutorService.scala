package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.ExecuteSystemTask
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.compute.{DurableService, DurableServiceFactory}
import scribe.Logging

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.{Duration, MINUTES, SECONDS}
import scala.concurrent.{ExecutionContext, Future}

object SystemTaskExecutorService extends DurableServiceFactory with Logging:

  val ServiceTypeUUID: UUID = UUID.fromString("a1e5c9d4-3b7f-4a21-9c8e-2d6f0b13e7a4")
  val ServiceUUID: UUID     = UUID.fromString("c4d8f2b6-1a09-4e73-8b5d-7f3c9e0a24d1")

  val typeUUID: UUID = ServiceTypeUUID

  /** Key in the service state object whose value is the encoded Root of the task TKVL. */
  val TaskTreeKey: Key = Key(0)

  // Tuning (overridable test seams; mirrors RebalancingDurableService.pollPeriod).
  val DefaultReconcilePeriod: Duration = Duration(2, MINUTES)
  val DefaultMonitorPeriod: Duration   = Duration(15, SECONDS)
  val DefaultFirstCheckDelay: Duration = Duration(30, SECONDS)
  val DefaultStallTimeout: Duration    = Duration(45, SECONDS)
  val DefaultHostCacheTtl: Duration    = Duration(30, MINUTES)

  @volatile var reconcilePeriod: Duration = DefaultReconcilePeriod
  @volatile var monitorPeriod: Duration   = DefaultMonitorPeriod
  @volatile var firstCheckDelay: Duration = DefaultFirstCheckDelay
  @volatile var stallTimeout: Duration    = DefaultStallTimeout
  @volatile var hostCacheTtl: Duration    = DefaultHostCacheTtl

  override def createService(client: AspenClient,
                             statePointer: KeyValueObjectPointer,
                             state: KeyValueObjectState): DurableService =
    new SystemTaskExecutorService(client, statePointer)


/** Per-task liveness tracking held by the running service instance. */
private final case class TaskTrack(assignedHost: HostId,
                                   lastRevision: org.aspen_ddp.aspen.common.objects.ObjectRevision,
                                   lastChangeNanos: Long,
                                   dispatchedNanos: Long)

class SystemTaskExecutorService(val client: AspenClient,
                                val statePointer: KeyValueObjectPointer) extends DurableService with Logging:
  import SystemTaskExecutorService.*

  private given ExecutionContext = client.clientContext

  private var tracking: Map[UUID, TaskTrack] = Map.empty
  private var dispatching: Set[UUID] = Set.empty
  private var hostCache: List[HostId] = Nil
  private var hostCacheStampNanos: Long = 0L
  private var reconcileTask: ScheduledTask = NoTask
  private var monitorTask: ScheduledTask = NoTask
  @volatile private var stopped = false

  reconcile()
  reconcileTask = client.backgroundTaskManager.scheduleNonConcurrentPollingTask(
    "system-task-reconcile", reconcilePeriod):
    reconcile()
  monitorTask = client.backgroundTaskManager.scheduleNonConcurrentPollingTask(
    "system-task-monitor", monitorPeriod):
    monitor()

  override def shutdown(): Unit = synchronized:
    stopped = true
    reconcileTask.cancel()
    monitorTask.cancel()
    tracking = Map.empty
    dispatching = Set.empty

  override def receiveMessage(msg: ServiceMessage): Unit =
    SystemTaskMessage.decode(msg.encodedContent) match
      case NewSystemTaskAdded(_)  => reconcile()
      case SystemTaskComplete(id) => synchronized { tracking -= id }

  private def refreshHostsIfNeeded(): Future[List[HostId]] =
    val now = System.nanoTime()
    val (cached, stamp) = synchronized((hostCache, hostCacheStampNanos))
    if cached.nonEmpty && (now - stamp) < hostCacheTtl.toNanos then
      Future.successful(cached)
    else
      client.listHosts().map: hosts =>
        val ids = hosts.map(_._2)
        synchronized:
          hostCache = ids
          hostCacheStampNanos = System.nanoTime()
        ids

  private def pickHost(): Future[Option[HostId]] =
    refreshHostsIfNeeded().map: all =>
      val offline = client.offlineHosts()
      val candidates = all.filterNot(offline.contains)
      if candidates.isEmpty then None
      else Some(candidates(ThreadLocalRandom.current().nextInt(candidates.size)))

  /** Reserve a NEW task for dispatch (must be neither tracked nor already dispatching). */
  private def reserveNew(taskId: UUID): Boolean = synchronized:
    if stopped || dispatching.contains(taskId) || tracking.contains(taskId) then false
    else { dispatching += taskId; true }

  /** Reserve a tracked task for REASSIGNMENT (only blocked by an in-flight dispatch). */
  private def reserveReassign(taskId: UUID): Boolean = synchronized:
    if stopped || dispatching.contains(taskId) then false
    else { dispatching += taskId; true }

  private def doDispatch(taskId: UUID, taskStatePtr: KeyValueObjectPointer): Future[Unit] =
    pickHost().flatMap:
      case None =>
        logger.warn(s"No online host to run system task $taskId")
        Future.unit
      case Some(host) =>
        client.read(taskStatePtr).map: kvos =>
          client.sendHostMessage(ExecuteSystemTask(host, client.clientId, taskId, taskStatePtr))
          val now = System.nanoTime()
          synchronized:
            if !stopped then
              tracking += taskId -> TaskTrack(host, kvos.revision, now, now)
    .recover:
      case err => logger.warn(s"Dispatch of system task $taskId failed: $err")
    .andThen:
      case _ => synchronized { dispatching -= taskId }

  /** Discover enrolled tasks and dispatch any not yet tracked; drop tracking for vanished tasks. */
  private def reconcile(): Future[Unit] =
    if stopped then Future.unit
    else
      SystemTaskServiceState.scan(client, statePointer).flatMap: enrolled =>
        val enrolledIds = enrolled.map(_._1).toSet
        synchronized { tracking = tracking.filter((id, _) => enrolledIds.contains(id)) }
        val toDispatch = enrolled.filter((id, _) => reserveNew(id))
        Future.traverse(toDispatch)((id, ptr) => doDispatch(id, ptr)).map(_ => ())
      .recover:
        case err => logger.warn(s"SystemTaskExecutorService reconcile failed: $err")

  /** Liveness check: reassign any tracked task whose state-object revision has stalled. */
  private def monitor(): Future[Unit] =
    if stopped then Future.unit
    else
      val snapshot = synchronized(tracking.toList)
      SystemTaskServiceState.scan(client, statePointer).flatMap: enrolled =>
        val enrolledMap = enrolled.toMap
        Future.traverse(snapshot): (taskId, track) =>
          enrolledMap.get(taskId) match
            case None =>
              // Completed (registry entry removed by the runner): drop tracking.
              synchronized { tracking -= taskId }
              Future.unit
            case Some(taskStatePtr) =>
              client.read(taskStatePtr).flatMap: kvos =>
                val now = System.nanoTime()
                if kvos.revision != track.lastRevision then
                  synchronized:
                    tracking.get(taskId).foreach: t =>
                      tracking += taskId -> t.copy(lastRevision = kvos.revision, lastChangeNanos = now)
                  Future.unit
                else
                  val stalled = (now - track.lastChangeNanos) > stallTimeout.toNanos
                  val pastFirstCheck = (now - track.dispatchedNanos) > firstCheckDelay.toNanos
                  if stalled && pastFirstCheck then
                    if reserveReassign(taskId) then
                      logger.info(s"System task $taskId stalled; reassigning")
                      doDispatch(taskId, taskStatePtr)
                    else
                      Future.unit
                  else
                    Future.unit
              .recover:
                case err => logger.warn(s"Monitor read for system task $taskId failed: $err")
        .map(_ => ())
      .recover:
        case err => logger.warn(s"SystemTaskExecutorService monitor failed: $err")
