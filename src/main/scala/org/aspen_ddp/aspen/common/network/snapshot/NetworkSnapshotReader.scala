package org.aspen_ddp.aspen.common.network.snapshot

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.MetadataManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import scribe.Logging

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Reads the NetworkCachingService snapshots and applies them to `mm`: once at `start` when the network cache was
 *  empty (always so for NoCache processes such as the CLI), then at a random point within the first `period`, so
 *  hosts do not all read at once, and every `period` after that.
 *
 *  Each snapshot is skipped unless its scanStart is newer than the last one of its kind this reader applied; when
 *  only one is skipped, an empty Zero snapshot, which adds and drops nothing, stands in for it. Host entries the
 *  manager replaces or drops go to `onRetired`, outside the manager's monitor. A failed read is logged and the
 *  schedule continues.
 *
 *  A process that started with an empty cache has nothing to send with but lookups until a read succeeds, so a
 *  failed startup read is retried after each of `StartupRetryDelays` in turn, rather than waiting up to a whole
 *  `period` for the random slot. The retries end at the first successful read, by either path, and once the
 *  delays run out the normal schedule alone remains.
 */
class NetworkSnapshotReader[T <: MetadataManager.HostEntry](mm: MetadataManager[T],
                                                            fetch: () => Future[Option[(HostSnapshot, StoreSnapshot)]],
                                                            tasks: BackgroundTaskManager,
                                                            onRetired: T => Unit,
                                                            period: Duration = 24.hours,
                                                            dropMargin: Duration = 10.minutes)
                                                           (using ExecutionContext) extends Logging:

  private var task: ScheduledTask = NoTask
  private var retryTask: ScheduledTask = NoTask
  private var stopped = false
  private var anyReadSucceeded = false
  // None until the first snapshot of each kind is applied, so a Zero snapshot is applied rather than skipped
  private var lastHosts: Option[HLCTimestamp] = None
  private var lastStores: Option[HLCTimestamp] = None

  def start(): Unit =
    synchronized:
      if !stopped then
        // scheduleRandomlyWithinWindow draws nextInt(window.length) in the window's own unit, so the window must be
        // in milliseconds for the slot to fall anywhere in the period
        task = tasks.scheduleRandomlyWithinWindow(Duration(math.max(1L, period.toMillis), MILLISECONDS)):
          scheduledRead()
    if mm.cacheWasEmpty then
      startupRead(NetworkSnapshotReader.StartupRetryDelays)

  /** Cancels the schedule and any startup retry. A read in flight applies nothing once this returns. */
  def stop(): Unit =
    synchronized:
      stopped = true
      task.cancel()
      retryTask.cancel()

  private def scheduledRead(): Unit =
    read().onComplete: _ =>
      synchronized:
        if !stopped then
          task = tasks.schedule(period)(scheduledRead())

  /** Reads, and on failure schedules the next of `delays`. Skips the read once stopped or once any read has
   *  succeeded, since the scheduled read may have got there first. */
  private def startupRead(delays: List[Duration]): Unit =
    val due = synchronized(!stopped && !anyReadSucceeded)
    if due then
      read().foreach: succeeded =>
        synchronized:
          if !succeeded && !stopped && !anyReadSucceeded then
            delays match
              case delay :: rest =>
                logger.info(s"Retrying the startup network snapshot read in $delay")
                retryTask = tasks.schedule(delay)(startupRead(rest))
              case Nil =>
                logger.info("Startup network snapshot reads failed; leaving it to the daily schedule")

  /** Never fails. Completes with whether the snapshots were fetched and applied (no service entry counts). */
  private def read(): Future[Boolean] =
    val fetched =
      try fetch()
      catch case NonFatal(t) => Future.failed(t)
    fetched
      .map: result =>
        result match
          case None => logger.debug("No network caching service entry; no snapshot to apply")
          case Some((hosts, stores)) => applyNewer(hosts, stores).foreach(retire)
        synchronized:
          anyReadSucceeded = true
        true
      .recover:
        case NonFatal(t) =>
          logger.warn(s"Failed to read the network snapshots: $t")
          false

  /** The entries to retire. The reader's lock is held across the stopped check and applySnapshot, so nothing is
   *  applied once stop has returned. */
  private def applyNewer(hosts: HostSnapshot, stores: StoreSnapshot): List[T] =
    synchronized:
      if stopped then
        Nil
      else
        val newHosts = lastHosts.forall(_ < hosts.scanStart)
        val newStores = lastStores.forall(_ < stores.scanStart)
        if !newHosts && !newStores then
          logger.debug("Network snapshots are no newer than those already applied")
          Nil
        else
          if newHosts then
            lastHosts = Some(hosts.scanStart)
          if newStores then
            lastStores = Some(stores.scanStart)
          mm.applySnapshot(
            if newHosts then hosts else HostSnapshot(HLCTimestamp.Zero, Nil),
            if newStores then stores else StoreSnapshot(HLCTimestamp.Zero, Nil),
            dropMargin)

  private def retire(entry: T): Unit =
    try onRetired(entry)
    catch
      case NonFatal(t) => logger.warn(s"Failed to retire the old entry for host ${entry.hostId}: $t")


object NetworkSnapshotReader:

  /** Delays before each retry of a failed startup read, for a process that started with an empty cache */
  val StartupRetryDelays: List[Duration] = List(1.minute, 5.minutes, 30.minutes)

  /** Starts a reader fetching through `client` and scheduling on its background task manager */
  def start[T <: MetadataManager.HostEntry](client: AspenClient,
                                            mm: MetadataManager[T],
                                            onRetired: T => Unit): NetworkSnapshotReader[T] =
    given ExecutionContext = client.clientContext
    val reader = new NetworkSnapshotReader[T](mm, () => NetworkCachingService.readSnapshots(client),
      client.backgroundTaskManager, onRetired)
    reader.start()
    reader
