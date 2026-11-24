package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState
import scribe.Logging

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** A loaded device's health (device hang detection, spec section 5.3): its status, the time of its last transition
 *  and the evidence built at that transition */
final case class DeviceHealth(status: StorageDeviceState.HealthStatus, since: HLCTimestamp, evidence: String)

object DeviceHealth:
  /** A device's health at load, before any transition: Healthy with no `since` */
  val Initial: DeviceHealth = DeviceHealth(StorageDeviceState.HealthStatus.Healthy, HLCTimestamp.Zero, "")

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  Judges each loaded device's health (spec section 5.2) from its probe: Hung while the probe attempt in flight is
 *  older than its `hangThreshold`, Healthy otherwise. A probe error alone, or any other operation however old, never
 *  makes a device Hung. Each pass (`check`, every `Period` from HostServices' trigger) reads only thread-safe,
 *  non-blocking state (the probe, the observer, `readsOverBound`): it never blocks and never submits device I/O.
 *
 *  A transition sets `LocalStorageDeviceState.health`, logs once (WARN to Hung with the evidence, INFO back to
 *  Healthy with how long the device was hung) and calls every `onTransition` hook (project 3's), on this thread. A
 *  pass with no change logs nothing. A probe error is logged once while it persists.
 *
 *  After judging a device, the pass publishes its health into the device's record (`publish`) until the record
 *  matches, through `DeviceControl.healthPublisher` on the client context: never on the device. */
final class DeviceHealthJudge(ctx: DeviceControl) extends Logging:
  private var hooks: List[(LocalStorageDeviceState, DeviceHealth) => Unit] = Nil

  /** Calls `f` on the control thread after each transition, with the device and its new health */
  def onTransition(f: (LocalStorageDeviceState, DeviceHealth) => Unit): Unit = hooks = hooks :+ f

  /** One pass over the loaded devices */
  def check(): Unit = ctx.devices.devices.valuesIterator.foreach: sds =>
    judge(sds)
    publish(sds)

  private def judge(sds: LocalStorageDeviceState): Unit =
    val probe = sds.io.probe
    noteProbeError(sds, probe)
    val status =
      if probe.hung then StorageDeviceState.HealthStatus.Hung
      else StorageDeviceState.HealthStatus.Healthy
    if status != sds.health.status then
      val previous = sds.health
      val now = HLCTimestamp.now
      sds.health = DeviceHealth(status, now, DeviceHealthJudge.evidence(probe, sds.io.observer, sds.io.readsOverBound))
      status match
        case StorageDeviceState.HealthStatus.Hung =>
          logger.warn(s"Storage device ${sds.storageDeviceId} at ${sds.devicePath} is Hung: ${sds.health.evidence}")
        case StorageDeviceState.HealthStatus.Healthy =>
          val hungFor = (now - previous.since) match
            case d: FiniteDuration => DeviceHealthJudge.format(d)
            case _ => "an unknown time"
          logger.info(s"Storage device ${sds.storageDeviceId} at ${sds.devicePath} is Healthy again, hung for $hungFor")
      hooks.foreach: hook =>
        try hook(sds, sds.health)
        catch case NonFatal(t) =>
          logger.error(s"A health transition hook failed for storage device ${sds.storageDeviceId}: $t")

  /** Starts a publish of the device's health while it differs from the one last published and none is in flight
   *  (spec section 5.3). The publisher reads `health` when its transaction body runs, on the client context, so a
   *  transition meanwhile is written by this publish or, failing that, by the next pass's. The outcome is applied
   *  here, on the control thread, only while `sds` is still the loaded state; a failure leaves `publishedHealth` for
   *  the next pass to retry, logged once per streak of failures */
  private def publish(sds: LocalStorageDeviceState): Unit =
    if !sds.healthPublishing && !sds.publishedHealth.contains(sds.health) then
      sds.healthPublishing = true
      val f =
        try ctx.healthPublisher.publish(sds.storageDeviceId, () => sds.health)
        catch case NonFatal(t) => Future.failed(t)
      ctx.onControlFuture(s"health publish ${sds.storageDeviceId}", f): result =>
        if ctx.devices.isLoaded(sds) then
          sds.healthPublishing = false
          result match
            case Success(outcome) =>
              sds.healthPublishFailureLogged = false
              sds.publishedHealth = outcome match
                case DeviceHealthPublisher.Outcome.Written(h) => Some(h)
                case DeviceHealthPublisher.Outcome.Unchanged(h) => Some(h)
                case DeviceHealthPublisher.Outcome.Skipped => Some(sds.health)
            case Failure(err) =>
              if !sds.healthPublishFailureLogged then
                sds.healthPublishFailureLogged = true
                logger.warn(s"Publishing the health of storage device ${sds.storageDeviceId} failed, retrying at " +
                  s"each health check: $err")

  /** Logs the probe's last error once while it persists: until a later attempt succeeds */
  private def noteProbeError(sds: LocalStorageDeviceState, probe: HealthProbe): Unit =
    probe.lastResult.map(_.error) match
      case Some(Some(err)) =>
        if !sds.probeErrorLogged then
          sds.probeErrorLogged = true
          logger.warn(s"The health probe of storage device ${sds.storageDeviceId} at ${sds.devicePath} failed: $err")
      case Some(None) => sds.probeErrorLogged = false
      case None => ()

object DeviceHealthJudge:
  val Period: FiniteDuration = 30.seconds
  val MaxEvidence = 1024

  /** The probe's attempt in flight or its last result; each observer kind in flight with its count and oldest age;
   *  `dropping client reads` when `readsOverBound`. At most `MaxEvidence` characters. It names no store: the
   *  observer's in-flight summary has none */
  def evidence(probe: HealthProbe, observer: DeviceIoObserver, readsOverBound: Boolean): String =
    val probePart = probe.inFlightAge match
      case Some(age) => s"probe in flight ${format(age)}"
      case None => probe.lastResult match
        case Some(HealthProbe.Result(_, d, Some(err))) => s"probe failed after ${format(d)}: $err"
        case Some(HealthProbe.Result(_, d, None)) => s"probe ok in ${format(d)}"
        case None => "no probe yet"
    val now = observer.now
    val kinds = observer.inFlight.toList.sortBy(_._1).map: (kind, entry) =>
      val (count, oldest) = entry
      s"$kind: $count in flight, oldest ${format((now - oldest).nanos)}"
    val reads = if readsOverBound then List("dropping client reads") else Nil
    val all = (probePart :: kinds ::: reads).mkString("; ")
    if all.length <= MaxEvidence then all else all.take(MaxEvidence - 3) + "..."

  /** e.g. 3m12s, 1h0m5s, 250ms */
  private[host] def format(d: FiniteDuration): String =
    val ms = d.toMillis.max(0)
    if ms < 1000 then s"${ms}ms"
    else
      val s = ms / 1000
      val (h, m, sec) = (s / 3600, s / 60 % 60, s % 60)
      if h > 0 then s"${h}h${m}m${sec}s"
      else if m > 0 then s"${m}m${sec}s"
      else s"${sec}s"
