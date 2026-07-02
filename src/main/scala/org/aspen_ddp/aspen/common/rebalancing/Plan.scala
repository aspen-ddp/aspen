package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.collection.mutable

/** Pure rebalance planning. See docs/superpowers/specs/2026-07-02-rebalance-planning-algorithm-design.md */
object Plan:

  /** A single planned store transfer. */
  case class Transfer(storeId: StoreId, fromDevice: StorageDeviceId, toDevice: StorageDeviceId)

  /** Tuning knobs for the balance phase.
   *  @param balanceSpreadThreshold begin/stop balancing when (max fill ratio - min fill ratio) crosses this
   *  @param minBalanceMoveGain     a balance move must reduce spread by at least this to be emitted
   */
  case class Config(balanceSpreadThreshold: Double = 0.05,
                    minBalanceMoveGain: Double = 0.01)

  /** Compute a stable, priority-ordered transfer plan for a level-0 device set. Pure: identical
   *  input always yields identical output, and re-running on the applied plan yields Nil. */
  def computePlan(state: State.PlanState, config: Config = Config()): List[Transfer] =
    val w = new Working(state)
    reliabilityRepair(w)
    availabilityRepair(w)
    balance(w, config)
    w.transfers.toList

  /** Mutable working copy of placement + usage. PlanState itself is never mutated. */
  private class Working(state: State.PlanState):
    val deviceTotal: Map[StorageDeviceId, Long] =
      state.devices.map((id, d) => id -> d.totalSize)
    val deviceHost: Map[StorageDeviceId, HostId] =
      state.devices.map((id, d) => id -> d.hostId)
    /** Devices in a deterministic (uuid string) order. */
    val deviceIds: Vector[StorageDeviceId] =
      state.devices.keys.toVector.sortBy(_.uuid.toString)

    val storeSize: Map[StoreId, Long] =
      state.devices.values.flatMap(_.stores.values).map(s => s.storeId -> s.currentSize).toMap
    /** Stores eligible to be selected as a move source (Active only). */
    val activeStores: Set[StoreId] =
      state.devices.values.flatMap(_.stores.values)
        .filter(_.status == StorageDeviceState.StoreStatus.Active)
        .map(_.storeId).toSet

    val location: mutable.Map[StoreId, StorageDeviceId] =
      mutable.Map.from(
        state.devices.values.flatMap(d => d.stores.keys.map(sid => sid -> d.deviceId)))
    val usage: mutable.Map[StorageDeviceId, Long] =
      mutable.Map.from(state.devices.map((id, d) => id -> d.currentUsage))

    val pinned: mutable.Set[StoreId] = mutable.Set.empty
    val transfers: mutable.ArrayBuffer[Transfer] = mutable.ArrayBuffer.empty

    def storesOn(dev: StorageDeviceId): Seq[StoreId] =
      location.collect { case (s, d) if d == dev => s }.toSeq

    def samePoolOnDevice(dev: StorageDeviceId, pool: PoolId): Int =
      location.count { case (s, d) => d == dev && s.poolId == pool }

    def samePoolOnHost(host: HostId, pool: PoolId): Int =
      location.count { case (s, d) => deviceHost(d) == host && s.poolId == pool }

    /** Fill ratio the device would have if `deltaBytes` were added to (or, if negative,
     *  removed from) its current usage. total<=0 devices report 1.0 (treated as full). */
    def fillRatioIf(dev: StorageDeviceId, deltaBytes: Long): Double =
      val tot = deviceTotal(dev)
      if tot <= 0L then 1.0 else (usage(dev) + deltaBytes).toDouble / tot.toDouble

    def fillRatio(dev: StorageDeviceId): Double = fillRatioIf(dev, 0L)

    def fits(dev: StorageDeviceId, s: StoreId): Boolean =
      usage(dev) + storeSize(s) <= deviceTotal(dev)

    /** A store may be selected as a move source if it is Active and not already moved this plan. */
    def movable(s: StoreId): Boolean = activeStores.contains(s) && !pinned.contains(s)

    def move(s: StoreId, to: StorageDeviceId): Unit =
      val from = location(s)
      usage(from) = usage(from) - storeSize(s)
      usage(to) = usage(to) + storeSize(s)
      location(s) = to
      pinned += s
      transfers += Transfer(s, from, to)

  // Phases — filled in by later tasks.
  /** Devices (other than `from`) that physically fit `s`, ranked deterministically.
   *  Priority order (ascending, lower wins): fewest same-pool on device, then on host,
   *  then lowest fill ratio, then uuid string. */
  private def rankedDestinations(w: Working, s: StoreId, from: StorageDeviceId): Seq[StorageDeviceId] =
    val pool = s.poolId
    w.deviceIds
      .filter(d => d != from && w.fits(d, s))
      .sortBy(d => (w.samePoolOnDevice(d, pool),
                    w.samePoolOnHost(w.deviceHost(d), pool),
                    w.fillRatio(d),
                    d.uuid.toString))

  private def reliabilityRepair(w: Working): Unit =
    // Terminates: each accepted move strictly reduces the max same-pool-per-device count, which is bounded.
    var progress = true
    while progress do
      progress = false
      for dev <- w.deviceIds do
        val devStores = w.storesOn(dev)
        val poolCounts: Seq[(PoolId, Int)] =
          devStores.groupBy(_.poolId).map((p, ss) => p -> ss.size)
            .toSeq.sortBy(_._1.uuid.toString)
        for (pool, count) <- poolCounts if count >= 2 do
          val candidate: Option[StoreId] =
            devStores.filter(s => s.poolId == pool && w.movable(s))
              .sortBy(_.poolIndex).headOption
          candidate.foreach { s =>
            rankedDestinations(w, s, dev).headOption.foreach { dest =>
              // accept only if it strictly reduces the max co-location of the two devices
              if w.samePoolOnDevice(dest, pool) + 1 < count then
                w.move(s, dest)
                progress = true
            }
          }

  private def availabilityRepair(w: Working): Unit =
    // Terminates: each accepted move strictly reduces the max same-pool-per-host count, which is bounded.
    var progress = true
    while progress do
      progress = false
      val hosts = w.deviceIds.map(w.deviceHost).distinct.sortBy(_.uuid.toString)
      for host <- hosts do
        val hostDevices = w.deviceIds.filter(d => w.deviceHost(d) == host)
        val hostStores = hostDevices.flatMap(w.storesOn)
        val poolCounts: Seq[(PoolId, Int)] =
          hostStores.groupBy(_.poolId).map((p, ss) => p -> ss.size)
            .toSeq.sortBy(_._1.uuid.toString)
        for (pool, count) <- poolCounts if count >= 2 do
          val candidate: Option[StoreId] =
            hostStores
              .filter(s => s.poolId == pool && w.movable(s))
              .sortBy(_.poolIndex).headOption
          candidate.foreach { s =>
            // different host, physically fits, and no device-level co-location (reliability preserved)
            val dest: Option[StorageDeviceId] =
              w.deviceIds
                .filter(d => w.deviceHost(d) != host && w.fits(d, s) && w.samePoolOnDevice(d, pool) == 0)
                .sortBy(d => (w.samePoolOnHost(w.deviceHost(d), pool),
                              w.fillRatio(d),
                              d.uuid.toString))
                .headOption
            dest.foreach { d =>
              if w.samePoolOnHost(w.deviceHost(d), pool) + 1 < count then
                w.move(s, d)
                progress = true
            }
          }
  private def balance(w: Working, config: Config): Unit =
    if w.deviceIds.size < 2 then return

    /** Max fill ratio - min fill ratio across all devices. */
    def spread(): Double =
      val ratios = w.deviceIds.map(w.fillRatio)
      ratios.max - ratios.min

    /** Spread that would result from moving `s` from `from` to `to`, without mutating state. */
    def spreadIfMoved(s: StoreId, from: StorageDeviceId, to: StorageDeviceId): Double =
      val size = w.storeSize(s)
      val ratios = w.deviceIds.map { d =>
        if d == to then w.fillRatioIf(to, size)
        else if d == from then w.fillRatioIf(from, -size)
        else w.fillRatio(d)
      }
      ratios.max - ratios.min

    def noOvershoot(s: StoreId, from: StorageDeviceId, to: StorageDeviceId): Boolean =
      val size = w.storeSize(s)
      val sinkAfter = w.fillRatioIf(to, size)
      val sourceAfter = w.fillRatioIf(from, -size)
      sinkAfter <= sourceAfter

    var continue = spread() > config.balanceSpreadThreshold
    while continue do
      continue = false
      val byFill = w.deviceIds.sortBy(w.fillRatio)
      val sink = byFill.head
      val source = byFill.last
      val before = spread()
      // largest movable store on the source first; poolIndex as deterministic tiebreak
      val candidates = w.storesOn(source).filter(w.movable)
        .sortBy(s => (-w.storeSize(s), s.poolIndex))
      val chosen = candidates.find { s =>
        val pool = s.poolId
        w.fits(sink, s) &&
        w.samePoolOnDevice(sink, pool) == 0 &&
        (w.deviceHost(sink) == w.deviceHost(source) || w.samePoolOnHost(w.deviceHost(sink), pool) == 0) &&
        noOvershoot(s, source, sink) &&
        (before - spreadIfMoved(s, source, sink)) >= config.minBalanceMoveGain
      }
      chosen.foreach { s =>
        w.move(s, sink)
        continue = spread() > config.balanceSpreadThreshold
      }
