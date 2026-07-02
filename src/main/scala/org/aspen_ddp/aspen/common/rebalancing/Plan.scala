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

    def fillRatio(dev: StorageDeviceId): Double =
      val tot = deviceTotal(dev)
      if tot <= 0L then 1.0 else usage(dev).toDouble / tot.toDouble

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
  private def reliabilityRepair(w: Working): Unit = ()
  private def availabilityRepair(w: Working): Unit = ()
  private def balance(w: Working, config: Config): Unit = ()
