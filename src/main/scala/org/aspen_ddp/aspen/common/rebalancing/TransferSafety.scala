package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceState}
import org.aspen_ddp.aspen.common.store.StoreId

/** Pure predicate: may a planned transfer be initiated right now without dropping its pool
 *  below the write threshold, and with an online destination host? Depends only on a
 *  PlanningState snapshot and the set of currently-offline hosts, so it is unit-testable. */
object TransferSafety:

  /** storeId -> the host currently owning it (via its device). */
  private def storeHosts(state: State.PlanningState): Map[StoreId, HostId] =
    state.devices.values.flatMap(d => d.stores.keys.map(sid => sid -> d.hostId)).toMap

  def isSafe(state: State.PlanningState,
             offlineHosts: Set[HostId],
             transfer: Plan.Transfer): Boolean =
    val hosts = storeHosts(state)
    state.devices.get(transfer.toDevice).exists: toDev =>
      !offlineHosts.contains(toDev.hostId) &&
        state.pools.get(transfer.storeId.poolId).exists: pool =>
          val usable = pool.stores.values.count: s =>
            s.status == StorageDeviceState.StoreStatus.Active &&
              hosts.get(s.storeId).exists(h => !offlineHosts.contains(h))
          // The store being moved goes TransferringOut (momentarily unavailable for writes),
          // so require the remaining usable count to still satisfy the write threshold.
          usable - 1 >= pool.ida.writeThreshold
