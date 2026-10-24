package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

/** The shared admission rule for starting store transfers, used by every initiator.
 *
 *  A transfer may start only if, after the store being moved goes offline, the pool still has at
 *  least `floor(ida)` online stores. Anything that is not Active is considered offline, so stores
 *  already TransferringOut, Rebuilding, etc. (and stores on offline hosts) all count against the
 *  pool and concurrent transfers in one pool add up correctly.
 *
 *  KNOWN RACE (accepted by design): the check and the start are NOT atomic. `transferStore`
 *  revision-checks only the source and destination device objects, not the other stores of the
 *  pool, so two initiators (rebalance and migration, or two tasks) can both pass `isSafe` against
 *  the same state and together take the pool below the floor. The mitigation is that
 *  `TransferStarter` re-reads state immediately before each start, verifies after the start
 *  commits with `isBelowFloor` (the same floor computation as admission, never the raw write
 *  threshold), and if the pool is below the floor rolls back the transfer it just started and
 *  returns `RolledBack`; its callers then retry with backoff (`TransferBackoff`). The pool is
 *  therefore only ever below the floor very briefly. Do not
 *  treat `isSafe` as a guarantee at the instant of the start. */
object TransferSafety:

  /** Minimum number of online stores a pool must keep while transfers are in progress:
   *  `min(writeThreshold + 2, width - 1)`, so at least one store can always be moved. */
  def floor(ida: IDA): Int =
    math.min(ida.writeThreshold + 2, ida.width - 1)

  /** storeId -> the host currently owning it (via its device). */
  private def storeHosts(state: State.PlanningState): Map[StoreId, HostId] =
    state.devices.values.flatMap(d => d.stores.keys.map(sid => sid -> d.hostId)).toMap

  /** Number of the pool's stores that are Active and whose owning host is not offline.
   *  Anything that is not Active is considered offline. Unknown pools count as zero. */
  def onlineCount(state: State.PlanningState,
                  offlineHosts: Set[HostId],
                  poolId: PoolId): Int =
    val hosts = storeHosts(state)
    state.pools.get(poolId).map: pool =>
      pool.stores.values.count: s =>
        s.status == StorageDeviceState.StoreStatus.Active &&
          hosts.get(s.storeId).exists(h => !offlineHosts.contains(h))
    .getOrElse(0)

  /** May this transfer start now? See the race note on the object. */
  def isSafe(state: State.PlanningState,
             offlineHosts: Set[HostId],
             transfer: Plan.Transfer): Boolean =
    val hosts = storeHosts(state)
    state.devices.get(transfer.toDevice).exists: toDev =>
      !offlineHosts.contains(toDev.hostId) &&
        state.pools.get(transfer.storeId.poolId).exists: pool =>
          val candidateOnline = pool.stores.get(transfer.storeId).exists: s =>
            s.status == StorageDeviceState.StoreStatus.Active &&
              hosts.get(s.storeId).exists(h => !offlineHosts.contains(h))
          candidateOnline &&
            onlineCount(state, offlineHosts, pool.poolId) - 1 >= floor(pool.ida)

  /** Post-start verification: is the pool now below the floor? */
  def isBelowFloor(state: State.PlanningState,
                   offlineHosts: Set[HostId],
                   poolId: PoolId): Boolean =
    state.pools.get(poolId).exists: pool =>
      onlineCount(state, offlineHosts, poolId) < floor(pool.ida)
