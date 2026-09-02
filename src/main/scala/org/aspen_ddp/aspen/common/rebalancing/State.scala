package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{byte2long, runBoundedParallel}

import scala.concurrent.{ExecutionContext, Future}

object State:
  /** Default cap on the number of concurrent metadata reads issued while gathering planning state.
   *  Bounds load on large systems with many devices and pools. */
  val DefaultMaxConcurrentReads: Int = 20

  case class Store(storeId: StoreId, currentSize: Long, status: StorageDeviceState.StoreStatus)
  case class Pool(poolId: PoolId, ida: IDA, stores: Map[StoreId, Store])
  /** `currentUsage` and `totalSize` are the device's own self-reported usage and capacity
   *  (from StorageDeviceState), authoritative for the device's overall capacity. The per-store
   *  sizes in `stores` (each Store.currentSize) come from the pool's separate per-store usage
   *  accounting, so they need not sum to `currentUsage`. */
  case class Device(deviceId: StorageDeviceId,
                    hostId: HostId,
                    currentUsage: Long,
                    totalSize: Long,
                    stores: Map[StoreId, Store])

  case class PlanningState(devices: Map[StorageDeviceId, Device], pools: Map[PoolId, Pool])

  /** Gather the device and pool state needed to compute a rebalancing plan for a level-0
   *  (leaf) storage device set.
   *
   *  Only level-0 sets are supported; a higher-level set fails with IllegalArgumentException.
   *  An empty level-0 set yields an empty PlanState.
   *
   *  Reads are issued with at most `maxConcurrentReads` in flight at a time (see runBoundedParallel)
   *  to bound load on large systems.
   */
  def getStateForRebalancePlanning(client: AspenClient,
                                   storageDeviceSet: StorageDeviceSetState,
                                   maxConcurrentReads: Int = DefaultMaxConcurrentReads): Future[PlanningState] =
    given ExecutionContext = client.clientContext

    if storageDeviceSet.level != 0 then
      Future.failed(new IllegalArgumentException(
        s"getStateForRebalancePlanning only supports level-0 sets; set " +
          s"${storageDeviceSet.setId.uuid} is level ${storageDeviceSet.level}"))
    else
      // A store being transferred appears as TransferringOut on its source device and
      // TransferringIn on its destination. The source device owns the store until the transfer
      // completes (at which point the source's state drops the entry entirely), so we ignore
      // TransferringIn entries. This attributes each store to exactly one device and removes any
      // possibility of a duplicate StoreId key.
      def ownedStores(ds: StorageDeviceState): Map[StoreId, StorageDeviceState.StoreEntry] =
        ds.stores.filter((_, entry) => entry.status != StorageDeviceState.StoreStatus.TransferringIn)

      for
        deviceStates <- runBoundedParallel(storageDeviceSet.memberDevices, maxConcurrentReads)(
                          client.getStorageDeviceState)

        poolIds = deviceStates.flatMap(ds => ownedStores(ds).keys).map(_.poolId).toSet

        poolPairs <- runBoundedParallel(poolIds.toSeq, maxConcurrentReads): poolId =>
                       client.getStoragePoolPointer(poolId)
                         .flatMap(pointer => client.read(pointer, "rebalance planning"))
                         .map(kvos => poolId -> kvos)
      yield
        val poolKvos = poolPairs.toMap

        // A pool with a migration in progress is owned by MigratePoolToSetDurableTask for the
        // duration; the rebalancer must not plan moves for its stores. Omitting it from `pools`
        // and its stores from every device makes Plan.computePlan skip them across all three
        // phases with no changes of its own. Device currentUsage/totalSize are self-reported by
        // StorageDeviceState rather than summed from stores, so fill ratios stay accurate.
        val migratingPools: Set[PoolId] =
          poolKvos.collect:
            case (poolId, kvos)
              if StoragePoolState(kvos).migration.exists(
                   _.status == StoragePoolState.MigrationStatus.InProgress) => poolId
          .toSet

        val plannedPoolIds = poolIds -- migratingPools

        def storeSize(storeId: StoreId): Long =
          poolKvos.get(storeId.poolId)
            .flatMap(_.contents.get(StoragePoolState.getStoreUsageKey(storeId.poolIndex)))
            .map(vs => byte2long(vs.value.bytes))
            .getOrElse(0L)

        // One Store per owned store id, shared between the device and pool views.
        val storesById: Map[StoreId, Store] =
          deviceStates.flatMap: ds =>
            ownedStores(ds)
              .filterNot((sid, _) => migratingPools.contains(sid.poolId))
              .map((sid, entry) => sid -> Store(sid, storeSize(sid), entry.status))
          .toMap

        // Keyed by the id inside the device's own state, which a tombstone zeroes -- so two
        // tombstoned devices in one set would collide on UUID(0,0) and .toMap would silently
        // drop one. Unreachable: a tombstone is a member of no set. Step 1 of
        // FailedStorageDeviceDurableTask removes it from the one it was in, and every path that
        // could put one back refuses -- StorageDeviceSetState.moveDevice,
        // AspenClient.createNewStoragePool, AspenClient.transferStore, and
        // BaseAspenClient.createStoragePool's stageDeviceUpdate all throw DeviceFailed on a
        // tombstoned device.
        val devices: Map[StorageDeviceId, Device] =
          deviceStates.map: ds =>
            val itsStores = ownedStores(ds).keys
              .filter(storesById.contains)
              .map(sid => sid -> storesById(sid)).toMap
            ds.storageDeviceId -> Device(ds.storageDeviceId, ds.hostId, ds.currentUsage, ds.totalSize, itsStores)
          .toMap

        val pools: Map[PoolId, Pool] =
          plannedPoolIds.map: poolId =>
            val ida = StoragePoolState(poolKvos(poolId)).ida
            val poolStores = storesById.filter((sid, _) => sid.poolId == poolId)
            poolId -> Pool(poolId, ida, poolStores)
          .toMap

        PlanningState(devices, pools)

  /** Gather the planning state needed to decide whether one store of `poolId` may be moved to
   *  `destinationDevice` right now. Holds exactly one pool and the devices that matter: the
   *  pool's own store devices plus the candidate destination.
   *
   *  This is the level-agnostic counterpart to `getStateForRebalancePlanning`, which is
   *  level-0-only. `TransferSafety.isSafe` needs only `devices.get(toDevice)` for the
   *  offline-host check and `pools.get(poolId)` for the write-threshold count; it never
   *  touches set membership, so this works for a target set at any level.
   *
   *  No `TransferringIn` filter is needed here. Each store's device of record comes from
   *  `poolState.stores`, which StoreManager rewrites atomically at transfer completion, so
   *  every store maps to exactly one device by construction.
   */
  def getStateForPoolMigration(client: AspenClient,
                               poolId: PoolId,
                               destinationDevice: StorageDeviceId,
                               maxConcurrentReads: Int = DefaultMaxConcurrentReads): Future[PlanningState] =
    given ExecutionContext = client.clientContext

    for
      poolPtr <- client.getStoragePoolPointer(poolId)
      poolKvos <- client.read(poolPtr, "pool migration planning")
      poolState = StoragePoolState(poolKvos)
      deviceIds = (poolState.stores.map(_.storageDeviceId).toSet + destinationDevice).toSeq
      deviceStates <- runBoundedParallel(deviceIds, maxConcurrentReads)(client.getStorageDeviceState)
    yield
      val stateByDevice = deviceStates.map(ds => ds.storageDeviceId -> ds).toMap

      val deviceOfStore: Map[StoreId, StorageDeviceId] =
        poolState.stores.indices
          .map(i => StoreId(poolId, i.toByte) -> poolState.stores(i).storageDeviceId)
          .toMap

      def storeSize(storeId: StoreId): Long =
        poolKvos.contents
          .get(StoragePoolState.getStoreUsageKey(storeId.poolIndex))
          .map(vs => byte2long(vs.value.bytes))
          .getOrElse(0L)

      // Status comes from the device of record, not from whichever device happens to list the
      // store: mid-transfer the source says TransferringOut and the destination says
      // TransferringIn, and the source is the one that still owns it. A store its device does
      // not list yet reads as Initializing, which TransferSafety does not count as usable --
      // the conservative direction.
      def statusOf(storeId: StoreId): StorageDeviceState.StoreStatus =
        stateByDevice.get(deviceOfStore(storeId))
          .flatMap(_.stores.get(storeId))
          .map(_.status)
          .getOrElse(StorageDeviceState.StoreStatus.Initializing)

      val storesById: Map[StoreId, Store] =
        deviceOfStore.keys.map(sid => sid -> Store(sid, storeSize(sid), statusOf(sid))).toMap

      val devices: Map[StorageDeviceId, Device] =
        deviceStates.map: ds =>
          val itsStores = storesById.filter((sid, _) => deviceOfStore(sid) == ds.storageDeviceId)
          ds.storageDeviceId -> Device(ds.storageDeviceId, ds.hostId, ds.currentUsage, ds.totalSize, itsStores)
        .toMap

      PlanningState(devices, Map(poolId -> Pool(poolId, poolState.ida, storesById)))
