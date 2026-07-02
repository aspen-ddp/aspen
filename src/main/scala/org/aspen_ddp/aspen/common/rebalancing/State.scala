package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
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
                    currentUsage: Long,
                    totalSize: Long,
                    stores: Map[StoreId, Store])

  case class PlanState(devices: Map[StorageDeviceId, Device], pools: Map[PoolId, Pool])

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
                                   maxConcurrentReads: Int = DefaultMaxConcurrentReads): Future[PlanState] =
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

        def storeSize(storeId: StoreId): Long =
          poolKvos.get(storeId.poolId)
            .flatMap(_.contents.get(StoragePoolState.getStoreUsageKey(storeId.poolIndex)))
            .map(vs => byte2long(vs.value.bytes))
            .getOrElse(0L)

        // One Store per owned store id, shared between the device and pool views.
        val storesById: Map[StoreId, Store] =
          deviceStates.flatMap: ds =>
            ownedStores(ds).map((sid, entry) => sid -> Store(sid, storeSize(sid), entry.status))
          .toMap

        val devices: Map[StorageDeviceId, Device] =
          deviceStates.map: ds =>
            val itsStores = ownedStores(ds).keys.map(sid => sid -> storesById(sid)).toMap
            ds.storageDeviceId -> Device(ds.storageDeviceId, ds.currentUsage, ds.totalSize, itsStores)
          .toMap

        val pools: Map[PoolId, Pool] =
          poolIds.map: poolId =>
            val ida = StoragePoolState(poolKvos(poolId)).ida
            val poolStores = storesById.filter((sid, _) => sid.poolId == poolId)
            poolId -> Pool(poolId, ida, poolStores)
          .toMap

        PlanState(devices, pools)
