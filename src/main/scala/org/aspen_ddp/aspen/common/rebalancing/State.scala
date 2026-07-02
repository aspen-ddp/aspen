package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetState, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.Future

object State:
  case class Store(storeId: StoreId, currentSize: Long, status: StorageDeviceState.StoreStatus)
  case class Pool(poolId: PoolId, ida: IDA, stores: Map[StoreId, Store])
  case class Device(deviceId: StorageDeviceId, stores: Map[StoreId, Store])

  case class PlanState(devices: Map[StorageDeviceId, Device], pools: Map[PoolId, Pool])

  def getStateForRebalancePlanning(client: AspenClient,
                                   storageDeviceSet: StorageDeviceSetState): Future[PlanState] = ???
