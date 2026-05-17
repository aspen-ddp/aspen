package org.aspen_ddp.aspen.client

import java.util.UUID
import org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StoragePoolState}
import org.aspen_ddp.aspen.server.store.backend.BackendConfig

trait StoragePool:

  val poolId: PoolId

  val name: String

  val numberOfStores: Int

  val maxObjectSize: Option[Int]

  val defaultIDA: IDA

  val stores: Array[StoragePoolState.StoreEntry]
  
  val backendConfig: BackendConfig

  def supportsIDA(ida: IDA): Boolean

  def createAllocator(ida: IDA): ObjectAllocator
  
  def defaultAllocator: ObjectAllocator = createAllocator(defaultIDA)

  /** Throws AllocationError: UnsupportedIDA if the IDA is not supported*/
  private[client] def selectStoresForAllocation(ida: IDA): Array[Int]

  def allocationTree: TieredKeyValueList

  def errorTree: TieredKeyValueList

