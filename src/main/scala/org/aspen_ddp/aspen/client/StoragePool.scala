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

  val maxObjectSize: Option[Int]

  val ida: IDA

  val stores: Array[StoragePoolState.StoreEntry]

  val backendConfig: BackendConfig

  def createAllocator: ObjectAllocator

  def allocationTree: TieredKeyValueList

  def errorTree: TieredKeyValueList
