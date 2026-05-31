package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.KeyValueObjectState
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.Key
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.server.store.backend.BackendConfig

import java.util.UUID

object StoragePoolState:
  private [aspen] val ConfigKey = Key(Array[Byte](0))
  private [aspen] val ErrorTreeKey = Key(Array[Byte](1))
  private [aspen] val AllocationTreeKey = Key(Array[Byte](2))
  private [aspen] val AllocationStrategyKey = Key(Array[Byte](3))
  private [aspen] val AllocationStrategyConfigKey = Key(Array[Byte](4))

  val BootstrapPoolId: PoolId = fixed_ids.BootstrapPoolId

  case class StoreEntry(hostId: HostId, storageDeviceId: StorageDeviceId)

  def getStoreUsageKey(poolIndex: Byte): Key = Key(s"sz${poolIndex}")

  def apply(cfg: Array[Byte]): StoragePoolState = Codec.decode(codec.StoragePoolState.parseFrom(cfg))

  def apply(kvos: KeyValueObjectState): StoragePoolState = StoragePoolState(kvos.contents(ConfigKey).value.bytes)


final case class StoragePoolState(
                                   poolId: PoolId,
                                   name: String,
                                   ida: IDA,
                                   maxObjectSize: Option[Int],
                                   stores: Array[StoragePoolState.StoreEntry],
                                   backendConfig: BackendConfig,
                                   currentUsage: Long = 0,
                                   maximumStoreSize: Long = 0,
                                   allocationGroups: List[UUID] = Nil
                                 ):

  def encode(): Array[Byte] = Codec.encode(this).toByteArray

  
    

