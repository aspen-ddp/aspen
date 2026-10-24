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
  private [aspen] val RepairPolicyKey = Key(Array[Byte](5))

  val BootstrapPoolId: PoolId = fixed_ids.BootstrapPoolId

  case class StoreEntry(hostId: HostId, storageDeviceId: StorageDeviceId)

  /** State of a pool-to-set migration. `Complete` persists rather than reverting to `None`,
   *  so a pool records its last migration. */
  enum MigrationStatus:
    case InProgress, Complete

  case class Migration(targetSet: StorageDeviceSetId, status: MigrationStatus)

  def getStoreUsageKey(poolIndex: Byte): Key = Key(s"sz${poolIndex}")

  def apply(cfg: Array[Byte]): StoragePoolState = Codec.decode(codec.StoragePoolState.parseFrom(cfg))

  def apply(kvos: KeyValueObjectState): StoragePoolState = StoragePoolState(kvos.contents(ConfigKey).value.bytes)


/** @param maxSliceSize the largest slice, in bytes, that any store of this pool will hold for one object, or
  *                     `None` for no limit. Fixed when the pool is created. Most code should use
  *                     `maxObjectSize` instead, which converts this into the largest object the pool accepts.
  */
final case class StoragePoolState(
                                   poolId: PoolId,
                                   name: String,
                                   ida: IDA,
                                   maxSliceSize: Option[Int],
                                   stores: Array[StoragePoolState.StoreEntry],
                                   backendConfig: BackendConfig,
                                   storageDeviceSet: StorageDeviceSetId,
                                   currentUsage: Long = 0,
                                   maximumStoreSize: Long = 0,
                                   allocationGroups: List[UUID] = Nil,
                                   migration: Option[StoragePoolState.Migration] = None
                                 ):

  def encode(): Array[Byte] = Codec.encode(this).toByteArray

  /** Largest object, in bytes, this pool accepts: `maxSliceSize` converted through `ida.maxObjectSize`, so an object
    * of exactly this size encodes to slices no larger than `maxSliceSize`. `None` when the pool has no slice limit.
    */
  def maxObjectSize: Option[Int] = maxSliceSize.map(ida.maxObjectSize(_))

  /** Every field, with `stores` as a Seq so it compares by content */
  private def contentFields = (poolId, name, ida, maxSliceSize, stores.toSeq, backendConfig, storageDeviceSet,
    currentUsage, maximumStoreSize, allocationGroups, migration)

  override def equals(other: Any): Boolean = other match
    case that: StoragePoolState => contentFields == that.contentFields
    case _ => false

  override def hashCode: Int = contentFields.##

  
    

