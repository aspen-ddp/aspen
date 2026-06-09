package org.aspen_ddp.aspen.common

import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.metadata.fixed_ids

object Radicle:
  val objectId: ObjectId = fixed_ids.RadicleObjectId

  val objectType: ObjectType.Value = ObjectType.KeyValue

  val poolId: PoolId = PoolId.BootstrapPoolId

  val pointer: KeyValueObjectPointer = KeyValueObjectPointer(objectId, poolId)

  private[aspen] val SystemIdKey = Key(Array[Byte](0))
  private[aspen] val BootstrapConfigKey = Key(Array[Byte](1))
  private[aspen] val ObjectRegistryKey = Key(Array[Byte](2))
  private[aspen] val NamespacedRegistryKey = Key(Array[Byte](3))

  private[aspen] val StoragePoolsTreeKey = Key(Array[Byte](4))
  private[aspen] val AllocationGroupsTreeKey = Key(Array[Byte](5))
  private[aspen] val HostsTreeKey = Key(Array[Byte](6))
  private[aspen] val StorageDevicesTreeKey = Key(Array[Byte](7))
  private[aspen] val StorageDeviceSetsTreeKey = Key(Array[Byte](8))


