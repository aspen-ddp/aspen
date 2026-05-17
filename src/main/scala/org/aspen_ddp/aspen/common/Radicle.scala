package org.aspen_ddp.aspen.common

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.metadata.fixed_ids

object Radicle:
  val objectId: ObjectId = fixed_ids.RadicleObjectId

  val objectType: ObjectType.Value = ObjectType.KeyValue

  val poolId: PoolId = PoolId.BootstrapPoolId

  private[aspen] val SystemIdKey = Key(Array[Byte](0))
  private[aspen] val BootstrapConfigKey = Key(Array[Byte](1))
  private[aspen] val ObjectRegistryKey = Key(Array[Byte](2))
  private[aspen] val NamespacedRegistryKey = Key(Array[Byte](3))


