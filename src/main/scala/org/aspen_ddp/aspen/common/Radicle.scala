package org.aspen_ddp.aspen.common

import java.util.UUID

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId

object Radicle {
  val objectId: ObjectId = ObjectId(new UUID(0, 0))

  val objectType: ObjectType.Value = ObjectType.KeyValue

  val poolId: PoolId = PoolId(new UUID(0, 0))

  private[aspen] val PoolTreeKey = Key(Array[Byte](0))
  private[aspen] val HostsTreeKey = Key(Array[Byte](1))
  private[aspen] val PoolNameTreeKey = Key(Array[Byte](3))
  private[aspen] val HostsNameTreeKey = Key(Array[Byte](4))
}
