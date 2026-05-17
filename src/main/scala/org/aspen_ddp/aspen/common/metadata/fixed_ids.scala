package org.aspen_ddp.aspen.common.metadata

import java.util.UUID

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.pool.PoolId

object fixed_ids:

  val RadicleObjectId: ObjectId = new ObjectId(new UUID(0, 0))
  val BootstrapPoolId: PoolId = new PoolId(new UUID(0, 1))
  val BootstrapStorageDeviceId: StorageDeviceId = new StorageDeviceId(new UUID(0, 2))
  val BootstrapHostId: HostId = new HostId(new UUID(0, 3))
