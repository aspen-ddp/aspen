package org.aspen_ddp.aspen.common.metadata

import java.util.UUID

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.pool.PoolId

object fixed_ids:

  val RadicleObjectId: ObjectId = new ObjectId(new UUID(0, 0))
  val BootstrapPoolId: PoolId = new PoolId(new UUID(0, 1))
  val BootstrapStorageDeviceId: StorageDeviceId = new StorageDeviceId(new UUID(0, 2))
  val BootstrapHostId: HostId = new HostId(new UUID(0, 3))
  val BootstrapStorageDeviceSetId: StorageDeviceSetId = new StorageDeviceSetId(new UUID(0, 4))

  /** A tombstoned storage device: an operator has declared it dead. Both ids are zeroed so a
   *  half-applied tombstone still reads as failed. Slot 0 is free for both types --
   *  BootstrapStorageDeviceId is UUID(0, 2) and BootstrapHostId is UUID(0, 3). */
  val FailedHostId: HostId = new HostId(new UUID(0, 0))
  val FailedStorageDeviceId: StorageDeviceId = new StorageDeviceId(new UUID(0, 0))
