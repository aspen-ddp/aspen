package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.DataObjectState
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.pool.PoolId

import java.util.UUID

final case class StorageDeviceSetId(uuid: UUID) extends AnyVal

object StorageDeviceSetId:
  val BootstrapStorageDeviceSetId: StorageDeviceSetId = fixed_ids.BootstrapStorageDeviceSetId

object StorageDeviceSetState:
  def apply(cfg: Array[Byte]): StorageDeviceSetState =
    Codec.decode(codec.StorageDeviceSetState.parseFrom(cfg))

  def apply(dos: DataObjectState): StorageDeviceSetState = apply(dos.data.getByteArray)

/** Hierarchical grouping of physical storage, parallel to AllocationGroupState.
 *
 *  The `level` strictly determines the member type:
 *    - level == 0 : a set of devices. `memberDevices` may be non-empty; `memberSets` must be empty.
 *    - level >= 1 : a set of sets.   `memberSets` may be non-empty; `memberDevices` must be empty.
 *
 *  `parent`, if present, must reference a set of a higher level. These invariants are
 *  documented here but enforcement (the mutation/management API) is out of scope for this pass.
 */
final case class StorageDeviceSetState(
    setId: StorageDeviceSetId,
    name: String,
    level: Int,
    parent: Option[StorageDeviceSetId],
    memberDevices: List[StorageDeviceId],
    memberSets: List[StorageDeviceSetId],
    assignedPools: List[PoolId]
):
  def toBytes: Array[Byte] = Codec.encode(this).toByteArray
