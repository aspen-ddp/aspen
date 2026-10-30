package org.aspen_ddp.aspen.common.allocation_group

import java.util.UUID

import org.aspen_ddp.aspen.common.metadata.fixed_ids

case class AllocationGroupId(uuid: UUID) extends AnyVal

object AllocationGroupId:
  val DefaultMetadataGroupName = "aspen-default-storage-pool-metadata"
  val DefaultMetadataGroupId: AllocationGroupId = fixed_ids.DefaultMetadataGroupId
