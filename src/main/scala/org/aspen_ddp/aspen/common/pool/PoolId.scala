package org.aspen_ddp.aspen.common.pool

import java.util.UUID

import org.aspen_ddp.aspen.common.metadata.fixed_ids

case class PoolId(uuid: UUID) extends AnyVal

object PoolId:
  val BootstrapPoolName = "aspen-bootstrap"
  val BootstrapPoolId: PoolId = fixed_ids.BootstrapPoolId
