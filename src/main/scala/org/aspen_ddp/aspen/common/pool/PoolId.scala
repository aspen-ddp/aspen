package org.aspen_ddp.aspen.common.pool

import java.util.UUID

case class PoolId(uuid: UUID) extends AnyVal

object PoolId:
  val BootstrapPoolId = PoolId(new UUID(0, 0))
