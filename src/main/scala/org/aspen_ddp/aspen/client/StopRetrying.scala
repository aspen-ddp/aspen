package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.AmoebaError

case class StopRetrying(reason: Throwable) extends AmoebaError("StopRetrying", reason)
