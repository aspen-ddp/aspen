package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.AspenError

case class StopRetrying(reason: Throwable) extends AspenError("StopRetrying", reason)
