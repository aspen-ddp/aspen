package org.aspen_ddp.aspen

abstract class AspenError(message:String = null, cause: Throwable = null) extends Exception(message, cause)
