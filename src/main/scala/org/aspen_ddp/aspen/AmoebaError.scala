package org.aspen_ddp.aspen

abstract class AmoebaError(message:String = null, cause: Throwable = null) extends Exception(message, cause)
