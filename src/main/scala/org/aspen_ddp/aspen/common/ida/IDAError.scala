package org.aspen_ddp.aspen.common.ida

import org.aspen_ddp.aspen.AspenError

sealed abstract class IDAError(msg: String = null) extends AspenError(msg)

/** Thrown when an unknown IDA type is found embedded within a serialized ObjectPointer */
class IDAEncodingError(msg: String = null) extends IDAError(msg)

class IDARestoreError(msg: String = null) extends IDAError(msg)

class IDANotSupportedError(msg: String = null) extends IDAError(msg)
