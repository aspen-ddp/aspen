package org.aspen_ddp.aspen.common.ida

import org.aspen_ddp.aspen.AspenError

sealed abstract class IDAError extends AspenError

/** Thrown when an unknown IDA type is found embedded within a serialized ObjectPointer */
class IDAEncodingError extends IDAError

class IDARestoreError extends IDAError

class IDANotSupportedError extends IDAError
