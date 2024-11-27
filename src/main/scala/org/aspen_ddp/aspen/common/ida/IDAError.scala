package org.aspen_ddp.aspen.common.ida

import org.aspen_ddp.aspen.AmoebaError

sealed abstract class IDAError extends AmoebaError

/** Thrown when an unknown IDA type is found embedded within a serialized ObjectPointer */
class IDAEncodingError extends IDAError

class IDARestoreError extends IDAError

class IDANotSupportedError extends IDAError
