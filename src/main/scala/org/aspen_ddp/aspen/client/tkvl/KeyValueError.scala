package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.AmoebaError
import org.aspen_ddp.aspen.common.objects.Key

sealed abstract class KeyValueError extends AmoebaError

class BelowMinimumError(minimum: Key, attempted: Key) extends KeyValueError

class OutOfRange extends KeyValueError

class CorruptedLinkedList extends KeyValueError

/** Thrown when a single split is insufficient to insert all requested content */
class NodeSizeExceeded extends KeyValueError

class KeyDoesNotExist(val key: Key) extends KeyValueError

class InvalidRoot extends KeyValueError

class TierAlreadyCreated extends KeyValueError

class InvalidConfiguration extends KeyValueError

class EmptyTree extends KeyValueError

class BrokenTree extends KeyValueError

