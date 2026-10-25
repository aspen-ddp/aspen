package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.AspenError
import org.aspen_ddp.aspen.common.objects.{Key, ObjectPointer}

sealed abstract class KeyValueError(cause: Throwable = null) extends AspenError(cause = cause)

class BelowMinimumError(minimum: Key, attempted: Key) extends KeyValueError

class OutOfRange extends KeyValueError

class CorruptedLinkedList extends KeyValueError

/** Thrown when a single split is insufficient to insert all requested content */
class NodeSizeExceeded extends KeyValueError

/** A TKVL tier is configured with a `maxNodeSize` larger than its allocator can hold. `limit` is the largest
  * node size allowed for `tier` (see `NodeAllocator.nodeSizeLimit`). Raised rather than clamping the node size.
  */
final case class NodeSizeExceedsAllocatorLimit(tier: Int, requested: Int, limit: Int) extends KeyValueError:
  override def getMessage: String =
    s"TKVL tier $tier has maxNodeSize $requested, above its allocator's limit of $limit bytes"

class KeyDoesNotExist(val key: Key) extends KeyValueError

/** `cause` carries the underlying decode failure when the root could not be read out of an
  * inode. Without it the original traceback is lost entirely. */
class InvalidRoot(cause: Throwable = null) extends KeyValueError(cause)

class TierAlreadyCreated extends KeyValueError

class InvalidConfiguration extends KeyValueError

class EmptyTree extends KeyValueError

class BrokenTree extends KeyValueError

/** The tree's node encoding cannot enforce the requested insert requirement (see `NodeEncoding.insert`) */
class UnsupportedRequirement(val kind: TreeKind) extends KeyValueError:
  override def getMessage: String = s"Insert requirements are not supported by $kind trees"

/** A DataObject-tree update would modify `pointer`, a node the same transaction already modifies. DataObject nodes
  * are rewritten whole, so one transaction can change a node only once.
  */
final case class NodeAlreadyModified(pointer: ObjectPointer) extends KeyValueError:
  override def getMessage: String = s"TKVL node ${pointer.id} is already modified in this transaction"
