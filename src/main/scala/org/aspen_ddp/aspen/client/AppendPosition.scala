package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRevision}

/** The state of a data object at which an append is valid: the object's revision and its logical length.
  *
  * Callers cannot construct one directly. Positions come from reading the object's append position or from
  * the return value of Transaction.append.
  */
final case class AppendPosition private[aspen] (
  pointer: DataObjectPointer,
  revision: ObjectRevision,
  length: Int)
