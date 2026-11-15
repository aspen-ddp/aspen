package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.AspenError
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.ObjectPointer

sealed abstract class ReadError(msg: String) extends AspenError(msg) {
  val pointer: ObjectPointer

  override def toString: String = s"${this.getClass.getSimpleName}(${pointer.getClass.getSimpleName}:${pointer.id})"
}


sealed abstract class FatalReadError(msg: String) extends ReadError(msg)

/** Thrown when the requested object is not found.
  *
  * @param provesDeletion the replies prove the object is not live: a consistent majority restored it at refcount 0,
  *                       or more than `width - writeThreshold` stores answered that they do not hold it. A committed
  *                       live object sits on at least writeThreshold stores, so neither can come from one. False when
  *                       the read gave up on other errors (StoreNotFound, say), which say nothing of the object
  */
class InvalidObject(val pointer: ObjectPointer, val provesDeletion: Boolean = false)
  extends FatalReadError("InvalidObject")

/** Thrown when the object content becomes corrupted. */
class CorruptedObject(val pointer: ObjectPointer) extends FatalReadError("CorruptedObject")

/** Thrown when IDA decoding fails */
class CorruptedIDA(pointer: ObjectPointer) extends CorruptedObject(pointer)

/** Thrown when a byte-range read is attempted on an object whose IDA does not support them */
class RangeReadNotSupported(val pointer: ObjectPointer, val ida: IDA) extends FatalReadError("RangeReadNotSupported")

/** Thrown when the content of a complex object such as a KeyValue object becomes corrupted
  *  and cannot be decoded.
  */
class CorruptedContent(pointer: ObjectPointer) extends CorruptedObject(pointer)

sealed abstract class TransientReadError(msg: String) extends ReadError(msg)

class ReadTimeout(val pointer: ObjectPointer) extends TransientReadError("ReadTimeout")
