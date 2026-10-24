package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.{AppendPosition, DataObjectRangeState, RangedWrite, RangedWriteState}
import org.aspen_ddp.aspen.common.objects.DataObjectPointer

/** Builds a RangedWriteState from one length read and one range read per write, all taken from a single
  * ByteRanges read and therefore from a single revision of the object.
  */
private[aspen] object RangedWriteFetch:

  /** Combines one round of reads. `reads` holds the result of `readRange(w.offset, Some(w.data.length))` for
    * each write, in the same order. Returns None when the reads disagree on the object's revision.
    *
    * The length read is authoritative: each piece is clipped to it, and a write starting at or past the end
    * contributes no piece. `oldBytes` clips to the length the same way, so the state holds every byte that
    * `rangedWrites` will ask for. Throws IllegalStateException if a read is too short
    * to cover its write.
    */
  def combine(pointer: DataObjectPointer,
              writes: List[RangedWrite],
              position: AppendPosition,
              reads: List[DataObjectRangeState]): RangedWriteState =
    require(writes.length == reads.length, s"${writes.length} writes but ${reads.length} range reads")
    val revision = position.revision
    val length = position.length
    val pieces = writes.zip(reads).flatMap: (w, r) =>
      if r.offset != w.offset then
        throw new IllegalStateException(s"Range read at ${r.offset} does not match the write at ${w.offset}")
      val needed = math.max(0L, math.min(w.end, length.toLong) - w.offset).toInt
      val bytes = r.data.getByteArray
      if bytes.length < needed then
        throw new IllegalStateException(
          s"Range read at ${w.offset} of object ${pointer.id} returned ${bytes.length} bytes. Needed $needed")
      if needed == 0 then
        None
      else
        Some((w.offset, if bytes.length == needed then bytes else bytes.take(needed)))
    RangedWriteState.fromRanges(pointer, revision, length, pieces)
