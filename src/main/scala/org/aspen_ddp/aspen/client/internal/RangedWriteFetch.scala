package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.{AppendPosition, DataObjectRangeState, RangedWrite, RangedWriteState, UnstableRangedWriteState}
import org.aspen_ddp.aspen.common.objects.DataObjectPointer

import scala.concurrent.{ExecutionContext, Future}

/** Builds a RangedWriteState from one length read and one range read per write.
  *
  * Every piece must come from the same revision of the object. Combining bytes read at different revisions
  * would produce deltas against content that never existed together, silently corrupting parity, so a
  * mismatch discards the whole round and the reads are repeated.
  */
private[aspen] object RangedWriteFetch:

  val MaxAttempts: Int = 3

  /** Combines one round of reads. `reads` holds the result of `readRange(w.offset, Some(w.data.length))` for
    * each write, in the same order. Returns None when the reads disagree on the object's revision.
    *
    * The length read is authoritative: each piece is clipped to it, and a write starting at or past the end
    * contributes no piece. `oldBytes` clips to the length the same way, so the state holds every byte that
    * `rangedWrites` will ask for. Throws IllegalStateException if a read at the agreed revision is too short
    * to cover its write.
    */
  def combine(pointer: DataObjectPointer,
              writes: List[RangedWrite],
              position: AppendPosition,
              reads: List[DataObjectRangeState]): Option[RangedWriteState] =
    require(writes.length == reads.length, s"${writes.length} writes but ${reads.length} range reads")
    val revision = position.revision
    if reads.exists(_.revision != revision) then
      None
    else
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
      Some(RangedWriteState.fromRanges(pointer, revision, length, pieces))

  /** Runs `fetchOnce` until a round's revisions agree, at most `maxAttempts` times, then fails with
    * UnstableRangedWriteState. A failed read fails the result immediately
    */
  def fetch(pointer: DataObjectPointer, writes: List[RangedWrite], maxAttempts: Int = MaxAttempts)
           (fetchOnce: () => Future[(AppendPosition, List[DataObjectRangeState])])
           (using ec: ExecutionContext): Future[RangedWriteState] =
    def attempt(n: Int): Future[RangedWriteState] =
      fetchOnce().flatMap: (position, reads) =>
        combine(pointer, writes, position, reads) match
          case Some(state) => Future.successful(state)
          case None =>
            if n < maxAttempts then
              attempt(n + 1)
            else
              Future.failed(new UnstableRangedWriteState(pointer, maxAttempts))
    attempt(1)
