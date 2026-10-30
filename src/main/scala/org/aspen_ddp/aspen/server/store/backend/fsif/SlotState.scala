package org.aspen_ddp.aspen.server.store.backend.fsif

import scala.collection.mutable

/** A journaled Delta for one slot: the complete new header plus absolute byte ranges. `durable` once its journal
  * batch is forced; only then may it be written in place.
  */
private[fsif] final class PendingDelta(val header: SegmentHeader, val ranges: List[DeltaRange]):
  var durable = false
  /** Written in place and not yet covered by a successful force of `data`: the id of the first data force that
    * covers it (FsifBackend's `nextForceId` when the write completed), else -1
    */
  var tag = -1L
  /** Forced in place or cancelled: no longer counted as unapplied, and no longer holding back a checkpoint */
  var done = false
  /** Sequence of the journal this Delta was appended to: a checkpoint to seq+1 waits for it to be applied */
  var journalSeq = 0L
  val bytes: Long = SegmentHeader.Size.toLong + ranges.iterator.map(_.bytes.length.toLong).sum

/** Serial-executor state of one slot.
  *
  * `deltas` holds the slot's Deltas in commit order, in four runs:
  *  - `[0, forced)`: written in place and covered by a successful force of `data` (or cancelled), but kept because a
  *    read was in flight. A read overlays every Delta in the list, so one that raced with an in-place write still
  *    sees all of its bytes;
  *  - `[forced, applied)`: written in place but not yet forced. A failed force may have dropped them (Linux marks
  *    failed-writeback pages clean, so a retried force would not write them), so they stay overlaid and are
  *    re-queued (`applied` set back to `forced`) when a data force fails;
  *  - `[applied, applied + writing)`: being written in place on io (one coalesced write task);
  *  - the rest: queued, durable ones first (journal batches complete in order).
  *
  * The forced run is dropped whenever no read is in flight. A slot whose occupant died (deleted or moved by
  * copy-on-write) is `dead`: its queued Deltas are cancelled and it becomes free (the slot-freeing rule) once
  * `unmapDurable`, `reads == 0` and `writing == 0`.
  */
private[fsif] final class SlotState(val slot: Int):
  val deltas: mutable.ArrayBuffer[PendingDelta] = mutable.ArrayBuffer()
  var forced = 0
  var applied = 0
  var writing = 0
  var reads = 0
  var dead = false
  var unmapDurable = false
  /** Bumped whenever the Delta list changes or an in-place write starts or ends, so a read can tell whether the
    * bytes it read raced with a write
    */
  var version = 0L

  /** Index of the first Delta that is neither written nor being written */
  def queuedFrom: Int = applied + writing

  /** Nothing pending, nothing in flight, not waiting to be freed: the state may be discarded */
  def unused: Boolean = deltas.isEmpty && reads == 0 && writing == 0 && !dead

  /** Removes and returns the queued (not started) Deltas */
  def cancelQueued(): Seq[PendingDelta] =
    val from = queuedFrom
    val dropped = deltas.drop(from).toList
    deltas.remove(from, deltas.size - from)
    dropped

  /** Drops the forced run if no read can still need it */
  def prune(): Unit =
    if reads == 0 && forced > 0 then
      deltas.remove(0, forced)
      applied -= forced
      forced = 0

  /** The run of durable Deltas that may be written next, or empty if a write is in flight */
  def nextWritable(): List[PendingDelta] =
    if writing > 0 then
      Nil
    else
      deltas.iterator.drop(applied).takeWhile(_.durable).toList

  /** The newest header in the list, which is the slot's current header whenever the list is non-empty */
  def newestHeader: Option[SegmentHeader] = deltas.lastOption.map(_.header)

private[fsif] object SlotState:
  /** Builds the slice described by `header` from bytes read from disk plus every Delta in `deltas`, in order.
    * Bytes the disk read did not reach are zero (a CRC check then fails unless a Delta covers them).
    */
  def overlay(header: SegmentHeader, disk: Array[Byte], deltas: Iterable[PendingDelta]): Array[Byte] =
    val buf = new Array[Byte](header.sliceLength)
    System.arraycopy(disk, 0, buf, 0, math.min(disk.length, buf.length))
    deltas.foreach: d =>
      d.ranges.foreach: r =>
        if r.offset < buf.length then
          System.arraycopy(r.bytes, 0, buf, r.offset, math.min(r.bytes.length, buf.length - r.offset))
    buf
