package org.aspen_ddp.aspen.server.store.backend.fsif

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.aspen_ddp.aspen.common.objects.ObjectId

/** Tail compaction for one FsifBackend: moves the occupant of the highest used slot into the lowest free slot below
  * it, one move at a time, and truncates `data` whenever the tail slots are free. Owned by the backend's serial
  * executor: every method runs there. Passive: `drive()` runs after every serial task.
  *
  * One move of ext X from slot t to the lowest free slot f < t (spec "Tail compaction" steps 1-6):
  *  1. `start`: reserve f in `used` (so no allocation can take it) and read slot t raw (header plus the whole
  *     payload area), registered as a read on t's SlotState so none of its pending Deltas is pruned meanwhile.
  *  2-3. `copied`: the copy is that raw image with every Delta in t's list overlaid in order (the same assembly as
  *     a read). It is exact even if in-place writes to t raced the raw read, since such a write only writes bytes
  *     of Deltas still in the list. It is written to f.
  *  4. Deltas for X appended to t after the copy was assembled (`onDelta`) are also written to f, in order and one
  *     write task at a time. Their records are journaled with target t and acknowledged normally.
  *  5. Once no write into f is in flight or queued, `data` is forced. Map(X, f) is appended only when no write into
  *     f was issued after that force started; otherwise another force follows (durable-before-map).
  *  6. Appending the Map kills t (cancelling its queued in-place writes, which f already holds) in the same serial
  *     task, and t is freed once the Map is durable, under the slot-freeing rule.
  *
  * A copy-on-write overwrite, a delete or any re-occupation of X before the Map is appended cancels the move
  * (`cancel`). f was never mapped, so it is released once the move's io in flight has finished.
  *
  * Truncation runs inline on serial, so no allocation can interleave with it: the slot count is lowered to
  * (highest used slot + 1), or 0 when nothing is used, and a longer `data` is cut to that many strides (never
  * grown). A used slot is mapped, reserved (copy-on-write, rebuild or move target) or waiting for the slot-freeing
  * rule, so every slot cut away has its unmap durable, no read and no pending write, and no record that recovery
  * would replay still maps it. It is not forced: the next force of `data` covers it, and recovery is correct whether
  * a crash keeps it or not.
  *
  * Moves run concurrently with checkpoints. Deltas to t keep being applied to t in place, so a checkpoint's wait
  * for journal-s Deltas is unaffected; the Deltas that kill(t) cancels are cancelled by Map(X, f) in the same serial
  * task, which the checkpoint's Barrier makes durable before index s+1 (taken with X -> t) is written; and f was
  * forced holding them before that Map.
  */
private[fsif] final class Compaction(host: Compaction.Host, layout: FsifLayout):
  import Compaction.*
  import FsifBackend.IoKind

  private var move: Option[Move] = None
  private var checkSize = true
  @volatile private var started = 0
  @volatile private var mapped = 0
  @volatile private var cancelled = 0

  def stats: FsifBackend.CompactionStats = FsifBackend.CompactionStats(started, mapped, cancelled)

  /** No move outstanding */
  def idle: Boolean = move.isEmpty

  /** Run after every serial task: truncate free tail slots, then start a move if none is outstanding */
  def drive(): Unit =
    if host.fatal then
      move.foreach(finishIfDone)
    else
      truncate()
      if move.isEmpty then
        start()

  /** X's occupant is about to change (copy-on-write, delete, re-allocation, rebuild): a move of X that has not yet
    * appended its Map is cancelled
    */
  def cancel(x: Int): Unit =
    move.foreach: m =>
      if m.x == x && !m.mapAppended && !m.cancelled then
        m.cancelled = true
        cancelled += 1
        m.queue.clear()
        finishIfDone(m)

  /** A Delta was added to `slot`'s pending list. If X is moving out of that slot and the copy is already assembled,
    * its bytes go to f as well.
    */
  def onDelta(slot: Int, pd: PendingDelta): Unit =
    move.foreach: m =>
      if m.from == slot && m.assembled && !m.cancelled && !m.mapAppended then
        val base = layout.slotOffset(m.to)
        pd.ranges.foreach(r => m.queue += ((base + SegmentHeader.Size + r.offset, r.bytes)))
        m.queue += ((base, SegmentHeader.encode(pd.header)))
        pump(m)

  private def truncate(): Unit =
    val top = host.used.length() // highest used slot + 1, or 0
    // At runtime every write lands inside a slot below the slot count, so only the file found at startup can
    // reach past slotCount * stride (a partial last slot, or a dropped truncate): check that once
    if top < host.slotCount || checkSize then
      checkSize = false
      val target = top * layout.stride
      // Shrink only. A file ending inside slot top - 1 (its in-place write still pending) stays as it is: growing it
      // would write a byte at the end of that slot, which could land after a concurrent write of a full segment.
      if host.dataFile.size > target then
        host.dataFile.truncate(target)
      host.setSlotCount(top)

  private def start(): Unit =
    val t = host.used.length() - 1
    val f = host.used.nextClearBit(0)
    if t > 0 && f < t then
      host.movable(t).foreach: (x, id) =>
        host.used.set(f)
        val m = new Move(x, id, t, f)
        move = Some(m)
        started += 1
        val st = host.slotState(t)
        st.reads += 1
        m.io += 1
        host.runIo(IoKind.MoveRead)(host.dataFile.read(layout.slotOffset(t), SegmentHeader.Size +
          layout.segmentPayloadSize))(r => copied(m, st, r))

  private def copied(m: Move, st: SlotState, r: Try[Array[Byte]]): Unit =
    m.io -= 1
    st.reads -= 1
    val image = r.map(disk => assemble(disk, st))
    host.readDone(st)
    image match
      case Failure(t) =>
        host.failStop(t)
        finishIfDone(m)
      case Success(img) =>
        if m.cancelled || host.fatal then
          finishIfDone(m)
        else
          m.assembled = true
          m.queue += ((layout.slotOffset(m.to), img))
          pump(m)

  /** The raw image of t with every Delta in its list overlaid in order, the newest header last */
  private def assemble(disk: Array[Byte], st: SlotState): Array[Byte] =
    val payload = layout.segmentPayloadSize
    val img = new Array[Byte](SegmentHeader.Size + payload)
    System.arraycopy(disk, 0, img, 0, math.min(disk.length, img.length))
    st.deltas.foreach: d =>
      d.ranges.foreach: r =>
        if r.offset >= 0 && r.offset < payload then
          System.arraycopy(r.bytes, 0, img, SegmentHeader.Size + r.offset, math.min(r.bytes.length, payload - r.offset))
    st.newestHeader.foreach(h => System.arraycopy(SegmentHeader.encode(h), 0, img, 0, SegmentHeader.Size))
    img

  /** Issues the next write into f, or the force once all writes are done, or appends the Map */
  private def pump(m: Move): Unit =
    if !m.cancelled && !m.mapAppended && !host.fatal && !m.writing then
      if m.queue.nonEmpty then
        val writes = m.queue.toList
        m.queue.clear()
        m.writing = true
        m.dirty = true
        m.io += 1
        host.runIo(IoKind.MoveWrite)(writes.foreach((pos, b) => host.dataFile.write(pos, b)))(r => written(m, r))
      else if m.assembled && !m.forcing then
        if m.dirty then
          m.forcing = true
          m.dirty = false
          m.io += 1
          host.runIo(IoKind.MoveForce)(host.dataFile.force())(r => forced(m, r))
        else
          appendMap(m)

  private def written(m: Move, r: Try[Unit]): Unit =
    m.io -= 1
    m.writing = false
    r match
      case Failure(t) =>
        host.failStop(t)
        finishIfDone(m)
      case Success(_) =>
        if m.cancelled || host.fatal then finishIfDone(m) else pump(m)

  private def forced(m: Move, r: Try[Unit]): Unit =
    m.io -= 1
    m.forcing = false
    r match
      case Failure(t) =>
        host.failStop(t)
        finishIfDone(m)
      case Success(_) =>
        // pump appends the Map only if no write into f was issued after this force started
        if m.cancelled || host.fatal then finishIfDone(m) else pump(m)

  private def appendMap(m: Move): Unit =
    if !host.maps(m.x, m.from, m.id) then
      // Defensive: every change of X's occupant cancels the move first
      cancel(m.x)
    else
      m.mapAppended = true
      host.commitMove(m.x, m.id, m.from, m.to): r =>
        if r.isSuccess then mapped += 1
        move = None

  /** A cancelled or failed move ends once none of its io is in flight; a cancelled move then releases f */
  private def finishIfDone(m: Move): Unit =
    if m.io == 0 && !m.mapAppended && (m.cancelled || host.fatal) && move.contains(m) then
      if !host.fatal then
        host.used.clear(m.to)
      move = None

private[fsif] object Compaction:
  /** What compaction needs from its backend; every call is made on the backend's serial executor */
  trait Host:
    def fatal: Boolean
    /** Slots that are mapped, reserved or waiting for the slot-freeing rule */
    def used: java.util.BitSet
    def slotCount: Int
    def setSlotCount(n: Int): Unit
    def dataFile: FsifFile
    /** (ext, ObjectId) of the live occupant of `slot` if it may move now: mapped, not dead, not behind a gate */
    def movable(slot: Int): Option[(Int, ObjectId)]
    def maps(x: Int, slot: Int, id: ObjectId): Boolean
    def slotState(slot: Int): SlotState
    /** A read registered on `st` finished: prune its written Deltas, free or drop it if possible */
    def readDone(st: SlotState): Unit
    /** Runs `work` on io and `done` on serial (guarded); a rejected task completes `done` with the failure */
    def runIo[A](kind: FsifBackend.IoKind)(work: => A)(done: Try[A] => Unit): Unit
    def failStop(t: Throwable): Unit
    /** Maps X to `to`, kills `from` and appends Map(X, to) in this serial task; frees `from` once it is durable,
      * then calls `done`
      */
    def commitMove(x: Int, id: ObjectId, from: Int, to: Int)(done: Try[Unit] => Unit): Unit

  private final class Move(val x: Int, val id: ObjectId, val from: Int, val to: Int):
    /** io tasks in flight: the read of t, a write into f, the force */
    var io = 0
    var assembled = false
    var cancelled = false
    var mapAppended = false
    /** Writes into f not yet issued, in order: (file position, bytes) */
    val queue: mutable.ArrayBuffer[(Long, Array[Byte])] = mutable.ArrayBuffer()
    var writing = false
    var forcing = false
    /** A write into f was issued after the most recent force started (or no force has run yet) */
    var dirty = false
