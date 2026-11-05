package org.aspen_ddp.aspen.vdisk.tcmu.ring

import org.aspen_ddp.aspen.vdisk.tcmu.ring.RingLayout.*

import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout.{JAVA_BYTE, JAVA_INT, JAVA_LONG_UNALIGNED, JAVA_SHORT}
import java.lang.invoke.VarHandle
import scala.collection.mutable

/** Decoder/encoder for one TCMU command ring mapped at `map`, which must be bounded to the exact mapped size.
  *
  * `nextItems` runs on the device thread and walks a private consume tail up to the kernel's `cmd_head`.
  * `complete` may run on any thread: it writes the response into the entry at the mailbox `cmd_tail` (skipping PAD
  * and TMR entries, as `tcmulib_command_complete` does) and advances `cmd_tail`. Every value read from the ring is
  * validated before it is used, and all access goes through `map`, so a bad value can only throw, never fault.
  */
final class Ring private (map: MemorySegment, initial: Ring.Geometry):
  import Ring.*

  private val mapSize = map.byteSize()

  @volatile private var geometry: Geometry = initial
  /** Offset within the command ring of the next entry to consume. Written only under the lock. */
  @volatile private var consumeTail: Long = initial.tail
  /** cmdId -> number of consumed, uncompleted CMD entries carrying it. Guarded by `this`. */
  private val outstanding = mutable.HashMap[Int, Int]()
  private var outstandingCount = 0
  /** The last `cmd_tail` this ring wrote, or adopted at open/resync. A mailbox `cmd_tail` that differs means the
    * kernel reset the ring under us, so `complete` writes nothing until `resync`. Guarded by `this`.
    */
  private var expectedTail: Long = initial.tail

  private def u8(off: Long): Int = map.get(JAVA_BYTE, off) & 0xff
  private def u16(off: Long): Int = map.get(JAVA_SHORT, off) & 0xffff
  private def u32(off: Long): Long = Integer.toUnsignedLong(map.get(JAVA_INT, off))
  private def acquire(off: Int): Long = Integer.toUnsignedLong(intHandle.getAcquire(map, off.toLong): Int)

  def inFlight: Int = synchronized(outstandingCount)

  def mailboxDump: String =
    val g = geometry
    s"version=${u16(MbVersion)} flags=0x${u16(MbFlags).toHexString} cmdr_off=${u32(MbCmdrOff)} " +
      s"cmdr_size=${u32(MbCmdrSize)} cmd_head=${acquire(MbCmdHead)} cmd_tail=${acquire(MbCmdTail)} " +
      s"consume_tail=$consumeTail inFlight=$inFlight (using cmdr_off=${g.cmdrOff} cmdr_size=${g.cmdrSize})"

  /** The mailbox plus the entry at ring offset `pos`. `full` adds the op-specific body, valid only once the
    * entry's length has been checked.
    */
  private def entryDump(g: Geometry, pos: Long, full: Boolean): String =
    val e = g.cmdrOff + pos
    val lenOp = u32(e + EntLenOp)
    val sb = StringBuilder(mailboxDump)
    sb ++= s"; entry@$pos len_op=0x${lenOp.toHexString} cmd_id=${u16(e + EntCmdId)} " +
      s"kflags=0x${u8(e + EntKFlags).toHexString} uflags=0x${u8(e + EntUFlags).toHexString}"
    val len = lenOp & ~OpMask.toLong
    if full && (lenOp & OpMask) == OpCmd then
      val iovCnt = u32(e + ReqIovCnt)
      val cdbOff = map.get(JAVA_LONG_UNALIGNED, e + ReqCdbOff)
      sb ++= s" iov_cnt=$iovCnt iov_bidi_cnt=${u32(e + ReqIovBidiCnt)} iov_dif_cnt=${u32(e + ReqIovDifCnt)}"
      sb ++= s" cdb_off=$cdbOff cdb="
      // Only within the entry: the rest of the map may hold unallocated data-area pages, which fault
      if cdbOff >= e && cdbOff < e + len then
        val n = math.min(16L, e + len - cdbOff).toInt
        sb ++= hex(map.asSlice(cdbOff, n).toArray(JAVA_BYTE))
      else
        sb ++= "(outside the entry)"
      val shown = math.min(iovCnt, math.min(MaxDumpIovecs.toLong, (len - ReqIov) / IovecSize))
      val iovs = (0L until shown).map: i =>
        val at = e + ReqIov + IovecSize * i
        s"(${map.get(JAVA_LONG_UNALIGNED, at)},${map.get(JAVA_LONG_UNALIGNED, at + 8)})"
      sb ++= iovs.mkString(" iovecs=[", ",", if shown < iovCnt then ",...]" else "]")
    sb.result()

  private def corrupt(reason: String, dump: String): Nothing = throw RingCorrupt(reason, dump)

  private def checkPointer(name: String, value: Long, g: Geometry): Unit =
    if value >= g.cmdrSize || value % Align != 0 then
      corrupt(s"$name=$value is not an 8-aligned offset below cmdr_size=${g.cmdrSize}", mailboxDump)

  /** Consumes every entry up to `cmd_head`. Throws RingCorrupt, in which case nothing is consumed. */
  def nextItems(): List[RingItem] =
    val g = geometry
    val head = acquire(MbCmdHead)
    checkPointer("cmd_head", head, g)
    checkPointer("cmd_tail", acquire(MbCmdTail), g)
    val items = List.newBuilder[RingItem]
    val ids = List.newBuilder[Int]
    var pos = consumeTail
    while pos != head do
      val e = g.cmdrOff + pos
      val lenOp = u32(e + EntLenOp)
      val len = lenOp & ~OpMask.toLong
      val op = (lenOp & OpMask).toInt
      // The low 3 bits are the op, so len is always 8-aligned.
      if len < HdrSize then
        corrupt(s"entry at $pos has length $len < 8", entryDump(g, pos, false))
      if pos + len > g.cmdrSize then
        corrupt(s"entry at $pos length $len runs past cmdr_size=${g.cmdrSize}", entryDump(g, pos, false))
      if len > (head - pos + g.cmdrSize) % g.cmdrSize then
        corrupt(s"entry at $pos length $len runs past cmd_head=$head", entryDump(g, pos, false))
      op match
        case OpPad => ()
        case OpCmd =>
          if len < RspSize then
            corrupt(s"CMD entry at $pos length $len is shorter than the $RspSize-byte response",
              entryDump(g, pos, false))
          val cmdId = u16(e + EntCmdId)
          ids += cmdId
          items += (decodeCmd(g, e, len, cmdId) match
            case Right(cmd) => cmd
            case Left(reason) =>
              RingItem.Malformed(cmdId, s"entry at $pos: $reason", entryDump(g, pos, true), completable = true))
        case OpTmr =>
          val cnt = if len < TmrCmdIds then -1L else u32(e + TmrCmdCnt)
          if cnt < 0 || TmrCmdIds + 2 * cnt > len then
            items += RingItem.Malformed(u16(e + EntCmdId),
              s"TMR entry at $pos: cmd_cnt=$cnt does not fit in length $len", entryDump(g, pos, false),
              completable = false)
          else
            val cmdIds = (0L until cnt).map(i => u16(e + TmrCmdIds + 2 * i)).toVector
            items += RingItem.Tmr(u8(e + TmrType), cmdIds)
        case other =>
          corrupt(s"entry at $pos has unknown op $other", entryDump(g, pos, false))
      pos = (pos + len) % g.cmdrSize
    synchronized:
      for id <- ids.result() do
        outstanding(id) = outstanding.getOrElse(id, 0) + 1
        outstandingCount += 1
      consumeTail = pos
    items.result()

  private def decodeCmd(g: Geometry, e: Long, len: Long, cmdId: Int): Either[String, RingItem.Command] =
    val iovCnt = u32(e + ReqIovCnt)
    val bidi = u32(e + ReqIovBidiCnt)
    val dif = u32(e + ReqIovDifCnt)
    val cdbOff = map.get(JAVA_LONG_UNALIGNED, e + ReqCdbOff)
    val dataStart = g.cmdrOff + g.cmdrSize
    val entryEnd = e + len
    if bidi != 0 then
      Left(s"iov_bidi_cnt=$bidi is not supported")
    else if dif != 0 then
      Left(s"iov_dif_cnt=$dif is not supported")
    else if ReqIov + IovecSize * iovCnt > len then
      Left(s"iov_cnt=$iovCnt needs ${ReqIov + IovecSize * iovCnt} bytes but the entry length is $len")
    // The kernel always places the CDB inside the entry. Anywhere else in the map may be a data-area page the
    // kernel has not allocated, and touching one faults.
    else if cdbOff < e || cdbOff >= entryEnd then
      Left(s"cdb_off=$cdbOff is outside the entry [$e, $entryEnd)")
    else
      val opcode = u8(cdbOff)
      // `cdbLength` reads byte 7 only for a variable-length CDB (0x7f)
      val cdbLen =
        if opcode == 0x7f && cdbOff + 8 > entryEnd then None
        else cdbLength(opcode, u8(cdbOff + 7))
      cdbLen match
        case None =>
          Left(s"cdb_off=$cdbOff opcode 0x${opcode.toHexString} has no supported CDB length")
        case Some(n) if cdbOff + n > entryEnd =>
          Left(s"cdb_off=$cdbOff with a $n-byte CDB runs past the entry end $entryEnd")
        case Some(n) =>
          val iovecs = (0L until iovCnt).map: i =>
            val at = e + ReqIov + IovecSize * i
            Iovec(map.get(JAVA_LONG_UNALIGNED, at), map.get(JAVA_LONG_UNALIGNED, at + 8))
          iovecs.indexWhere(v => v.offset < dataStart || v.length < 0 || v.offset > mapSize ||
            v.length > mapSize - v.offset) match
            case -1 => Right(RingItem.Command(cmdId, map.asSlice(cdbOff, n).toArray(JAVA_BYTE), iovecs.toVector))
            case i =>
              Left(s"iovec $i (base=${iovecs(i).offset}, len=${iovecs(i).length}) is outside the data area " +
                s"[$dataStart, $mapSize)")

  /** Completes `cmdId`. Returns false, writing nothing, when it is not outstanding (a double completion, or a
    * command consumed before a `resync`), when the mailbox `cmd_tail` is not the one this ring last wrote (the
    * kernel reset the ring and `resync` has not run yet), or the ring no longer holds an entry for it.
    */
  def complete(cmdId: Int, status: Int, sense: Array[Byte], readLen: Option[Int]): Boolean = synchronized:
    outstanding.get(cmdId) match
      case None => false
      case Some(count) =>
        val g = geometry
        val limit = consumeTail
        var tail = acquire(MbCmdTail)
        val reset = tail != expectedTail
        def lenAt(t: Long): Long = u32(g.cmdrOff + t + EntLenOp) & ~OpMask.toLong
        def opAt(t: Long): Long = u32(g.cmdrOff + t + EntLenOp) & OpMask
        def usable(t: Long): Boolean = t < g.cmdrSize && t % Align == 0 && t != limit && lenAt(t) >= HdrSize &&
          t + lenAt(t) <= g.cmdrSize
        while usable(tail) && opAt(tail) != OpCmd do
          tail = (tail + lenAt(tail)) % g.cmdrSize
        if reset || !usable(tail) || lenAt(tail) < RspSize then
          false
        else
          val e = g.cmdrOff + tail
          map.set(JAVA_SHORT, e + EntCmdId, cmdId.toShort)
          map.set(JAVA_BYTE, e + RspScsiStatus, status.toByte)
          // Always the whole area: the slot may hold another command's request bytes, which the kernel would
          // copy out as sense on CHECK CONDITION.
          val n = math.min(sense.length, SenseSize)
          MemorySegment.copy(sense, 0, map, JAVA_BYTE, e + RspSense, n)
          map.asSlice(e + RspSense + n, (SenseSize - n).toLong).fill(0)
          val uflags = u8(e + EntUFlags)
          readLen match
            case Some(n) if (g.flags & FlagReadLen) != 0 =>
              map.set(JAVA_INT, e + RspReadLen, n)
              map.set(JAVA_BYTE, e + EntUFlags, (uflags | UFlagReadLen).toByte)
            case _ =>
              map.set(JAVA_BYTE, e + EntUFlags, (uflags & ~UFlagReadLen).toByte)
          expectedTail = (tail + lenAt(tail)) % g.cmdrSize
          intHandle.setRelease(map, MbCmdTail.toLong, expectedTail.toInt): Unit
          if count == 1 then outstanding.remove(cmdId) else outstanding(cmdId) = count - 1
          outstandingCount -= 1
          true

  /** After the kernel resets the ring: revalidates the mailbox, restarts consumption at its `cmd_tail` and forgets
    * every outstanding command (the kernel retries them).
    */
  def resync(): Either[String, Unit] = resync(() => ())

  /** Runs `reset` (which makes the kernel reset the ring) and then resyncs, all under the completion lock, so no
    * stale completion can land in between. The `cmd_tail` check in `complete` cannot see a reset when the tail
    * was already 0, so this is the safe way to reset.
    */
  def resync(reset: () => Unit): Either[String, Unit] = synchronized:
    reset()
    validate(map).map: g =>
      geometry = g
      consumeTail = g.tail
      expectedTail = g.tail
      outstanding.clear()
      outstandingCount = 0

  def dataLength(iovecs: Vector[Iovec]): Long = iovecs.foldLeft(0L)(_ + _.length)

  /** Copies up to `length` bytes out of the iovecs, in order. */
  def gather(iovecs: Vector[Iovec], length: Int): Array[Byte] =
    val out = new Array[Byte](math.min(length.toLong, dataLength(iovecs)).toInt)
    var done = 0
    for v <- iovecs if done < out.length do
      val n = math.min(v.length, (out.length - done).toLong).toInt
      MemorySegment.copy(map, JAVA_BYTE, v.offset, out, done, n)
      done += n
    out

  /** Copies `data` into the iovecs, in order, stopping when they are full. Returns the bytes written. */
  def scatter(iovecs: Vector[Iovec], data: Array[Byte]): Int =
    var done = 0
    for v <- iovecs if done < data.length do
      val n = math.min(v.length, (data.length - done).toLong).toInt
      MemorySegment.copy(data, done, map, JAVA_BYTE, v.offset, n)
      done += n
    done

object Ring:
  private val intHandle: VarHandle = JAVA_INT.varHandle()
  private val MaxDumpIovecs = 16

  private final case class Geometry(cmdrOff: Long, cmdrSize: Long, flags: Int, tail: Long)

  private def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & 0xff}%02x").mkString

  private def validate(map: MemorySegment): Either[String, Geometry] =
    val size = map.byteSize()
    if size < MailboxSize then
      Left(s"map of $size bytes is smaller than the $MailboxSize-byte mailbox")
    else if map.maxByteAlignment() < Align then
      Left(s"map alignment ${map.maxByteAlignment()} is below $Align")
    else
      val version = map.get(JAVA_SHORT, MbVersion.toLong) & 0xffff
      val flags = map.get(JAVA_SHORT, MbFlags.toLong) & 0xffff
      val cmdrOff = Integer.toUnsignedLong(map.get(JAVA_INT, MbCmdrOff.toLong))
      val cmdrSize = Integer.toUnsignedLong(map.get(JAVA_INT, MbCmdrSize.toLong))
      val tail = Integer.toUnsignedLong(intHandle.getAcquire(map, MbCmdTail.toLong): Int)
      if version != MailboxVersion then
        Left(s"mailbox version=$version, expected $MailboxVersion")
      else if (flags & FlagOooc) == 0 then
        Left(s"mailbox flags=0x${flags.toHexString} lack the OOOC capability")
      else if cmdrOff < MailboxSize || cmdrOff % Align != 0 then
        Left(s"cmdr_off=$cmdrOff is not 8-aligned past the $MailboxSize-byte mailbox")
      else if cmdrSize == 0 || cmdrSize % Align != 0 then
        Left(s"cmdr_size=$cmdrSize is not a nonzero multiple of 8")
      else if cmdrOff + cmdrSize > size then
        Left(s"cmdr_off=$cmdrOff + cmdr_size=$cmdrSize exceeds the map size $size")
      else if tail >= cmdrSize || tail % Align != 0 then
        Left(s"cmd_tail=$tail is not an 8-aligned offset below cmdr_size=$cmdrSize")
      else
        Right(Geometry(cmdrOff, cmdrSize, flags, tail))

  /** Validates the mailbox and starts consuming at its `cmd_tail`, as tcmu-runner does. */
  def open(map: MemorySegment): Either[String, Ring] = validate(map).map(g => new Ring(map, g))
