package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.tcmu.ring.RingLayout.*

import java.lang.foreign.{Arena, MemorySegment}
import java.lang.foreign.ValueLayout.{JAVA_BYTE, JAVA_INT, JAVA_LONG_UNALIGNED, JAVA_SHORT}
import java.lang.invoke.VarHandle
import scala.collection.mutable

/** A completed command as the kernel would see it. `data` is the command's data area contents after completion. */
final case class FakeCompletion(cmdId: Int, status: Int, sense: Array[Byte], readLen: Option[Int], data: Array[Byte])

/** Plays the kernel's side of a TCMU command ring on an Arena-allocated segment.
  *
  * Entries are laid out as the kernel does: the iovec array, then the CDB, the whole rounded up to 8 bytes and never
  * shorter than the response. Data-area space is first-fit and reclaimed only when the command's completion is
  * seen by `completions()`.
  */
class FakeKernel(val cmdrSize: Int = 4096,
                 val dataSize: Int = 64 * 1024,
                 val flags: Int = FlagOooc | FlagReadLen | FlagTmr):

  val arena: Arena = Arena.ofShared()
  val cmdrOff: Int = 128
  val dataOff: Int = cmdrOff + cmdrSize
  val segment: MemorySegment = arena.allocate((dataOff + dataSize).toLong, 4096)

  private val intHandle: VarHandle = JAVA_INT.varHandle()

  private var head = 0
  private var lastSeenTail = 0
  private var nextId = 1
  /** cmdId -> (data-area offset, length) */
  private val allocations = mutable.Map[Int, (Int, Int)]()
  private var pads = 0

  writeMailbox()

  private def writeMailbox(): Unit =
    segment.set(JAVA_SHORT, MbVersion, MailboxVersion.toShort)
    segment.set(JAVA_SHORT, MbFlags, flags.toShort)
    segment.set(JAVA_INT, MbCmdrOff, cmdrOff)
    segment.set(JAVA_INT, MbCmdrSize, cmdrSize)
    intHandle.setRelease(segment, MbCmdTail.toLong, 0): Unit
    intHandle.setRelease(segment, MbCmdHead.toLong, 0): Unit

  def padsWritten: Int = synchronized(pads)

  def mailboxTail: Int = (intHandle.getAcquire(segment, MbCmdTail.toLong): Int)
  def mailboxHead: Int = (intHandle.getAcquire(segment, MbCmdHead.toLong): Int)

  /** Overwrites `cmd_head` without touching anything else, to simulate corruption. */
  def setCmdHead(value: Int): Unit = intHandle.setRelease(segment, MbCmdHead.toLong, value): Unit

  /** Simulates the kernel's ring reset: head and tail return to 0 and every outstanding command is forgotten. */
  def resetRing(): Unit = synchronized:
    head = 0
    lastSeenTail = 0
    allocations.clear()
    writeMailbox()

  def allocId(): Int = synchronized:
    val id = nextId
    nextId = if nextId == 0xffff then 1 else nextId + 1
    id

  private def spaceFree: Int =
    val tail = mailboxTail
    if head >= tail then cmdrSize - head + tail - 1 else tail - head - 1

  /** Where an entry of `len` bytes would be written next: at `head`, or at 0 after a PAD. */
  def nextEntryOffset(len: Int): Int = synchronized:
    if head + len > cmdrSize then 0 else head

  private def placeEntry(bytes: Array[Byte]): Int =
    val len = bytes.length
    val padLen = if head + len > cmdrSize then cmdrSize - head else 0
    require(padLen + len <= spaceFree, s"fake ring full: need ${padLen + len}, free $spaceFree")
    if padLen > 0 then
      val off = cmdrOff + head
      segment.set(JAVA_INT, off.toLong + EntLenOp, padLen | OpPad)
      segment.set(JAVA_SHORT, off.toLong + EntCmdId, 0.toShort)
      pads += 1
      head = 0
    val at = head
    MemorySegment.copy(bytes, 0, segment, JAVA_BYTE, (cmdrOff + at).toLong, len)
    head = (head + len) % cmdrSize
    intHandle.setRelease(segment, MbCmdHead.toLong, head): Unit
    at

  private def allocData(len: Int): Int =
    val used = allocations.values.toList.sortBy(_._1)
    val gaps = (0 :: used.map((o, l) => o + l)).zip(used.map(_._1) :+ dataSize)
    gaps.collectFirst { case (start, end) if end - start >= len => start } match
      case Some(at) => at
      case None => throw IllegalStateException(s"fake data area full: need $len")

  /** Builds a CMD entry, `cdb_off` pointing just past the iovecs at wherever the entry will land. */
  def commandEntry(cmdId: Int, cdb: Array[Byte], iovecs: Seq[(Long, Long)],
                   bidiCnt: Int = 0, difCnt: Int = 0): Array[Byte] =
    val iovEnd = ReqIov + IovecSize * iovecs.size
    val len = align8(math.max(iovEnd, RspSize) + align8(cdb.length))
    val cdbRel = math.max(iovEnd, RspSize)
    val at = nextEntryOffset(len)
    val buf = java.nio.ByteBuffer.allocate(len).order(java.nio.ByteOrder.nativeOrder())
    buf.putInt(EntLenOp, len | OpCmd)
    buf.putShort(EntCmdId, cmdId.toShort)
    buf.putInt(ReqIovCnt, iovecs.size)
    buf.putInt(ReqIovBidiCnt, bidiCnt)
    buf.putInt(ReqIovDifCnt, difCnt)
    buf.putLong(ReqCdbOff, (cmdrOff + at + cdbRel).toLong)
    for ((base, l), i) <- iovecs.zipWithIndex do
      buf.putLong(ReqIov + IovecSize * i, base)
      buf.putLong(ReqIov + IovecSize * i + 8, l)
    buf.put(cdbRel, cdb)
    buf.array()

  /** Queues a command. `data` is write data; `readLen` reserves that many bytes for a read. Returns the cmdId. */
  def submit(cdb: Array[Byte], data: Array[Byte] = Array.empty, readLen: Int = 0,
             iovSplit: Int = Int.MaxValue): Int = synchronized:
    val id = allocId()
    val size = math.max(data.length, readLen)
    val dataAt = allocData(size)
    allocations(id) = (dataAt, size)
    MemorySegment.copy(data, 0, segment, JAVA_BYTE, (dataOff + dataAt).toLong, data.length)
    val iovecs = (0 until size by iovSplit).map: o =>
      ((dataOff + dataAt + o).toLong, math.min(iovSplit, size - o).toLong)
    placeEntry(commandEntry(id, cdb, iovecs))
    id

  /** Writes a hand-built entry at the head (after a PAD if it would cross the ring end). Returns its ring offset. */
  def submitRaw(bytes: Array[Byte]): Int = synchronized(placeEntry(bytes))

  def submitTmr(tpe: Int, ids: Seq[Int]): Unit = synchronized:
    val len = align8(TmrCmdIds + 2 * ids.size)
    val buf = java.nio.ByteBuffer.allocate(len).order(java.nio.ByteOrder.nativeOrder())
    buf.putInt(EntLenOp, len | OpTmr)
    buf.put(TmrType, tpe.toByte)
    buf.putInt(TmrCmdCnt, ids.size)
    for (id, i) <- ids.zipWithIndex do buf.putShort(TmrCmdIds + 2 * i, id.toShort)
    placeEntry(buf.array())

  /** Every CMD entry completed since the last call, walking from the last-seen tail to `cmd_tail`. */
  def completions(): List[FakeCompletion] = synchronized:
    val tail = mailboxTail
    val out = List.newBuilder[FakeCompletion]
    while lastSeenTail != tail do
      val off = (cmdrOff + lastSeenTail).toLong
      val lenOp = segment.get(JAVA_INT, off + EntLenOp)
      if (lenOp & OpMask) == OpCmd then
        val id = segment.get(JAVA_SHORT, off + EntCmdId) & 0xffff
        val uflags = segment.get(JAVA_BYTE, off + EntUFlags) & 0xff
        val sense = segment.asSlice(off + RspSense, SenseSize).toArray(JAVA_BYTE)
        val readLen =
          if (uflags & UFlagReadLen) != 0 then Some(segment.get(JAVA_INT, off + RspReadLen)) else None
        val data = allocations.remove(id) match
          case Some((at, l)) => segment.asSlice((dataOff + at).toLong, l.toLong).toArray(JAVA_BYTE)
          case None => Array.emptyByteArray
        out += FakeCompletion(id, segment.get(JAVA_BYTE, off + RspScsiStatus) & 0xff, sense, readLen, data)
      lastSeenTail = (lastSeenTail + (lenOp & ~OpMask)) % cmdrSize
    out.result()

  def close(): Unit = arena.close()

object FakeKernel:
  def read10(lba: Int, blocks: Int): Array[Byte] =
    val c = new Array[Byte](10)
    c(0) = 0x28
    c(2) = (lba >>> 24).toByte
    c(3) = (lba >>> 16).toByte
    c(4) = (lba >>> 8).toByte
    c(5) = lba.toByte
    c(7) = (blocks >>> 8).toByte
    c(8) = blocks.toByte
    c

  def write10(lba: Int, blocks: Int): Array[Byte] =
    val c = read10(lba, blocks)
    c(0) = 0x2a
    c
