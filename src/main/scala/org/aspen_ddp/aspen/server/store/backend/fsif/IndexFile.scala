package org.aspen_ddp.aspen.server.store.backend.fsif

import java.nio.ByteBuffer
import java.util.UUID
import java.util.zip.CRC32

import org.aspen_ddp.aspen.common.objects.ObjectId

/** slots(ext) = internal slot or -1; ids(ext) = occupant (zero-UUID ObjectId when unmapped) */
final class IndexMap(val slots: Array[Int], val ids: Array[ObjectId]):
  def copy(): IndexMap = new IndexMap(slots.clone(), ids.clone())

object IndexMap:
  private val Zero = ObjectId(new UUID(0, 0))

  def empty(segmentCount: Int): IndexMap =
    new IndexMap(Array.fill(segmentCount)(-1), Array.fill(segmentCount)(Zero))

/** The index's attachment to a device journal (spec 4.2). `covered` is the highest journal serial whose records for
  * `attachment` are reflected in this index and in forced data; replay applies the attachment's records above it.
  * `detached` means the directory needs nothing from journal `journalId` (set by a detach and by device-load replay).
  */
final case class IndexMeta(journalId: UUID, attachment: UUID, covered: Long, detached: Boolean)

object IndexMeta:
  /** Attached to no journal: a fresh store */
  val None: IndexMeta = IndexMeta(new UUID(0, 0), new UUID(0, 0), -1L, detached = true)

/** magic "FSIFINDX"(8) seq(8) journalId(16) attachment(16) covered(8) detached(1, 0 or 1), then
  * segmentCount * (slot(4) ObjectId(16)), then CRC(4) over all preceding bytes
  */
object IndexFile:
  private val Magic = "FSIFINDX".getBytes("US-ASCII")
  private val MetaSize = 41
  private val EntrySize = 20

  private[fsif] def sizeFor(segmentCount: Int): Long = 16L + MetaSize + segmentCount.toLong * EntrySize + 4

  def encode(seq: Long, meta: IndexMeta, map: IndexMap): Array[Byte] =
    val n = map.slots.length
    val arr = new Array[Byte](sizeFor(n).toInt)
    val bb = ByteBuffer.wrap(arr)
    bb.put(Magic)
    bb.putLong(seq)
    bb.putLong(meta.journalId.getMostSignificantBits)
    bb.putLong(meta.journalId.getLeastSignificantBits)
    bb.putLong(meta.attachment.getMostSignificantBits)
    bb.putLong(meta.attachment.getLeastSignificantBits)
    bb.putLong(meta.covered)
    bb.put(if meta.detached then 1.toByte else 0.toByte)
    for i <- 0 until n do
      bb.putInt(map.slots(i))
      bb.putLong(map.ids(i).uuid.getMostSignificantBits)
      bb.putLong(map.ids(i).uuid.getLeastSignificantBits)
    val c = new CRC32
    c.update(arr, 0, arr.length - 4)
    bb.putInt(c.getValue.toInt)
    arr

  def decode(bytes: Array[Byte], segmentCount: Int): Option[(Long, IndexMeta, IndexMap)] =
    if segmentCount < 0 || bytes.length.toLong != sizeFor(segmentCount) then
      None
    else if !java.util.Arrays.equals(bytes, 0, 8, Magic, 0, 8) then
      None
    else
      val c = new CRC32
      c.update(bytes, 0, bytes.length - 4)
      val bb = ByteBuffer.wrap(bytes)
      if bb.getInt(bytes.length - 4) != c.getValue.toInt then
        None
      else
        bb.position(8)
        val seq = bb.getLong()
        val journalId = new UUID(bb.getLong(), bb.getLong())
        val attachment = new UUID(bb.getLong(), bb.getLong())
        val covered = bb.getLong()
        val detachedByte = bb.get()
        val map = IndexMap.empty(segmentCount)
        var ok = detachedByte == 0 || detachedByte == 1
        var i = 0
        while ok && i < segmentCount do
          val slot = bb.getInt()
          val id = ObjectId(new UUID(bb.getLong(), bb.getLong()))
          if slot < -1 then
            ok = false
          else
            map.slots(i) = slot
            map.ids(i) = id
          i += 1
        if ok then Some((seq, IndexMeta(journalId, attachment, covered, detachedByte == 1), map)) else None
