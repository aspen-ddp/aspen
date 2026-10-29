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

/** magic "FSIFINDX"(8) seq(8) then segmentCount * (slot(4) ObjectId(16)) then CRC(4) over all preceding bytes */
object IndexFile:
  private val Magic = "FSIFINDX".getBytes("US-ASCII")
  private val EntrySize = 20

  private[fsif] def sizeFor(segmentCount: Int): Long = 16L + segmentCount.toLong * EntrySize + 4

  def encode(seq: Long, map: IndexMap): Array[Byte] =
    val n = map.slots.length
    val arr = new Array[Byte](sizeFor(n).toInt)
    val bb = ByteBuffer.wrap(arr)
    bb.put(Magic)
    bb.putLong(seq)
    for i <- 0 until n do
      bb.putInt(map.slots(i))
      bb.putLong(map.ids(i).uuid.getMostSignificantBits)
      bb.putLong(map.ids(i).uuid.getLeastSignificantBits)
    val c = new CRC32
    c.update(arr, 0, arr.length - 4)
    bb.putInt(c.getValue.toInt)
    arr

  def decode(bytes: Array[Byte], segmentCount: Int): Option[(Long, IndexMap)] =
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
        val map = IndexMap.empty(segmentCount)
        var ok = true
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
        if ok then Some((seq, map)) else None
