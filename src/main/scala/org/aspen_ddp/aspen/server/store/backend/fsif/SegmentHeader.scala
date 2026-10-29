package org.aspen_ddp.aspen.server.store.backend.fsif

import java.nio.ByteBuffer
import java.util.UUID
import java.util.zip.CRC32

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType}

/** Layout: type(1) ObjectId(16) ext(4) length(4) Metadata(32) CRC(4), big-endian.
  * The CRC covers the first 57 header bytes followed by the slice bytes.
  */
final case class SegmentHeader(objectType: ObjectType.Value,
                               objectId: ObjectId,
                               externalIndex: Int,
                               sliceLength: Int,
                               metadata: Metadata,
                               crc: Int)

object SegmentHeader:
  val Size = 61
  private val CrcOffset = Size - 4

  private def typeByte(t: ObjectType.Value): Byte =
    if t == ObjectType.Data then 0 else 1

  private def crcOf(prefix: Array[Byte], slice: Array[Byte]): Int =
    val c = new CRC32
    c.update(prefix, 0, CrcOffset)
    c.update(slice, 0, slice.length)
    c.getValue.toInt

  private def encodeNoCrc(h: SegmentHeader, crc: Int): Array[Byte] =
    val arr = new Array[Byte](Size)
    val bb = ByteBuffer.wrap(arr)
    bb.put(typeByte(h.objectType))
    bb.putLong(h.objectId.uuid.getMostSignificantBits)
    bb.putLong(h.objectId.uuid.getLeastSignificantBits)
    bb.putInt(h.externalIndex)
    bb.putInt(h.sliceLength)
    h.metadata.encodeInto(bb)
    bb.putInt(crc)
    arr

  def forSlice(objectType: ObjectType.Value, objectId: ObjectId, externalIndex: Int,
               metadata: Metadata, slice: DataBuffer): SegmentHeader =
    val bytes = slice.getByteArray
    val h = SegmentHeader(objectType, objectId, externalIndex, bytes.length, metadata, 0)
    h.copy(crc = crcOf(encodeNoCrc(h, 0), bytes))

  def encode(h: SegmentHeader): Array[Byte] = encodeNoCrc(h, h.crc)

  def decode(bytes: Array[Byte]): Option[SegmentHeader] =
    if bytes.length < Size then
      None
    else
      val tpe = bytes(0) match
        case 0 => Some(ObjectType.Data)
        case 1 => Some(ObjectType.KeyValue)
        case _ => None
      tpe.map: t =>
        val bb = ByteBuffer.wrap(bytes, 1, Size - 1)
        val id = ObjectId(new UUID(bb.getLong(), bb.getLong()))
        val ext = bb.getInt()
        val len = bb.getInt()
        val md = Metadata(bb)
        SegmentHeader(t, id, ext, len, md, bb.getInt())

  def verify(h: SegmentHeader, slice: Array[Byte]): Boolean =
    h.sliceLength == slice.length && crcOf(encodeNoCrc(h, 0), slice) == h.crc
