package org.aspen_ddp.aspen.common


import java.nio.ByteBuffer
import java.util.zip.CRC32

import scala.language.implicitConversions
import scala.Conversion

/** This class serves as a Read-Only, compile-time wrapper around ByteBuffer.
  *
  *  ByteBuffer is mutable and it's very easy to forget to save and restore the position variable when reading data buffer.
  *  This class serves primarily to guard against accidental mutation of the ByteBuffer attributes and content.
  *
  *  Implemented as an AnyVal so no run-time allocation overhead is required by this wrapper.
  *
  */
final class DataBuffer private (private val buf: ByteBuffer) extends AnyVal {

  /** Creates a new read-only copy of the wrapped byte buffer */
  def asReadOnlyBuffer(): ByteBuffer = buf.asReadOnlyBuffer()

  def size: Int = buf.limit() - buf.position()

  def get(byteOffset: Int): Byte = buf.get(byteOffset)
  def getShort(byteOffset: Int): Short = buf.getShort(byteOffset)
  def getInt(byteOffset: Int): Int = buf.getInt(byteOffset)
  def getLong(byteOffset: Int): Long = buf.getLong(byteOffset)

  def getDouble(byteOffset: Int): Double = buf.getDouble(byteOffset)
  def getFloat(byteOffset: Int): Float = buf.getFloat(byteOffset)

  def hashString: String = DataBuffer.hash(this :: Nil).toHexString

  /** Creates a copy of the wrapped byte buffer content */
  def getByteArray: Array[Byte] = {
    val arr = new Array[Byte](size)
    buf.asReadOnlyBuffer().get(arr)
    arr
  }

  /** Returns the underlying Array storage for this data buffer IFF an underlying array exists and exactly matches
    * the buffer size. Otherwise it returns a copy
    */
  private[aspen] def getDirectByteArray: Array[Byte] = {

    if (buf.hasArray && buf.arrayOffset() == 0 && buf.array().length == this.size)
      buf.array
    else
      getByteArray
  }

  def copy(): DataBuffer = DataBuffer(this.getByteArray)

  def compareTo(that: DataBuffer): Int = buf.compareTo(that.buf)

  def slice(offset: Int, length: Int): DataBuffer = {
    val bb = buf.asReadOnlyBuffer()
    bb.position( bb.position() + offset )
    bb.limit(bb.position() + length)
    DataBuffer(bb)
  }

  def slice(offset: Int): DataBuffer = {
    val bb = buf.asReadOnlyBuffer()
    bb.position( bb.position() + offset )
    DataBuffer(bb)
  }

  def split(offset: Int): (DataBuffer, DataBuffer) = (slice(0, offset), slice(offset))

  def append(append: DataBuffer): DataBuffer = {
    val buf = ByteBuffer.allocate( this.size + append.size )
    buf.put(this.asReadOnlyBuffer())
    buf.put(append.asReadOnlyBuffer())
    buf.position(0)
    DataBuffer(buf)
  }
}

object DataBuffer {
  val Empty = DataBuffer(new Array[Byte](0))

  given Conversion[ByteBuffer, DataBuffer] = buf => new DataBuffer(buf.asReadOnlyBuffer())
  given Conversion[Array[Byte], DataBuffer] = arr => new DataBuffer(ByteBuffer.wrap(arr))
  given Conversion[DataBuffer, ByteBuffer] = db => db.asReadOnlyBuffer()

  def apply(): DataBuffer = Empty
  def apply(bb: ByteBuffer): DataBuffer = new DataBuffer(bb.asReadOnlyBuffer())
  def apply(arr: Array[Byte]): DataBuffer = DataBuffer(ByteBuffer.wrap(arr))
  

  def zeroed(nbytes: Int): DataBuffer = if (nbytes == 0) Empty else DataBuffer(ByteBuffer.allocate(nbytes))

  def zeroed(nbytes: Long): DataBuffer = zeroed(nbytes.asInstanceOf[Int])

  def compact(maxSize: Long, buffers: List[DataBuffer]): (DataBuffer, List[DataBuffer]) = compact(maxSize.asInstanceOf[Int], buffers)

  def compact(maxSize: Int, buffers: List[DataBuffer]): (DataBuffer, List[DataBuffer]) = {
    val nbytes = buffers.foldLeft(0)((sz, db) => sz + db.size)
    val arr = new Array[Byte](if (nbytes <= maxSize) nbytes else maxSize)
    val bb = ByteBuffer.wrap(arr)
    val remaining = fill(bb, buffers)
    (DataBuffer(arr), remaining)
  }

  def fill(bb: ByteBuffer, remaining: List[DataBuffer]): List[DataBuffer] = {
    if (remaining.isEmpty || bb.remaining() == 0)
      remaining
    else {
      val db = remaining.head

      if (db.size <= bb.remaining()) {
        bb.put(db)
        fill(bb, remaining.tail)
      }
      else {
        val (w, r) = db.split(bb.remaining())
        bb.put(w)
        fill(bb, r :: remaining.tail)
      }
    }
  }

  def hash(buffers: List[DataBuffer]): Long = {
    val crc = new CRC32
    buffers.foreach(db => crc.update(db.getByteArray))
    crc.getValue
  }

  def hashString(buffers: List[DataBuffer]): String = hash(buffers).toHexString
}
