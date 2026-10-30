package org.aspen_ddp.aspen.client.internal.allocation

import org.aspen_ddp.aspen.common.objects.{Key, Value}
import org.aspen_ddp.aspen.common.util.int2byte

import java.nio.ByteBuffer

/** Pure helpers for the allocated-index bitmap of an FSIF pool. The bitmap is split into 1024-bit segments, one per
  * key. Bit `b` of a segment is bit `b % 8` (LSB first) of byte `b / 8`.
  */
object FsifIndexBitmap:

  val SegmentBits: Int = 1024
  val SegmentBytes: Int = 128

  def segmentsFor(indexCount: Int): Int =
    require(indexCount > 0, "indexCount must be positive")
    (indexCount + SegmentBits - 1) / SegmentBits

  def segmentKey(segment: Int): Key = Key(int2byte(segment))

  def zeroedContent(indexCount: Int): Map[Key, Value] =
    (0 until segmentsFor(indexCount)).map(s => segmentKey(s) -> Value(new Array[Byte](SegmentBytes))).toMap

  def isSet(seg: Array[Byte], bit: Int): Boolean =
    (seg(bit / 8) & (1 << (bit % 8))) != 0

  def withBit(seg: Array[Byte], bit: Int, value: Boolean): Array[Byte] =
    val copy = seg.clone()
    val mask = 1 << (bit % 8)
    copy(bit / 8) =
      if value then (copy(bit / 8) | mask).toByte
      else (copy(bit / 8) & ~mask).toByte
    copy

  def freeBit(seg: Array[Byte], usableBits: Int, reserved: Int => Boolean): Option[Int] =
    (0 until usableBits).find(b => !isSet(seg, b) && !reserved(b))

  def usableBits(segment: Int, indexCount: Int): Int =
    math.min(SegmentBits, indexCount - segment * SegmentBits)

  def index(segment: Int, bit: Int): Int = segment * SegmentBits + bit

  def locate(index: Int): (Int, Int) = (index / SegmentBits, index % SegmentBits)

  def storePointer(index: Int): Array[Byte] = int2byte(index)

  def indexOf(storePointer: Array[Byte], indexCount: Int): Int =
    if storePointer.length != 4 then
      throw new IllegalArgumentException(s"FSIF store pointer must be 4 bytes, got ${storePointer.length}")
    val i = ByteBuffer.wrap(storePointer).getInt()
    if i < 0 || i >= indexCount then
      throw new IllegalArgumentException(s"FSIF store pointer index $i out of range [0, $indexCount)")
    i

  def encodeIndices(indices: Seq[Int]): Array[Byte] =
    val bb = ByteBuffer.allocate(indices.size * 4)
    indices.foreach(bb.putInt)
    bb.array()

  def decodeIndices(bytes: Array[Byte]): List[Int] =
    val bb = ByteBuffer.wrap(bytes)
    List.fill(bytes.length / 4)(bb.getInt())
