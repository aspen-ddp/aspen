package org.aspen_ddp.aspen.common.ida

import java.nio.ByteBuffer
import java.util.Arrays

import org.aspen_ddp.aspen.common.DataBuffer

/** A run of bytes to XOR into a slice starting at offset */
final case class XorRegion(offset: Int, bytes: Array[Byte]):

  def end: Long = offset.toLong + bytes.length

  override def equals(other: Any): Boolean = other match
    case that: XorRegion => offset == that.offset && Arrays.equals(bytes, that.bytes)
    case _ => false

  override def hashCode: Int = 31 * Arrays.hashCode(bytes) + offset


/** The per-store payload for updating an object. The store checks that its slice is exactly
  * expectedSliceLength bytes long (and, for the store holding the trailer, that the final bytes
  * of the slice equal expectedTail) and then zero-extends the slice to newSliceLength and XORs
  * each region's bytes into it at the region's offset. Regions are sorted, non-overlapping and
  * may begin beyond expectedSliceLength (they land in the zero-extended area).
  */
final case class AppendDelta(
    expectedSliceLength: Int,
    newSliceLength: Int,
    regions: List[XorRegion],
    expectedTail: Option[Array[Byte]]):

  require(expectedSliceLength >= 0 && expectedSliceLength <= newSliceLength,
    s"Invalid slice lengths: expected $expectedSliceLength, new $newSliceLength")
  regions.foldLeft(0L) { (prevEnd, r) =>
    require(r.offset >= 0, s"Invalid region offset ${r.offset}")
    require(r.bytes.length > 0, "Empty XOR region")
    require(r.offset >= prevEnd, s"XOR region at ${r.offset} is unsorted or overlaps a previous region")
    require(r.end <= newSliceLength,
      s"XOR region (${r.offset} + ${r.bytes.length}) extends past the new slice length $newSliceLength")
    r.end
  }
  require(expectedTail.forall(_.length <= expectedSliceLength),
    "Expected tail is longer than the expected slice")

  def matches(slice: DataBuffer): Boolean =
    slice.size == expectedSliceLength && (expectedTail match
      case None => true
      case Some(tail) =>
        tail.length <= slice.size && {
          val start = slice.size - tail.length
          var i = 0
          while i < tail.length && slice.get(start + i) == tail(i) do
            i += 1
          i == tail.length
        })

  def applyTo(slice: DataBuffer): DataBuffer =
    require(matches(slice), "Append delta does not match the current slice")
    val arr = Arrays.copyOf(slice.getByteArray, newSliceLength)
    regions.foreach { r =>
      var i = 0
      while i < r.bytes.length do
        arr(r.offset + i) = (arr(r.offset + i) ^ r.bytes(i)).toByte
        i += 1
    }
    DataBuffer(arr)

  def encode: DataBuffer =
    val tailLen = expectedTail.map(_.length).getOrElse(0)
    val dataLen = regions.foldLeft(0)((n, r) => n + r.bytes.length)
    val bb = ByteBuffer.allocate(AppendDelta.HeaderSizeV2 + regions.length * 8 + dataLen + tailLen)
    bb.put(AppendDelta.FormatVersion)
    bb.putInt(expectedSliceLength)
    bb.putInt(newSliceLength)
    bb.putInt(regions.length)
    bb.put(if expectedTail.isDefined then 1.toByte else 0.toByte)
    bb.putInt(tailLen)
    regions.foreach { r =>
      bb.putInt(r.offset)
      bb.putInt(r.bytes.length)
    }
    regions.foreach(r => bb.put(r.bytes))
    expectedTail.foreach(t => bb.put(t))
    DataBuffer(bb.array())

  override def equals(other: Any): Boolean = other match
    case that: AppendDelta =>
      expectedSliceLength == that.expectedSliceLength &&
        newSliceLength == that.newSliceLength &&
        regions == that.regions &&
        (expectedTail, that.expectedTail).match
          case (None, None) => true
          case (Some(a), Some(b)) => Arrays.equals(a, b)
          case _ => false
    case _ => false

  override def hashCode: Int =
    var h = regions.hashCode
    h = 31 * h + expectedSliceLength
    h = 31 * h + newSliceLength
    31 * h + expectedTail.map(Arrays.hashCode).getOrElse(0)


object AppendDelta:

  val FormatVersion: Byte = 2

  /** Single-region convenience constructor */
  def apply(
      expectedSliceLength: Int,
      newSliceLength: Int,
      xorOffset: Int,
      xorBytes: Array[Byte],
      expectedTail: Option[Array[Byte]]): AppendDelta =
    AppendDelta(expectedSliceLength, newSliceLength, List(XorRegion(xorOffset, xorBytes)), expectedTail)

  // v1: <version:1><expectedSliceLength:4><newSliceLength:4><xorOffset:4><xorLength:4><hasTail:1><tailLength:4>
  private val HeaderSizeV1: Int = 22
  // v2: <version:1><expectedSliceLength:4><newSliceLength:4><regionCount:4><hasTail:1><tailLength:4>
  //     then regionCount x (<offset:4><length:4>), the region bytes in order, and the tail
  private val HeaderSizeV2: Int = 18

  def decode(db: DataBuffer): AppendDelta =
    try
      val bb = ByteBuffer.wrap(db.getByteArray)
      if bb.remaining < 1 then
        throw new IllegalArgumentException("Truncated append delta")
      bb.get() match
        case 1 => decodeV1(bb)
        case 2 => decodeV2(bb)
        case _ => throw new IllegalArgumentException("Unsupported append delta format")
    catch
      case e: java.nio.BufferUnderflowException =>
        throw new IllegalArgumentException("Truncated append delta", e)

  private def readTail(bb: ByteBuffer, hasTail: Byte): Option[Array[Byte]] =
    if hasTail == 1 then
      val t = new Array[Byte](bb.remaining)
      bb.get(t)
      Some(t)
    else
      None

  private def decodeV1(bb: ByteBuffer): AppendDelta =
    if bb.remaining < HeaderSizeV1 - 1 then
      throw new IllegalArgumentException("Truncated append delta")
    val expectedSliceLength = bb.getInt()
    val newSliceLength = bb.getInt()
    val xorOffset = bb.getInt()
    val xorLength = bb.getInt()
    val hasTail = bb.get()
    val tailLength = bb.getInt()

    if hasTail != 0 && hasTail != 1 then
      throw new IllegalArgumentException("Invalid append delta tail flag")
    if xorLength < 0 || tailLength < 0 || (hasTail == 0 && tailLength != 0) ||
        xorLength.toLong + tailLength != bb.remaining then
      throw new IllegalArgumentException("Append delta length mismatch")

    val xorBytes = new Array[Byte](xorLength)
    bb.get(xorBytes)
    AppendDelta(expectedSliceLength, newSliceLength, List(XorRegion(xorOffset, xorBytes)), readTail(bb, hasTail))

  private def decodeV2(bb: ByteBuffer): AppendDelta =
    if bb.remaining < HeaderSizeV2 - 1 then
      throw new IllegalArgumentException("Truncated append delta")
    val expectedSliceLength = bb.getInt()
    val newSliceLength = bb.getInt()
    val count = bb.getInt()
    val hasTail = bb.get()
    val tailLength = bb.getInt()

    if hasTail != 0 && hasTail != 1 then
      throw new IllegalArgumentException("Invalid append delta tail flag")
    if count < 0 || tailLength < 0 || (hasTail == 0 && tailLength != 0) ||
        count.toLong * 8 > bb.remaining then
      throw new IllegalArgumentException("Append delta length mismatch")

    val table = Array.fill(count)((bb.getInt(), bb.getInt()))
    var total = tailLength.toLong
    table.foreach { (_, len) =>
      if len < 0 then
        throw new IllegalArgumentException("Append delta length mismatch")
      total += len
    }
    if total != bb.remaining then
      throw new IllegalArgumentException("Append delta length mismatch")

    val regions = table.toList.map { (off, len) =>
      val b = new Array[Byte](len)
      bb.get(b)
      XorRegion(off, b)
    }
    AppendDelta(expectedSliceLength, newSliceLength, regions, readTail(bb, hasTail))
