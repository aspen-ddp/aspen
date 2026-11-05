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

  /** Decodes and copies every region. Stores use `EncodedAppendDelta.parse`, which copies nothing. */
  def decode(db: DataBuffer): AppendDelta = EncodedAppendDelta.parse(db).toAppendDelta


/** A validated view of an encoded AppendDelta that leaves the region bytes in the encoded buffer. Parsing reads only
  * the header and region table, so a store can check a delta at prepare and again at commit without copying it, and
  * `applyTo` XORs straight from the buffer. Parsing rejects exactly what `AppendDelta.decode` and the AppendDelta
  * constructor reject, with IllegalArgumentException.
  */
final class EncodedAppendDelta private (
    buf: ByteBuffer, // read-only, index 0 is the first byte of the encoding
    val expectedSliceLength: Int,
    val newSliceLength: Int,
    regionOffsets: Array[Int],
    regionLengths: Array[Int],
    regionStarts: Array[Int], // index in buf of each region's bytes
    tailStart: Int,
    tailLength: Int, // -1 for no tail
):

  def regionCount: Int = regionOffsets.length

  /** (offset, length) of each region in the new slice, sorted and non-overlapping */
  def regionSpans: List[(Int, Int)] = List.tabulate(regionCount)(i => (regionOffsets(i), regionLengths(i)))

  def matches(slice: DataBuffer): Boolean =
    slice.size == expectedSliceLength && (tailLength < 0 || {
      val start = slice.size - tailLength
      var i = 0
      while i < tailLength && slice.get(start + i) == buf.get(tailStart + i) do
        i += 1
      i == tailLength
    })

  /** The new slice. One copy of the current slice, then each region XORed in 8 bytes at a time */
  def applyTo(slice: DataBuffer): DataBuffer =
    require(matches(slice), "Append delta does not match the current slice")
    val arr = new Array[Byte](newSliceLength)
    slice.asReadOnlyBuffer().get(arr, 0, slice.size)
    val out = ByteBuffer.wrap(arr) // big-endian, as is buf, so the longs line up byte for byte
    var r = 0
    while r < regionCount do
      var d = regionOffsets(r)
      var s = regionStarts(r)
      val end = d + regionLengths(r)
      while d + 8 <= end do
        out.putLong(d, out.getLong(d) ^ buf.getLong(s))
        d += 8
        s += 8
      while d < end do
        arr(d) = (arr(d) ^ buf.get(s)).toByte
        d += 1
        s += 1
      r += 1
    DataBuffer(arr)

  def toAppendDelta: AppendDelta =
    def bytes(start: Int, length: Int): Array[Byte] =
      val b = new Array[Byte](length)
      buf.get(start, b)
      b
    val regions = List.tabulate(regionCount)(i => XorRegion(regionOffsets(i), bytes(regionStarts(i), regionLengths(i))))
    AppendDelta(expectedSliceLength, newSliceLength, regions,
      Option.when(tailLength >= 0)(bytes(tailStart, tailLength)))


object EncodedAppendDelta:

  // v1: <version:1><expectedSliceLength:4><newSliceLength:4><xorOffset:4><xorLength:4><hasTail:1><tailLength:4>
  private val HeaderSizeV1: Int = 22
  // v2: <version:1><expectedSliceLength:4><newSliceLength:4><regionCount:4><hasTail:1><tailLength:4>
  //     then regionCount x (<offset:4><length:4>), the region bytes in order, and the tail
  private val HeaderSizeV2: Int = 18

  private def invalid(msg: String): Nothing = throw new IllegalArgumentException(msg)

  def parse(db: DataBuffer): EncodedAppendDelta =
    val bb = db.asReadOnlyBuffer().slice()
    if bb.remaining < 1 then
      invalid("Truncated append delta")
    bb.get(0) match
      case 1 => parseV1(bb)
      case 2 => parseV2(bb)
      case _ => invalid("Unsupported append delta format")

  private def parseV1(bb: ByteBuffer): EncodedAppendDelta =
    if bb.limit() < HeaderSizeV1 then
      invalid("Truncated append delta")
    val expectedSliceLength = bb.getInt(1)
    val newSliceLength = bb.getInt(5)
    val xorOffset = bb.getInt(9)
    val xorLength = bb.getInt(13)
    val hasTail = bb.get(17)
    val tailLength = bb.getInt(18)

    if hasTail != 0 && hasTail != 1 then
      invalid("Invalid append delta tail flag")
    if xorLength < 0 || tailLength < 0 || (hasTail == 0 && tailLength != 0) ||
        xorLength.toLong + tailLength != bb.limit() - HeaderSizeV1 then
      invalid("Append delta length mismatch")

    build(bb, expectedSliceLength, newSliceLength, Array(xorOffset), Array(xorLength), Array(HeaderSizeV1),
      HeaderSizeV1 + xorLength, if hasTail == 1 then tailLength else -1)

  private def parseV2(bb: ByteBuffer): EncodedAppendDelta =
    if bb.limit() < HeaderSizeV2 then
      invalid("Truncated append delta")
    val expectedSliceLength = bb.getInt(1)
    val newSliceLength = bb.getInt(5)
    val count = bb.getInt(9)
    val hasTail = bb.get(13)
    val tailLength = bb.getInt(14)

    if hasTail != 0 && hasTail != 1 then
      invalid("Invalid append delta tail flag")
    val dataStart = HeaderSizeV2.toLong + count.toLong * 8
    if count < 0 || tailLength < 0 || (hasTail == 0 && tailLength != 0) || dataStart > bb.limit() then
      invalid("Append delta length mismatch")

    val offsets = new Array[Int](count)
    val lengths = new Array[Int](count)
    val starts = new Array[Int](count)
    var pos = dataStart
    var i = 0
    while i < count do
      offsets(i) = bb.getInt(HeaderSizeV2 + i * 8)
      lengths(i) = bb.getInt(HeaderSizeV2 + i * 8 + 4)
      if lengths(i) < 0 then
        invalid("Append delta length mismatch")
      starts(i) = math.min(pos, Int.MaxValue).toInt
      pos += lengths(i)
      i += 1
    if pos + tailLength != bb.limit() then
      invalid("Append delta length mismatch")

    build(bb, expectedSliceLength, newSliceLength, offsets, lengths, starts, pos.toInt,
      if hasTail == 1 then tailLength else -1)

  /** The AppendDelta constructor's checks, on the region table */
  private def build(bb: ByteBuffer, expectedSliceLength: Int, newSliceLength: Int, offsets: Array[Int],
                    lengths: Array[Int], starts: Array[Int], tailStart: Int, tailLength: Int): EncodedAppendDelta =
    if expectedSliceLength < 0 || expectedSliceLength > newSliceLength then
      invalid(s"Invalid slice lengths: expected $expectedSliceLength, new $newSliceLength")
    var prevEnd = 0L
    var i = 0
    while i < offsets.length do
      val end = offsets(i).toLong + lengths(i)
      if offsets(i) < 0 then
        invalid(s"Invalid region offset ${offsets(i)}")
      if lengths(i) == 0 then
        invalid("Empty XOR region")
      if offsets(i) < prevEnd then
        invalid(s"XOR region at ${offsets(i)} is unsorted or overlaps a previous region")
      if end > newSliceLength then
        invalid(s"XOR region (${offsets(i)} + ${lengths(i)}) extends past the new slice length $newSliceLength")
      prevEnd = end
      i += 1
    if tailLength > expectedSliceLength then
      invalid("Expected tail is longer than the expected slice")
    new EncodedAppendDelta(bb, expectedSliceLength, newSliceLength, offsets, lengths, starts, tailStart, tailLength)
