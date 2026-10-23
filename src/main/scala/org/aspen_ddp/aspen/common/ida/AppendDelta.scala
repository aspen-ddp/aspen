package org.aspen_ddp.aspen.common.ida

import java.nio.ByteBuffer
import java.util.Arrays

import org.aspen_ddp.aspen.common.DataBuffer

/** The per-store payload for appending to an object. The store checks that its slice is exactly
  * expectedSliceLength bytes long (and, for the store holding the trailer, that the final bytes
  * of the slice equal expectedTail) and then zero-extends the slice to newSliceLength and XORs
  * xorBytes into it starting at xorOffset.
  */
final case class AppendDelta(
    expectedSliceLength: Int,
    newSliceLength: Int,
    xorOffset: Int,
    xorBytes: Array[Byte],
    expectedTail: Option[Array[Byte]]):

  require(expectedSliceLength >= 0 && expectedSliceLength <= newSliceLength,
    s"Invalid slice lengths: expected $expectedSliceLength, new $newSliceLength")
  require(xorOffset >= 0, s"Invalid xorOffset $xorOffset")
  require(xorOffset.toLong + xorBytes.length <= newSliceLength,
    s"XOR range ($xorOffset + ${xorBytes.length}) exceeds new slice length $newSliceLength")
  require(expectedTail.forall(_.length <= expectedSliceLength),
    "Expected tail is longer than the expected slice")

  def matches(slice: DataBuffer): Boolean =
    slice.size == expectedSliceLength && (expectedTail match
      case None => true
      case Some(tail) =>
        val arr = slice.getByteArray
        tail.length <= arr.length &&
          Arrays.equals(arr, arr.length - tail.length, arr.length, tail, 0, tail.length))

  def applyTo(slice: DataBuffer): DataBuffer =
    require(matches(slice), "Append delta does not match the current slice")
    val arr = Arrays.copyOf(slice.getByteArray, newSliceLength)
    var i = 0
    while i < xorBytes.length do
      arr(xorOffset + i) = (arr(xorOffset + i) ^ xorBytes(i)).toByte
      i += 1
    DataBuffer(arr)

  def encode: DataBuffer =
    val tailLen = expectedTail.map(_.length).getOrElse(0)
    val bb = ByteBuffer.allocate(AppendDelta.HeaderSize + xorBytes.length + tailLen)
    bb.put(AppendDelta.FormatVersion)
    bb.putInt(expectedSliceLength)
    bb.putInt(newSliceLength)
    bb.putInt(xorOffset)
    bb.putInt(xorBytes.length)
    bb.put(if expectedTail.isDefined then 1.toByte else 0.toByte)
    bb.putInt(tailLen)
    bb.put(xorBytes)
    expectedTail.foreach(t => bb.put(t))
    DataBuffer(bb.array())

  override def equals(other: Any): Boolean = other match
    case that: AppendDelta =>
      expectedSliceLength == that.expectedSliceLength &&
        newSliceLength == that.newSliceLength &&
        xorOffset == that.xorOffset &&
        Arrays.equals(xorBytes, that.xorBytes) &&
        (expectedTail, that.expectedTail).match
          case (None, None) => true
          case (Some(a), Some(b)) => Arrays.equals(a, b)
          case _ => false
    case _ => false

  override def hashCode: Int =
    var h = Arrays.hashCode(xorBytes)
    h = 31 * h + expectedSliceLength
    h = 31 * h + newSliceLength
    h = 31 * h + xorOffset
    31 * h + expectedTail.map(Arrays.hashCode).getOrElse(0)


object AppendDelta:

  val FormatVersion: Byte = 1

  // <version:1><expectedSliceLength:4><newSliceLength:4><xorOffset:4><xorLength:4><hasTail:1><tailLength:4>
  private val HeaderSize: Int = 22

  def decode(db: DataBuffer): AppendDelta =
    val bb = ByteBuffer.wrap(db.getByteArray)
    if bb.remaining < HeaderSize then
      throw new IllegalArgumentException("Truncated append delta")
    if bb.get() != FormatVersion then
      throw new IllegalArgumentException("Unsupported append delta format")
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
    val tail = if hasTail == 1 then
      val t = new Array[Byte](tailLength)
      bb.get(t)
      Some(t)
    else
      None

    AppendDelta(expectedSliceLength, newSliceLength, xorOffset, xorBytes, tail)
