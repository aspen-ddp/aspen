package org.aspen_ddp.aspen.common.ida

import java.nio.ByteBuffer

import com.backblaze.erasure.ReedSolomon as RSCodec
import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import org.aspen_ddp.aspen.common.DataBuffer
import scala.language.implicitConversions

object IDA {
  val ReplicationCode: Byte = 0
  val ReedSolomonCode: Byte = 1
  val AppendingReedSolomonCode: Byte = 2

  /** Deserializes the IDA type and returns the matching IDA instance */
  def deserializeIDAType(bb: ByteBuffer): IDA = {
    val typeCode = bb.get()

    typeCode match {
      case ReplicationCode =>
        val width = bb.get()
        val writeThreshold = bb.get()
        bb.get() // discard empty byte
        Replication(width, writeThreshold)

      case ReedSolomonCode =>
        val width = bb.get()
        val readThreshold = bb.get()
        val writeThreshold = bb.get()
        ReedSolomon(width, readThreshold, writeThreshold)

      case AppendingReedSolomonCode =>
        val width = bb.get()
        val readThreshold = bb.get()
        val writeThreshold = bb.get()
        val cellSize = bb.getShort() & 0xFFFF
        AppendingReedSolomon(width, readThreshold, writeThreshold, cellSize)

      case _ => throw new IDAEncodingError
    }
  }
}

/** IDAs whose slices can be extended in place by XOR-ing a per-store AppendDelta into them */
sealed trait AppendableIDA:
  this: IDA =>

  /** One delta per encoding index, for appending `data` (non-empty) to an object of logical length `currentLength` */
  def appendDeltas(currentLength: Int, data: Array[Byte]): Array[AppendDelta]

  /** One delta per encoding index, for overwriting byte ranges of an object of logical length
    * `currentLength`. Edits must be non-empty and non-overlapping. An edit may begin beyond the current
    * end, in which case the gap is zero filled. The new length is the larger of the current length
    * and the furthest edit end. Only the old bytes under each edit are needed: by linearity the slice
    * deltas depend only on the data deltas (old XOR new at the edited bytes, zero elsewhere).
    */
  def rangedWriteDeltas(currentLength: Int, edits: List[RangeEdit]): Array[AppendDelta]

  /** Row-aligned slice offset at which logical byte `offset` begins */
  def sliceOffsetFor(offset: Int): Int

  /** Row-aligned slice range [start, end) covering logical [offset, offset + length). Stores clip the end to the
    * slice size. A restored window that does not reach the end of the slices may contain zero fill only in its
    * last row, beyond the requested end, which callers trim to the request.
    */
  def sliceRangeFor(offset: Int, length: Int): (Int, Int)

  /** Bytes from the end of each slice that are needed to learn the logical length: cellSize, or 0 for Replication */
  def lengthProbeBytes: Int

  /** Decodes equal-length slice segments that start at slice offset `sliceStart`, where `sliceSize` is the full slice length */
  def restoreRange(sliceStart: Int, sliceSize: Int, segments: List[(Byte, Array[Byte])]): RestoredRange

/** Result of restoring a slice range. `data` begins at logical byte `logicalOffset`. `objectLength` is
  * known only when the range included the end of the slices.
  */
final case class RestoredRange(logicalOffset: Int, data: Array[Byte], objectLength: Option[Int]):
  override def equals(that: Any): Boolean = that match
    case r: RestoredRange =>
      logicalOffset == r.logicalOffset && objectLength == r.objectLength && java.util.Arrays.equals(data, r.data)
    case _ => false

  override def hashCode(): Int =
    31 * (31 * logicalOffset + objectLength.hashCode()) + java.util.Arrays.hashCode(data)

sealed abstract class IDA extends Ordered[IDA] {

  /** Algorithm name like "replication" or "reed-solomon" */
  def name: String
  
  /** Number of slices/replicas */
  def width: Int

  /** Minimum number of slices needed to restore the object to a readable state.
    * This value may be less than consistentRestoreThreshold.
    */
  def restoreThreshold: Int

  /** Minimum number of slices/replicas that must agree on the current object revision in order to guarantee
    * consistency.
    */
  def consistentRestoreThreshold: Int

  /** Minimum number of slices/replicas that must be successfully written for a successful update transaction */
  def writeThreshold: Int

  /** Restores the data or throws an Exception if the restore operation fails.
    *  Accepts a list of (EncodingIndex, Option[DataBuffer]).
    *  Where the encoding index is the index of this data within the corresponding encode() call
    *
    *  Note that any padding required to achieve alignment requirements must be transparently handled by
    *  the encode/restore methods.
    */
  def restore(segments: List[(Byte,DataBuffer)]): DataBuffer

  /** Restores the data or throws an Exception if the restore operation fails.
    *  Accepts a list of (EncodingIndex, Option[DataBuffer]).
    *  Where the encoding index is the index of this data within the corresponding encode() call
    *
    *  Note that any padding required to achieve alignment requirements must be transparently handled by
    *  the encode/restore methods.
    */
  def restoreArray(segments: List[(Byte, Array[Byte])]): Array[Byte]

  /** Encodes the object into an array of ByteBuffers
    *
    *  Note that the indices of this array are known as the EncodingIndex and are significant to the
    *  corresponding decode operation. The correct index in this array must be used during the decoding
    *  process.
    *
    *  Note that any padding required to achieve alignment requirements must be transparently handled by
    *  the encode/decode methods.
    */
  def encode(objectContent: DataBuffer): Array[DataBuffer]

  def encode(objectContent: Array[Byte]): Array[Array[Byte]]

  /** Encodes the object into the provided array of ByteBuffers. The the size of the array must match the
    *  IDA width and each buffer must support writing at least calculateEncodedSegmentLength bytes.
    */
  def encodeInto(objectContent: DataBuffer, bbArray: Array[ByteBuffer]): Unit

  def failureTolerance: Int = width - writeThreshold

  def compare(that: IDA): Int = failureTolerance - that.failureTolerance

  /** Returns length of the DataBuffers that would be returned by calling encode() on the provided number of bytes */
  def calculateEncodedSegmentLength(nbytes: Int): Int

  /** Largest object size, in bytes, whose slices each fit within `maxSliceSize` bytes. This is the
    * limit applications should code against.
    *
    * It is the exact inverse of `calculateEncodedSegmentLength`: the result is the largest n for which
    * `calculateEncodedSegmentLength(n) <= maxSliceSize`, capped at `Int.MaxValue` (object lengths are
    * Ints). Each IDA derives it from its own slice layout:
    *
    *   - Replication: every slice is a full copy, so n = maxSliceSize
    *   - ReedSolomon: slice = n / k + 1 bytes (k = restoreThreshold, with at least one pad byte),
    *     so n = k * maxSliceSize - 1
    *   - AppendingReedSolomon: slices are whole rows of cellSize bytes and the stream holds the content
    *     plus an 8 byte trailer, so n = (maxSliceSize / cellSize) * rowBytes - 8, where
    *     rowBytes = k * cellSize. Capped at Int.MaxValue - rowBytes, the largest length supported by
    *     appends and ranged writes.
    *
    * @throws IllegalArgumentException if `maxSliceSize < minSliceSize`
    */
  def maxObjectSize(maxSliceSize: Long): Int

  /** Smallest `maxSliceSize` that can hold the slices of an empty object. Smaller values admit no
    * objects at all and are rejected by `maxObjectSize`.
    */
  def minSliceSize: Int

  /** Returns the number of bytes needed to serialize the IDA type. */
  def getSerializedIDATypeLength: Int

  /** Serializes the IDA type into the ByteBuffer such that when the IDA type is decoded, an identical copy
    *  of this class is returned.
    */
  def serializeIDAType(bb: ByteBuffer): Unit
}

case class Replication(width: Int, writeThreshold: Int) extends IDA with AppendableIDA {

  def appendDeltas(currentLength: Int, data: Array[Byte]): Array[AppendDelta] =
    require(data.length > 0, "Append data must not be empty")
    Array.fill(width)(AppendDelta(currentLength, currentLength + data.length, currentLength, data, None))

  /** Each region is exactly an edit's XOR delta: old ^ new over the existing content and the new bytes
    * beyond it. Edits that touch are merged. A gap beyond the old end needs no bytes since the store
    * zero-extends the slice.
    */
  def rangedWriteDeltas(currentLength: Int, edits: List[RangeEdit]): Array[AppendDelta] =
    val (sorted, newLength) = RangeEdit.validate(currentLength, edits)
    require(newLength <= Int.MaxValue, "Object length overflow")

    val regions = sorted.foldRight(List.empty[XorRegion]) { (e, acc) =>
      val bytes = Array.tabulate(e.newBytes.length)(j => e.deltaAt(e.offset + j))
      acc match
        case next :: rest if next.offset == e.end => XorRegion(e.offset, bytes ++ next.bytes) :: rest
        case _ => XorRegion(e.offset, bytes) :: acc
    }

    Array.fill(width)(AppendDelta(currentLength, newLength.toInt, regions, None))

  def sliceOffsetFor(offset: Int): Int = offset

  def sliceRangeFor(offset: Int, length: Int): (Int, Int) =
    (offset, math.min(offset.toLong + length, Int.MaxValue.toLong).toInt)

  def lengthProbeBytes: Int = 0

  def restoreRange(sliceStart: Int, sliceSize: Int, segments: List[(Byte, Array[Byte])]): RestoredRange =
    if segments.size < restoreThreshold then
      throw new IDARestoreError
    RestoredRange(sliceStart, segments.head._2, Some(sliceSize))

  def name: String = "replication"
  
  def restoreThreshold: Int = 1

  def consistentRestoreThreshold: Int = width / 2 + 1

  def restore(segments: List[(Byte,DataBuffer)]): DataBuffer = if (segments.size < restoreThreshold) throw new IDARestoreError else segments.head._2

  def restoreArray(segments: List[(Byte, Array[Byte])]): Array[Byte] = if (segments.size < restoreThreshold) throw new IDARestoreError else segments.head._2

  def encode(objectContent: DataBuffer): Array[DataBuffer] = {
    val arr = new Array[DataBuffer](width)
    for (i <- 0 until width)
      arr(i) = objectContent
    arr
  }

  def encode(objectContent: Array[Byte]): Array[Array[Byte]] = {
    val arr = new Array[Array[Byte]](width)
    for (i <- 0 until width)
      arr(i) = objectContent
    arr
  }

  /** Terribly inefficient. Avoid the use of this method when using replication */
  def encodeInto(objectContent: DataBuffer, bbArray: Array[ByteBuffer]): Unit = {
    for (bb <- bbArray)
      bb.put(objectContent)
  }

  def calculateEncodedSegmentLength(nbytes: Int): Int = nbytes

  /** Every slice is a full copy of the object, so the object size equals the slice size */
  def maxObjectSize(maxSliceSize: Long): Int =
    require(maxSliceSize >= minSliceSize,
      s"maxSliceSize ($maxSliceSize) must be at least $minSliceSize")
    math.min(maxSliceSize, Int.MaxValue.toLong).toInt

  /** Zero would admit only empty objects, which is never a meaningful limit, so a slice limit must be positive */
  def minSliceSize: Int = 1

  def getSerializedIDATypeLength: Int = 4 // <type><width><writeThreshold>

  def serializeIDAType(bb: ByteBuffer): Unit = {
    bb.put(IDA.ReplicationCode)
    bb.put(width.toByte)
    bb.put(writeThreshold.toByte)
    bb.put(0.toByte) // reserved
  }
}

object ReedSolomon:

  /** Largest supported width.
    *
    * Encoding indices travel through the system as signed Bytes (StoreId.poolIndex) and are
    * compared against the IDA width with signed arithmetic, so they must stay within [0, 127].
    * This is stricter than the underlying library's 256-shard limit and it also guarantees that
    * the pad count, which is always in 1..restoreThreshold, fits within a single byte.
    */
  val MaxWidth: Int = 127

  /** Constructing a codec builds and inverts a Vandermonde matrix, which is O(k^3). ReedSolomon
    * instances, on the other hand, are cheap and are created constantly - every decode of a
    * serialized ObjectPointer or storage pool configuration makes a new one. Cache the codecs
    * on the shard counts so the matrix work is done once per distinct configuration.
    *
    * Backblaze codecs are immutable and safe to share across threads.
    */
  private val codecCache: LoadingCache[(Int, Int), RSCodec] = Scaffeine().
    maximumSize(128).
    build[(Int, Int), RSCodec]((key: (Int, Int)) => RSCodec.create(key._1, key._2))

  private[ida] def codecFor(dataShards: Int, parityShards: Int): RSCodec =
    codecCache.get((dataShards, parityShards))

  /** Scatters the supplied segments into encoding-index order and recovers any missing data shards.
    * Returns the shards (the first k of which are all populated) and the common slice length.
    *
    * The segment list arrives sparse and in arbitrary order. It may also hold more than
    * restoreThreshold entries and may repeat an index. Duplicates are tolerated with the first
    * occurrence winning.
    */
  private[ida] def recoverDataShards(
      segments: List[(Byte, Array[Byte])],
      width: Int,
      k: Int): (Array[Array[Byte]], Int) =
    val shards = new Array[Array[Byte]](width)
    var nDistinct = 0
    var shardSize = -1

    segments.foreach { case (encodingIndex, slice) =>
      // An index outside the valid range means the slice was produced under a different pool
      // width. That is a configuration error rather than a recoverable loss, so fail loudly
      // instead of silently discarding the slice
      val i = encodingIndex.toInt

      if i < 0 || i >= width then
        throw new IDARestoreError(s"Encoding index $i falls outside the valid range [0, $width)")

      if shardSize < 0 then
        shardSize = slice.length
      else if slice.length != shardSize then
        throw new IDARestoreError(
          s"Slice $i has length ${slice.length} but a length of $shardSize was expected")

      if shards(i) == null then
        shards(i) = slice
        nDistinct += 1
    }

    if nDistinct < k then
      throw new IDARestoreError(s"Insufficient slices for restore. Have $nDistinct, need $k")

    if shardSize < 1 then
      throw new IDARestoreError(s"Invalid slice length $shardSize. Every slice holds at least one pad byte")

    var haveAllDataShards = true
    var i = 0
    while i < k do
      if shards(i) == null then
        haveAllDataShards = false
      i += 1

    if !haveAllDataShards then
      val present = new Array[Boolean](width)
      i = 0
      while i < width do
        if shards(i) == null then
          shards(i) = new Array[Byte](shardSize)
        else
          present(i) = true
        i += 1

      // decodeMissing only writes to shards flagged absent, so the caller's arrays are left
      // untouched. It also regenerates the missing parity shards, which we simply discard
      try
        codecFor(k, width - k).decodeMissing(shards, present, 0, shardSize)
      catch
        case e: IllegalArgumentException =>
          throw new IDARestoreError(s"Reed-Solomon decode failed: ${e.getMessage}")

    (shards, shardSize)


/** Systematic Reed-Solomon erasure code over GF(2^8), implemented in terms of the Backblaze
  * JavaReedSolomon library.
  *
  * Encoding indices 0 until restoreThreshold hold data shards and are byte-for-byte slices of
  * the padded object content. The remaining indices hold parity shards. The code is Maximum
  * Distance Separable, so the original content is recoverable from *any* restoreThreshold
  * slices, be they data slices, parity slices, or a mix of the two.
  *
  * Padding is PKCS#7 style and is applied unconditionally, even when the content length already
  * divides evenly by restoreThreshold. Given k = restoreThreshold and n content bytes:
  *
  *   padBytes  = k - (n % k)      always in 1..k
  *   paddedLen = n + padBytes     always an exact multiple of k
  *   shardSize = paddedLen / k    == n / k + 1
  *
  * and every pad byte holds the value padBytes. Because paddedLen is exactly k * shardSize, the
  * final byte of data shard k-1 is always the pad count, which is what restore uses to recover
  * the original length. Note that the padding is not confined to the last shard: when padBytes
  * exceeds shardSize the padding spans several trailing data shards.
  */
case class ReedSolomon(width: Int, restoreThreshold: Int, writeThreshold: Int) extends IDA:

  require(restoreThreshold >= 1,
    s"Reed-Solomon restoreThreshold must be at least 1. Got $restoreThreshold")
  require(restoreThreshold <= width,
    s"Reed-Solomon restoreThreshold ($restoreThreshold) may not exceed width ($width)")
  require(width <= ReedSolomon.MaxWidth,
    s"Reed-Solomon width ($width) may not exceed ${ReedSolomon.MaxWidth}")
  require(writeThreshold >= restoreThreshold && writeThreshold <= width,
    s"Reed-Solomon writeThreshold ($writeThreshold) must fall within [$restoreThreshold, $width]")

  def name: String = "reed-solomon"

  def consistentRestoreThreshold: Int = restoreThreshold

  private def k: Int = restoreThreshold

  private def nparity: Int = width - restoreThreshold

  private def codec: RSCodec = ReedSolomon.codecFor(k, nparity)

  def calculateEncodedSegmentLength(nbytes: Int): Int = nbytes / k + 1

  /** Slice = n / k + 1, so the largest n with a slice of at most maxSliceSize bytes is k * maxSliceSize - 1 */
  def maxObjectSize(maxSliceSize: Long): Int =
    require(maxSliceSize >= minSliceSize,
      s"maxSliceSize ($maxSliceSize) must be at least $minSliceSize")
    // Divide first so the product cannot overflow a Long
    if maxSliceSize > (Int.MaxValue.toLong + 1) / k then
      Int.MaxValue
    else
      math.min(k * maxSliceSize - 1, Int.MaxValue.toLong).toInt

  /** Even an empty object has one pad byte in each slice */
  def minSliceSize: Int = 1

  def encode(objectContent: Array[Byte]): Array[Array[Byte]] =
    encodeToShards(objectContent, 0, objectContent.length)

  def encode(objectContent: DataBuffer): Array[DataBuffer] =
    val arr = objectContent.getByteArray
    encodeToShards(arr, 0, arr.length).map(shard => DataBuffer(shard))

  def encodeInto(objectContent: DataBuffer, bbArray: Array[ByteBuffer]): Unit =
    require(bbArray.length == width,
      s"encodeInto requires exactly $width buffers. Got ${bbArray.length}")
    val arr = objectContent.getByteArray
    val shards = encodeToShards(arr, 0, arr.length)
    var i = 0
    while i < width do
      bbArray(i).put(shards(i))
      i += 1

  def restoreArray(segments: List[(Byte, Array[Byte])]): Array[Byte] =
    restoreToArray(segments)

  def restore(segments: List[(Byte, DataBuffer)]): DataBuffer =
    DataBuffer(restoreToArray(segments.map(t => (t._1, t._2.getByteArray))))

  /** Pads the content out to an exact multiple of k, slices it into the k data shards, and
    * calculates the parity shards
    */
  private def encodeToShards(src: Array[Byte], offset: Int, length: Int): Array[Array[Byte]] =
    val shardSize = length / k + 1
    val padValue = (k - (length % k)).toByte // pad count, always 1..k
    val shards = Array.ofDim[Byte](width, shardSize)

    var i = 0
    while i < k do
      val shardFrom = i * shardSize
      val nReal = math.max(0, math.min(shardSize, length - shardFrom))

      if nReal > 0 then
        System.arraycopy(src, offset + shardFrom, shards(i), 0, nReal)

      // Padding is not confined to the last shard. When padValue exceeds shardSize it spans
      // several trailing data shards, so fill per-shard rather than appending to shard k-1
      var j = nReal
      while j < shardSize do
        shards(i)(j) = padValue
        j += 1

      i += 1

    if nparity > 0 then
      codec.encodeParity(shards, 0, shardSize)

    shards

  private def restoreToArray(segments: List[(Byte, Array[Byte])]): Array[Byte] =
    val (shards, shardSize) = ReedSolomon.recoverDataShards(segments, width, k)
    concatAndStrip(shards, shardSize)

  /** Concatenates the data shards and removes the trailing padding */
  private def concatAndStrip(shards: Array[Array[Byte]], shardSize: Int): Array[Byte] =
    // The padded length is exactly k * shardSize, so the pad count is always the final byte of
    // data shard k-1
    val padBytes = shards(k - 1)(shardSize - 1) & 0xFF

    if padBytes < 1 || padBytes > k then
      throw new IDARestoreError(s"Invalid padding count $padBytes. Expected a value within 1..$k")

    val nbytes = k * shardSize - padBytes
    val arr = new Array[Byte](nbytes)

    var i = 0
    while i < k do
      val offset = i * shardSize
      // nCopy is non-positive for trailing shards that hold nothing but padding
      val nCopy = math.min(shardSize, nbytes - offset)
      if nCopy > 0 then
        System.arraycopy(shards(i), 0, arr, offset, nCopy)
      i += 1

    arr

  def getSerializedIDATypeLength: Int = 4 // <type><width><restoreThreshold><writeThreshold>

  def serializeIDAType(bb: ByteBuffer): Unit =
    bb.put(IDA.ReedSolomonCode)
    bb.put(width.toByte)
    bb.put(restoreThreshold.toByte)
    bb.put(writeThreshold.toByte)


object AppendingReedSolomon:

  val DefaultCellSize: Int = 64

  /** The cell size is serialized as an unsigned 16-bit value */
  val MaxCellSize: Int = 0xFFFF

  /** Big-endian logical length stored in the last 8 bytes of the final row */
  val TrailerSize: Int = 8


/** Systematic Reed-Solomon code with a row-interleaved layout that supports appending without
  * rewriting existing data.
  *
  * The logical stream is the content, then zeros, then an 8-byte big-endian content length in
  * the last 8 bytes of the final row. The stream is laid out in rows of rowBytes = k * cellSize
  * bytes. Stream byte p lands in data shard (p % rowBytes) / cellSize at slice offset
  * (p / rowBytes) * cellSize + p % cellSize. Appending therefore only extends the slices and
  * rewrites the tail. The trailer ends up in the last 8 bytes of data shard k-1's slice. Parity
  * shards are computed independently over each column of the slices.
  *
  * Unlike ReedSolomon, the slice length is always a multiple of cellSize.
  */
case class AppendingReedSolomon(
    width: Int,
    restoreThreshold: Int,
    writeThreshold: Int,
    cellSize: Int = AppendingReedSolomon.DefaultCellSize) extends IDA with AppendableIDA:

  require(restoreThreshold >= 1,
    s"Appending Reed-Solomon restoreThreshold must be at least 1. Got $restoreThreshold")
  require(restoreThreshold <= width,
    s"Appending Reed-Solomon restoreThreshold ($restoreThreshold) may not exceed width ($width)")
  require(width <= ReedSolomon.MaxWidth,
    s"Appending Reed-Solomon width ($width) may not exceed ${ReedSolomon.MaxWidth}")
  require(writeThreshold >= restoreThreshold && writeThreshold <= width,
    s"Appending Reed-Solomon writeThreshold ($writeThreshold) must fall within [$restoreThreshold, $width]")
  require(cellSize >= AppendingReedSolomon.TrailerSize,
    s"Appending Reed-Solomon cellSize ($cellSize) must be at least ${AppendingReedSolomon.TrailerSize}")
  require(cellSize <= AppendingReedSolomon.MaxCellSize,
    s"Appending Reed-Solomon cellSize ($cellSize) may not exceed ${AppendingReedSolomon.MaxCellSize}")

  def name: String = "appending-reed-solomon"

  def consistentRestoreThreshold: Int = restoreThreshold

  private def k: Int = restoreThreshold

  private def nparity: Int = width - restoreThreshold

  /** Number of stream bytes in each row */
  def rowBytes: Int = restoreThreshold * cellSize

  /** Smallest row count that holds nbytes of content plus the trailer */
  def rowsFor(nbytes: Int): Int =
    ((nbytes.toLong + AppendingReedSolomon.TrailerSize + rowBytes - 1) / rowBytes).toInt

  def calculateEncodedSegmentLength(nbytes: Int): Int = rowsFor(nbytes) * cellSize

  /** Slices hold whole rows of cellSize bytes, so maxSliceSize / cellSize rows fit. Each row carries
    * rowBytes stream bytes of which the final 8 are the trailer, giving
    * n = (maxSliceSize / cellSize) * rowBytes - 8. Capped at Int.MaxValue - rowBytes, the bound
    * enforced by appends and ranged writes.
    */
  def maxObjectSize(maxSliceSize: Long): Int =
    require(maxSliceSize >= minSliceSize,
      s"maxSliceSize ($maxSliceSize) must be at least $minSliceSize")
    val cap = Int.MaxValue.toLong - rowBytes
    val rows = maxSliceSize / cellSize
    if rows > (cap + AppendingReedSolomon.TrailerSize) / rowBytes + 1 then
      cap.toInt
    else
      math.min(rows * rowBytes - AppendingReedSolomon.TrailerSize, cap).toInt

  /** An empty object still occupies one row, whose slices are cellSize bytes */
  def minSliceSize: Int = cellSize

  def encode(objectContent: Array[Byte]): Array[Array[Byte]] =
    encodeToShards(objectContent, 0, objectContent.length)

  def encode(objectContent: DataBuffer): Array[DataBuffer] =
    val arr = objectContent.getByteArray
    encodeToShards(arr, 0, arr.length).map(shard => DataBuffer(shard))

  def encodeInto(objectContent: DataBuffer, bbArray: Array[ByteBuffer]): Unit =
    require(bbArray.length == width,
      s"encodeInto requires exactly $width buffers. Got ${bbArray.length}")
    val arr = objectContent.getByteArray
    val shards = encodeToShards(arr, 0, arr.length)
    var i = 0
    while i < width do
      bbArray(i).put(shards(i))
      i += 1

  def restoreArray(segments: List[(Byte, Array[Byte])]): Array[Byte] =
    restoreToArray(segments)

  def restore(segments: List[(Byte, DataBuffer)]): DataBuffer =
    DataBuffer(restoreToArray(segments.map(t => (t._1, t._2.getByteArray))))

  /** Scatters the content into the data shards cell by cell, writes the trailer, and calculates
    * the parity shards
    */
  private def encodeToShards(src: Array[Byte], offset: Int, length: Int): Array[Array[Byte]] =
    val rows = rowsFor(length)
    val sliceLength = rows * cellSize
    val shards = scatter(src, offset, length, rows)

    ByteBuffer.wrap(shards(k - 1)).putLong(sliceLength - AppendingReedSolomon.TrailerSize, length.toLong)

    addParity(shards, sliceLength)

  /** Allocates width shards of the given row count and scatters length stream bytes from src into
    * the data shards. Stream bytes beyond length are left zero.
    */
  private def scatter(src: Array[Byte], offset: Int, length: Int, rows: Int): Array[Array[Byte]] =
    val shards = Array.ofDim[Byte](width, rows * cellSize)

    var row = 0
    while row < rows do
      var i = 0
      while i < k do
        val streamPos = row * rowBytes + i * cellSize
        val nReal = math.min(cellSize, length - streamPos)
        if nReal > 0 then
          System.arraycopy(src, offset + streamPos, shards(i), row * cellSize, nReal)
        i += 1
      row += 1

    shards

  private def addParity(shards: Array[Array[Byte]], sliceLength: Int): Array[Array[Byte]] =
    if nparity > 0 then
      ReedSolomon.codecFor(k, nparity).encodeParity(shards, 0, sliceLength)
    shards

  /** Builds the XOR delta stream covering the old final row through the new final row and encodes
    * it with the same layout and code as the content. Because the code is linear, the encoded delta
    * XORed into the current slices yields exactly the slices of the extended object.
    */
  def appendDeltas(currentLength: Int, data: Array[Byte]): Array[AppendDelta] =
    require(data.length > 0, "Append data must not be empty")
    require(currentLength >= 0, s"Invalid current length $currentLength")
    require(currentLength.toLong + data.length <= Int.MaxValue - rowBytes, "Object length overflow")

    val newLength = currentLength + data.length
    val oldRows = rowsFor(currentLength)
    val newRows = rowsFor(newLength)
    val startRow = currentLength / rowBytes
    val base = startRow * rowBytes
    val deltaRows = newRows - startRow
    val stream = new Array[Byte](deltaRows * rowBytes)

    System.arraycopy(data, 0, stream, currentLength - base, data.length)
    xorLong(stream, oldRows * rowBytes - AppendingReedSolomon.TrailerSize - base, currentLength.toLong)
    xorLong(stream, newRows * rowBytes - AppendingReedSolomon.TrailerSize - base, newLength.toLong)

    val shards = addParity(scatter(stream, 0, stream.length, deltaRows), deltaRows * cellSize)

    val oldTrailer = ByteBuffer.allocate(AppendingReedSolomon.TrailerSize).putLong(currentLength.toLong).array()

    Array.tabulate(width) { i =>
      AppendDelta(oldRows * cellSize, newRows * cellSize, startRow * cellSize, shards(i),
        if i == k - 1 then Some(oldTrailer) else None)
    }

  /** Sparse, row by row: only rows holding an edited byte, plus the old and new trailer rows when the
    * length changes, contribute. Runs of adjacent affected rows are encoded together as an XOR delta
    * stream and each store receives that run's cells, trimmed to their nonzero span. The store
    * holding the trailer always receives the old trailer as the expected tail.
    */
  def rangedWriteDeltas(currentLength: Int, edits: List[RangeEdit]): Array[AppendDelta] =
    val (sorted, newLengthL) = RangeEdit.validate(currentLength, edits)
    require(newLengthL <= Int.MaxValue - rowBytes, "Object length overflow")

    val newLength = newLengthL.toInt
    val oldRows = rowsFor(currentLength)
    val newRows = rowsFor(newLength)
    val lengthChanges = newLength != currentLength

    // Inclusive row intervals, merged where they overlap or are adjacent
    val editRows = sorted.map(e => (e.offset / rowBytes, ((e.end - 1) / rowBytes).toInt))
    val trailerRows = if lengthChanges then List((oldRows - 1, oldRows - 1), (newRows - 1, newRows - 1)) else Nil
    val runs = (editRows ++ trailerRows).sortBy(_._1).foldLeft(List.empty[(Int, Int)]) { (acc, r) =>
      acc match
        case (a, b) :: rest if r._1 <= b + 1 => (a, math.max(b, r._2)) :: rest
        case _ => r :: acc
    }.reverse

    val regions = Array.fill(width)(List.newBuilder[XorRegion])

    runs.foreach { (firstRow, lastRow) =>
      val nrows = lastRow - firstRow + 1
      val base = firstRow.toLong * rowBytes
      val stream = new Array[Byte](nrows * rowBytes)

      sorted.foreach { e =>
        val from = math.max(e.offset.toLong, base)
        val to = math.min(e.end, base + stream.length)
        var p = from
        while p < to do
          stream((p - base).toInt) = e.deltaAt(p.toInt)
          p += 1
      }

      // Each trailer lies entirely within its final row
      if lengthChanges then
        if oldRows - 1 >= firstRow && oldRows - 1 <= lastRow then
          xorLong(stream, (oldRows.toLong * rowBytes - AppendingReedSolomon.TrailerSize - base).toInt, currentLength.toLong)
        if newRows - 1 >= firstRow && newRows - 1 <= lastRow then
          xorLong(stream, (newRows.toLong * rowBytes - AppendingReedSolomon.TrailerSize - base).toInt, newLength.toLong)

      val shards = addParity(scatter(stream, 0, stream.length, nrows), nrows * cellSize)

      var i = 0
      while i < width do
        val cells = shards(i)
        var lo = 0
        while lo < cells.length && cells(lo) == 0 do
          lo += 1
        if lo < cells.length then
          var hi = cells.length
          while cells(hi - 1) == 0 do
            hi -= 1
          regions(i) += XorRegion(firstRow * cellSize + lo, java.util.Arrays.copyOfRange(cells, lo, hi))
        i += 1
    }

    val oldTrailer = ByteBuffer.allocate(AppendingReedSolomon.TrailerSize).putLong(currentLength.toLong).array()

    Array.tabulate(width) { i =>
      AppendDelta(oldRows * cellSize, newRows * cellSize, regions(i).result(),
        if i == k - 1 then Some(oldTrailer) else None)
    }

  private def xorLong(arr: Array[Byte], at: Int, value: Long): Unit =
    var i = 0
    while i < AppendingReedSolomon.TrailerSize do
      arr(at + i) = (arr(at + i) ^ (value >>> (8 * (AppendingReedSolomon.TrailerSize - 1 - i))).toByte).toByte
      i += 1

  /** Recovers any missing data shards, validates the trailer, and gathers the content */
  private def restoreToArray(segments: List[(Byte, Array[Byte])]): Array[Byte] =
    val (shards, sliceLength) = ReedSolomon.recoverDataShards(segments, width, k)

    if sliceLength % cellSize != 0 then
      throw new IDARestoreError(s"Invalid slice length $sliceLength. Expected a multiple of $cellSize")

    val rows = sliceLength / cellSize
    val length = trailerLength(shards, sliceLength, rows)
    val arr = new Array[Byte](length)

    gather(shards, 0, rows, arr, 0)

    arr

  /** Reads the logical length from the trailer in the last 8 bytes of the final segment of data
    * shard k-1 and validates it against the total row count of the full slices
    */
  private def trailerLength(shards: Array[Array[Byte]], segmentLength: Int, totalRows: Int): Int =
    val nbytes = ByteBuffer.wrap(shards(k - 1)).getLong(segmentLength - AppendingReedSolomon.TrailerSize)

    // Checked as a long so a corrupt trailer cannot overflow the row arithmetic
    if nbytes < 0 || nbytes + AppendingReedSolomon.TrailerSize > totalRows.toLong * rowBytes ||
        rowsFor(nbytes.toInt) != totalRows then
      throw new IDARestoreError(s"Invalid trailer length $nbytes for a slice of $totalRows rows")

    nbytes.toInt

  /** Copies the stream bytes held by the data shards into dest. Shard row 0 corresponds to stream
    * row firstRow and dest index 0 to stream position firstRow * rowBytes - destBase. Bytes at or
    * beyond dest.length are not copied.
    */
  private def gather(shards: Array[Array[Byte]], firstRow: Int, rows: Int, dest: Array[Byte], destBase: Int): Unit =
    var row = 0
    while row < rows do
      var i = 0
      while i < k do
        val streamPos = (firstRow + row) * rowBytes + i * cellSize
        val nCopy = math.min(cellSize, dest.length + destBase - streamPos)
        if nCopy > 0 then
          System.arraycopy(shards(i), row * cellSize, dest, streamPos - destBase, nCopy)
        i += 1
      row += 1

  def sliceOffsetFor(offset: Int): Int = (offset / rowBytes) * cellSize

  def sliceRangeFor(offset: Int, length: Int): (Int, Int) =
    // One extra row so a requested end in the second-to-last row pulls in the final row, whose
    // trailer lets restoreRange truncate the zero fill. Stores clip the end to the slice size.
    // Computed in Long and clamped, since the end may exceed Int range (e.g. k=1 with a huge length)
    val endRow = (offset.toLong + length + rowBytes - 1) / rowBytes + 1
    (sliceOffsetFor(offset), math.min(endRow * cellSize, Int.MaxValue.toLong).toInt)

  def lengthProbeBytes: Int = cellSize

  def restoreRange(sliceStart: Int, sliceSize: Int, segments: List[(Byte, Array[Byte])]): RestoredRange =
    if sliceStart < 0 || sliceStart % cellSize != 0 then
      throw new IDARestoreError(s"Invalid slice start $sliceStart. Expected a multiple of $cellSize")

    if sliceSize % cellSize != 0 then
      throw new IDARestoreError(s"Invalid slice size $sliceSize. Expected a multiple of $cellSize")

    // An empty window means the read began beyond the end of the slices. The length is unknown
    if sliceStart <= sliceSize && segments.size >= k && segments.forall(_._2.isEmpty) then
      return RestoredRange((sliceStart / cellSize) * rowBytes, Array.emptyByteArray, None)

    val (shards, segmentLength) = ReedSolomon.recoverDataShards(segments, width, k)

    if segmentLength % cellSize != 0 || sliceStart.toLong + segmentLength > sliceSize then
      throw new IDARestoreError(
        s"Invalid segment length $segmentLength at slice start $sliceStart for a slice of $sliceSize bytes")

    val firstRow = sliceStart / cellSize
    val rows = segmentLength / cellSize
    val logicalOffset = firstRow * rowBytes

    if sliceStart + segmentLength == sliceSize then
      val length = trailerLength(shards, segmentLength, sliceSize / cellSize)
      val arr = new Array[Byte](math.max(0, length - logicalOffset))
      gather(shards, firstRow, rows, arr, logicalOffset)
      RestoredRange(logicalOffset, arr, Some(length))
    else
      val arr = new Array[Byte](rows * rowBytes)
      gather(shards, firstRow, rows, arr, logicalOffset)
      RestoredRange(logicalOffset, arr, None)

  def getSerializedIDATypeLength: Int = 6 // <type><width><restoreThreshold><writeThreshold><cellSize:2>

  def serializeIDAType(bb: ByteBuffer): Unit =
    bb.put(IDA.AppendingReedSolomonCode)
    bb.put(width.toByte)
    bb.put(restoreThreshold.toByte)
    bb.put(writeThreshold.toByte)
    bb.putShort(cellSize.toShort)
