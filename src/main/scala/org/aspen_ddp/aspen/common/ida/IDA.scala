package org.aspen_ddp.aspen.common.ida

import java.nio.ByteBuffer

import com.backblaze.erasure.ReedSolomon as RSCodec
import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import org.aspen_ddp.aspen.common.DataBuffer
import scala.language.implicitConversions

object IDA {
  val ReplicationCode: Byte = 0
  val ReedSolomonCode: Byte = 1

  val EncodedIDASize: Int = 4

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

      case _ => throw new IDAEncodingError
    }
  }
}

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

  /** Returns the number of bytes needed to serialize the IDA type. */
  def getSerializedIDATypeLength: Int

  /** Serializes the IDA type into the ByteBuffer such that when the IDA type is decoded, an identical copy
    *  of this class is returned.
    */
  def serializeIDAType(bb: ByteBuffer): Unit
}

case class Replication(width: Int, writeThreshold: Int) extends IDA {

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

  def encode(objectContent: Array[Byte]): Array[Array[Byte]] =
    encodeToShards(objectContent, 0, objectContent.length)

  def encode(objectContent: DataBuffer): Array[DataBuffer] =
    val arr = objectContent.getDirectByteArray
    encodeToShards(arr, 0, arr.length).map(shard => DataBuffer(shard))

  def encodeInto(objectContent: DataBuffer, bbArray: Array[ByteBuffer]): Unit =
    require(bbArray.length == width,
      s"encodeInto requires exactly $width buffers. Got ${bbArray.length}")
    val arr = objectContent.getDirectByteArray
    val shards = encodeToShards(arr, 0, arr.length)
    var i = 0
    while i < width do
      bbArray(i).put(shards(i))
      i += 1

  def restoreArray(segments: List[(Byte, Array[Byte])]): Array[Byte] =
    restoreToArray(segments)

  def restore(segments: List[(Byte, DataBuffer)]): DataBuffer =
    DataBuffer(restoreToArray(segments.map(t => (t._1, t._2.getDirectByteArray))))

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

  /** Scatters the supplied segments into encoding-index order, recovers any missing data shards,
    * and strips the padding.
    *
    * The segment list arrives sparse and in arbitrary order. It may also hold more than
    * restoreThreshold entries and may repeat an index. Duplicates are tolerated with the first
    * occurrence winning.
    */
  private def restoreToArray(segments: List[(Byte, Array[Byte])]): Array[Byte] =
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
        codec.decodeMissing(shards, present, 0, shardSize)
      catch
        case e: IllegalArgumentException =>
          throw new IDARestoreError(s"Reed-Solomon decode failed: ${e.getMessage}")

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
