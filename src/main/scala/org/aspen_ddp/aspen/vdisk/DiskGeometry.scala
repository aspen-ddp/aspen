package org.aspen_ddp.aspen.vdisk

/** The piece of a request that falls within a single block object */
final case class BlockSegment(blockIndex: Long, offsetInBlock: Int, length: Int, offsetInRequest: Int)

/** Maps the byte range of a virtual disk onto fixed-size block objects.
  *
  * @param size total disk size in bytes
  * @param logicalBlockSize alignment required of every request
  * @param blockObjectSize bytes per block object; a multiple of logicalBlockSize
  */
final case class DiskGeometry(size: Long, logicalBlockSize: Int, blockObjectSize: Int):

  /** Number of block objects needed to cover the disk */
  def blockCount: Long = (size + blockObjectSize - 1) / blockObjectSize

  def validate(offset: Long, length: Int): Option[VDiskError] =
    if offset < 0 || length < 0 || offset % logicalBlockSize != 0 || length % logicalBlockSize != 0 then
      val what = if offset < 0 || offset % logicalBlockSize != 0 then s"offset $offset" else s"length $length"
      Some(VDiskError.InvalidRequest(s"$what is not aligned to $logicalBlockSize"))
    else if offset > size || length > size - offset then
      Some(VDiskError.OutOfRange(s"range $offset+$length exceeds disk size $size"))
    else
      None

  /** Splits an in-range request into per-block segments. Does not validate. */
  def segments(offset: Long, length: Int): List[BlockSegment] =
    val builder = List.newBuilder[BlockSegment]
    var done = 0
    while done < length do
      val pos = offset + done
      val inBlock = (pos % blockObjectSize).toInt
      val len = math.min(length - done, blockObjectSize - inBlock)
      builder += BlockSegment(pos / blockObjectSize, inBlock, len, done)
      done += len
    builder.result()

  /** Indices of the blocks entirely contained within the byte range,  */
  def blocksFullyCovered(offset: Long, length: Long): List[Long] =
    val end = offset + length
    val first = (offset + blockObjectSize - 1) / blockObjectSize
    val last = end / blockObjectSize // exclusive
    (first until last).toList

object DiskGeometry:
  /** Largest multiple of logicalBlockSize not exceeding maxObjectSize, if at least one block fits */
  def blockObjectSizeFor(maxObjectSize: Int, logicalBlockSize: Int): Option[Int] =
    val sz = (maxObjectSize / logicalBlockSize) * logicalBlockSize
    if sz < logicalBlockSize then None else Some(sz)
