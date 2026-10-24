package org.aspen_ddp.aspen.common.objects

sealed abstract class ReadType

case class MetadataOnly() extends ReadType

case class FullObject() extends ReadType

/** Several byte ranges, each an (offset, length) pair in slice coordinates, optionally followed by the last
  * tail bytes of the slice. The result is the clipped ranges in request order, then the tail, concatenated.
  * Ranges are not merged or de-duplicated so overlapping ranges repeat bytes. tail = 0 reads no tail
  */
case class ByteRanges(ranges: List[(Int, Int)], tail: Int) extends ReadType:

  /** The [start, end) pieces of a slice of sliceSize bytes that this read returns: each range in request order,
    * followed by the tail piece. Both ends of every piece are clamped to [0, sliceSize]. The tail piece is always
    * present, even when empty. Stores and readers must both use this so they agree on the response layout
    */
  def pieces(sliceSize: Int): List[(Int, Int)] =
    val size = sliceSize.toLong
    def clip(start: Long, end: Long): (Int, Int) =
      val s = math.min(math.max(start, 0L), size)
      val e = math.max(s, math.min(end, size))
      (s.toInt, e.toInt)
    ranges.map((offset, length) => clip(offset.toLong, offset.toLong + length.toLong)) :+
      clip(size - tail.toLong, size)

case class SingleKey(key: Key, ordering: KeyOrdering) extends ReadType

case class LargestKeyLessThan(key: Key, ordering: KeyOrdering) extends ReadType

case class LargestKeyLessThanOrEqualTo(key: Key, ordering: KeyOrdering) extends ReadType

/** Reads all keys k where minimum <= k <= maximum. Both ends are inclusive */
case class KeyRange(minimum: Key, maximum: Key, ordering: KeyOrdering) extends ReadType

/** Reads only the listed keys. Keys that do not exist are omitted from the result */
case class KeySet(keys: List[Key]) extends ReadType
