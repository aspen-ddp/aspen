package org.aspen_ddp.aspen.common.objects

sealed abstract class ReadType

case class MetadataOnly() extends ReadType

case class FullObject() extends ReadType

/** Byte ranges are expressed in slice coordinates: offsets into the slice held by each store */
case class ByteRange(offset: Int, length: Int) extends ReadType

/** Everything from offset to the end of the slice */
case class ByteRangeToEnd(offset: Int) extends ReadType

/** The last length bytes of the slice, or the whole slice when it is shorter */
case class SliceTail(length: Int) extends ReadType

/** Several byte ranges, each an (offset, length) pair in slice coordinates, optionally followed by the last
  * tail bytes of the slice. The result is the clipped ranges in request order, then the tail, concatenated.
  * Ranges are not merged or de-duplicated so overlapping ranges repeat bytes. tail = 0 reads no tail
  */
case class ByteRanges(ranges: List[(Int, Int)], tail: Int) extends ReadType

case class SingleKey(key: Key, ordering: KeyOrdering) extends ReadType

case class LargestKeyLessThan(key: Key, ordering: KeyOrdering) extends ReadType

case class LargestKeyLessThanOrEqualTo(key: Key, ordering: KeyOrdering) extends ReadType

/** Reads all keys k where minimum <= k <= maximum. Both ends are inclusive */
case class KeyRange(minimum: Key, maximum: Key, ordering: KeyOrdering) extends ReadType

/** Reads only the listed keys. Keys that do not exist are omitted from the result */
case class KeySet(keys: List[Key]) extends ReadType
