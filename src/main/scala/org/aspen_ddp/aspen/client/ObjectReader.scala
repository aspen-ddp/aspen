package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer}

import scala.concurrent.{ExecutionContext, Future}

trait ObjectReader {

  def client: AspenClient

  def read(pointer: DataObjectPointer): Future[DataObjectState] = read(pointer, "")

  def read(pointer: KeyValueObjectPointer): Future[KeyValueObjectState] = read(pointer, "")

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState]

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState]

  /** Reads only the listed keys. Keys that do not exist are absent from the result, which is marked `partial` */
  def readKeys(pointer: KeyValueObjectPointer, keys: List[Key], comment: String): Future[KeyValueObjectState]

  def readKeys(pointer: KeyValueObjectPointer, keys: List[Key]): Future[KeyValueObjectState] = readKeys(pointer, keys, "")

  def readKeys(pointer: KeyValueObjectPointer, key: Key, moreKeys: Key*): Future[KeyValueObjectState] =
    readKeys(pointer, key :: moreKeys.toList, "")

  /** Reads the keys k where startKey <= k <= endKey under `ordering`. The result is marked `partial` */
  def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering,
                   comment: String): Future[KeyValueObjectState]

  def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key,
                   ordering: KeyOrdering): Future[KeyValueObjectState] =
    readKeyRange(pointer, startKey, endKey, ordering, "")

  /** Reads only the object's metadata. The object's content is neither transferred nor restored */
  def readMetadata(pointer: ObjectPointer, comment: String): Future[MetadataObjectState]

  def readMetadata(pointer: ObjectPointer): Future[MetadataObjectState] = readMetadata(pointer, "")

  /** Reads the logical bytes [offset, offset + length) of a data object, or [offset, end) when length is None.
    * The result's `offset` is the requested offset and its data is clipped to the object's length when that is
    * known. Fails with RangeReadNotSupported unless the object's IDA supports append and range reads.
    */
  def readRange(pointer: DataObjectPointer, offset: Int, length: Option[Int], comment: String): Future[DataObjectRangeState]

  def readRange(pointer: DataObjectPointer, offset: Int, length: Option[Int]): Future[DataObjectRangeState] =
    readRange(pointer, offset, length, "")

  /** Reads several byte ranges of a data object concurrently. Each range is an (offset, length) pair and the
    * results are in the same order, each as `readRange(pointer, offset, Some(length))` would return it. All
    * ranges are read in a single request and therefore come from the same revision of the object. Fails with
    * RangeReadNotSupported unless the object's IDA supports append and range reads.
    */
  def readRanges(pointer: DataObjectPointer, ranges: List[(Int, Int)], comment: String): Future[List[DataObjectRangeState]]

  def readRanges(pointer: DataObjectPointer, ranges: List[(Int, Int)]): Future[List[DataObjectRangeState]] =
    readRanges(pointer, ranges, "")

  /** Reads the state `Transaction.rangedWrites` needs for `writes`: the object's revision, its length and the
    * current bytes under each write, all taken from a single revision of the object by a single read. Fails with
    * RangeReadNotSupported unless the object's IDA supports range reads.
    */
  def readRangedWriteState(pointer: DataObjectPointer, writes: List[RangedWrite], comment: String): Future[RangedWriteState]

  def readRangedWriteState(pointer: DataObjectPointer, writes: List[RangedWrite]): Future[RangedWriteState] =
    readRangedWriteState(pointer, writes, "")

  /** Reads the position at which the object can currently be appended to */
  def readAppendPosition(pointer: DataObjectPointer, comment: String): Future[AppendPosition]

  def readAppendPosition(pointer: DataObjectPointer): Future[AppendPosition] = readAppendPosition(pointer, "")

  def readOptional(pointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[Option[DataObjectState]] =
    read(pointer, "").map(Some(_)).recover:
      case e: InvalidObject => None

  def readOptional(pointer: KeyValueObjectPointer)(implicit ec: ExecutionContext): Future[Option[KeyValueObjectState]] =
    read(pointer, "").map(Some(_)).recover:
      case e: InvalidObject => None

  def readMetadataOptional(pointer: ObjectPointer)(implicit ec: ExecutionContext): Future[Option[MetadataObjectState]] =
    readMetadata(pointer, "").map(Some(_)).recover:
      case e: InvalidObject => None
}
