package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/** The reader AspenClient.withReadTimeout returns: every read is the client's, bounded by `timeout` */
final class TimedObjectReader(val client: BaseAspenClient, val timeout: FiniteDuration) extends ObjectReader:

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
    client.read(pointer, comment, timeout, settled = false)

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
    client.read(pointer, comment, timeout, settled = false)

  def readKeys(pointer: KeyValueObjectPointer, keys: List[Key], comment: String): Future[KeyValueObjectState] =
    client.readKeys(pointer, keys, comment, timeout, settled = false)

  def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering,
                   comment: String): Future[KeyValueObjectState] =
    client.readKeyRange(pointer, startKey, endKey, ordering, comment, timeout, settled = false)

  def readMetadata(pointer: ObjectPointer, comment: String): Future[MetadataObjectState] =
    client.readMetadata(pointer, comment, timeout, settled = false)

  def readRange(pointer: DataObjectPointer, offset: Int, length: Option[Int],
                comment: String): Future[DataObjectRangeState] =
    client.readRange(pointer, offset, length, comment, timeout, settled = false)

  def readRanges(pointer: DataObjectPointer, ranges: List[(Int, Int)],
                 comment: String): Future[List[DataObjectRangeState]] =
    client.readRanges(pointer, ranges, comment, timeout, settled = false)

  def readRangedWriteState(pointer: DataObjectPointer, writes: List[RangedWrite],
                           comment: String): Future[RangedWriteState] =
    client.readRangedWriteState(pointer, writes, comment, timeout, settled = false)

  def readAppendPosition(pointer: DataObjectPointer, comment: String): Future[AppendPosition] =
    client.readAppendPosition(pointer, comment, timeout, settled = false)
