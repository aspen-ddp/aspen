package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer}

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/** The reader AspenClient.settledReader returns: every read is the client's, untimed and settled (ReadManager.read) */
final class SettledObjectReader(val client: BaseAspenClient) extends ObjectReader:

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
    client.read(pointer, comment, Duration.Inf, settled = true)

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
    client.read(pointer, comment, Duration.Inf, settled = true)

  def readKeys(pointer: KeyValueObjectPointer, keys: List[Key], comment: String): Future[KeyValueObjectState] =
    client.readKeys(pointer, keys, comment, Duration.Inf, settled = true)

  def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering,
                   comment: String): Future[KeyValueObjectState] =
    client.readKeyRange(pointer, startKey, endKey, ordering, comment, Duration.Inf, settled = true)

  def readMetadata(pointer: ObjectPointer, comment: String): Future[MetadataObjectState] =
    client.readMetadata(pointer, comment, Duration.Inf, settled = true)

  def readRange(pointer: DataObjectPointer, offset: Int, length: Option[Int],
                comment: String): Future[DataObjectRangeState] =
    client.readRange(pointer, offset, length, comment, Duration.Inf, settled = true)

  def readRanges(pointer: DataObjectPointer, ranges: List[(Int, Int)],
                 comment: String): Future[List[DataObjectRangeState]] =
    client.readRanges(pointer, ranges, comment, Duration.Inf, settled = true)

  def readRangedWriteState(pointer: DataObjectPointer, writes: List[RangedWrite],
                           comment: String): Future[RangedWriteState] =
    client.readRangedWriteState(pointer, writes, comment, Duration.Inf, settled = true)

  def readAppendPosition(pointer: DataObjectPointer, comment: String): Future[AppendPosition] =
    client.readAppendPosition(pointer, comment, Duration.Inf, settled = true)
