package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyOrdering, KeyValueObjectPointer}

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

  def readOptional(pointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[Option[DataObjectState]] =
    read(pointer, "").map(Some(_)).recover:
      case e: InvalidObject => None

  def readOptional(pointer: KeyValueObjectPointer)(implicit ec: ExecutionContext): Future[Option[KeyValueObjectState]] =
    read(pointer, "").map(Some(_)).recover:
      case e: InvalidObject => None
}
