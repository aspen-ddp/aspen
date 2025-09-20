package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer}

import scala.concurrent.{ExecutionContext, Future}

trait ObjectReader {

  def client: AspenClient

  def read(pointer: DataObjectPointer): Future[DataObjectState] = read(pointer, "")

  def read(pointer: KeyValueObjectPointer): Future[KeyValueObjectState] = read(pointer, "")

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState]

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState]

  def readOptional(pointer: DataObjectPointer)(implicit ec: ExecutionContext): Future[Option[DataObjectState]] =
    read(pointer, "").map(Some(_)).recover:
      case e: InvalidObject => None

  def readOptional(pointer: KeyValueObjectPointer)(implicit ec: ExecutionContext): Future[Option[KeyValueObjectState]] =
    read(pointer, "").map(Some(_)).recover:
      case e: InvalidObject => None
}
