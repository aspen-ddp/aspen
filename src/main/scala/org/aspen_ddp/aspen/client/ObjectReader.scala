package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer}

import scala.concurrent.Future

trait ObjectReader {

  def client: AspenClient

  def read(pointer: DataObjectPointer): Future[DataObjectState] = read(pointer, "")

  def read(pointer: KeyValueObjectPointer): Future[KeyValueObjectState] = read(pointer, "")

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState]

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState]
}
