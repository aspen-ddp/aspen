package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.store.{StoreId, StorePointer}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.Locater
import org.apache.logging.log4j.scala.Logging

import java.nio.file.Path
import scala.concurrent.{Future, Promise}

trait Backend extends Logging {
  val storeId: StoreId

  def path: Path

  def crlSaveFile: Path = path.resolve("crl_save.log")
  
  def close(): Future[Unit] 

  def setCompletionHandler(handler: CompletionHandler): Unit

  /** Bootstrap-only allocation method. It cannot fail and must return a StorePointer to data committed to disk */
  def bootstrapAllocate(objectId: ObjectId,
                        objectType: ObjectType.Value,
                        metadata: Metadata,
                        data: DataBuffer): StorePointer

  /** Bootstrap-only overwrite method. It cannot fail and must return after the data is committed to disk */
  def bootstrapOverwrite(objectId: ObjectId, pointer: StorePointer, data:DataBuffer): Unit

  /** Fast write objects to the store during a rebuild operation. This method may return before the
   * object is written out to stable storage.
   * */
  def rebuildWrite(objectId: ObjectId,
                   objectType: ObjectType.Value,
                   metadata: Metadata,
                   pointer: StorePointer,
                   data:DataBuffer): Unit

  /** Force all cached/in-memory data to stable storage before returning */
  def rebuildFlush(): Unit

  def allocate(objectId: ObjectId,
               objectType: ObjectType.Value,
               metadata: Metadata,
               data: DataBuffer,
               maxSize: Option[Int]): Either[StorePointer, AllocationError.Value]

  def abortAllocation(objectId: ObjectId): Unit

  def read(locater: Locater): Unit

  def commit(state: CommitState, transactionId: TransactionId): Unit
  
  def repair(state: CommitState, complete: Promise[Unit]): Unit
}
