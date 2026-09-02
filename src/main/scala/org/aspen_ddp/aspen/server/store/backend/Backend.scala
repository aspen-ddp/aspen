package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import scribe.Logging

import java.nio.file.Path
import scala.concurrent.{Future, Promise}

trait Backend extends Logging {
  val storeId: StoreId

  def path: Path

  def crlSaveFile: Path = path.resolve("crl_save.log")

  def close(): Future[Unit]

  def estimateSize(): Long

  def setCompletionHandler(handler: CompletionHandler): Unit

  def bootstrapAllocate(objectId: ObjectId,
                        objectType: ObjectType.Value,
                        metadata: Metadata,
                        data: DataBuffer): Unit

  def bootstrapOverwrite(objectId: ObjectId, data:DataBuffer): Unit

  def rebuildWrite(objectId: ObjectId,
                   objectType: ObjectType.Value,
                   metadata: Metadata,
                   data:DataBuffer): Unit

  /** Force all cached/in-memory data to stable storage before returning */
  def rebuildFlush(): Unit

  def read(pointer: ObjectPointer): Unit

  def commit(state: CommitState, transactionId: TransactionId): Unit
  
  def repair(state: CommitState, complete: Promise[Unit]): Unit

  /** Repairs a missed deletion by removing the object from the store.
    *
    * Unlike `repair`, no object state is available -- the object is already gone from the
    * rest of the system, so all we have is the ObjectId and the storePointer bytes recorded
    * in the pool's errorTree. The storePointer may be empty, which means "locate by ObjectId
    * alone"; backends that do not need it may ignore it entirely.
    *
    * Must be idempotent: the object may never have reached this store, and a repair pass may
    * run more than once. Removing an object that isn't present is a success, not an error.
    */
  def repairDelete(objectId: ObjectId, storePointer: Array[Byte], complete: Promise[Unit]): Unit
}
