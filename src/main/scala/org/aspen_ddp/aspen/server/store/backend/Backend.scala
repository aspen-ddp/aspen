package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import scribe.Logging

import scala.concurrent.{Future, Promise}

object Backend:
  /** Where ShutdownStore saves a closed store's CRL state, and LoadStore looks for it. Computed
   *  from the store directory alone so a host can check for it without opening the backend. */
  def crlSaveFile(storePath: os.Path): os.Path = storePath / "crl_save.log"

  /** True if "no space left" (ENOSPC) or a quota error (EDQUOT, "disk quota exceeded") appears, ignoring case, in
   *  the message of the throwable or of any cause: the device is full, and the store's data is intact. */
  def isOutOfSpace(t: Throwable): Boolean =
    def matches(e: Throwable): Boolean =
      val msg = Option(e.getMessage).getOrElse("").toLowerCase
      msg.contains("no space left") || msg.contains("disk full") ||
        msg.contains("insufficient space") || msg.contains("injected out of space") ||
        msg.contains("disk quota exceeded") || msg.contains("edquot")
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(8).exists(matches)

/** Thrown by a backend's open when the store's acknowledged state cannot be reconstructed: the store must be rebuilt */
class BackendStateLost(msg: String) extends Exception(msg)

trait Backend extends Logging {
  val storeId: StoreId

  def path: os.Path

  def crlSaveFile: os.Path = Backend.crlSaveFile(path)

  /** The store has failed-stop on an I/O error and can no longer be trusted. Readable from any thread. */
  def hasFailed: Boolean = false

  /** Writes are held because the device is full; reads go on. Readable from any thread. */
  def isWriteFrozen: Boolean = false

  def close(): Future[Unit]

  def estimateSize(): Long

  def setCompletionHandler(handler: CompletionHandler): Unit

  def bootstrapAllocate(objectId: ObjectId,
                        objectType: ObjectType.Value,
                        metadata: Metadata,
                        data: DataBuffer): Unit

  def bootstrapOverwrite(objectId: ObjectId, data:DataBuffer): Unit

  def rebuildWrite(pointer: ObjectPointer,
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
