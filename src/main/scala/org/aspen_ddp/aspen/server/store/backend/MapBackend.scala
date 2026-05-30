package org.aspen_ddp.aspen.server.store.backend
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.server.store.ObjectState

import java.nio.file.Path
import scala.concurrent.{Future, Promise}

class MapBackend(val storeId: StoreId) extends Backend {

  private var chandler: Option[CompletionHandler] = None

  var m: Map[ObjectId, ObjectState] = Map()

  override def close(): Future[Unit] = Future.unit
  
  override def estimateSize(): Long = 0
  
  override def path: Path = Path.of("/")

  override def setCompletionHandler(handler: CompletionHandler): Unit = {
    chandler = Some(handler)
  }

  override def bootstrapAllocate(objectId: ObjectId,
                                 objectType: ObjectType.Value,
                                 metadata: Metadata,
                                 data: DataBuffer): Unit = {
    val os = new ObjectState(objectId, metadata, objectType, data)

    m += (objectId -> os)
  }

  override def bootstrapOverwrite(objectId: ObjectId, data:DataBuffer): Unit = {
    m(objectId).data = data
  }

  override def rebuildWrite(objectId: ObjectId,
                   objectType: ObjectType.Value,
                   metadata: Metadata,
                   data: DataBuffer): Unit = ???

  /** Force all cached/in-memory data to stable storage before returning */
  def rebuildFlush(): Unit = ???

  override def read(pointer: ObjectPointer): Unit = {
    chandler.foreach { handler =>
      val objectId = pointer.id
      m.get(objectId) match {
        case None =>
          handler.complete(Read(storeId, objectId, Right(ReadError.ObjectNotFound)))
        case Some(os) =>
          val rs = ReadState(os.objectId, os.metadata, os.objectType, os.data, Set())
          handler.complete(Read(storeId, objectId, Left(rs)))
      }
    }
  }

  override def commit(state: CommitState, transactionId: TransactionId): Unit = {
    val os = new ObjectState(objectId = state.objectId,
      metadata = state.metadata,
      objectType = state.objectType,
      data = state.data)
    m += (state.objectId -> os)
    chandler.foreach { handler =>
      handler.complete(Commit(storeId, state.objectId, transactionId, Left(())))
    }
  }

  def get(objectId: ObjectId): Option[ObjectState] = m.get(objectId)

  override def repair(state: CommitState, complete: Promise[Unit]): Unit =
    val os = new ObjectState(objectId = state.objectId,
      metadata = state.metadata,
      objectType = state.objectType,
      data = state.data)
    m += (state.objectId -> os)
    complete.success(())
}
