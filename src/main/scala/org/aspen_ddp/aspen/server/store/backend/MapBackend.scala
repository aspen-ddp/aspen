package org.aspen_ddp.aspen.server.store.backend
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId, StorePointer}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.{Locater, ObjectState}

import java.nio.file.Path
import scala.concurrent.{Future, Promise}

class MapBackend(val storeId: StoreId) extends Backend {

  private var chandler: Option[CompletionHandler] = None

  var m: Map[ObjectId, ObjectState] = Map()

  override def close(): Future[Unit] = Future.successful(())
  
  override def path: Path = Path.of("/")

  override def setCompletionHandler(handler: CompletionHandler): Unit = {
    chandler = Some(handler)
  }

  override def bootstrapAllocate(objectId: ObjectId,
                                 objectType: ObjectType.Value,
                                 metadata: Metadata,
                                 data: DataBuffer): StorePointer = {
    val sp = StorePointer(storeId.poolIndex, Array())
    val os = new ObjectState(objectId, sp, metadata, objectType, data, None)

    m += (objectId -> os)

    sp
  }

  override def bootstrapOverwrite(objectId: ObjectId, pointer: StorePointer, data:DataBuffer): Unit = {
    m(objectId).data = data
  }

  override def rebuildWrite(objectId: ObjectId,
                   objectType: ObjectType.Value,
                   metadata: Metadata,
                   pointer: StorePointer,
                   data: DataBuffer): Unit = ???

  /** Force all cached/in-memory data to stable storage before returning */
  def rebuildFlush(): Unit = ???

  override def allocate(objectId: ObjectId,
                        objectType: ObjectType.Value,
                        metadata: Metadata,
                        data: DataBuffer,
                        maxSize: Option[Int]): Either[StorePointer, AllocationError.Value] = {

    val sp = StorePointer(storeId.poolIndex, Array())
    val os = new ObjectState(objectId, sp, metadata, objectType, data, maxSize)
    // Do not add to store during allocation since not all store implementations will be
    // able to do so.
    //m += (objectId -> os)
    Left(sp)
  }

  override def abortAllocation(objectId: ObjectId): Unit = {
    //m -= objectId
  }

  override def read(locater: Locater): Unit = {
    chandler.foreach { handler =>
      m.get(locater.objectId) match {
        case None =>
          handler.complete(Read(storeId, locater.objectId, locater.storePointer, Right(ReadError.ObjectNotFound)))
        case Some(os) =>
          val rs = ReadState(os.objectId, os.metadata, os.objectType, os.data, Set())
          handler.complete(Read(storeId, locater.objectId, os.storePointer, Left(rs)))
      }
    }
  }

  override def commit(state: CommitState, transactionId: TransactionId): Unit = {
    val os = new ObjectState(objectId = state.objectId,
      storePointer = state.storePointer,
      metadata = state.metadata,
      objectType = state.objectType,
      data = state.data,
      maxSize = state.maxSize)
    m += (state.objectId -> os)
    chandler.foreach { handler =>
      handler.complete(Commit(storeId, state.objectId, transactionId, Left(())))
    }
  }

  def get(objectId: ObjectId): Option[ObjectState] = m.get(objectId)

  override def repair(state: CommitState, complete: Promise[Unit]): Unit =
    val os = new ObjectState(objectId = state.objectId,
      storePointer = state.storePointer,
      metadata = state.metadata,
      objectType = state.objectType,
      data = state.data,
      maxSize = state.maxSize)
    m += (state.objectId -> os)
    complete.success(())
}
