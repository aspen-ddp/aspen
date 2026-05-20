package org.aspen_ddp.aspen.server.store.backend

import java.nio.ByteBuffer
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.objects.ObjectPointer

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.language.implicitConversions

object RocksDBBackend {
  val NullArray: Array[Byte] = new Array[Byte](0)
  val MetadataIndex:Byte = 0
  val DataIndex:Byte = 1

  private def tokey(objectId: ObjectId) = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(0, objectId.uuid.getMostSignificantBits)
    bb.putLong(8, objectId.uuid.getLeastSignificantBits)
    bb.array()
  }

  def encodeDBValue(objectType: ObjectType.Value,
                    metadata: Metadata,
                    data: DataBuffer): Array[Byte] = {
    val arr = new Array[Byte](1 + Metadata.EncodedSize + data.size)
    val bb = ByteBuffer.wrap(arr)
    val otype = objectType match {
      case ObjectType.Data => 0
      case ObjectType.KeyValue => 1
    }
    bb.put(otype.toByte)
    metadata.encodeInto(bb)
    bb.put(data)
    arr
  }
  def decodeDBValue(data: Array[Byte]): (ObjectType.Value, Metadata, DataBuffer) = {
    val bb = ByteBuffer.wrap(data)
    val otype = bb.get() match {
      case 0 => ObjectType.Data
      case 1 => ObjectType.KeyValue
      case _ => ObjectType.Data // TODO handle corrupted object encoding
    }
    val metadata = Metadata(bb)
    (otype, metadata, bb)
  }
}

class RocksDBBackend(dbPath:Path,
                     override val storeId: StoreId,
                     val executionContext: ExecutionContext) extends Backend {

  import RocksDBBackend._
  
  given ExecutionContext = executionContext

  private var chandler: Option[CompletionHandler] = None

  private val db = new BufferedConsistentRocksDB(dbPath)


  override def close(): Future[Unit] = db.close()
  
  override def estimateSize(): Long = db.estimateSize()
  
  override def path: Path = dbPath

  override def setCompletionHandler(handler: CompletionHandler): Unit = {
    chandler = Some(handler)
  }

  override def bootstrapAllocate(objectId: ObjectId,
                                 objectType: ObjectType.Value,
                                 metadata: Metadata,
                                 data: DataBuffer): Unit = {
    val key = tokey(objectId)
    val value = encodeDBValue(objectType, metadata, data)
    db.bootstrapPut(key, value)
  }

  override def bootstrapOverwrite(objectId: ObjectId, data:DataBuffer): Unit = {
    val key = tokey(objectId)
    val (objectType, metadata, _) = decodeDBValue(db.bootstrapGet(key))
    val value = encodeDBValue(objectType, metadata, data)
    db.bootstrapPut(key, value)
  }

  override def rebuildWrite(objectId: ObjectId,
                            objectType: ObjectType.Value,
                            metadata: Metadata,
                            data: DataBuffer): Unit =
    val key = tokey(objectId)
    val value = encodeDBValue(objectType, metadata, data)
    db.rebuildWrite(key, value)

  override def rebuildFlush(): Unit = db.rebuildFlush()

  override def read(pointer: ObjectPointer): Unit = {
    val objectId = pointer.id
    logger.debug(s"RocksDBBackend beginning load of object: $objectId")

    val key = tokey(objectId)
    db.get(key).onComplete {
      case Failure(err) => logger.error(s"RocksDBBackend failed to load object: $objectId. Error: $err")
      case Success(oresult) =>
        chandler.foreach { handler =>
          oresult match {
            case None =>
              logger.info(s"RocksDBBackend ObjectNotFound: $objectId")
              handler.complete(Read(storeId, objectId, Right(ReadError.ObjectNotFound)))
            case Some(value) =>
              logger.debug(s"RocksDBBackend loaded object: $objectId")
              val (objectType, metadata, data) = decodeDBValue(value)
              val rs = ReadState(objectId, metadata, objectType, data, Set())
              handler.complete(Read(storeId, objectId, Left(rs)))
          }
        }
    }
  }

  override def commit(state: CommitState, transactionId: TransactionId): Unit = {
    chandler.foreach { handler =>
      val fcommit = if (state.metadata.refcount.count == 0)
        db.delete(tokey(state.objectId))
      else
        db.put(tokey(state.objectId), encodeDBValue(state.objectType, state.metadata, state.data))

      fcommit.foreach { _ =>
        handler.complete(Commit(storeId, state.objectId, transactionId, Left(())))
      }
    }
  }
  
  override def repair(state: CommitState, complete: Promise[Unit]): Unit =
    db.put(tokey(state.objectId), encodeDBValue(state.objectType, state.metadata, state.data)).foreach: _ =>
      complete.success(())
}
