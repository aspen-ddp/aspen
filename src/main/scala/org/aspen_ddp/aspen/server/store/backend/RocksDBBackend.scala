package org.aspen_ddp.aspen.server.store.backend

import java.nio.ByteBuffer
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.rocksdb.{BlockBasedTableConfig, Cache, Env, LRUCache, Options, Priority, RocksDB, WriteBufferManager}
import scribe.Logging

import java.lang.management.ManagementFactory
import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.language.implicitConversions

object RocksDBBackend extends Logging {
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
  /** Host-wide limits on the resources used by all RocksDB databases.
    *
    * @param memoryBytes          Capacity of the block cache shared by every database. Memtable memory is charged
    *                             against this cache so this is the overall RocksDB memory budget.
    * @param writeBufferBytes     Portion of memoryBytes that the memtables of all databases may use before flushes
    *                             are forced.
    * @param maxBackgroundThreads Upper bound on the flush plus compaction threads shared by all databases
    */
  case class ResourceConfig(memoryBytes: Long, writeBufferBytes: Long, maxBackgroundThreads: Int) {
    require(memoryBytes > 0, s"RocksDB memory must be positive: $memoryBytes")
    require(writeBufferBytes > 0 && writeBufferBytes <= memoryBytes,
      s"RocksDB write buffer size must be positive and no larger than the memory budget: $writeBufferBytes")
    require(maxBackgroundThreads >= 2, s"RocksDB requires at least 2 background threads: $maxBackgroundThreads")
  }

  object ResourceConfig {
    /** Share of physical RAM given to RocksDB when the host config does not specify it */
    val DefaultMemoryFraction = 0.25
    /** Share of the RocksDB memory budget usable by memtables when the host config does not specify it */
    val DefaultWriteBufferFraction = 0.25

    /** Fills in any unspecified values with defaults derived from the machine's RAM and core count */
    def resolve(memoryBytes: Option[Long] = None,
                writeBufferBytes: Option[Long] = None,
                maxBackgroundThreads: Option[Int] = None): ResourceConfig = {
      val memory = memoryBytes.getOrElse((physicalMemory() * DefaultMemoryFraction).toLong)
      ResourceConfig(
        memory,
        writeBufferBytes.getOrElse((memory * DefaultWriteBufferFraction).toLong),
        maxBackgroundThreads.getOrElse(math.max(2, Runtime.getRuntime.availableProcessors)))
    }

    private def physicalMemory(): Long = ManagementFactory.getOperatingSystemMXBean match {
      case os: com.sun.management.OperatingSystemMXBean => os.getTotalMemorySize
      case _ => Runtime.getRuntime.maxMemory
    }
  }

  /** Splits the shared background threads into (compaction, flush) pool sizes.
    *
    * Each database runs one flush and one compaction at a time by default (max_background_jobs = 2) so the pools
    * grow by two threads per open database until maxBackgroundThreads is reached. A quarter of the threads, and
    * never less than one, serve flushes since a stalled flush blocks writes.
    */
  def threadAllocation(openDatabases: Int, maxBackgroundThreads: Int): (Int, Int) = {
    val total = math.max(2, math.min(maxBackgroundThreads, 2 * openDatabases))
    val flush = math.max(1, total / 4)
    (total - flush, flush)
  }

  // All databases use RocksDB's default Env, whose background thread pools are process-wide. RocksDB only grows
  // those pools to what a single database needs so they are resized here as databases are opened and closed.
  // The block cache and write buffer manager cannot be resized once created so they are sized for the whole host
  // and created when the first database is opened.
  private var resourceConfig: Option[ResourceConfig] = None
  private var sharedMemory: Option[(Cache, WriteBufferManager)] = None
  private var openDatabases = 0

  /** Sets the host-wide resource limits. Should be called before any database is opened since the memory limits
    * are fixed once the first one is. The thread limit is applied immediately.
    */
  def configureResources(cfg: ResourceConfig): Unit = synchronized {
    val applied = resourceConfig match {
      case Some(current) if sharedMemory.isDefined =>
        if (current.memoryBytes != cfg.memoryBytes || current.writeBufferBytes != cfg.writeBufferBytes)
          logger.warn(s"RocksDB memory limits cannot be changed once a database is open. Retaining " +
            s"memory=${current.memoryBytes} writeBuffer=${current.writeBufferBytes} bytes")
        cfg.copy(memoryBytes = current.memoryBytes, writeBufferBytes = current.writeBufferBytes)
      case _ => cfg
    }
    resourceConfig = Some(applied)
    resizeThreadPools()
  }

  private def currentConfig: ResourceConfig = synchronized {
    resourceConfig match {
      case Some(cfg) => cfg
      case None =>
        val cfg = ResourceConfig.resolve()
        resourceConfig = Some(cfg)
        cfg
    }
  }

  private def resizeThreadPools(): Unit = synchronized {
    val (compaction, flush) = threadAllocation(openDatabases, currentConfig.maxBackgroundThreads)
    val env = Env.getDefault
    env.setBackgroundThreads(compaction, Priority.LOW)
    env.setBackgroundThreads(flush, Priority.HIGH)
  }

  private def getSharedMemory(): (Cache, WriteBufferManager) = synchronized {
    sharedMemory match {
      case Some(m) => m
      case None =>
        val cfg = currentConfig
        // Unlike most RocksDB objects, LRUCache does not load the native library itself
        RocksDB.loadLibrary()
        val cache = new LRUCache(cfg.memoryBytes)
        val m = (cache, new WriteBufferManager(cfg.writeBufferBytes, cache))
        logger.info(s"RocksDB shared resources: memory=${cfg.memoryBytes} writeBuffer=${cfg.writeBufferBytes} " +
          s"bytes, maxBackgroundThreads=${cfg.maxBackgroundThreads}")
        sharedMemory = Some(m)
        m
    }
  }

  /** Opens, creating if necessary, a database that uses the host-wide shared resources. The database must be
    * closed via closeDatabase so that the background thread pools shrink accordingly.
    */
  def openDatabase(dbPath: Path): RocksDB = {
    val (cache, wbm) = getSharedMemory()
    val options = new Options()
      .setCreateIfMissing(true)
      .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache))
      .setWriteBufferManager(wbm)

    // Opening may replay a large WAL so it is done outside the lock
    val db = try {
      RocksDB.open(options, dbPath.toString)
    } finally {
      options.close()
    }

    synchronized {
      openDatabases += 1
      resizeThreadPools()
    }
    db
  }

  def closeDatabase(db: RocksDB): Unit = {
    db.close()
    synchronized {
      openDatabases -= 1
      resizeThreadPools()
    }
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

  override def repairDelete(objectId: ObjectId, storePointer: Array[Byte], complete: Promise[Unit]): Unit =
    // Objects are keyed by ObjectId alone, so the storePointer is not needed here. Deleting a
    // key that isn't present is a no-op in RocksDB, which gives us idempotency for free.
    db.delete(tokey(objectId)).foreach: _ =>
      complete.success(())
}
