package org.aspen_ddp.aspen.server.store.backend.rocksdb

import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.host.DeviceIo
import org.aspen_ddp.aspen.server.store.backend.*
import java.nio.ByteBuffer
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.common.util.physicalMemory
import org.rocksdb.{BlockBasedTableConfig, Cache, Env, LRUCache, Options, Priority, RocksDB, RocksDBException, Status,
  WriteBufferManager}
import scribe.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.language.implicitConversions

object RocksDBBackend extends Logging {

  /** Thrown by `decodeDBValue` when a stored value does not have a valid encoding */
  class CorruptedObjectEncoding(msg: String) extends RuntimeException(msg)

  /** True for a RocksDB error that reports damaged data rather than a failure to read it */
  def isCorruption(e: RocksDBException): Boolean =
    Option(e.getStatus).exists(_.getCode == Status.Code.Corruption)

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
    */
  case class ResourceConfig(memoryBytes: Long, writeBufferBytes: Long) {
    require(memoryBytes > 0, s"RocksDB memory must be positive: $memoryBytes")
    require(writeBufferBytes > 0 && writeBufferBytes <= memoryBytes,
      s"RocksDB write buffer size must be positive and no larger than the memory budget: $writeBufferBytes")
  }

  object ResourceConfig {
    /** Share of physical RAM given to RocksDB when the host config does not specify it */
    val DefaultMemoryFraction = 0.25
    /** Share of the RocksDB memory budget usable by memtables when the host config does not specify it */
    val DefaultWriteBufferFraction = 0.25

    /** Fills in any unspecified values with defaults derived from the machine's RAM and core count */
    def resolve(memoryBytes: Option[Long] = None,
                writeBufferBytes: Option[Long] = None): ResourceConfig = {
      val memory = memoryBytes.getOrElse((physicalMemory() * DefaultMemoryFraction).toLong)
      ResourceConfig(
        memory,
        writeBufferBytes.getOrElse((memory * DefaultWriteBufferFraction).toLong))
    }
  }

  /** Background job slots that `openDatabases` open databases get from the process-wide default Env, as
    * (compaction, flush) pool sizes. Every database is opened with max_background_jobs = 2, which RocksDB turns into
    * at most one flush and one compaction at a time, so one slot of each kind per database lets every database always
    * find a free thread even if the databases of a hung device hold theirs forever. Uncapped, minimum one each.
    */
  def threadAllocation(openDatabases: Int): (Int, Int) =
    val slots = math.max(1, openDatabases)
    (slots, slots)

  /** One flush and one compaction at most per database, see threadAllocation */
  private val MaxBackgroundJobsPerDatabase = 2

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
    val (compaction, flush) = threadAllocation(openDatabases)
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
          "bytes")
        sharedMemory = Some(m)
        m
    }
  }

  /** RocksDB's pointer to the live manifest: present in every database RocksDB has created */
  val CurrentFile = "CURRENT"

  /** Thrown by an open of a store directory holding no database: a store directory exists only once its backend was
    * created (StoreCreation), so its database was lost
    */
  final class RocksDBStateLost(msg: String) extends BackendStateLost(msg)

  /** Creates an empty database in `dbPath`, durably, and closes it. Run inside StoreCreation's scratch directory */
  def initializeStore(dbPath: os.Path): Unit =
    closeDatabase(openDatabase(dbPath, createIfMissing = true))

  /** Opens a database that uses the host-wide shared resources. The database must be closed via closeDatabase so
    * that the background thread pools shrink accordingly. Without `createIfMissing` (every open of a store) a
    * directory with no database throws RocksDBStateLost and nothing is written; only `initializeStore` creates one.
    * Only a `CURRENT` known absent is: a stat that fails throws its IOException (server.FileStat), which the host's
    * load retries.
    */
  def openDatabase(dbPath: os.Path, createIfMissing: Boolean = false): RocksDB = {
    if !createIfMissing && !FileStat.exists(dbPath / CurrentFile) then
      throw new RocksDBStateLost(s"RocksDB store at $dbPath has no database ($CurrentFile is missing)")
    val (cache, wbm) = getSharedMemory()
    val options = new Options()
      .setCreateIfMissing(createIfMissing)
      .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache))
      .setWriteBufferManager(wbm)
      .setMaxBackgroundJobs(MaxBackgroundJobsPerDatabase)

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
      case b => throw new CorruptedObjectEncoding(s"Unknown object type byte: $b")
    }
    val metadata = Metadata(bb)
    (otype, metadata, bb)
  }
}

class RocksDBBackend(dbPath: os.Path,
                     override val storeId: StoreId,
                     io: DeviceIo,
                     readRetryDelays: Seq[FiniteDuration] = RetryScheduler.ReadRetryDelays,
                     scheduler: RetryScheduler = SharedRetryScheduler) extends Backend {

  import RocksDBBackend._
  
  val executionContext: ExecutionContext = io.dataEc
  given ExecutionContext = executionContext

  private var chandler: Option[CompletionHandler] = None

  private val db = new BufferedConsistentRocksDB(dbPath)


  override def close(): Future[Unit] = db.close()
  
  override def estimateSize(): Long = db.estimateSize()
  
  override def path: os.Path = dbPath

  override def readsOverBound: Boolean = io.readsOverBound

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

  override def rebuildWrite(pointer: ObjectPointer,
                            objectType: ObjectType.Value,
                            metadata: Metadata,
                            data: DataBuffer): Future[Unit] =
    try
      val key = tokey(pointer.id)
      val value = encodeDBValue(objectType, metadata, data)
      db.rebuildWrite(key, value)
    catch case NonFatal(t) => Future.failed(t)

  override def rebuildFlush(): Future[Unit] = db.rebuildFlush()

  /** Reads the stored value of a key. Overridable so tests may inject read failures. */
  protected def getValue(key: Array[Byte]): Future[Option[Array[Byte]]] = db.get(key)

  /** Completes as `CorruptedObject` only on evidence of damage: a RocksDB Corruption status or a value that does not
    * decode. Any other failure (an IOError, Busy, TryAgain or Incomplete status, EMFILE or ENOMEM surfacing as an
    * exception, a throw while issuing) is retried after each of `readRetryDelays`, then reported as `CorruptedObject`
    * too: a recovered commit vote fail-stops the store on it (D7), so one transient host condition must not. A read
    * of a closed database is not retried.
    */
  override def read(pointer: ObjectPointer): Unit =
    logger.debug(s"RocksDBBackend beginning load of object: ${pointer.id}")
    attemptRead(pointer.id, readRetryDelays.toList)

  private def attemptRead(objectId: ObjectId, retries: List[FiniteDuration]): Unit =
    def complete(result: Either[ReadState, ReadError.Value]): Unit =
      chandler.foreach(_.complete(Read(storeId, objectId, result)))

    def failed(err: Throwable): Unit = (err, retries) match
      case (_: BufferedConsistentRocksDB.DBClosed, _) =>
        logger.info(s"RocksDBBackend read of $objectId found the database closed")
        complete(Right(ReadError.CorruptedObject))
      case (e: RocksDBException, _) if isCorruption(e) =>
        logger.error(s"RocksDBBackend found corrupted object $objectId: $e")
        complete(Right(ReadError.CorruptedObject))
      case (_, delay :: rest) =>
        logger.warn(s"RocksDBBackend read of $objectId failed, retrying in $delay: $err")
        scheduler.schedule(delay, () => attemptRead(objectId, rest))
      case (_, Nil) =>
        logger.error(s"RocksDBBackend failed to load object $objectId after ${readRetryDelays.size} retries: $err")
        complete(Right(ReadError.CorruptedObject))

    def decoded(oresult: Option[Array[Byte]]): Unit = oresult match
      case None =>
        logger.info(s"RocksDBBackend ObjectNotFound: $objectId")
        complete(Right(ReadError.ObjectNotFound))
      case Some(value) =>
        Try(decodeDBValue(value)) match
          case Success((objectType, metadata, data)) =>
            logger.debug(s"RocksDBBackend loaded object: $objectId")
            complete(Left(ReadState(objectId, metadata, objectType, data, Set())))
          case Failure(e) =>
            logger.error(s"RocksDBBackend found corrupted object $objectId: $e")
            complete(Right(ReadError.CorruptedObject))

    // Counted on the device from submission until the get has finished, whatever its outcome. The get is a Future on
    // the device's data pool that always completes (a closed database fails it with DBClosed), so a count never leaks
    io.beginRead()
    Try(getValue(tokey(objectId))) match
      case Failure(err) =>
        io.endRead()
        failed(err)
      case Success(fget) => fget.onComplete:
        case Failure(err) =>
          io.endRead()
          failed(err)
        case Success(oresult) =>
          io.endRead()
          decoded(oresult)

  override def commit(state: CommitState, transactionId: TransactionId): Unit = {
    chandler.foreach { handler =>
      val start = System.nanoTime()
      val fcommit = if (state.metadata.refcount.count == 0)
        db.delete(tokey(state.objectId))
      else
        db.put(tokey(state.objectId), encodeDBValue(state.objectType, state.metadata, state.data))

      fcommit.foreach { _ =>
        BackendTiming.report.record("rocksdb commit", System.nanoTime() - start)
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
