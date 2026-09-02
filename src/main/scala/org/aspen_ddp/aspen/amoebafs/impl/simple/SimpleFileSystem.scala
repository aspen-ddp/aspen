package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, NodeAllocator, Root, SinglePoolNodeAllocator}
import org.aspen_ddp.aspen.client.{AspenClient, ExponentialBackoffRetryStrategy, KeyValueObjectState, ObjectAllocator, ObjectAllocatorId, Transaction}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{IntegerKeyOrdering, Key, KeyValueObjectPointer, KeyValueOperation, LexicalKeyOrdering, Value}
import org.aspen_ddp.aspen.common.util.{byte2uuid, uuid2byte}
import org.aspen_ddp.aspen.compute.TaskExecutor
import org.aspen_ddp.aspen.compute.impl.SimpleTaskExecutor
import org.aspen_ddp.aspen.amoebafs.{DirectoryInode, DirectoryPointer, File, FileFactory, FileHandle, FileMode, FileSystem}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

object SimpleFileSystem {

  private val FileSystemUUIDKey    = Key(1)
  private val TaskExecutorRootKey  = Key(2)
  private val InodeTableRootKey    = Key(3)

  /** Create a new filesystem and register it under `fsName`.
   *
   *  Returns the new filesystem's UUID and the pointer to its root object. The name is
   *  registered in the namespaced registry under [[FileSystem.RegistryNamespace]] and the id
   *  is registered in the object registry against the root object, so that
   *  `load(client, fsName, _)` finds it again on a later run -- and on a later process.
   *
   *  Fails with KeyAlreadyExists if `fsName` is already taken. Note that this is the bare
   *  exception, not a StopRetrying wrapper: the registration is staged into the transaction
   *  rather than issued through a retry strategy.
   */
  def create(client: AspenClient,
             allocator: ObjectAllocator,
             fsName: String): Future[(UUID, KeyValueObjectPointer)] = {

    given ExecutionContext = client.clientContext

    val fileSystemUUID: UUID = UUID.randomUUID()

    val rootDirMode = FileMode.S_IFDIR | FileMode.S_IRWXU

    // A single attempt rather than a retry loop. Each attempt allocates four fresh objects, so
    // retrying would strand the previous attempt's rather than reuse them, and the failure this
    // is most likely to see -- a name already taken -- is not one a retry could fix.
    //
    // The four allocations and both registrations commit together, so there is no window in
    // which a name resolves to nothing or a filesystem exists that nothing can name.
    client.transact { t =>
      given tx: Transaction = t

      for
        // Claim the name first. Registry.prepareRegister invalidates the transaction
        // synchronously when it finds the key already present, so a taken name costs nothing
        // beyond the containing node's read.
        _ <- client.prepareRegisterId(FileSystem.RegistryNamespace, fsName, fileSystemUUID)
        taskRoot <- allocator.allocateKeyValueObject(Map())
        rootRoot = new Root(0, LexicalKeyOrdering, None, new SinglePoolNodeAllocator(client, taskRoot.poolId))
        rootDirInode = DirectoryInode.init(rootDirMode, 0, 0, None, Some(1), rootRoot)
        rootDirectory <- allocator.allocateDataObject(rootDirInode.toArray)
        rootDirectoryPointer = new DirectoryPointer(1, rootDirectory)
        inodeTableContentRoot <- allocator.allocateKeyValueObject(Map(Key(1) -> Value(rootDirectoryPointer.toArray)))
        inodeTableRoot = new Root(0, IntegerKeyOrdering, Some(inodeTableContentRoot), new SinglePoolNodeAllocator(client, taskRoot.poolId) )
        content = Map( FileSystemUUIDKey -> Value(uuid2byte(fileSystemUUID)),
          TaskExecutorRootKey -> Value(taskRoot.toArray),
          InodeTableRootKey -> Value(inodeTableRoot.encode()))
        fsRootPointer <- allocator.allocateKeyValueObject(content)
        _ <- client.prepareRegisterObject(fileSystemUUID, fsRootPointer)
      yield
        (fileSystemUUID, fsRootPointer)
    }
  }

  /** Create a filesystem named `fsName` and load it. */
  def bootstrap(client: AspenClient,
                allocator: ObjectAllocator,
                fsName: String,
                numContextThreads: Int = 4): Future[FileSystem] = {

    given ExecutionContext = client.clientContext

    for
      (_, fsRootPointer) <- create(client, allocator, fsName)
      fs <- load(client, fsRootPointer, numContextThreads)
    yield
      fs
  }

  /** Load the filesystem registered under `fsName`.
   *
   *  Fails with NoSuchElementException if no filesystem of that name is registered.
   */
  def load(client: AspenClient,
           fsName: String,
           numContextThreads: Int): Future[SimpleFileSystem] = {

    given ExecutionContext = client.clientContext

    for
      fileSystemUUID <- client.getRegisteredId(FileSystem.RegistryNamespace, fsName)
      fsRoot <- client.getRegisteredKeyValueObject(fileSystemUUID)
      fs <- load(client, fsRoot, numContextThreads)
    yield
      fs
  }

  def load(client: AspenClient,
           fsRoot: KeyValueObjectPointer,
           numContextThreads: Int): Future[SimpleFileSystem] = {
    given ExecutionContext = client.clientContext

    for
      kvos <- client.read(fsRoot)
      rootPool <- client.getStoragePool(kvos.pointer.poolId)
      defaultAllocator = new PoolObjectAllocator(client, rootPool)
      executorRoot = KeyValueObjectPointer(kvos.contents(TaskExecutorRootKey).value.bytes)
      executor <- SimpleTaskExecutor(client, defaultAllocator, executorRoot)
    yield
      new SimpleFileSystem(client, kvos, rootPool.ida, defaultAllocator, executor, numContextThreads)

  }
}
class SimpleFileSystem(aclient: AspenClient,
                       fsRoot: KeyValueObjectState,
                       val ida: IDA,
                       defaultAllocator: ObjectAllocator,
                       executor: TaskExecutor,
                       val numContextThreads: Int = 4,
                       writeBufferSize: Int = 4 * 1024 * 1024) extends FileSystem {

  import SimpleFileSystem._

  override val uuid: UUID =  byte2uuid(fsRoot.contents(FileSystemUUIDKey).value.bytes)

  override def shutdown(): Unit = ()

  def defaultSegmentSize: Int = 4 * 1024 * 1024
  def defaultFileIndexNodeSize(iter: Int): Int = 1024*1024

  override protected val fileFactory: FileFactory = new SimpleFileFactory(writeBufferSize)

  override private[amoebafs] def retryStrategy: org.aspen_ddp.aspen.client.RetryStrategy = new ExponentialBackoffRetryStrategy(client)

  override private[amoebafs] def taskExecutor = executor

  override private[amoebafs] def defaultInodeAllocator = defaultAllocator

  override private[amoebafs] def defaultSegmentAllocator(): Future[ObjectAllocator] = Future.successful(defaultAllocator)

  override private[amoebafs] def defaultFileContentTierAllocator(tier: Int): Future[ObjectAllocator] = Future.successful(defaultAllocator)

  override private[amoebafs] def client = aclient

  override private[amoebafs] val executionContext: scala.concurrent.ExecutionContext = client.clientContext //ExecutionContext.fromExecutorService(sched)

  override private[amoebafs] def getObjectAllocator(id: ObjectAllocatorId) = Future.successful(defaultAllocator)

  override private[amoebafs] def inodeTable: org.aspen_ddp.aspen.amoebafs.InodeTable = new SimpleInodeTable(this, defaultAllocator,
    new KVObjectRootManager(client, InodeTableRootKey, fsRoot.pointer))
  
  override def openFileHandle(file: File): FileHandle = new SimpleFileHandle(file, 1024*1024)
  override def closeFileHandle(fh: FileHandle): Unit = fh.flush()

  FileSystem.register(this)
}
