package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, NodeAllocator, Root, SinglePoolNodeAllocator}
import org.aspen_ddp.aspen.client.{AspenClient, ExponentialBackoffRetryStrategy, KeyValueObjectState, ObjectAllocator, ObjectAllocatorId, Transaction}
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, Insert, IntegerKeyOrdering, Key, KeyValueObjectPointer, KeyValueOperation, LexicalKeyOrdering, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
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

  def bootstrap(client: AspenClient,
                guard: AllocationRevisionGuard,
                allocator: ObjectAllocator,
                hostingObject: KeyValueObjectPointer,
                amoebafsKey: Key): Future[FileSystem] = {

    given ExecutionContext = client.clientContext
    given tx: Transaction = client.newTransaction()

    val fileSystemUUID: UUID = UUID.randomUUID()

    val rootDirMode = FileMode.S_IFDIR | FileMode.S_IRWXU

    for
      taskRoot <- allocator.allocateKeyValueObject(guard, Map())
      rootRoot = new Root(0, LexicalKeyOrdering, None, new SinglePoolNodeAllocator(client, taskRoot.poolId))
      rootDirInode = DirectoryInode.init(rootDirMode, 0, 0, None, Some(1), rootRoot)
      rootDirectory <- allocator.allocateDataObject(guard, rootDirInode.toArray)
      rootDirectoryPointer = new DirectoryPointer(1, rootDirectory)
      inodeTableContentRoot <- allocator.allocateKeyValueObject(guard, Map(Key(1) -> Value(rootDirectoryPointer.toArray)))
      inodeTableRoot = new Root(0, IntegerKeyOrdering, Some(inodeTableContentRoot), new SinglePoolNodeAllocator(client, taskRoot.poolId) )
      content = Map( FileSystemUUIDKey -> Value(uuid2byte(fileSystemUUID)),
        TaskExecutorRootKey -> Value(taskRoot.toArray),
        InodeTableRootKey -> Value(inodeTableRoot.encode()))
      fsRootPointer <- allocator.allocateKeyValueObject(guard, content)
      _=tx.overwrite(rootDirectory, tx.revision, rootDirInode.toArray) // ensure Tx has an object to modify
      _=tx.update(hostingObject,
        None,
        None,
        List(KeyValueUpdate.DoesNotExist(amoebafsKey)),
        List(Insert(amoebafsKey, fsRootPointer.toArray)))
      _ <- tx.commit()
      fs <- load(client, fsRootPointer, numContextThreads = 4)
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
      defaultAllocator = new SinglePoolObjectAllocator(client, rootPool.get, fsRoot.ida, None)
      executorRoot = KeyValueObjectPointer(kvos.contents(TaskExecutorRootKey).value.bytes)
      executor <- SimpleTaskExecutor(client, StaticTaskTypeRegistry.registeredTasks, defaultAllocator, executorRoot)
    yield
      new SimpleFileSystem(client, kvos, defaultAllocator, executor, numContextThreads)

  }
}
class SimpleFileSystem(aclient: AspenClient,
                       fsRoot: KeyValueObjectState,
                       defaultAllocator: ObjectAllocator,
                       executor: TaskExecutor,
                       val numContextThreads: Int = 4,
                       writeBufferSize: Int = 4 * 1024 * 1024) extends FileSystem {

  import SimpleFileSystem._

  private val sched = Executors.newScheduledThreadPool(numContextThreads)

  override val uuid: UUID =  byte2uuid(fsRoot.contents(FileSystemUUIDKey).value.bytes)

  override def shutdown(): Unit = {
    sched.shutdown()
    sched.awaitTermination(5, TimeUnit.SECONDS)
  }

  def defaultSegmentSize: Int = 4 * 1024 * 1024
  def defaultFileIndexNodeSize(iter: Int): Int = 1024*1024

  override protected val fileFactory: FileFactory = new SimpleFileFactory(writeBufferSize)

  override private[amoebafs] def retryStrategy: org.aspen_ddp.aspen.client.RetryStrategy = new ExponentialBackoffRetryStrategy(client)

  override private[amoebafs] def taskExecutor = executor

  override private[amoebafs] def defaultInodeAllocator = defaultAllocator

  override private[amoebafs] def defaultSegmentAllocator(): Future[ObjectAllocator] = Future.successful(defaultAllocator)

  override private[amoebafs] def defaultIndexNodeAllocator(tier: Int): Future[ObjectAllocator] = Future.successful(defaultAllocator)

  override private[amoebafs] def client = aclient

  override private[amoebafs] val executionContext: scala.concurrent.ExecutionContext = ExecutionContext.fromExecutorService(sched)

  override private[amoebafs] def getObjectAllocator(id: ObjectAllocatorId) = Future.successful(defaultAllocator)

  override private[amoebafs] def inodeTable: org.aspen_ddp.aspen.amoebafs.InodeTable = new SimpleInodeTable(this, defaultAllocator,
    new KVObjectRootManager(client, InodeTableRootKey, fsRoot.pointer))
  
  override def openFileHandle(file: File): FileHandle = new SimpleFileHandle(file, 1024*1024)
  override def closeFileHandle(fh: FileHandle): Unit = fh.flush()

  FileSystem.register(this)
}
