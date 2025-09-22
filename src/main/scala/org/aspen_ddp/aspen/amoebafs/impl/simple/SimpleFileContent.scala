package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.nio.ByteBuffer
import java.util.UUID
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, InvalidObject, KeyValueObjectState, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectRevisionGuard, Value}
import org.aspen_ddp.aspen.common.util.{Varint, db2string}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer}
import org.aspen_ddp.aspen.amoebafs.{FileInode, FileSystem}
import org.apache.logging.log4j.scala.{Logger, Logging}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, Root, TieredKeyValueList}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions
import scala.concurrent.duration.*

class SimpleFileContent(file: SimpleFile, 
                        osegmentSize: Option[Int]=None) extends Logging:
  
  import SimpleFileContent.*

  private val fs = file.fs

  private val dosCache: Cache[Long, DataObjectState] = Scaffeine().
    maximumSize(5).
    expireAfterAccess(10.seconds).
    build[Long, DataObjectState]()

  private val segmentSize = osegmentSize.getOrElse(fs.defaultSegmentSize)

  private def contentTree: TieredKeyValueList =
    val rootManager = new SimpleFileRootManager(fs.client, file.pointer.pointer)
    new TieredKeyValueList(fs.client, rootManager)

  /** If the root node has already been created, immediately return the tree.
   *  Otherwise, use a new transaction to create the root node and then
   *  refresh the file to update the cached inode state. This prevents
   *  duplicated attempts to update the inode object on the first write. Once
   *  for creating the root node and once for updating the inode mtime
   */
  def initializeContentTree()(using ec: ExecutionContext): Future[TieredKeyValueList] =
    file.inode.contents.orootObject match
      case Some(_) => Future.successful(contentTree)
      case None =>
        val fsuccess = fs.client.transactUntilSuccessful: tx =>

          tx.result.failed.foreach: cause =>
            println(s"initializeContentTree Transaction failed! $cause")

          for
            _ <- file.refresh()
            _ <- file.inode.contents.orootObject match
              case Some(_) => Future.unit
              case None =>
                val rootManager = new SimpleFileRootManager(fs.client, file.pointer.pointer)
                rootManager.createInitialNode(Map())(using tx).map(_ => ())
          yield
            ()

        fsuccess.flatMap: _ =>
          file.refresh().map: _ =>
            contentTree

  def getSegmentOffset(offset: Long): SegmentOffset = 
    val offsetWithinSegment = (offset % segmentSize).toInt
    SegmentOffset(offset - offsetWithinSegment, offsetWithinSegment)

  private def readSegment(segmentOffset: Long)
                         (using ec: ExecutionContext): Future[Option[DataObjectState]] =
    dosCache.getIfPresent(segmentOffset) match
      case Some(dos) => Future.successful(Some(dos))
      case None => contentTree.get(Key(segmentOffset)).flatMap:
        case None => Future.successful(None)
        case Some(vs) => fs.client.read(DataObjectPointer(vs.value.bytes)).map: dos =>
          dosCache.put(segmentOffset, dos)
          Some(dos)

  def read(offset: Long, nbytes: Int)(using ec: ExecutionContext): Future[DataBuffer] =
    val fileSize = file.inode.size

    if offset >= fileSize then
      Future.successful(DataBuffer.Empty)
    else
      val nread = if offset + nbytes > fileSize then
        (fileSize - offset).toInt
      else
        nbytes
        
      val rbuff = ByteBuffer.allocate(nread)
      val readComplete = Promise[DataBuffer]()
      
      def writeBuff(so: SegmentOffset, db: DataBuffer, nput: Int): Int =
        val maxWrite = segmentSize - so.offsetWithinSegment
        val nwrite =  if nput <= maxWrite then nput else maxWrite
        val putDb = db.zappend(so.offsetWithinSegment + nwrite).slice(so.offsetWithinSegment)
        
        rbuff.put(putDb.slice(0, nwrite).asReadOnlyBuffer())
        
        nwrite

      def recurse(so: SegmentOffset, numRemaining: Int): Unit =
        if numRemaining == 0 then
          rbuff.position(0)
          readComplete.success(DataBuffer(rbuff))
        else
          for
            odos <- readSegment(so.segmentBeginOffset)
            db = odos match
              case None => DataBuffer.Empty
              case Some(dos) => dos.data
            nwrite = if numRemaining > segmentSize then segmentSize else numRemaining
            nwritten = writeBuff(so, db, nwrite)
            nextSo = getSegmentOffset(so.segmentBeginOffset + segmentSize)
          yield
            recurse(nextSo, numRemaining - nwritten)
            
      recurse(getSegmentOffset(offset), nread)

      readComplete.future

  def debugReadFully()(using ec: ExecutionContext): Future[Array[Byte]] = 
    read(0, file.inode.size.toInt).map(_.getByteArray)

  /** Outer promise completes when the Inode is ready to be updated with
   *  the new size. Inner future completes when the file truncation task
   *  is complete
   */
  def truncate(endOffset: Long)
              (using tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] =
    val inode = file.inode

    if endOffset > inode.size then
      write(endOffset - 1, List(DataBuffer.Empty)).map(_ => Future.unit)

    else if inode.size == endOffset then
      Future.successful(Future.unit)

    else
      val segment = getSegmentOffset(endOffset)
      for
        odos <- readSegment(segment.segmentBeginOffset)
        _ = odos.map: dos =>
            val newDb = if dos.data.size >= segment.offsetWithinSegment then
              dos.data.slice(0, segment.offsetWithinSegment)
            else
              dos.data.zappend(segment.offsetWithinSegment)
            tx.overwrite(dos.pointer, dos.revision, newDb)
            dosCache.invalidate(segment.segmentBeginOffset)
        newRoot <- contentTree.splitTree(Key(segment.segmentBeginOffset), true)
        fcomplete <- DeleteFileContentTask.prepareTask(fs, newRoot)
      yield
        fcomplete
        
  def deleteFile(): Future[Unit] = 
    given ExecutionContext = fs.executionContext
    fs.client.retryStrategy.retryUntilSuccessful:
      val inode = file.inode
      inode.contents.orootObject match
        case None => Future.unit
        case Some(root) => 
          val tx = fs.client.newTransaction()
          DeleteFileContentTask.prepareTask(fs, inode.contents)(using tx)
          tx.commit().map(_ => ())

  def write(offset: Long,
            buffers: List[DataBuffer])
           (using tx: Transaction, ec: ExecutionContext): Future[WriteResult] =

    val fileSize = file.inode.size
    val nbytes = buffers.foldLeft(0)((sz, db) => sz + db.size)
    val segment = getSegmentOffset(offset)
    val maxWrite = segmentSize - segment.offsetWithinSegment
    val nwrite = if nbytes <= maxWrite then nbytes else maxWrite

    def updateExistingSegment(dos: DataObjectState): Future[(DataObjectState,List[DataBuffer])] =
      val bbSize = if dos.data.size >= segment.offsetWithinSegment + nwrite then
        dos.data.size
      else
        segment.offsetWithinSegment + nwrite

      val bb = ByteBuffer.allocate(bbSize)

      bb.put(dos.data)
      bb.position(segment.offsetWithinSegment)
      val remaining = rfill(bb, buffers)

      bb.position(0)
      val db = DataBuffer(bb)

      val newDos = new DataObjectState(
        dos.pointer,
        tx.revision,
        ObjectRefcount(0, 1),
        HLCTimestamp.now,
        HLCTimestamp.now,
        dos.pointer.ida.calculateEncodedSegmentLength(db.size),
        db)

      tx.overwrite(dos.pointer, dos.revision, db)

      Future.successful(newDos, remaining)

    def allocateNewSegment(): Future[(DataObjectState,List[DataBuffer])] =
      val bb = ByteBuffer.allocate(segment.offsetWithinSegment + nwrite)
      bb.position(segment.offsetWithinSegment)
      val remaining = rfill(bb, buffers)
      bb.position(0)
      val db = DataBuffer(bb)

      for
        tree <- initializeContentTree()
        alloc <- fs.defaultSegmentAllocator()
        guard = ObjectRevisionGuard(file.pointer.pointer, file.revision)
        ptr <- alloc.allocateDataObject(guard, db)
        _ <- tree.set(
          Key(segment.segmentBeginOffset),
          Value(ptr.toArray),
          Some(Left(true)))
      yield
        val dos = new DataObjectState(
          ptr,
          tx.revision,
          ObjectRefcount(0, 1),
          HLCTimestamp.now,
          HLCTimestamp.now,
          ptr.ida.calculateEncodedSegmentLength(db.size),
          db)

        (dos, remaining)

    tx.result.failed.foreach: _ =>
      dosCache.invalidate(segment.segmentBeginOffset)

    for
      odos <- readSegment(segment.segmentBeginOffset)
      (dos, remaining) <- odos match
        case None => allocateNewSegment()
        case Some(dos) => updateExistingSegment(dos)
    yield
      def updateDos(): Future[Unit] =
        dosCache.put(segment.segmentBeginOffset, dos)
        Future.unit

      val newFileSize = if offset + nwrite > file.inode.size then
        offset + nwrite
      else
        file.inode.size

      WriteResult(newFileSize, offset + nwrite, remaining, updateDos)


object SimpleFileContent:

  case class WriteResult(newFileSize: Long,
                         remainingOffset: Long,
                         remainingData: List[DataBuffer],
                         postCommitOp: () => Future[Unit])

  case class SegmentOffset(segmentBeginOffset: Long, offsetWithinSegment: Int)

  @tailrec
  def rfill(bb: ByteBuffer, buffs: List[DataBuffer]): List[DataBuffer] =
    if buffs.isEmpty then
      Nil
    else if bb.remaining == 0 then
      buffs
    else if buffs.head.size > bb.remaining then
      val (toWrite, remaining) = buffs.head.split(bb.remaining)
      bb.put(toWrite)
      rfill(bb, remaining :: buffs.tail)
    else
      bb.put(buffs.head)
      rfill(bb, buffs.tail)

  object DeleteFileContentTask extends DurableTaskFactory:
    private val RootPointerKey: Key = Key(1)

    val typeUUID: UUID = UUID.fromString("c1fb782f-7f13-4921-8ddf-155123445730")

    override def createTask(client: AspenClient,
                            pointer: DurableTaskPointer,
                            revision: ObjectRevision,
                            state: Map[Key, KeyValueObjectState.ValueState]): DurableTask =
      new DeleteFileContentTask(client, pointer, revision, state)
  
    /** Returns a Future that completes then the task creation is prepared for commit of the transaction. The inner future
      * completes when the index completion task finishes
      */
    def prepareTask(fs: FileSystem,
                    root: Root)
                   (using tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] =
      fs.taskExecutor.prepareTask(
        DeleteFileContentTask,
        List(DeleteFileContentTask.RootPointerKey -> root.encode())
      ).map: fcomplete => 
        fcomplete.map(_ => ()) 
  
  /** Deletes an index. Note this this implementation is NOT for indicies with shared data. That would require
    *  exactly-once reference count decrements which this implementation does not currently enforce.
    */
  class DeleteFileContentTask private(client: AspenClient,
                                      val taskPointer: DurableTaskPointer,
                                      revision: ObjectRevision,
                                      initialState: Map[Key, KeyValueObjectState.ValueState]
                                ) extends DurableTask:

    import DeleteFileContentTask.*

    given ExecutionContext = client.clientContext

    private val promise = Promise[Option[AnyRef]]()

    def completed: Future[Option[AnyRef]] = promise.future

    resume()

    def resume(): Unit =

      def deleteDataSegment(key: Key, vs: ValueState): Future[Unit] =
        val segmentPointer = DataObjectPointer(vs.value.bytes)

        client.retryStrategy.retryUntilSuccessful:
          client.readOptional(segmentPointer).flatMap:
            case None => Future.unit
            case Some(dos) =>
              given tx: Transaction = client.newTransaction()
              tx.setRefcount(dos.pointer, dos.refcount, dos.refcount.decrement())
              tx.commit().map(_ => ())

      client.retryStrategy.retryUntilSuccessful:
        val contentTree = new TieredKeyValueList(
          client,
          KVObjectRootManager(client, RootPointerKey, taskPointer.kvPointer))

        contentTree.deleteTree(Some(deleteDataSegment)).map: _ =>
          promise.success(None)

  

