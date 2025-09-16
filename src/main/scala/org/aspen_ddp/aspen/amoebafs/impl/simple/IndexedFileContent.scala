package org.aspen_ddp.aspen.amoebafs.impl.simple

import java.nio.ByteBuffer
import java.util.UUID

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, InvalidObject, KeyValueObjectState, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, ObjectId, ObjectPointer, ObjectRevision, ObjectRevisionGuard}
import org.aspen_ddp.aspen.common.util.Varint
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskPointer, DurableTaskType}
import org.aspen_ddp.aspen.amoebafs.{FileInode, FileSystem}
import org.apache.logging.log4j.scala.{Logger, Logging}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions

class IndexedFileContent(file: SimpleFile, osegmentSize: Option[Int]=None, otierNodeSize: Option[Int]=None) extends Logging {
  import IndexedFileContent._

  private var otail: Option[Tail] = None

  private val fs = file.fs

  def refresh()(using ec: ExecutionContext): Future[Unit] = refreshRoot().map(_=>())

  private def refreshRoot()(using ec: ExecutionContext): Future[IndexNode] = {

    dropCache()

    file.refresh() flatMap { _ =>
      file.inode.ocontents match {
        case Some(dp) => load(dp)

        case None => throw new FatalIndexError("Root Index Pointer Deleted")
      }
    }
  }

  private def getOrAllocateRoot()(using tx: Transaction, ec: ExecutionContext): Future[IndexNode] = {
    val (inode, revision) = file.inodeState

    inode.ocontents match {
      case None =>
        dropCache()

        val content = IndexNode.getEncodedNodeContent()

        allocateIndexNode(file.pointer.pointer, revision, tier=0, content=content).map { newPointer =>
          //println(s"New Root Node: ${newPointer.uuid}")
          IndexNode(newPointer, ObjectRevision(tx.id), content, this)
        }

      case Some(dp) => load(dp)
    }
  }

  private val segmentSize = osegmentSize.getOrElse(fs.defaultSegmentSize)

  def getSegmentOffset(offset: Long): SegmentOffset = {
    val offsetWithinSegment = (offset % segmentSize).toInt
    SegmentOffset(offset - offsetWithinSegment, offsetWithinSegment)
  }

  private def tierNodeSize(tier: Int): Int = otierNodeSize.getOrElse(fs.defaultFileIndexNodeSize(tier))

  private def read(pointer: DataObjectPointer): Future[DataObjectState] = fs.client.read(pointer)

  private val cache: Cache[ObjectId, IndexNode] = Scaffeine().maximumSize(50).build[ObjectId, IndexNode]()

  def dropCache(): Unit = synchronized {
    otail = None
    cache.invalidateAll()
  }

  private def updateCachedNodes(updatedNodes: List[IndexNode])(using tx: Transaction): Unit = {
    logger.trace(s"Tx ${tx.id} Updating IndexNodes ${updatedNodes.map(n => (n.id, n.revision))}")
    updatedNodes.foreach(n => cache.put(n.id, n))
  }

  private def load(nodePointer: DataObjectPointer)(using ec: ExecutionContext): Future[IndexNode] = {
    cache.getIfPresent(nodePointer.id) match {
      case Some(n) =>
        logger.trace(s"Loading cached IndexNode ${nodePointer.id} revision ${n.revision}")
        Future.successful(n)
      case None => fs.client.read(nodePointer).flatMap { dos =>
        val n = IndexNode(dos.pointer, dos.revision, dos.data, this)
        //println(s"Loading Index Node ${n.uuid}. Tier ${n.tier} Entries: ${n.entries.toList.map(e => (e.offset -> e.pointer.uuid))}")
        cache.put(n.id, n)
        Future.successful(n)
      }
    }
  }

  private def allocateIndexNode(
                                 allocObj: ObjectPointer,
                                 allocRev: ObjectRevision,
                                 tier: Int,
                                 content: DataBuffer)(using tx: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    logger.trace("Allocating new index node")

    fs.defaultIndexNodeAllocator(tier).flatMap { allocater =>
      allocater.allocateDataObject(ObjectRevisionGuard(allocObj, allocRev), content).map { p =>
        tx.note(s"IndexedFileContent - Allocating new index node ${p.id} for tier $tier")
        p
      }
    }
  }

  private def allocateDataSegment(
                                   allocObj: ObjectPointer,
                                   allocRev: ObjectRevision,
                                   offset: Long,
                                   content: DataBuffer)(using tx: Transaction, ec: ExecutionContext): Future[DataObjectPointer] = {
    logger.trace("Allocating new data segment")

    fs.defaultSegmentAllocator().flatMap { allocater =>
      allocater.allocateDataObject(ObjectRevisionGuard(allocObj, allocRev), content).map { p =>
        tx.note(s"IndexedFileContent - Allocating new data segment ${p.id} offset $offset, size ${content.size}, hash ${content.hashString}")
        p
      }
    }
  }

  def read(offset: Long, nbytes: Int)(using ec: ExecutionContext): Future[Option[DataBuffer]] = {
    file.inode.ocontents match {
      case None => Future.successful(None)
      case Some(dp) =>

        val bb = ByteBuffer.allocate(nbytes)
        //println(s"Loading index")
        load(dp).flatMap { node =>
          //println(s"Index Node Loaded ${node.uuid}. offset: $offset. nbytes $nbytes Entries: ${node.entries.toList.map(d => (d.offset -> d.pointer.uuid))}")
          node.getIndexEntriesForRange(offset, nbytes).flatMap { t =>
            val (_, segments) = t
            //println(s"Entry for range: ${segments.map(x => (x._1.offset -> x._1.pointer.uuid))}")
            val fbufs = Future.sequence(segments.map { t =>
              val (d, _) = t
              read(d.pointer).map( dos => (d.offset, dos.data, dos.pointer.id) )
            })

            fbufs.map { ebufs =>
              logger.info(s"*** READ DATA SEGMENT SIZES from offset $offset: ${ebufs.map(t => (t._3, t._2.size))}")
              ebufs.foreach { t =>
                val (doffset, rawdb, _) = t

                val bufferOffset = if (doffset <= offset)
                  0
                else
                  (doffset - offset).toInt

                try {

                  if (bufferOffset < nbytes) {
                    if (doffset + rawdb.size > offset) {
                      logger.info(s"*** lslice doffset $doffset offset $offset rawdb.size ${rawdb.size} lslice ${offset - doffset}")
                      val lslice = if (doffset < offset) rawdb.slice((offset - doffset).toInt) else rawdb
                      logger.info(s"*** trim bufferOffset $bufferOffset lslice.size ${lslice.size} nbytes $nbytes nbytes-bufferOffset ${nbytes - bufferOffset}")
                      val trimmed = if (bufferOffset + lslice.size > nbytes) lslice.slice(0, nbytes - bufferOffset) else lslice
                      logger.info(s"*** bb.position($bufferOffset)")
                      bb.position(bufferOffset)
                      bb.put(trimmed)
                    }
                  }
                } catch {
                  case omg: java.lang.IllegalArgumentException =>
                    logger.error(s"OMG!!! THIS IS IT: $omg")
                    logger.error(s"offset: $offset nbytes: $nbytes doffset: $doffset bufferOffset $bufferOffset rawsize: ${rawdb.size}")
                }
              }

              bb.position(0)
              Some(DataBuffer(bb))
            }
          }
        }
    }
  }

  def debugReadFully()(using ec: ExecutionContext): Future[Array[Byte]] = read(0, file.inode.size.toInt).map {
    case None => new Array[Byte](0)
    case Some(db) => db.getByteArray
  }

  /** Allocates a segmentOffset aligned Segment, prepended with a zero-filled buffer if the offset is does not match
    * the segment offset
    */

  @tailrec private def recursiveAlloc(
                              offset: Long,
                              remaining: List[DataBuffer],
                              allocated: List[Future[(DownPointer, DataBuffer)]]=Nil)(using tx: Transaction, ec: ExecutionContext): List[Future[(DownPointer, DataBuffer)]] = {

    if (remaining.isEmpty)
      allocated.reverse
    else {

      val segmentOffset = getSegmentOffset(offset)

      val content = if (segmentOffset.offsetWithinSegment == 0)
        remaining
      else {
        DataBuffer.zeroed(segmentOffset.offsetWithinSegment) :: remaining
      }

      val nbytes = content.foldLeft(0)((sz, db) => sz + db.size)
      val arr = new Array[Byte](if (nbytes <= segmentSize) nbytes else segmentSize)
      val bb = ByteBuffer.wrap(arr)
      val leftover = DataBuffer.fill(bb, content)
      val newSegmentContent = DataBuffer(arr)
      val falloc = allocateDataSegment(file.pointer.pointer,
        file.revision, segmentOffset.segmentBeginOffset, newSegmentContent).map { newPointer =>
        (DownPointer(segmentOffset.segmentBeginOffset, newPointer), newSegmentContent)
      }

      recursiveAlloc(segmentOffset.segmentBeginOffset + segmentSize, leftover, falloc :: allocated)
    }
  }

  /** Returns updated segment content, remaining buffers, and the offset at which they begin */
  private def updateSegment(segmentOffset: Long, pointer: DataObjectPointer, revision: ObjectRevision, data: DataBuffer,
                            offset: Long, buffers: List[DataBuffer])(using tx: Transaction, ec: ExecutionContext): (DataBuffer, List[DataBuffer], Long) = {

    if (offset < segmentOffset)
      logger.error(s"OFFSET IS LESS THAN SEGMENT OFFSET offset=$offset segmentOffset=$segmentOffset")

    val nbytes = buffers.foldLeft(0)((sz, db) => sz + db.size)
    val writeEnd = offset + nbytes
    val segmentEnd = segmentOffset + segmentSize
    val offsetInSegment = (offset - segmentOffset).toInt

    if (offset >= segmentEnd) {
      (data, buffers, offset)
    }
    else if (offsetInSegment >= data.size) {

      val appendBuffers = if (offsetInSegment > data.size) DataBuffer.zeroed(offsetInSegment - data.size) :: buffers else buffers
      val maxAppendSize = segmentSize - data.size
      val (appendBuff, remaining) = DataBuffer.compact(maxAppendSize, appendBuffers)
      val remainingOffset = segmentOffset + data.size + appendBuff.size

      tx.note(s"IndexedFileContent - appending ${appendBuff.size} bytes to data segment ${pointer.id} with segment offset $segmentOffset and current size ${data.size}")
      tx.append(pointer, revision, appendBuff)

      (data.append(appendBuff), remaining, remainingOffset)
    }
    else {

      val objectSize = if (writeEnd >= segmentEnd)
        segmentSize
      else if (writeEnd > segmentOffset + data.size)
        (writeEnd - segmentOffset).toInt
      else
        data.size

      val bb = ByteBuffer.allocate(objectSize)

      if (offset != segmentOffset)
        bb.put(data.slice(0, (offset - segmentOffset).toInt))

      val (overwriteBuff, remaining) = DataBuffer.compact(bb.remaining(), buffers)
      bb.put(overwriteBuff)

      if (bb.remaining() != 0)
        bb.put(data.slice(data.size - bb.remaining()))

      bb.position(0)

      tx.note(s"IndexedFileContent - overwriting data segment ${pointer.id} with segment offset $segmentOffset and current size ${data.size} with new content of size ${bb.remaining()}")

      val newContent = DataBuffer(bb)

      tx.overwrite(pointer, revision, newContent)

      val remainingSize = remaining.foldLeft(0)((sz, db) => sz + db.size)

      (newContent, remaining, writeEnd - remainingSize)
    }
  }

  /**
    * The WriteStatus.writeComplete references the Inode being successfully updated. The future returned in the
    * tuple completes when the background index deletion task is fininshed deleting all of the file content.
    */
  def truncate(endOffset: Long)(using tx: Transaction, ec: ExecutionContext): Future[(WriteStatus, Future[Unit])] = {
    val inode = file.inode

    tx.note(s"IndexedFileContent - preparing file truncation task")

    if (endOffset > inode.size) {
      write(endOffset - 1, List(DataBuffer(Array[Byte](0)))).map(ws => (ws, ws.writeComplete))
    }
    else if (inode.size == endOffset) {
      Future.successful((WriteStatus(inode.ocontents, 0, Nil, Future.unit), Future.unit))
    } else {
      def truncateOrDelete(root: IndexNode): Future[(Future[Unit], Option[DataObjectPointer])] = {
        if (endOffset == 0)
          prepareIndexDeletionTask(fs, root.pointer).map(fdeleteComplete => (fdeleteComplete, None))
        else
          root.truncate(endOffset).map(fdeleteComplete => (fdeleteComplete, Some(root.pointer)))
      }
      for {
        root <- getOrAllocateRoot()
        (fdeleteComplete, optr) <- truncateOrDelete(root)
      } yield {
        val fcomplete = tx.result.map { _ =>
          dropCache()
        }
        (WriteStatus(optr, 0, Nil, fcomplete), fdeleteComplete)
      }
    }
  }

  private def getTail()(using tx: Transaction, ec: ExecutionContext): Future[(Option[Tail], List[IndexNode])] = synchronized {

    val validCachedTail = otail.flatMap { tail =>
      val tailPath = tail.path.map { dp =>
        cache.getIfPresent(dp.pointer.id) match {
          case None => None
          case Some(node) =>

            if (tail.offset >= node.startOffset && node.endOffset.isEmpty)
              Some(node)
            else
              None
        }
      }.collect {
        case Some(node) => node
      }

      if (tailPath.size == tail.path.size)
        Some((tail, tailPath))
      else
        None
    }

    validCachedTail match {
      case Some((tail, tailPath)) => Future.successful((Some(tail), tailPath))

      case None =>
        def readTail(tailPath: List[IndexNode]): Future[(Option[Tail], List[IndexNode])] = synchronized {
          val tailIndexNode = tailPath.head

          if (tailIndexNode.entries.isEmpty)
            Future.successful((None, tailPath))
          else {
            val tailDP = tailIndexNode.entries.last
            read(tailDP.pointer).map { dos =>
              synchronized {
                otail = Some(Tail(tailDP.offset, tailDP.pointer, dos.revision, dos.data, tailPath))
                (otail, tailPath)
              }
            }
          }
        }

        for {
          root <- getOrAllocateRoot()
          tailPath <- root.getTail()
          ot <- readTail(tailPath)
        } yield ot
    }
  }

  private def writeTail(inode: FileInode,
                        beginOffset: Long,
                        beginBuffers: List[DataBuffer])(using tx: Transaction, ec: ExecutionContext): Future[WriteStatus] = {
    val nbytes = beginBuffers.foldLeft(0)((sz, db) => sz + db.size)

    tx.note(s"IndexedFileContent - writeTail(offset=$beginOffset, nbytes=$nbytes)")

    case class WritePrepped(newRoot: IndexNode,
                            updatedNodes: List[IndexNode],
                            newTailPath: List[IndexNode],
                            offset: Long,
                            pointer: DataObjectPointer,
                            newTailContent: DataBuffer)

    def allocNewSegments(tailPath: List[IndexNode], appendOffset: Long, appendBuffers: List[DataBuffer]): Future[WritePrepped] = for {
      allocated <- Future.sequence(recursiveAlloc(appendOffset, appendBuffers))
      (newRoot, updatedNodes, updatedTailPath) <- IndexNode.rupdate(logger, allocated.map(t => t._1), tailPath, tailPath.head)
    } yield {
      val (dp, content) = allocated.last
      WritePrepped(newRoot, updatedNodes, updatedTailPath, dp.offset, dp.pointer, content)
    }

    getTail().flatMap { tpl =>
      val tailPath = tpl._2

      val fprep = tpl._1 match {
        case None => allocNewSegments(tailPath, beginOffset, beginBuffers)

        case Some(tail) =>
          val (newTailData, remainingBuffers, remainingOffset) = updateSegment(tail.offset,
            tail.pointer, tail.revision, tail.data, beginOffset, beginBuffers)

          if (remainingBuffers.isEmpty)
            Future.successful(WritePrepped(tailPath.last, Nil, tailPath, tail.offset, tail.pointer, newTailData))
          else
            allocNewSegments(tailPath, remainingOffset, remainingBuffers)
      }

      fprep.map { prep =>

        val newTail = Tail(prep.offset, prep.pointer, ObjectRevision(tx.id), prep.newTailContent, prep.newTailPath)

        val fcomplete = tx.result.map { _ =>
          synchronized {
            otail = Some(newTail)
            updateCachedNodes(prep.updatedNodes)
          }
        }

        WriteStatus(Some(prep.newRoot.pointer), 0, Nil, fcomplete)
      }
    }
  }



  /** A single write operation cannot add content to two index nodes in the same transaction. When this
    *  condition is encountered, only the data going into the first index node will be written. The
    *  remaining data will be returned in the future. Subsequent write operations will be needed to finish
    *  writing the remaining data
    *
    *  This write method creates DataObjects only at boundaries aligned to index.segmentSize in order to prevent
    *  backwards-writing applications from creating a large number of tiny file segments.
    *
    *  For writes spanning gaps (<obj> <gap> <obj>), update the first object and allocate to fill the gap but stop
    *  at the beginning of the next object? Should simplify impl
    *
    */
  def write(offset: Long,
            buffers: List[DataBuffer])(using tx: Transaction, ec: ExecutionContext): Future[WriteStatus] = {

    val inode = file.inode

    val nbytes = buffers.foldLeft(0)((sz, db) => sz + db.size)
    val writeEnd = offset + nbytes

    val tailSegment = getSegmentOffset(inode.size)

    if (offset >= tailSegment.segmentBeginOffset)
      writeTail(inode, offset, buffers)
    else {

      def checkTail(): Unit = if (writeEnd > tailSegment.segmentBeginOffset) {
        otail.foreach { tail =>
          if (writeEnd > tail.offset)
            otail = None // We've overwritten the tail. Drop the cached copy
        }
      }

      @tailrec def dropRangeAtFirstDiscontinuity(remaining: List[(DownPointer, IndexNode)],
                                        contiguous: List[(DownPointer, IndexNode)] = Nil): List[(DownPointer, IndexNode)] = {
        if (remaining.isEmpty)
          contiguous.reverse
        else {
          if (contiguous.isEmpty)
            dropRangeAtFirstDiscontinuity(remaining.tail, remaining.head :: Nil)
          else {
            if (contiguous.head._1.offset + segmentSize == remaining.head._1.offset)
              dropRangeAtFirstDiscontinuity(remaining.tail, remaining.head :: contiguous)
            else
              contiguous.reverse // Discontinuity found, drop the remaining entries
          }
        }
      }

      // Note, the entries must be filtered by dropRangeAtFirstDiscontinuity above to ensure that no allocations
      // will be required to fill any gaps. Allocations must be avoided to avoid potentially disjoint updates to
      // the index structure which can't be handled by the current recursive index update mechanism. Instead we'll,
      // write what we can and return the rest as unwritten data in the WriteStatus object.
      def updateContiguousRange(
                                 segmentSize: Long,
                                 beginOffset: Long,
                                 remaining: List[DataBuffer],
                                 entries: List[(DownPointer, IndexNode, DataObjectState)] ): (Long, List[DataBuffer]) = {

        require(beginOffset >= entries.head._1.offset && beginOffset < entries.head._1.offset + segmentSize)

        @tailrec def rupdate(writeOffset: Long, toWrite: List[DataBuffer], elist: List[(DownPointer, IndexNode, DataObjectState)]): (Long, List[DataBuffer]) = {
          if (toWrite.isEmpty)
            (writeOffset, Nil)
          else if (elist.isEmpty)
            (writeOffset, toWrite)
          else if (writeOffset < elist.head._1.offset || writeOffset >= elist.head._1.offset + segmentSize)
            (writeOffset, toWrite) // shouldn't be possible due to discontiguous range filter but just to be safe...
          else {

            val (d, _, dos) = elist.head
            val nleft = toWrite.foldLeft(0)((sz, db) => sz + db.size)
            val objOffset = (writeOffset - d.offset).toInt
            val objSize: Long = if (objOffset + nleft > dos.data.size) {
              if (objOffset + nleft > segmentSize) segmentSize else objOffset + nleft
            } else
              dos.size

            val bb = ByteBuffer.allocate(objSize.toInt)
            bb.put(dos.data)

            val nwrite: Long = if (objOffset + nleft < segmentSize) nleft else segmentSize - objOffset
            val (writeBuff, remaining) = if (objOffset + toWrite.head.size > bb.limit()) {
              val (wb, rb) = toWrite.head.split(bb.limit() - objOffset)
              (wb, rb :: toWrite.tail)
            } else {
              DataBuffer.compact(nwrite.toInt, toWrite)
            }

            bb.position(objOffset)
            bb.put(writeBuff)
            bb.position(0)

            tx.note(s"IndexedFileContent - updateContiguousRange(segment=${dos.pointer.id}, nbytes=${bb.remaining()})")
            tx.overwrite(dos.pointer, dos.revision, bb)

            rupdate(writeOffset + nwrite.toInt, remaining, elist.tail)
          }
        }

        rupdate(beginOffset, remaining, entries)
      }

      def prepareWrite(headPath: List[IndexNode], entries: List[(DownPointer, IndexNode, DataObjectState)]): Future[WriteStatus] = {

        def entryContains(d: DownPointer, tgtOffset: Long): Boolean = tgtOffset >= d.offset && tgtOffset < d.offset + segmentSize

        if (entries.isEmpty) {
          // pure allocation within a hole in the file

          for {
            allocated <- Future.sequence(recursiveAlloc(offset, buffers, Nil))
            (newRoot, updatedNodes, _) <- IndexNode.rupdate(logger, allocated.map(t => t._1), headPath, headPath.head)
          } yield {
            val fcomplete = tx.result.map { _ =>
              synchronized {
                checkTail()
                updateCachedNodes(updatedNodes)
              }
            }
            WriteStatus(Some(newRoot.pointer), 0, Nil, fcomplete)
          }
        }
        else if (!entryContains(entries.head._1, offset)) {
          // Write begins with an allocation in a hole and extends over at least one object.
          val allocSize = (entries.head._1.offset - offset).toInt

          val (alloc, remaining) = DataBuffer.compact(allocSize, buffers)

          for {
            allocated <- Future.sequence(recursiveAlloc(offset, alloc :: Nil, Nil))
            (newRoot, updatedNodes, _) <- IndexNode.rupdate(logger, allocated.map(t => t._1), headPath, headPath.head)
          } yield {
            val (remainingOffset, remainingData) = updateContiguousRange(segmentSize, entries.head._1.offset, remaining, entries)
            val fcomplete = tx.result.map { _ =>
              synchronized {
                checkTail()
                updateCachedNodes(updatedNodes)
              }
            }
            WriteStatus(Some(newRoot.pointer), remainingOffset, remainingData, fcomplete)
          }
        }
        else {
          // Write begins in an allocated segment
          val (remainingOffset, remainingData) = updateContiguousRange(segmentSize, offset, buffers, entries)

          val fcomplete = tx.result.map { _ =>
            synchronized {
              checkTail()
            }
          }

          Future.successful(WriteStatus(Some(headPath.last.pointer), remainingOffset, remainingData, fcomplete))
        }
      }

      for {
        root <- getOrAllocateRoot()
        (headPath, rawEntries) <- root.getIndexEntriesForRange(offset, nbytes)
        entries = dropRangeAtFirstDiscontinuity(rawEntries)
        objs <- Future.sequence(entries.map( t => fs.client.read(t._1.pointer).map{ dos => (t._1, t._2, dos) } ))
        ftuple <- prepareWrite(headPath, objs)
      } yield ftuple
    }
  }

}

object IndexedFileContent {

  class FatalIndexError(msg: String) extends Exception(msg)

  class CorruptedIndex extends FatalIndexError("Corrupted Index")

  case class WriteStatus(newRoot: Option[DataObjectPointer],
                         remainingOffset: Long,
                         remainingData: List[DataBuffer],
                         writeComplete: Future[Unit])

  case class SegmentOffset(segmentBeginOffset: Long, offsetWithinSegment: Int)

  /** path is reversed where the first element is the tier-0 IndexNode and the last element is the root IndexNode */
  private class Tail(val offset: Long,
                     val pointer: DataObjectPointer,
                     val revision: ObjectRevision,
                     val data: DataBuffer,
                     val path: List[DownPointer]) {
    def size: Int = data.size
  }

  private object Tail {
    def apply(offset: Long, pointer: DataObjectPointer, revision: ObjectRevision, data: DataBuffer, path: List[IndexNode]): Tail = {
      new Tail(offset, pointer, revision, data, path.map(n => DownPointer(n.startOffset, n.pointer)))
    }
  }

  private case class DownPointer(offset: Long, pointer: DataObjectPointer) {

    def encodedSize: Int = Varint.getUnsignedLongEncodingLength(offset) + pointer.encodedSize

    def encodeInto(bb: ByteBuffer): Unit = {
      Varint.putUnsignedLong(bb, offset)
      pointer.encodeInto(bb)
    }

    def encode(): DataBuffer = {
      val arr = new Array[Byte](encodedSize)
      val bb = ByteBuffer.wrap(arr)
      encodeInto(bb)
      DataBuffer(bb)
    }
  }

  private object DownPointer {
    def apply(bb: ByteBuffer): DownPointer = {
      val offset = Varint.getUnsignedLong(bb)
      val pointer = DataObjectPointer(bb)
      DownPointer(offset, pointer)
    }
  }

  object DeleteIndexTask {

    val RootPointerKey: Key = Key(1)

    object TaskType extends DurableTaskType {

      val typeUUID: UUID = UUID.fromString("c1fb782f-7f13-4921-8ddf-155123445730")

      override def createTask(client: AspenClient,
                              pointer: DurableTaskPointer,
                              revision: ObjectRevision,
                              state: Map[Key, KeyValueObjectState.ValueState]): DurableTask = new DeleteIndexTask(client, pointer, revision, state)

    }
  }

  /** Returns a Future that completes then the task creation is prepared for commit of the transaction. The inner future
    * completes when the index completion task finishes
    */
  private def prepareIndexDeletionTask(
                                        fs: FileSystem,
                                        root: DataObjectPointer)(using tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] = {

    fs.taskExecutor.prepareTask(DeleteIndexTask.TaskType, List((DeleteIndexTask.RootPointerKey, root.toArray))).map { fcomplete => fcomplete.map(_ => ()) }

  }

  /** Deletes an index. Note this this implementation is NOT for indicies with shared data. That would require
    *  exactly-once reference count decrements which this implementation does not currently enforce.
    */
  class DeleteIndexTask private (client: AspenClient,
                                 val taskPointer: DurableTaskPointer,
                                 revision: ObjectRevision,
                                 initialState: Map[Key, KeyValueObjectState.ValueState])
    extends DurableTask {

    import DeleteIndexTask._

    given ExecutionContext = client.clientContext

    private val promise = Promise[Option[AnyRef]]()

    def completed: Future[Option[AnyRef]] = promise.future

    resume()

    def resume(): Unit = {

      def rdelete(nodePointer: DataObjectPointer): Future[Unit] = {
        client.read(nodePointer).flatMap { dos =>

          val (tier, _, _, entries) = IndexNode.decode(dos.data)

          // Recursively destroys one node at a time to prevent a flood of reads and transactions
          // from occurring simultaneously

          def destroyEntry(remaining: List[DownPointer]): Future[Unit] = {
            if (remaining.isEmpty)
              Future.unit
            else {
              val fdeleted = if (tier > 0)
                rdelete(remaining.head.pointer)
              else {
                client.retryStrategy.retryUntilSuccessful {
                  client.read(remaining.head.pointer).flatMap { sdos =>
                    given tx: Transaction = client.newTransaction()
                    tx.note(s"IndexedFileContent - deleteIndexTask decrementing refcount of ${sdos.pointer.id} to ${sdos.refcount.decrement()}")
                    tx.setRefcount(sdos.pointer, sdos.refcount, sdos.refcount.decrement())
                    tx.commit().map(_ => ())
                  } recover {
                    case _: InvalidObject => () // already deleted
                    case t: Throwable => println(s"Unexpected error while deleting truncated data node ${remaining.head.pointer.id}: $t")
                  }
                }
              }

              fdeleted.flatMap(_ => destroyEntry(remaining.tail))
            }
          }

          // Recursively destroy all node contents, then destroy the node itself

          destroyEntry(entries.sortBy(e => e.offset).reverse).flatMap { _ =>
            client.retryStrategy.retryUntilSuccessful {
              client.read(nodePointer).flatMap { sdos =>
                given tx: Transaction = client.newTransaction()
                tx.note(s"IndexedFileContent - deleteIndexTask decrementing self refcount of ${sdos.pointer.id} to ${sdos.refcount.decrement()}")
                tx.setRefcount(sdos.pointer, sdos.refcount, sdos.refcount.decrement())
                tx.commit().map(_=>())
              } recover {
                case _: InvalidObject => () // already deleted
                case t: Throwable => println(s"Unexpected error while deleting truncated index node: $t")
              }
            }
          }.map(_ => ())
        } recover {
          case _: InvalidObject => () // Already done!
        }
      }

      //println(s"******* STARTING FILE TRUNCATION TASK *****")
      client.retryStrategy.retryUntilSuccessful {
        for {
          kvos <- client.read(taskPointer.kvPointer)
          root = kvos.contents(RootPointerKey)
          _ <- rdelete(DataObjectPointer(root.value.bytes))
        } yield {
          promise.success(None)
        }
      }
    }
  }

  private object IndexNode {
    def apply(
               pointer: DataObjectPointer,
               revision: ObjectRevision,
               data: DataBuffer,
               index: IndexedFileContent): IndexNode = {

      val (tier, offset, maxOffset, entries) = decode(data)

      new IndexNode(tier, index, pointer, revision, data.size, offset, maxOffset,
        entries.sortBy(_.offset).toArray)
    }

    def decode(data: DataBuffer): (Byte, Long, Option[Long], List[DownPointer]) = {
      val bb = data.asReadOnlyBuffer()
      val tier = bb.get()
      val offset = Varint.getUnsignedLong(bb)
      val rawMax = Varint.getUnsignedLong(bb)
      val maxOffset = if (rawMax == 0) None else Some(rawMax)

      var entries: List[DownPointer] = Nil

      while(bb.remaining() != 0)
        entries = DownPointer(bb) :: entries

      (tier, offset, maxOffset, entries)
    }

    def getEncodedNodeContent(tier: Int = 0, content: List[DownPointer]=Nil, startOffset: Long = 0, endOffset: Option[Long]=None): DataBuffer = {
      val end = endOffset.getOrElse(0L)
      val sz = 1 + Varint.getUnsignedLongEncodingLength(startOffset) + Varint.getUnsignedLongEncodingLength(end) + content.foldLeft(0)((sz, e) => sz + e.encodedSize)
      val arr = new Array[Byte](sz)
      val bb = ByteBuffer.wrap(arr)
      bb.put(tier.asInstanceOf[Byte])
      Varint.putUnsignedLong(bb, startOffset)
      Varint.putUnsignedLong(bb, end)
      content.foreach(e => e.encodeInto(bb))
      bb.position(0)
      DataBuffer(bb)
    }

    def prepareTruncation(
                           endOffset: Long,
                           path: List[IndexNode])(using tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] = {

      val ftruncateData = path.head.getEntryForOffset(endOffset) match {
        case Some(d) =>
          if (endOffset < d.offset + path.head.index.segmentSize) {
            path.head.index.read(d.pointer).map { dos =>
              if (d.offset + dos.data.size > endOffset) {
                tx.note(s"IndexedFileContent - prepareTruncation shortening final segment ${dos.pointer.id}")
                tx.overwrite(dos.pointer, dos.revision, dos.data.slice(0, (endOffset - d.offset).toInt))
              }
            }
          } else
            Future.unit
        case None => Future.unit
      }

      def rsplit(nodes: List[IndexNode], createdLowerNode: Option[DownPointer]): Future[DownPointer] = {
        if (nodes.isEmpty)
          Future.successful(createdLowerNode.get)
        else {
          val node = nodes.head
          val (keep, discard) = node.entries.partition(e => e.offset <= endOffset)
          val newEntries = createdLowerNode match {
            case None => discard.toList
            case Some(e) => e :: discard.toList.filter(d => d.offset != e.offset) // replace original lower node with the new one
          }
          val updateContent = getEncodedNodeContent(node.tier, keep.toList, node.startOffset, None)
          val newContent = getEncodedNodeContent(node.tier, newEntries, endOffset, node.endOffset)

          tx.note(s"IndexedFileContent - prepareTruncation rsplit of index node ${node.pointer.id}")
          tx.overwrite(node.pointer, node.revision, updateContent)

          node.index.allocateIndexNode(node.pointer, node.revision, tier=node.tier, content=newContent).flatMap { newPointer =>
            rsplit(nodes.tail, Some(new DownPointer(node.startOffset, newPointer)))
          }
        }
      }

      val fsplit = rsplit(path, None)

      for {
        _ <- ftruncateData
        truncatedRoot <- fsplit
        fdeleteComplete <- prepareIndexDeletionTask(path.head.index.fs, truncatedRoot.pointer)
      } yield fdeleteComplete
    }

    /** Updates the head node of the supplied path and propagates index changes up the tree. The updatedPath
      * element of the tuple is a new version of the path argument that contains newly allocated/updated
      * index nodes for the far right (highest offset) updates.
      *
      *  Returns: (NewRootNode, ListOfUpdatedNodes, updatedPath)
      */
    def rupdate(
                 logger: Logger,
                 adds: List[DownPointer],
                 path: List[IndexNode],
                 last: IndexNode,
                 updatedNodes: List[IndexNode] = Nil,
                 rupdatedPath: List[IndexNode] = Nil)(using tx: Transaction, ec: ExecutionContext): Future[(IndexNode, List[IndexNode], List[IndexNode])] = {
      if (adds.isEmpty) {

        if (path.isEmpty)
          Future.successful((last, updatedNodes, rupdatedPath.reverse))
        else
          rupdate(logger, Nil, path.tail, path.head, updatedNodes, path.head :: rupdatedPath)
      }
      else {
        if (path.isEmpty) {
          // Allocate new root
          logger.trace("Allocating new index root")
          val oldRoot = updatedNodes.head

          val newTier = oldRoot.tier + 1

          val content = getEncodedNodeContent(newTier, new DownPointer(oldRoot.startOffset, oldRoot.pointer) :: adds)

          oldRoot.index.allocateIndexNode(oldRoot.pointer, oldRoot.revision, tier=newTier, content=content).map { newPointer =>
            tx.note(s"IndexedFileContent - allocating new index root node ${newPointer.id} tier $newTier")
            val newRoot = IndexNode(newPointer, ObjectRevision(tx.id), content, oldRoot.index)
            (newRoot, newRoot :: updatedNodes, (newRoot :: rupdatedPath).reverse)
          }
        }
        else {
          val node = path.head

          val addSize = adds.foldLeft(0)((sz, e) => sz + e.encodedSize)
          //println(s"ADD INDEX SIZE: $addSize")
          if (node.haveRoomFor(addSize)) {
            val newEntries = (adds ++ node.entries.toList).sortBy(e => e.offset)

            val updated = new IndexNode(node.tier, node.index, node.pointer, ObjectRevision(tx.id), node.encodedSize + addSize,
              node.startOffset, node.endOffset, newEntries.toArray)

            val arr = new Array[Byte](addSize)
            val bb = ByteBuffer.wrap(arr)
            adds.foreach(e => e.encodeInto(bb))

            //println(s"TX APPEND: Adds: ${adds}")
            logger.trace(s"Appending to index tier ${node.tier}")
            tx.note(s"IndexedFileContent - rupdate appending to index tier ${node.tier}, node ${node.pointer.id}")
            tx.append(node.pointer, node.revision, DataBuffer(arr))

            rupdate(logger, Nil, path.tail, path.head, updated :: updatedNodes, updated :: rupdatedPath)
          }
          else {
            logger.trace(s"New index nodes required for tier tier ${node.tier}")
            val allEntries = (adds ++ node.entries).sortBy(e => e.offset)

            val baseNodeSize = 33 // worst case base size for type byte and two Varint longs
            val maxNodeSize = node.index.tierNodeSize(node.tier)

            // Returns a list of node contents (in sorted order). The first element is the content to overwrite the
            // existing index node with, all subsequent lists correspond to nodes that need to be allocated
            //
            @tailrec def rfillNodes(entries: List[DownPointer],
                           currentList: List[DownPointer],
                           currentSize: Int,
                           nodeContents: List[List[DownPointer]]): List[List[DownPointer]] = {

              if (entries.isEmpty)
                (currentList :: nodeContents).map(l => l.reverse).reverse
              else {
                val esize = entries.head.encodedSize
                if (currentSize + esize <= maxNodeSize)
                  rfillNodes(entries.tail, entries.head :: currentList, currentSize + esize, nodeContents)
                else
                  rfillNodes(entries.tail, entries.head :: Nil, baseNodeSize + esize, currentList :: nodeContents)
              }
            }

            val nodeContents = rfillNodes(allEntries, Nil, baseNodeSize, Nil)

            val nodesToAllocate = nodeContents.tail

            val updateEnd = if (nodesToAllocate.isEmpty) node.endOffset else Some(nodesToAllocate.head.head.offset)

            val updateContent = getEncodedNodeContent(node.tier, nodeContents.head, node.startOffset, updateEnd)

            val txrev = ObjectRevision(tx.id)

            tx.note(s"IndexedFileContent - rupdate updating index node ${node.pointer.id}")
            tx.overwrite(node.pointer, node.revision, updateContent)

            def allocNode(endOffset: Option[Long], newNodeEntries: List[DownPointer]): Future[IndexNode] = {
              val newNodeContent = getEncodedNodeContent(node.tier, newNodeEntries, newNodeEntries.head.offset, endOffset)
              node.index.allocateIndexNode(node.pointer, node.revision, tier=node.tier, content=newNodeContent).map { newPointer =>
                tx.note(s"IndexedFileContent - rupdate allocating new index node ${newPointer.id} tier ${node.tier}")
                IndexNode(newPointer, txrev, newNodeContent, node.index)
              }
            }

            @tailrec def rallocNodes(newNodes: List[List[DownPointer]], allocs: List[Future[IndexNode]]): List[Future[IndexNode]] = {
              if (newNodes.isEmpty)
                allocs
              else {
                val endOffset = if (newNodes.tail.isEmpty) node.endOffset else Some(newNodes.tail.head.head.offset)
                rallocNodes(newNodes.tail, allocNode(endOffset, newNodes.head) :: allocs)
              }
            }

            logger.trace(s"Allocating ${nodesToAllocate.length} new index node(s) for tier ${node.tier}")

            Future.sequence(rallocNodes(nodesToAllocate, Nil)).flatMap { allocatedNodes =>
              val updated = IndexNode(node.pointer, txrev, updateContent, node.index)
              rupdate(logger, allocatedNodes.map(n => DownPointer(n.startOffset, n.pointer)), path.tail, path.head,
                updated :: (allocatedNodes ++ updatedNodes), allocatedNodes.last :: rupdatedPath)
            }
          }
        }
      }
    }
  }

  private class IndexNode(
                           val tier: Byte,
                           val index: IndexedFileContent,
                           val pointer: DataObjectPointer,
                           val revision: ObjectRevision,
                           val dataSize: Int,
                           val startOffset: Long,
                           val endOffset: Option[Long],
                           val entries: Array[DownPointer]
                         ) {

    import IndexNode._

    def id: ObjectId = pointer.id

    def isTailNode: Boolean = endOffset.isEmpty

    def encodedSize: Int = {
      val loff = Varint.getUnsignedLongEncodingLength(startOffset)
      val lend = Varint.getUnsignedLongEncodingLength(endOffset.getOrElse(0))
      val lentries = entries.foldLeft(0)((sz, e) => sz + e.encodedSize)
      1 + loff + lend + lentries
    }

    def haveRoomFor(nbytes: Int): Boolean = {
      encodedSize + nbytes <= index.tierNodeSize(tier)
    }

    def getEntryForOffset(targetOffset: Long): Option[DownPointer] = {
      if (targetOffset < startOffset || endOffset.exists(targetOffset > _))
        None
      else if (entries.length == 0)
        None
      else if (entries.length == 1)
        if (entries(0).offset <= targetOffset) Some(entries(0)) else None
      else {
        @tailrec def rfind(idx: Int, last: Option[DownPointer]): Option[DownPointer] = {
          if (idx == entries.length || entries(idx).offset > targetOffset)
            last
          else
            rfind(idx+1, Some(entries(idx)))
        }
        rfind(1, Some(entries(0)))
      }
    }

    def getDownPointerForOffset(targetOffset: Long): Option[DownPointer] = getEntryForOffset(targetOffset) match {
      case Some(d: DownPointer) => Some(d)
      case _ => None
    }

    /** Seeks to the tier0 node owning the specified offset. ALL seeks must start from the root node.
      */
    def seek(targetOffset: Long, path: List[IndexNode]=Nil)(using ec: ExecutionContext): Future[List[IndexNode]] = {
      // If this node doesn't contain the target offset, we've hit an inconsistency during navigation
      // re-start from a refreshed root node
      if (targetOffset < startOffset || endOffset.exists(targetOffset > _))
        index.refreshRoot().flatMap(newRoot => newRoot.seek(targetOffset))
      else {
        if (tier == 0)
          Future.successful(this :: path)
        else {
          getDownPointerForOffset(targetOffset) match {
            case Some(d) => index.load(d.pointer).flatMap(lowerNode => lowerNode.seek(targetOffset, this :: path))
            case None => Future.failed(new CorruptedIndex)
          }
        }
      }
    }

    /** Returns future to reversed path to tail index node. The first element of the list is the tier-0 index
      * node and the last element is the root index node.
      */
    def getTail(path: List[IndexNode]=Nil)(using ec: ExecutionContext): Future[List[IndexNode]] = {
      // If this node doesn't contain the target offset, we've hit an inconsistency during navigation
      // re-start from a refreshed root node
      if (!isTailNode)
        index.refreshRoot().flatMap(newRoot => newRoot.getTail())
      else {
        if (tier == 0)
          Future.successful(this :: path)
        else {
          entries.lastOption match {
            case Some(d: DownPointer) => index.load(d.pointer).flatMap(lowerNode => lowerNode.getTail(this :: path))
            case _ => Future.failed(new CorruptedIndex)
          }
        }
      }
    }

    /** Returns the path to the head IndexNode for the range and a list of the entries within the range along with which index node they
      *  are present within (range queries can potentially span multiple index nodes)
      */
    def getIndexEntriesForRange(offset: Long, nbytes: Int)(using ec: ExecutionContext): Future[(List[IndexNode], List[(DownPointer, IndexNode)])] = {
      val seekOffset = offset - offset % index.segmentSize

      val endOffset = offset + nbytes

      def rgetMore(headPath: List[IndexNode], node: IndexNode, lst: List[(DownPointer, IndexNode)]): Future[(List[IndexNode], List[(DownPointer, IndexNode)])] = {
        val updated = node.entries.foldLeft(lst)( (l, e) => if (e.offset >= seekOffset && e.offset < endOffset) (e, node) :: l else l )

        node.endOffset match {
          case None =>
            Future.successful((headPath, updated.reverse))

          case Some(nodeEnd) =>
            if (nodeEnd >= endOffset)
              Future.successful((headPath, updated.reverse))
            else
              seek(nodeEnd).flatMap(right => rgetMore(headPath, right.head, updated))
        }
      }

      val f = seek(seekOffset).flatMap(path => rgetMore(path, path.head, Nil))

      //      f.foreach { r =>
      //        val (path, l) = r
      //        println(s"IndexEntriesForRange. offset: $offset, nbytes: $nbytes, seekOffset: $seekOffset, path: ${path.map(_.startOffset)}, entries: ${l.map(t => (t._2.startOffset, t._1.offset))}")
      //      }

      f
    }

    /** MUST be called only from the root node */
    def insert(logger: Logger, newEntry: DownPointer)(using tx: Transaction, ec: ExecutionContext): Future[(IndexNode, List[IndexNode])] = {
      seek(newEntry.offset).flatMap(path => rupdate(logger, List(newEntry), path, path.head, Nil).map(t => t._1 -> t._2))
    }

    /** MUST be called only from the root node */
    def insert(logger: Logger, newEntries: List[DownPointer])(using tx: Transaction, ec: ExecutionContext): Future[(IndexNode, List[IndexNode])] = {
      seek(newEntries.head.offset).flatMap(path => rupdate(logger, newEntries, path, path.head, Nil).map(t => t._1 -> t._2))
    }

    /** MUST be called only from the root node */
    def truncate(endOffset: Long)(using tx: Transaction, ec: ExecutionContext): Future[Future[Unit]] = {
      seek(endOffset).flatMap(path => prepareTruncation(endOffset, path))
    }

  } // end IndexNode class
}
