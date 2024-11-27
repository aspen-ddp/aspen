package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.amoebafs.impl.simple.SimpleBaseFile.FileOperation
import org.aspen_ddp.aspen.amoebafs.{File, FileInode, FilePointer, FileSystem, Inode, Timespec}

import scala.concurrent.{ExecutionContext, Future, Promise}

class SimpleFile(override val pointer: FilePointer,
                 cachedInodeRevision: ObjectRevision,
                 initialInode: FileInode,
                 fs: FileSystem,
                 osegmentSize: Option[Int]=None,
                 otierNodeSize: Option[Int]=None) extends SimpleBaseFile(pointer, cachedInodeRevision, initialInode, fs) with File {

  import SimpleFile._

  private var oifc: Option[IndexedFileContent] = None

  // This is called whenever file operations fail. Drop the IFC cache as well just to be safe
  override def refresh(): Future[Unit] =  {
    synchronized {
      oifc.foreach(_.dropCache())
    }
    super.refresh()
  }

  private def content: IndexedFileContent = synchronized {
    if (oifc.isEmpty)
      oifc = Some(new IndexedFileContent(this, osegmentSize, otierNodeSize))
    oifc.get
  }

  override protected def setCachedInode(newInode: Inode, newRevision:ObjectRevision): Unit = synchronized {
    super.setCachedInode(newInode, newRevision)
    if (inode.ocontents.isEmpty)
      oifc = None
  }

  override def inode: FileInode = super.inode.asInstanceOf[FileInode]

  override def inodeState: (FileInode, ObjectRevision) = {
    val t = super.inodeState
    (t._1.asInstanceOf[FileInode], t._2)
  }

  override def freeResources(): Future[Unit] = {
    new SimpleFileHandle(this, 0).truncate(0).map(_ => ())
  }

  def debugReadFully(): Future[Array[Byte]] = content.debugReadFully()

  def read(offset: Long, nbytes: Int): Future[Option[DataBuffer]] = {
    content.read(offset, nbytes)
  }

  def write(offset: Long,
            buffers: List[DataBuffer]): Future[(Long, List[DataBuffer])] = {
    val op = Write(this, offset, buffers)
    enqueueOp(op)
    op.writePromise.future
  }

  def truncate(offset: Long): Future[Future[Unit]] = {
    val op = Truncate(this, offset)
    enqueueOp(op).map(_ => op.deleteComplete)
  }
}

object SimpleFile {
  case class Truncate(file: SimpleFile, offset: Long) extends FileOperation {

    private val p = Promise[Unit]()

    def deleteComplete: Future[Unit] = p.future

    def prepareTransaction(pointer: DataObjectPointer,
                           revision: ObjectRevision,
                           inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): Future[Inode] = synchronized {
      file.content.truncate(offset).map { t =>
        val (ws, fdeleteComplete) = t

        fdeleteComplete.foreach(_ => p.success(()))

        val updatedInode = inode.asInstanceOf[FileInode].updateContent(offset, Timespec.now, ws.newRoot)
        tx.overwrite(pointer, revision, updatedInode.toDataBuffer)
        updatedInode
      }
    }

  }

  case class Write(file: SimpleFile, offset: Long, buffers: List[DataBuffer]) extends FileOperation {
    val writePromise: Promise[(Long, List[DataBuffer])] = Promise()

    def prepareTransaction(pointer: DataObjectPointer,
                           revision: ObjectRevision,
                           inode: Inode)(implicit tx: Transaction, ec: ExecutionContext): Future[Inode] = synchronized {

      val finode = inode.asInstanceOf[FileInode]

      val totalSize = buffers.foldLeft(0)((sz, db) => sz + db.size)

      file.content.write(offset, buffers).map { ws =>

        val nwritten = totalSize - ws.remainingData.foldLeft(0)((sz, db) => sz + db.size)

        val endOffset = offset + nwritten

        val newSize = if (endOffset > finode.size) endOffset else finode.size

        val updatedInode = inode.asInstanceOf[FileInode].updateContent(newSize, Timespec.now, ws.newRoot)

        tx.overwrite(pointer, revision, updatedInode.toDataBuffer)

        tx.result.foreach { _ =>
          writePromise.success((ws.remainingOffset, ws.remainingData))
        }

        updatedInode
      }
    }
  }
}
