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
                 otierNodeSize: Option[Int]=None) extends SimpleBaseFile(pointer, cachedInodeRevision, initialInode, fs) with File:

  import SimpleFile._

  private val sfc: SimpleFileContent = new SimpleFileContent(this, osegmentSize)

  override def inode: FileInode = super.inode.asInstanceOf[FileInode]

  override def inodeState: (FileInode, ObjectRevision) =
    val t = super.inodeState
    (t._1.asInstanceOf[FileInode], t._2)

  override def freeResources(): Future[Unit] =
    new SimpleFileHandle(this, 0).truncate(0).map(_ => ())

  def debugReadFully(): Future[Array[Byte]] = sfc.debugReadFully()

  def read(offset: Long, nbytes: Int): Future[Option[DataBuffer]] =
    sfc.read(offset, nbytes).map(Some(_))

  def write(offset: Long,
            buffers: List[DataBuffer]): Future[(Long, List[DataBuffer])] =
    sfc.initializeContentTree().flatMap: _ =>
      val op = Write(this, offset, buffers)
      enqueueOp(op)
      op.writePromise.future

  def truncate(offset: Long): Future[Future[Unit]] =
    val op = Truncate(this, offset)
    enqueueOp(op).map(_ => op.deleteComplete)


object SimpleFile:
  case class Truncate(file: SimpleFile, offset: Long) extends FileOperation:

    private val p = Promise[Unit]()

    def deleteComplete: Future[Unit] = p.future

    def prepareTransaction(pointer: DataObjectPointer,
                           revision: ObjectRevision,
                           inode: Inode)
                          (using tx: Transaction, ec: ExecutionContext): Future[(Inode, () => Future[Unit])] =
      file.sfc.truncate(offset).map: fdeleteComplete =>
        fdeleteComplete.foreach(_ => p.success(()))

        val updatedInode = inode.asInstanceOf[FileInode].updateContent(offset, Timespec.now)
        tx.overwrite(pointer, revision, updatedInode.toDataBuffer)

        (updatedInode, () => Future.successful(()))

  case class Write(file: SimpleFile, offset: Long, buffers: List[DataBuffer]) extends FileOperation:
    val writePromise: Promise[(Long, List[DataBuffer])] = Promise()

    def prepareTransaction(pointer: DataObjectPointer,
                           revision: ObjectRevision,
                           inode: Inode)
                          (using tx: Transaction, ec: ExecutionContext): Future[(Inode, () => Future[Unit])] =

      val finode = inode.asInstanceOf[FileInode]

      val totalSize = buffers.foldLeft(0)((sz, db) => sz + db.size)

      file.sfc.write(offset, buffers).map: result =>
        val updatedInode = inode.asInstanceOf[FileInode].updateContent(result.newFileSize, Timespec.now)

        tx.overwrite(pointer, revision, updatedInode.toDataBuffer)

        tx.result.foreach: _ =>
          writePromise.success((result.remainingOffset, result.remainingData))

        (updatedInode, result.postCommitOp)




