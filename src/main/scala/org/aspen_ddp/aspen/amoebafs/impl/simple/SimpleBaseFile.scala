package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.client.{InvalidObject, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.amoebafs.{BaseFile, FileMode, FileSystem, Inode, InodePointer, Timespec}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object SimpleBaseFile {

  trait FileOperation {
    /** This promise completes after the file has been successfully updated and all internal data
      * structures (such as the FileIndex) have been updated to reflect the new file state.
      */
    val promise: Promise[Unit] = Promise[Unit]()

    def result: Future[Unit] = promise.future

    def prepareTransaction(pointer: DataObjectPointer,
                           revision: ObjectRevision,
                           inode: Inode)
                          (using tx: Transaction, ec: ExecutionContext): Future[(Inode, () => Future[Unit])]
  }

  case class Flush() extends FileOperation {
    override def prepareTransaction(pointer: DataObjectPointer,
                                    revision: ObjectRevision,
                                    inode: Inode)
                                   (using tx: Transaction, ec: ExecutionContext): Future[(Inode, () => Future[Unit])] = {
      // Nothing to do. Due to serial nature of execution, by the time this method is called, all previous operations
      // have successfully committed
      Future.successful((inode, () => Future.unit))
    }
  }

  abstract class SimpleSet extends FileOperation {

    def update(inode: Inode): Inode

    def prepareTransaction(pointer: DataObjectPointer,
                           revision: ObjectRevision,
                           inode: Inode)
                          (using tx: Transaction, ec: ExecutionContext): Future[(Inode, () => Future[Unit])] = {

      val updatedInode = update(inode)

      tx.overwrite(pointer, revision, updatedInode.toDataBuffer)

      Future.successful((updatedInode, () => Future.unit))
    }
  }

  case class SetUID(uid: Int) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(uid = Some(uid))
  }

  case class SetGID(gid: Int) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(gid = Some(gid))
  }

  case class SetLinks(links: Int) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(links = Some(links))
  }

  case class SetMode(newMode: Int) extends SimpleSet {
    private val maskedMode = newMode & ~FileMode.S_IFMT
    def update(inode: Inode): Inode = inode.update(mode = Some((inode.mode & FileMode.S_IFMT) | maskedMode))
  }

  case class SetCtime(ts: Timespec) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(ctime = Some(ts))
  }

  case class SetMtime(ts: Timespec) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(mtime = Some(ts))
  }

  case class SetAtime(ts: Timespec) extends SimpleSet {
    def update(inode: Inode): Inode = inode.update(atime = Some(ts))
  }

  case class SetAttr(uid: Int, gid: Int, ct: Timespec, mt: Timespec, at: Timespec, mode: Int)
    extends SimpleSet {

    private val maskedMode = mode & ~FileMode.S_IFMT

    def update(inode: Inode): Inode = {
      inode.update(mode = Some((inode.mode & FileMode.S_IFMT) | maskedMode), uid = Some(uid), gid = Some(gid),
        ctime = Some(ct), mtime = Some(mt), atime = Some(at))
    }
  }
}

/** Provides a simple mechanism for satisfying file modification operations across all file types.
  *
  * All updates to a file involve updating the mtime. To minimize conflicts, we'll use a per-file singleton object that multiple
  * file handles can refer to. All modification options are serialized by the File interface (internally maintains a concurrent
  * linked list of requested operations).
  *
  * All file operations are functions called to accomplish the specific requested task and return a Future to the result.
  * E.g. setMtime(newMtime), setCtime(newCtime) Base inode trait has common methods for doing the basic metadata updates across
  * all file types. Requires maintaining an internal queue of operations to prevent self-contention
  */
abstract class SimpleBaseFile(val pointer: InodePointer,
                              protected var cachedInodeRevision: ObjectRevision,
                              private var cachedInode: Inode,
                              val fs: FileSystem) extends BaseFile with Logging {

  import SimpleBaseFile._

  given ExecutionContext = fs.executionContext

  private val pendingOps = new java.util.concurrent.ConcurrentLinkedQueue[FileOperation]()
  private var activeOp: Option[FileOperation] = None

  def inode: Inode = synchronized { cachedInode }
  def revision: ObjectRevision = synchronized { cachedInodeRevision }

  def inodeState: (Inode, ObjectRevision) = synchronized {(cachedInode, cachedInodeRevision)}

  protected def setCachedInode(newInode: Inode, newRevision:ObjectRevision): Unit = synchronized {
    cachedInode = newInode
    cachedInodeRevision = newRevision
  }

  def refresh(): Future[Unit] = synchronized {
    fs.readInode(pointer).map(t => setCachedInode(t._1, t._3))
  }

  def mode: Int = inode.mode
  def uid: Int = inode.uid
  def gid: Int = inode.gid
  def links: Int = inode.links
  def ctime: Timespec = inode.ctime
  def mtime: Timespec = inode.mtime
  def atime: Timespec = inode.atime

  def setMode(newMode: Int): Future[Unit] = enqueueOp(SetMode(newMode))
  def setUID(uid: Int): Future[Unit] = enqueueOp(SetUID(uid))
  def setGID(gid: Int): Future[Unit] = enqueueOp(SetGID(gid))
  def setLinks(links: Int): Future[Unit] = enqueueOp(SetLinks(links))
  def setCtime(ts: Timespec): Future[Unit] = enqueueOp(SetCtime(ts))
  def setMtime(ts: Timespec): Future[Unit] = enqueueOp(SetMtime(ts))
  def setAtime(ts: Timespec): Future[Unit] = enqueueOp(SetAtime(ts))

  def flush(): Future[Unit] = enqueueOp(Flush())

  def prepareHardLink()(using tx: Transaction): Unit = synchronized {
    val updatedInode = cachedInode.update(links=Some(cachedInode.links+1))
    tx.overwrite(pointer.pointer, cachedInodeRevision, updatedInode.toDataBuffer)
    tx.result.foreach { _ =>
      logger.info(s"prepareHardLink - Updating cached inode with links = ${updatedInode.links}")
      setCachedInode(updatedInode, tx.revision)
    }
  }

  def prepareUnlink()(using tx: Transaction): Future[Future[Unit]] = synchronized {
    val updatedInode = inode.update(links=Some(inode.links-1))

    tx.overwrite(pointer.pointer, cachedInodeRevision, updatedInode.toDataBuffer)

    tx.result.foreach(_ => setCachedInode(updatedInode, tx.revision))

    if (inode.links == 1)
      UnlinkFileTask.prepareTask(fs, pointer).map(f => f.map(_=>()))
    else
      Future.successful(Future.unit)
  }

  def setattr(
               newUID: Int,
               newGID: Int,
               ctime: Timespec,
               mtime: Timespec,
               atime: Timespec,
               newMode: Int): Future[Unit] = enqueueOp(SetAttr(newUID, newGID, ctime, mtime, atime, newMode))

  protected def enqueueOp(op: FileOperation): Future[Unit] = {
    pendingOps.add(op)
    beginNextOp()
    op.result.map(_=>())
  }

  private def beginNextOp(): Unit = synchronized {
    activeOp match {
      case Some(_) =>
      case None =>
        val next = pendingOps.poll()
        if (next != null) {
          //println(s"Begining Inode operation on ${inode.pointer.pointer.uuid} with revision ${inode.revision}")
          activeOp = Some(next)
          executeOp(next)
        }
    }
  }

  private def executeOp(op: FileOperation): Unit = {

    def onCommitFailure(foo: Throwable): Future[Unit] = {
      //println(s"OnCommitFailure: $foo")
      //foo.printStackTrace()
      refresh().recover {
        case err: InvalidObject => throw StopRetrying(err) // Only InvalidObject should cause a read failure, which means the object has been deleted
        case err =>
          logger.error(s"Unexpected error encountered during inode refresh: $err")
          throw StopRetrying(err)
      }.map(_=>())
    }

    def attempt(): Future[(ObjectRevision, Inode, () => Future[Unit])] = {
      given tx: Transaction = fs.client.newTransaction()

      tx.note(s"FileOperation: ${op.getClass.getSimpleName}: ${op.toString}")

      val (ainode, arevision) = inodeState

      def commit(): Future[ObjectRevision] = {
        if (tx.valid)
          tx.commit().map(_ => tx.revision)
        else
          Future.unit.map(_ => arevision) // op added nothing to the transaction
      }

      val fresult = for
        (updatedInode, postCommitOp) <- op.prepareTransaction(pointer.pointer, arevision, ainode)

        updatedRevision <- commit()
      yield 
        (updatedRevision, updatedInode, postCommitOp)

      fresult.failed.foreach(err => tx.invalidateTransaction(err))

      fresult
    }

    var retryCount = 0

    fs.retryStrategy.retryUntilSuccessful(onCommitFailure) {

      retryCount += 1

      if (retryCount % 10 == 0)
        println(s"Slow/Hung write. Retry count: $retryCount. ${op.getClass.getSimpleName}: ${op.toString}")

      val fattempt = attempt()

      //fattempt.failed.foreach: cause =>
      //  println(s"OP FAILED ${this.getClass.getSimpleName}: $cause")

      fattempt.map: t =>
        synchronized:
          val (newRevision, updatedInode, postCommitOp) = t

          setCachedInode(updatedInode, newRevision)
          
          postCommitOp().foreach: _ =>
            synchronized:
              activeOp = None
              op.promise.success(())
              beginNextOp()

    }.failed.foreach { cause =>
      // Propagate critical failures to the caller (attempted operation on deleted inode)
      op.promise.failure(cause)
    }
  }
}
