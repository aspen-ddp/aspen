package org.aspen_ddp.aspen.amoebafs

import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.objects.ObjectRevision

import scala.concurrent.Future

trait BaseFile {
  val pointer: InodePointer
  val fs: FileSystem

  def inode: Inode
  def revision: ObjectRevision

  def inodeNumber: Long = pointer.number

  def refresh(): Future[Unit]

  protected def setCachedInode(newInode: Inode, newRevision:ObjectRevision): Unit

  def mode: Int
  def uid: Int
  def gid: Int
  def ctime: Timespec
  def mtime: Timespec
  def atime: Timespec
  def links: Int

  def setMode(newMode: Int): Future[Unit]

  def setUID(uid: Int): Future[Unit]

  def setGID(gid: Int): Future[Unit]

  def setCtime(ts: Timespec): Future[Unit]

  def setMtime(ts: Timespec): Future[Unit]

  def setAtime(ts: Timespec): Future[Unit]

  def flush(): Future[Unit]

  def prepareHardLink()(implicit tx: Transaction): Unit

  def prepareUnlink()(implicit tx: Transaction): Future[Future[Unit]]

  def setattr(
               newUID: Int,
               newGID: Int,
               ctime: Timespec,
               mtime: Timespec,
               atime: Timespec,
               newMode: Int): Future[Unit]

  /** Frees all objects owned by the inode */
  def freeResources(): Future[Unit] = Future.unit
}
