package org.aspen_ddp.aspen.amoebafs

import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, DataObjectPointer}

import scala.concurrent.{ExecutionContext, Future}

object InodeTable {
  val NullInode = 0L
  val RootInode = 1L
}

trait InodeTable {

  val fs: FileSystem

  /** Future completes when the transaction is ready for commit */
  def prepareInodeAllocation(inode: Inode,
                             guard: AllocationRevisionGuard)(implicit tx: Transaction): Future[InodePointer]

  /** Removes the Inode from the table. This method does NOT decrement the reference count on the Inode object. */
  def delete(pointer: InodePointer)(implicit tx: Transaction): Future[Unit]

  def lookup(inodeNumber: Long): Future[Option[InodePointer]]

  def lookupRoot(): Future[DirectoryPointer] = {
    given ExecutionContext = fs.executionContext

    lookup(InodeTable.RootInode).map(_.get.asInstanceOf[DirectoryPointer])
  }
}
