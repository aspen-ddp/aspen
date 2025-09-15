package org.aspen_ddp.aspen.amoebafs.impl.simple

import scala.language.implicitConversions
import org.aspen_ddp.aspen.client.{ObjectAllocator, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{RootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.objects.{AllocationRevisionGuard, DataObjectPointer, Key, Value}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.DoesNotExist
import org.aspen_ddp.aspen.amoebafs.{FileSystem, Inode, InodePointer, InodeTable}

import scala.concurrent.{ExecutionContext, Future}

class SimpleInodeTable(
                      val fs: FileSystem,
                      val inodeAllocator: ObjectAllocator,
                      val root: RootManager
                      ) extends InodeTable {

  given ExecutionContext = fs.executionContext

  protected val rnd = new java.util.Random

  protected val table = new TieredKeyValueList(fs.client, root)

  protected var nextInodeNumber: Long = rnd.nextLong()

  protected def allocateInode(): Long = synchronized {
    val t = nextInodeNumber
    nextInodeNumber += 1
    t
  }

  protected def selectNewInodeAllocationPosition(): Unit = synchronized {
    nextInodeNumber = rnd.nextLong()

    while (nextInodeNumber == InodeTable.NullInode)
      nextInodeNumber = rnd.nextLong()
  }

  /** Future completes when the transaction is ready for commit */
  override def prepareInodeAllocation(inode: Inode,
                                      guard: AllocationRevisionGuard)(implicit tx: Transaction): Future[InodePointer] = {

    // Jump to new location if the transaction fails for any reason
    tx.result.failed.foreach( _ => selectNewInodeAllocationPosition() )

    val inodeNumber = allocateInode()
    val updatedInode = inode.update(inodeNumber=Some(inodeNumber))
    val key = Key(inodeNumber)

    for {
      ptr <- fs.defaultInodeAllocator.allocateDataObject(guard, updatedInode.toArray)
      iptr = InodePointer(inode.fileType, inodeNumber, ptr)
      _ <- table.set(key, Value(iptr.toArray), requirement = Some(Left(true)))
    } yield {
      iptr
    }
  }

  /** Removes the Inode from the table. This method does NOT decrement the reference count on the Inode object. */
  override def delete(pointer: InodePointer)(implicit tx: Transaction): Future[Unit] = {
    table.delete(Key(pointer.number))
  }

  override def lookup(inodeNumber: Long): Future[Option[InodePointer]] = {
    for {
      ovs <- table.get(Key(inodeNumber))
    } yield {
      ovs match {
        case None => None
        case Some(vs) => Some(InodePointer(vs.value.bytes))
      }
    }
  }
}
