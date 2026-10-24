package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.amoebafs.{DirectoryPointer, FileInode, FilePointer}
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.amoebafs.Directory

import scala.concurrent.Future

/** A file system on a pool whose slice limit is far below the 1 MiB default TKVL node size. New directories and
  * files must get node sizes that fit the pool, or their first write fails with NodeSizeExceedsAllocatorLimit.
  */
class LimitedPoolFileSystemSuite extends FilesSystemTestSuite:
  override def testPoolMaxSliceSize: Option[Int] = Some(4096)
  override def testStoreMaxSliceSize: Option[Long] = Some(4096L)

  private def mkdir(dir: Directory, name: String): Future[DirectoryPointer] =
    given tx: Transaction = dir.fs.client.newTransaction()
    val fprep = dir.prepareCreateDirectory(name, mode = 0, uid = 1, gid = 2)
    fprep.foreach(_ => tx.commit())
    fprep.flatMap(fresult => fresult)

  private def mkfile(dir: Directory, name: String): Future[FilePointer] =
    given tx: Transaction = dir.fs.client.newTransaction()
    val fprep = dir.prepareCreateFile(name, mode = 0, uid = 1, gid = 2)
    fprep.foreach(_ => tx.commit())
    fprep.flatMap(fresult => fresult)

  /** Writes until nothing remains; a single write call may stop part way, as in the "Resume write" test */
  private def writeAll(file: SimpleFile, offset: Long, data: List[DataBuffer]): Future[Unit] =
    file.write(offset, data).flatMap: (remainingOffset, remainingData) =>
      if remainingData.isEmpty then
        Future.unit
      else
        writeAll(file, remainingOffset, remainingData)

  atest("a new subdirectory and a new file can be created and written on a limited pool"):
    val data = Array.tabulate[Byte](3000)(i => (i % 251).toByte)
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      subPtr <- mkdir(rootDir, "sub")
      sub <- fs.loadDirectory(subPtr)
      // Both of these write the new subdirectory's own entry tree
      nestedPtr <- mkdir(sub, "nested")
      filePtr <- mkfile(sub, "file")
      subContents <- sub.getContents()
      (inode, _, revision) <- fs.readInode(filePtr)
      // Small segments keep each data object within the pool limit, so the write inserts several entries
      // into the new file's segment tree
      file = new SimpleFile(filePtr, revision, inode.asInstanceOf[FileInode], fs, Some(1024))
      _ <- writeAll(file, 0, List(DataBuffer(data)))
      _ <- waitForTransactionsToComplete()
      readBack <- file.debugReadFully()
    yield
      subContents.map(_.name).toSet should be (Set("nested", "file"))
      readBack should be (data)

  atest("a file using the default segment size can be written past the pool's maxObjectSize"):
    val data = Array.tabulate[Byte](10000)(i => (i % 251).toByte)
    for
      fs <- bootstrap()
      rootDir <- fs.loadRoot()
      filePtr <- mkfile(rootDir, "default-segments")
      (inode, _, revision) <- fs.readInode(filePtr)
      // No explicit segment size: the file system's default must be fitted to the pool
      file = new SimpleFile(filePtr, revision, inode.asInstanceOf[FileInode], fs)
      _ <- writeAll(file, 0, List(DataBuffer(data)))
      _ <- waitForTransactionsToComplete()
      readBack <- file.debugReadFully()
    yield
      readBack should be (data)
