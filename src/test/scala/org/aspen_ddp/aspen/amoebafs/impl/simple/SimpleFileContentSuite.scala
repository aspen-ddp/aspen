package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.amoebafs.FileInode

import scala.concurrent.Future
import scala.language.implicitConversions

class SimpleFileContentSuite extends FilesSystemTestSuite:
  def boot(osegmentSize: Option[Int]=None): Future[SimpleFile] = for
    fs <- bootstrap()

    rootDir <- fs.loadRoot()

    tx = fs.client.newTransaction()

    fdir <- rootDir.prepareCreateFile("foo", mode=0, uid=1, gid=2)(using tx)

    _ <- tx.commit()

    newFilePointer <- fdir

    (newInode, _, revision) <- fs.readInode(newFilePointer)
  yield
    new SimpleFile(newFilePointer, revision, newInode.asInstanceOf[FileInode], fs, osegmentSize)

  def readFully(file: SimpleFile): Future[Array[Byte]] =
    for
      _ <- waitForTransactionsToComplete()
      arr <- file.debugReadFully()
    yield arr

  test("Read empty File"):
    for
      file <- boot()
      a <- readFully(file)
    yield
      a.length should be (0)

  test("Read write empty file"):
    val w = Array[Byte](5)
    for
      file <- boot()
      (remainingOffset, remainingData) <- file.write(0, w)
      a <- readFully(file)
    yield
      file.inode.size should be (w.length)
      remainingOffset should be (1)
      remainingData should be (Nil)
      a should be (w)

  test("Read past end of file returns empty DataBuffer"):
    val w = Array[Byte](5)
    for
      file <- boot()
      (remainingOffset, remainingData) <- file.write(0, w)
      a <- readFully(file)
      b <- file.read(6,6)
    yield
      file.inode.size should be(w.length)
      remainingOffset should be(1)
      remainingData should be(Nil)
      a should be(w)
      b.get should be (DataBuffer.Empty)

  test("Read write empty file with hole"):
    val w = Array[Byte](5)
    val e = Array[Byte](0,0,5)
    for
      file <- boot(Some(5))
      (remainingOffset, remainingData) <- file.write(2, w)
      a <- readFully(file)
    yield
      file.inode.size should be (3)
      remainingOffset should be (3)
      remainingData should be (Nil)
      a should be (e)

  test("Read write empty file with hole, multi-segment"):
    val w = Array[Byte](5)
    val e = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5)
    for
      file <- boot(Some(5))
      (remainingOffset, remainingData) <- file.write(12, w)
      a <- readFully(file)
    yield
      file.inode.size should be(13)
      remainingOffset should be(13)
      remainingData should be(Nil)
      a should be(e)

  test("Read write into empty segment mid file"):
    val w = Array[Byte](5)
    val e = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5)
    val f = Array[Byte](0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5)
    for
      file <- boot(Some(5))
      (remainingOffset, remainingData) <- file.write(12, w)
      a <- readFully(file)
      (remainingOffset2, remainingData2) <- file.write(2, w)
      b <- readFully(file)
    yield
      file.inode.size should be(13)
      remainingOffset should be(13)
      remainingOffset2 should be(3)
      remainingData should be(Nil)
      remainingData2 should be(Nil)
      a should be(e)
      b should be(f)

  test("Read write empty file with full segment hole"):
    val w = Array[Byte](5)
    val e = Array[Byte](0,0,0,0,0,0,5)
    for
      file <- boot(Some(5))
      (remainingOffset, remainingData) <- file.write(6, w)
      a <- readFully(file)
    yield
      file.inode.size should be (7)
      remainingOffset should be (7)
      remainingData should be (Nil)
      a should be (e)

  test("Write past single segment"):
    val w = Array[Byte](1,2,3,4,5,6,7,8,9)
    val e = Array[Byte](1,2,3,4,5)
    val r = Array[Byte](6,7,8,9)
    for
      file <- boot(Some(5))
      (remainingOffset, remainingData) <- file.write(0, w)
      _ <- waitForTransactionsToComplete()
      a <- file.read(0,5)
    yield
      file.inode.size should be (5)
      remainingOffset should be (file.inode.size)
      remainingData should not be (Nil)
      remainingData.head.getByteArray should be (r)
      a.get.getByteArray should be (e)

  test("Resume write"):
    val w = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9)
    for
      file <- boot(Some(5))
      (remainingOffset, remainingData) <- file.write(0, w)
      (remainingOffset2, remainingData2) <- file.write(remainingOffset, remainingData.head)
      _ <- waitForTransactionsToComplete()
      a <- file.read(0, 100)
    yield
      file.inode.size should be(w.length)
      a.get.getByteArray should be(w)

  test("Overwrite segment - head"):
    val w = Array[Byte](1, 2, 3, 4, 5)
    val x = Array[Byte](9)
    val r = Array[Byte](9, 2, 3, 4, 5)
    for
      file <- boot(Some(5))
      _ <- file.write(0, w)
      _ <- file.write(0, x)
      _ <- waitForTransactionsToComplete()
      a <- file.read(0, 100)
    yield
      file.inode.size should be(w.length)
      a.get.getByteArray should be(r)

  test("Overwrite segment - middle"):
    val w = Array[Byte](1, 2, 3, 4, 5)
    val x = Array[Byte](9)
    val r = Array[Byte](1, 9, 3, 4, 5)
    for
      file <- boot(Some(5))
      _ <- file.write(0, w)
      _ <- file.write(1, x)
      _ <- waitForTransactionsToComplete()
      a <- file.read(0, 100)
    yield
      file.inode.size should be(w.length)
      a.get.getByteArray should be(r)

  test("Overwrite segment - end"):
    val w = Array[Byte](1, 2, 3, 4, 5)
    val x = Array[Byte](9)
    val r = Array[Byte](1, 2, 3, 4, 9)
    for
      file <- boot(Some(5))
      _ <- file.write(0, w)
      _ <- file.write(4, x)
      _ <- waitForTransactionsToComplete()
      a <- file.read(0, 100)
    yield
      file.inode.size should be(w.length)
      a.get.getByteArray should be(r)


