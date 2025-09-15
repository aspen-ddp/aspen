package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.amoebafs.FileInode

import scala.concurrent.Future
import scala.language.implicitConversions

class IndexedFileContentSuite extends FilesSystemTestSuite {
  def boot(osegmentSize: Option[Int]=None, otierNodeSize: Option[Int]=None): Future[SimpleFile] = for {
    fs <- bootstrap()

    rootDir <- fs.loadRoot()

    tx = fs.client.newTransaction()

    fdir <- rootDir.prepareCreateFile("foo", mode=0, uid=1, gid=2)(using tx)

    _ <- tx.commit()

    newFilePointer <- fdir

    (newInode, _, revision) <- fs.readInode(newFilePointer)
  } yield {
    new SimpleFile(newFilePointer, revision, newInode.asInstanceOf[FileInode], fs, osegmentSize, otierNodeSize)
  }

  def readFully(file: SimpleFile): Future[Array[Byte]] = {
    for {
      _ <- waitForTransactionsToComplete()
      arr <- file.debugReadFully()
    } yield arr
  }

  test("Read empty File") {
    for {
      file <- boot()
      a <- readFully(file)
    } yield {
      a.length should be (0)
    }
  }

  test("Read write empty file") {

    val w = Array[Byte](5)
    for {
      file <- boot()

      (remainingOffset, remainingData) <- file.write(0, w)

      a <- readFully(file)
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }

  test("Read write empty file with hole") {
    val w = Array[Byte](5)
    val e = Array[Byte](0,0,5)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(2, w)
      a <- readFully(file)
    } yield {
      file.inode.size should be (3)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e)
    }
  }

  test("Read write empty file with full segment hole") {
    val w = Array[Byte](5)
    val e = Array[Byte](0,0,0,0,0,0,5)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(6, w)
      a <- readFully(file)
    } yield {
      file.inode.size should be (7)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e)
    }
  }

  test("Partial read, single segment") {

    val w = Array[Byte](1,2,3,4,5,6,7,8,9)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, w)
      _ <- waitForTransactionsToComplete()
      a <- file.read(2,2)
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a.get.getByteArray should be (Array[Byte](3,4))
    }
  }

  test("Partial read, multi segment") {

    val w = Array[Byte](1,2,3,4,5,6,7,8,9)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, w)
      _<-waitForTransactionsToComplete()
      a <- file.read(2,5)
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a.get.getByteArray should be (Array[Byte](3,4,5,6,7))
    }
  }

  test("Write two segments") {
    val w = Array[Byte](1,2,3,4,5,6,7,8,9)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, w)
      a <- readFully(file)
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }

  test("Write three segments") {
    val w = Array[Byte](1,2,3,4,5,6,7,8,9,10,11)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, w)
      a <- readFully(file)
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }

  test("Write emtpy to multi-tier index") {

    val w = Array[Byte](1,2,3,4,5,6,7,8,9,10,11,13,14,15,16)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, w)
      a <- readFully(file)
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }

  test("Multi-buffer write") {
    val a = Array[Byte](5,6)
    val b = Array[Byte](7,8)
    val w = Array[Byte](5,6,7,8)
    for {
      file <- boot(Some(5), Some(500))
      (remainingOffset, remainingData) <- file.write(0, List(DataBuffer(a),DataBuffer(b)))
      a <- readFully(file)
    } yield {
      file.inode.size should be (w.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w)
    }
  }

  test("Overwrite single segment, no extend") {
    val w1 = Array[Byte](0,1,2,3)
    val w2 = Array[Byte](4,5,6,7)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      a <- readFully(file)
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      file.inode.size should be (w1.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (w2)
    }
  }

  test("Overwrite single segment, with extend") {
    val w1 = Array[Byte](0,1)
    val w2 = Array[Byte](4,5,6,7)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      a <- readFully(file)
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (w2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (w2)
    }
  }

  test("Full write tail overwrite segment with allocation") {
    val w1 = Array[Byte](0,1)
    val w2 = Array[Byte](0,1,2,3,4,5,6)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      a <- readFully(file)
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      file.inode.size should be (w2.length)
      remainingData.isEmpty should be (true)
      a should be (w1)
      b should be (w2)
    }
  }

  test("Partial write tail overwrite segment with allocation") {
    val w1 = Array[Byte](0,9)
    val w2 = Array[Byte](1,2,3,4,5,6)
    val e = Array[Byte](0,1,2,3,4,5,6)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      a <- readFully(file)
      (remainingOffset, remainingData) <- file.write(1, w2)
      b <- readFully(file)
    } yield {
      file.inode.size should be (7)
      remainingData.isEmpty should be (true)
      a should be (w1)
      b should be (e)
    }
  }

  test("Allocate into hole, short segment") {
    val w1 = Array[Byte](5)
    val w2 = Array[Byte](0,1,2,3)
    val e1 = Array[Byte](0,0,0,0,0,5)
    val e2 = Array[Byte](0,1,2,3,0,5)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- readFully(file)
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      file.inode.size should be (6)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }

  test("Allocate into hole, full segment") {
    val w1 = Array[Byte](5)
    val w2 = Array[Byte](0,1,2,3,4)
    val e1 = Array[Byte](0,0,0,0,0,5)
    val e2 = Array[Byte](0,1,2,3,4,5)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- readFully(file)
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      file.inode.size should be (6)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }

  test("Allocate into hole, multi-segment") {

    val w1 = Array[Byte](10)
    val w2 = Array[Byte](0,1,2,3,4,5,6,7,8,9)
    val e1 = Array[Byte](0,0,0,0,0,0,0,0,0,0,10)
    val e2 = Array[Byte](0,1,2,3,4,5,6,7,8,9,10)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(10, w1)
      a <- readFully(file)
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (e2.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, partial overwrite") {

    val w1 = Array[Byte](5,6,7)
    val w2 = Array[Byte](0,1,2,3,4,9,9)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7)
    val e2 = Array[Byte](0,1,2,3,4,9,9,7)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- readFully(file)
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (7)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, full overwrite") {

    val w1 = Array[Byte](5,6,7)
    val w2 = Array[Byte](0,1,2,3,4,9,9,9)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7)
    val e2 = Array[Byte](0,1,2,3,4,9,9,9)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- readFully(file)
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (8)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment partial overwrite") {

    val w1 = Array[Byte](5,6,7,8,9,10,11)
    val w2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7,8,9,10,11)
    val e2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,11)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- readFully(file)
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (11)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment full overwrite") {

    val w1 = Array[Byte](5,6,7,8,9,10,11)
    val w2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7,8,9,10,11)
    val e2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- readFully(file)
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (12)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, multi-segment full overwrite with extension") {

    val w1 = Array[Byte](5,6,7,8,9,10,11)
    val w2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21,22)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7,8,9,10,11)
    val e2 = Array[Byte](0,1,2,3,4,15,16,17,18,19,20,21,22)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- readFully(file)
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (13)
      remainingData should be (Nil)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, full overwrite plus allocation") {

    val w1 = Array[Byte](5,6,7)
    val w2 = Array[Byte](0,1,2,3,4,9,9,9,8,9,10,11,12)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7)
    val e2 = Array[Byte](0,1,2,3,4,9,9,9,8,9)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      a <- readFully(file)
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(0, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (10)
      remainingData.foldLeft(0)((sz, db) => sz + db.size) should be (3)
      a should be (e1)
      b should be (e2)
    }
  }
  test("Allocate into hole, over disjoint segments stops short") {

    val w1 = Array[Byte](5,6,7)
    val w2 = Array[Byte](1,2,3,4,9,9,9,8,9,10,11,12,13,14,15,16)
    val e1 = Array[Byte](0,0,0,0,0,5,6,7,0,0,0,0,0,0,0,5,6,7)
    val e2 = Array[Byte](0,1,2,3,4,9,9,9,8,9,0,0,0,0,0,5,6,7)
    val r = Array[Byte](10,11,12,13,14,15,16)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(5, w1)
      (_, _) <- file.write(15, w1)
      a <- readFully(file)
      inode2 = file.inode
      (remainingOffset, remainingData) <- file.write(1, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (e1.length)
      file.inode.size should be (e2.length)
      remainingOffset should be (10)
      remainingData.foldLeft(0)((sz, db) => sz + db.size) should be (7)
      a should be (e1)
      b should be (e2)
      DataBuffer.compact(100, remainingData)._1.getByteArray should be (r)
    }
  }
  test("Append to file, partial segment") {

    val w1 = Array[Byte](0,1,2)
    val w2 = Array[Byte](3,4)
    val e  = Array[Byte](0,1,2,3,4)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      _<-waitForTransactionsToComplete()
      inode2 = file.inode
      a <- readFully(file)
      (remainingOffset, remainingData) <- file.write(w1.length, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, single segment allocation") {

    val w1 = Array[Byte](0,1,2)
    val w2 = Array[Byte](3,4,5,6,7)
    val e  = Array[Byte](0,1,2,3,4,5,6,7)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      _<-waitForTransactionsToComplete()
      inode2 = file.inode
      a <- readFully(file)
      (remainingOffset, remainingData) <- file.write(w1.length, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, multi segment allocation") {

    val w1 = Array[Byte](0,1,2)
    val w2 = Array[Byte](3,4,5,6,7,8,9,10,11,12)
    val e  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      _<-waitForTransactionsToComplete()
      inode2 = file.inode
      a <- readFully(file)
      (remainingOffset, remainingData) <- file.write(w1.length, w2)
      b <- readFully(file)
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, multi segment, multi-buffer allocation") {

    val w1 = Array[Byte](0,1,2)
    val w2 = List(Array[Byte](3,4,5,6), Array[Byte](7,8,9,10,11), Array[Byte](12))
    val e  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      _<-waitForTransactionsToComplete()
      inode2 = file.inode
      a <- readFully(file)
      (remainingOffset, remainingData) <- file.write(w1.length, w2.map(DataBuffer(_)))
      b <- readFully(file)
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e)
    }
  }
  test("Append to file, three appends") {

    val w1 = Array[Byte](0,1,2)
    val w2 = List(Array[Byte](3,4,5,6), Array[Byte](7,8,9,10,11))
    val w3 = Array[Byte](12)
    val e1  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11)
    val e2  = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      _<-waitForTransactionsToComplete()
      inode2 = file.inode
      a <- readFully(file)
      (_, _) <- file.write(w1.length, w2.map(DataBuffer(_)))
      _<-waitForTransactionsToComplete()
      inode3 = file.inode
      b <- readFully(file)
      (remainingOffset, remainingData) <- file.write(12, w3)
      _<-waitForTransactionsToComplete()
      inode4 = file.inode
      c <- readFully(file)
    } yield {
      inode2.size should be (w1.length)
      inode3.size should be (e1.length)
      inode4.size should be (e2.length)
      remainingOffset should be (0)
      remainingData should be (Nil)
      a should be (w1)
      b should be (e1)
      c should be (e2)
    }
  }

  test("Truncate, single segment") {

    val w1 = Array[Byte](0,1,2,3)
    val e  = Array[Byte](0,1,2)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      _<-waitForTransactionsToComplete()
      inode2 = file.inode
      a <- readFully(file)
      fdeleteComplete <- file.truncate(3)
      _ <- fdeleteComplete
      b <- readFully(file)
      //_ <- file.fs.getLocalTasksCompleted
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }

  test("Truncate, delete segment") {

    val w1 = Array[Byte](0,1,2,3,4,5,6,7)
    val e  = Array[Byte](0,1,2)
    for {
      file <- boot(Some(5), Some(500))
      (_, _) <- file.write(0, w1)
      _<-waitForTransactionsToComplete()
      inode2 = file.inode
      a <- readFully(file)
      fdeleteComplete <- file.truncate(3)
      _ <- fdeleteComplete
      b <- readFully(file)
      //_ <- file.fs.getLocalTasksCompleted
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }

  test("Truncate, multi tier") {

    val w1 = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
    val e  = Array[Byte](0,1,2)
    for {
      file <- boot(Some(5), Some(120))
      (_, _) <- file.write(0, w1)
      _<-waitForTransactionsToComplete()
      inode2 = file.inode
      a <- readFully(file)
      fdeleteComplete <- file.truncate(3)
      _ <- fdeleteComplete
      b <- readFully(file)
      //_ <- file.fs.getLocalTasksCompleted
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }

  test("Truncate, multi tier to zero") {

    val w1 = Array[Byte](0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
    val e  = Array[Byte]()
    for {
      file <- boot(Some(5), Some(120))
      (_, _) <- file.write(0, w1)
      _<-waitForTransactionsToComplete()
      inode2 = file.inode
      a <- readFully(file)
      fdeleteComplete <- file.truncate(0)
      _ <- fdeleteComplete
      b <- readFully(file)
      //_ <- file.fs.getLocalTasksCompleted
    } yield {
      inode2.size should be (w1.length)
      file.inode.size should be (e.length)
      a should be (w1)
      b should be (e)
    }
  }

}
