package org.aspen_ddp.aspen.amoebafs.impl.simple

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.amoebafs.FileInode

import scala.concurrent.Future
import scala.language.implicitConversions

class SimpleFileHandleSuite extends FilesSystemTestSuite:

  def bootFileHandle(writeBufferSize: Int = 1024, osegmentSize: Option[Int] = None): Future[SimpleFileHandle] = for
    fs <- bootstrap()

    rootDir <- fs.loadRoot()

    tx = fs.client.newTransaction()

    fdir <- rootDir.prepareCreateFile("testfile", mode=0, uid=1, gid=2)(using tx)

    _ <- tx.commit()

    newFilePointer <- fdir

    (newInode, _, revision) <- fs.readInode(newFilePointer)

    file = new SimpleFile(newFilePointer, revision, newInode.asInstanceOf[FileInode], fs, osegmentSize)
  yield
    new SimpleFileHandle(file, writeBufferSize)

  def readFileContent(handle: SimpleFileHandle, offset: Long = 0, nbytes: Int = 1024): Future[Array[Byte]] =
    for
      //_ <- waitForTransactionsToComplete()
      odata <- handle.read(offset, nbytes)
    yield
      odata.map(_.getByteArray).getOrElse(Array.empty[Byte])

  test("Write consecutive data blocks"):
    val data1 = Array[Byte](1, 2, 3, 4, 5)
    val data2 = Array[Byte](6, 7, 8, 9, 10)
    val expected = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    for
      handle <- bootFileHandle(writeBufferSize = 0) // Direct I/O for immediate completion
      _ <- handle.write(0, List(DataBuffer(data1)))
      _ <- handle.write(5, List(DataBuffer(data2)))
      _ <- handle.flush()
      content <- readFileContent(handle, 0, 10)
    yield
      content should be (expected)

  test("Write consecutive data blocks with buffering"):
    val data1 = Array[Byte](1, 2, 3)
    val data2 = Array[Byte](4, 5, 6)
    val data3 = Array[Byte](7, 8, 9)
    val expected = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9)

    for
      handle <- bootFileHandle(writeBufferSize = 100) // Allow buffering
      f1 <- handle.write(0, List(DataBuffer(data1)))
      f2 <- handle.write(3, List(DataBuffer(data2)))
      f3 <- handle.write(6, List(DataBuffer(data3)))
      _ <- handle.flush()
      content <- readFileContent(handle, 0, 9)
    yield
      content should be (expected)

  test("Write out-of-order data blocks direct-io"):
    val data1 = Array[Byte](1, 2, 3)    // offset 0
    val data2 = Array[Byte](7, 8, 9)    // offset 6
    val data3 = Array[Byte](4, 5, 6)    // offset 3 (fills gap)
    val expected = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9)

    for
      handle <- bootFileHandle(writeBufferSize = 0) // Direct I/O
      _ <- handle.write(0, List(DataBuffer(data1)))
      _ <- handle.write(6, List(DataBuffer(data2)))
      _ <- handle.write(3, List(DataBuffer(data3)))
      _ <- handle.flush()
      content <- readFileContent(handle, 0, 9)
    yield
      content should be (expected)

  test("Write out-of-order data blocks with buffering"):
    val data1 = Array[Byte](1, 2, 3) // offset 0
    val data2 = Array[Byte](7, 8, 9) // offset 6
    val data3 = Array[Byte](4, 5, 6) // offset 3 (fills gap)
    val expected = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9)

    for
      handle <- bootFileHandle(writeBufferSize = 100) // Direct I/O
      _ <- handle.write(0, List(DataBuffer(data1)))
      _ <- handle.write(6, List(DataBuffer(data2)))
      _ <- handle.write(3, List(DataBuffer(data3)))
      _ <- handle.flush()
      content <- readFileContent(handle, 0, 9)
    yield
      content should be(expected)

  test("Write out-of-order with gaps"):
    val data1 = Array[Byte](1, 2)       // offset 0
    val data2 = Array[Byte](7, 8)       // offset 6 (creates gap 2-5)
    val expected = Array[Byte](1, 2, 0, 0, 0, 0, 7, 8) // zeros fill gaps

    for
      handle <- bootFileHandle(writeBufferSize = 0)
      _ <- handle.write(0, List(DataBuffer(data1)))
      _ <- handle.write(6, List(DataBuffer(data2)))
      _ <- handle.flush()
      content <- readFileContent(handle, 0, 8)
    yield
      content should be (expected)

  test("Buffered writes return same promise for consecutive operations"):
    val data1 = Array[Byte](1, 2, 3)
    val data2 = Array[Byte](4, 5, 6)

    for
      handle <- bootFileHandle(writeBufferSize = 100) // Enable buffering
      f1 = handle.write(0, List(DataBuffer(data1)))
      f2 = handle.write(3, List(DataBuffer(data2)))  // Should merge with previous write
      _ <- f1
      _ <- f2
      _ <- handle.flush()
      content <- readFileContent(handle, 0, 6)
    yield {
      f1.eq(f2) should be (true)
      // Both futures should complete successfully
      content should be (Array[Byte](1, 2, 3, 4, 5, 6))
    }

  test("Large write exceeding buffer size returns completion future"):
    val largeData = Array.fill[Byte](200)(42) // Larger than buffer

    for
      handle <- bootFileHandle(writeBufferSize = 100)
      f1 = handle.write(0, List(DataBuffer(largeData)))
      _ <- f1 // Should wait for actual completion
      _ <- handle.flush()
      content <- readFileContent(handle, 0, 200)
    yield
      content should be (largeData)

  test("Write with prepend merging (out-of-order that prepends to existing write)"):
    val data1 = Array[Byte](4, 5, 6)    // offset 3
    val data2 = Array[Byte](1, 2, 3)    // offset 0 (should prepend to first write)
    val expected = Array[Byte](1, 2, 3, 4, 5, 6)

    for
      handle <- bootFileHandle(writeBufferSize = 100) // Enable buffering for merging
      f1 <- handle.write(3, List(DataBuffer(data1)))
      f2 <- handle.write(0, List(DataBuffer(data2)))  // Should merge as prepend
      _ <- handle.flush()
      content <- readFileContent(handle, 0, 6)
    yield
      content should be (expected)

  test("Write with append merging (consecutive writes that append)"):
    val data1 = Array[Byte](1, 2, 3)    // offset 0
    val data2 = Array[Byte](4, 5, 6)    // offset 3 (should append to first write)
    val expected = Array[Byte](1, 2, 3, 4, 5, 6)

    for
      handle <- bootFileHandle(writeBufferSize = 100) // Enable buffering for merging
      f1 <- handle.write(0, List(DataBuffer(data1)))
      f2 <- handle.write(3, List(DataBuffer(data2)))  // Should merge as append
      _ <- handle.flush()
      content <- readFileContent(handle, 0, 6)
    yield
      content should be (expected)

  test("Multiple data buffers in single write"):
    val buf1 = DataBuffer(Array[Byte](1, 2))
    val buf2 = DataBuffer(Array[Byte](3, 4))
    val buf3 = DataBuffer(Array[Byte](5, 6))
    val expected = Array[Byte](1, 2, 3, 4, 5, 6)

    for
      handle <- bootFileHandle(writeBufferSize = 0)
      _ <- handle.write(0, List(buf1, buf2, buf3))
      _ <- handle.flush()
      content <- readFileContent(handle, 0, 6)
    yield
      content should be (expected)

  test("Complex interleaved writes with different buffer configurations"):
    val writeBufferSize = 50

    for
      handle <- bootFileHandle(writeBufferSize = writeBufferSize)

      // Write some data at offset 10
      f1 <- handle.write(10, List(DataBuffer(Array[Byte](10, 11, 12))))

      // Write data at offset 0 (separate write)
      f2 <- handle.write(0, List(DataBuffer(Array[Byte](0, 1, 2))))

      // Write data that connects offset 3 to offset 10 (should create new write)
      f3 <- handle.write(3, List(DataBuffer(Array[Byte](3, 4, 5, 6, 7, 8, 9))))

      // Write data that extends the first write
      f4 <- handle.write(13, List(DataBuffer(Array[Byte](13, 14, 15))))

      _ <- handle.flush()
      content <- readFileContent(handle, 0, 16)
    yield
      val expected = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
      content should be (expected)

  test("Flush waits for all pending writes to complete"):
    val data1 = Array[Byte](1, 2, 3)
    val data2 = Array[Byte](4, 5, 6)

    for
      handle <- bootFileHandle(writeBufferSize = 100)
      f1 = handle.write(0, List(DataBuffer(data1)))
      f2 = handle.write(10, List(DataBuffer(data2))) // Non-consecutive, separate write
      flushFuture = handle.flush()
      _ <- flushFuture
      content1 <- readFileContent(handle, 0, 3)
      content2 <- readFileContent(handle, 10, 3)
    yield
      content1 should be (data1)
      content2 should be (data2)

  test("Read caching works correctly after writes"):
    val data = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    for
      handle <- bootFileHandle(writeBufferSize = 0)
      _ <- handle.write(0, List(DataBuffer(data)))
      _ <- handle.flush()

      // First read should cache the data
      content1 <- readFileContent(handle, 0, 5)

      // Second read should use cache
      content2 <- readFileContent(handle, 2, 3)

      // Read beyond cached range should trigger new read
      content3 <- readFileContent(handle, 8, 2)
    yield
      content1 should be (Array[Byte](1, 2, 3, 4, 5))
      content2 should be (Array[Byte](3, 4, 5))
      content3 should be (Array[Byte](9, 10))

  test("Write invalidates read cache"):
    val data1 = Array[Byte](1, 2, 3, 4, 5)
    val data2 = Array[Byte](10, 20, 30)

    for
      handle <- bootFileHandle(writeBufferSize = 0)
      _ <- handle.write(0, List(DataBuffer(data1)))
      _ <- handle.flush()

      // Read to populate cache
      _ <- readFileContent(handle, 0, 5)

      // Write should invalidate cache
      _ <- handle.write(1, List(DataBuffer(data2)))
      _ <- handle.flush()

      // Read should get updated content
      content <- readFileContent(handle, 0, 5)
    yield
      val expected = Array[Byte](1, 10, 20, 30, 5)
      content should be (expected)