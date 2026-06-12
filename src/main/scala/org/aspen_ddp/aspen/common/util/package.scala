package org.aspen_ddp.aspen.common

import java.io.{File, IOException, PrintWriter, StringWriter}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.*
import java.util.UUID
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

package object util {
  import scala.language.implicitConversions
  import scala.Conversion

  given Conversion[UUID, Array[Byte]] = uuid => {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(0, uuid.getMostSignificantBits)
    bb.putLong(8, uuid.getLeastSignificantBits)
    bb.array()
  }

  def uuid2byte(uuid: UUID): Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(0, uuid.getMostSignificantBits)
    bb.putLong(8, uuid.getLeastSignificantBits)
    bb.array()
  }

  def byte2uuid(arr: Array[Byte]): UUID = {
    val bb = ByteBuffer.wrap(arr)
    val msb = bb.getLong()
    val lsb = bb.getLong()
    new UUID(msb, lsb)
  }

  def uuids2byte(uuids: List[UUID]): Array[Byte] = {
    val bb = ByteBuffer.allocate(16 * uuids.length)
    uuids.foreach: uuid =>
      bb.putLong(uuid.getMostSignificantBits)
      bb.putLong(uuid.getLeastSignificantBits)
    bb.array()
  }

  def byte2uuids(arr: Array[Byte]): List[UUID] = {
    require(arr.length % 16 == 0)
    
    val bb = ByteBuffer.wrap(arr)

    @tailrec
    def recurse(count: Int, uuids: List[UUID]): List[UUID] =
      if count * 16 == arr.length then
        uuids.reverse
      else
        val msb = bb.getLong()
        val lsb = bb.getLong()
        recurse(count + 1, new UUID(msb, lsb) :: uuids)

    recurse(0, Nil)
  }

  def int2byte(i: Int): Array[Byte] = {
    val arr = new Array[Byte](4)
    val bb = ByteBuffer.wrap(arr)
    bb.putInt(i)
    arr
  }
  def byte2int(arr: Array[Byte]): Int = ByteBuffer.wrap(arr).getInt()

  def long2byte(l: Long): Array[Byte] = {
    val arr = new Array[Byte](8)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(l)
    arr
  }
  def byte2long(arr: Array[Byte]): Long = ByteBuffer.wrap(arr).getLong()

  def getStack: String = {
    val e = new Exception("printing stack")
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    e.printStackTrace(pw)
    sw.toString
  }

  def printStack(): Unit = println(getStack)

  def db2string(db: DataBuffer): String = {
    val enc = java.util.Base64.getEncoder
    enc.encodeToString(db.getByteArray)
  }
  def printableArray(arr: Array[Byte]): String = {
    val enc = java.util.Base64.getEncoder
    enc.encodeToString(arr)
  }

  def deleteDirectory(directoryToBeDeleted: File): Boolean = {
    val allContents = directoryToBeDeleted.listFiles
    if allContents != null then
      for (file <- allContents)
        deleteDirectory(file)
    directoryToBeDeleted.delete
  }
  
  def someOrThrow[U, T <: Throwable](o: Future[Option[U]], exceptionToThrow: => T)(using ec: ExecutionContext): Future[U] = o.map:
    case None => throw exceptionToThrow
    case Some(u) => u

  def atomicWrite(targetFile: Path, content: String): Unit =
    // 1. Get the parent directory.
    // CRITICAL: The temp file MUST be on the same partition/volume as the target file,
    // otherwise a native atomic rename is physically impossible.
    val parentDir = Option(targetFile.getParent).getOrElse(Paths.get("."))

    // 2. Create a temporary file in that same directory
    val tempFile = Files.createTempFile(parentDir, "atomic-", ".tmp")

    try
      // 3. Write content to the temporary file
      Files.writeString(tempFile, content, StandardCharsets.UTF_8)

      // 4. Atomically replace the target file with the temp file
      Files.move(
        tempFile,
        targetFile,
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE
      )
    catch
      case ex: IOException =>
        // 5. Clean up the temporary file if anything failed prior to the atomic move
        try
          Files.deleteIfExists(tempFile)
        catch
          case cleanupEx: IOException => ex.addSuppressed(cleanupEx)
        throw ex

  def runSequentially[A, B](items: Seq[A])(f: A => Future[B])(implicit ec: ExecutionContext): Future[Seq[B]] =
    items.foldLeft(Future.successful(Vector.empty[B])): (accFuture, item) =>
      accFuture.flatMap: accResults =>
        // This body only executes after the previous Future finishes
        f(item).map(result => accResults :+ result)

  /** 
   * This method ensures that repeated invocations of the supplied code block (such as a scheduled periodic task)
   * will ignore redundant calls while the Future is outstanding. It's intent is to be used with schedulePeriodic
   * to ensure that duplicate reads don't pile up during extended offline periods. An example is of a host polling
   * the physical device state to look for new store creations or transfers. Without the use of this method, an
   * offline period could result in hundreds of backed-up read operations.
   *  */
  def ignoreExtraCallsWhileRunning[T](fn: => Future[T])(implicit ec: ExecutionContext): () => Unit =
    object tracker:
      var running = false
      def call(): Unit =
        synchronized:
          if !running then
            running = true
            fn.foreach: _ =>
              synchronized:
                running = false
    tracker.call


}
