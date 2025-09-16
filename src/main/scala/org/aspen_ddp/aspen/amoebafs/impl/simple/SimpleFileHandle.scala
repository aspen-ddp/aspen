package org.aspen_ddp.aspen.amoebafs.impl.simple

import scala.language.implicitConversions
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.amoebafs.{File, FileHandle}
import org.apache.logging.log4j.scala.Logging

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

object SimpleFileHandle {

  private class PendingWrite(val writeNumber: Int,
                             var offset: Long,
                             var nbytes: Int,
                             var reversedBuffers: List[DataBuffer]) {

    val completePromise: Promise[Unit] = Promise()

    def complete: Future[Unit] = completePromise.future

    def endOffset: Long = offset + nbytes
  }

}

/** If the currently queued number of bytes is less than writeBufferSize, the write method will
  *  enqueue the write operation and the returned future will complete immediately. Otherwise the
  *  future will complete once the the full write operation has been successfully committed. Files
  *  opened in 'direct I/O' should set the buffer size to zero to ensure all writes complete before
  *  returning.
  */
class SimpleFileHandle(
                        val file: File,
                        val writeBufferSize: Int) extends FileHandle with Logging {

  import SimpleFileHandle._

  given ExecutionContext = file.fs.executionContext

  private var writeQueue: Queue[PendingWrite] = Queue()
  private var ocurrentWrite: Option[PendingWrite] = None
  private var nbuffered: Int = 0
  private var writeCount: Int = 0
  private var readCache: Option[(Long, DataBuffer)] = None

  // Always try to read a megabyte at a time and serve reads from the cached content if possible
  def read(offset: Long, nbytes: Int): Future[Option[DataBuffer]] = synchronized {
    val odata = readCache match {
      case None => None
      case Some((doffset, db)) =>
        if (offset >= doffset && offset + nbytes <= doffset + db.size)
          Some(db.slice((offset-doffset).asInstanceOf[Int], nbytes))
        else
          None
    }

    odata match {
      case Some(db) => Future.successful(Some(db))
      case None =>
        val rsize = if (nbytes > 1024*1024) nbytes else 1024*1024
        file.read(offset, rsize).map {
          case None => None

          case Some(data) =>
            synchronized {
              readCache = Some((offset, data))
              Some(data.slice(0, nbytes))
            }
        }
    }
  }

  def truncate(offset: Long): Future[Future[Unit]] = synchronized {
    readCache = None
    file.truncate(offset)
  }

  def flush(): Future[Unit] = synchronized {
    ocurrentWrite match {
      case None => Future.unit
      case Some(pw) => if (writeQueue.isEmpty) pw.complete else writeQueue.last.complete
    }
  }

  def write(offset: Long, buffers: List[DataBuffer]): Future[Unit] = synchronized {

    val copies = buffers.map(_.copy())

    val wsize = copies.foldLeft(0)((sz, db) => sz + db.size)
    val rbuffers = copies.reverse

    nbuffered += wsize

    val pw = writeQueue.find(pw => pw.offset == offset + wsize || pw.endOffset == offset) match {
      case None =>
        writeCount += 1
        logger.info(s"Queuing new write number $writeCount at offset $offset, end offset ${offset + wsize}.")
        val p = new PendingWrite(writeCount, offset, wsize, rbuffers)
        writeQueue = writeQueue.enqueue(p)
        p

      case Some(p) =>
        val ooff = p.offset
        val oend = p.offset + p.nbytes
        p.nbytes += wsize
        if (p.offset == offset + wsize) {
          p.reversedBuffers = p.reversedBuffers ++ rbuffers
          p.offset -= wsize
          logger.info(s"Buffering write of $wsize bytes as Prepend to write number ${p.writeNumber} (off:$ooff, end:$oend). New (off:${p.offset}, end:${p.offset + p.nbytes})")
        } else {
          p.reversedBuffers = rbuffers ++ p.reversedBuffers
          logger.info(s"Buffering write of $wsize bytes as Append  to write number ${p.writeNumber} (off:$ooff, end:$oend). New (off:${p.offset}, end:${p.offset + p.nbytes})")
        }
        p
    }

    beginNextWrite()

    if (nbuffered < writeBufferSize) {
      logger.info(s"Returning immediately. $nbuffered < $writeBufferSize")
      Future.unit
    } else {
      logger.info(s"Returning future to write completion ! $nbuffered < $writeBufferSize")
      pw.complete
    }
  }

  private def beginNextWrite(): Unit = synchronized {
    if (ocurrentWrite.isEmpty) {

      writeQueue.dequeueOption.foreach { t =>
        val pw = t._1
        writeQueue = t._2

        ocurrentWrite = Some(pw)

        def writeSome(offset: Long, buffers: List[DataBuffer]): Unit = {
          logger.info(s"  write ${pw.writeNumber}: writeSome(offset=$offset, bufferCount=${buffers.length}, nbytes=${buffers.foldLeft(0)((sz,b) => sz+b.remaining())})")
          val sb = new StringBuilder
          buffers.foldLeft(offset) { (off, db) =>
            sb.append(s"    offset $off crc: ${db.hashString}\n")
            off + db.size
          }
          logger.info(sb.toString)

          file.write(offset, buffers).foreach { t =>
            val (remainingOffset, remainingBuffers) = t

            if (remainingBuffers.isEmpty) {
              synchronized {
                readCache = None
                nbuffered -= pw.nbytes
                ocurrentWrite = None
                logger.info(s"Completed write ${pw.writeNumber} at offset ${pw.offset}, endOffset ${pw.endOffset}")
                pw.completePromise.success(())
                beginNextWrite()
              }
            }
            else
              writeSome(remainingOffset, remainingBuffers)
          }
        }

        logger.info(s"Beginning write ${pw.writeNumber} at offset ${pw.offset}, endOffset ${pw.endOffset}, numBuffers: ${pw.reversedBuffers.size}")
        writeSome(pw.offset, pw.reversedBuffers.reverse)
      }
    }
  }

}
