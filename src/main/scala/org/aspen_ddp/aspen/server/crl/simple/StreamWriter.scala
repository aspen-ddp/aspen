package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.server.store.backend.BackendTiming
import scribe.Logging

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.concurrent.LinkedBlockingQueue

object WriterThread:

  sealed abstract class Command

  case class Write(streamId: StreamId,
                   offset: Long,
                   buffers: Array[ByteBuffer],
                   onWriteComplete: () => Unit) extends Command:
    // Buffers compare by their remaining content; the callback can only compare by reference
    override def equals(other: Any): Boolean = other match
      case that: Write => streamId == that.streamId && offset == that.offset &&
        java.util.Arrays.equals(buffers.asInstanceOf[Array[AnyRef]], that.buffers.asInstanceOf[Array[AnyRef]]) &&
        onWriteComplete == that.onWriteComplete
      case _ => false

    override def hashCode: Int =
      (streamId, offset, java.util.Arrays.hashCode(buffers.asInstanceOf[Array[AnyRef]]), onWriteComplete).##

  /** A stream a growth committed (SimpleCRL), opened by the caller: writes queued after it may go to it */
  case class AddStream(streamId: StreamId, channel: FileChannel) extends Command

  case class Shutdown(completionHandler: () => Unit) extends Command


class StreamWriter(val maxSizeInBytes: Long,
                   files: List[(StreamId, os.Path)],
                   onThreadDeath: Throwable => Unit = _ => ()) extends Logging:
  
  private val queue = new LinkedBlockingQueue[WriterThread.Command]()

  // The files exist, preallocated at their full size (SimpleCRL.createStreams, a growth's preallocation): a write
  // never allocates, so it cannot fail with ENOSPC (crash-recovery-log 5.1)
  private def open(path: os.Path): FileChannel =
    // FileChannel rather than os-lib: os-lib cannot open with DSYNC
    val channel = FileChannel.open(path.toNIO,
      StandardOpenOption.DSYNC,
      StandardOpenOption.WRITE)
    if channel.size() != maxSizeInBytes then
      val size = channel.size()
      channel.close()
      throw new IllegalStateException(s"CRL stream $path is $size bytes, not $maxSizeInBytes")
    channel

  // Built here, then touched by the writer thread only (AddStream)
  private val streams = scala.collection.mutable.HashMap.from(files.map(t => t._1 -> open(t._2)))
  private var exitLoop = false
  // Set once the writer thread has died. Nothing takes from the queue after that, so a Shutdown queued then (or
  // still queued) is completed by completeShutdowns instead: the CRL's own shutdown waits on it.
  @volatile private var dead = false
  private val writerThread = new Thread {
    override def run(): Unit =
      try writeThreadLoop()
      catch case t: Throwable =>
        logger.error("CRL writer thread died", t)
        dead = true
        streams.valuesIterator.foreach: channel =>
          try channel.close()
          catch case _: Throwable => ()
        completeShutdowns()
        onThreadDeath(t)
  }
  writerThread.start()

  private def writeThreadLoop(): Unit =
    while !exitLoop do
      queue.take() match
        case w: WriterThread.Write =>
          // A write to a stream the writer does not know would complete unwritten: the writer dies of it instead
          val channel = streams.getOrElse(w.streamId,
            throw IllegalStateException(s"CRL write to unknown stream ${w.streamId.number}"))
          channel.position(w.offset)
          val start = System.nanoTime()
          val bytes = w.buffers.foldLeft(0L)(_ + _.remaining)
          writeFully(channel, w.buffers)
          BackendTiming.report.record("crl write", System.nanoTime() - start, bytes)
          w.onWriteComplete()

        case WriterThread.AddStream(streamId, channel) =>
          streams += streamId -> channel

        case WriterThread.Shutdown(completionHandler) =>
          streams.valuesIterator.foreach(_.close)
          exitLoop = true
          completionHandler()
        
  /** A gathering write may write less than all of its buffers: Linux takes at most IOV_MAX (1024) per call, and an
   *  entry has a few buffers per transaction. Loops until every buffer is written, so completion means all of it is */
  private def writeFully(channel: FileChannel, buffers: Array[ByteBuffer]): Unit =
    var first = 0
    while first < buffers.length do
      channel.write(buffers, first, buffers.length - first)
      while first < buffers.length && !buffers(first).hasRemaining do
        first += 1

  def shutdown(completionHandler: () => Unit = () => ()): Unit =
    queue.add(WriterThread.Shutdown(completionHandler))
    // Checked after the add: either the dying thread's completeShutdowns finds this Shutdown, or this check sees it dead
    if dead then
      completeShutdowns()

  /** Runs the completion handler of every queued Shutdown, once the writer thread has died. Each is polled off the
   *  queue, so it runs once even when the dying thread and a shutdown call both get here. */
  private def completeShutdowns(): Unit =
    var next = queue.poll()
    while next != null do
      next match
        case WriterThread.Shutdown(completionHandler) => completionHandler()
        case WriterThread.AddStream(_, channel) =>
          try channel.close()
          catch case _: Throwable => ()
        case _: WriterThread.Write => ()
      next = queue.poll()
  
  /** Opens `path`, a preallocated stream of `maxSizeInBytes`, here, so a file that cannot be opened throws to the
   *  caller, and queues it to the writer thread ahead of every later write */
  def addStream(id: StreamId, path: os.Path): Unit =
    queue.add(WriterThread.AddStream(id, open(path)))

  def write(streamId: StreamId, 
            offset: Long, 
            buffers: Array[ByteBuffer],
            completionHandler: () => Unit): Unit =
    queue.add(WriterThread.Write(streamId, offset, buffers, completionHandler))
