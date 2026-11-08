package org.aspen_ddp.aspen.server.crl.simple

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

  case class Shutdown(completionHandler: () => Unit) extends Command


class StreamWriter(val maxSizeInBytes: Long,
                   files: List[(StreamId, os.Path)],
                   onThreadDeath: Throwable => Unit = _ => ()) extends Logging:
  
  private val queue = new LinkedBlockingQueue[WriterThread.Command]()
  private val streams = files.map(t =>
    // FileChannel rather than os-lib: os-lib cannot open with DSYNC
    val channel = FileChannel.open(t._2.toNIO,
      StandardOpenOption.CREATE,
      StandardOpenOption.DSYNC,
      StandardOpenOption.WRITE)

    if channel.size() < maxSizeInBytes then
      channel.position(maxSizeInBytes-1)
      channel.write(ByteBuffer.allocate(1))
      channel.position(0)

    t._1 -> channel
  ).toMap
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
          streams.get(w.streamId).foreach: channel =>
            channel.position(w.offset)
            channel.write(w.buffers)
            w.onWriteComplete()
            
        case WriterThread.Shutdown(completionHandler) =>
          streams.valuesIterator.foreach(_.close)
          exitLoop = true
          completionHandler()
        
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
        case _: WriterThread.Write => ()
      next = queue.poll()
  
  def write(streamId: StreamId, 
            offset: Long, 
            buffers: Array[ByteBuffer],
            completionHandler: () => Unit): Unit =
    queue.add(WriterThread.Write(streamId, offset, buffers, completionHandler))
