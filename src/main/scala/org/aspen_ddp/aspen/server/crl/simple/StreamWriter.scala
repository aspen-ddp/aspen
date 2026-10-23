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
                   files: List[(StreamId, os.Path)]) extends Logging:
  
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
  private val writerThread = new Thread {
    override def run(): Unit = writeThreadLoop()
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
        
  def shutdown(completionHandler: () => Unit = () => ()): Unit = queue.add(WriterThread.Shutdown(completionHandler))
  
  def write(streamId: StreamId, 
            offset: Long, 
            buffers: Array[ByteBuffer],
            completionHandler: () => Unit): Unit =
    queue.add(WriterThread.Write(streamId, offset, buffers, completionHandler))
