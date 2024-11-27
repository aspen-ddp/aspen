package org.aspen_ddp.aspen.server.crl.simple

import org.apache.logging.log4j.scala.Logging

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.LinkedBlockingQueue

object WriterThread:

  sealed abstract class Command

  case class Write(streamId: StreamId,
                   offset: Long,
                   buffers: Array[ByteBuffer],
                   onWriteComplete: () => Unit) extends Command

  case class Shutdown(completionHandler: () => Unit) extends Command


class StreamWriter(val maxSizeInBytes: Long,
                   files: List[(StreamId, Path)]) extends Logging:
  
  private val queue = new LinkedBlockingQueue[WriterThread.Command]()
  private val streams = files.map(t =>
    val channel = FileChannel.open(t._2,
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
