package org.aspen_ddp.aspen.demo.network

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.deleteDirectory
import org.apache.logging.log4j.scala.Logging
import org.zeromq.{SocketType, ZMQ}

import java.nio.file.Path

class ZStoreTransferFrontend(val storesDir: Path, val net: ZMQNetwork) extends Logging:

  def send(storeId: StoreId, toHost: String, toPort: Int): Unit =
    val sendThread = new Thread {
      override def run(): Unit = sendInThread(storeId, toHost, toPort)
    }
    sendThread.start()

  private def sendInThread(storeId: StoreId, toHost: String, toPort: Int): Unit =
    logger.info(s"Sending store ${storeId.directoryName} to tcp://$toHost:$toPort")

    val sock = net.context.createSocket(SocketType.PUSH)

    sock.connect(s"tcp://$toHost:$toPort")

    val storeIdBytes = storeId.toBytes

    val pb = new ProcessBuilder()
    pb.command("tar", "-czf", "-", storeId.directoryName)
    pb.directory(storesDir.toFile)

    val tarProcess = pb.start
    val stdin = tarProcess.getInputStream

    val buff = new Array[Byte](1024*1024)

    def readFully(): (Int, Boolean) =
      var pos = 0
      var nread = 0
      while nread != -1 && pos < buff.length do
        nread = stdin.read(buff, pos, buff.length-pos)
        if nread != -1 then
          pos += nread
      (pos, nread == -1)

    var eof = false

    while !eof do
      val (nread, eofFound) = readFully()
      eof = eofFound

      val arr = if nread == buff.length then
        buff
      else
        java.util.Arrays.copyOfRange(buff, 0, nread)

      sock.send(storeIdBytes, ZMQ.SNDMORE)

      if nread == 0 then
        sock.send(new Array[Byte](0))
      else
        sock.send(arr)

      if eof && nread != 0 then
        sock.send(storeIdBytes, ZMQ.SNDMORE)
        sock.send(new Array[Byte](0))

    sock.close()

    deleteDirectory(storesDir.resolve(storeId.directoryName).toFile)

    logger.info(s"Completed sending store ${storeId.directoryName} to tcp://$toHost:$toPort")



