package org.aspen_ddp.aspen.demo.network

import org.aspen_ddp.aspen.client.{AspenClient, HostId}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreManager
import org.apache.logging.log4j.scala.Logging
import org.zeromq.SocketType

import java.nio.file.Files
import scala.concurrent.ExecutionContext

class ZStoreTransferBackend(val transferPort: Int,
                            val net: ZMQNetwork,
                            val hostId: HostId,
                            val client: AspenClient,
                            val storeManager: StoreManager) extends Logging:

  logger.info("Starting StoreTransferBackend")
  val incomingDir = storeManager.storesDir.resolve("incoming")

  if ! Files.exists(incomingDir) then
    Files.createDirectories(incomingDir)

  private val rthread = new Thread {
    override def run(): Unit = rcvThread()
  }
  rthread.start()

  private def rcvThread(): Unit =
    logger.info(s"Store Transfer Backend listening on tcp://*:$transferPort")
    val incomingSock = net.context.createSocket(SocketType.PULL)

    incomingSock.bind(s"tcp://*:$transferPort")

    var imap: Map[StoreId, Process] = Map()

    while true do
      val sidBytes = incomingSock.recv(0)
      if sidBytes.length == 17 && incomingSock.hasReceiveMore then
        val storeId = StoreId(sidBytes)
        val data = incomingSock.recv(0)

        val ps = imap.get(storeId) match
          case Some(process) => process
          case None =>
            logger.info(s"Beginning transfer of store ${storeId.directoryName}")
            val pb = new ProcessBuilder()
            pb.command("tar", "-xzf", "-")
            pb.directory(incomingDir.toFile)

            val process = pb.start
            imap += (storeId -> process)

            process

        if data.isEmpty then
          ps.getOutputStream.close()
          ps.waitFor() // Block here till process exits
          imap -= storeId
          val storeDir = storeManager.storesDir.resolve(storeId.directoryName)
          Files.move(incomingDir.resolve(storeId.directoryName), storeDir)
          storeManager.loadStoreById(storeId)
          logger.info(s"Completed transfer of store ${storeId.directoryName}")

          given ExecutionContext = client.clientContext

          client.updateStorageHost(storeId, hostId).foreach: _ =>
            logger.info(s"Successfully updated Storage Pool configuration for store: ${storeId.directoryName}")

        else
          ps.getOutputStream.write(data)



