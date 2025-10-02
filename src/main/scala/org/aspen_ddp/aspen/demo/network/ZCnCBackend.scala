package org.aspen_ddp.aspen.demo.network

import org.aspen_ddp.aspen.client.{AspenClient, Host, HostId, StoragePool}
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.someOrThrow
import org.aspen_ddp.aspen.demo.StoreConfig
import org.aspen_ddp.aspen.server.StoreManager
import org.aspen_ddp.aspen.server.cnc.*
import org.aspen_ddp.aspen.server.store.backend.{RocksDBBackend, RocksDBConfig}
import org.apache.logging.log4j.scala.Logging
import org.zeromq.SocketType

import java.nio.file.{Files, Path}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

class ZCnCBackend(val network: ZMQNetwork,
                  val client: AspenClient,
                  val storesDir: Path,
                  val storeManagers: List[StoreManager],
                  val cncPort: Int) extends Logging:

  private val completionQueue = new LinkedBlockingQueue[String]()

  private val repSocket = network.context.createSocket(SocketType.REP)
  repSocket.bind(s"tcp://*:$cncPort")

  private val serviceThread = new Thread {
    override def run(): Unit = {
      service()
    }
  }
  serviceThread.start()

  private def service(): Unit =
    while true do

      val encodedMessage = repSocket.recv()

      val bb = ByteBuffer.wrap(encodedMessage)
      bb.order(ByteOrder.BIG_ENDIAN)

      val r = try Some(codec.CnCRequest.parseFrom(bb)) catch
        case t: Throwable =>
          logger.error(s"******* PARSE CNC MESSAGE ERROR: $t", t)
          None

      r match
        case None =>
        case Some(cmsg) =>

          val replyBuilder = codec.CnCReply.newBuilder()

          if cmsg.hasNewStore then
            val message = Codec.decode(cmsg.getNewStore)
            logger.trace(s"Got CnC message $message")
            onNewStore(message)

          if cmsg.hasShutdownStore then
            val message = Codec.decode(cmsg.getShutdownStore)
            logger.trace(s"Got CnC message $message")
            onShutdownStore(message)

          if cmsg.hasTransferStore then
            val message = Codec.decode(cmsg.getTransferStore)
            logger.trace(s"Got CnC message $message")
            onTransferStore(message)

          // Wait for operation completion
          val m = completionQueue.take()

          val okBuilder = codec.CnCOk.newBuilder()

          replyBuilder.setOk(okBuilder.build)

          val rmsg = replyBuilder.build.toByteArray

          repSocket.send(rmsg)


  def onNewStore(msg: NewStore): Unit =
    val backend = msg.backendType match
      case b: RocksDBConfig =>
        val dir = storesDir.resolve(s"${msg.storeId.poolId.uuid}:${msg.storeId.poolIndex}")
        Files.createDirectories(dir)
        Files.writeString(dir.resolve("store_config.yaml"),
          s"""
             |pool-uuid: "${msg.storeId.poolId.uuid}"
             |store-index: ${msg.storeId.poolIndex}
             |backend:
             |  storage-engine: rocksdb
             |""".stripMargin)
        logger.info(s"Creating NEW data store ${msg.storeId.poolId.uuid}:${msg.storeId.poolIndex}. Path $dir")
        new RocksDBBackend(dir, msg.storeId,
          scala.concurrent.ExecutionContext.Implicits.global)

    storeManagers.head.loadStore(backend).foreach(_ => completionQueue.put(""))

  def onShutdownStore(msg: ShutdownStore): Unit =
    storeManagers.find(_.containsStore(msg.storeId)) match
      case None => completionQueue.put("")
      case Some(mgr) => mgr.closeStore(msg.storeId).foreach(_ => completionQueue.put(""))

  def onTransferStore(msg: TransferStore): Future[Unit] =
    completionQueue.put("")

    storeManagers.find(_.containsStore(msg.storeId)) match
      case None =>
        Future.failed(new Exception(f"Store ${msg.storeId} not found"))
      case Some(storeManager) =>
        for
          host <- someOrThrow(client.getHost(msg.toHost), new Exception(f"Host name not found: ${msg.toHost}"))
        yield
          val stf = new ZStoreTransferFrontend(storeManager.storesDir, network)
          stf.send(msg.storeId, host.address, host.storeTransferPort)





