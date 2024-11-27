package org.aspen_ddp.aspen.demo.network

import org.aspen_ddp.aspen.client.Host
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.server.cnc.{CnCFrontend, CnCRequest, NewStore, ShutdownStore, TransferStore}
import org.apache.logging.log4j.scala.Logging
import org.zeromq.SocketType

import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.{Future, Promise}

object ZCnCFrontend:
  sealed abstract class QMsg

  final case class CnCNewStore(msg: NewStore, promise: Promise[Unit]) extends QMsg
  final case class CnCShutdownStore(msg: ShutdownStore, promise: Promise[Unit]) extends QMsg
  final case class CnCTransferStore(msg: TransferStore, promise: Promise[Unit]) extends QMsg

  final case class Shutdown(promise: Promise[Unit]) extends QMsg

class ZCnCFrontend(val network: ZMQNetwork, 
                   val host: Host) extends CnCFrontend with Logging:

  import ZCnCFrontend._

  private val msgQueue = new LinkedBlockingQueue[QMsg]()

  private val reqSocket = network.context.createSocket(SocketType.REQ)
  reqSocket.connect(s"tcp://${host.address}:${host.cncPort}")

  private val networkThread = new Thread {
    override def run(): Unit = {
      ioThread()
    }
  }
  networkThread.start()

  private def ioThread(): Unit =
    while true do
      val m = msgQueue.take()
      val builder = codec.CnCRequest.newBuilder()

      m match
        case Shutdown(p) =>
          p.success(())
          return
        case CnCNewStore(msg, p) => builder.setNewStore(Codec.encode(msg))
        case CnCShutdownStore(msg, p) => builder.setShutdownStore(Codec.encode(msg))
        case CnCTransferStore(msg, p) => builder.setTransferStore(Codec.encode(msg))

      val encodedMessage = builder.build.toByteArray

      reqSocket.send(encodedMessage)

      val rmsg = reqSocket.recv()

      if rmsg != null then
        val bb = ByteBuffer.wrap(rmsg)
        bb.order(ByteOrder.BIG_ENDIAN)
        val rm = try codec.CnCReply.parseFrom(bb) catch
          case t: Throwable =>
            logger.error(s"******* PARSE CnCReply ERROR: $t", t)
            m match
              case Shutdown(p) =>
              case CnCNewStore(msg, p) => p.failure(t)
              case CnCShutdownStore(msg, p) => p.failure(t)
              case CnCTransferStore(msg, p) => p.failure(t)
            throw t

        m match
          case Shutdown(p) =>
          case CnCNewStore(msg, p) =>
            if rm.hasOk then
              p.success(())
            else
              p.failure(new Exception("Invalid CnCReply received"))
          case CnCShutdownStore(msg, p) =>
            if rm.hasOk then
              p.success(())
            else
              p.failure(new Exception("Invalid CnCReply received"))
          case CnCTransferStore(msg, p) =>
            // msg.storeId, msg.toHost,
            if rm.hasOk then
              p.success(())
            else
              p.failure(new Exception("Invalid CnCReply received"))


  
  def send(msg: NewStore): Future[Unit] =
    val p = Promise[Unit]
    msgQueue.put(CnCNewStore(msg, p))
    p.future

  def send(msg: ShutdownStore): Future[Unit] =
    val p = Promise[Unit]
    msgQueue.put(CnCShutdownStore(msg, p))
    p.future

  def send(msg: TransferStore): Future[Unit] =
    val p = Promise[Unit]
    msgQueue.put(CnCTransferStore(msg, p))
    p.future
