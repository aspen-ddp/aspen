package org.aspen_ddp.aspen.demo.network

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.server.cnc.{CnCFrontend, CnCRequest, NewStore, ShutdownStore, TransferStore}
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.common.metadata.HostState
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
                   val host: HostState) extends CnCFrontend with Logging:

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

      val msgVariant: codec.CnCRequest.Msg = m match
        case Shutdown(p) =>
          p.success(())
          return
        case CnCNewStore(msg, p) => codec.CnCRequest.Msg.NewStore(Codec.encode(msg))
        case CnCShutdownStore(msg, p) => codec.CnCRequest.Msg.ShutdownStore(Codec.encode(msg))
        case CnCTransferStore(msg, p) => codec.CnCRequest.Msg.TransferStore(Codec.encode(msg))

      val encodedMessage = codec.CnCRequest(msg = msgVariant).toByteArray

      reqSocket.send(encodedMessage)

      val rmsg = reqSocket.recv()

      if rmsg != null then
        val rm = try codec.CnCReply.parseFrom(rmsg) catch
          case t: Throwable =>
            logger.error(s"******* PARSE CnCReply ERROR: $t", t)
            m match
              case Shutdown(p) =>
              case CnCNewStore(msg, p) => p.failure(t)
              case CnCShutdownStore(msg, p) => p.failure(t)
              case CnCTransferStore(msg, p) => p.failure(t)
            throw t

        val isOk = rm.msg match
          case codec.CnCReply.Msg.Ok(_) => true
          case _ => false

        m match
          case Shutdown(p) =>
          case CnCNewStore(msg, p) =>
            if isOk then p.success(())
            else p.failure(new Exception("Invalid CnCReply received"))
          case CnCShutdownStore(msg, p) =>
            if isOk then p.success(())
            else p.failure(new Exception("Invalid CnCReply received"))
          case CnCTransferStore(msg, p) =>
            if isOk then p.success(())
            else p.failure(new Exception("Invalid CnCReply received"))


  
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
