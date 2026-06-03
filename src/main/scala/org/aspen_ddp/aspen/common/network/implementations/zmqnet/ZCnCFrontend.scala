package org.aspen_ddp.aspen.common.network.implementations.zmqnet

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.HostState
import org.aspen_ddp.aspen.common.network.{Codec, CnCFrontend, CnCReply, CnCRequest}
import org.zeromq.SocketType
import scribe.Logging

import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.{Future, Promise}

object ZCnCFrontend:
  sealed abstract class QMsg
  final case class Request(msg: CnCRequest, promise: Promise[CnCReply]) extends QMsg
  final case class Shutdown(promise: Promise[Unit]) extends QMsg

// ZeroMQ REQ-socket implementation of CnCFrontend. Borrows the ZContext from
// the supplied ZMQNet but owns its own socket and IO thread. One frontend
// targets one host.
class ZCnCFrontend(val network: ZMQNet,
                   val host: HostState) extends CnCFrontend with Logging:

  import ZCnCFrontend.*

  private val msgQueue = new LinkedBlockingQueue[QMsg]()

  private val reqSocket = network.context.createSocket(SocketType.REQ)
  reqSocket.connect(s"tcp://${host.address}:${host.cncPort}")

  private val networkThread = new Thread:
    override def run(): Unit = ioThread()
  networkThread.start()

  private def ioThread(): Unit =
    while true do
      msgQueue.take() match
        case Shutdown(p) =>
          p.success(())
          return
        case Request(msg, p) =>
          try
            reqSocket.send(Codec.encodeCnCRequest(msg).toByteArray)
            val rmsg = reqSocket.recv()
            if rmsg == null then
              p.failure(new Exception("Null CnC reply received"))
            else
              p.success(Codec.decode(codec.CnCReply.parseFrom(rmsg)))
          catch
            case t: Throwable =>
              logger.error(s"CnC request failed: $t", t)
              p.failure(t)

  def send(msg: CnCRequest): Future[CnCReply] =
    val p = Promise[CnCReply]()
    msgQueue.put(Request(msg, p))
    p.future
