package org.aspen_ddp.aspen.common.network.implementations.zmqnet

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.{Codec, CnCBackend, CnCMessageReceiver, CnCReply}
import org.zeromq.SocketType
import scribe.Logging

import scala.concurrent.Await
import scala.concurrent.duration.Duration

// ZeroMQ REP-socket implementation of the CnC listening side. Borrows the
// ZContext from the supplied ZMQNet but owns its own socket and service thread.
// Has no knowledge of the application; all handling is delegated to the
// CnCMessageReceiver. The receiver's Future is the completion signal that gates
// the synchronous REP reply.
class ZCnCBackend(val network: ZMQNet,
                  val cncPort: Int,
                  val receiver: CnCMessageReceiver) extends CnCBackend with Logging:

  private val repSocket = network.context.createSocket(SocketType.REP)
  repSocket.bind(s"tcp://*:$cncPort")

  private val networkThread = new Thread:
    override def run(): Unit = service()
  networkThread.start()

  private def service(): Unit =
    while true do
      val encodedMessage = repSocket.recv()

      if encodedMessage != null then
        val reply: CnCReply =
          try
            val request = Codec.decodeCnCRequest(codec.CnCRequest.parseFrom(encodedMessage))
            Await.result(receiver.onCnCRequestReceived(request), Duration.Inf)
          catch
            case t: Throwable =>
              logger.error(s"Failed to process CnC request: $t", t)
              CnCReply.Error(Option(t.getMessage).getOrElse(t.toString))

        repSocket.send(Codec.encode(reply).toByteArray)
