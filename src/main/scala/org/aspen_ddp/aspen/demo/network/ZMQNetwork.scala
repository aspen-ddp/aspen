package org.aspen_ddp.aspen.demo.network

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.{ClientId, ClientRequest, ClientResponse, NodeHeartbeat, TxMessage}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId}
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, PreTransactionOpportunisticRebuild}
import org.apache.logging.log4j.scala.Logging
import org.zeromq.ZMQ.{DONTWAIT, PollItem}
import org.zeromq.{SocketType, ZContext, ZMQ}

import scala.concurrent.duration.Duration

object ZMQNetwork {
  class SocketState(val nodeName: String,
                    var dealer: ZMQ.Socket)

  class NodeState(val nodeName: String,
                  var lastHeartbeatTime: Long,
                  var isOnline: Boolean) extends Logging {

    def heartbeatReceived(): Unit = {
      lastHeartbeatTime = System.nanoTime()
      if (!isOnline)
        logger.info(s"Node $nodeName is Online")
      isOnline = true
    }
    def setOffline(): Unit = {
      if (isOnline)
        logger.info(s"Node $nodeName is Offline")
      isOnline = false
    }
  }

  sealed abstract class SendQueueMsg
  case class SendClientRequest(msg: ClientRequest) extends SendQueueMsg
  case class SendClientTransactionMessage(msg: TxMessage) extends SendQueueMsg
  case class SendClientResponse(msg: ClientResponse) extends SendQueueMsg
  case class SendServerTransactionMessage(msg: TxMessage) extends SendQueueMsg

  class CliMessenger(net: ZMQNetwork) extends ClientMessenger with Logging {
    def sendClientRequest(msg: ClientRequest): Unit = synchronized {
      //logger.trace(s"Sending $msg")
      net.queueMessageForSend(SendClientRequest(msg))
    }

    def sendTransactionMessage(msg: TxMessage): Unit = synchronized {
      //logger.trace(s"Sending $msg")
      net.queueMessageForSend(SendClientTransactionMessage(msg))
    }

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)
  }

  class SrvMessenger(net: ZMQNetwork) extends ServerMessenger with Logging {
    def sendClientResponse(msg: ClientResponse): Unit = synchronized {
      val zaddr = net.clients.get(msg.toClient) match
        case Some(zmqIdentity) => String(zmqIdentity)
        case None => "UNKNOWN!"

      //logger.trace(s"Sending $msg")
      net.queueMessageForSend(SendClientResponse(msg))
    }

    def sendTransactionMessage(msg: TxMessage): Unit = synchronized {
      //logger.trace(s"Sending $msg")
      net.queueMessageForSend(SendServerTransactionMessage(msg))
    }

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)
  }
}

class ZMQNetwork(val oclientId: Option[ClientId],
                 val nodes: Map[String, (String, Int)], // NodeName -> (hostname, port)
                 val stores: Map[StoreId, String], // StoreId -> NodeName
                 val storageNode: Option[(String, String, Int)], // (NodeName, hostname/IP, port)
                 val heartbeatPeriod: Duration,
                 val onClientResponseReceived: ClientResponse => Unit,
                 val onClientRequestReceived: ClientRequest => Unit,
                 val onTransactionMessageReceived: TxMessage => Unit) extends Logging {

  import ZMQNetwork._

  val clientId: ClientId = oclientId.getOrElse(ClientId(UUID.randomUUID()))

  logger.debug(s"ZMQNetwork Client ID: ${clientId.uuid.toString}")
  
  private[network] val context = new ZContext()

  private var clients: Map[ClientId, Array[Byte]] = Map()

  private var lastRouterMessageReceived = System.nanoTime()

  private var nodeStates: Map[String, NodeState] = nodes.map{t => t._1 -> new NodeState(t._1, 0, false)}

  private val sendQueueSocket = context.createSocket(SocketType.DEALER)
  sendQueueSocket.bind("inproc://send-message-queued")

  private val sendQueue = new java.util.concurrent.ConcurrentLinkedQueue[SendQueueMsg]()

  private val sendQueueClientSocket = ThreadLocal.withInitial[ZMQ.Socket]: () =>
    val socket = context.createSocket(SocketType.DEALER)
    socket.connect("inproc://send-message-queued")
    socket

  private val sendQueuePollItem = new PollItem(sendQueueSocket, ZMQ.Poller.POLLIN)

  private val routerSocket = storageNode.map { t =>
    val (_, _, port) = t
    val router = context.createSocket(SocketType.ROUTER)
    router.bind(s"tcp://*:$port")
    router
  }

  private val heartbeatMessage = storageNode.map { t =>
    val (nodeName, _, _) = t
    ProtobufMessageEncoder.encodeMessage(NodeHeartbeat(nodeName))
  }

  case class Peer(nodeName: String, dealer: ZMQ.Socket, pollItem: PollItem)

  private val dealers = nodes.map { t =>
    val (nodeName, (host, port)) = t
    val dealer = context.createSocket(SocketType.DEALER)
    dealer.setIdentity(clientId.toBytes)
    dealer.connect(s"tcp://$host:$port")
    heartbeatMessage.foreach(msg => dealer.send(msg))
    Peer(nodeName, dealer, new PollItem(dealer, ZMQ.Poller.POLLIN))
  }.toArray

  private val peers = dealers.map(p => p.nodeName -> p).toMap

  private val routerPollItem = routerSocket.map { router =>
    new PollItem(router, ZMQ.Poller.POLLIN)
  }

  def clientMessenger: ClientMessenger = new CliMessenger(this)

  def serverMessenger: ServerMessenger = new SrvMessenger(this)

  // Queue message in concurrent linked list and send an empty message to the queue socket
  // to wake the IO thread if it's sleeping in a call to poll()
  def queueMessageForSend(msg: SendQueueMsg): Unit =
    sendQueue.add(msg)
    sendQueueClientSocket.get().send("")

  private def heartbeat(): Unit = {
    val offlineThreshold = System.nanoTime() - (heartbeatPeriod * 3).toNanos
    nodeStates.foreach { t =>
      val (_, ns) = t
      if (ns.lastHeartbeatTime <= offlineThreshold && ns.isOnline) {
        ns.setOffline()
      }
    }
    heartbeatMessage.foreach { msg =>
      logger.trace("Sending node heartbeat to peers")
      dealers.foreach(_.dealer.send(msg))
    }
  }

  def ioThread(): Unit = {

    val (size, routerSocketIndex) = routerPollItem match
      case Some(_) => (dealers.length + 2, dealers.length + 1)
      case None => (dealers.length + 1, -1)

    var poller = context.createPoller(size)

    dealers.foreach(peer => poller.register(peer.pollItem))
    poller.register(sendQueuePollItem)
    routerPollItem.foreach(poller.register)

    val heartBeatPeriodMillis = heartbeatPeriod.toMillis.toInt
    var nextHeartbeat = System.currentTimeMillis() + heartBeatPeriodMillis

    while (!Thread.currentThread().isInterrupted) {
      val now = System.currentTimeMillis()

      if (now >= nextHeartbeat) {
        nextHeartbeat = now + heartBeatPeriodMillis
        heartbeat()
      }

      try {
        val timeToNextHB = nextHeartbeat - now
        if timeToNextHB > 0 then
          //logger.trace(s"*** SLEEPING. Time to next HB: $timeToNextHB")
          poller.poll(timeToNextHB)
          //logger.trace(s"*** Woke from poll. Time to next HB: ${nextHeartbeat - System.currentTimeMillis()}")
      } catch {
        case e: Throwable =>
          logger.warn(s"Poll method threw an exception. Creating a new poller. Error: $e")

          dealers.foreach(peer => poller.unregister(peer.dealer))
          routerSocket.foreach(poller.unregister)

          poller = context.createPoller(dealers.length + 1)

          dealers.foreach(peer => poller.register(peer.pollItem))
          routerPollItem.foreach(poller.register)
      }

      for (i <- dealers.indices) {
        if (poller.pollin(i)) {
          var msg = dealers(i).dealer.recv(ZMQ.DONTWAIT)
          while msg != null do
            try
              onDealerMessageReceived(msg)
            catch
              case t: Throwable => logger.error(s"**** Error in onDealerMessageReceived: $t", t)
            msg = dealers(i).dealer.recv(ZMQ.DONTWAIT)
        }
      }

      routerSocket.foreach { router =>
        if (poller.pollin(routerSocketIndex)) {
          var from = router.recv(ZMQ.DONTWAIT)
          var msg = router.recv(ZMQ.DONTWAIT)
          while from != null && msg != null do
            try
              onRouterMessageReceived(from, msg)
            catch
              case t: Throwable => logger.error(s"**** Error in onRouterMessageReceived: $t", t)
            from = router.recv(ZMQ.DONTWAIT)
            msg = router.recv(ZMQ.DONTWAIT)
        }
      }

      if poller.pollin(dealers.length) then
        var msg = sendQueueSocket.recv(ZMQ.DONTWAIT)
        while msg != null do
          msg = sendQueueSocket.recv(ZMQ.DONTWAIT)

      var qmsg = sendQueue.poll()
      while qmsg != null do
        qmsg match
          case SendClientRequest(msg) =>
            logger.trace(s"Sending $msg")
            peers(stores(msg.toStore)).dealer.send(ProtobufMessageEncoder.encodeMessage(msg))
          case SendClientTransactionMessage(msg) =>
            logger.trace(s"Sending $msg")
            peers(stores(msg.to)).dealer.send(ProtobufMessageEncoder.encodeMessage(msg))
          case SendClientResponse(msg) =>
            logger.trace(s"Sending $msg")
            clients.get(msg.toClient).foreach: zmqIdentity =>
              routerSocket.foreach: router =>
                router.send(zmqIdentity, ZMQ.SNDMORE)
                router.send(ProtobufMessageEncoder.encodeMessage(msg))
          case SendServerTransactionMessage(msg) =>
            logger.trace(s"Sending $msg")
            peers(stores(msg.to)).dealer.send(ProtobufMessageEncoder.encodeMessage(msg))
        qmsg = sendQueue.poll()
    }

    logger.trace("ZMQNetwork.enterEventLoop EXITING")
  }

  private def onDealerMessageReceived(msg: Array[Byte]): Unit = {
    val bb = ByteBuffer.wrap(msg)
    bb.order(ByteOrder.BIG_ENDIAN)

    val msgLen = bb.getInt()

    val m = try codec.Message.parseFrom(bb) catch
      case t: Throwable =>
        logger.error(s"******* PARSE DEALER MESSAGE ERROR: $t", t)
        throw t

    if m.hasReadResponse then
      val message = Codec.decode(m.getReadResponse)
      logger.trace(s"Got $message")
      onClientResponseReceived(message)

    else if m.hasTxResolved then
      val message = Codec.decode(m.getTxResolved)
      logger.trace(s"Got $message")
      onClientResponseReceived(message)

    else if m.hasTxFinalized then
      val message = Codec.decode(m.getTxFinalized)
      logger.trace(s"Got $message")
      onClientResponseReceived(message)

    else if m.hasAllocateResponse then
      val message = Codec.decode(m.getAllocateResponse)
      logger.trace(s"Got $message")
      onClientResponseReceived(message)
  }

  private def updateClientId(clientId: ClientId, routerAddress: Array[Byte]): Unit = {
    clients.get(clientId) match {
      case None => clients += clientId -> routerAddress
      case Some(addr) =>
        if (!java.util.Arrays.equals(routerAddress, addr))
          clients += clientId -> routerAddress
    }
  }

  private def onRouterMessageReceived(from: Array[Byte], msg: Array[Byte]): Unit = {
    val bb = ByteBuffer.wrap(msg)
    bb.order(ByteOrder.BIG_ENDIAN)

    lastRouterMessageReceived = System.nanoTime()

    val msgLen = bb.getInt()
    bb.limit(4 + msgLen)

    // Must pass a read-only copy to the following method. It'll corrupt the rest of the buffer otherwise
    val m = try codec.Message.parseFrom(bb) catch
      case t: Throwable =>
        logger.error(s"******* PARSE ROUTER MESSAGE ERROR: $t", t)
        throw t

    if m.hasNodeHeartbeat then
      val msg = Codec.decode(m.getNodeHeartbeat)
      logger.trace(s"Got $msg")
      nodeStates.get(msg.nodeName) match
        case None =>
          val ns = new NodeState(msg.nodeName,0, false)
          ns.heartbeatReceived()
          nodeStates += msg.nodeName -> ns
        case Some(ns) => ns.heartbeatReceived()

    else if m.hasRead then
      val message = Codec.decode(m.getRead)
      logger.trace(s"Got $message")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)

    else if m.hasPrepare then
      bb.limit(msg.length)
      bb.position(4 + msgLen)
      val contentSize = bb.getInt()
      val preTxSize = bb.getInt()

      val contentEndPos = bb.position() + contentSize
      val preTxEndPos = contentEndPos + preTxSize

      //val sb = message.txd.allReferencedObjectsSet.foldLeft(new StringBuilder)((sb, o) => sb.append(s" ${o.uuid}"))
      //println(s"got prepare txid ${message.txd.transactionUUID} Leader ${message.txd.designatedLeaderUID} for objects: ${sb.toString()}")

      val updateContent = if (bb.remaining() == 0) (Nil, Nil) else {

        var localUpdates: List[ObjectUpdate] = Nil
        var preTxRebuilds: List[PreTransactionOpportunisticRebuild] = Nil

        // local update content is a series of <16-byte-uuid><4-byte-length><data>

        while (bb.position() != contentEndPos) {
          val msb = bb.getLong()
          val lsb = bb.getLong()
          val len = bb.getInt()
          val uuid = new UUID(msb, lsb)

          val slice = bb.asReadOnlyBuffer()

          slice.limit( slice.position() + len )
          bb.position( bb.position() + len )
          localUpdates = ObjectUpdate(ObjectId(uuid), DataBuffer(slice)) :: localUpdates
        }

        // PreTx Rebuilds are a series of <16-byte-uuid><encoded-object-metadata><4-byte-length><data>

        while (bb.position() != preTxEndPos) {
          val msb = bb.getLong()
          val lsb = bb.getLong()
          val uuid = new UUID(msb, lsb)
          val metadata = Metadata(bb)
          val len = bb.getInt()

          val slice = bb.asReadOnlyBuffer()
          slice.limit( slice.position() + len )
          bb.position( bb.position() + len )
          preTxRebuilds = PreTransactionOpportunisticRebuild(ObjectId(uuid), metadata, DataBuffer(slice)) :: preTxRebuilds
        }

        (localUpdates, preTxRebuilds)
      }
      val message = Codec.decode(m.getPrepare, updateContent._1, updateContent._2)
      logger.trace(s"Got $message")
      onTransactionMessageReceived(message)

    else if m.hasPrepareResponse then
      //println("got prepareResponse")
      val message = Codec.decode(m.getPrepareResponse)
      logger.trace(s"Got $message")
      onTransactionMessageReceived(message)

    else if m.hasAccept then
      //println("got accept")
      val message = Codec.decode(m.getAccept)
      logger.trace(s"Got $message")
      onTransactionMessageReceived(message)

    else if m.hasAcceptResponse then
      val message = Codec.decode(m.getAcceptResponse)
      logger.trace(s"Got $message")
      onTransactionMessageReceived(message)

    else if m.hasResolved then
      val message = Codec.decode(m.getResolved)
      logger.trace(s"Got $message")
      onTransactionMessageReceived(message)

    else if m.hasCommitted then
      val message = Codec.decode(m.getCommitted)
      logger.trace(s"Got $message")
      onTransactionMessageReceived(message)

    else if m.hasFinalized then
      val message = Codec.decode(m.getFinalized)
      logger.trace(s"Got $message")
      onTransactionMessageReceived(message)

    else if m.hasHeartbeat then
      val message = Codec.decode(m.getHeartbeat)
      logger.trace(s"Got $message")
      onTransactionMessageReceived(message)

    else if m.hasAllocate then
      //println(s"got allocate request. Receiver: $a")
      val message = Codec.decode(m.getAllocate)
      logger.trace(s"Got $message")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)

    else if m.hasOpportunisticRebuild then
      val message = Codec.decode(m.getOpportunisticRebuild)
      logger.trace(s"Got $message")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)

    else if m.hasTransactionCompletionQuery then
      val message = Codec.decode(m.getTransactionCompletionQuery)
      logger.trace(s"Got $message")
      updateClientId(message.fromClient, from)
      onClientRequestReceived(message)

    else
      logger.error("Unknown Message!")
  }
}
