package org.aspen_ddp.aspen.demo.network

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.{ClientId, ClientRequest, ClientResponse, HostHeartbeat, TxMessage}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId}
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, PreTransactionOpportunisticRebuild}
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.client.{AspenClient, Host, HostId, StoragePool}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.demo.BootstrapConfig
import org.zeromq.ZMQ.{DONTWAIT, PollItem}
import org.zeromq.{SocketType, ZContext, ZMQ}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object ZMQNetwork {

  private class HostState(val hostId: HostId,
                          val hostName: String,
                          val address: String,
                          val port: Int,
                          val oinitialHeartbeat: Option[Array[Byte]],
                          context: ZContext,
                          clientId: ClientId) extends Logging:

    var lastHeartbeatTime: Long = 0
    var isOnline: Boolean = false
    val dealer: ZMQ.Socket = context.createSocket(SocketType.DEALER)
    val pollItem: PollItem = new PollItem(dealer, ZMQ.Poller.POLLIN)
    
    dealer.setIdentity(clientId.toBytes)
    dealer.connect(s"tcp://$address:$port")
    
    oinitialHeartbeat.foreach: msg =>
      dealer.send(msg)
    
    def heartbeatReceived(): Unit =
      lastHeartbeatTime = System.nanoTime()
      if (!isOnline)
        logger.info(s"Node $hostName is Online")
      isOnline = true
    
    def setOffline(): Unit = 
      if (isOnline)
        logger.info(s"Node $hostName is Offline")
      isOnline = false

  private class PendingPoolLookup:
    var storeMessages: Map[StoreId, List[Array[Byte]]] = Map()
    
    def addMessage(storeId: StoreId, msg: Array[Byte]): Unit =
      val lst = storeMessages.get(storeId) match 
        case None => msg :: Nil
        case Some(l) => msg :: l
      storeMessages += storeId -> lst
      
  private class PendingHostLookup:
    var pendingMessages: List[Array[Byte]] = Nil
    
    def addMessage(msg: Array[Byte]): Unit = 
      pendingMessages = msg :: pendingMessages
    
    def addMessages(lst: List[Array[Byte]]): Unit =
      pendingMessages = lst ++ pendingMessages
  
  private sealed abstract class SendQueueMsg
  private case class SendClientRequest(msg: ClientRequest) extends SendQueueMsg
  private case class SendClientTransactionMessage(msg: TxMessage) extends SendQueueMsg
  private case class SendClientResponse(msg: ClientResponse) extends SendQueueMsg
  private case class SendServerTransactionMessage(msg: TxMessage) extends SendQueueMsg
  private case class NewHostConnected() extends SendQueueMsg

  private class CliMessenger(net: ZMQNetwork) extends ClientMessenger with Logging {
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

  private class SrvMessenger(net: ZMQNetwork) extends ServerMessenger with Logging {
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
                 val bootstrapConfig: BootstrapConfig.Config,
                 val ohostNode: Option[(HostId, Int)],
                 val heartbeatPeriod: Duration,
                 val onClientResponseReceived: ClientResponse => Unit,
                 val onClientRequestReceived: ClientRequest => Unit,
                 val onTransactionMessageReceived: TxMessage => Unit) extends Logging {

  import ZMQNetwork._

  val clientId: ClientId = oclientId.getOrElse(ClientId(UUID.randomUUID()))
  
  var client: AspenClient = null

  logger.debug(s"ZMQNetwork Client ID: ${clientId.uuid.toString}")
  
  private[network] val context = new ZContext()

  private var clients: Map[ClientId, Array[Byte]] = Map()

  private var lastRouterMessageReceived = System.nanoTime()

  private val sendQueueSocket = context.createSocket(SocketType.DEALER)
  sendQueueSocket.bind("inproc://send-message-queued")

  private val sendQueue = new java.util.concurrent.ConcurrentLinkedQueue[SendQueueMsg]()

  private val sendQueueClientSocket = ThreadLocal.withInitial[ZMQ.Socket]: () =>
    val socket = context.createSocket(SocketType.DEALER)
    socket.connect("inproc://send-message-queued")
    socket

  private val sendQueuePollItem = new PollItem(sendQueueSocket, ZMQ.Poller.POLLIN)

  private val orouterSocket = ohostNode.map: (_, port) =>
    val router = context.createSocket(SocketType.ROUTER)
    router.bind(s"tcp://*:$port")
    router
  
  private val orouterPollItem = orouterSocket.map: router =>
    new PollItem(router, ZMQ.Poller.POLLIN)

  private val oheartbeatMessage = ohostNode.map: (hostId, _) =>
    ProtobufMessageEncoder.encodeMessage(HostHeartbeat(hostId))
  
  private var storeToHost: Map[StoreId, HostId] =
    bootstrapConfig.hosts.flatMap { host =>
      host.stores.map(storeId => storeId -> host.hostId)
    }.toMap

  private var hostStates: Map[HostId, HostState] = bootstrapConfig.hosts.map { host =>
    host.hostId -> HostState(
      host.hostId,
      host.name,
      host.address,
      host.dataPort,
      oheartbeatMessage,
      context,
      clientId
    )
  }.toMap
  
  private var pendingPoolLookup: Map[PoolId, PendingPoolLookup] = Map()
  private var pendingHostLookup: Map[HostId, PendingHostLookup] = Map()
  
  private val networkThread = new Thread {
    override def run(): Unit = ioThread()
  }
  
  def startIoThread(aspenClient: AspenClient): Unit =
    client = aspenClient
    networkThread.start()
    
  def joinIoThread(): Unit = networkThread.join()

  def clientMessenger: ClientMessenger = new CliMessenger(this)

  def serverMessenger: ServerMessenger = new SrvMessenger(this)
  
  private def poolLookedUp(pool: StoragePool): Unit = synchronized {
    pool.stores.zipWithIndex.foreach: (entry, index) =>
      val storeId = StoreId(pool.poolId, index.toByte)
      storeToHost += storeId -> entry.hostId
      
    pendingPoolLookup.get(pool.poolId).foreach: ppl =>
      ppl.storeMessages.foreach: (storeId, msgList) =>
        val hostId = storeToHost(storeId)
        hostStates.get(hostId) match 
          case Some(hostState) => msgList.reverse.foreach(hostState.dealer.send)
          case None => 
            val phl = pendingHostLookup.get(hostId) match
              case Some(p) => p
              case None =>
                val p = PendingHostLookup()
                pendingHostLookup += hostId -> p
                p
            phl.addMessages(msgList)
          
    pendingPoolLookup -= pool.poolId     
  }

  private def hostLookedUp(host: Host): Unit = synchronized {
    val hostState = HostState(
      host.hostId,
      host.name,
      host.address,
      host.dataPort,
      oheartbeatMessage,
      context,
      clientId
    )
    
    hostStates += host.hostId -> hostState
    
    pendingHostLookup.get(host.hostId).foreach: phl =>
      phl.pendingMessages.reverse.foreach(hostState.dealer.send)
      
    pendingHostLookup -= host.hostId

    // Tell I/O thread about the new connection so it can rebuild it's Poller
    // instance
    queueMessageForSend(NewHostConnected())
  }
  
  private def sendStoreMessage(storeId: StoreId, msg: Array[Byte]): Unit = synchronized {
    given ExecutionContext = client.clientContext
    
    storeToHost.get(storeId) match
      case Some(hostId) => hostStates.get(hostId) match
        case Some(hostState) => hostState.dealer.send(msg)
        case None =>
          val phl = pendingHostLookup.get(hostId) match
            case Some(p) => p
            case None => 
              val p = PendingHostLookup()
              pendingHostLookup += hostId -> p
              client.getHost(hostId).foreach(hostLookedUp)
              p
          phl.addMessage(msg)
      case None => 
        val ppl = pendingPoolLookup.get(storeId.poolId) match 
          case Some(p) => p
          case None =>
            val p = PendingPoolLookup()
            pendingPoolLookup += storeId.poolId -> p
            client.getStoragePool(storeId.poolId).foreach(poolLookedUp)
            p
        ppl.addMessage(storeId, msg)
  }
  
  // Queue message in concurrent linked list and send an empty message to the queue socket
  // to wake the IO thread if it's sleeping in a call to poll()
  private def queueMessageForSend(msg: SendQueueMsg): Unit =
    sendQueue.add(msg)
    sendQueueClientSocket.get().send("")

  private def heartbeat(): Unit =
    synchronized:
      val offlineThreshold = System.nanoTime() - (heartbeatPeriod * 3).toNanos
      
      hostStates.valuesIterator.foreach: host =>
        if host.lastHeartbeatTime <= offlineThreshold && host.isOnline then
          host.setOffline()
        oheartbeatMessage.foreach: msg =>
          host.dealer.send(msg)
  
  private def ioThread(): Unit = {

    val routerPoll = orouterPollItem match
      case Some(_) => 1
      case None => 0

    val heartBeatPeriodMillis = heartbeatPeriod.toMillis.toInt
    var nextHeartbeat = System.currentTimeMillis() + heartBeatPeriodMillis
    
    var hostsArray: Array[HostState] = null
    var poller: ZMQ.Poller = null
    
    def recreatePoller(): Unit = synchronized {
      // Unregister
      if hostsArray != null then
        hostsArray.foreach(host => poller.unregister(host.dealer))
        orouterSocket.foreach(poller.unregister)
        poller.unregister(sendQueueSocket)
      
      // Recreate hostArray to pick up any new connections
      hostsArray = hostStates.valuesIterator.toArray
      // Poller size is size of hosts array plus one for sendQueue plus 0 or 1
      // depending on if we have a router socket or not
      poller = context.createPoller(hostsArray.length + 1 + routerPoll)

      hostsArray.foreach(host => poller.register(host.pollItem))
      poller.register(sendQueuePollItem)
      orouterPollItem.foreach(poller.register)
    }

    recreatePoller()
    
    while (!Thread.currentThread().isInterrupted) {
      val now = System.currentTimeMillis()

      if now >= nextHeartbeat then
        nextHeartbeat = now + heartBeatPeriodMillis
        heartbeat()

      try 
        val timeToNextHB = nextHeartbeat - now
        if timeToNextHB > 0 then
          //logger.trace(s"*** SLEEPING. Time to next HB: $timeToNextHB")
          poller.poll(timeToNextHB)
          //logger.trace(s"*** Woke from poll. Time to next HB: ${nextHeartbeat - System.currentTimeMillis()}")
      catch 
        case e: Throwable =>
          logger.warn(s"Poll method threw an exception. Creating a new poller. Error: $e")
          recreatePoller()

      // Process messages coming from hosts
      for i <- hostsArray.indices do
        if poller.pollin(i) then
          var msg = hostsArray(i).dealer.recv(ZMQ.DONTWAIT)
          while msg != null do
            try
              onDealerMessageReceived(msg)
            catch
              case t: Throwable => logger.error(s"**** Error in onDealerMessageReceived: $t", t)
            msg = hostsArray(i).dealer.recv(ZMQ.DONTWAIT)

      // Process messages from sendQueue
      if poller.pollin(hostsArray.length) then
        var msg = sendQueueSocket.recv(ZMQ.DONTWAIT)
        while msg != null do
          msg = sendQueueSocket.recv(ZMQ.DONTWAIT)
          
      // Process messages sent to our local host
      orouterSocket.foreach: router =>
        if poller.pollin(hostsArray.length + 1) then
          var from = router.recv(ZMQ.DONTWAIT)
          var msg = router.recv(ZMQ.DONTWAIT)
          while from != null && msg != null do
            try
              onRouterMessageReceived(from, msg)
            catch
              case t: Throwable => logger.error(s"**** Error in onRouterMessageReceived: $t", t)
            from = router.recv(ZMQ.DONTWAIT)
            msg = router.recv(ZMQ.DONTWAIT)

      var qmsg = sendQueue.poll()
      while qmsg != null do
        qmsg match
          case SendClientRequest(msg) =>
            logger.trace(s"Sending $msg")
            sendStoreMessage(msg.toStore, ProtobufMessageEncoder.encodeMessage(msg))
            
          case SendClientTransactionMessage(msg) =>
            logger.trace(s"Sending $msg")
            sendStoreMessage(msg.to, ProtobufMessageEncoder.encodeMessage(msg))
            
          case SendClientResponse(msg) =>
            logger.trace(s"Sending $msg")
            clients.get(msg.toClient).foreach: zmqIdentity =>
              orouterSocket.foreach: router =>
                router.send(zmqIdentity, ZMQ.SNDMORE)
                router.send(ProtobufMessageEncoder.encodeMessage(msg))
                
          case SendServerTransactionMessage(msg) =>
            logger.trace(s"Sending $msg")
            sendStoreMessage(msg.to, ProtobufMessageEncoder.encodeMessage(msg))

          case NewHostConnected() =>
            recreatePoller()
            
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

    if m.hasHostHeartbeat then
      val msg = Codec.decode(m.getHostHeartbeat)
      logger.trace(s"Got $msg")
      hostStates.get(msg.hostId) match
        case None =>
          //val ns = new HostState(msg.nodeName,0, false)
          //ns.heartbeatReceived()
          //nodeStates += msg.nodeName -> ns
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
