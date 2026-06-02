package org.aspen_ddp.aspen.common.network.implementations.zmqnet

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.EvictingQueue
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.zeromq.ZMQ.{DONTWAIT, PollItem}
import org.zeromq.{SocketType, ZContext, ZMQ}
import scribe.Logging

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

object ZMQNet:

  class ZMQHostEntry(hostId: HostId,
                     name: String,
                     address: String,
                     dataPort: Int,
                     cncPort: Int,
                     storeTransferPort: Int) extends MetadataManager.HostEntry(hostId, name, address, dataPort, cncPort, storeTransferPort) with Logging:
    var odealer: Option[ZMQ.Socket] = None
    var opollItem: Option[PollItem] = None
    val pendingMessages: ConcurrentLinkedQueue[Message] = new ConcurrentLinkedQueue[Message]()
    var lastHeartbeatTime: Long = 0
    var isOnline: Boolean = false

    def heartbeatReceived(): Unit =
      lastHeartbeatTime = System.nanoTime()
      if !isOnline then
        logger.info(s"Node $name is Online")
      isOnline = true

    def setOffline(): Unit =
      if isOnline then
        logger.info(s"Node $name is Offline")
      isOnline = false

  private sealed abstract class SendQueueMsg
  private case class SendToStore(storeId: StoreId, msg: Message) extends SendQueueMsg
  private case class SendToHost(hostId: HostId, msg: Message) extends SendQueueMsg
  private case class SendToClient(msg: ClientResponse) extends SendQueueMsg
  private case class NewHostAvailable(entry: ZMQHostEntry) extends SendQueueMsg
  private case class ProcessPendingMessages(entry: ZMQHostEntry) extends SendQueueMsg

  private class CliMessenger(net: ZMQNet) extends ClientMessenger:
    def sendClientRequest(msg: ClientRequest): Unit =
      net.queueForSend(SendToStore(msg.toStore, msg))

    def sendTransactionMessage(msg: TxMessage): Unit =
      net.queueForSend(SendToStore(msg.to, msg))

    def sendTransactionMessages(msg: List[TxMessage]): Unit =
      msg.foreach(sendTransactionMessage)

    def sendHostMessage(msg: HostMessage): Unit =
      net.queueForSend(SendToHost(msg.toHost, msg))

    def dropCacheForStore(storeId: StoreId): Unit =
      net.metadataManager.dropStoreMapping(storeId)

  private class SrvMessenger(net: ZMQNet) extends ServerMessenger:
    def sendClientResponse(msg: ClientResponse): Unit =
      net.queueForSend(SendToClient(msg))

    def sendTransactionMessage(msg: TxMessage): Unit =
      net.queueForSend(SendToStore(msg.to, msg))

    def sendTransactionMessages(msg: List[TxMessage]): Unit =
      msg.foreach(sendTransactionMessage)

    def dropCacheForStore(storeId: StoreId): Unit =
      net.metadataManager.dropStoreMapping(storeId)


class ZMQNet(val bootstrapConfigFile: os.Path,
             val oclientId: Option[ClientId],
             val ohostNode: Option[(HostId, Int)],
             val heartbeatPeriod: Duration,
             val messageHandler: MessageHandler) extends Logging:

  import ZMQNet.*

  val clientId: ClientId = oclientId.getOrElse(ClientId(UUID.randomUUID()))

  logger.debug(s"ZMQNet Client ID: ${clientId.uuid.toString}")

  val context = new ZContext()

  private var clients: Map[ClientId, Array[Byte]] = Map()

  private val sendQueue = new ConcurrentLinkedQueue[SendQueueMsg]()

  private val sendQueueSocket = context.createSocket(SocketType.DEALER)
  sendQueueSocket.bind("inproc://send-message-queued")

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
    ProtobufMessageCodec.encodeHeartbeat(HostHeartbeat(hostId))

  private val networkImpl = new MetadataManager.NetworkImplInterface[ZMQHostEntry]:
    def createHostEntry(hostId: HostId,
                        name: String,
                        address: String,
                        dataPort: Int,
                        cncPort: Int,
                        storeTransferPort: Int,
                        queuedMessages: EvictingQueue[Message]): ZMQHostEntry =
      val entry = new ZMQHostEntry(hostId, name, address, dataPort, cncPort, storeTransferPort)
      // Drain any queued messages into the host entry's pending queue
      var omsg = queuedMessages.dequeue()
      while omsg.isDefined do
        omsg.foreach(entry.pendingMessages.add)
        omsg = queuedMessages.dequeue()
      // Tell the IO thread to create a socket for this host
      sendQueue.add(NewHostAvailable(entry))
      wakeIoThread()
      entry

    def storeResolved(hostEntry: ZMQHostEntry, storeId: StoreId, queuedMessages: EvictingQueue[Message]): Unit =
      // Drain queued messages into the host entry's pending queue
      var omsg = queuedMessages.dequeue()
      while omsg.isDefined do
        omsg.foreach(hostEntry.pendingMessages.add)
        omsg = queuedMessages.dequeue()
      // Tell the IO thread to process pending messages
      sendQueue.add(ProcessPendingMessages(hostEntry))
      wakeIoThread()

  val metadataManager: MetadataManager[ZMQHostEntry] =
    new MetadataManager[ZMQHostEntry](bootstrapConfigFile, networkImpl)

  val clientMessenger: ClientMessenger = new CliMessenger(this)
  val serverMessenger: ServerMessenger = new SrvMessenger(this)

  private val networkThread = new Thread:
    override def run(): Unit = ioThread()

  def startIoThread(client: AspenClient): Unit =
    metadataManager.setAspenClient(client)
    networkThread.start()

  def joinIoThread(): Unit = networkThread.join()

  private def queueForSend(msg: SendQueueMsg): Unit =
    sendQueue.add(msg)
    wakeIoThread()

  private def wakeIoThread(): Unit =
    sendQueueClientSocket.get().send("")

  private def updateClientId(cid: ClientId, routerAddress: Array[Byte]): Unit =
    clients.get(cid) match
      case None => clients += cid -> routerAddress
      case Some(addr) =>
        if !java.util.Arrays.equals(routerAddress, addr) then
          clients += cid -> routerAddress

  private def decodeAndDispatch(rawMsg: Array[Byte], routerFrom: Option[Array[Byte]]): Unit =
    ProtobufMessageCodec.decodeMessage(rawMsg) match
      case None => // malformed or empty message, already logged by codec
      case Some(decoded) =>
        decoded match
          case hb: HostHeartbeat =>
            metadataManager.getHostEntry(hb.hostId).foreach(_.heartbeatReceived())

          case req: ClientRequest =>
            routerFrom.foreach(from => updateClientId(req.fromClient, from))
            messageHandler.onClientRequestReceived(req)

          case resp: ClientResponse =>
            messageHandler.onClientResponseReceived(resp)

          case tx: TxMessage =>
            messageHandler.onTransactionMessageReceived(tx)

          case hm: HostMessage =>
            messageHandler.onHostMessageReceived(hm)

          case _: HostResponse =>
            // Not currently handled

  private def heartbeat(hostsArray: Array[ZMQHostEntry]): Unit =
    val offlineThreshold = System.nanoTime() - (heartbeatPeriod * 3).toNanos

    hostsArray.foreach: host =>
      if host.lastHeartbeatTime <= offlineThreshold && host.isOnline then
        host.setOffline()
      host.odealer.foreach: dealer =>
        oheartbeatMessage.foreach: msg =>
          dealer.send(msg)

  private def ioThread(): Unit =
    val connectedHosts = ArrayBuffer[ZMQHostEntry]()
    var hostsArray: Array[ZMQHostEntry] = Array.empty
    var poller: ZMQ.Poller = null

    val routerPollCount = if orouterPollItem.isDefined then 1 else 0

    def rebuildPoller(): Unit =
      // Unregister old poller items
      if poller != null then
        hostsArray.foreach(host => host.odealer.foreach(poller.unregister))
        poller.unregister(sendQueueSocket)
        orouterSocket.foreach(poller.unregister)

      hostsArray = connectedHosts.toArray
      poller = context.createPoller(hostsArray.length + 1 + routerPollCount)

      hostsArray.foreach: host =>
        host.opollItem.foreach(poller.register)
      poller.register(sendQueuePollItem)
      orouterPollItem.foreach(poller.register)

    // Initialize with any bootstrap hosts that MetadataManager already created
    // (those were created synchronously in the MetadataManager constructor)
    // They don't have sockets yet so we need to connect them here.

    rebuildPoller()

    val heartBeatPeriodMillis = heartbeatPeriod.toMillis.toInt
    var nextHeartbeat = System.currentTimeMillis() + heartBeatPeriodMillis

    while !Thread.currentThread().isInterrupted do
      val now = System.currentTimeMillis()

      if now >= nextHeartbeat then
        nextHeartbeat = now + heartBeatPeriodMillis
        heartbeat(hostsArray)

      try
        val timeToNextHB = nextHeartbeat - now
        if timeToNextHB > 0 then
          poller.poll(timeToNextHB)
      catch
        case e: Throwable =>
          logger.warn(s"Poll method threw an exception. Creating a new poller. Error: $e")
          rebuildPoller()

      // Process messages from dealer sockets (connected hosts)
      for i <- hostsArray.indices do
        if poller.pollin(i) then
          hostsArray(i).odealer.foreach: dealer =>
            var msg = dealer.recv(ZMQ.DONTWAIT)
            while msg != null do
              try
                decodeAndDispatch(msg, None)
              catch
                case t: Throwable => logger.error(s"Error in decodeAndDispatch (dealer): $t", t)
              msg = dealer.recv(ZMQ.DONTWAIT)

      // Drain send queue wake signals
      if poller.pollin(hostsArray.length) then
        var msg = sendQueueSocket.recv(ZMQ.DONTWAIT)
        while msg != null do
          msg = sendQueueSocket.recv(ZMQ.DONTWAIT)

      // Process router messages (if server node)
      orouterSocket.foreach: router =>
        if poller.pollin(hostsArray.length + 1) then
          var from = router.recv(ZMQ.DONTWAIT)
          var msg = router.recv(ZMQ.DONTWAIT)
          while from != null && msg != null do
            try
              decodeAndDispatch(msg, Some(from))
            catch
              case t: Throwable => logger.error(s"Error in decodeAndDispatch (router): $t", t)
            from = router.recv(ZMQ.DONTWAIT)
            msg = router.recv(ZMQ.DONTWAIT)

      // Process send queue items
      var qmsg = sendQueue.poll()
      while qmsg != null do
        qmsg match
          case SendToStore(storeId, msg) =>
            metadataManager.getHostEntryOrQueueMessage(storeId, msg) match
              case Some(hostEntry) =>
                hostEntry.odealer match
                  case Some(dealer) =>
                    dealer.send(ProtobufMessageCodec.encodeMessage(msg))
                  case None =>
                    hostEntry.pendingMessages.add(msg)
              case None =>
                // MetadataManager queued the message for later delivery

          case SendToHost(hostId, msg) =>
            metadataManager.getHostEntryOrQueueMessage(hostId, msg) match
              case Some(hostEntry) =>
                hostEntry.odealer match
                  case Some(dealer) =>
                    dealer.send(ProtobufMessageCodec.encodeMessage(msg))
                  case None =>
                    hostEntry.pendingMessages.add(msg)
              case None =>
                // MetadataManager queued the message for later delivery

          case SendToClient(msg) =>
            clients.get(msg.toClient).foreach: zmqIdentity =>
              orouterSocket.foreach: router =>
                router.send(zmqIdentity, ZMQ.SNDMORE)
                router.send(ProtobufMessageCodec.encodeMessage(msg))

          case NewHostAvailable(entry) =>
            val dealer = context.createSocket(SocketType.DEALER)
            dealer.setIdentity(clientId.toBytes)
            dealer.connect(s"tcp://${entry.address}:${entry.dataPort}")
            entry.odealer = Some(dealer)
            entry.opollItem = Some(new PollItem(dealer, ZMQ.Poller.POLLIN))

            // Send initial heartbeat if we are a server node
            oheartbeatMessage.foreach(dealer.send(_))

            // Drain any pending messages
            var pending = entry.pendingMessages.poll()
            while pending != null do
              dealer.send(ProtobufMessageCodec.encodeMessage(pending))
              pending = entry.pendingMessages.poll()

            connectedHosts += entry
            rebuildPoller()

          case ProcessPendingMessages(entry) =>
            entry.odealer.foreach: dealer =>
              var pending = entry.pendingMessages.poll()
              while pending != null do
                dealer.send(ProtobufMessageCodec.encodeMessage(pending))
                pending = entry.pendingMessages.poll()

        qmsg = sendQueue.poll()

    logger.trace("ZMQNet.ioThread EXITING")
