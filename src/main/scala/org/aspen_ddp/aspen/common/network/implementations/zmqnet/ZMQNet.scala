package org.aspen_ddp.aspen.common.network.implementations.zmqnet

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{DaemonThreads, EvictingQueue}
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
    // Unless noted otherwise, the state below is read and written only by the zmq-io thread and
    // is published without synchronization. Reading one of these fields from another thread
    // requires making it volatile first -- see odealer. pendingMessages is the exception: it is
    // a concurrent queue and is deliberately added to from other threads.
    //
    // Written by the zmq-io thread outside of any lock. Volatile because awaitPendingMessagesSent
    // reads it from the caller's thread and would otherwise have no happens-before edge with
    // that write. The referenced Option is immutable, so publishing the reference is enough.
    @volatile var odealer: Option[ZMQ.Socket] = None
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
             val ohost: Option[(HostId, Int)],
             val heartbeatPeriod: Duration,
             val messageHandler: MessageHandler) extends Logging:

  import ZMQNet.*

  val clientId: ClientId = oclientId.getOrElse(ClientId(UUID.randomUUID()))

  logger.debug(s"ZMQNet Client ID: ${clientId.uuid.toString}")

  val context = new ZContext()

  private var clients: Map[ClientId, Array[Byte]] = Map()

  private val sendQueue = new ConcurrentLinkedQueue[SendQueueMsg]()

  // Dealer sockets created by the IO thread, recorded here so shutdown() can set their linger.
  // Appended to and never drained: one entry per dealer created, normally one per host this
  // process talks to.
  private val connectedDealers = new ConcurrentLinkedQueue[ZMQ.Socket]()

  // Set by shutdown() so the IO thread can tell deliberate context teardown from a real network
  // failure and exit quietly instead of logging and trying to recover on a dead context.
  @volatile private var shuttingDown = false

  private val sendQueueSocket = context.createSocket(SocketType.DEALER)
  sendQueueSocket.bind("inproc://send-message-queued")

  private val sendQueueClientSocket = ThreadLocal.withInitial[ZMQ.Socket]: () =>
    val socket = context.createSocket(SocketType.DEALER)
    socket.connect("inproc://send-message-queued")
    socket

  private val sendQueuePollItem = new PollItem(sendQueueSocket, ZMQ.Poller.POLLIN)

  private val orouterSocket = ohost.map: (_, port) =>
    val router = context.createSocket(SocketType.ROUTER)
    router.bind(s"tcp://*:$port")
    router

  private val orouterPollItem = orouterSocket.map: router =>
    new PollItem(router, ZMQ.Poller.POLLIN)

  private val oheartbeatMessage = ohost.map: (hostId, _) =>
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

  // Daemon thread so that client processes exit once their work is done. Long-running
  // processes keep themselves alive by blocking the main thread in joinIoThread().
  private val networkThread = DaemonThreads.thread("zmq-io") { ioThread() }

  def startIoThread(client: AspenClient): Unit =
    metadataManager.setAspenClient(client)
    networkThread.start()

  def joinIoThread(): Unit = networkThread.join()

  private def queueForSend(msg: SendQueueMsg): Unit =
    sendQueue.add(msg)
    wakeIoThread()

  private def wakeIoThread(): Unit =
    sendQueueClientSocket.get().send("")

  /** Blocks until ZMQNet is holding no outbound messages, or `timeout` elapses, polling every
   *  25ms. Returns true if it drained.
   *
   *  Takes no host id because its callers cannot name the hosts involved: a command may nudge
   *  hosts it never looked up itself. The predicate therefore covers all three places a
   *  message can be waiting:
   *
   *    - the shared send queue, which holds messages for every host
   *    - MetadataManager's lookup queues, where a message sits while the host or pool it is
   *      addressed to is resolved -- the common case for a nudge to a host this process has
   *      not talked to before
   *    - every resolved host entry's own pending queue, each of which needs a dealer socket
   *      before anything can leave it
   *
   *  Known limits, none fixable at this layer:
   *
   *    - The IO thread removes an item from the send queue just before handing it to a socket
   *      or to MetadataManager, so a return of true can beat the final send, or briefly miss a
   *      message on its way into a lookup queue.
   *    - A failed host or pool lookup drops the entry and the messages parked on it (see
   *      MetadataManager.peekHostEntry). The predicate then goes quiet because the message is
   *      gone rather than sent.
   *
   *  This is not a delivery guarantee in any case. ZMQ buffers internally and the peer may be
   *  down. It establishes only that ZMQNet is no longer holding the message, which is exactly
   *  what a short-lived process would otherwise abandon on exit.
   *
   *  A false return is not a command failure. It means only that ZMQNet may still be holding
   *  something; the caller should report degraded latency rather than an error, since the
   *  receiving host's periodic polling remains the correctness guarantee.
   *
   *  A short-lived process should follow this with shutdown() before exiting: draining
   *  ZMQNet's own queues only hands the message to ZMQ, and shutdown() is what gives ZMQ a
   *  window to put it on the wire.
   */
  def awaitPendingMessagesSent(timeout: Duration): Boolean =
    val deadline = System.nanoTime() + timeout.toNanos
    val pollIntervalMillis = 25L

    // hasParkedMessages is checked before resolvedHostEntries so the handoff from a pending
    // lookup to the newly created host entry cannot slip between the two reads unseen: if it
    // happens in between, the first read has already returned true.
    def drained: Boolean =
      sendQueue.isEmpty &&
        !metadataManager.hasParkedMessages &&
        metadataManager.resolvedHostEntries.forall: entry =>
          entry.odealer.isDefined && entry.pendingMessages.isEmpty

    while !drained && System.nanoTime() - deadline < 0 do
      Thread.sleep(pollIntervalMillis)

    drained

  /** Closes the ZMQ context, allowing buffered outbound data up to `linger` to flush.
   *
   *  Intended for short-lived processes on their way out; a long-running host keeps its network
   *  up for the life of the process. Call awaitPendingMessagesSent() first: this only bounds how
   *  long ZMQ may spend flushing what it already holds, so anything still sitting in ZMQNet's
   *  own queues when this is called is discarded rather than flushed.
   *
   *  Linger is set on each dealer individually because ZContext propagates its own linger
   *  setting to sockets only as it creates them and closes them without re-applying it, so
   *  context.setLinger() here would have no effect. Touching the sockets from the caller's
   *  thread races with the IO thread's use of them, which is acceptable only because the
   *  process is terminating.
   */
  def shutdown(linger: Duration): Unit =
    shuttingDown = true
    val lingerMillis = linger.toMillis.toInt
    connectedDealers.forEach: dealer =>
      try
        // jeromq reports an already-terminated context by returning false rather than throwing.
        // The socket then keeps the linger ZContext gave it at creation, which is zero, so the
        // flush window this method exists to provide is silently lost.
        if !dealer.setLinger(lingerMillis) then
          logger.debug("setLinger returned false during shutdown; dealer will not linger")
      catch
        case t: Throwable => logger.debug(s"Failed to set linger during shutdown: $t")
    context.close()

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

    // Once the context has been terminated the poll, recv and send calls below can all throw.
    // When shutdown() did the terminating that is expected rather than a failure, so unwind to
    // here and let the thread run off its normal end instead of dying with a stack trace on
    // stderr. Anything thrown while not shutting down propagates exactly as it did before.
    try
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
          // Guarded because shutdown() closing the context also makes poll() fail. That is not
          // a fault worth reporting, and a poller rebuilt on a dead context could not work
          // anyway, so leave that case to the handler below.
          case e: Throwable if !shuttingDown =>
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
          try
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
                connectedDealers.add(dealer)

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
          catch
            // One bad item must not take the thread down with it. Everything else depends on this
            // loop surviving: the rest of the queue, every other host's traffic, and all inbound
            // polling. Guarded on !shuttingDown for the same reason the poll above is -- a throw
            // from a context that shutdown() closed should unwind to the handler below and let the
            // thread run off its normal end, not be logged as a fault and retried.
            //
            // A NewHostAvailable that fails partway leaves a created socket that was never
            // registered in connectedDealers nor assigned to entry.odealer. It leaks until the
            // context closes and that host stays unsendable; the thread surviving is worth more.
            // Recorded in TODO.txt.
            case t: Throwable if !shuttingDown =>
              logger.error(s"Error processing send queue item: $t", t)

          qmsg = sendQueue.poll()
    catch
      case t: Throwable if shuttingDown =>
        logger.trace(s"ZMQNet.ioThread exiting during shutdown(): $t")

    logger.trace("ZMQNet.ioThread EXITING")
