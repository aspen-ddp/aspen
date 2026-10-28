package org.aspen_ddp.aspen.common.network.implementations.quic

import org.aspen_ddp.aspen.{TestNetwork, TestTLS}
import org.aspen_ddp.aspen.client.{DataObjectState, KeyValueObjectState}
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StoragePoolState}
import org.aspen_ddp.aspen.common.config.BootstrapConfig
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.network.implementations.quic.gossip.GossipConfig
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyOrdering, KeyValueObjectPointer, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.store.StoreId

import java.net.{DatagramSocket, InetAddress, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, LinkedBlockingQueue, TimeUnit}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

/** Two host-mode `QuicNet`s on loopback sharing a bootstrap config, plus any number of client-mode nets.
  *
  * Host A owns bootstrap stores 0 and 1, host B owns 2 and 3. Every net gets fresh credentials from `TestTLS` (a
  * host's signed for its HostId, a client's for a random UUID) and a `RecordingHandler`. `close()` tears down every
  * net it created, including restarted and extra ones.
  *
  * Every host-mode net looks hosts up in `directory`, as a real host reads host objects through its `AspenClient`.
  * It holds A and B; a test adding a host, or moving one, puts its state there. Client-mode nets get no lookup client.
  */
final class QuicNetFixture(val config: QuicConfig = QuicConfig(gossip = GossipConfig.ForTests),
                           val heartbeatPeriod: Duration = Duration(500, MILLISECONDS)):
  import QuicNetFixture.*

  val hostIdA: HostId = HostId(UUID.randomUUID())
  val hostIdB: HostId = HostId(UUID.randomUUID())
  val portA: Int = freeUdpPort()
  val portB: Int = freeUdpPort()

  val storesA: List[StoreId] = List(StoreId(PoolId.BootstrapPoolId, 0), StoreId(PoolId.BootstrapPoolId, 1))
  val storesB: List[StoreId] = List(StoreId(PoolId.BootstrapPoolId, 2), StoreId(PoolId.BootstrapPoolId, 3))

  val bootstrap: BootstrapConfig.Config = BootstrapConfig.Config(
    UUID.randomUUID(),
    Replication(4, 3),
    List(
      BootstrapConfig.BootstrapHost(hostIdA, "host_a", "127.0.0.1", portA, 0, 0, storesA),
      BootstrapConfig.BootstrapHost(hostIdB, "host_b", "127.0.0.1", portB, 0, 0, storesB)),
    TestTLS.caCertificatePem)

  /** The host objects host-mode nets resolve hosts from */
  val directory: HostDirectory = new HostDirectory
  directory.put(HostState(hostIdA, "host_a", "127.0.0.1", portA, 0, 0, Set()))
  directory.put(HostState(hostIdB, "host_b", "127.0.0.1", portB, 0, 0, Set()))

  private val nets = ArrayBuffer.empty[QuicNet]

  var a: Node = host(hostIdA, portA)
  var b: Node = host(hostIdB, portB)

  def host(hostId: HostId, port: Int, cfg: QuicConfig = config,
           networkCache: NetworkCache = NetworkCache.NoCache): Node =
    node(None, Some((hostId, port)), cfg, credentials(hostId.uuid), networkCache = networkCache)

  def client(cfg: QuicConfig = config, creds: TlsCredentials = credentials(),
             resolve: String => InetAddress = QuicNet.blockingResolve,
             networkCache: NetworkCache = NetworkCache.NoCache): Node =
    node(Some(ClientId(UUID.randomUUID())), None, cfg, creds, resolve, networkCache)

  private def node(oclientId: Option[ClientId], ohost: Option[(HostId, Int)], cfg: QuicConfig,
                   creds: TlsCredentials, resolve: String => InetAddress = QuicNet.blockingResolve,
                   networkCache: NetworkCache = NetworkCache.NoCache): Node =
    val handler = new RecordingHandler
    val net = new QuicNet(bootstrap, (_, _) => (), oclientId, ohost, heartbeatPeriod, handler, creds, cfg, resolve,
      networkCache = networkCache)
    if ohost.isDefined then
      net.startIoThread(new DirectoryClient(directory))
    synchronized:
      nets += net
    Node(net, handler)

  /** Shuts down B and starts a fresh B on the same port */
  def restartB(): Unit =
    stopB()
    startB()

  /** Shuts down B. `b` keeps referring to the stopped net until `startB`. */
  def stopB(): Unit = b.net.closeForTests()

  /** Stops B as a crash would: no QUIC close frame reaches A. `b` keeps referring to the stopped net until `startB`. */
  def crashB(): Unit = b.net.crashForTests()

  /** Starts a fresh B on B's port */
  def startB(): Unit = b = host(hostIdB, portB)

  def close(): Unit =
    val all = synchronized:
      val l = nets.toList
      nets.clear()
      l
    all.foreach: n =>
      try n.closeForTests()
      catch case _: Throwable => ()

object QuicNetFixture:
  /** A single-section message addressed to a store, carrying `size` bytes of seeded data */
  def rebuild(to: StoreId, size: Int, seed: Int = 0): OpportunisticRebuild =
    rebuildOf(to, Array.tabulate[Byte](size)(i => (i * 31 + seed).toByte))

  /** A single-section message addressed to a store, carrying exactly `payload` */
  def rebuildOf(to: StoreId, payload: Array[Byte]): OpportunisticRebuild =
    val ptr = DataObjectPointer(ObjectId(UUID.randomUUID()), PoolId.BootstrapPoolId, Array[Byte](1))
    OpportunisticRebuild(to, ClientId(UUID.randomUUID()), ptr, ObjectRevision(TransactionId(UUID.randomUUID())),
      ObjectRefcount(1, 1), HLCTimestamp(1000L), DataBuffer(payload))

  /** Upper bound for waits; stretched on a loaded machine (see `LoadProbe`), and free when the wait succeeds */
  def AwaitMillis: Long = LoadProbe.scaleMillis(10000)

  /** Credentials for `id`. A host's must be for its HostId, which outbound connections to it verify. */
  def credentials(id: UUID = UUID.randomUUID()): TlsCredentials =
    val (cert, key) = TestTLS.credentials(id)
    TlsCredentials.fromPem(TestTLS.caCertificatePem, cert, key)

  /** Ports for test QuicNets come from 10000-19999: below Linux's ephemeral range (32768-60999), which every QuicNet's
    * client channels and the OS draw from, and clear of `QuicEndToEndSuite`'s 20000-31999. A JVM-wide counter hands
    * each port out once per JVM; the random start keeps concurrent JVMs apart.
    */
  private val PortBase = 10000
  private val PortRange = 10000
  private val nextPort = new java.util.concurrent.atomic.AtomicInteger(scala.util.Random.nextInt(PortRange))

  /** The next port from the test range that a plain UDP socket (no `SO_REUSEPORT`) can bind, skipping busy ones */
  def freeUdpPort(): Int =
    var port = -1
    while port < 0 do
      val candidate = PortBase + Math.floorMod(nextPort.getAndIncrement(), PortRange)
      try
        val s = new DatagramSocket(new InetSocketAddress(candidate))
        s.close()
        port = candidate
      catch case _: java.io.IOException => ()
    port

  final class Node(val net: QuicNet, val handler: RecordingHandler)

  /** Host objects by host id, and every lookup made against them. It holds no pools: every pool lookup is recorded
    * in `poolLookups` and fails.
    */
  final class HostDirectory:
    private val states = new ConcurrentHashMap[HostId, HostState]()
    val lookups = new ConcurrentLinkedQueue[HostId]()
    val poolLookups = new ConcurrentLinkedQueue[PoolId]()

    def put(state: HostState): Unit = states.put(state.hostId, state)

    def remove(hostId: HostId): Unit = states.remove(hostId)

    def lookup(hostId: HostId): Option[HostState] =
      lookups.add(hostId)
      Option(states.get(hostId))

  /** An `AspenClient` serving only host lookups, from `directory`. Inherits the rest from `TestNetwork.TClient`; no
    * read or transaction ever leaves it. A bootstrap refresh and every object read fail at once: there is nothing
    * to read.
    */
  final class DirectoryClient(directory: HostDirectory) extends TestNetwork.TClient(
      ExecutionContext.global, ClientMessenger.None, Radicle.pointer, Replication(1, 1)):

    // The directory has no transactions, so every host object reads as written at time zero
    override def getHostStateAndTimestamp(hostId: HostId): Future[(HostState, HLCTimestamp)] =
      directory.lookup(hostId) match
        case Some(state) => Future.successful((state, HLCTimestamp.Zero))
        case None => Future.failed(new NoSuchElementException(s"no host object for $hostId"))

    override def getStoragePoolStateAndTimestamp(poolId: PoolId): Future[(StoragePoolState, HLCTimestamp)] =
      directory.poolLookups.add(poolId)
      Future.failed(new NoSuchElementException(s"no pool object for $poolId"))

    override def getBootstrapConfig(): Future[String] =
      Future.failed(new UnsupportedOperationException("QuicNetFixture has no bootstrap config to refetch"))

    // No object reads either, so the snapshot reader startIoThread starts fails at once rather than hanging
    override def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
      Future.failed(new UnsupportedOperationException("QuicNetFixture has no objects to read"))

    override def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
      Future.failed(new UnsupportedOperationException("QuicNetFixture has no objects to read"))

    override def readKeys(pointer: KeyValueObjectPointer, keys: List[Key], comment: String): Future[KeyValueObjectState] =
      Future.failed(new UnsupportedOperationException("QuicNetFixture has no objects to read"))

    override def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering,
                              comment: String): Future[KeyValueObjectState] =
      Future.failed(new UnsupportedOperationException("QuicNetFixture has no objects to read"))

  final case class Delivery[M](message: M, threadName: String)

  /** A queue per `MessageHandler` callback, recording the delivering thread's name */
  final class RecordingHandler extends MessageHandler:
    val clientResponses = new LinkedBlockingQueue[Delivery[ClientResponse]]()
    val clientRequests = new LinkedBlockingQueue[Delivery[ClientRequest]]()
    val txMessages = new LinkedBlockingQueue[Delivery[TxMessage]]()
    val hostMessages = new LinkedBlockingQueue[Delivery[HostMessage]]()

    private def name = Thread.currentThread().getName

    def onClientResponseReceived(msg: ClientResponse): Unit = clientResponses.add(Delivery(msg, name))
    def onClientRequestReceived(msg: ClientRequest): Unit = clientRequests.add(Delivery(msg, name))
    def onTransactionMessageReceived(msg: TxMessage): Unit = txMessages.add(Delivery(msg, name))
    def onHostMessageReceived(msg: HostMessage): Unit = hostMessages.add(Delivery(msg, name))

    def total: Int = clientResponses.size + clientRequests.size + txMessages.size + hostMessages.size

  /** Takes exactly `n` deliveries from `q`, failing if they do not all arrive within `timeoutMillis` */
  def await[M](q: LinkedBlockingQueue[Delivery[M]], n: Int, timeoutMillis: Long = AwaitMillis): List[Delivery[M]] =
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
    val out = ArrayBuffer.empty[Delivery[M]]
    while out.size < n do
      val left = deadline - System.nanoTime()
      val d = if left > 0 then q.poll(left, TimeUnit.NANOSECONDS) else null
      if d == null then
        throw new AssertionError(s"received ${out.size} of $n messages within ${timeoutMillis}ms")
      out += d
    out.toList

  /** Polls `cond` every 10ms until it holds, failing after `timeoutMillis` */
  def eventually(what: String, timeoutMillis: Long = AwaitMillis)(cond: => Boolean): Unit =
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
    while !cond do
      if System.nanoTime() - deadline > 0 then
        throw new AssertionError(s"timed out after ${timeoutMillis}ms waiting for: $what")
      Thread.sleep(10)
