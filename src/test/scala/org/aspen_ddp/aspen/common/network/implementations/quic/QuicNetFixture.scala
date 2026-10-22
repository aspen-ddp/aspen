package org.aspen_ddp.aspen.common.network.implementations.quic

import org.aspen_ddp.aspen.TestTLS
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.net.{DatagramSocket, InetAddress, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{Duration, MILLISECONDS}

/** Two host-mode `QuicNet`s on loopback sharing a bootstrap config, plus any number of client-mode nets.
  *
  * Host A owns bootstrap stores 0 and 1, host B owns 2 and 3. Every net gets fresh credentials from `TestTLS` and a
  * `RecordingHandler`. `close()` tears down every net it created, including restarted and extra ones.
  */
final class QuicNetFixture(val config: QuicConfig = QuicConfig(),
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

  private val nets = ArrayBuffer.empty[QuicNet]

  var a: Node = host(hostIdA, portA)
  var b: Node = host(hostIdB, portB)

  def host(hostId: HostId, port: Int, cfg: QuicConfig = config): Node =
    node(None, Some((hostId, port)), cfg, credentials())

  def client(cfg: QuicConfig = config, creds: TlsCredentials = credentials()): Node =
    node(Some(ClientId(UUID.randomUUID())), None, cfg, creds)

  private def node(oclientId: Option[ClientId], ohost: Option[(HostId, Int)], cfg: QuicConfig,
                   creds: TlsCredentials): Node =
    val handler = new RecordingHandler
    val net = new QuicNet(bootstrap, (_, _) => (), oclientId, ohost, heartbeatPeriod, handler, creds, cfg)
    synchronized:
      nets += net
    Node(net, handler)

  /** Shuts down B and starts a fresh B on the same port */
  def restartB(): Unit =
    b.net.closeForTests()
    b = host(hostIdB, portB)

  def close(): Unit =
    val all = synchronized:
      val l = nets.toList
      nets.clear()
      l
    all.foreach: n =>
      try n.closeForTests()
      catch case _: Throwable => ()

object QuicNetFixture:
  val AwaitMillis: Long = 10000

  def credentials(): TlsCredentials =
    val (cert, key) = TestTLS.credentials(UUID.randomUUID())
    TlsCredentials.fromPem(TestTLS.caCertificatePem, cert, key)

  def freeUdpPort(): Int =
    val s = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    try s.getLocalPort
    finally s.close()

  final class Node(val net: QuicNet, val handler: RecordingHandler)

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
