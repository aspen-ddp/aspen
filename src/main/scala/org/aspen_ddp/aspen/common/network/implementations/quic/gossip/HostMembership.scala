package org.aspen_ddp.aspen.common.network.implementations.quic.gossip

import io.scalecube.cluster.{ClusterImpl, ClusterMessageHandler}
import io.scalecube.cluster.membership.MembershipEvent
import io.scalecube.cluster.transport.api.{Message, TransportConfig, TransportFactory}
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.{ClusterTransportMessage, Codec, Gossip}
import org.aspen_ddp.aspen.common.util.DaemonThreads
import scribe.Logging

import java.util.UUID
import java.util.concurrent.{CopyOnWriteArrayList, Executors, RejectedExecutionException}
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Receives host membership changes and gossip. Every method defaults to a no-op.
  *
  * All calls for one HostMembership arrive in order on a single thread, so implementations
  * need no locking of their own but must not block for long.
  */
trait MembershipListener:
  def online(hostId: HostId): Unit = ()
  def offline(hostId: HostId): Unit = ()
  def restarted(hostId: HostId): Unit = ()
  def gossipReceived(g: Gossip): Unit = ()

object HostMembership:
  /** Scalecube gossip qualifier for Aspen Gossip messages. */
  val GossipQualifier = "aspen/gossip"

/** Host membership and gossip for one Aspen host, built on a scalecube-cluster `Cluster`.
  *
  * Scalecube is fully hidden here. Its traffic travels as ClusterTransportMessages through `sendFn`,
  * and the owner hands inbound ones to `receive`. Member added/removed events are turned into
  * per-host online/offline/restarted events by a MemberTracker.
  *
  * Tracker updates and listener calls run on one daemon thread owned by this instance, never on
  * scalecube's scheduler, so a slow listener cannot stall failure detection.
  */
final class HostMembership(localHost: HostId,
                           val bootId: UUID,
                           seeds: List[HostId],
                           config: GossipConfig,
                           sendFn: ClusterTransportMessage => Unit) extends Logging:
  import HostMembership.*

  private val transport = new QuicClusterTransport(localHost, bootId, sendFn)
  private val executor = Executors.newSingleThreadExecutor(DaemonThreads.factory("aspen-gossip"))
  private val listeners = new CopyOnWriteArrayList[MembershipListener]()
  private val isShutdown = new AtomicBoolean(false)

  // Only touched on `executor`.
  private val tracker = new MemberTracker

  // Snapshot of tracker.onlineHosts, republished on `executor` after every change.
  @volatile private var online: Set[HostId] = Set()

  private val handler = new ClusterMessageHandler:
    override def onMembershipEvent(e: MembershipEvent): Unit = membershipEvent(e)
    override def onGossip(m: Message): Unit = gossip(m)

  private val transportFactory: TransportFactory = (_: TransportConfig) => transport

  private val cluster = new ClusterImpl()
    .transportFactory(() => transportFactory)
    .failureDetector(fd => fd
      .pingInterval(config.pingInterval.toMillis.toInt)
      .pingTimeout(config.pingTimeout.toMillis.toInt))
    .gossip(g => g.gossipInterval(config.gossipInterval.toMillis))
    .membership(m => m
      .seedMembers(seeds.map(QuicClusterTransport.addressOf).asJava)
      .syncInterval(config.syncInterval.toMillis.toInt)
      .suspicionMult(config.suspicionMult))
    .handler(_ => handler)

  /** Starts joining the cluster in the background. Never blocks the caller. */
  def start(): Unit =
    cluster.start().subscribe(
      _ => logger.info(s"Host membership started for $localHost, boot id $bootId"),
      err => logger.error(s"Host membership failed to start for $localHost: $err"))

  /** Inbound scalecube traffic from another host. */
  def receive(m: ClusterTransportMessage): Unit = transport.receive(m)

  def addListener(l: MembershipListener): Unit = listeners.add(l)

  def isOnline(hostId: HostId): Boolean = online.contains(hostId)

  def onlineHosts: Set[HostId] = online

  /** Spreads `g` to every other host. Fire and forget; failures are logged. */
  def broadcast(g: Gossip): Unit = spread(Codec.encodeGossip(g))

  /** Test hook: spreads arbitrary bytes under the Aspen gossip qualifier. */
  private[gossip] def spreadRawGossip(bytes: Array[Byte]): Unit = spread(bytes)

  /** Stops the cluster and the event thread. Waits up to 5 seconds for scalecube to finish. */
  def shutdown(): Unit =
    if isShutdown.compareAndSet(false, true) then
      try
        cluster.shutdown()
        cluster.onShutdown().block(java.time.Duration.ofSeconds(5))
      catch
        case NonFatal(err) => logger.warn(s"Host membership shutdown for $localHost: $err")
      executor.shutdownNow()

  /** Test hook simulating a crash. Stops the transport first, so nothing more (in particular no LEAVING gossip)
    * reaches a peer, then shuts the cluster down in the background. Never blocks.
    */
  private[quic] def crashForTests(): Unit =
    if isShutdown.compareAndSet(false, true) then
      try
        // Completes synchronously: the transport's stop is a plain flag flip
        transport.stop().block()
        // Starts scalecube's shutdown without waiting for it; its LEAVING gossip dies in the stopped transport
        cluster.shutdown()
      catch
        case NonFatal(err) => logger.warn(s"Host membership crash for $localHost: $err")
      executor.shutdownNow()

  private def spread(bytes: Array[Byte]): Unit =
    try
      cluster.spreadGossip(Message.withData(bytes).qualifier(GossipQualifier).build()).subscribe(
        _ => (),
        err => logger.warn(s"Failed to spread gossip from $localHost: $err"))
    catch
      // spreadGossip throws if the cluster has not started yet
      case NonFatal(err) => logger.warn(s"Failed to spread gossip from $localHost: $err")

  /** Runs `task` on the event thread. Tasks submitted after shutdown are dropped. */
  private def onEventThread(task: => Unit): Unit =
    try
      executor.execute(() => task)
    catch
      case _: RejectedExecutionException => ()

  private def membershipEvent(e: MembershipEvent): Unit =
    QuicClusterTransport.hostIdOf(e.member().address()) match
      case Some(hostId) if hostId != localHost =>
        val memberId = e.member().id()
        if e.isAdded then
          onEventThread(publish(tracker.added(hostId, memberId)))
        else if e.isRemoved || e.isLeaving then
          onEventThread(publish(tracker.removed(hostId, memberId)))
      case _ => ()

  private def gossip(m: Message): Unit =
    if m.qualifier() == GossipQualifier then
      val data: Any = m.data()
      onEventThread:
        val decoded = data match
          case bytes: Array[Byte] => Codec.decodeGossip(bytes)
          case other => Left(s"Unexpected gossip payload: $other")
        decoded match
          case Right(g) => notifyListeners(_.gossipReceived(g))
          case Left(err) => logger.warn(s"Dropping undecodable gossip on $localHost: $err")

  private def publish(events: List[HostEvent]): Unit =
    if events.nonEmpty then
      online = tracker.onlineHosts
    events.foreach:
      case HostEvent.Online(h) => notifyListeners(_.online(h))
      case HostEvent.Offline(h) => notifyListeners(_.offline(h))
      case HostEvent.Restarted(h) => notifyListeners(_.restarted(h))

  private def notifyListeners(call: MembershipListener => Unit): Unit =
    listeners.asScala.foreach: l =>
      try call(l)
      catch
        case NonFatal(err) => logger.error(s"Membership listener failed on $localHost: $err")
