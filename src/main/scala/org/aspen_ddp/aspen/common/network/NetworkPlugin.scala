package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger

import scala.concurrent.duration.Duration

/** The seam between Aspen and a pluggable network implementation. */
trait NetworkPlugin:

  /** Identity this process presents to hosts when acting as a client. */
  def clientId: ClientId

  /** Messenger used by client code to send requests to hosts. */
  def clientMessenger: ClientMessenger

  /** Messenger used by server code to send responses and transaction messages. */
  def serverMessenger: ServerMessenger

  /** Starts the plugin's network IO and gives it the AspenClient it uses for metadata lookups.
   *
   *  The IO threads must be daemon threads so that client processes exit once their work is
   *  done. Long-running processes keep themselves alive by blocking in joinIoThread().
   */
  def startIoThread(client: AspenClient): Unit

  /** True if `storeId` is believed offline for a transfer or rebuild, and so a poor choice of designated leader.
   *  Advisory: see StoreAvailabilityTracker. Networks without gossip know of no offline stores.
   */
  def isStoreOffline(storeId: StoreId): Boolean = false

  /** Blocks until the plugin's network IO has terminated. */
  def joinIoThread(): Unit

  /** Blocks until the plugin is holding no outbound messages, or `timeout` elapses. Returns
   *  true if it drained.
   *
   *  Callers cannot name the hosts involved, so the predicate covers every place a message may
   *  be waiting, including messages parked while a host or pool address is resolved.
   *
   *  This is not a delivery guarantee: the peer may be down and an implementation may have
   *  small windows where a message is in transit between its internal queues. It establishes
   *  only that the plugin is no longer holding the message, which is what a short-lived process
   *  would otherwise abandon on exit.
   *
   *  A false return is not a command failure. It means only that the plugin may still be
   *  holding something; callers should report degraded latency rather than an error, since the
   *  receiving host's periodic polling remains the correctness guarantee.
   *
   *  A short-lived process should follow this with shutdown() before exiting.
   */
  def awaitPendingMessagesSent(timeout: Duration): Boolean

  /** Shuts the network down, allowing already-accepted outbound data up to `linger` to reach
   *  the wire.
   *
   *  Intended for short-lived processes on their way out; a long-running host keeps its network
   *  up for the life of the process. Call awaitPendingMessagesSent() first: `linger` only
   *  bounds how long the transport may spend flushing what it already holds, so anything still
   *  sitting in the plugin's own queues when this is called is discarded rather than flushed.
   */
  def shutdown(linger: Duration): Unit
