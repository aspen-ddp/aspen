package org.aspen_ddp.aspen.common.network.implementations.quic

import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.MetadataManager
import scribe.Logging

import java.util.concurrent.atomic.AtomicReference

/** `QuicNet`'s host entry.
  *
  * `connection` is the current outbound connection to this host, or null. Senders install a new one with a CAS
  * when it is null or closed, so exactly one thread dials; a connection clears itself (CAS from itself to null)
  * when it closes. `isOnline` is driven by host membership events and read from other threads, hence volatile, as is
  * the known-wrong mark.
  */
class QuicHostEntry(hostId: HostId,
                    name: String,
                    address: String,
                    dataPort: Int,
                    httpPort: Int,
                    storeTransferPort: Int)
  extends MetadataManager.HostEntry(hostId, name, address, dataPort, httpPort, storeTransferPort) with Logging:

  val connection: AtomicReference[QuicConnection | Null] = new AtomicReference[QuicConnection | Null](null)
  @volatile var isOnline: Boolean = false

  /** When (System.nanoTime) this entry's address was found to belong to another host, while it is marked. Sends
    * through a marked entry are dropped until a peer answer replaces the entry (the replacement is a fresh entry, so
    * the mark goes with this one) or `QuicNet`'s scan clears the mark after `knownWrongRetryAfter`.
    */
  @volatile private var markedWrongAt: Option[Long] = None

  def knownWrong: Boolean = markedWrongAt.isDefined

  /** When the current known-wrong mark was set, if the entry is marked */
  def knownWrongSince: Option[Long] = markedWrongAt

  def markKnownWrong(): Unit = markedWrongAt = Some(System.nanoTime())

  def clearKnownWrong(): Unit = markedWrongAt = None

  def setOnline(): Unit =
    if !isOnline then
      logger.info(s"Node $name is Online")
    isOnline = true

  def setOffline(): Unit =
    if isOnline then
      logger.info(s"Node $name is Offline")
    isOnline = false
