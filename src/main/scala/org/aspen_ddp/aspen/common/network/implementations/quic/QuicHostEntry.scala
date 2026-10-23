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
  * `knownWrong`.
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

  /** Set once this entry's address is known to belong to another host. Sends through it are dropped until a peer
    * answer replaces the entry (the replacement is a fresh entry, so the mark goes with this one).
    */
  @volatile var knownWrong: Boolean = false

  def setOnline(): Unit =
    if !isOnline then
      logger.info(s"Node $name is Online")
    isOnline = true

  def setOffline(): Unit =
    if isOnline then
      logger.info(s"Node $name is Offline")
    isOnline = false
