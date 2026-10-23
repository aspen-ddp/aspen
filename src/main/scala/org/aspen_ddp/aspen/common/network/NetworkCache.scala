package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.store.StoreId
import scribe.Logging

import java.util.UUID
import scala.util.control.NonFatal

final case class CachedStore(storeId: StoreId, hostId: HostId, timestamp: HLCTimestamp)

final case class CachedHost(hostId: HostId, name: String, address: String, dataPort: Int,
                            httpPort: Int, storeTransferPort: Int, timestamp: HLCTimestamp)

final case class NetworkCacheContent(stores: List[CachedStore], hosts: List[CachedHost])

/** Persistent, best-effort local cache of StoreId->HostId and HostId->endpoint mappings */
trait NetworkCache:
  def load(): NetworkCacheContent

  /** Unconditionally overwrites any held entry */
  def putStore(e: CachedStore): Unit

  def removeStore(storeId: StoreId): Unit

  /** Unconditionally overwrites any held entry */
  def putHost(e: CachedHost): Unit

  def removeHost(hostId: HostId): Unit

  /** Writes each entry only if it is newer than the one held (or none is held), then applies the removals
    * unconditionally. When `content` repeats a key the newest entry wins.
    */
  def bulkUpdate(content: NetworkCacheContent, removedStores: Iterable[StoreId] = Nil,
                 removedHosts: Iterable[HostId] = Nil): Unit

  def close(): Unit

object NetworkCache extends Logging:

  object NoCache extends NetworkCache:
    def load(): NetworkCacheContent = NetworkCacheContent(Nil, Nil)
    def putStore(e: CachedStore): Unit = ()
    def removeStore(storeId: StoreId): Unit = ()
    def putHost(e: CachedHost): Unit = ()
    def removeHost(hostId: HostId): Unit = ()
    def bulkUpdate(content: NetworkCacheContent, removedStores: Iterable[StoreId] = Nil,
                   removedHosts: Iterable[HostId] = Nil): Unit = ()
    def close(): Unit = ()

  /** RocksDB at `dir`, scoped to `systemId`; NoCache (with a logged warning) if it cannot be opened */
  def open(dir: os.Path, systemId: UUID): NetworkCache =
    try
      RocksDBNetworkCache(dir, systemId)
    catch
      case NonFatal(t) =>
        logger.warn(s"Unable to open network cache at $dir. Continuing without a cache: $t")
        NoCache

  def directoryFor(hostRoot: os.Path): os.Path = hostRoot / "cache" / "network"
