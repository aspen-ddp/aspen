package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.network.snapshot.{HostSnapshot, StoreSnapshot}
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.*

/** MetadataManager.applySnapshot and cacheWasEmpty */
class MetadataManagerSnapshotSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  private val dropMargin = 10.minutes

  private val store0 = StoreId(unknownPoolId, 0.toByte)
  private val bootstrapStore = StoreId(poolId, 0.toByte)

  // Real milliseconds in the wall-time half, so dropMargin comparisons are meaningful
  private val T: Long = 1_000_000_000L
  private def at(ms: Long) = HLCTimestamp(ms << 16)
  private val minute: Long = 60 * 1000L

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  private def remote(ms: Long, address: String = "1.2.3.4") =
    CachedHost(remoteHostId, "remote_host", address, 1, 2, 3, at(ms))

  private def seeded(ms: Long) =
    new RecordingNetworkCache(NetworkCacheContent(
      List(CachedStore(store0, remoteHostId, at(ms))), List(remote(ms))))

  private def hostSnap(scanMs: Long, hosts: CachedHost*) = HostSnapshot(at(scanMs), hosts.toList)
  private def storeSnap(scanMs: Long, stores: CachedStore*) = StoreSnapshot(at(scanMs), stores.toList)

  test("unknown hosts and stores are loaded with no lookup"):
    val (mgr, client, _) = newManager(new RecordingNetworkCache())

    val result = mgr.applySnapshot(
      hostSnap(T, remote(T)),
      storeSnap(T, CachedStore(store0, remoteHostId, at(T))),
      dropMargin)

    result should be(empty)
    mgr.getHostEntryOrQueueMessage(store0, nudge()).map(_.address) should be(Some("1.2.3.4"))
    mgr.hostTimestamp(remoteHostId) should be(Some(at(T)))
    client.lookups should be(empty)
    client.poolLookups should be(empty)

  test("older snapshot entries do not replace newer held ones"):
    val cache = seeded(T)
    val (mgr, _, _) = newManager(cache)
    val held = mgr.peekHostEntry(remoteHostId).get

    val result = mgr.applySnapshot(
      hostSnap(T, remote(T - 1, "5.6.7.8")),
      storeSnap(T, CachedStore(store0, otherHostId, at(T - 1))),
      dropMargin)

    result should be(empty)
    mgr.peekHostEntry(remoteHostId).get should be theSameInstanceAs held
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("1.2.3.4"))
    mgr.peekStoreHost(store0) should be(Some(remoteHostId))
    mgr.hostTimestamp(remoteHostId) should be(Some(at(T)))

  test("a newer entry with a new endpoint replaces and returns the old one"):
    val cache = seeded(T)
    val (mgr, _, _) = newManager(cache)
    val old = mgr.peekHostEntry(remoteHostId).get

    val result = mgr.applySnapshot(
      hostSnap(T + 1, remote(T + 1, "5.6.7.8")),
      storeSnap(T + 1, CachedStore(store0, otherHostId, at(T + 1))),
      dropMargin)

    result.size should be(1)
    result.head should be theSameInstanceAs old
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("5.6.7.8"))
    mgr.hostTimestamp(remoteHostId) should be(Some(at(T + 1)))
    mgr.peekStoreHost(store0) should be(Some(otherHostId))
    cache.bulkUpdates.size should be(1)
    val (content, _, _) = cache.bulkUpdates.head
    content.hosts should be(List(remote(T + 1, "5.6.7.8")))
    content.stores should be(List(CachedStore(store0, otherHostId, at(T + 1))))

  test("a newer entry with the same endpoint only advances the timestamp"):
    val cache = seeded(T)
    val (mgr, _, _) = newManager(cache)
    val held = mgr.peekHostEntry(remoteHostId).get

    val result = mgr.applySnapshot(hostSnap(T + 1, remote(T + 1)), storeSnap(T + 1), dropMargin)

    result should be(empty)
    mgr.peekHostEntry(remoteHostId).get should be theSameInstanceAs held
    mgr.hostTimestamp(remoteHostId) should be(Some(at(T + 1)))
    cache.bulkUpdates.size should be(1)
    cache.bulkUpdates.head._1.hosts should be(List(remote(T + 1)))

  test("entries absent from the snapshot and older than the margin are dropped"):
    val cache = seeded(T - 11 * minute)
    val (mgr, _, _) = newManager(cache)
    val old = mgr.peekHostEntry(remoteHostId).get

    val result = mgr.applySnapshot(hostSnap(T), storeSnap(T), dropMargin)

    result.size should be(1)
    result.head should be theSameInstanceAs old
    mgr.peekHostEntry(remoteHostId) should be(None)
    mgr.peekStoreHost(store0) should be(None)
    // Kept, so late gossip cannot resurrect them
    mgr.hostTimestamp(remoteHostId) should be(Some(at(T - 11 * minute)))
    cache.removedHosts.toList should be(List(remoteHostId))
    cache.removedStores.toList should be(List(store0))

  test("entries within the margin are kept"):
    val cache = seeded(T - 9 * minute)
    val (mgr, _, _) = newManager(cache)

    val result = mgr.applySnapshot(hostSnap(T), storeSnap(T), dropMargin)

    result should be(empty)
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("1.2.3.4"))
    mgr.peekStoreHost(store0) should be(Some(remoteHostId))
    cache.removedHosts should be(empty)
    cache.removedStores should be(empty)

  test("each snapshot uses its own scanStart"):
    val cache = seeded(T - 11 * minute)
    val (mgr, _, _) = newManager(cache)

    // The host scan started late enough to drop; the store scan did not
    mgr.applySnapshot(hostSnap(T), storeSnap(T - 5 * minute), dropMargin).size should be(1)

    mgr.peekHostEntry(remoteHostId) should be(None)
    mgr.peekStoreHost(store0) should be(Some(remoteHostId))

  test("a dropped store is not resurrected by gossip at its old timestamp"):
    val (mgr, _, _) = newManager(seeded(T - 11 * minute))
    mgr.applySnapshot(hostSnap(T), storeSnap(T), dropMargin)
    mgr.peekStoreHost(store0) should be(None)

    mgr.storeMoved(store0, remoteHostId, at(T - 11 * minute))

    mgr.peekStoreHost(store0) should be(None)

  test("a later snapshot carrying a dropped host at its kept timestamp does not re-add it"):
    val (mgr, _, _) = newManager(seeded(T - 11 * minute))
    mgr.applySnapshot(hostSnap(T), storeSnap(T), dropMargin)
    mgr.peekHostEntry(remoteHostId) should be(None)

    val result = mgr.applySnapshot(
      hostSnap(T + minute, remote(T - 11 * minute)),
      storeSnap(T + minute, CachedStore(store0, remoteHostId, at(T - 11 * minute))),
      dropMargin)

    result should be(empty)
    mgr.peekHostEntry(remoteHostId) should be(None)
    mgr.peekStoreHost(store0) should be(None)

  test("bootstrap and self entries are never touched"):
    val cache = new RecordingNetworkCache()
    val (mgr, client, _) = newManager(cache, Some(otherHostId))
    val bootstrapEntry = mgr.peekHostEntry(bootstrapHostId).get

    // Resolve self through a lookup so it holds an old timestamp a snapshot could drop
    client.setLookupTimestamp(otherHostId, at(T - 30 * minute))
    mgr.getHostEntry(otherHostId)
    client.lookupPromise(otherHostId).success(otherHostState)
    val selfEntry = mgr.peekHostEntry(otherHostId).get

    // Names bootstrap with another address, omits self and the bootstrap store
    val r1 = mgr.applySnapshot(
      hostSnap(T, CachedHost(bootstrapHostId, "bootstrap_host", "9.9.9.9", 1, 2, 3, at(T))),
      storeSnap(T),
      dropMargin)
    // Names self with a newer address
    val r2 = mgr.applySnapshot(
      hostSnap(T, CachedHost(otherHostId, "other_host", "9.9.9.9", 1, 2, 3, at(T))),
      storeSnap(T),
      dropMargin)

    r1 should be(empty)
    r2 should be(empty)
    mgr.peekHostEntry(bootstrapHostId).get should be theSameInstanceAs bootstrapEntry
    mgr.hostTimestamp(bootstrapHostId) should be(None)
    mgr.peekStoreHost(bootstrapStore) should be(Some(bootstrapHostId))
    mgr.peekHostEntry(otherHostId).get should be theSameInstanceAs selfEntry
    mgr.hostTimestamp(otherHostId) should be(Some(at(T - 30 * minute)))
    cache.bulkUpdates.flatMap(_._1.hosts) should be(empty)
    cache.removedHosts should be(empty)
    cache.removedStores should be(empty)

  test("a pending host lookup resolves with the newer of snapshot and read"):
    val (mgr, client, impl) = newManager(new RecordingNetworkCache())
    client.setLookupTimestamp(remoteHostId, at(T))
    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge())

    val result = mgr.applySnapshot(hostSnap(T + 5, remote(T + 5, "5.6.7.8")), storeSnap(T + 5), dropMargin)

    result should be(empty)
    mgr.peekHostEntry(remoteHostId) should be(None) // still pending, the parked message intact
    client.lookupPromise(remoteHostId).success(remoteHostState)
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("5.6.7.8"))
    mgr.hostTimestamp(remoteHostId) should be(Some(at(T + 5)))
    impl.deliveredTo(remoteHostId).size should be(1)

  test("pending host lookups are never dropped"):
    val (mgr, client, _) = newManager(new RecordingNetworkCache())
    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge())

    mgr.applySnapshot(hostSnap(T), storeSnap(T), dropMargin) should be(empty)

    mgr.hasParkedMessages should be(true)
    client.lookupPromise(remoteHostId).success(remoteHostState)
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("10.0.0.9"))

  test("the empty bootstrap snapshot drops nothing"):
    val cache = seeded(T)
    val (mgr, _, _) = newManager(cache)

    val result = mgr.applySnapshot(HostSnapshot(HLCTimestamp.Zero, Nil), StoreSnapshot(HLCTimestamp.Zero, Nil),
      dropMargin)

    result should be(empty)
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("1.2.3.4"))
    mgr.peekStoreHost(store0) should be(Some(remoteHostId))
    cache.removedHosts should be(empty)
    cache.removedStores should be(empty)

  test("a failing createHostEntry leaves the host never-looked-up and the rest applied"):
    val (mgr, client, impl) = newManager(new RecordingNetworkCache())
    impl.throwOnCreateHostEntry(remoteHostId, new RuntimeException("boom"))

    val result = mgr.applySnapshot(
      hostSnap(T, remote(T), CachedHost(otherHostId, "other_host", "5.6.7.8", 1, 2, 3, at(T))),
      storeSnap(T, CachedStore(store0, remoteHostId, at(T))),
      dropMargin)

    result should be(empty)
    mgr.peekHostEntry(remoteHostId) should be(None)
    mgr.peekHostEntry(otherHostId).map(_.address) should be(Some("5.6.7.8"))
    mgr.peekStoreHost(store0) should be(Some(remoteHostId))
    mgr.getHostEntry(remoteHostId)
    client.lookups.toList should be(List(remoteHostId))

  test("a throwing cache does not undo an applied snapshot"):
    val cache = new RecordingNetworkCache()
    cache.throwOnWrite = true
    val (mgr, _, _) = newManager(cache)

    mgr.applySnapshot(hostSnap(T, remote(T)), storeSnap(T, CachedStore(store0, remoteHostId, at(T))), dropMargin)

    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("1.2.3.4"))
    mgr.peekStoreHost(store0) should be(Some(remoteHostId))
    cache.bulkUpdates.size should be(1)

  test("the cache is written after the manager's monitor is released"):
    val cache = new RecordingNetworkCache()
    val (mgr, _, _) = newManager(cache)
    @volatile var heldMonitor: Option[Boolean] = None
    cache.onBulkUpdate = () => heldMonitor = Some(Thread.holdsLock(mgr))

    mgr.applySnapshot(hostSnap(T, remote(T)), storeSnap(T, CachedStore(store0, remoteHostId, at(T))), dropMargin)

    heldMonitor should be(Some(false))
    cache.bulkUpdates.size should be(1)

  test("cacheWasEmpty"):
    newManager(NetworkCache.NoCache)._1.cacheWasEmpty should be(true)
    newManager(new RecordingNetworkCache())._1.cacheWasEmpty should be(true)
    newManager(seeded(T))._1.cacheWasEmpty should be(false)
