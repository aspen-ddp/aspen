package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** MetadataManager seeding from, and writing through to, a NetworkCache */
class MetadataManagerNetworkCacheSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  private val store0 = StoreId(unknownPoolId, 0.toByte)
  private def ts(v: Long) = HLCTimestamp(v)

  private def cachedRemote(t: Long, address: String = "1.2.3.4") =
    CachedHost(remoteHostId, "remote_host", address, 1, 2, 3, ts(t))

  private def changed(t: Long, address: String = "10.9.9.9") =
    HostEndpointChanged(remoteHostId, address, 9000, 9001, 9002, ts(t))

  private def seeded(t: Long = 5) =
    new RecordingNetworkCache(NetworkCacheContent(
      List(CachedStore(store0, remoteHostId, ts(t))), List(cachedRemote(t))))

  test("cached store and host are used with no lookup"):
    val (mgr, client, _) = newManager(seeded())

    val e = mgr.getHostEntryOrQueueMessage(store0, nudge())

    e.map(_.address) should be(Some("1.2.3.4"))
    client.lookups should be(empty)
    client.poolLookups should be(empty)

  test("gossip older than a seeded entry is ignored, newer is applied"):
    val (mgr, _, _) = newManager(seeded())

    mgr.storeMoved(store0, otherHostId, ts(4))
    mgr.peekStoreHost(store0) should be(Some(remoteHostId))
    mgr.storeMoved(store0, otherHostId, ts(6))
    mgr.peekStoreHost(store0) should be(Some(otherHostId))

    mgr.endpointChanged(changed(4))
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("1.2.3.4"))
    mgr.endpointChanged(changed(6))
    mgr.peekHostEntry(remoteHostId).map(_.address) should be(Some("10.9.9.9"))

  test("cached entries for bootstrap stores and hosts are ignored"):
    val bootstrapStore = StoreId(poolId, 0.toByte)
    val cache = new RecordingNetworkCache(NetworkCacheContent(
      List(CachedStore(bootstrapStore, remoteHostId, ts(5))),
      List(CachedHost(bootstrapHostId, "x", "9.9.9.9", 1, 2, 3, ts(5)))))
    val (mgr, _, _) = newManager(cache)

    mgr.peekStoreHost(bootstrapStore) should be(Some(bootstrapHostId))
    mgr.peekHostEntry(bootstrapHostId).map(_.address) should be(Some("127.0.0.1"))
    mgr.hostTimestamp(bootstrapHostId) should be(None)

  test("self host entry is neither seeded nor written"):
    val cache = new RecordingNetworkCache(NetworkCacheContent(Nil, List(cachedRemote(5))))
    val (mgr, client, _) = newManager(cache, Some(remoteHostId))

    mgr.peekHostEntry(remoteHostId) should be(None)
    mgr.getHostEntry(remoteHostId)
    client.lookupPromise(remoteHostId).success(remoteHostState)

    mgr.peekHostEntry(remoteHostId) should not be None
    cache.hostPuts should be(empty)

  test("write-through: storeMoved"):
    val cache = new RecordingNetworkCache()
    val (mgr, _, _) = newManager(cache)
    mgr.storeMoved(store0, remoteHostId, ts(7))
    cache.storePuts.toList should be(List(CachedStore(store0, remoteHostId, ts(7))))
    mgr.storeMoved(store0, otherHostId, ts(3)) // ignored
    cache.storePuts.size should be(1)

  test("write-through: endpointChanged"):
    val cache = new RecordingNetworkCache(NetworkCacheContent(Nil, List(cachedRemote(5))))
    val (mgr, _, _) = newManager(cache)
    mgr.endpointChanged(changed(4))
    cache.hostPuts should be(empty)
    mgr.endpointChanged(changed(6))
    cache.hostPuts.toList should be(List(CachedHost(remoteHostId, "remote_host", "10.9.9.9", 9000, 9001, 9002, ts(6))))

  test("write-through: pool lookup puts every store in the pool"):
    val cache = new RecordingNetworkCache()
    val (mgr, client, _) = newManager(cache)
    client.setPoolLookupTimestamp(unknownPoolId, ts(8))
    mgr.getHostEntryOrQueueMessage(store0, nudge())
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, remoteHostId, otherHostId))

    cache.storePuts.toList should be(List(
      CachedStore(store0, remoteHostId, ts(8)),
      CachedStore(StoreId(unknownPoolId, 1.toByte), otherHostId, ts(8))))

  test("write-through: host lookup uses the read timestamp, or newer gossip's"):
    val cache = new RecordingNetworkCache()
    val (mgr, client, _) = newManager(cache)
    client.setLookupTimestamp(remoteHostId, ts(5))
    mgr.getHostEntry(remoteHostId)
    client.lookupPromise(remoteHostId).success(remoteHostState)
    cache.hostPuts.toList.map(h => (h.hostId, h.timestamp, h.address)) should be(
      List((remoteHostId, ts(5), "10.0.0.9")))

    val cache2 = new RecordingNetworkCache()
    val (mgr2, client2, _) = newManager(cache2)
    client2.setLookupTimestamp(remoteHostId, ts(5))
    mgr2.getHostEntry(remoteHostId)
    mgr2.endpointChanged(changed(9))
    client2.lookupPromise(remoteHostId).success(remoteHostState)
    cache2.hostPuts.toList.map(h => (h.timestamp, h.address)) should be(List((ts(9), "10.9.9.9")))

  test("write-through: dropStoreMapping and receivedUnknownStoreFromHost remove; bootstrap does not"):
    val cache = new RecordingNetworkCache()
    val (mgr, _, _) = newManager(cache)
    mgr.storeMoved(store0, remoteHostId, ts(7))
    mgr.dropStoreMapping(store0)
    cache.storeRemoves.toList should be(List(store0))

    val store1 = StoreId(unknownPoolId, 1.toByte)
    mgr.storeMoved(store1, remoteHostId, ts(7))
    mgr.receivedUnknownStoreFromHost(otherHostId, store1) // different host: no removal
    cache.storeRemoves.size should be(1)
    mgr.receivedUnknownStoreFromHost(remoteHostId, store1)
    cache.storeRemoves.toList should be(List(store0, store1))

    val before = cache.writeCount
    mgr.dropStoreMapping(StoreId(poolId, 0.toByte)) // bootstrap refresh
    cache.writeCount should be(before)

  test("a throwing cache never breaks a send"):
    val cache = new RecordingNetworkCache()
    cache.throwOnWrite = true
    val (mgr, client, impl) = newManager(cache)
    mgr.getHostEntryOrQueueMessage(store0, nudge())
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, bootstrapHostId))

    impl.deliveredTo(bootstrapHostId).size should be(1)
    cache.storePuts should not be empty
    mgr.hasParkedMessages should be(false)

  test("endpointAnswer"):
    val (mgr, client, _) = newManager(seeded())

    mgr.endpointAnswer(remoteHostId) should be(Some(
      HostEndpointChanged(remoteHostId, "1.2.3.4", 1, 2, 3, ts(5))))
    mgr.endpointAnswer(bootstrapHostId) should be(None)
    mgr.endpointAnswer(otherHostId) should be(None)
    client.lookups should be(empty)

    mgr.getHostEntry(otherHostId) // pending
    mgr.endpointAnswer(otherHostId) should be(None)
    client.lookups.toList should be(List(otherHostId))
