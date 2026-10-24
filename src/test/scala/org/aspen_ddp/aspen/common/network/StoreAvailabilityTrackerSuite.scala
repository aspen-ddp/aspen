package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.TestNetwork
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

class StoreAvailabilityTrackerSuite extends AnyFunSuite with Matchers:

  private val poolId = PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
  private val storeA = StoreId(poolId, 0.toByte)
  private val storeB = StoreId(poolId, 1.toByte)
  private val hostId = HostId(UUID.fromString("22222222-2222-2222-2222-222222222222"))

  /** A timestamp `ago` before now, so it survives pruning unless `ago` is past OnlineRetention */
  private def ts(ago: FiniteDuration = Duration.Zero, logical: Int = 0): HLCTimestamp =
    HLCTimestamp(((System.currentTimeMillis() - ago.toMillis) << 16) | logical)

  private def offline(storeId: StoreId, t: HLCTimestamp): StoreOffline = StoreOffline(storeId, t)

  private def moved(storeId: StoreId, online: HLCTimestamp): StoreMoved =
    StoreMoved(storeId, hostId, ts(1.hour), online)

  private def cacheFile: os.Path = os.temp.dir() / "cache" / StoreAvailabilityTracker.CacheFilename

  test("StoreOffline marks a store offline and a newer StoreMoved clears it"):
    val t = new StoreAvailabilityTracker()
    t.gossipReceived(offline(storeA, ts(10.minutes)))
    t.isOffline(storeA) shouldBe true
    t.isOffline(storeB) shouldBe false
    t.offlineStores shouldBe Set(storeA)
    t.gossipReceived(moved(storeA, ts(5.minutes)))
    t.isOffline(storeA) shouldBe false
    t.offlineStores shouldBe empty

  test("a StoreOffline older than the store's last StoreMoved is ignored"):
    val t = new StoreAvailabilityTracker()
    t.gossipReceived(offline(storeA, ts(10.minutes)))
    t.gossipReceived(moved(storeA, ts(5.minutes)))
    // A retransmit of the original StoreOffline
    t.gossipReceived(offline(storeA, ts(10.minutes)))
    t.isOffline(storeA) shouldBe false

  test("a StoreMoved older than the store's StoreOffline does not clear it"):
    val t = new StoreAvailabilityTracker()
    t.gossipReceived(moved(storeA, ts(10.minutes)))
    t.gossipReceived(offline(storeA, ts(5.minutes)))
    t.isOffline(storeA) shouldBe true
    t.gossipReceived(moved(storeA, ts(10.minutes)))
    t.isOffline(storeA) shouldBe true

  test("a message as old as what is held is ignored"):
    val t = new StoreAvailabilityTracker()
    val same = ts(5.minutes)
    t.gossipReceived(offline(storeA, same))
    t.update(storeA, StoreAvailability(offline = false, same)) shouldBe false
    t.isOffline(storeA) shouldBe true

  test("HostEndpointChanged is ignored"):
    val t = new StoreAvailabilityTracker()
    t.gossipReceived(HostEndpointChanged(hostId, "10.1.2.3", 1, 2, 3, ts()))
    t.snapshot shouldBe empty

  test("merge applies each entry of a snapshot by its timestamp"):
    val t = new StoreAvailabilityTracker()
    t.gossipReceived(moved(storeA, ts(5.minutes)))
    t.merge(Map(
      storeA -> StoreAvailability(offline = true, ts(10.minutes)),
      storeB -> StoreAvailability(offline = true, ts(10.minutes))))
    t.offlineStores shouldBe Set(storeB)

  test("state survives a restart through the cache file"):
    val file = cacheFile
    val t = new StoreAvailabilityTracker(Some(file))
    t.gossipReceived(offline(storeA, ts(10.minutes, logical = 3)))
    t.gossipReceived(offline(storeB, ts(10.minutes)))
    t.gossipReceived(moved(storeB, ts(5.minutes)))
    os.exists(file) shouldBe true

    val restored = new StoreAvailabilityTracker(Some(file))
    restored.snapshot shouldBe t.snapshot
    restored.offlineStores shouldBe Set(storeA)
    // The remembered online entry still beats a stale retransmit after the restart
    restored.gossipReceived(offline(storeB, ts(10.minutes)))
    restored.isOffline(storeB) shouldBe false

  test("an unreadable cache file is discarded"):
    val file = cacheFile
    os.write(file, "stores: [ {store: nonsense} ]", createFolders = true)
    new StoreAvailabilityTracker(Some(file)).snapshot shouldBe empty

  test("online entries past OnlineRetention are forgotten, offline ones are kept"):
    val file = cacheFile
    val old = StoreAvailabilityTracker.OnlineRetention + 1.hour
    os.write(file, StoreAvailabilityTracker.encode(Map(
      storeA -> StoreAvailability(offline = true, ts(old)),
      storeB -> StoreAvailability(offline = false, ts(old)))), createFolders = true)
    new StoreAvailabilityTracker(Some(file)).snapshot.keySet shouldBe Set(storeA)

  // ---- recheck -------------------------------------------------------------------------------------------------

  private val deviceA = StorageDeviceId(UUID.randomUUID())
  private val deviceB = StorageDeviceId(UUID.randomUUID())
  private val setId = StorageDeviceSetId(UUID.randomUUID())

  private def device(id: StorageDeviceId, storeId: StoreId, status: StorageDeviceState.StoreStatus) =
    StorageDeviceState(id, hostId, 0L, 1024L, Map(storeId -> StorageDeviceState.StoreEntry(status, None)), setId)

  /** Serves one pool, whose two stores live on deviceA and deviceB, and the given device states */
  private class StubClient(devices: Map[StorageDeviceId, StorageDeviceState]) extends TestNetwork.TClient(
      ExecutionContext.global, ClientMessenger.None, Radicle.pointer, Replication(1, 1)):

    override def getStoragePoolState(poolId: PoolId): Future[StoragePoolState] =
      Future.successful(StoragePoolState(poolId, "pool", Replication(2, 2), None,
        Array(StoragePoolState.StoreEntry(hostId, deviceA), StoragePoolState.StoreEntry(hostId, deviceB)),
        RocksDBConfig(), setId))

    override def getStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
      devices.get(storageDeviceId) match
        case Some(d) => Future.successful(d)
        case None => Future.failed(new NoSuchElementException(s"no device $storageDeviceId"))

  test("recheck marks online a store its pool's device lists as Active, and keeps the rest offline"):
    val t = new StoreAvailabilityTracker()
    t.gossipReceived(offline(storeA, ts(10.minutes)))
    t.gossipReceived(offline(storeB, ts(10.minutes)))
    val client = new StubClient(Map(
      deviceA -> device(deviceA, storeA, StorageDeviceState.StoreStatus.Active),
      deviceB -> device(deviceB, storeB, StorageDeviceState.StoreStatus.Rebuilding)))
    Await.result(t.recheck(client), 5.seconds)
    t.offlineStores shouldBe Set(storeB)

  test("recheck keeps a store offline when its reads fail"):
    val t = new StoreAvailabilityTracker()
    t.gossipReceived(offline(storeA, ts(10.minutes)))
    Await.result(t.recheck(new StubClient(Map())), 5.seconds)
    t.offlineStores shouldBe Set(storeA)
