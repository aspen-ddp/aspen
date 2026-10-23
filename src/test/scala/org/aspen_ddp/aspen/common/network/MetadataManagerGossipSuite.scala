package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** Coverage for StoreMoved and HostEndpointChanged gossip: applied when newer than what the manager holds, ignored
 *  otherwise, and never the cause of a read.
 */
class MetadataManagerGossipSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  private val store0 = StoreId(unknownPoolId, 0.toByte)

  private def changed(timestamp: Long, dataPort: Int = 9000): HostEndpointChanged =
    HostEndpointChanged(remoteHostId, "10.9.9.9", dataPort, 9001, 9002, HLCTimestamp(timestamp))

  /** A manager holding a resolved entry for remoteHostId, read at `readTimestamp` */
  private def withResolvedRemote(readTimestamp: Long) =
    val (mgr, client, impl) = newManager()
    client.setLookupTimestamp(remoteHostId, HLCTimestamp(readTimestamp))
    client.lookupPromise(remoteHostId).success(remoteHostState)
    mgr.getHostEntry(remoteHostId)
    mgr.peekHostEntry(remoteHostId) should not be None
    (mgr, client, impl)

  // ---- StoreMoved ---------------------------------------------------------------------------------------------

  test("StoreMoved maps a store so sends to it need no pool lookup"):
    val (mgr, client, _) = newManager()

    mgr.storeMoved(store0, remoteHostId, HLCTimestamp(10))
    mgr.getHostEntryOrQueueMessage(store0, nudge())

    client.poolLookups should be(empty)
    client.lookups.toList should be(List(remoteHostId))

  test("StoreMoved no newer than the mapping held is ignored"):
    val (mgr, client, _) = newManager()

    mgr.storeMoved(store0, remoteHostId, HLCTimestamp(10))
    mgr.storeMoved(store0, otherHostId, HLCTimestamp(10))
    mgr.storeMoved(store0, otherHostId, HLCTimestamp(5))
    mgr.getHostEntryOrQueueMessage(store0, nudge())

    client.lookups.toList should be(List(remoteHostId))

  test("newer StoreMoved replaces a mapping from a pool lookup"):
    val (mgr, client, _) = newManager()
    client.setPoolLookupTimestamp(unknownPoolId, HLCTimestamp(10))
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, remoteHostId))
    mgr.getHostEntryOrQueueMessage(store0, nudge())
    client.lookups.toList should be(List(remoteHostId))

    mgr.storeMoved(store0, otherHostId, HLCTimestamp(20))
    mgr.getHostEntryOrQueueMessage(store0, nudge())

    client.poolLookups.toList should be(List(unknownPoolId))
    client.lookups.toList should be(List(remoteHostId, otherHostId))

  test("a pool lookup older than StoreMoved gossip neither overwrites it nor misroutes the parked messages"):
    val (mgr, client, impl) = newManager()
    val msg = nudge()
    mgr.getHostEntryOrQueueMessage(store0, msg)

    // The move commits after the pool read was served, so the read names the old host
    mgr.storeMoved(store0, otherHostId, HLCTimestamp(20))
    client.setPoolLookupTimestamp(unknownPoolId, HLCTimestamp(10))
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, remoteHostId))

    client.lookups.toList should be(List(otherHostId))
    client.lookupPromise(otherHostId).success(otherHostState)
    impl.deliveredTo(otherHostId) should be(List(msg))
    impl.deliveredTo(remoteHostId) should be(empty)

  test("StoreMoved naming a bootstrap store's current host does not refresh the bootstrap config"):
    val (mgr, client, _) = newManager()

    mgr.storeMoved(StoreId(poolId, 0.toByte), bootstrapHostId, HLCTimestamp(10))

    client.bootstrapConfigFetches should be(0)

  test("StoreMoved naming another host for a bootstrap store refreshes the bootstrap config"):
    val (mgr, client, _) = newManager()

    mgr.storeMoved(StoreId(poolId, 0.toByte), remoteHostId, HLCTimestamp(10))

    client.bootstrapConfigFetches should be(1)

  // ---- HostEndpointChanged ------------------------------------------------------------------------------------

  test("newer HostEndpointChanged replaces a resolved entry with no read"):
    val (mgr, client, _) = withResolvedRemote(10)
    val old = mgr.peekHostEntry(remoteHostId).get

    mgr.endpointChanged(changed(20)).map(_ eq old) should be(Some(true))

    val entry = mgr.peekHostEntry(remoteHostId).get
    entry.name should be(old.name)
    (entry.address, entry.dataPort, entry.httpPort, entry.storeTransferPort) should be(("10.9.9.9", 9000, 9001, 9002))
    client.lookups.toList should be(List(remoteHostId))

  test("HostEndpointChanged no newer than the entry held is ignored"):
    val (mgr, _, _) = withResolvedRemote(10)
    mgr.endpointChanged(changed(20))
    val current = mgr.peekHostEntry(remoteHostId).get

    mgr.endpointChanged(changed(20, dataPort = 1)) should be(None)
    mgr.endpointChanged(changed(15, dataPort = 2)) should be(None)

    mgr.peekHostEntry(remoteHostId).get should be theSameInstanceAs current

  test("HostEndpointChanged older than the lookup's read is ignored"):
    val (mgr, _, _) = withResolvedRemote(30)
    val current = mgr.peekHostEntry(remoteHostId).get

    mgr.endpointChanged(changed(20)) should be(None)

    mgr.peekHostEntry(remoteHostId).get should be theSameInstanceAs current

  test("HostEndpointChanged replaces a bootstrap config entry, which has no timestamp"):
    val (mgr, _, _) = newManager()
    val old = mgr.peekHostEntry(bootstrapHostId).get

    mgr.endpointChanged(changed(20).copy(hostId = bootstrapHostId)).map(_ eq old) should be(Some(true))

    mgr.peekHostEntry(bootstrapHostId).map(_.dataPort) should be(Some(9000))

  test("HostEndpointChanged for a host with no entry starts no lookup"):
    val (mgr, client, _) = newManager()

    mgr.endpointChanged(changed(20)) should be(None)

    mgr.peekHostEntry(remoteHostId) should be(None)
    client.lookups should be(empty)

  test("HostEndpointChanged newer than an in-flight lookup's read supplies the entry's endpoint"):
    val (mgr, client, _) = newManager()
    mgr.getHostEntry(remoteHostId)

    mgr.endpointChanged(changed(20)) should be(None)
    client.setLookupTimestamp(remoteHostId, HLCTimestamp(10))
    client.lookupPromise(remoteHostId).success(remoteHostState)

    val entry = mgr.peekHostEntry(remoteHostId).get
    entry.name should be(remoteHostState.name)
    entry.dataPort should be(9000)
    // The entry is now as of the gossip, so a repeat of it is ignored
    mgr.endpointChanged(changed(20, dataPort = 1)) should be(None)

  test("HostEndpointChanged older than an in-flight lookup's read loses to it"):
    val (mgr, client, _) = newManager()
    mgr.getHostEntry(remoteHostId)

    mgr.endpointChanged(changed(20))
    client.setLookupTimestamp(remoteHostId, HLCTimestamp(30))
    client.lookupPromise(remoteHostId).success(remoteHostState)

    mgr.peekHostEntry(remoteHostId).map(_.dataPort) should be(Some(remoteHostState.dataPort))
