package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** Coverage for what startPoolLookup does with the messages parked on it once the pool resolves.
 *
 *  The message type is incidental -- MetadataManager never inspects a message, only the address
 *  it was sent to -- so these use the same nudge the drain suite does. Each call to nudge()
 *  carries a fresh device id, which makes individual messages distinguishable in assertions.
 */
class MetadataManagerPoolLookupSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  test("a resolving pool lookup starts a host lookup for a store on an unknown host"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val msg = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg) should be(None)
    client.poolLookups.toList should be(List(unknownPoolId))

    // remoteHostId is absent from the bootstrap config and has never been looked up, so resolving
    // the pool onto it takes the hosts.get(...) == None branch -- the one that used to drop the
    // queue on the floor.
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, remoteHostId))

    client.lookups.toList should be(List(remoteHostId))
    // The message moved from the pool's store queue onto the host lookup rather than vanishing,
    // so the exit drain still knows it is holding something.
    mgr.hasParkedMessages should be(true)
    impl.deliveredTo(remoteHostId) should be(empty)

    client.lookupPromise(remoteHostId).success(remoteHostState)

    impl.deliveredTo(remoteHostId) should be(List(msg))
    mgr.hasParkedMessages should be(false)

  test("a host that resolves inline still receives the rescued messages"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val msg = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg) should be(None)

    // Completing the host promise up front makes getHostState hand back an already-completed
    // future, so parasitic runs startHostLookup's continuation inline -- inside the pool loop,
    // and inside the same synchronized block. That is the case the seed-before-start ordering
    // exists for: the continuation builds the host entry out of phl.messageQueue, so anything
    // drained in after the call would miss the handoff entirely.
    client.lookupPromise(remoteHostId).success(remoteHostState)

    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, remoteHostId))

    client.lookups.toList should be(List(remoteHostId))
    impl.deliveredTo(remoteHostId) should be(List(msg))
    mgr.hasParkedMessages should be(false)

  test("two stores on the same unknown host share a single lookup"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val store1 = StoreId(unknownPoolId, 1.toByte)
    val msg0 = nudge()
    val msg1 = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg0) should be(None)
    // The second send finds the pool lookup already pending and parks behind it in its own
    // per-store queue, so there is still only one pool lookup in flight.
    mgr.getHostEntryOrQueueMessage(store1, msg1) should be(None)
    client.poolLookups.toList should be(List(unknownPoolId))

    client.poolLookupPromise(unknownPoolId).success(
      poolStateWith(unknownPoolId, remoteHostId, remoteHostId))

    // Store 0's iteration installs the pending host entry, so store 1's iteration finds it and
    // drains into the same queue rather than starting a second lookup and orphaning the first.
    client.lookups.toList should be(List(remoteHostId))
    mgr.hasParkedMessages should be(true)

    client.lookupPromise(remoteHostId).success(remoteHostState)

    impl.deliveredTo(remoteHostId) should be(List(msg0, msg1))
    mgr.hasParkedMessages should be(false)

  test("a store on an already-resolved host is handed over by storeResolved"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val msg = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg) should be(None)

    // bootstrapHostId is Right(...) from the constructor, so this takes the branch the fix leaves
    // alone -- and which nothing covered before.
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, bootstrapHostId))

    client.lookups.toList should be(empty)
    impl.storeResolutions.toList should be(List(bootstrapHostId -> store0))
    impl.deliveredTo(bootstrapHostId) should be(List(msg))
    mgr.hasParkedMessages should be(false)

  test("a message rescued onto a host lookup is still dropped if that lookup fails"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)

    mgr.getHostEntryOrQueueMessage(store0, nudge()) should be(None)
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, remoteHostId))
    mgr.hasParkedMessages should be(true)

    // The limit the fix does not remove: a failed lookup drops the entry and everything parked on
    // it. What changed is only where that can happen -- a message parked on a pool lookup can now
    // be dropped one stage later, by the host lookup it was moved to. hasParkedMessages' scaladoc
    // says so, and this is what it is describing.
    client.lookupPromise(remoteHostId).failure(new NoSuchElementException("no such host"))

    mgr.hasParkedMessages should be(false)
    impl.deliveredTo(remoteHostId) should be(empty)
