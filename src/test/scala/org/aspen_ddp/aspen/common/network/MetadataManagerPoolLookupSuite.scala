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
