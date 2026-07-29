package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** Coverage for the two read-only accessors ZMQNet.awaitPendingMessagesSent polls. */
class MetadataManagerDrainSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  test("resolvedHostEntries omits a host whose lookup is still pending"):
    val (mgr, client) = newManager()

    mgr.resolvedHostEntries.map(_.hostId) should be(List(bootstrapHostId))

    mgr.getHostEntry(remoteHostId) should be(None)
    // A lookup with no message parked on it must not hold the drain open.
    mgr.hasParkedMessages should be(false)
    mgr.resolvedHostEntries.map(_.hostId) should be(List(bootstrapHostId))

    client.lookupPromise(remoteHostId).success(remoteHostState)
    mgr.resolvedHostEntries.map(_.hostId).toSet should be(Set(bootstrapHostId, remoteHostId))

  test("hasParkedMessages sees a message held behind a pending host lookup"):
    val (mgr, client) = newManager()

    mgr.hasParkedMessages should be(false)

    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge()) should be(None)
    mgr.hasParkedMessages should be(true)

    // Resolving hands the queue to the newly created host entry, which is where the network
    // implementation picks it up. Nothing is parked in MetadataManager any more, and the
    // drain's remaining check -- the resolved entry's own pending queue -- takes over.
    client.lookupPromise(remoteHostId).success(remoteHostState)
    mgr.hasParkedMessages should be(false)

  test("hasParkedMessages sees a message held behind a pending pool lookup"):
    val (mgr, client) = newManager()

    val unknownStore = StoreId(unknownPoolId, 0.toByte)
    mgr.getHostEntryOrQueueMessage(unknownStore, nudge()) should be(None)
    client.poolLookups.toList should be(List(unknownPoolId))
    mgr.hasParkedMessages should be(true)

  test("a failed lookup drops the parked message rather than reporting it"):
    val (mgr, client) = newManager()

    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge()) should be(None)
    mgr.hasParkedMessages should be(true)

    // The limit the drain's scaladoc has to state: a failure drops the entry and the messages
    // parked on it, so the predicate goes quiet because the message is gone, not because it
    // was sent. Nothing at this layer can distinguish the two.
    client.lookupPromise(remoteHostId).failure(new NoSuchElementException("no such host"))
    mgr.hasParkedMessages should be(false)
