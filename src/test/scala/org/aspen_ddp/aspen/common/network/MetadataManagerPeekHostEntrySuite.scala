package org.aspen_ddp.aspen.common.network

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MetadataManagerPeekHostEntrySuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  test("peekHostEntry returns a bootstrap host without starting a lookup"):
    val (mgr, client, _) = newManager()

    mgr.peekHostEntry(bootstrapHostId).map(_.name) should be(Some("bootstrap_host"))
    client.lookups.toList should be(empty)

  test("peekHostEntry returns None for an unknown host and starts no lookup"):
    val (mgr, client, _) = newManager()

    mgr.peekHostEntry(remoteHostId) should be(None)
    mgr.peekHostEntry(remoteHostId) should be(None)
    client.lookups.toList should be(empty)

    // The contrast that gives the assertion above its meaning: getHostEntry answers None just
    // the same, but does it by starting the lookup peekHostEntry deliberately does not.
    mgr.getHostEntry(remoteHostId) should be(None)
    client.lookups.toList should be(List(remoteHostId))

  test("peekHostEntry returns None while a lookup is pending"):
    val (mgr, client, _) = newManager()

    mgr.getHostEntry(remoteHostId) should be(None)
    // The Promise is deliberately left uncompleted, so the entry is still Left(PendingHostLookup).
    mgr.peekHostEntry(remoteHostId) should be(None)

    // And peeking at a pending entry does not start a second lookup either.
    client.lookups.toList should be(List(remoteHostId))

  test("peekHostEntry returns the entry once the lookup resolves"):
    val (mgr, client, _) = newManager()

    mgr.getHostEntry(remoteHostId) should be(None)
    client.lookupPromise(remoteHostId).success(remoteHostState)

    val entry = mgr.peekHostEntry(remoteHostId)
    entry.map(_.hostId) should be(Some(remoteHostId))
    entry.map(_.name) should be(Some("remote_host"))
    entry.map(_.address) should be(Some("10.0.0.9"))
    client.lookups.toList should be(List(remoteHostId))

  test("a failed lookup returns the host to the never-looked-up state"):
    val (mgr, client, _) = newManager()

    mgr.getHostEntry(remoteHostId) should be(None)
    client.lookupPromise(remoteHostId).failure(new NoSuchElementException("no such host"))

    // peekHostEntry's scaladoc rests on this: a failure drops the entry rather than leaving it
    // pending, so a poll-only caller keeps seeing None and has to time out.
    mgr.peekHostEntry(remoteHostId) should be(None)

    // Dropped rather than remembered as failed, so the next getHostEntry starts a fresh lookup.
    mgr.getHostEntry(remoteHostId) should be(None)
    client.lookups.toList should be(List(remoteHostId, remoteHostId))
