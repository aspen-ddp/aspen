package org.aspen_ddp.aspen.common.network.implementations.quic.gossip

import org.aspen_ddp.aspen.common.metadata.HostId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class MemberTrackerSuite extends AnyFunSuite with Matchers:

  private val h: HostId = HostId(UUID.randomUUID())

  test("first member is Online"):
    val t = new MemberTracker
    t.added(h, "m1") shouldBe List(HostEvent.Online(h))
    t.isOnline(h) shouldBe true
    t.onlineHosts shouldBe Set(h)

  test("removal of the only member is Offline"):
    val t = new MemberTracker
    t.added(h, "m1")
    t.removed(h, "m1") shouldBe List(HostEvent.Offline(h))
    t.isOnline(h) shouldBe false
    t.onlineHosts shouldBe Set.empty

  test("new member while old is live is Restarted, no Offline"):
    val t = new MemberTracker
    t.added(h, "m1")
    t.added(h, "m2") shouldBe List(HostEvent.Restarted(h))
    t.removed(h, "m1") shouldBe Nil
    t.isOnline(h) shouldBe true

  test("old removed before new added is Offline then Online+Restarted"):
    val t = new MemberTracker
    t.added(h, "m1")
    t.removed(h, "m1") shouldBe List(HostEvent.Offline(h))
    t.added(h, "m2") shouldBe List(HostEvent.Online(h), HostEvent.Restarted(h))

  test("same member re-added is not a restart"):
    val t = new MemberTracker
    t.added(h, "m1")
    t.removed(h, "m1")
    t.added(h, "m1") shouldBe List(HostEvent.Online(h))

  test("duplicate add and unknown remove are no-ops"):
    val t = new MemberTracker
    t.added(h, "m1")
    t.added(h, "m1") shouldBe Nil
    t.removed(h, "zz") shouldBe Nil
    t.isOnline(h) shouldBe true
