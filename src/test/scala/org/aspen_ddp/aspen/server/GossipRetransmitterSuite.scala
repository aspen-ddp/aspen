package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.{Gossip, HostEndpointChanged, StoreMoved}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.duration.*

class GossipRetransmitterSuite extends AnyFunSuite with Matchers:

  private val moved = StoreMoved(StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), 3.toByte))
  private val changed = HostEndpointChanged(HostId(UUID.fromString("22222222-2222-2222-2222-222222222222")))

  private val start = Instant.parse("2026-10-22T12:00:00Z")

  /** A retransmitter on a fresh state file with a clock the test advances */
  private class Fixture(val stateFile: os.Path = os.temp.dir() / "cache" / GossipRetransmitter.StateFilename):
    var clock: Instant = start
    val sent: mutable.ListBuffer[Gossip] = mutable.ListBuffer()

    def retransmitter(): GossipRetransmitter =
      new GossipRetransmitter(stateFile, g => sent += g, () => clock)

    def advance(d: FiniteDuration): Unit = clock = clock.plusMillis(d.toMillis)

  test("broadcast sends immediately and saves the message"):
    val f = new Fixture
    val r = f.retransmitter()

    r.broadcast(moved)

    f.sent.toList should be(List(moved))
    os.exists(f.stateFile) should be(true)
    GossipRetransmitter.load(f.stateFile) should be(Map(moved -> start))

  test("retransmit re-sends every message within its lifetime"):
    val f = new Fixture
    val r = f.retransmitter()
    r.broadcast(moved)
    f.advance(1.hour)
    r.broadcast(changed)
    f.sent.clear()

    f.advance(GossipRetransmitter.Period)
    r.retransmit()

    f.sent.toSet should be(Set(moved, changed))

  test("messages older than the lifetime are dropped and no longer sent"):
    val f = new Fixture
    val r = f.retransmitter()
    r.broadcast(moved)
    f.advance(1.hour)
    r.broadcast(changed)
    f.sent.clear()

    f.advance(GossipRetransmitter.Lifetime - 30.minutes)
    r.retransmit()

    f.sent.toList should be(List(changed))
    r.pendingMessages.keySet should be(Set(changed))
    GossipRetransmitter.load(f.stateFile).keySet should be(Set(changed))

  test("the state file is removed once nothing is pending"):
    val f = new Fixture
    val r = f.retransmitter()
    r.broadcast(moved)

    f.advance(GossipRetransmitter.Lifetime + 1.minute)
    r.retransmit()

    r.pendingMessages should be(Map())
    os.exists(f.stateFile) should be(false)

  test("broadcasting a pending message again restarts its lifetime"):
    val f = new Fixture
    val r = f.retransmitter()
    r.broadcast(moved)
    f.advance(20.hours)
    r.broadcast(moved)
    f.sent.clear()

    f.advance(10.hours)
    r.retransmit()

    f.sent.toList should be(List(moved))

  test("a new retransmitter resumes the messages saved by a previous one"):
    val f = new Fixture
    f.retransmitter().broadcast(moved)
    f.advance(1.hour)
    f.retransmitter().broadcast(changed)
    f.sent.clear()

    val restarted = f.retransmitter()
    restarted.pendingMessages should be(Map(moved -> start, changed -> start.plusSeconds(3600)))
    restarted.retransmit()

    f.sent.toSet should be(Set(moved, changed))

  test("an unreadable state file is treated as empty"):
    val f = new Fixture
    os.write(f.stateFile, "messages: [ not: valid", createFolders = true)

    val r = f.retransmitter()
    r.pendingMessages should be(Map())

    r.broadcast(moved)
    GossipRetransmitter.load(f.stateFile) should be(Map(moved -> start))

  test("a failed save does not prevent the broadcast"):
    // A regular file where the cache directory should be makes every save fail.
    val dir = os.temp.dir()
    os.write(dir / "cache", "not a directory")
    val f = new Fixture(dir / "cache" / GossipRetransmitter.StateFilename)
    val r = f.retransmitter()

    r.broadcast(moved)

    f.sent.toList should be(List(moved))
    r.pendingMessages.keySet should be(Set(moved))
