package org.aspen_ddp.aspen.common.network.implementations.quic

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId}
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.network.snapshot.{HostSnapshot, StoreSnapshot}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

class QuicNetSnapshotSuite extends AnyFunSuite with Matchers with BeforeAndAfterEach:
  import QuicNetFixture.*

  private val WaitMillis = 15000L

  private var fx: QuicNetFixture = scala.compiletime.uninitialized

  override def beforeEach(): Unit = fx = new QuicNetFixture()

  override def afterEach(): Unit = fx.close()

  private def send(from: Node, to: HostId): Unit =
    from.net.clientMessenger.sendHostMessage(CheckStorageDevice(to, from.net.clientId, StorageDeviceId(UUID.randomUUID())))

  /** Sends from `from` to `to` every 100ms until one of the messages reaches `node` */
  private def sendUntilDelivered(from: Node, to: HostId, node: Node): Unit =
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WaitMillis)
    var delivered = false
    while !delivered do
      if System.nanoTime() - deadline > 0 then
        throw new AssertionError(s"no message reached host $to within ${WaitMillis}ms")
      send(from, to)
      delivered = node.handler.hostMessages.poll(100, TimeUnit.MILLISECONDS) != null

  test("a snapshot with a new endpoint closes the old connection"):
    // C is not a bootstrap host, so a snapshot may move it
    val hostIdC = HostId(UUID.randomUUID())
    val portC1 = freeUdpPort()
    fx.directory.put(HostState(hostIdC, "host_c", "127.0.0.1", portC1, 0, 0, Set()))
    val c1 = fx.host(hostIdC, portC1)

    sendUntilDelivered(fx.a, hostIdC, c1)
    val oldEntry = fx.a.net.metadataManager.peekHostEntry(hostIdC).get
    val oldConn = oldEntry.connection.get()
    oldConn should not be null
    oldConn.isClosed shouldBe false

    // C moves to a new port. The directory still names the old one, so only the snapshot can tell A.
    val portC2 = freeUdpPort()
    val snapshot = (
      HostSnapshot(HLCTimestamp(10), List(CachedHost(hostIdC, "host_c", "127.0.0.1", portC2, 0, 0, HLCTimestamp(5)))),
      StoreSnapshot(HLCTimestamp(10), Nil))
    fx.a.net.startSnapshotReaderForTests(() => Future.successful(Some(snapshot)))

    eventually("A's old connection to C is closed", WaitMillis)(oldConn.isClosed)
    fx.a.net.metadataManager.peekHostEntry(hostIdC).map(_.dataPort) shouldBe Some(portC2)

    c1.net.crashForTests()
    val c2 = fx.host(hostIdC, portC2)
    sendUntilDelivered(fx.a, hostIdC, c2)
