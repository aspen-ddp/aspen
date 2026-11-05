package org.aspen_ddp.aspen.common.network.implementations.quic

import org.aspen_ddp.aspen.common.metadata.{HostId, HostState, StorageDeviceId}
import org.aspen_ddp.aspen.common.network.*
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.TimeUnit

class QuicNetCacheSuite extends AnyFunSuite with Matchers with BeforeAndAfterEach:
  import QuicNetFixture.*

  private var fx: QuicNetFixture = scala.compiletime.uninitialized
  private var cacheDir: os.Path = scala.compiletime.uninitialized

  override def beforeEach(): Unit =
    fx = new QuicNetFixture()
    cacheDir = os.temp.dir(prefix = "net-cache-suite")

  override def afterEach(): Unit =
    fx.close()
    try os.remove.all(cacheDir)
    catch case _: Throwable => ()

  /** Sends a message from `from` to `to` and waits for `receiver` to get it */
  private def deliver(from: Node, to: HostId, receiver: Node): CheckStorageDevice =
    val m = CheckStorageDevice(to, from.net.clientId, StorageDeviceId(UUID.randomUUID()))
    from.net.clientMessenger.sendHostMessage(m)
    val d = receiver.handler.hostMessages.poll(AwaitMillis, TimeUnit.MILLISECONDS)
    if d == null || d.message != m then
      throw new AssertionError(s"the message did not reach host $to within ${AwaitMillis}ms: $d")
    m

  test("a host restarted with a warm cache reaches a non-bootstrap host without a lookup"):
    val hostIdC = HostId(UUID.randomUUID())
    val portC = freeUdpPort()
    fx.directory.put(HostState(hostIdC, "host_c", "127.0.0.1", portC, 0, 0, Set()))
    val c = fx.host(hostIdC, portC)

    val hostIdD = HostId(UUID.randomUUID())
    val portD = freeUdpPort()
    fx.directory.put(HostState(hostIdD, "host_d", "127.0.0.1", portD, 0, 0, Set()))
    val cache1 = NetworkCache.open(cacheDir, fx.bootstrap.aspenSystemId)
    val d1 = fx.host(hostIdD, portD, networkCache = cache1)
    deliver(d1, hostIdC, c)

    // Nothing but the cache can now say where C is
    d1.net.closeForTests()
    cache1.close() // a crash would drop RocksDB's directory lock; a test must release it
    fx.directory.remove(hostIdC)
    val d2 = fx.host(hostIdD, portD, networkCache = NetworkCache.open(cacheDir, fx.bootstrap.aspenSystemId))
    deliver(d2, hostIdC, c)
