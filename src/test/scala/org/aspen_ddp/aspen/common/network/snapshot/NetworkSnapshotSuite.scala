package org.aspen_ddp.aspen.common.network.snapshot

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.{CachedHost, CachedStore, Codec}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

class NetworkSnapshotSuite extends AnyFunSuite with Matchers:

  private def host(i: Int) =
    CachedHost(HostId(new UUID(0, i)), "host", "10.0.0.1", 100, 200, 300, HLCTimestamp(i))

  test("hosts round-trip through encode/decode"):
    val s = HostSnapshot(HLCTimestamp(42), List(host(1), host(2)))
    NetworkSnapshot.decodeHosts(NetworkSnapshot.encodeHosts(s)) should be (s)

  test("stores round-trip through encode/decode"):
    val st = CachedStore(StoreId(PoolId(UUID.randomUUID()), 3.toByte), HostId(UUID.randomUUID()), HLCTimestamp(7))
    val s = StoreSnapshot(HLCTimestamp(42), List(st))
    NetworkSnapshot.decodeStores(NetworkSnapshot.encodeStores(s)) should be (s)

  test("empty snapshots round-trip"):
    val h = HostSnapshot(HLCTimestamp.Zero, Nil)
    val s = StoreSnapshot(HLCTimestamp.Zero, Nil)
    NetworkSnapshot.decodeHosts(NetworkSnapshot.encodeHosts(h)) should be (h)
    NetworkSnapshot.decodeStores(NetworkSnapshot.encodeStores(s)) should be (s)

  test("encoded output is compressed"):
    val hosts = (1 to 1000).map(host).toList
    val raw = Codec.encodeHostSnapshot(hosts, HLCTimestamp(1))
    NetworkSnapshot.encodeHosts(HostSnapshot(HLCTimestamp(1), hosts)).length should be < (raw.length / 5)

  test("corrupt bytes fail to decode"):
    an[Exception] should be thrownBy NetworkSnapshot.decodeHosts(Array[Byte](1, 2, 3))
    an[Exception] should be thrownBy NetworkSnapshot.decodeStores(Array[Byte](1, 2, 3))

  /** A zstd frame header alone (single segment, 8-byte content size) declaring `size` decompressed bytes */
  private def frameDeclaring(size: Long): Array[Byte] =
    ByteBuffer.allocate(13).order(ByteOrder.LITTLE_ENDIAN)
      .putInt(0xFD2FB528).put(0xE0.toByte).putLong(size).array()

  private def rejection(b: Array[Byte]): String =
    val hosts = the[IllegalArgumentException] thrownBy NetworkSnapshot.decodeHosts(b)
    val stores = the[IllegalArgumentException] thrownBy NetworkSnapshot.decodeStores(b)
    hosts.getMessage should be(stores.getMessage)
    hosts.getMessage

  test("a declared size above the cap is rejected before allocating"):
    rejection(frameDeclaring(NetworkSnapshot.MaxDecompressedSize + 1)) should include("exceeds")
    rejection(frameDeclaring(1L << 40)) should include("exceeds")
    // Would overflow an Int
    rejection(frameDeclaring(Int.MaxValue.toLong + 1)) should include("exceeds")

  test("a size the frame does not declare is rejected"):
    rejection(frameDeclaring(-1L)) should include("unknown")
    rejection(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)) should include("unknown")
