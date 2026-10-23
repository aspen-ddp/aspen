package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.rocksdb.{Options, RocksDB}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class NetworkCacheSuite extends AnyFunSuite with Matchers:

  private val poolId = PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
  private val host1 = HostId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
  private val host2 = HostId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
  private val sysA = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
  private val sysB = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

  private def ts(v: Long) = HLCTimestamp(v)
  private def store(i: Int, h: HostId, t: Long) = CachedStore(StoreId(poolId, i.toByte), h, ts(t))
  private def host(h: HostId, addr: String, t: Long) = CachedHost(h, "name", addr, 1, 2, 3, ts(t))

  test("NoCache loads nothing and ignores writes"):
    val c = NetworkCache.NoCache
    c.putStore(store(0, host1, 1))
    c.putHost(host(host1, "a", 1))
    c.bulkUpdate(NetworkCacheContent(List(store(1, host1, 1)), List(host(host2, "b", 1))))
    c.load() should be (NetworkCacheContent(Nil, Nil))

  test("RocksDB round-trips stores and hosts across reopen"):
    val dir = os.temp.dir()
    val s1 = store(3, host1, 7)
    val h1 = CachedHost(host1, "nm", "10.0.0.1", 100, 200, 300, ts(9))
    val c = NetworkCache.open(dir, sysA)
    c.putStore(s1)
    c.putHost(h1)
    c.close()
    val c2 = NetworkCache.open(dir, sysA)
    c2.load() should be (NetworkCacheContent(List(s1), List(h1)))
    c2.close()

  test("removeStore deletes the mapping"):
    val c = NetworkCache.open(os.temp.dir(), sysA)
    c.putStore(store(0, host1, 1))
    c.putStore(store(1, host1, 1))
    c.removeStore(store(0, host1, 1).storeId)
    c.load().stores should be (List(store(1, host1, 1)))
    c.close()

  test("putStore and putHost overwrite unconditionally"):
    val c = NetworkCache.open(os.temp.dir(), sysA)
    c.putStore(store(0, host1, 10))
    c.putStore(store(0, host2, 5))
    c.putHost(host(host1, "new", 10))
    c.putHost(host(host1, "old", 5))
    c.load() should be (NetworkCacheContent(List(store(0, host2, 5)), List(host(host1, "old", 5))))
    c.close()

  test("bulkUpdate writes only newer entries"):
    val c = NetworkCache.open(os.temp.dir(), sysA)
    c.putStore(store(0, host1, 10))
    c.putHost(host(host1, "held", 10))
    c.bulkUpdate(NetworkCacheContent(
      List(store(0, host2, 5), store(1, host2, 1)),
      List(host(host1, "fresh", 20))))
    val content = c.load()
    content.stores.toSet should be (Set(store(0, host1, 10), store(1, host2, 1)))
    content.hosts should be (List(host(host1, "fresh", 20)))
    c.close()

  test("a different system id wipes the cache"):
    val dir = os.temp.dir()
    val c = NetworkCache.open(dir, sysA)
    c.putStore(store(0, host1, 1))
    c.putHost(host(host1, "a", 1))
    c.close()
    val b = NetworkCache.open(dir, sysB)
    b.load() should be (NetworkCacheContent(Nil, Nil))
    b.close()
    val a = NetworkCache.open(dir, sysA)
    a.load() should be (NetworkCacheContent(Nil, Nil))
    a.close()

  test("corrupt records are skipped"):
    val dir = os.temp.dir()
    val c = NetworkCache.open(dir, sysA)
    c.putStore(store(0, host1, 1))
    c.putHost(host(host1, "a", 1))
    c.close()
    val opts = new Options()
    val raw = RocksDB.open(opts, dir.toString)
    try
      val skey = 's'.toByte +: (new Array[Byte](16) :+ 9.toByte)
      raw.put(skey, Array[Byte](-1, -2, -3, -4))
      val hkey = 'h'.toByte +: Array.fill[Byte](16)(7)
      raw.put(hkey, Array[Byte](-1, -2, -3, -4))
    finally
      raw.close()
      opts.close()
    val c2 = NetworkCache.open(dir, sysA)
    c2.load() should be (NetworkCacheContent(List(store(0, host1, 1)), List(host(host1, "a", 1))))
    c2.close()

  test("a second open of a locked directory degrades to NoCache"):
    val dir = os.temp.dir()
    val first = NetworkCache.open(dir, sysA)
    val second = NetworkCache.open(dir, sysA)
    second should be theSameInstanceAs NetworkCache.NoCache
    first.close()

  test("removeHost deletes the host"):
    val dir = os.temp.dir()
    val c = NetworkCache.open(dir, sysA)
    c.putHost(host(host1, "a", 1))
    c.removeHost(host1)
    c.close()
    val c2 = NetworkCache.open(dir, sysA)
    c2.load().hosts should be (Nil)
    c2.close()

  test("bulkUpdate applies removals"):
    val dir = os.temp.dir()
    val s1 = store(0, host1, 1)
    val h1 = host(host1, "a", 1)
    val c = NetworkCache.open(dir, sysA)
    c.putStore(s1)
    c.putHost(h1)
    c.bulkUpdate(NetworkCacheContent(Nil, Nil), List(s1.storeId), List(h1.hostId))
    c.close()
    val c2 = NetworkCache.open(dir, sysA)
    c2.load() should be (NetworkCacheContent(Nil, Nil))
    c2.close()

  test("bulkUpdate keeps the newest of duplicate keys"):
    val c = NetworkCache.open(os.temp.dir(), sysA)
    c.bulkUpdate(NetworkCacheContent(
      List(store(0, host1, 9), store(0, host2, 5)),
      List(host(host1, "new", 9), host(host1, "old", 5))))
    c.load() should be (NetworkCacheContent(List(store(0, host1, 9)), List(host(host1, "new", 9))))
    c.close()
