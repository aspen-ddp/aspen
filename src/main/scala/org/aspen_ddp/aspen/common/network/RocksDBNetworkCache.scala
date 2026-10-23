package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.rocksdb.{Options, RocksDB, WriteBatch, WriteOptions}
import scribe.Logging

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable
import scala.util.control.NonFatal

object RocksDBNetworkCache:
  private val StorePrefix: Byte = 's'.toByte
  private val HostPrefix: Byte = 'h'.toByte
  private val MarkerPrefix: Byte = 'm'.toByte

  private val SystemIdKey: Array[Byte] = MarkerPrefix +: "system-id".getBytes(StandardCharsets.UTF_8)

  private def uuidBytes(u: UUID): Array[Byte] =
    val bb = ByteBuffer.allocate(16)
    bb.putLong(u.getMostSignificantBits).putLong(u.getLeastSignificantBits)
    bb.array()

  private def storeKey(s: StoreId): Array[Byte] =
    val bb = ByteBuffer.allocate(18)
    bb.put(StorePrefix).put(uuidBytes(s.poolId.uuid)).put(s.poolIndex)
    bb.array()

  private def hostKey(h: HostId): Array[Byte] =
    val bb = ByteBuffer.allocate(17)
    bb.put(HostPrefix).put(uuidBytes(h.uuid))
    bb.array()

  private def storeIdFromKey(key: Array[Byte]): StoreId =
    require(key.length == 18, s"Invalid store key length ${key.length}")
    val bb = ByteBuffer.wrap(key, 1, 17)
    StoreId(PoolId(new UUID(bb.getLong, bb.getLong)), bb.get())

/** Keys are 's' + pool UUID + store index, 'h' + host UUID, and an 'm' + "system-id" scope marker. A cache written
  * for a different system id is wiped on open.
  *
  * Throws if the database cannot be opened. Use NetworkCache.open to degrade to NoCache instead.
  */
class RocksDBNetworkCache(dir: os.Path, systemId: UUID) extends NetworkCache with Logging:
  import RocksDBNetworkCache.*

  private val db: RocksDB =
    os.makeDir.all(dir)
    val options = new Options().setCreateIfMissing(true)
    val opened =
      try
        RocksDB.open(options, dir.toString)
      finally
        options.close()
    try
      verifyScope(opened)
      opened
    catch
      case t: Throwable =>
        opened.close()
        throw t

  private def verifyScope(db: RocksDB): Unit =
    val expected = uuidBytes(systemId)
    val held = db.get(SystemIdKey)
    if held == null || !java.util.Arrays.equals(held, expected) then
      // Spans the whole keyspace: every key starts with a prefix byte far below 0xFF
      db.deleteRange(Array[Byte](0), Array.fill[Byte](32)(0xFF.toByte))
      db.put(SystemIdKey, expected)

  def load(): NetworkCacheContent =
    val stores = List.newBuilder[CachedStore]
    val hosts = List.newBuilder[CachedHost]
    val it = db.newIterator()
    try
      it.seekToFirst()
      while it.isValid do
        val key = it.key()
        try
          if key.nonEmpty && key(0) == StorePrefix then
            stores += Codec.decodeCachedStore(storeIdFromKey(key), it.value())
          else if key.nonEmpty && key(0) == HostPrefix then
            hosts += Codec.decodeCachedHost(it.value())
        catch
          case NonFatal(t) =>
            logger.warn(s"Skipping corrupt network cache record: $t")
        it.next()
    finally
      it.close()
    NetworkCacheContent(stores.result(), hosts.result())

  def putStore(e: CachedStore): Unit =
    try db.put(storeKey(e.storeId), Codec.encode(e))
    catch case NonFatal(t) => logger.warn(s"Network cache putStore failed: $t")

  def removeStore(storeId: StoreId): Unit =
    try db.delete(storeKey(storeId))
    catch case NonFatal(t) => logger.warn(s"Network cache removeStore failed: $t")

  def putHost(e: CachedHost): Unit =
    try db.put(hostKey(e.hostId), Codec.encode(e))
    catch case NonFatal(t) => logger.warn(s"Network cache putHost failed: $t")

  def bulkUpdate(content: NetworkCacheContent): Unit =
    try
      val held = load()
      val heldStores = mutable.HashMap.from(held.stores.map(s => s.storeId -> s))
      val heldHosts = mutable.HashMap.from(held.hosts.map(h => h.hostId -> h))
      val batch = new WriteBatch()
      val wo = new WriteOptions()
      try
        for s <- content.stores if heldStores.get(s.storeId).forall(_.timestamp < s.timestamp) do
          batch.put(storeKey(s.storeId), Codec.encode(s))
        for h <- content.hosts if heldHosts.get(h.hostId).forall(_.timestamp < h.timestamp) do
          batch.put(hostKey(h.hostId), Codec.encode(h))
        if batch.count() > 0 then
          db.write(wo, batch)
      finally
        wo.close()
        batch.close()
    catch case NonFatal(t) => logger.warn(s"Network cache bulkUpdate failed: $t")

  def close(): Unit =
    try db.close()
    catch case NonFatal(t) => logger.warn(s"Network cache close failed: $t")
