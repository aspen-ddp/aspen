package org.aspen_ddp.aspen.server.store.cache

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.ObjectState

/** Host-wide object cache with LRU eviction and a capacity in bytes.
  *
  * All stores on a host share one budget and one recency order. Each store accesses the cache
  * through the view returned by forStore, so the stores of a pool hosted together never see each
  * other's slices of the same object.
  *
  * Objects referenced by a transaction are never evicted; eviction walks from the least-recently-used
  * end skipping them. If too few unreferenced entries remain, the cache grows beyond maxBytes rather
  * than dropping state that a transaction still needs.
  *
  * A store's insert may evict another store's entries. That is only safe because the Host serializes
  * every store operation on its event loop: a transaction that obtains an object from the cache adds its
  * reference before any other store can run an insert. The cache must not be shared between Hosts.
  */
class HostObjectCache(val maxBytes: Long):

  require(maxBytes > 0)

  private case class Entry(state: ObjectState, size: Long)

  // Access-ordered: iteration runs least-recently-used first, and get()/put()
  // move the touched entry to the most-recently-used end.
  private val entries = new java.util.LinkedHashMap[(StoreId, ObjectId), Entry](16, 0.75f, true)

  private var totalBytes: Long = 0

  /** Estimated bytes currently held */
  def sizeInBytes: Long = synchronized(totalBytes)

  def entryCount: Int = synchronized(entries.size)

  /** Returns the cache view for a store. Any entries left over from a previous load of the store are dropped */
  def forStore(storeId: StoreId): ObjectCache =
    clear(storeId)
    new StoreView(storeId)

  private class StoreView(storeId: StoreId) extends ObjectCache:
    def clear(): Unit = HostObjectCache.this.clear(storeId)
    def get(objectId: ObjectId): Option[ObjectState] = HostObjectCache.this.get(storeId, objectId)
    def insert(state: ObjectState): List[ObjectState] = HostObjectCache.this.insert(storeId, state)
    def remove(objectId: ObjectId): Unit = HostObjectCache.this.remove(storeId, objectId)

  private def clear(storeId: StoreId): Unit = synchronized:
    val iter = entries.entrySet.iterator
    while iter.hasNext do
      val e = iter.next()
      if e.getKey._1 == storeId then
        totalBytes -= e.getValue.size
        iter.remove()

  private def get(storeId: StoreId, objectId: ObjectId): Option[ObjectState] = synchronized:
    Option(entries.get((storeId, objectId))).map(_.state)

  private def remove(storeId: StoreId, objectId: ObjectId): Unit = synchronized:
    Option(entries.remove((storeId, objectId))).foreach(e => totalBytes -= e.size)

  /** Inserts or re-measures an object. Returns the objects evicted to make room for it */
  private def insert(storeId: StoreId, state: ObjectState): List[ObjectState] = synchronized:
    val key = (storeId, state.objectId)
    val entry = Entry(state, HostObjectCache.estimateSize(state))

    Option(entries.put(key, entry)).foreach(old => totalBytes -= old.size)
    totalBytes += entry.size

    if totalBytes <= maxBytes then
      Nil
    else
      // Evict least-recently-used entries that aren't pinned by a transaction
      val iter = entries.entrySet.iterator
      var evicted: List[ObjectState] = Nil

      while totalBytes > maxBytes && iter.hasNext do
        val e = iter.next()
        val victim = e.getValue
        if e.getKey != key && victim.state.transactionReferences == 0 then
          iter.remove()
          totalBytes -= victim.size
          evicted = victim.state :: evicted

      evicted.reverse


object HostObjectCache:

  /** Approximate fixed cost of a cached object: the ObjectState, its metadata, and the cache entry */
  val EntryOverhead: Long = 256

  /** Approximate cost of each decoded key/value pair beyond its key and value bytes */
  val KeyValueEntryOverhead: Long = 128

  /** Estimates the heap consumed by a cached object.
    *
    * Key-Value objects hold both their encoded data and its decoded content, so both are counted.
    */
  def estimateSize(state: ObjectState): Long =
    val kvBytes = state.kvState match
      case None => 0L
      case Some(kvs) =>
        var n = 0L
        kvs.min.foreach(k => n += k.bytes.length)
        kvs.max.foreach(k => n += k.bytes.length)
        kvs.left.foreach(v => n += v.bytes.length)
        kvs.right.foreach(v => n += v.bytes.length)
        kvs.content.foreachEntry: (k, vs) =>
          n += k.bytes.length + vs.value.bytes.length + KeyValueEntryOverhead
        n

    EntryOverhead + state.data.size + kvBytes
