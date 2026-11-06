package org.aspen_ddp.aspen.server.store.cache

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.ObjectState

import scala.concurrent.ExecutionContext

/** One store's view of the host object cache.
  *
  * Every method runs on the store's executor, which exclusively owns the view's mutable state, so nothing here is
  * locked. The only state touched from other threads is the host byte total (an AtomicLong), the volatile `bytes`
  * counter (read when a replacement view uncounts this one) and the volatile `cleared` flag.
  *
  * Evictable entries sit in an access-ordered LRU. States a transaction references are pinned: they are counted but
  * never evicted. A pinned state that is also the cached state for its object is held out of the LRU and is returned
  * by `get`; unpinning puts it back at the most recently used end. Eviction only ever drops this view's own LRU head;
  * a view over the host budget evicts until the host total fits or its LRU is empty, never the entry just inserted.
  */
final class StoreObjectCache private[cache] (host: HostObjectCache,
                                             val storeId: StoreId,
                                             executor: ExecutionContext) extends ObjectCache:

  private case class Entry(state: ObjectState, size: Long, var lastAccess: Long)

  private class Pinned(val state: ObjectState, var size: Long, var cached: Boolean)

  // Access-ordered: iteration runs least-recently-used first
  private val lru = new java.util.LinkedHashMap[ObjectId, Entry](16, 0.75f, true)

  private val pinned = new java.util.IdentityHashMap[ObjectState, Pinned]

  // Cached pinned states by object id, which are not in the LRU
  private val pinnedCached = new java.util.HashMap[ObjectId, Pinned]

  @volatile private var bytes: Long = 0

  @volatile private var cleared: Boolean = false

  /** Resident bytes of this store: evictable and pinned */
  def sizeInBytes: Long = bytes

  /** Number of evictable entries */
  def entryCount: Int = lru.size

  def pinnedCount: Int = pinned.size

  private def adjust(delta: Long): Unit =
    bytes += delta
    host.total.addAndGet(delta)

  /** Called by the host when a new view replaces this one while the old store is no longer running */
  private[cache] def abandon(): Unit =
    if !cleared then
      cleared = true
      host.total.addAndGet(-bytes)

  def clear(): Unit =
    if !cleared then
      cleared = true
      adjust(-bytes)
      lru.clear()
      pinned.clear()
      pinnedCached.clear()
      host.unregister(this)

  def get(objectId: ObjectId): Option[ObjectState] =
    if cleared then
      None
    else
      val e = lru.get(objectId)
      if e != null then
        e.lastAccess = host.clock()
        Some(e.state)
      else
        val p = pinnedCached.get(objectId)
        if p != null then Some(p.state) else None

  def insert(state: ObjectState): Unit =
    if !cleared then
      val size = HostObjectCache.estimateSize(state)
      val p = pinned.get(state)
      if p != null then
        adjust(size - p.size)
        p.size = size
        if !p.cached then
          p.cached = true
          pinnedCached.put(state.objectId, p)
      else
        val old = lru.put(state.objectId, Entry(state, size, host.clock()))
        adjust(size - (if old != null then old.size else 0L))
      evictToFit(state.objectId)

  def remove(objectId: ObjectId): Unit =
    if !cleared then
      val old = lru.remove(objectId)
      if old != null then adjust(-old.size)

  def pin(state: ObjectState): Unit =
    if !cleared && !pinned.containsKey(state) then
      val e = lru.get(state.objectId)
      if e != null && (e.state eq state) then
        lru.remove(state.objectId)
        val p = new Pinned(state, e.size, true)
        pinned.put(state, p)
        pinnedCached.put(state.objectId, p)
      else
        val size = HostObjectCache.estimateSize(state)
        pinned.put(state, new Pinned(state, size, false))
        adjust(size)

  def unpin(state: ObjectState): Unit =
    if !cleared then
      val p = pinned.remove(state)
      if p != null then
        if p.cached then pinnedCached.remove(state.objectId, p)
        if p.cached && !state.reloadRequired && !lru.containsKey(state.objectId) then
          lru.put(state.objectId, Entry(state, p.size, host.clock()))
          evictToFit(state.objectId)
        else
          adjust(-p.size)

  /** Evicts this view's least recently used entries until the host total fits, sparing `keep` */
  private def evictToFit(keep: ObjectId): Unit =
    if host.total.get > host.maxBytes then
      val iter = lru.entrySet.iterator
      while host.total.get > host.maxBytes && iter.hasNext do
        val e = iter.next()
        if e.getKey != keep then
          iter.remove()
          adjust(-e.getValue.size)
