package org.aspen_ddp.aspen.server.store.cache

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.ObjectState

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext

/** One store's view of the host object cache.
  *
  * Every method runs on the store's executor, which exclusively owns the view's mutable state, so nothing here is
  * locked. The only state touched from other threads is the host byte total (an AtomicLong), the volatile
  * `oldestAccess` (read by other views choosing a victim) and the `shedPending` flag. `bytes` is volatile only so that
  * sizeInBytes may be read from any thread; it is written only here. A view is ended by `clear`, on its executor.
  *
  * Evictable entries sit in an access-ordered LRU. States a transaction references are pinned: they are counted but
  * never evicted. A pinned state that is also the cached state for its object is held out of the LRU and is returned
  * by `get`, ahead of any LRU entry for the object; unpinning puts it back at the most recently used end, replacing
  * that entry.
  *
  * Eviction only ever drops this view's own LRU head, on this view's executor. When an insert or unpin takes the host
  * total over its budget, [[HostObjectCache.rebalance]] picks the view whose LRU head is oldest: this view evicts
  * locally (never the entry just inserted), and any other view is asked to `shed` by a task posted to its executor.
  */
final class StoreObjectCache private[cache] (host: HostObjectCache,
                                             val storeId: StoreId,
                                             private[cache] val executor: ExecutionContext) extends ObjectCache:

  private case class Entry(state: ObjectState, size: Long, var lastAccess: Long)

  private class Pinned(val state: ObjectState, var size: Long, var cached: Boolean)

  // Access-ordered: iteration runs least-recently-used first
  private val lru = new java.util.LinkedHashMap[ObjectId, Entry](16, 0.75f, true)

  private val pinned = new java.util.IdentityHashMap[ObjectState, Pinned]

  // Cached pinned states by object id, which are not in the LRU
  private val pinnedCached = new java.util.HashMap[ObjectId, Pinned]

  @volatile private var bytes: Long = 0

  private var cleared: Boolean = false

  /** The LRU head's last access, or Long.MaxValue when the LRU is empty. Republished after every mutation */
  @volatile private[cache] var oldestAccess: Long = Long.MaxValue

  /** True from posting a shed to this view's executor until that shed starts */
  private[store] val shedPending = new AtomicBoolean(false)

  /** Resident bytes of this store: evictable and pinned */
  def sizeInBytes: Long = bytes

  /** Number of evictable entries */
  def entryCount: Int = lru.size

  def pinnedCount: Int = pinned.size

  private def adjust(delta: Long): Unit =
    bytes += delta
    host.total.addAndGet(delta)

  private def publishOldest(): Unit =
    oldestAccess = if lru.isEmpty then Long.MaxValue else lru.firstEntry.getValue.lastAccess

  def clear(): Unit =
    if !cleared then
      cleared = true
      adjust(-bytes)
      lru.clear()
      pinned.clear()
      pinnedCached.clear()
      oldestAccess = Long.MaxValue
      host.unregister(this)

  def get(objectId: ObjectId): Option[ObjectState] =
    if cleared then
      None
    else
      // A pinned cached state is preferred to an LRU entry for the same object, unless it must be reloaded
      val p = pinnedCached.get(objectId)
      if p != null && !p.state.reloadRequired then
        Some(p.state)
      else
        val e = lru.get(objectId)
        if e != null then
          e.lastAccess = host.clock()
          publishOldest()
          Some(e.state)
        else if p != null then
          Some(p.state)
        else
          None

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
        publishOldest()
      rebalanceIfOver(state.objectId)

  def remove(objectId: ObjectId): Unit =
    if !cleared then
      val old = lru.remove(objectId)
      if old != null then
        adjust(-old.size)
        publishOldest()

  def pin(state: ObjectState): Unit =
    if !cleared && !pinned.containsKey(state) then
      val e = lru.get(state.objectId)
      if e != null && (e.state eq state) then
        lru.remove(state.objectId)
        publishOldest()
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
        // The cached state get returns for its object: it replaces any LRU entry for the object, as it did in get
        val served = p.cached && pinnedCached.remove(state.objectId, p)
        if served && !state.reloadRequired then
          val old = lru.put(state.objectId, Entry(state, p.size, host.clock()))
          if old != null then adjust(-old.size)
          publishOldest()
          rebalanceIfOver(state.objectId)
        else
          adjust(-p.size)

  private def rebalanceIfOver(keep: ObjectId): Unit =
    if host.total.get > host.maxBytes then host.rebalance(this, Some(keep))

  /** Evicts the LRU head unless it is `keep`. Returns false when nothing was evicted */
  private[cache] def evictOldest(keep: Option[ObjectId]): Boolean =
    val iter = lru.entrySet.iterator
    if !iter.hasNext then
      false
    else
      val e = iter.next()
      if keep.contains(e.getKey) then
        false
      else
        iter.remove()
        adjust(-e.getValue.size)
        publishOldest()
        true

  /** Runs on this view's executor, posted by another view's rebalance: evicts this view's oldest entries until the
    * host total is down to the shed low-water mark, then passes any pressure left over the budget on to the next
    * victim.
    */
  private[cache] def shed(): Unit =
    shedPending.set(false)
    if !cleared then
      while host.total.get > host.shedLowWater && evictOldest(None) do ()
      if host.total.get > host.maxBytes then host.rebalance(this, None)
