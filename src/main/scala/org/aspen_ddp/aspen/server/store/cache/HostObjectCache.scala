package org.aspen_ddp.aspen.server.store.cache

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.ObjectState
import scribe.Logging

import scala.concurrent.ExecutionContext

/** Host-wide object cache: one byte budget shared by per-store LRU views.
  *
  * Each store accesses the cache through the [[StoreObjectCache]] returned by forStore, so the stores of a pool
  * hosted together never see each other's slices of the same object. A view's state is owned by its store's
  * executor and takes no locks. The only state shared between store threads is the host byte total
  * (an AtomicLong), the registry of views (a ConcurrentHashMap) and the views' volatile and atomic published values.
  * The budget is soft: a view over it sheds or asks another view to shed, so the total may briefly overshoot.
  *
  * @param maxBytes Host-wide budget, in estimated bytes
  * @param clock Source of access times (nanoseconds)
  */
class HostObjectCache(val maxBytes: Long, private[cache] val clock: () => Long = () => System.nanoTime())
  extends Logging:

  require(maxBytes > 0)

  private[cache] val total = new AtomicLong(0)

  private val views = new ConcurrentHashMap[StoreId, StoreObjectCache]

  /** A shed frees down to this, below the budget, so the next inserts fit without posting another shed */
  private[cache] val shedLowWater: Long = maxBytes - maxBytes / HostObjectCache.ShedSlackDivisor

  /** Estimated bytes currently held by all stores */
  def sizeInBytes: Long = total.get

  /** Returns the cache view for a store, to be used only from `executor`, where other views post its sheds.
    *
    * A view of the store that was never cleared belongs to a store that is no longer running. It is warned about,
    * uncounted and ended.
    */
  def forStore(storeId: StoreId, executor: ExecutionContext = ExecutionContext.parasitic): StoreObjectCache =
    val view = new StoreObjectCache(this, storeId, executor)
    val old = views.put(storeId, view)
    if old != null then
      logger.warn(s"Object cache view for store $storeId replaced before it was cleared")
      old.abandon()
    view

  private[cache] def unregister(view: StoreObjectCache): Unit = views.remove(view.storeId, view)

  /** Called on `from`'s executor when the host total is over budget. Picks the view whose LRU head is oldest (`from`
    * on a tie). If that is `from`, it evicts its own oldest entries, never `keep`, re-picking after each one, until
    * the total fits or another view's head is older. Another view is asked to shed by a task posted to its executor,
    * at most one pending per view; it then carries the pressure on itself.
    */
  private[cache] def rebalance(from: StoreObjectCache, keep: Option[ObjectId]): Unit =
    var done = false
    while !done && total.get > maxBytes do
      val victim = pickVictim(from)
      if victim == null then
        done = true
      else if victim eq from then
        done = !from.evictOldest(keep)
      else
        if victim.shedPending.compareAndSet(false, true) then
          victim.executor.execute(() => victim.shed())
        done = true

  /** The registered view with the smallest published oldestAccess, `from` winning ties; null if every LRU is empty */
  private def pickVictim(from: StoreObjectCache): StoreObjectCache =
    var victim: StoreObjectCache = null
    var oldest = from.oldestAccess
    if oldest < Long.MaxValue then victim = from
    val iter = views.values.iterator
    while iter.hasNext do
      val v = iter.next()
      val t = v.oldestAccess
      if t < oldest then
        oldest = t
        victim = v
    victim


object HostObjectCache:

  /** A shed frees an extra 1/ShedSlackDivisor of the budget below it. Freeing only to the budget would leave the host
    * exactly at it, so under sustained pressure every next insert would go over and post another shed to the same
    * victim: one posted task per insert. With the slack a shed covers about maxBytes / 32 bytes of inserts. Local
    * eviction by the inserting store needs no slack, as it posts nothing, and stays exact to the budget.
    */
  val ShedSlackDivisor: Long = 32

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
