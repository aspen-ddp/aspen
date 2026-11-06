package org.aspen_ddp.aspen.server.store.cache

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.ObjectState
import scribe.Logging

import scala.concurrent.ExecutionContext

/** Host-wide object cache: one byte budget shared by per-store LRU views.
  *
  * Each store accesses the cache through the [[StoreObjectCache]] returned by forStore, so the stores of a pool
  * hosted together never see each other's slices of the same object. A view's state is owned by its store's
  * executor and takes no locks. The only state shared between store threads is the host byte total
  * (an AtomicLong), the registry of views (a ConcurrentHashMap) and the views' volatile flags.
  *
  * @param maxBytes Host-wide budget, in estimated bytes
  * @param clock Source of access times (nanoseconds)
  */
class HostObjectCache(val maxBytes: Long, private[cache] val clock: () => Long = () => System.nanoTime())
  extends Logging:

  require(maxBytes > 0)

  private[cache] val total = new AtomicLong(0)

  private val views = new ConcurrentHashMap[StoreId, StoreObjectCache]

  /** Estimated bytes currently held by all stores */
  def sizeInBytes: Long = total.get

  /** Returns the cache view for a store, to be used only from `executor`.
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
