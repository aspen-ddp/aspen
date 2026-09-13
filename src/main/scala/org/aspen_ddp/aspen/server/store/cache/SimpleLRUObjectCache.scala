package org.aspen_ddp.aspen.server.store.cache

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.server.store.ObjectState

/** Fixed-capacity object cache with LRU eviction.
  *
  * Objects currently locked to a transaction are never evicted; eviction walks
  * from the least-recently-used end until it finds an unlocked entry. If every
  * entry is locked, the cache grows beyond maxEntries rather than dropping state
  * that a transaction still needs.
  */
class SimpleLRUObjectCache(val maxEntries: Int) extends ObjectCache:

  require(maxEntries > 0)

  // Access-ordered: iteration runs least-recently-used first, and get()/put()
  // move the touched entry to the most-recently-used end.
  private val entries = new java.util.LinkedHashMap[ObjectId, ObjectState](16, 0.75f, true)

  def clear(): Unit = synchronized:
    entries.clear()

  def remove(objectId: ObjectId): Unit = synchronized:
    entries.remove(objectId)

  def get(objectId: ObjectId): Option[ObjectState] = synchronized:
    Option(entries.get(objectId))

  def insert(state: ObjectState): Option[ObjectState] = synchronized:
    entries.put(state.objectId, state)

    if entries.size <= maxEntries then
      None
    else
      // Evict the least-recently-used entry that isn't pinned by a transaction
      val iter = entries.entrySet.iterator
      var evicted: Option[ObjectState] = None

      while evicted.isEmpty && iter.hasNext do
        val e = iter.next()
        if e.getKey != state.objectId && e.getValue.transactionReferences == 0 then
          val victim = e.getValue
          iter.remove()
          evicted = Some(victim)

      evicted
