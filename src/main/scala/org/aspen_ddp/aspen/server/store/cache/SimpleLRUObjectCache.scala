package org.aspen_ddp.aspen.server.store.cache

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.server.store.ObjectState

import scala.collection.immutable.HashMap

object SimpleLRUObjectCache {
  class Entry(initialState: ObjectState) {
    var next: Int = 0
    var prev: Int = 0
    var state: ObjectState = initialState
  }
}

class SimpleLRUObjectCache(val maxEntries: Int) extends ObjectCache {
  import SimpleLRUObjectCache._

  var leastRecentlyUsed: Int = 0
  var mostRecentlyUsed: Int = 0
  var entries: Vector[Entry] = Vector()
  var map: HashMap[ObjectId, Int] = new HashMap

  def clear(): Unit = {
    entries = Vector()
    map = map.empty
    leastRecentlyUsed = 0
    mostRecentlyUsed = 0
  }

  def remove(objectId: ObjectId): Unit = {
    // Just remove the entry from the lookup index. To keep ownership simple,
    // we'll leave it in the LRU chain and let it fall off the end naturally
    // since it can no longer be accessed.
    map -= objectId
  }

  def get(objectId: ObjectId): Option[ObjectState] = {
    if (entries.isEmpty)
      None
    else {
      map.get(objectId) match {
        case None => None
        case Some(idx) =>
          if (idx == leastRecentlyUsed) {
            val newLru = entries(leastRecentlyUsed).next
            entries(newLru).prev = newLru
            leastRecentlyUsed = newLru
          }

          if (idx != mostRecentlyUsed) {
            val mru = entries(mostRecentlyUsed)
            mru.next = idx

            val prev = entries(idx).prev
            entries(prev).next = entries(idx).next

            val next = entries(idx).next
            entries(next).prev = entries(idx).prev

            entries(idx).prev = mostRecentlyUsed
            entries(idx).next = idx
          }

          Some(entries(idx).state)
      }
    }
  }

  def insert(state: ObjectState): Option[ObjectState] = {
    if (entries.isEmpty) {
      leastRecentlyUsed = 0
      mostRecentlyUsed = 0
      map += (state.objectId -> 0)
      val e = new Entry(state)
      entries = entries.:+(e)
      None
    }
    else if (entries.size < maxEntries) {
      val idx = entries.size
      entries(mostRecentlyUsed).next = idx
      map += (state.objectId -> idx)
      val e = new Entry(state)
      e.prev = mostRecentlyUsed
      e.next = idx
      entries = entries.:+(e)
      mostRecentlyUsed = idx
      None
    }
    else {
      // Index is full, need to pop an entry. However, we cannot pop objects locked
      // to transactions. So we'll use get() on them to put those at the head of
      // the list until a non-locked object occurs
      while (entries(leastRecentlyUsed).state.transactionReferences != 0) {
        get(entries(leastRecentlyUsed).state.objectId)
      }

      val lruObjectId = entries(leastRecentlyUsed).state.objectId
      val newLru = entries(leastRecentlyUsed).next
      val newMru = entries(newLru).prev
      entries(newLru).prev = newLru

      map -= lruObjectId
      map += (state.objectId -> newMru)

      val removedState = entries(leastRecentlyUsed).state

      entries(leastRecentlyUsed).prev = mostRecentlyUsed
      entries(leastRecentlyUsed).next = leastRecentlyUsed
      entries(leastRecentlyUsed).state = state

      mostRecentlyUsed = leastRecentlyUsed
      leastRecentlyUsed = newLru

      Some(removedState)
    }
  }
}
