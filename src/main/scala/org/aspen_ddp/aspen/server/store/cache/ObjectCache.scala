package org.aspen_ddp.aspen.server.store.cache

import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.server.store.ObjectState

trait ObjectCache {

  /** Clears the cache */
  def clear(): Unit

  /** Retrieves an object from the cache
    *
    * @param objectId Object to retrieve
    * @return Some if the object is in the cache, None otherwise
    */
  def get(objectId: ObjectId): Option[ObjectState]

  /** Inserts an object into the cache, or re-measures one already present after its
    * content changed, evicting other objects as needed to stay within capacity
    */
  def insert(state: ObjectState): Unit

  /** Starts counting a state a transaction references and exempts it from eviction until unpinned.
    * Pinning an already-pinned state does nothing.
    */
  def pin(state: ObjectState): Unit

  /** Ends a pin. A state that is cached and still valid becomes evictable again, most recently used; any
    * other state stops being counted.
    */
  def unpin(state: ObjectState): Unit

  /** Removes an object from the cache
    *
    * Used when the cached state must not be served to later readers: the object has been
    * deleted, or its in-memory state is suspect and should be reloaded from the backing store.
    *
    * @param objectId Identity of the object to drop
    */
  def remove(objectId: ObjectId): Unit
}

object ObjectCache {
  object NoCache extends ObjectCache {

    override def clear(): Unit = ()

    override def get(objectId: ObjectId): Option[ObjectState] = None

    override def insert(state: ObjectState): Unit = ()

    override def pin(state: ObjectState): Unit = ()

    override def unpin(state: ObjectState): Unit = ()

    override def remove(objectId: ObjectId): Unit = ()
  }
}