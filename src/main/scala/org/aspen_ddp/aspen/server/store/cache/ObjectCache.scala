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
    *
    * @return The evicted objects
    */
  def insert(state: ObjectState): List[ObjectState]

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

    override def insert(state: ObjectState): List[ObjectState] = Nil

    override def remove(objectId: ObjectId): Unit = ()
  }
}