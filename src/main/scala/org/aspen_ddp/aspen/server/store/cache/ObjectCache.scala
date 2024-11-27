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

  /** Inserts an object into the cache and optionally removes one from
    * the cache
    */
  def insert(state: ObjectState): Option[ObjectState]

  /** Only used by aborted allocations. Removes an object from the cache
    *
    * @param objectId Identity of the object for which allocation was aborted
    */
  def remove(objectId: ObjectId): Unit
}

object ObjectCache {
  object NoCache extends ObjectCache {

    override def clear(): Unit = ()

    override def get(objectId: ObjectId): Option[ObjectState] = None

    override def insert(state: ObjectState): Option[ObjectState] = None

    override def remove(objectId: ObjectId): Unit = ()
  }
}