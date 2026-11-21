package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.objects.ObjectId

/** A count of outstanding operations per object, present only while positive. Owned by the store's executor */
final class ObjectCounts:
  private val counts = scala.collection.mutable.HashMap[ObjectId, Int]()

  def contains(oid: ObjectId): Boolean = counts.contains(oid)

  def increment(oid: ObjectId): Unit =
    counts(oid) = counts.getOrElse(oid, 0) + 1

  /** Counts one operation done. Returns true when none remain for `oid` */
  def decrement(oid: ObjectId): Boolean =
    val n = counts.getOrElse(oid, 1) - 1
    if n == 0 then
      counts.remove(oid)
      true
    else
      counts(oid) = n
      false
