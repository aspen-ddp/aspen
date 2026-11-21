package org.aspen_ddp.aspen.server.store

import java.util.UUID

import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer, ReadType}
import org.aspen_ddp.aspen.common.transaction.TransactionId

/** A client's read waiting for committed transactions it names (Read.awaitCommit) to release their locks on its
  * object, or, for `allocation`, for this store's unapplied commit vote on the object's allocation to apply or abort,
  * until `until` (wallClockMillis). See Frontend.parkRead and parkAllocationRead
  */
final class ParkedRead(val clientId: ClientId, val pointer: ObjectPointer, val readType: ReadType,
                       val awaitCommit: Set[TransactionId], val until: Long, val allocation: Boolean = false)

/** The reads a Frontend has parked, by object, then read: a resent read replaces its entry. At most `max` at once.
  * Owned by the store's executor. Declared before the Frontend's constructor recovery, whose Txs release locks
  * (answerParkedReads)
  */
final class ParkedReads(max: Int):
  private val reads = scala.collection.mutable.HashMap[ObjectId, scala.collection.mutable.HashMap[UUID, ParkedRead]]()
  private var count: Int = 0

  def nonEmpty: Boolean = count > 0

  def hasRoom: Boolean = count < max

  def add(objectId: ObjectId, readUUID: UUID, read: ParkedRead): Unit =
    reads.getOrElseUpdate(objectId, scala.collection.mutable.HashMap())(readUUID) = read
    count += 1

  def remove(objectId: ObjectId, readUUID: UUID): Unit =
    if count > 0 then
      reads.get(objectId).foreach: forObject =>
        if forObject.remove(readUUID).isDefined then
          count -= 1
          if forObject.isEmpty then reads.remove(objectId)

  def of(objectId: ObjectId): Option[Map[UUID, ParkedRead]] = reads.get(objectId).map(_.toMap)

  /** The parked reads whose deadline `now` has reached */
  def due(now: Long): List[(ObjectId, UUID, ParkedRead)] =
    reads.toList.flatMap((objectId, forObject) =>
      forObject.toList.collect { case (readUUID, r) if r.until <= now => (objectId, readUUID, r) })

  def clear(): Unit =
    reads.clear()
    count = 0
