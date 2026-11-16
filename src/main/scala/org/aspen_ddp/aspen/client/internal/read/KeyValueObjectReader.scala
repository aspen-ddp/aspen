package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{KeyValueObjectState, ObjectState}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.ReadResponse
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectRefcount, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId

object KeyValueObjectReader {


  private case class Segment(revision: ObjectRevision, timestamp: HLCTimestamp, storeID: StoreId, data: Value)

  private case class Restorable(revision: ObjectRevision, timestamp: HLCTimestamp, slices: List[(Byte, Value)])
}

class KeyValueObjectReader(metadataOnly: Boolean, pointer: KeyValueObjectPointer, ida: IDA, readUUID: UUID,
                           partial: Boolean = false)
  extends BaseObjectReader[KeyValueObjectPointer, KeyValueObjectStoreState](metadataOnly, pointer, ida, readUUID) {

  import KeyValueObjectReader._

  override protected def createObjectState(storeId:StoreId, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): KeyValueObjectStoreState = {
    new KeyValueObjectStoreState(storeId, cs.revision, cs.refcount, cs.timestamp, readTime, cs.objectData, cs.lockedWriteTransactions)
  }

  override protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                                       readTime: HLCTimestamp, matchingStoreStates: List[KeyValueObjectStoreState],
                                       allStoreStates: List[KeyValueObjectStoreState], debug: Boolean): ObjectState = {
    //println(s"ATTEMPTING TO Restore object with refcount ${refcount}")
    if (debug)
      println(s"DEBUG KV Restore Object")

    val storeStates = allStoreStates

    val min = if (matchingStoreStates.count(_.kvoss.minimum.nonEmpty) >= threshold)
      matchingStoreStates.find(_.kvoss.minimum.nonEmpty).flatMap(_.kvoss.minimum)
    else
      None

    val max = if (matchingStoreStates.count(_.kvoss.maximum.nonEmpty) >= threshold)
      matchingStoreStates.find(_.kvoss.maximum.nonEmpty).flatMap(_.kvoss.maximum)
    else
      None

    val left = if (matchingStoreStates.head.kvoss.left.nonEmpty) {
      val lleft = matchingStoreStates.flatMap { ss => ss.kvoss.left.map(v => ss.storeId.poolIndex -> v.bytes) }

      if (lleft.size < threshold)
        throw BaseObjectReader.NotRestorable(s"KVObject left object attribute is below threshold")
      else
        Some(Value(ida.restoreArray(lleft)))
    } else {
      None
    }

    if (debug)
      println(s"DEBUG KV Restore Object pre max")

    val right = if (matchingStoreStates.head.kvoss.right.nonEmpty) {
      val lright = matchingStoreStates.flatMap { ss => ss.kvoss.right.map(v => ss.storeId.poolIndex -> v.bytes) }

      if (lright.size < threshold)
        throw BaseObjectReader.NotRestorable(s"KVObject right object attribute is below threshold")
      else {
        Some(Value(ida.restoreArray(lright)))
      }
    } else {
      None
    }

    if (debug)
      println(s"DEBUG KV Restore Object pre kv")

    val kvrestores = storeStates.foldLeft(Map[Key, List[Segment]]()) { (m, ss) =>
      ss.kvoss.contents.iterator.foldLeft(m) { (subm, v) =>
        val x = subm.get(v._1) match {
          case None => Segment(v._2.revision, v._2.timestamp, ss.storeId, v._2.value) :: Nil
          case Some(l) => Segment(v._2.revision, v._2.timestamp, ss.storeId, v._2.value) :: l
        }
        subm + (v._1 -> x)
      }
    }.map(t => t._1 -> resolve(t._1, storeStates, t._2, debug))

    val contents = kvrestores.foldLeft(Map[Key,KeyValueObjectState.ValueState]()) { (m, t) => t._2 match {
      case None => m
      case Some(r) =>
        if (debug)
          println(s"Creating value state for key ${t._1}.")
        val v = Value(ida.restoreArray(r.slices.map(t => t._1 -> t._2.bytes)))
        m + (t._1 -> KeyValueObjectState.ValueState(v, r.revision, r.timestamp))
    }}

    if (debug)
      println(s"Restored Contents: $contents")

    new KeyValueObjectState(pointer, revision, refcount, timestamp, readTime, ida, min, max, left, right, contents, partial)
  }


  private def anyStoreHasLocked(rev: ObjectRevision, storeStates: List[KeyValueObjectStoreState]): Boolean = {
    storeStates.exists(_.lockedWriteTransactions.contains(TransactionId(rev.lastUpdateTxUUID)))
  }

  private def resolve[T](key: Key,
                         storeStates: List[KeyValueObjectStoreState],
                         segments: List[Segment],
                         debug: Boolean): Option[Restorable] = if (segments.isEmpty) None else {

    // Blind writes of a key need not commit in timestamp order, so the revision is chosen by count alone: at most one
    // reaches the threshold, which is a majority (see BaseObjectReader.getStoreStates)
    val byRevision = segments.groupBy(_.revision)

    byRevision.valuesIterator.find(_.size >= threshold) match
      case Some(matches) =>
        Some(Restorable(matches.head.revision, matches.head.timestamp, matches.map(s => s.storeID.poolIndex -> s.data)))

      case None =>
        // If we cannot restore even though we have a threshold number of responses, we need to determine whether
        // the item has been deleted or we need to wait for more responses. It is deleted only if no revision could
        // still reach the threshold from the stores yet to answer, and no store holds the lock of a transaction that
        // wrote one of its revisions (an allocation or write in progress)
        val potential = width - storeStates.size
        val largest = byRevision.valuesIterator.map(_.size).max

        if debug then
          println(s"Restoring key $key. largest match: $largest thresh: $threshold")

        if largest + potential >= threshold || byRevision.keysIterator.exists(anyStoreHasLocked(_, storeStates)) then
          throw BaseObjectReader.NotRestorable(s"KVObject $key is below threshold")
        else
          None // Item deleted
  }
}
