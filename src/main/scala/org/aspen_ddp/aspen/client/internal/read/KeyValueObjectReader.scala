package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{KeyValueObjectState, ObjectState}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.ReadResponse
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectRefcount, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId

object KeyValueObjectReader {


  private case class Segment(revision: ObjectRevision, timestamp: HLCTimestamp, storeID: StoreId, data: Value)

  private case class Restorable(revision: ObjectRevision, timestamp: HLCTimestamp, slices: List[(Byte, Value)])
}

class KeyValueObjectReader(metadataOnly: Boolean, pointer: KeyValueObjectPointer, readUUID: UUID)
  extends BaseObjectReader[KeyValueObjectPointer, KeyValueObjectStoreState](metadataOnly, pointer, readUUID) {

  import KeyValueObjectReader._

  override protected def createObjectState(storeId:StoreId, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): KeyValueObjectStoreState = {
    new KeyValueObjectStoreState(storeId, cs.revision, cs.refcount, cs.timestamp, readTime, cs.objectData, cs.lockedWriteTransactions)
  }

  override protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                                       readTime: HLCTimestamp, matchingStoreStates: List[KeyValueObjectStoreState],
                                       allStoreStates: List[KeyValueObjectStoreState], debug: Boolean): ObjectState = {

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
        Some(Value(pointer.ida.restoreArray(lleft)))
    } else {
      None
    }

    val right = if (matchingStoreStates.head.kvoss.right.nonEmpty) {
      val lright = matchingStoreStates.flatMap { ss => ss.kvoss.right.map(v => ss.storeId.poolIndex -> v.bytes) }

      if (lright.size < threshold)
        throw BaseObjectReader.NotRestorable(s"KVObject right object attribute is below threshold")
      else {
        Some(Value(pointer.ida.restoreArray(lright)))
      }
    } else {
      None
    }

    val kvrestores = storeStates.foldLeft(Map[Key, List[Segment]]()) { (m, ss) =>
      ss.kvoss.contents.iterator.foldLeft(m) { (subm, v) =>
        val x = subm.get(v._1) match {
          case None => Segment(v._2.revision, v._2.timestamp, ss.storeId, v._2.value) :: Nil
          case Some(l) => Segment(v._2.revision, v._2.timestamp, ss.storeId, v._2.value) :: l
        }
        subm + (v._1 -> x)
      }
    }.map(t => t._1 -> resolve(s"Key(${t._1}", storeStates, t._2))

    val contents = kvrestores.foldLeft(Map[Key,KeyValueObjectState.ValueState]()) { (m, t) => t._2 match {
      case None => m
      case Some(r) =>
        val v = Value(pointer.ida.restoreArray(r.slices.map(t => t._1 -> t._2.bytes)))
        m + (t._1 -> KeyValueObjectState.ValueState(v, r.revision, r.timestamp))
    }}

    new KeyValueObjectState(pointer, revision, refcount, timestamp, readTime, min, max, left, right, contents)
  }


  private def anyStoreHasLocked(rev: ObjectRevision, storeStates: List[KeyValueObjectStoreState]): Boolean = {
    storeStates.exists(_.lockedWriteTransactions.contains(TransactionId(rev.lastUpdateTxUUID)))
  }

  private def resolve[T](what: String,
                         storeStates: List[KeyValueObjectStoreState],
                         segments: List[Segment]): Option[Restorable] = if (segments.isEmpty) None else {

    //println("HELLO")
    val highestRevision = segments.foldLeft((HLCTimestamp.Zero, ObjectRevision.Null)) { (h,s) =>
      if (s.timestamp > h._1)
        (s.timestamp, s.revision)
      else h
    }._2

    val matches = segments.filter { s => if (s.revision == highestRevision) true else {
      storeStates.find(p => p.storeId == s.storeID).foreach(ss => knownBehind += s.storeID -> ss.readTimestamp)
      false
    }}

    //println(s"Resolving $what. hrev: $highestRevision matches: $matches")

    val matching = matches.size
    val mismatching = storeStates.size - matching

    //println(s"Highest Revision: $highestRevision, matching size ${matching}. Matching: $matches")

    if (matching < threshold) {
      // If we cannot restore even though we have a threshold number of responses, we need to determine whether
      // the item has been deleted or we need to wait for more responses. Deletion is determined by ruling out
      // the possibility of partial allocation. To do this, we check to see if any other store has a locked transaction
      // matching the revision of the item. If so, the item is most likely in the process of being allocated and we'll
      // need the allocation to complete before the item can be restored.
      //
      val potential = width - matching - mismatching

      if (matching + potential >= threshold || anyStoreHasLocked(highestRevision, storeStates))
        throw BaseObjectReader.NotRestorable(s"KVObject $what is below threshold")
      else
        None // Item deleted
    } else {
      Some(Restorable(highestRevision, matches.head.timestamp, matches.map(s => s.storeID.poolIndex -> s.data)))
    }
  }
}
