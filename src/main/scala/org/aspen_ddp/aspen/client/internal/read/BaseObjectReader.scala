package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID

import org.aspen_ddp.aspen.client.{CorruptedObject, InvalidObject, MetadataObjectState, ObjectState, ReadError => ClientReadError}
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.ReadResponse
import org.aspen_ddp.aspen.common.objects.{ObjectPointer, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.store.StoreId
import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.common.objects.{ReadError => CommonReadError}

object BaseObjectReader {
  case class NotRestorable(reason: String) extends Throwable
}

abstract class BaseObjectReader[PointerType <: ObjectPointer, StoreStateType <: StoreState](
                                 val metadataOnly: Boolean,
                                 val pointer: PointerType,
                                 val readUUID: UUID) extends ObjectReader with Logging {

  import BaseObjectReader._

  def width: Int = pointer.ida.width
  def threshold: Int = pointer.ida.consistentRestoreThreshold

  protected var responses: Map[StoreId, Either[CommonReadError.Value, StoreStateType]] = Map()
  protected var endResult: Option[Either[ClientReadError, ObjectState]] = None
  protected var knownBehind: Map[StoreId, HLCTimestamp] = Map()

  def receivedResponseFrom(storeId: StoreId): Boolean = responses.contains(storeId)

  def noResponses: Set[StoreId] = allStores &~ responses.keySet

  /** Returns the map of store ids that are known to have returned responses with out-of-date results. The value
    * is the read time of the returned read response. If a reread is also out-of-date the timestamp value will be
    * updated. If the response is fully up-to-date, the store's entry will be removed from the map
    */
  def rereadCandidates: Map[StoreId, HLCTimestamp] = knownBehind

  def result: Option[Either[ClientReadError, ObjectState]] = endResult

  def numResponses: Int = responses.size

  def debugLogStatus(header: String, log: String => Unit): Unit = {
    val sb = new StringBuilder
    sb.append(header)
    sb.append("\n")
    sb.append(s"Read Transaction: $readUUID\n")
    sb.append(s"Num Responses: $numResponses. Num Errors: $numErrors\n")
    responses.keys.toList.sortWith((a,b) => a.poolIndex < b.poolIndex).foreach { storeId =>
      responses(storeId) match {
        case Left(err) =>
          sb.append(s"StoreID: $storeId\n")
          sb.append(s"  Error: $err\n")
        case Right(s) =>
          sb.append(s"StoreID: $storeId\n")
          s.debugLogStatus {str =>
            sb.append(str)
            sb.append("\n")
          }
      }
    }
    log(sb.toString)
    restore(true)
  }

  protected def createObjectState(storeId:StoreId, readTime: HLCTimestamp, cs: ReadResponse.CurrentState): StoreStateType

  /** Called with a list of store states with matching, highest-seen revisions. The list will contain >= threshold
    * elements.
    *
    * @throws NotRestorable if the object cannot be restored
    */
  protected def restoreObject(revision:ObjectRevision, refcount: ObjectRefcount, timestamp:HLCTimestamp,
                              readTime: HLCTimestamp, matchingStoreStates: List[StoreStateType],
                              allStoreStates: List[StoreStateType], debug: Boolean=false): ObjectState

  def numErrors: Int = responses.valuesIterator.foldLeft(0) { (count, e) => e match {
    case Left(_) => count + 1
    case Right(_) => count
  }}

  def receiveReadResponse(response:ReadResponse): Option[Either[ClientReadError, ObjectState]] = {
    knownBehind -= response.fromStore // Start fresh for this node

    response.result match {
      case Left(err) => responses += response.fromStore -> Left(err)

      case Right(cs) =>
        try {
          responses += response.fromStore -> Right(createObjectState(response.fromStore, response.readTime, cs))
        } catch {
          case _: ObjectEncodingError =>
            responses += response.fromStore -> Left(CommonReadError.CorruptedObject)
        }
    }

    restore()
  }

  protected def restore(debug: Boolean = false): Option[Either[ClientReadError, ObjectState]] = endResult match {
    case Some(r) => Some(r)
    case None =>
      if (responses.size >= threshold) {

        if (numErrors > width - threshold) {
          val (invalidCount, corruptCount) = responses.values.foldLeft((0, 0)) { (t, e) =>
            e match {
              case Left(CommonReadError.ObjectMismatch) => (t._1 + 1, t._2)
              case Left(CommonReadError.ObjectNotFound) => (t._1 + 1, t._2)
              case Left(CommonReadError.CorruptedObject) => (t._1, t._2 + 1)
              case Left(_) => t
              case Right(_) => t
            }
          }
          val err = if (invalidCount >= corruptCount)
            new InvalidObject(pointer)
          else
            new CorruptedObject(pointer)

          endResult = Some(Left(err))

        }
        else
          attemptRestore(debug)
      }

      endResult
  }

  protected def attemptRestore(debug: Boolean): Unit = {

    /*
    val revisions = responses.valuesIterator.foldLeft(Map[ObjectRevision, Int]()) { (m, r) => r match {
      case Left(_) => m
      case Right(ss) =>
        val count =  1 + m.getOrElse(ss.revision, 0)
        m + (ss.revision -> count)
    }}

    val (restorableRevision, restorableCount) = revisions.foldLeft(revisions.head) { (h, r) =>
      if (h._2 > r._2) h else r
    }
     */
    val mostRecent = responses.valuesIterator.foldLeft((ObjectRevision.Null, HLCTimestamp.Zero)) { (t,r) => r match {
      case Left(_) => t
      case Right(ss) => if (ss.timestamp > t._2) (ss.revision, ss.timestamp) else t
    }}

    //val storeStates = responses.values.collect{ case Right(ss) if ss.revision == mostRecent._1 => ss }.toList

    val allStoreStates = responses.valuesIterator.collect { case Right(ss) => ss }.toList

    val matchingStoreStates = allStoreStates.filter { ss =>
      if (mostRecent._1 == ss.revision) true else {
        knownBehind += ss.storeId -> ss.readTimestamp
        false
      }
    }
    if (debug)
      println(s"** Matching size ${matchingStoreStates.size} >= $threshold")

    if (matchingStoreStates.size >= threshold) {
      // The current refcount is the one with the highest updateSerial
      val refcount = matchingStoreStates.foldLeft(ObjectRefcount(-1,0))((ref, ss) => if (ss.refcount.updateSerial > ref.updateSerial) ss.refcount else ref)
      val revision = matchingStoreStates.head.revision
      val timestamp = matchingStoreStates.head.timestamp

      val readTime = matchingStoreStates.foldLeft(HLCTimestamp.now)((maxts, ss) => if (ss.readTimestamp > maxts) ss.readTimestamp else maxts)

      if (metadataOnly)
        endResult = Some(Right(MetadataObjectState(pointer, revision, refcount, timestamp, readTime)))
      else {
        try {
          val restoredObject = restoreObject(revision, refcount, timestamp, readTime, matchingStoreStates, allStoreStates, debug)
          
          // Check if the object was recently deleted but the state hasn't yet been
          // flushed out of the backends
          if restoredObject.refcount.count > 0 then
            endResult = Some(Right(restoredObject))
          else
            endResult = Some(Left(InvalidObject(pointer)))
        } catch {
          case NotRestorable(reason) =>
            if (debug)
              println(s"****** READ ERROR: $reason")
            logger.info(s"Read $readUUID object ${pointer.id} cannot be restored due to: $reason")
        }
      }
    }
  }
}

