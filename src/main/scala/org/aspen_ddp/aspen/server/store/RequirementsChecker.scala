package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction._
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}

object RequirementsChecker {

  case class ObjectErr(objectId: ObjectId, err: RequirementError) extends Exception
  case class NonObjectErr(err: RequirementError) extends Exception

  /**
    * @return Tuple of object specific errors and a list of non-object errors
    */
  def check(transactionId: TransactionId,
            txTimestamp: HLCTimestamp,
            requirements: List[TransactionRequirement],
            objects: Map[ObjectId, ObjectState],
            objectUpdates: Map[ObjectId, DataBuffer]):
  (Map[ObjectId, RequirementError], List[RequirementError]) = {

    def getState(oid: ObjectId): ObjectState = {
      objects.get(oid) match {
        case None => throw ObjectErr(oid, MissingObject())
        case Some(os) =>
          os.lockedToTransaction match {
            case None => os
            case Some(lockedTxId) =>
              if (lockedTxId != transactionId)
                throw ObjectErr(oid, TransactionCollision(lockedTxId))
              else
                os
          }
      }
    }

    var objectErrors: Map[ObjectId, RequirementError] = Map()
    var nonObjectErrors: List[RequirementError] = Nil

    for (req <- requirements) {
      try {
        req match {
          case r: LocalTimeRequirement => checkLocalTime(r)

          case r: DataUpdate =>
            checkRevision(getState(r.objectPointer.id), r.requiredRevision, txTimestamp)
            if (!objectUpdates.contains(r.objectPointer.id))
              throw ObjectErr(r.objectPointer.id, MissingObjectUpdate())

          case r: KeyValueUpdate =>
            checkKVRequirements(getState(r.objectPointer.id), transactionId, r.requiredRevision,
              txTimestamp, r.contentLock, r.requirements)

            if (!objectUpdates.contains(r.objectPointer.id))
              throw ObjectErr(r.objectPointer.id, MissingObjectUpdate())

          case r: RefcountUpdate => checkRefcount(getState(r.objectPointer.id), r.requiredRefcount)

          case r: VersionBump => checkRevision(getState(r.objectPointer.id), r.requiredRevision, txTimestamp)

          case r: RevisionLock => checkRevision(getState(r.objectPointer.id), r.requiredRevision, txTimestamp)
        }
      } catch {
        case err: ObjectErr => objectErrors += (err.objectId -> err.err)
        case err: NonObjectErr => nonObjectErrors = err.err :: nonObjectErrors
        case e: Throwable => println(s"UNEXPECTED ERROR IN TX REQ CHECK: $e")
      }
    }

    (objectErrors, nonObjectErrors)
  }

  def checkLocalTime(tsReq: LocalTimeRequirement): Unit = {
    val now = HLCTimestamp.now
    tsReq.tsRequirement match {
      case LocalTimeRequirement.Requirement.Equals =>
        if (now == tsReq.timestamp)
          throw NonObjectErr(LocalTimeError())

      case LocalTimeRequirement.Requirement.LessThan =>
        if (now < tsReq.timestamp)
          throw NonObjectErr(LocalTimeError())

      case LocalTimeRequirement.Requirement.GreaterThan =>
        if (now > tsReq.timestamp)
          throw NonObjectErr(LocalTimeError())
    }
  }

  def checkRevision(state: ObjectState, requiredRevision: ObjectRevision, txTimestamp: HLCTimestamp): Unit = {
    if (state.metadata.revision != requiredRevision)
      throw ObjectErr(state.objectId, RevisionMismatch())

    if (state.metadata.timestamp > txTimestamp)
      throw ObjectErr(state.objectId, ObjectTimestampError())
  }

  def checkRefcount(state: ObjectState, requiredRefcount: ObjectRefcount): Unit = {
    if (state.metadata.refcount != requiredRefcount)
      throw ObjectErr(state.objectId, RefcountMismatch())
  }

  def checkKVRequirements(state: ObjectState,
                          transactionId: TransactionId,
                          requiredRevision: Option[ObjectRevision],
                          txTimestamp: HLCTimestamp,
                          contentLock: Option[FullContentLock],
                          keyRequirements: List[KeyValueUpdate.KeyRequirement]): Unit = {

    requiredRevision.foreach { rev =>
      if (rev != state.metadata.revision)
        throw ObjectErr(state.objectId, RevisionMismatch())

      if (state.metadata.timestamp > txTimestamp)
        throw ObjectErr(state.objectId, ObjectTimestampError())
    }

    def checkLock(vs: ValueState, kvs: KVObjectState): Unit = {
      vs.lockedToTransaction.foreach { lockedTransactionId =>
        if (lockedTransactionId != transactionId)
          throw ObjectErr(state.objectId, TransactionCollision(lockedTransactionId))
      }
      kvs.contentLocked match {
        case None =>
        case Some(lockedTransactionId) =>
          if (lockedTransactionId != transactionId)
            throw ObjectErr(state.objectId, TransactionCollision(lockedTransactionId))
      }
    }


    state.kvState match {
      case None => throw ObjectErr(state.objectId, ObjectTypeError())
      case Some(kvs) =>

        // Check for full content lock
        contentLock.foreach { cl =>
          val expected = cl.fullContents.map(kr => kr.key -> kr.revision).toSet
          val actual = kvs.content.map(t => t._1 -> t._2.revision).toSet

          if (expected != actual)
            throw ObjectErr(state.objectId, ContentMismatch())

          kvs.content.valuesIterator.foreach(checkLock(_, kvs))
        }

        // Check for object revision lock and read-only locks
        if (requiredRevision.nonEmpty && kvs.rangeLocks.nonEmpty)
          throw ObjectErr(state.objectId, TransactionCollision(kvs.rangeLocks.head))

        for (req <- keyRequirements) {
          req match {
            case r: KeyValueUpdate.KeyRevision => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) =>
                checkLock(vs, kvs)
                if (vs.revision != r.revision)
                  throw ObjectErr(state.objectId, RevisionMismatch())
                if (vs.timestamp > txTimestamp)
                  throw ObjectErr(state.objectId, KeyTimestampError())
            }

            case r: KeyValueUpdate.KeyObjectRevision =>
              if (state.metadata.revision != r.revision)
                throw ObjectErr(state.objectId, RevisionMismatch())

            case r: KeyValueUpdate.WithinRange =>
              kvs.min.foreach { min =>
                if (r.ordering.compare(r.key, min) < 0)
                  throw ObjectErr(state.objectId, WithinRangeError())
              }
              kvs.max.foreach { max =>
                if (r.ordering.compare(r.key, max) > 0)
                  throw ObjectErr(state.objectId, WithinRangeError())
              }

            case r: KeyValueUpdate.Exists => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) => checkLock(vs, kvs)
            }

            case r: KeyValueUpdate.MayExist => kvs.content.get(r.key) match {
              case None =>
                if (kvs.noExistenceLocks.contains(r.key))
                  throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) => checkLock(vs, kvs)
            }

            case r: KeyValueUpdate.DoesNotExist => kvs.content.get(r.key) match {
              case None => kvs.noExistenceLocks.get(r.key).foreach { txid =>
                throw ObjectErr(state.objectId, TransactionCollision(txid))
              }
              case Some(_) => throw ObjectErr(state.objectId, KeyExistenceError())
            }

            case r: KeyValueUpdate.TimestampEquals => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) =>
                if (vs.timestamp != r.timestamp)
                  throw ObjectErr(state.objectId, KeyTimestampError())
                checkLock(vs, kvs)
            }

            case r: KeyValueUpdate.TimestampLessThan => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) =>
                if (vs.timestamp < r.timestamp)
                  throw ObjectErr(state.objectId, KeyTimestampError())
                checkLock(vs, kvs)
            }

            case r: KeyValueUpdate.TimestampGreaterThan => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) =>
                if (vs.timestamp > r.timestamp)
                  throw ObjectErr(state.objectId, KeyTimestampError())
                checkLock(vs, kvs)
            }
          }
        }
    }
  }
}
