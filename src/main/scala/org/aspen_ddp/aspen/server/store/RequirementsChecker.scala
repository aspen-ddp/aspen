package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.ida.AppendDelta
import org.aspen_ddp.aspen.common.objects.{KeyValueOperation, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction._
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import scribe.Logging

import scala.language.implicitConversions

object RequirementsChecker extends Logging {

  case class ObjectErr(objectId: ObjectId, err: RequirementError) extends Exception
  case class NonObjectErr(err: RequirementError) extends Exception

  /**
    * @param maxSliceSize The store's effective slice size limit. None disables the size check. See checkSliceSize.
    * @param applying True for the re-check of a committed transaction, which takes no new locks. Locks exist to
    *                 serialize commit votes. The transaction's outcome is already decided, so locks held by other
    *                 transactions (and the exclusive preference window) are not enforced then: only whether this
    *                 store's state satisfies the requirements. A lock holder whose requirements conflict with this
    *                 transaction's can no longer commit, so enforcing its lock would only turn this store's part of
    *                 a committed update into a missed update.
    * @param nanoTime Clock for the exclusive preference window
    * @return Tuple of object specific errors and a list of non-object errors
    */
  def check(transactionId: TransactionId,
            txTimestamp: HLCTimestamp,
            requirements: List[TransactionRequirement],
            objects: Map[ObjectId, ObjectState],
            objectUpdates: Map[ObjectId, DataBuffer],
            maxSliceSize: Option[Long] = None,
            applying: Boolean = false,
            nanoTime: () => Long = () => System.nanoTime()):
  (Map[ObjectId, RequirementError], List[RequirementError]) = {

    def collide(oid: ObjectId, holders: Iterable[TransactionId]): Unit =
      holders.find(_ != transactionId).foreach { other =>
        throw ObjectErr(oid, TransactionCollision(other))
      }

    /** Object-level locks: lockedToTransaction and a KV object's content lock. Every requirement collides with them */
    def checkObjectLocks(oid: ObjectId, os: ObjectState): Unit =
      collide(oid, os.lockedToTransaction)
      os.kvState.foreach(kvs => collide(oid, kvs.contentLocked))

    /** Locks on parts of a KV object (keys, absent keys, ranges). Requirements on the whole object collide with them */
    def checkPartLocks(oid: ObjectId, os: ObjectState): Unit =
      collide(oid, os.lockedWriteTransactions -- os.sharedLocks)

    def loaded(oid: ObjectId): ObjectState =
      objects.get(oid) match
        case None => throw ObjectErr(oid, MissingObject())
        case Some(os) =>
          if os.reloadRequired then
            throw ObjectErr(oid, StaleObjectState())
          os

    /** State for every object requirement except SharedRevisionLock. `wholeObject` requirements also collide with locks
      * on parts of a KV object, and vice versa, since part-level requirements collide with object-level locks. Held
      * shared locks block it and open the exclusive preference window so that new shared lockers back off until this
      * transaction can get in.
      */
    def getState(oid: ObjectId, wholeObject: Boolean = true): ObjectState =
      val os = loaded(oid)
      if !applying then
        checkObjectLocks(oid, os)
        os.sharedLocks.find(_ != transactionId).foreach { sharedTxId =>
          os.exclusiveWaitUntil = nanoTime() + SharedRevisionLock.ExclusivePreferenceWindow.toNanos
          throw ObjectErr(oid, TransactionCollision(sharedTxId))
        }
        if wholeObject then
          checkPartLocks(oid, os)
      os

    /** A shared lock excludes every transaction that modifies or exclusively locks the object */
    def getSharedState(oid: ObjectId): ObjectState =
      val os = loaded(oid)
      if !applying then
        checkObjectLocks(oid, os)
        checkPartLocks(oid, os)
        if os.exclusiveWaitUntil != 0 && nanoTime() - os.exclusiveWaitUntil < 0 then
          // The waiting exclusive transaction is not tracked, so the collision names this one. It is never a
          // completed transaction, which keeps the driver from treating the collision as a stale race.
          throw ObjectErr(oid, TransactionCollision(transactionId))
      os

    var objectErrors: Map[ObjectId, RequirementError] = Map()
    var nonObjectErrors: List[RequirementError] = Nil

    for (req <- requirements) {
      try {
        req match {
          case r: LocalTimeRequirement => checkLocalTime(r)

          case r: DataUpdate =>
            val state = getState(r.objectPointer.id)
            checkRevision(state, r.requiredRevision, txTimestamp)
            if (!objectUpdates.contains(r.objectPointer.id))
              throw ObjectErr(r.objectPointer.id, MissingObjectUpdate())

            if r.operation == DataUpdateOperation.Append || r.operation == DataUpdateOperation.WriteRange then
              // A payload that cannot be decoded is a mismatch, not an unexpected failure
              val matches =
                try
                  AppendDelta.decode(objectUpdates(r.objectPointer.id)).matches(state.data)
                catch
                  case _: IllegalArgumentException => false
              if !matches then
                throw ObjectErr(r.objectPointer.id, AppendMismatch())

            maxSliceSize.foreach { limit =>
              val update = objectUpdates(r.objectPointer.id)
              val newSize = r.operation match
                case DataUpdateOperation.Overwrite => update.size.toLong
                // The delta matched the current slice above, so applying it yields exactly newSliceLength bytes
                case DataUpdateOperation.Append | DataUpdateOperation.WriteRange =>
                  AppendDelta.decode(update).newSliceLength.toLong
              checkSliceSize(r.objectPointer.id, state.data.size.toLong, newSize, limit)
            }

          case r: KeyValueUpdate =>
            // Requirements that lock the whole object, rather than only the keys they name
            val wholeObject = r.requiredRevision.nonEmpty || r.contentLock.nonEmpty ||
              r.requirements.exists(_.isInstanceOf[KeyValueUpdate.KeyObjectRevision])
            val state = getState(r.objectPointer.id, wholeObject)
            checkKVRequirements(state, transactionId, r.requiredRevision,
              txTimestamp, r.contentLock, r.requirements, ignoreLocks = applying)

            if (!objectUpdates.contains(r.objectPointer.id))
              throw ObjectErr(r.objectPointer.id, MissingObjectUpdate())

            // checkKVRequirements guarantees kvState is defined. The measure is the serialized KVObjectState,
            // which is what the store persists for the object (RequirementsApplyer sets data = kvs.encode()).
            maxSliceSize.foreach { limit =>
              val kvs = state.kvState.get
              val ops = KeyValueOperation.decode(objectUpdates(r.objectPointer.id), ObjectRevision(transactionId),
                txTimestamp)
              checkSliceSize(r.objectPointer.id, kvs.encodedSize.toLong, kvs.encodedSizeAfter(ops).toLong, limit)
            }

          case r: RefcountUpdate => checkRefcount(getState(r.objectPointer.id), r.requiredRefcount)

          case r: VersionBump => checkRevision(getState(r.objectPointer.id), r.requiredRevision, txTimestamp)

          case r: RevisionLock => checkRevision(getState(r.objectPointer.id), r.requiredRevision, txTimestamp)

          case r: SharedRevisionLock =>
            checkRevision(getSharedState(r.objectPointer.id), r.requiredRevision, txTimestamp)
        }
      } catch {
        case err: ObjectErr => objectErrors += (err.objectId -> err.err)
        case err: NonObjectErr => nonObjectErrors = err.err :: nonObjectErrors

        // An unexpected error means the requirement could not be evaluated. Record it as a
        // failure so the store votes to abort rather than silently treating the requirement
        // as satisfied. Object-scoped requirements are attributed to their object: apply()
        // consumes only the object errors, so a failure reported as non-object would let the
        // update be applied anyway.
        case e: Throwable =>
          req match
            case tor: TransactionObjectRequirement =>
              logger.error(s"Unexpected error checking requirement $req of transaction " +
                s"$transactionId against object ${tor.objectPointer.id}. Failing the requirement.", e)
              objectErrors += (tor.objectPointer.id -> RequirementCheckFailure())

            case _ =>
              logger.error(s"Unexpected error checking requirement $req of transaction " +
                s"$transactionId. Failing the requirement.", e)
              nonObjectErrors = RequirementCheckFailure() :: nonObjectErrors
      }
    }

    (objectErrors, nonObjectErrors)
  }

  /** Rejects an update whose resulting slice exceeds the limit, unless it is no larger than the current slice.
    * Allowing non-growing updates means an object that is already oversized (e.g. created before the limit
    * applied) can always be shrunk or deleted.
    */
  def checkSliceSize(objectId: ObjectId, currentSize: Long, newSize: Long, maxSliceSize: Long): Unit =
    if newSize > maxSliceSize && newSize > currentSize then
      throw ObjectErr(objectId, SliceSizeExceeded(newSize, maxSliceSize))

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
                          keyRequirements: List[KeyValueUpdate.KeyRequirement],
                          ignoreLocks: Boolean = false): Unit = {

    requiredRevision.foreach { rev =>
      if (rev != state.metadata.revision)
        throw ObjectErr(state.objectId, RevisionMismatch())

      if (state.metadata.timestamp > txTimestamp)
        throw ObjectErr(state.objectId, ObjectTimestampError())
    }

    /** Other transactions' lock, if any. None when locks are ignored */
    def otherLock(lock: Option[TransactionId]): Option[TransactionId] =
      if ignoreLocks then None else lock.filter(_ != transactionId)

    def checkContentLock(kvs: KVObjectState): Unit =
      otherLock(kvs.contentLocked).foreach { lockedTransactionId =>
        throw ObjectErr(state.objectId, TransactionCollision(lockedTransactionId))
      }

    def checkLock(vs: ValueState, kvs: KVObjectState): Unit = {
      otherLock(vs.lockedToTransaction).foreach { lockedTransactionId =>
        throw ObjectErr(state.objectId, TransactionCollision(lockedTransactionId))
      }
      checkContentLock(kvs)
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

          // An empty object has no values to check, so the content lock is checked directly too
          checkContentLock(kvs)
          kvs.content.valuesIterator.foreach(checkLock(_, kvs))
        }

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
                if (otherLock(kvs.noExistenceLocks.get(r.key)).nonEmpty)
                  throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) => checkLock(vs, kvs)
            }

            case r: KeyValueUpdate.DoesNotExist => kvs.content.get(r.key) match {
              case None => otherLock(kvs.noExistenceLocks.get(r.key)).foreach { txid =>
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
