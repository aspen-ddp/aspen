package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.ida.EncodedAppendDelta
import org.aspen_ddp.aspen.common.objects._
import org.aspen_ddp.aspen.common.transaction._
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import scribe.Logging

import scala.language.implicitConversions

object RequirementsApplyer extends Logging {

  /** Outcome of applying a transaction's updates to local object state.
    *
    * @param skipped Objects that failed their requirement checks. Their state is untouched.
    * @param failed  Objects whose update threw an unexpected error partway through application.
    *                Their in-memory state is potentially corrupt: it must not be committed and
    *                the caller must discard it rather than serve it to later readers.
    */
  case class Result(skippedReasons: Map[ObjectId, RequirementError], failed: Set[ObjectId]) {

    def skipped: Set[ObjectId] = skippedReasons.keySet

    /** Objects that must not be committed by this transaction */
    def uncommittable: Set[ObjectId] = skipped | failed
  }

  /** Objects the requirements modify: those named by a DataUpdate, KeyValueUpdate, RefcountUpdate or VersionBump.
    * Objects named only by ExclusiveRevisionLock or SharedRevisionLock are unchanged. Exhaustive, with no wildcard, so a new
    * requirement must be classified here (an unchanged object gets no backend commit).
    */
  def changedObjects(requirements: List[TransactionRequirement]): Set[ObjectId] =
    requirements.iterator.flatMap { (r: TransactionRequirement) =>
      r match
        case r: DataUpdate => Some(r.objectPointer.id)
        case r: KeyValueUpdate => Some(r.objectPointer.id)
        case r: RefcountUpdate => Some(r.objectPointer.id)
        case r: VersionBump => Some(r.objectPointer.id)
        case _: ExclusiveRevisionLock => None
        case _: SharedRevisionLock => None
        case _: LocalTimeRequirement => None
    }.toSet

  /** The objects in `objects` that already show `transactionId`'s update, which proves that it committed: only applying
    * a committed update, or repair and rebuild copying a committed state, gives an object that revision. An object
    * shows it when its revision is the transaction's, for an update that moves the revision (DataUpdate, VersionBump,
    * KeyValueUpdate with a required revision), or when a key carries it, for a KeyValueUpdate without one. A
    * RefcountUpdate, or a KeyValueUpdate that only deletes, leaves no trace.
    */
  def appliedBy(transactionId: TransactionId,
                requirements: List[TransactionRequirement],
                objects: Map[ObjectId, ObjectState]): Set[ObjectId] =
    val rev = ObjectRevision(transactionId)
    def atRevision(oid: ObjectId): Option[ObjectState] = objects.get(oid).filter(_.metadata.revision == rev)
    requirements.iterator.flatMap { (r: TransactionRequirement) =>
      r match
        case r: DataUpdate => atRevision(r.objectPointer.id)
        case r: VersionBump => atRevision(r.objectPointer.id)
        case r: KeyValueUpdate if r.requiredRevision.isDefined => atRevision(r.objectPointer.id)
        case r: KeyValueUpdate =>
          objects.get(r.objectPointer.id).filter(_.kvState.exists(_.content.valuesIterator.exists(_.revision == rev)))
        case _ => None
    }.map(_.objectId).toSet

  def apply(transactionId: TransactionId,
            timestamp: HLCTimestamp,
            requirements: List[TransactionRequirement],
            objects: Map[ObjectId, ObjectState],
            objectUpdates: Map[ObjectId, DataBuffer],
            maxSliceSize: Option[Long] = None): Result = {

    // Get a list of all objects that are unfit for accepting the changes in this transaction
    val skippedReasons = RequirementsChecker.check(transactionId, timestamp, requirements, objects, objectUpdates,
      maxSliceSize, applying = true)._1

    var failedObjects: Set[ObjectId] = Set()

    // Filter out all non-object requirements and any objects not fit for accepting tx changes
    val reqIter = requirements.iterator.filter(r => r.isInstanceOf[TransactionObjectRequirement]).map { r =>
      val tr = r.asInstanceOf[TransactionObjectRequirement]
      (tr.objectPointer, tr)
    }.filter { t => !skippedReasons.contains(t._1.id) }

    // Note: reqIter is lazy, so failures recorded below are visible to later iterations
    for ((ptr, req) <- reqIter if !failedObjects.contains(ptr.id)) {
      try {
        req match {

          case r: DataUpdate =>
            val s = objects(ptr.id)
            s.metadata = s.metadata.copy(
              revision=ObjectRevision(transactionId),
              timestamp=timestamp)

            r.operation match {
              case DataUpdateOperation.Overwrite => s.data = objectUpdates(ptr.id)

              case DataUpdateOperation.Append | DataUpdateOperation.WriteRange =>
                s.data = EncodedAppendDelta.parse(objectUpdates(ptr.id)).applyTo(s.data)
            }

          case r: KeyValueUpdate =>

            val state = objects(ptr.id)
            val kvs = state.kvState.get

            // Every update advances the object's timestamp, so the object's timestamp tells repair whether a store
            // has caught up. Only an update with a required revision moves the revision.
            state.metadata = r.requiredRevision match {
              case Some(_) => state.metadata.copy(revision=ObjectRevision(transactionId), timestamp=timestamp)
              case None => state.metadata.copy(timestamp=timestamp)
            }

            val kvops = KeyValueOperation.decode(objectUpdates(ptr.id), ObjectRevision(transactionId), timestamp)

            for (op <- kvops) {
              op match {
                case o: SetMin => kvs.min = Some(Key(o.value))
                case o: SetMax => kvs.max = Some(Key(o.value))
                case o: SetLeft => kvs.left = Some(Value(o.value))
                case o: SetRight => kvs.right = Some(Value(o.value))
                case _: DeleteMin => kvs.min = None
                case _: DeleteMax => kvs.max = None
                case _: DeleteLeft => kvs.left = None
                case _: DeleteRight => kvs.right = None
                case o: Insert =>
                  val vs = new ValueState(Value(o.value), ObjectRevision(transactionId), timestamp, None)
                  kvs.content += (o.key -> vs)
                case o: Delete => kvs.content -= o.key
              }
            }

            state.data = kvs.encode()

          case r: RefcountUpdate =>
            val s = objects(ptr.id)
            s.metadata = s.metadata.copy(
              refcount=r.newRefcount,
              timestamp=timestamp)

          case _: VersionBump =>
            val s = objects(ptr.id)
            s.metadata = s.metadata.copy(
              revision=ObjectRevision(transactionId),
              timestamp=timestamp)

          case _: ExclusiveRevisionLock =>

          case _: SharedRevisionLock =>
        }
      } catch {
        // Updates are applied in place, so an object that throws partway through is left in a
        // partially-updated, potentially corrupt state. Mark it failed so the caller neither
        // commits it nor keeps it in memory.
        case e: Throwable =>
          logger.error(s"Unexpected error applying requirement $req of transaction " +
            s"$transactionId to object ${ptr.id}. Object state is potentially corrupt " +
            s"and will not be committed.", e)
          failedObjects += ptr.id
      }
    }

    Result(skippedReasons, failedObjects)
  }
}
