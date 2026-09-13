package org.aspen_ddp.aspen.server.store

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
  case class Result(skipped: Set[ObjectId], failed: Set[ObjectId]) {

    /** Objects that must not be committed by this transaction */
    def uncommittable: Set[ObjectId] = skipped | failed
  }

  def apply(transactionId: TransactionId,
            timestamp: HLCTimestamp,
            requirements: List[TransactionRequirement],
            objects: Map[ObjectId, ObjectState],
            objectUpdates: Map[ObjectId, DataBuffer]): Result = {

    // Get a list of all objects that are unfit for accepting the changes in this transaction
    val skippedObjects = RequirementsChecker.check(transactionId, timestamp, requirements, objects, objectUpdates)._1.keySet

    var failedObjects: Set[ObjectId] = Set()

    // Filter out all non-object requirements and any objects not fit for accepting tx changes
    val reqIter = requirements.iterator.filter(r => r.isInstanceOf[TransactionObjectRequirement]).map { r =>
      val tr = r.asInstanceOf[TransactionObjectRequirement]
      (tr.objectPointer, tr)
    }.filter { t => !skippedObjects.contains(t._1.id) }

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
              case DataUpdateOperation.Append => s.data = s.data.append(objectUpdates(ptr.id))
            }

          case r: KeyValueUpdate =>

            val state = objects(ptr.id)
            val kvs = state.kvState.get

            r.requiredRevision.foreach { _ =>
              state.metadata = state.metadata.copy(revision=ObjectRevision(transactionId), timestamp=timestamp)
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

          case _: RevisionLock =>
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

    Result(skippedObjects, failedObjects)
  }
}
