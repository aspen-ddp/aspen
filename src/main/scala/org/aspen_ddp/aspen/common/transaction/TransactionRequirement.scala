package org.aspen_ddp.aspen.common.transaction

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer, ObjectRefcount, ObjectRevision}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

sealed abstract class TransactionRequirement

/** Holds when the local time of each store checking it equals, is less than, or is greater than `timestamp`
  * (`Equals`, `LessThan`, `GreaterThan`). Every store hosting an object of the transaction checks it against its own
  * clock when it votes, so stores can disagree, and a store's failure aborts its vote for all of its objects.
  * Test-only: no production code constructs it and TransactionBuilder has no method for it (transaction-requirements
  * 2.4). A per-store clock is no basis for a decision the stores must share; use HLC object timestamps instead.
  */
case class LocalTimeRequirement(timestamp: HLCTimestamp,
                                tsRequirement: LocalTimeRequirement.Requirement.Value) extends TransactionRequirement

object LocalTimeRequirement {

  object Requirement extends Enumeration {
    val Equals: Value = Value("Equals")
    val LessThan: Value = Value("LessThan")
    val GreaterThan: Value = Value("GreaterThan")
  }
}


sealed abstract class TransactionObjectRequirement extends TransactionRequirement {
  val objectPointer: ObjectPointer
}

case class DataUpdate(
                       objectPointer: ObjectPointer,
                       requiredRevision: ObjectRevision,
                       operation: DataUpdateOperation.Value) extends TransactionObjectRequirement

case class RefcountUpdate(
                           objectPointer: ObjectPointer,
                           requiredRefcount: ObjectRefcount,
                           newRefcount: ObjectRefcount) extends TransactionObjectRequirement

case class VersionBump(
                        objectPointer: ObjectPointer,
                        requiredRevision: ObjectRevision) extends TransactionObjectRequirement

case class ExclusiveRevisionLock(
                         objectPointer: ObjectPointer,
                         requiredRevision: ObjectRevision) extends TransactionObjectRequirement

/** Like ExclusiveRevisionLock, but any number of transactions may hold it on the same object at once. It excludes only
  * transactions that modify or exclusively lock the object. An exclusive requirement that collides with held shared
  * locks opens an ExclusivePreferenceWindow during which new shared locks are refused, so a writer cannot be starved
  * by a continuous stream of shared lockers.
  *
  * Only a writer blocked by nothing but held shared locks at a store opens the window there: one that also fails on
  * anything else (a stale revision, another lock, a non-object requirement) could not get in after the holders
  * release, so it opens none. A writer that opens it and then aborts for reasons elsewhere, or never retries, still
  * makes shared lockers abort with TransactionCollision for up to ExclusivePreferenceWindow.
  */
case class SharedRevisionLock(
                               objectPointer: ObjectPointer,
                               requiredRevision: ObjectRevision) extends TransactionObjectRequirement

object SharedRevisionLock:
  val ExclusivePreferenceWindow: FiniteDuration = 250.millis

sealed abstract class KeyValueTransactionRequirement extends TransactionObjectRequirement {
  override val objectPointer: KeyValueObjectPointer
}

/** @param deletesKeys Set when the operations contain a key `Delete`. `DeleteMin`/`DeleteMax`/`DeleteLeft`/
  *                    `DeleteRight` do not set it, and a use of them without a required revision is a traceless
  *                    change this flag does not cover.
  * @param pinned      Set when one of the update's KeyRevision keys is written (Inserted or Deleted) by its
  *                    operations: the update is a pin (Pins). Stores holding the object check the claim
  *                    (RequirementsChecker, FalsePin), so stores that do not hold it may rely on it.
  */
case class KeyValueUpdate(
                           objectPointer: KeyValueObjectPointer,
                           requiredRevision: Option[ObjectRevision],
                           contentLock: Option[KeyValueUpdate.FullContentLock],
                           requirements: List[KeyValueUpdate.KeyRequirement],
                           deletesKeys: Boolean = false,
                           pinned: Boolean = false) extends KeyValueTransactionRequirement

object KeyValueUpdate {

  sealed abstract class Requirement

  case class FullContentLock(fullContents: List[KeyRevision]) extends Requirement

  sealed abstract class KeyRequirement extends Requirement {
    val key: Key
  }

  case class KeyRevision(key: Key, revision: ObjectRevision) extends KeyRequirement
  case class KeyObjectRevision(key: Key, revision: ObjectRevision) extends KeyRequirement
  case class WithinRange(key: Key, ordering: KeyOrdering) extends KeyRequirement
  case class Exists(key: Key) extends KeyRequirement
  case class MayExist(key: Key) extends KeyRequirement
  case class DoesNotExist(key: Key) extends KeyRequirement
  /** The key exists and its timestamp equals `timestamp` */
  case class TimestampEquals(key: Key, timestamp: HLCTimestamp) extends KeyRequirement
  /** The key exists and its timestamp is less than `timestamp` */
  case class TimestampLessThan(key: Key, timestamp: HLCTimestamp) extends KeyRequirement
  /** The key exists and its timestamp is greater than `timestamp` */
  case class TimestampGreaterThan(key: Key, timestamp: HLCTimestamp) extends KeyRequirement

}

