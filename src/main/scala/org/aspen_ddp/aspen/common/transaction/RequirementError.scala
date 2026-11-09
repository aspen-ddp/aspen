package org.aspen_ddp.aspen.common.transaction

sealed abstract class RequirementError

case class TransactionCollision(txid: TransactionId) extends RequirementError

case class LocalTimeError() extends RequirementError
case class ObjectTimestampError() extends RequirementError
case class MissingObject() extends RequirementError
case class MissingObjectUpdate() extends RequirementError
case class ObjectTypeError() extends RequirementError
case class RevisionMismatch() extends RequirementError
case class RefcountMismatch() extends RequirementError
case class KeyTimestampError() extends RequirementError
case class KeyExistenceError() extends RequirementError
case class ContentMismatch() extends RequirementError
case class WithinRangeError() extends  RequirementError

/** An unexpected error prevented the store from evaluating a requirement.
  *
  * The store cannot know whether the requirement is satisfied, so it must vote to abort
  * rather than assume success. Purely local: RequirementErrors are never placed on the wire.
  */
case class RequirementCheckFailure() extends RequirementError

/** A prepare for a transaction the store does not know arrived after the store's horizon (Frontend.horizon): the host's
  * status cache may have forgotten it finalized, so it must not be checked afresh. Its requirements are not checked and
  * no locks are taken. Purely local, like RequirementCheckFailure.
  */
case class TransactionTooOld() extends RequirementError

/** The store's in-memory state of the object was discarded (deleted, or no longer matching the backend) while
  * transactions held locks on it. Purely local, like RequirementCheckFailure.
  */
case class StaleObjectState() extends RequirementError

/** A backend commit of the object failed on this store: every later transaction on the object votes abort until the
  * store reloads. Purely local, like RequirementCheckFailure.
  */
case class BackendCommitFailed() extends RequirementError

/** A KeyValueUpdate's operations contain a key Delete but the update's deletesKeys flag is not set. Purely local,
  * like RequirementCheckFailure.
  */
case class UnflaggedDelete() extends RequirementError

/** An Append delta does not apply to the store's current slice.
  *
  * Either the payload could not be decoded or its expected slice length or trailer differs from
  * what the store holds. See DataUpdateOperation.Append.
  */
case class AppendMismatch() extends RequirementError

/** An update would grow the store's slice of an object beyond the store's max slice size.
  *
  * Updates that leave the slice no larger than it currently is are never rejected, so shrinking or deleting an
  * object that is already oversized always succeeds. Clients should check against the pool's maxObjectSize
  * instead of relying on this error.
  *
  * @param newSize      Size in bytes of the slice the update would produce
  * @param maxSliceSize The store's effective slice size limit
  */
case class SliceSizeExceeded(newSize: Long, maxSliceSize: Long) extends RequirementError
