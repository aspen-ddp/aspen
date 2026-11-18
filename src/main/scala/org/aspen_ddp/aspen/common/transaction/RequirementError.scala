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

/** The store's in-memory state of the object was discarded (deleted, or no longer matching the backend) while
  * transactions held locks on it. Purely local, like RequirementCheckFailure.
  */
case class StaleObjectState() extends RequirementError

/** A backend commit of the object failed on this store: every later transaction on the object votes abort until the
  * store reloads. Purely local, like RequirementCheckFailure.
  */
case class BackendCommitFailed() extends RequirementError

/** The object is quarantined on this store (Frontend.quarantine): a recovered commit vote could not apply its committed
  * update, and the store's copy is not trusted until repair verifies it. Purely local, like RequirementCheckFailure.
  */
case class ObjectQuarantined() extends RequirementError

/** This store was rebuilt, and a transaction its lost incarnation voted commit on may still commit and may lock the
  * object (Frontend.inheritedVoteLock): the rebuilt store holds none of that transaction's locks, so it grants no
  * commit vote on the object until its peers show the transaction resolved. Purely local, like
  * RequirementCheckFailure.
  */
case class InheritedVoteLock() extends RequirementError

/** A transaction allocates an object this store already holds (in memory or in its backend): a replayed allocation,
  * which must not overwrite the live object (transaction-requirements 9.4). Purely local, like RequirementCheckFailure.
  */
case class ObjectExists() extends RequirementError

/** A transaction prepared on this store only by store-side drivers (TxPrepare.fromClient = false) got no client
  * prepare within Frontend.missingDataWait. A driver's prepare never gathers a fresh commit vote (prefer-abort), so
  * the store votes abort without checking the requirements. Purely local, like RequirementCheckFailure.
  */
case class NoClientPrepare() extends RequirementError

/** A KeyValueUpdate's operations contain a key Delete but the update's deletesKeys flag is not set. Purely local,
  * like RequirementCheckFailure.
  */
case class UnflaggedDelete() extends RequirementError

/** A KeyValueUpdate with no required revision, no content lock and no allocation of its object deletes a key it holds
  * no KeyRevision on, or changes the object's minimum, maximum, left or right. TransactionBuilder refuses such an
  * update (BlindKeyValueWrite); the store refuses it too (transaction-requirements 2.5). Purely local, like
  * RequirementCheckFailure.
  */
case class BlindWrite() extends RequirementError

/** A KeyValueUpdate is flagged `pinned` but writes none of the keys it carries a KeyRevision on. Stores that do not
  * hold the object count the flag as the transaction's pin, so the stores holding it refuse a false one. Purely
  * local, like RequirementCheckFailure.
  */
case class FalsePin() extends RequirementError

/** The transaction description breaks a rule the store checks before voting (DescriptionValidator): its pool IDAs,
  * primary object, designated leader, one update per object, or the pin rule. The store's whole vote is abort.
  * Purely local, like RequirementCheckFailure.
  */
case class InvalidDescription(reason: String) extends RequirementError

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
