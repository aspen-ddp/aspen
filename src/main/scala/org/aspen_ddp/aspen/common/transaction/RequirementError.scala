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

