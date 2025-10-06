package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.AmoebaError
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.common.transaction.TransactionDescription

sealed abstract class TransactionError extends AmoebaError

abstract class TransactionCreationError extends TransactionError

/** Used if multiple data modifications are attempted on the same object within a single transaction */
final case class MultipleDataUpdatesToObject(objectPointer:ObjectPointer) extends TransactionCreationError

/** Used if revision modifications and revision-locks are applied to the same object */
final case class ConflictingRequirements(objectPointer:ObjectPointer) extends TransactionCreationError

/** Used if multiple refcount modifications are attempted on the same object within a single transaction */
final case class MultipleRefcountUpdatesToObject(objectPointer:ObjectPointer) extends TransactionCreationError

/** Thrown if attempts are made to add content to a transaction that has already had commit() called */
final case class PostCommitTransactionModification() extends TransactionCreationError

/** Used when procedure adding content to a transaction detects a logical flaw that causes the overall
  * Transaction to become invalid.
  *
  * For example, while attempting to insert key/value pairs into a BTree host it could be discovered that one or more
  * of the keys is outside the valid key range owned by the host. In this case, the transaction cannot be allowed to
  * succeed so the transaction will be failed and the "reason" attribute will be set to an instance of KeyOutOfRange.
  *
  */
final case class InvalidTransaction(reason: Throwable) extends TransactionCreationError


abstract class TransactionProcessingError extends TransactionError {
  def txd: TransactionDescription
}

final case class TransactionAborted(txd: TransactionDescription) extends TransactionProcessingError

final case class TransactionTimedOut(txd: TransactionDescription) extends TransactionProcessingError
