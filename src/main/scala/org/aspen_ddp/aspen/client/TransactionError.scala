package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.AspenError
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}

sealed abstract class TransactionError extends AspenError

abstract class TransactionCreationError extends TransactionError

/** Used if multiple data modifications are attempted on the same object within a single transaction */
final case class MultipleDataUpdatesToObject(objectPointer:ObjectPointer) extends TransactionCreationError

/** Used if revision modifications and revision-locks are applied to the same object */
final case class ConflictingRequirements(objectPointer:ObjectPointer) extends TransactionCreationError

/** Used if an append is attempted on an object whose pool IDA does not support appends (e.g. classic ReedSolomon) */
final case class AppendNotSupported(objectPointer:ObjectPointer, ida: IDA) extends TransactionCreationError

/** Used if a ranged write is attempted on an object whose pool IDA does not support them (e.g. classic ReedSolomon) */
final case class RangeWriteNotSupported(objectPointer:ObjectPointer, ida: IDA) extends TransactionCreationError

/** Used if a ranged write starts beyond the current end of the object and zero fill was not requested */
final case class RangeWriteBeyondEnd(objectPointer:ObjectPointer, currentLength: Int, offset: Int) extends TransactionCreationError

/** Used if an overwrite, append, ranged write or allocation's initial content would make an object larger than
  * the pool's derived `maxObjectSize` (its `maxSliceSize` converted through the pool's IDA), or larger than the
  * largest length the pool's IDA can represent when the pool has no limit. `maxObjectSize` is the limit that applied.
  *
  * For the initial content of a key-value object allocation the check is made on slices instead: `newLength` is
  * the slice each store would hold (`KVObjectState.allocationSliceSize`) and `maxObjectSize` is the pool's
  * effective slice limit (`StoragePool.maxSliceSize`), since that is what the stores enforce for key-value objects.
  */
final case class ObjectSizeLimitExceeded(objectPointer:ObjectPointer, newLength: Long, maxObjectSize: Long) extends TransactionCreationError

/** Thrown when a transaction has no pin (see `Pins`: an update at a required revision, a version bump, a key update
  * that requires the key's revision while writing that key, or a refcount change) and was not marked with
  * `Transaction.replaySafe`. Replaying such a transaction after it committed would succeed a second time, so the
  * builder refuses it.
  */
final case class UnpinnedTransaction(transactionId: TransactionId) extends TransactionCreationError

/** Thrown for a KeyValue update with neither a required revision nor a content lock that deletes a key it holds no
  * `KeyRevision` on, or changes min, max, left or right, outside an allocated object's initial content. Stores apply
  * blind Inserts last-writer-wins, but keep no tombstone or timestamp to order those writes by, so two of them applied
  * in opposite orders, or a crash re-apply, would leave a store permanently diverged (transaction-requirements F1).
  */
final case class BlindKeyValueWrite(objectPointer: ObjectPointer, reason: String) extends TransactionCreationError

/** Used if multiple refcount modifications are attempted on the same object within a single transaction */
final case class MultipleRefcountUpdatesToObject(objectPointer:ObjectPointer) extends TransactionCreationError

/** Thrown if attempts are made to add content to a transaction that has already had commit() called */
final case class PostCommitTransactionModification() extends TransactionCreationError

/** Thrown if abort() or abortAndThrow() is called on a transaction that has already had commit() called.
  * Once the commit process has begun the outcome is decided by the stores, so the abort cannot take effect.
  * `reason` is the abort reason that was rejected.
  */
final case class AbortAfterCommit(reason: Throwable) extends TransactionCreationError

/** Used when procedure adding content to a transaction detects a logical flaw that causes the overall
  * Transaction to become invalid.
  *
  * For example, while attempting to insert key/value pairs into a BTree node it could be discovered that one or more
  * of the keys is outside the valid key range owned by the node. In this case, the transaction cannot be allowed to
  * succeed so the transaction will be failed and the "reason" attribute will be set to an instance of KeyOutOfRange.
  *
  */
final case class InvalidTransaction(reason: Throwable) extends TransactionCreationError


abstract class TransactionProcessingError extends TransactionError {
  def txd: TransactionDescription
}

final case class TransactionAborted(txd: TransactionDescription) extends TransactionProcessingError

/** The transaction may have committed or aborted and the client can no longer find out: it was not resolved within the
  * transaction horizon, so no store will join it again. A new instance of the transaction can never change the
  * outcome, but an instance already in progress at stores may still resolve it. Not a subtype of `TransactionAborted`,
  * which is final; retry helpers retry both.
  */
final case class TransactionOutcomeUnknown(txd: TransactionDescription) extends TransactionProcessingError

