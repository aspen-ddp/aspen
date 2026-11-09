package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectPointer, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, FullContentLock, KeyRevision}
import scribe.Logging

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Proof transactions: the repair scan's fallback for parts of an object that stay locked across
  * `RepairPolicy.proofAfterDeferrals` passes.
  *
  * Each is an ordinary transaction from the repairing host's client that requires what a read saw and changes nothing
  * else. Its requirements take the same locks a real write of those parts does, so it commits only if a write
  * threshold of stores agreed with the read on them at that moment: the read is then current on those parts, and the
  * scan reconciles them from it (still under the snapshot check). An abort means they were changing; a later pass
  * tries again.
  *
  * The key and refcount proofs change nothing, so they disable missed-update tracking
  * (`AspenClient.newRepairTransaction`) and cannot create repair entries. The object-level proof writes the object
  * (its update moves the revision), so it keeps tracking on: a store that misses it is marked like any other.
  * Each completes with true when the proof committed, false otherwise.
  */
object RepairProofs extends Logging:

  /** `KeyRevision(key, revision)`, or `DoesNotExist(key)` when the read saw the key absent, on an update with no
    * operations
    */
  def proveKey(client: AspenClient, ptr: KeyValueObjectPointer, key: Key,
               readRevision: Option[ObjectRevision]): Future[Boolean] =
    val requirement = readRevision match
      case Some(revision) => KeyRevision(key, revision)
      case None => DoesNotExist(key)
    prove(client, s"key of ${ptr.id}")(_.update(ptr, None, None, List(requirement), Nil))

  /** A FullContentLock of the read's contents with the read's revision required, and no operations: proves the
    * revision, min, max, left and right along with every key
    */
  def proveObjectLevel(client: AspenClient, read: KeyValueObjectState): Future[Boolean] =
    val lock = FullContentLock(read.contents.iterator.map((k, vs) => KeyRevision(k, vs.revision)).toList)
    prove(client, s"object level of ${read.pointer.id}", tracked = true):
      _.update(read.pointer, Some(read.revision), Some(lock), Nil, Nil)

  /** A RefcountUpdate requiring `refcount` and setting that same refcount. Its exclusive object lock is the one every
    * refcount change takes
    */
  def proveRefcount(client: AspenClient, ptr: ObjectPointer, refcount: ObjectRefcount): Future[Boolean] =
    prove(client, s"refcount of ${ptr.id}")(_.setRefcount(ptr, refcount, refcount))

  /** The transaction a proof is built on: missed-update tracking disabled, unless `tracked` (a proof that writes) */
  private[repair] def newProofTransaction(client: AspenClient, tracked: Boolean = false): Transaction =
    if tracked then client.newTransaction() else client.newRepairTransaction()

  private def prove(client: AspenClient, what: String, tracked: Boolean = false)
                   (build: Transaction => Unit): Future[Boolean] =
    given ExecutionContext = client.clientContext
    Future.unit
      .flatMap: _ =>
        val tx = newProofTransaction(client, tracked)
        build(tx)
        tx.commit()
      .map: _ =>
        logger.info(s"Repair proof of the $what committed")
        true
      .recover:
        case _: TransactionAborted =>
          logger.debug(s"Repair proof of the $what aborted")
          false
        case NonFatal(err) =>
          logger.info(s"Repair proof of the $what failed: $err")
          false
