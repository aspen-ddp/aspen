package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.{AspenClient, ObjectState, Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, KeyRevision}
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
  * The key and object-level proofs change no content (only the object's timestamp moves, for the object-level proof's
  * refcount update). So all of them disable missed-update tracking (`AspenClient.newRepairTransaction`) and cannot
  * create repair entries. Each completes with true when the proof committed, false otherwise.
  *
  * None carries a pin (the object-level proof sets the refcount it requires), and none needs one: a replay changes
  * nothing either, so every proof is marked `replaySafe`.
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

  /** The object level of either object type: its refcount and its revision (a DataObject's data, a KeyValue object's
    * min, max, left and right), proven together in one transaction, as of one moment. Neither part covers the other:
    * a refcount change moves no revision, and a write moving the revision leaves the refcount as it was.
    *
    *  - A `RefcountUpdate` requiring the read's refcount and setting that same refcount. Its exclusive object lock is
    *    the one every refcount change takes. It is a real write: a backend commit that raises the object timestamp.
    *  - A `SharedRevisionLock` of the read's revision. Its check fails at every store that applied a later write moving
    *    the revision (a DataObject's data write; a KeyValue object's min, max, left or right change, each of which
    *    requires a revision, Q8). The refcount update's exclusive lock already collides with every write lock, so a
    *    shared lock suffices: it adds the revision check and, beside the transaction's own exclusive lock, excludes
    *    nothing more an exclusive one would.
    *
    * A lock beside an update of one object is refused, but a refcount update is not an update in that sense
    * (`TransactionBuilder`, `DescriptionValidator`), so both fit in one transaction. Keys have proofs of their own
    */
  def proveObjectLevel(client: AspenClient, read: ObjectState): Future[Boolean] =
    prove(client, s"object level of ${read.pointer.id}"): tx =>
      tx.setRefcount(read.pointer, read.refcount, read.refcount)
      tx.sharedLockRevision(read.pointer, read.revision)

  /** The transaction a proof is built on: missed-update tracking disabled */
  private[repair] def newProofTransaction(client: AspenClient): Transaction = client.newRepairTransaction()

  private def prove(client: AspenClient, what: String)(build: Transaction => Unit): Future[Boolean] =
    given ExecutionContext = client.clientContext
    Future.unit
      .flatMap: _ =>
        val tx = newProofTransaction(client)
        tx.replaySafe("repair proof changes nothing")
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
