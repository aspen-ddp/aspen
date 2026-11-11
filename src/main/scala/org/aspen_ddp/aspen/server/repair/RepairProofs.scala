package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer, ObjectPointer, ObjectRefcount, ObjectRevision}
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
  * The object-level proof is a pure lock and changes nothing. The key and refcount proofs change no content (only the
  * object's timestamp moves). So all of them disable missed-update tracking (`AspenClient.newRepairTransaction`) and
  * cannot create repair entries. Each completes with true when the proof committed, false otherwise.
  *
  * None carries a pin (the refcount proof sets the refcount it requires), and none needs one: a replay changes
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

  /** A ExclusiveRevisionLock of the read's revision. It takes the exclusive object lock, which collides with every lock a
    * write of the object level or of any key holds, and changes nothing: no revision, timestamp or backend commit.
    * Keys and the refcount have proofs of their own
    */
  def proveObjectLevel(client: AspenClient, read: KeyValueObjectState): Future[Boolean] =
    prove(client, s"object level of ${read.pointer.id}"):
      _.exclusiveLockRevision(read.pointer, read.revision)

  /** A RefcountUpdate requiring `refcount` and setting that same refcount. Its exclusive object lock is the one every
    * refcount change takes
    */
  def proveRefcount(client: AspenClient, ptr: ObjectPointer, refcount: ObjectRefcount): Future[Boolean] =
    prove(client, s"refcount of ${ptr.id}")(_.setRefcount(ptr, refcount, refcount))

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
