package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.AspenError

import scala.concurrent.Future

case class StopRetrying(reason: Throwable) extends AspenError("StopRetrying", reason)

object StopRetrying:
  /** An `onCommitFailure` handler for `transactUntilSuccessfulWithRecovery` that retries only a
   *  collision and stops on everything else.
   *
   *  A commit fails transiently only with `TransactionAborted` or `TransactionOutcomeUnknown`: the
   *  transaction manager never fails a transaction, it reports it committed, aborted or, past the
   *  transaction horizon, unknown. Every other error -- an
   *  `InvalidObject` from a deleted object, a `CorruptedObject`, a `TransactionCreationError`, a
   *  failed lookup -- fails the same way on every attempt, so retrying it never completes the
   *  future. An unmatched error in a hand-written handler is no better: the `MatchError` it throws
   *  is caught by the retry strategy, which reschedules.
   *
   *  Tiered key-value list traversals and deletes restart internally when a racing join deletes a
   *  node they were about to read, so the `InvalidObject` such a join produces does not reach here.
   */
  def unlessCollision(err: Throwable): Future[Unit] = err match
    case _: TransactionAborted => Future.unit
    case _: TransactionOutcomeUnknown => Future.unit
    case e => throw StopRetrying(e)

  /** A fresh `CollisionRetry` for one retried call */
  def collisionRetry(): CollisionRetry = new CollisionRetry

/** `StopRetrying.unlessCollision` that also remembers whether an earlier attempt ended `TransactionOutcomeUnknown`.
 *
 *  Such an attempt may have committed, and the retry then re-runs `prepare` against the state it wrote
 *  (paxos-transactions 10.11). A retry closure that is not naturally idempotent checks `mayHaveCommitted` and accepts
 *  its own post-commit state as success instead of failing or redoing the work. The check must recognize this call's
 *  own write: a fixed id or key chosen outside the closure, or the exact state the call writes. Outside an Unknown the
 *  same state is still an error, so a genuine conflict on a first attempt is reported.
 */
final class CollisionRetry:
  @volatile private var unknown = false

  /** True once an earlier attempt ended TransactionOutcomeUnknown: it may have committed */
  def mayHaveCommitted: Boolean = unknown

  def onFailure(err: Throwable): Future[Unit] =
    if err.isInstanceOf[TransactionOutcomeUnknown] then
      unknown = true
    StopRetrying.unlessCollision(err)
