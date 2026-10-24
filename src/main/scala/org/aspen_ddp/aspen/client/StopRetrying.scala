package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.AspenError

import scala.concurrent.Future

case class StopRetrying(reason: Throwable) extends AspenError("StopRetrying", reason)

object StopRetrying:
  /** An `onCommitFailure` handler for `transactUntilSuccessfulWithRecovery` that retries only a
   *  collision and stops on everything else.
   *
   *  A commit fails transiently only with `TransactionAborted`: the transaction manager never
   *  fails a transaction, it reports it committed or aborted. Every other error -- an
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
    case e => throw StopRetrying(e)
