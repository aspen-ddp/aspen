package org.aspen_ddp.aspen.client

import scala.concurrent.Future

trait RetryStrategy {
  def retryUntilSuccessful[T](attempt: => Future[T]): Future[T]

  def retryUntilSuccessful[T](onAttemptFailure: Throwable => Future[Unit])(attempt: => Future[T]): Future[T]

  /** Immeidately cancels all activity scheduled for the future */
  def shutdown(): Unit
}