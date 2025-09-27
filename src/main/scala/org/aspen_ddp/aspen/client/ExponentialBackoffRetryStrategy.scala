package org.aspen_ddp.aspen.client

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class ExponentialBackoffRetryStrategy(client: AspenClient, backoffLimit: Int = 60 * 1000, initialRetryDelay: Int = 16) extends RetryStrategy {

  private  var exit = false

  def shutdown(): Unit = synchronized { exit = true }

  def retryUntilSuccessful[T](attempt: => Future[T]): Future[T] =
    val p = Promise[T]()

    given ExecutionContext = client.clientContext

    def scheduleNextAttempt(retryCount: Int): Unit =
      var countdown = retryCount
      var window = 2

      while countdown > 0 && countdown < backoffLimit do
        window = window * 2
        countdown -= 1

      if countdown > backoffLimit then
        window = backoffLimit

      val rand = new java.util.Random
      val delay = rand.nextInt(window)

      client.backgroundTaskManager.schedule(Duration(delay, TimeUnit.MILLISECONDS)) {
        retry(retryCount + 1)
      }

    def retry(retryCount: Int): Unit =
      val shouldExit = synchronized { exit }

      if shouldExit then
        return

      def onFail(cause: Throwable): Unit =
        cause match
          case StopRetrying(reason) => p.failure(reason)
          case _ => scheduleNextAttempt(retryCount)

      try
        attempt onComplete {
          case Success(result) => p.success(result)

          case Failure(cause) => onFail(cause)
        }
      catch
        case cause: Throwable => onFail(cause)

    retry(1)

    p.future

  def retryUntilSuccessful[T](onAttemptFailure: Throwable => Future[Unit])(attempt: => Future[T]): Future[T] =
    val p = Promise[T]()

    given ExecutionContext = client.clientContext

    def scheduleNextAttempt(retryCount: Int): Unit =
      var countdown = retryCount
      var window = 2

      while countdown > 0 && countdown < backoffLimit do
        window = window * 2
        countdown -= 1

      if countdown > backoffLimit then
        window = backoffLimit

      val rand = new java.util.Random
      val delay = rand.nextInt(window)

      client.backgroundTaskManager.schedule(Duration(delay, TimeUnit.MILLISECONDS)) {
        retry(retryCount + 1)
      }

    def retry(retryCount: Int): Unit =
      val shouldExit = synchronized { exit }

      if shouldExit then
        return

      def onFail(cause: Throwable): Unit =
        cause match
          case StopRetrying(reason) => p.failure(reason)
          case _ =>
            try
              onAttemptFailure(cause) onComplete {
                case Success(_) => scheduleNextAttempt(retryCount)
                case Failure(StopRetrying(reason)) => p.failure(reason)
                case Failure(_) => scheduleNextAttempt(retryCount)
              }
            catch
              case StopRetrying(reason) => p.failure(reason)
              case cause: Throwable => scheduleNextAttempt(retryCount)

      try
        attempt onComplete {
          case Success(result) => p.success(result)

          case Failure(cause) => onFail(cause)
        }
      catch
        case cause: Throwable => onFail(cause)

    retry(1)

    p.future
}
