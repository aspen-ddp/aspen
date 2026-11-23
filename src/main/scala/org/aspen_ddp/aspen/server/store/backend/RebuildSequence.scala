package org.aspen_ddp.aspen.server.store.backend

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success
import scala.util.control.NonFatal

/** Runs a backend's rebuild operations (`rebuildWrite`, `rebuildFlush`) one at a time, in call order: each starts
  * once the one called before it has completed, whatever its outcome, so a flush covers every write called before
  * it even when the caller does not wait for each. `op` must only start its work (it runs on whichever thread
  * completed the previous one) and return its Future. The lock is held only to swap the tail, never across I/O.
  */
private[backend] final class RebuildSequence:
  private var tail: Future[Unit] = Future.unit

  def next[T](op: () => Future[T]): Future[T] =
    given ExecutionContext = ExecutionContext.parasitic
    val p = Promise[T]()
    val prev = synchronized:
      val t = tail
      tail = p.future.transform(_ => Success(()))
      t
    prev.onComplete: _ =>
      p.completeWith(try op() catch case NonFatal(t) => Future.failed(t))
    p.future
