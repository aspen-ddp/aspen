package org.aspen_ddp.aspen.server.store.backend.fsif

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/** Appends batches to a journal whose valid content ends at `startLength`, continuing with batch number `nextBatch`.
  *
  * Owned by the backend's SerialExecutor: every public method must be called on `serial`, and every returned Future
  * completes on it. I/O runs on `io`. At most one batch is in flight; records appended meanwhile are buffered and
  * become the next batch, so a single write plus a single force covers all of them. A Future completes only once its
  * batch is forced.
  *
  * If a batch's write or force fails, the io thread truncates back to the last durable length and forces (best
  * effort), so a torn tail never precedes later batches. The writer then keeps the failed batch and everything
  * buffered, with their Futures pending, calls `onError` with the original exception and sends nothing more until
  * `retry()` (which re-sends the same batch under the same batch number) or `failAll`. Once a retried batch is
  * forced, `onResumed` is called, before that batch's Futures complete. When an io task ends after `failAll` (so no
  * Future completes and `busy` just turned false), `onSettled` is called so the owner can re-check its idle state.
  */
final class JournalWriter(file: FsifFile, startLength: Long, nextBatch: Long, salt: Long,
                          io: ExecutionContext, serial: SerialExecutor, onError: Throwable => Unit,
                          onResumed: () => Unit = () => (),
                          onSettled: () => Unit = () => ()):
  import JournalWriter.Batch

  private var durable = startLength
  private var batchNumber = nextBatch
  // The batch in flight, or the failed batch awaiting retry()
  private var current: Option[Batch] = None
  private var paused = false
  /** The batch in flight was re-sent by retry() */
  private var retried = false
  /** An io task is running, whether or not failAll has abandoned its batch */
  private var sending = false
  private var failed: Option[Throwable] = None
  /** abandon() was called: nothing more is sent and no pending Future completes */
  private var abandoned = false

  private val buffered = mutable.ArrayBuffer[JournalRecord]()
  private var bufferedSize = 0L
  private val bufferedPromises = mutable.ArrayBuffer[Promise[Unit]]()

  /** Completes when the batch holding `records` is forced */
  def append(records: List[JournalRecord]): Future[Unit] =
    failed match
      case Some(t) => Future.failed(t)
      case None =>
        val p = Promise[Unit]()
        buffered ++= records
        bufferedSize += records.iterator.map(JournalFormat.recordSize).sum
        bufferedPromises += p
        sendBuffered()
        p.future

  /** Durable, in-flight and buffered bytes (as encoded) */
  def length: Long =
    val buf = if bufferedPromises.isEmpty then 0L else JournalFormat.BatchOverhead + bufferedSize
    durable + current.map(_.bytes.length.toLong).getOrElse(0L) + buf

  /** Nothing in flight, awaiting retry or buffered */
  def idle: Boolean = current.isEmpty && bufferedPromises.isEmpty

  /** An io task may still touch the file. Unlike `idle`, this stays true after failAll until that task ends, so the
    * file must not be closed or reused before it is false.
    */
  def busy: Boolean = sending

  /** After `onError`: re-sends the failed batch under the same batch number. No effect otherwise. */
  def retry(): Unit =
    if failed.isEmpty && !abandoned && paused then
      paused = false
      retried = true
      current.foreach(send(_, truncateFirst = true))

  /** The owner is closing without waiting for the batches still pending (a crash-equivalent close of a store frozen
    * by ENOSPC): nothing more is sent, and no pending or later Future ever completes. An io task in flight still ends
    * (`busy` turns false, then `onSettled` is called), so the file is not closed under it.
    */
  def abandon(): Unit =
    abandoned = true
    paused = false

  /** Permanently failed: every pending and future append fails with `t` */
  def failAll(t: Throwable): Unit =
    if failed.isEmpty then
      failed = Some(t)
      paused = false
      val promises = current.map(_.promises).getOrElse(Nil) ++ bufferedPromises
      current = None
      buffered.clear()
      bufferedSize = 0
      bufferedPromises.clear()
      promises.foreach(_.failure(t))

  private def sendBuffered(): Unit =
    if current.isEmpty && !paused && !abandoned && failed.isEmpty && bufferedPromises.nonEmpty then
      val batch = Batch(JournalFormat.encodeBatch(salt, batchNumber, buffered.toList), bufferedPromises.toList)
      buffered.clear()
      bufferedSize = 0
      bufferedPromises.clear()
      current = Some(batch)
      send(batch, truncateFirst = false)

  private def send(batch: Batch, truncateFirst: Boolean): Unit =
    val pos = durable
    sending = true
    try io.execute(new FsifBackend.IoTask(FsifBackend.IoKind.Journal, () => {
      val result = Try:
        // A retry also clears any tail left by a failed recovery truncate
        if truncateFirst then file.truncate(pos)
        file.write(pos, batch.bytes)
        file.force()
      if result.isFailure then
        try
          file.truncate(pos)
          file.force()
        catch case NonFatal(_) => () // retry() truncates again
      serial.execute(() => completed(batch, result))
    }))
    catch
      case t: Throwable =>
        sending = false
        throw t

  private def completed(batch: Batch, result: Try[Unit]): Unit =
    sending = false
    // After failAll the batch's Futures have already failed, and after abandon none completes: tell the owner busy
    // has cleared
    if failed.isDefined || abandoned then
      onSettled()
    else
      result match
        case Success(_) =>
          durable += batch.bytes.length
          batchNumber += 1
          current = None
          val resumed = retried
          retried = false
          sendBuffered()
          if resumed then onResumed()
          batch.promises.foreach(_.success(()))
        case Failure(t) =>
          paused = true
          onError(t)

object JournalWriter:
  private final case class Batch(bytes: Array[Byte], promises: List[Promise[Unit]])
