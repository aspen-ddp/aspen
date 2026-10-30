package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Metadata, ObjectId, ObjectPointer, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.store.{ReadState, StoreId}
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.backend.{Backend, Commit, CommitError, CommitState, CompletionHandler, Read,
  UpdateForm}

/** A store backend that keeps every object slice in a fixed-size segment of one flat `data` file, located through an
  * in-memory index map that is journaled and checkpointed. See docs/superpowers/specs/2026-10-29-fsif-backend-design.md.
  *
  * Threading: every interface method only posts to `serial`, which owns all state below. I/O runs on `io`, and its
  * results are posted back to `serial`. Completions are delivered to the handler from `serial`. The constructor runs
  * recovery synchronously.
  *
  * Write paths (all journal records for an external index X are appended in commit order):
  *  - journaled: the commit's Delta (preceded by `Mapping(X, f, id)` when X gets a slot) is appended; the commit
  *    completes when that batch is forced, and the Delta is then written in place in the background. Reads overlay
  *    pending Deltas, so they never wait for the in-place write.
  *  - copy-on-write (Full and larger than `cowThreshold`): the whole segment is written to the lowest free slot f and
  *    `data` is forced; only then is `Mapping(X, f, id)` appended (durable-before-map). Operations on X that arrive
  *    meanwhile wait until that Mapping is appended, then target f.
  *
  * Checkpoints (s to s+1, one at a time; started when the active journal exceeds max(indexFileSize / 2,
  * journalBound), by `rebuildFlush`, or by `checkpointNow`), each step re-runnable from its start:
  *  1. Snapshot the map as of the last record appended to journal s; later records are buffered. Once every
  *     journal-s batch is durable, header s+1 is written and forced into the other journal file, and only then does a
  *     new writer take the buffered records (Ruling 19: no record of s+1 is durable unless all of s is).
  *  2. Wait until every journal-s Delta is applied in place or cancelled, and until everything appended so far
  *     (which includes any record that cancelled one) is durable; then force `data`.
  *  3. Write index s+1 into the other index file and force it.
  *  4. Empty journal s and index s, forcing each.
  *
  * Tail compaction (Compaction.scala): after every serial task, free tail slots are truncated away and, if no move
  * is outstanding, the occupant of the highest used slot is moved into the lowest free slot below it.
  *
  * Out of space (ENOSPC from any write, force or truncate) freezes writes instead of failing the store: the failed
  * operation and everything queued behind it are held, never completed with an error, and their I/O is retried every
  * `tuning.enospcRetry` on `scheduler` until it succeeds. Commits are still accepted, reads go on (overlaying pending
  * Deltas), and checkpoints, compaction and in-place writes pause and later resume where they stood. Any other I/O
  * error is fail-stop: commits complete with IOFailure, reads with CorruptedObject, and `onFatal` is called once.
  * Backpressure: a commit that arrives while unapplied Delta bytes exceed `tuning.unappliedBound` is acknowledged only
  * once they no longer do.
  */
class FsifBackend(val storeId: StoreId,
                  val path: os.Path,
                  layout: FsifLayout,
                  files: FsifFiles,
                  io: ExecutionContext,
                  tuning: FsifTuning = FsifTuning(),
                  onFatal: Throwable => Unit = _ => (),
                  scheduler: FsifBackend.RetryScheduler = FsifBackend.SharedRetryScheduler) extends Backend:
  import FsifBackend.*

  private type Reply = Either[Unit, CommitError.Value] => Unit

  private val serial = new SerialExecutor(io)
  private val recovered = FsifRecovery.recover(files, layout)
  private val dataFile = files.open(FsifRecovery.DataFile)
  /** The journal the current writer appends to: its handle and name follow a checkpoint's journal switch */
  @volatile private var journalFile: FsifFile = files.open(recovered.activeJournal)
  @volatile private var activeJournal: String = recovered.activeJournal
  private var journal = newJournal(journalFile, recovered.journalLength, recovered.nextBatch, recovered.salt)

  // ---- State, owned by serial ----
  private val index: IndexMap = recovered.index
  @volatile private var slotTotal: Int = recovered.slotCount
  /** Slots that are mapped, reserved (copy-on-write or rebuild targets) or waiting for the slot-freeing rule */
  private val used = new java.util.BitSet
  index.slots.foreach(s => if s >= 0 then used.set(s))
  private val slotStates = mutable.HashMap[Int, SlotState]()
  /** slot -> the ext mapped to it, for every mapped slot */
  private val slotOwner = mutable.HashMap[Int, Int]()
  for x <- index.slots.indices do
    if index.slots(x) >= 0 then slotOwner(index.slots(x)) = x
  /** Exts with a copy-on-write in progress, holding the operations that arrived meanwhile, in order */
  private val gated = mutable.HashMap[Int, mutable.ArrayDeque[() => Unit]]()
  private var readsInFlight = 0
  private var writesInFlight = 0
  private var unappliedCount = 0
  private var unappliedBytes = 0L
  private var fatal: Option[Throwable] = None
  private val quiescers = mutable.ArrayBuffer[Promise[Unit]]()
  /** Copy-on-write targets that grew the slot count and are not yet forced. Recovery bounds every slot by
    * max(forced data size / stride, 1 + highest indexed slot) + the Mappings it replays (Ruling 12), and such a slot
    * is counted by none of these until its force. So a journaled allocation into any higher slot (whose data is not
    * forced) waits for it before appending its Mapping (`afterTailCows`).
    */
  private val tailCows = mutable.TreeSet[Int]()
  private val awaitingTail = mutable.ArrayBuffer[(Int, () => Unit)]()
  /** The pending Map(X, -1) of each X unmapped by a delete whose record is not yet durable */
  private val unmapping = mutable.HashMap[Int, Future[Unit]]()
  /** Rebuild writes whose Mapping is appended by the next rebuildFlush: ext -> (slot, id) */
  private val rebuildMaps = mutable.LinkedHashMap[Int, (Int, ObjectId)]()
  /** Serializes rebuildWrite / rebuildFlush callers, whose segment writes run on the calling thread */
  private val rebuildLock = new Object

  // ---- Out-of-space freeze and backpressure, owned by serial ----
  /** Some operation's I/O failed with ENOSPC and has not yet succeeded on a retry */
  @volatile private var frozen = false
  /** Operations held by ENOSPC, waiting for the next retry round, in the order they failed */
  private val heldOps = mutable.LinkedHashMap[AnyRef, HeldOp]()
  /** Held operations whose retry is in flight */
  private val retryingOps = mutable.LinkedHashMap[AnyRef, HeldOp]()
  private var retryScheduled = false
  /** Promises that fail with out-of-space as soon as the store freezes (rebuildFlush waits on them) */
  private val freezeWaiters = mutable.ArrayBuffer[Promise[Unit]]()
  /** Test counter: holds by kind of I/O */
  private val holdCounts = mutable.HashMap[String, Int]()
  /** Successful commit replies held by backpressure, in order */
  private val throttled = mutable.ArrayBuffer[Reply]()

  // ---- Checkpoint state, owned by serial ----
  /** The index file holding index `currentSeq` */
  private var activeIndex: String = recovered.activeIndex
  @volatile private var currentSeq: Long = recovered.seq
  /** The journal seq that new records go to: `currentSeq`, or `currentSeq + 1` once a checkpoint has taken its
    * snapshot (the records may still be buffered in `switchBuffer`)
    */
  private var appendSeq: Long = recovered.seq
  private var checkpoint: Option[Checkpoint] = None
  /** Records appended while a checkpoint switches journals (Ruling 19), released into the new writer in order */
  private val switchBuffer = mutable.ArrayBuffer[(List[JournalRecord], Promise[Unit])]()
  /** The Future of the most recent append. Batches complete in order, so it completing means every earlier append
    * (to the same writer) is durable.
    */
  private var lastAppend: Future[Unit] = Future.unit
  /** Journal seq -> Deltas appended to that journal and neither applied in place nor cancelled */
  private val unappliedBySeq = mutable.HashMap[Long, Int]()
  private val checkpointThreshold: Long =
    math.max(IndexFile.sizeFor(layout.segmentCount) / 2, tuning.journalBound)

  @volatile private var handler: Option[CompletionHandler] = None

  private val compaction = new Compaction(new Compaction.Host:
    def fatal: Boolean = FsifBackend.this.fatal.isDefined
    def frozen: Boolean = FsifBackend.this.frozen
    def hold(key: AnyRef, kind: String, retry: () => Unit, abort: () => Unit): Unit =
      FsifBackend.this.hold(key, kind, retry, abort)
    def released(key: AnyRef): Unit = FsifBackend.this.released(key)
    def used: java.util.BitSet = FsifBackend.this.used
    def slotCount: Int = slotTotal
    def setSlotCount(n: Int): Unit = slotTotal = n
    def dataFile: FsifFile = FsifBackend.this.dataFile
    def movable(slot: Int): Option[(Int, ObjectId)] =
      slotOwner.get(slot).filter: x =>
        index.slots(x) == slot && !gated.contains(x) && !rebuildMaps.contains(x) &&
          !slotStates.get(slot).exists(_.dead)
      .map(x => (x, index.ids(x)))
    def maps(x: Int, slot: Int, id: ObjectId): Boolean = index.slots(x) == slot && index.ids(x) == id
    def slotState(slot: Int): SlotState = FsifBackend.this.slotState(slot)
    def readDone(st: SlotState): Unit =
      st.prune()
      if st.dead then tryFree(st) else dropIfUnused(st)
    def runIo[A](kind: IoKind)(work: => A)(done: Try[A] => Unit): Unit =
      try io.execute(new IoTask(kind, () => {
        val r = Try(work)
        post(done(r))
      }))
      catch case NonFatal(t) => done(Failure(t))
    def failStop(t: Throwable): Unit =
      logger.error(s"FSIF store $storeId: compaction I/O failed: $t")
      FsifBackend.this.failStop(t)
    def commitMove(x: Int, id: ObjectId, from: Int, to: Int)(done: Try[Unit] => Unit): Unit =
      assign(x, to, id)
      val dead = kill(from)
      onDurable(appendRecords(List(JournalRecord.Mapping(x, to, id)))): r =>
        if r.isSuccess then unmapDurable(dead)
        done(r)
  , layout)

  // Compaction resumes after recovery without waiting for the first operation
  post(())

  // ---- Backend interface ----

  /** Writes are frozen by a full device: held I/O is retried every `tuning.enospcRetry` while reads go on */
  def isWriteFrozen: Boolean = frozen

  /** Slots the data file is laid out for: every occupied or reserved slot is below it */
  def slotCount: Int = slotTotal

  private[fsif] def compactionStats: CompactionStats = compaction.stats

  /** Test hook: how many times each kind of I/O has been held by ENOSPC */
  private[fsif] def holdStats: Map[String, Int] = holdCounts.synchronized(holdCounts.toMap)

  override def setCompletionHandler(h: CompletionHandler): Unit = handler = Some(h)

  /** Completes when no journal batch, in-place write, copy-on-write, read, commit, checkpoint or compaction move is
    * outstanding and the store is not frozen. Moves start after every completion while one is possible, so afterwards
    * none is possible and the free tail slots are truncated away. After a fatal error, held work no longer counts
    * (io still in flight does).
    */
  def quiesce(): Future[Unit] =
    val p = Promise[Unit]()
    post(quiescers += p)
    p.future

  /** Test hook: (slot, ObjectId) that ext x maps to (-1 and the zero id when unmapped) */
  private[fsif] def mapping(x: Int): Future[(Int, ObjectId)] =
    val p = Promise[(Int, ObjectId)]()
    post(p.success((index.slots(x), index.ids(x))))
    p.future

  /** Sequence of the newest durable index; the active journal holds the records made after it */
  def journalSeq: Long = currentSeq

  /** Test hook: starts a checkpoint, or returns the running one's Future */
  private[fsif] def checkpointNow(): Future[Unit] =
    val p = Promise[Unit]()
    post(p.completeWith(startCheckpoint()))
    p.future

  override def close(): Future[Unit] =
    quiesce().transform { _ =>
      Try {
        dataFile.close()
        journalFile.close()
      }
    }(using ExecutionContext.parasitic)

  /** The held data and active journal handles, plus the other journal and both index files (not held) */
  override def estimateSize(): Long =
    val active = activeJournal
    val others = FsifRecovery.IndexFiles ++ FsifRecovery.JournalFiles.filter(_ != active)
    def sizeOf(name: String): Long =
      val f = files.open(name)
      try f.size
      finally f.close()
    dataFile.size + journalFile.size + others.filter(files.exists).map(sizeOf).sum

  override def bootstrapAllocate(objectId: ObjectId, objectType: ObjectType.Value, metadata: Metadata,
                                 data: DataBuffer): Unit =
    throw new UnsupportedOperationException("FSIF stores cannot be bootstrap stores")

  override def bootstrapOverwrite(objectId: ObjectId, data: DataBuffer): Unit =
    throw new UnsupportedOperationException("FSIF stores cannot be bootstrap stores")

  override def read(pointer: ObjectPointer): Unit = post(startRead(pointer))

  override def commit(state: CommitState, transactionId: TransactionId): Unit =
    val id = state.objectId
    post(update(state, repairing = false, r => handler.foreach(_.complete(Commit(storeId, id, transactionId, r)))))

  override def repair(state: CommitState, complete: Promise[Unit]): Unit =
    post(update(state, repairing = true, promiseReply(complete, s"repair of ${state.objectId}")))

  override def repairDelete(objectId: ObjectId, storePointer: Array[Byte], complete: Promise[Unit]): Unit =
    post(startRepairDelete(objectId, storePointer, promiseReply(complete, s"repairDelete of $objectId")))

  /** Rebuild only: the store is not live, so nothing else touches X's slot. Writes the segment in place on the
    * calling thread and buffers the Mapping for `rebuildFlush`. Throws IllegalArgumentException on an invalid pointer
    * or a slice larger than a segment, and an out-of-space IOException while writes are frozen or if the write itself
    * hits ENOSPC (Ruling 21: RebuildingStore handles it; the store does not wait for space).
    */
  override def rebuildWrite(pointer: ObjectPointer, objectType: ObjectType.Value, metadata: Metadata,
                            data: DataBuffer): Unit =
    val x = decodeStorePointer(pointer.storePointer, layout.segmentCount).getOrElse:
      throw new IllegalArgumentException(s"invalid FSIF store pointer in $pointer")
    if data.size > layout.segmentPayloadSize then
      throw new IllegalArgumentException(
        s"slice of ${data.size} bytes exceeds the segment payload size ${layout.segmentPayloadSize}")
    val header = SegmentHeader.forSlice(objectType, pointer.id, x, metadata, data)
    val segment = SegmentHeader.encode(header) ++ data.getByteArray
    rebuildLock.synchronized:
      val (slot, fresh) = onSerialSync:
        throwIfFatal()
        throwIfFrozen()
        compaction.cancel(x) // the segment is written to X's slot directly, bypassing a move's copy
        rebuildMaps.get(x) match
          case Some((s, _)) =>
            rebuildMaps(x) = (s, pointer.id)
            (s, false)
          case None =>
            val mapped = index.slots(x) >= 0
            val s = if mapped then index.slots(x) else allocateSlot()
            rebuildMaps(x) = (s, pointer.id)
            (s, !mapped)
      try dataFile.write(layout.slotOffset(slot), segment)
      catch
        case NonFatal(t) =>
          // A slot taken for this write alone is released, so the next flush never maps an unwritten segment.
          // Otherwise the slot holds an earlier or mapped copy of X that the resumed rebuild writes again.
          if fresh then
            onSerialSync:
              if rebuildMaps.get(x).exists(_._1 == slot) then
                rebuildMaps.remove(x)
                used.clear(slot)
          throw t

  /** Forces `data`, then appends and forces the buffered Mappings (durable-before-map), then checkpoints. Throws an
    * out-of-space IOException if `data`'s force hits ENOSPC, or if the store is frozen before the Mappings are durable
    * or the checkpoint has finished (Ruling 21). Mappings already appended then stay held and land once space
    * returns.
    */
  override def rebuildFlush(): Unit =
    rebuildLock.synchronized:
      dataFile.force()
      val appended = Promise[Unit]()
      onSerialSync:
        throwIfFatal()
        throwIfFrozen()
        addFreezeWaiter(appended)
        val maps = rebuildMaps.toList
        rebuildMaps.clear()
        maps.foreach:
          case (x, (slot, id)) =>
            compaction.cancel(x)
            assign(x, slot, id)
        if maps.isEmpty then
          appended.success(())
        else
          val records = maps.map { case (x, (slot, id)) => JournalRecord.Mapping(x, slot, id) }
          onDurable(appendRecords(records))(r => appended.tryComplete(r)) // a freeze may have failed it
      // Completes when durable, on fail-stop, or with out-of-space when the store freezes
      Await.result(appended.future, Duration.Inf)
      val checkpointed = Promise[Unit]()
      onSerialSync:
        throwIfFatal()
        throwIfFrozen()
        addFreezeWaiter(checkpointed)
        startCheckpoint().onComplete(r => checkpointed.tryComplete(r))(using ExecutionContext.parasitic)
      Await.result(checkpointed.future, Duration.Inf)

  // ---- Serial plumbing ----

  private def post(f: => Unit): Unit = serial.execute(() => guarded(f))

  private def guarded(f: => Unit): Unit =
    try f
    catch
      case NonFatal(t) =>
        logger.error(s"FSIF store $storeId: internal error: $t")
        failStop(t)
    finally
      try
        advanceCheckpoint()
        maybeCheckpoint()
      catch
        case NonFatal(t) =>
          logger.error(s"FSIF store $storeId: internal error in a checkpoint: $t")
          failStop(t)
          advanceCheckpoint()
      try compaction.drive()
      catch
        case NonFatal(t) =>
          logger.error(s"FSIF store $storeId: compaction failed: $t")
          failStop(t)
          compaction.drive()
      releaseThrottled()
      checkQuiesce()

  private def onSerialSync[A](f: => A): A =
    val p = Promise[A]()
    serial.execute(() => p.complete(Try(f)))
    Await.result(p.future, Duration.Inf)

  private def throwIfFatal(): Unit =
    fatal.foreach(t => throw new IOException(s"FSIF store $storeId has failed", t))

  private def throwIfFrozen(): Unit =
    if frozen then
      throw outOfSpace()

  /** Matches FsifIo.isOutOfSpace and RebuildingStore's out-of-space test */
  private def outOfSpace(): IOException =
    new IOException(s"No space left on device: FSIF store $storeId has frozen writes")

  /** Runs `op` now, or after the copy-on-write in progress for `x` has appended its Mapping */
  private def withExt(x: Int)(op: => Unit): Unit =
    gated.get(x) match
      case Some(q) => q.append(() => guarded(op))
      case None => op

  /** Runs the operations held behind x's copy-on-write, in order, until one starts another copy-on-write */
  private def releaseExt(x: Int): Unit =
    val q = gated.remove(x).getOrElse(mutable.ArrayDeque())
    while q.nonEmpty && !gated.contains(x) do
      q.removeHead()()
    if q.nonEmpty then
      gated(x).prependAll(q)

  /** Runs `append` (which appends a Mapping to `slot`) once no unforced tail copy-on-write slot lies below `slot` */
  private def afterTailCows(slot: Int)(append: => Unit): Unit =
    if tailCows.headOption.exists(_ < slot) then
      awaitingTail += ((slot, () => guarded(append)))
    else
      append

  private def tailCowDone(f: Int): Unit =
    tailCows -= f
    val ready = awaitingTail.filter((slot, _) => !tailCows.headOption.exists(_ < slot)).toList
    awaitingTail.filterInPlace((slot, _) => tailCows.headOption.exists(_ < slot))
    ready.foreach((_, run) => run())

  /** Replies IOFailure if `f` throws, then rethrows so the caller's `guarded` fail-stops */
  private def replyOnError(reply: Reply)(f: => Unit): Unit =
    try f
    catch
      case NonFatal(t) =>
        reply(Right(CommitError.IOFailure))
        throw t

  private def promiseReply(p: Promise[Unit], what: String): Reply = {
    case Left(_) => p.trySuccess(())
    case Right(err) => p.tryFailure(new FsifBackend.OperationFailed(s"$what on store $storeId failed: $err"))
  }

  private def onDurable(f: Future[Unit])(body: Try[Unit] => Unit): Unit =
    f.onComplete(r => guarded(body(r)))(using ExecutionContext.parasitic)

  private def checkQuiesce(): Unit =
    if quiescers.nonEmpty && idle then
      val ps = quiescers.toList
      quiescers.clear()
      ps.foreach(_.trySuccess(()))

  /** After a fatal error, held work no longer counts, but io still in flight does (including the journal's, which
    * failAll does not wait for), so `close` never closes a file under it
    */
  private def idle: Boolean =
    gated.isEmpty && writesInFlight == 0 && readsInFlight == 0 && checkpoint.isEmpty && compaction.idle &&
      !journal.busy &&
      (fatal.isDefined || (!frozen && throttled.isEmpty && journal.idle && unappliedCount == 0))

  // ---- Failure handling ----
  //
  // Out of space freezes writes. Every I/O source (journal write/force/truncate, copy-on-write write/force, in-place
  // write, checkpoint step, compaction write/force and truncation) that fails with ENOSPC calls `hold` with a key, a
  // `retry` that re-issues exactly the failed I/O (each such step is re-runnable from its start) and an `abort` for
  // fail-stop. Nothing held completes, with an error or otherwise. While any operation is held the store is frozen:
  // in-place writes, checkpoint steps, compaction moves and truncation start nothing new (their own held retries
  // excepted), commits are still accepted (their journal batches buffer behind a held one), and reads go on,
  // overlaying pending Deltas. Every `tuning.enospcRetry` a retry round re-issues every held operation; one that
  // succeeds calls `released`, one that hits ENOSPC again is held again. When none is held or retrying, the store
  // unfreezes and the paused work resumes. Any other I/O error, including one during a retry, is fail-stop.

  private def newJournal(f: FsifFile, start: Long, next: Long, salt: Long): JournalWriter =
    var w: JournalWriter = null
    // onSettled: an io task abandoned by failAll has ended; run the guarded epilogue so a quiesce or close waiting
    // on `!journal.busy` completes
    w = new JournalWriter(f, start, next, salt, io, serial, t => journalError(w, t), () => released(w),
      () => post(()))
    w

  /** The writer has truncated back to its durable length and holds the failed batch and everything behind it */
  private def journalError(w: JournalWriter, t: Throwable): Unit =
    if fatal.isEmpty && FsifIo.isOutOfSpace(t) then
      hold(w, "journal", () => w.retry(), () => ())
    else
      failStop(t)

  /** Holds an operation whose I/O hit ENOSPC, freezing the store; `retry` runs in the next retry round */
  private def hold(key: AnyRef, kind: String, retry: () => Unit, abort: () => Unit): Unit =
    if fatal.isEmpty then
      holdCounts.synchronized(holdCounts(kind) = holdCounts.getOrElse(kind, 0) + 1)
      retryingOps.remove(key)
      heldOps(key) = new HeldOp(retry, abort)
      if !frozen then
        logger.warn(s"FSIF store $storeId: out of space; writes frozen, retrying every ${tuning.enospcRetry}")
        frozen = true
        val waiters = freezeWaiters.toList
        freezeWaiters.clear()
        waiters.foreach(_.tryFailure(outOfSpace()))
      scheduleRetry()

  /** The held operation `key` succeeded (or became moot); the last one to do so unfreezes the store. No effect if
    * `key` is not held.
    */
  private def released(key: AnyRef): Unit =
    if retryingOps.remove(key).isDefined || heldOps.remove(key).isDefined then
      if frozen && heldOps.isEmpty && retryingOps.isEmpty then
        logger.info(s"FSIF store $storeId: space is available again; writes resume")
        frozen = false
        // Paused in-place writes restart now; checkpoints, compaction and backpressure resume in a guarded task's
        // epilogue (not every caller is guarded, e.g. the journal writer's completion)
        slotStates.values.toList.foreach(st => kick(st))
        post(())

  private def scheduleRetry(): Unit =
    if !retryScheduled then
      retryScheduled = true
      scheduler.schedule(tuning.enospcRetry, () => post(retryRound()))

  /** Re-issues every held operation's I/O */
  private def retryRound(): Unit =
    retryScheduled = false
    if fatal.isEmpty && heldOps.nonEmpty then
      val round = heldOps.toList
      heldOps.clear()
      // The whole round counts as retrying before any retry runs: one that releases synchronously must not find
      // both maps empty and unfreeze while later ops of the round are still unretried
      round.foreach((key, op) => retryingOps(key) = op)
      round.foreach: (key, op) =>
        // A retry earlier in this round may have fail-stopped, which aborted the rest
        if fatal.isEmpty && retryingOps.contains(key) then
          op.retry()

  private def addFreezeWaiter(p: Promise[Unit]): Unit =
    freezeWaiters.filterInPlace(!_.isCompleted)
    freezeWaiters += p

  /** Stops accepting writes: pending commits complete with IOFailure (through their failed journal futures and the
    * held operations' aborts), later commits likewise, and reads with CorruptedObject. ENOSPC retries stop. Calls
    * onFatal once.
    */
  private def failStop(t: Throwable): Unit =
    if fatal.isEmpty then
      logger.error(s"FSIF store $storeId: fatal I/O error, failing the store: $t")
      fatal = Some(t)
      frozen = false
      journal.failAll(t)
      val buffered = switchBuffer.toList
      switchBuffer.clear()
      buffered.foreach((_, p) => p.tryFailure(t))
      val held = (heldOps.values ++ retryingOps.values).toList
      heldOps.clear()
      retryingOps.clear()
      held.foreach: op =>
        try op.abort()
        catch case NonFatal(e) => logger.error(s"FSIF store $storeId: aborting a held operation threw $e")
      val acks = throttled.toList
      throttled.clear()
      acks.foreach(_(Right(CommitError.IOFailure)))
      try onFatal(t)
      catch case NonFatal(e) => logger.error(s"FSIF store $storeId: onFatal threw $e")

  /** Backpressure: a successful reply of a commit that arrived while unapplied Delta bytes exceeded
    * `tuning.unappliedBound` waits until they no longer do
    */
  private def ack(reply: Reply, wasThrottled: Boolean): Unit =
    if wasThrottled && unappliedBytes > tuning.unappliedBound then
      throttled += reply
    else
      reply(Left(()))

  /** Run after every serial task */
  private def releaseThrottled(): Unit =
    if throttled.nonEmpty && unappliedBytes <= tuning.unappliedBound then
      val acks = throttled.toList
      throttled.clear()
      acks.foreach(_(Left(())))

  // ---- Slot allocation and freeing ----

  /** Maps ext x to `slot` (-1: unmapped) as occupant `id`, keeping `slotOwner` in step */
  private def assign(x: Int, slot: Int, id: ObjectId): Unit =
    val old = index.slots(x)
    if old >= 0 && slotOwner.get(old).contains(x) then slotOwner.remove(old)
    index.slots(x) = slot
    index.ids(x) = id
    if slot >= 0 then slotOwner(slot) = x

  /** The lowest free slot, always: recovery bounds slots by assuming that allocation never skips one */
  private def allocateSlot(): Int =
    val s = used.nextClearBit(0)
    if s > slotTotal then
      throw new IllegalStateException(s"lowest free slot $s is above the slot count $slotTotal")
    used.set(s)
    if s == slotTotal then
      slotTotal += 1
    s

  private def slotState(slot: Int): SlotState = slotStates.getOrElseUpdate(slot, new SlotState(slot))

  private def dropIfUnused(st: SlotState): Unit =
    if st.unused && slotStates.get(st.slot).contains(st) then
      slotStates.remove(st.slot)

  private def cancelQueued(st: SlotState): Unit =
    val dropped = st.cancelQueued()
    st.version += 1
    unappliedCount -= dropped.size
    unappliedBytes -= dropped.iterator.map(_.bytes).sum
    dropped.foreach(resolved)

  /** The occupant of `slot` is dead (deleted, replaced by copy-on-write): its queued in-place writes are cancelled,
    * and the slot is freed once the unmapping record is durable (`unmapDurable`) and nothing is in flight on it.
    */
  private def kill(slot: Int): SlotState =
    val st = slotState(slot)
    cancelQueued(st)
    st.dead = true
    st.prune()
    released(st) // a held in-place write of the dead occupant was just cancelled
    st

  private def unmapDurable(st: SlotState): Unit =
    st.unmapDurable = true
    tryFree(st)

  private def tryFree(st: SlotState): Unit =
    if st.dead && st.unmapDurable && st.reads == 0 && st.writing == 0 && slotStates.get(st.slot).contains(st) then
      slotStates.remove(st.slot)
      used.clear(st.slot)

  // ---- In-place writes ----

  /** A Delta was applied in place or cancelled: it no longer holds back a checkpoint of its journal */
  private def resolved(pd: PendingDelta): Unit =
    val left = unappliedBySeq.getOrElse(pd.journalSeq, 0) - 1
    if left > 0 then unappliedBySeq(pd.journalSeq) = left else unappliedBySeq.remove(pd.journalSeq)

  private def addDelta(slot: Int, d: JournalRecord.Delta): PendingDelta =
    val pd = new PendingDelta(d.header, d.ranges)
    pd.journalSeq = appendSeq
    unappliedBySeq(appendSeq) = unappliedBySeq.getOrElse(appendSeq, 0) + 1
    val st = slotState(slot)
    st.deltas += pd
    st.version += 1
    unappliedCount += 1
    unappliedBytes += pd.bytes
    compaction.onDelta(slot, pd)
    pd

  /** Starts the next coalesced in-place write for `st` if none is in flight. Physical writes per slot are serial.
    * While frozen, only a retry of `st`'s own held write starts one. Returns whether a write was started.
    */
  private def kick(st: SlotState, retrying: Boolean = false): Boolean =
    if fatal.isEmpty && (!frozen || retrying) && !st.dead && slotStates.get(st.slot).contains(st) then
      val batch = st.nextWritable()
      if batch.isEmpty then
        false
      else
        st.writing = batch.size
        writesInFlight += 1
        val base = layout.slotOffset(st.slot)
        val header = SegmentHeader.encode(batch.last.header)
        st.version += 1
        try
          io.execute(new IoTask(IoKind.InPlace, () => {
            val r = Try {
              batch.foreach(_.ranges.foreach(r => dataFile.write(base + SegmentHeader.Size + r.offset, r.bytes)))
              dataFile.write(base, header)
            }
            post(inPlaceDone(st, batch, r))
          }))
        catch
          case NonFatal(t) => inPlaceDone(st, batch, Failure(t))
        true
    else
      false

  /** A held in-place write's retry; the write is moot if its occupant died or its Deltas went meanwhile */
  private def retryInPlace(st: SlotState): Unit =
    if !kick(st, retrying = true) then
      released(st)

  private def inPlaceDone(st: SlotState, batch: List[PendingDelta], r: Try[Unit]): Unit =
    writesInFlight -= 1
    st.version += 1
    r match
      case Failure(t) if fatal.isEmpty && FsifIo.isOutOfSpace(t) =>
        // The batch stays in the slot's list, queued again, so reads keep overlaying it until it lands
        logger.warn(s"FSIF store $storeId: in-place write to slot ${st.slot} is out of space; holding it")
        st.writing = 0
        if st.dead then
          // The occupant died while this write was in flight, so kill() could not cancel the batch: cancel it now
          cancelQueued(st)
          tryFree(st)
          released(st)
        else
          hold(st, "in-place", () => retryInPlace(st), () => tryFree(st))
      case Failure(t) =>
        logger.error(s"FSIF store $storeId: in-place write to slot ${st.slot} failed: $t")
        st.writing = 0
        failStop(t)
        tryFree(st)
      case Success(_) =>
        st.writing = 0
        st.applied += batch.size
        unappliedCount -= batch.size
        unappliedBytes -= batch.iterator.map(_.bytes).sum
        batch.foreach(resolved)
        released(st)
        st.prune()
        if st.dead then
          tryFree(st)
        else
          kick(st)
          dropIfUnused(st)

  // ---- Journal appends and checkpoints ----

  /** Every journal append goes through here. While a checkpoint switches journals, records wait in `switchBuffer`
    * until the new journal's header is forced and the old writer is idle (Ruling 19).
    */
  private def appendRecords(records: List[JournalRecord]): Future[Unit] =
    val f =
      if checkpoint.exists(_.switching) then
        val p = Promise[Unit]()
        switchBuffer += ((records, p))
        p.future
      else
        journal.append(records)
    lastAppend = f
    f

  /** The trigger, run after every serial task (so after every commit and compaction completion) */
  private def maybeCheckpoint(): Unit =
    if checkpoint.isEmpty && fatal.isEmpty && !frozen && journal.length > checkpointThreshold then
      startCheckpoint()

  /** Step 1 begins: the map as of the last record appended to journal s is index s+1's content, and every later
    * record belongs to journal s+1 (buffered until that journal exists)
    */
  private def startCheckpoint(): Future[Unit] =
    checkpoint match
      case Some(cp) => cp.done.future
      case None =>
        fatal match
          case Some(t) => Future.failed(new IOException(s"FSIF store $storeId has failed", t))
          case None =>
            val cp = new Checkpoint(currentSeq + 1, index.copy(), lastAppend)
            checkpoint = Some(cp)
            appendSeq = cp.seq
            wake(cp.oldTail)
            cp.done.future

  /** Re-runs advanceCheckpoint once `f` completes */
  private def wake(f: Future[Unit]): Unit = f.onComplete(_ => post(()))(using ExecutionContext.parasitic)

  /** Moves the running checkpoint forward as far as its conditions allow. Called after every serial task. */
  private def advanceCheckpoint(): Unit =
    checkpoint.foreach: cp =>
      if fatal.isDefined then
        if !cp.ioInFlight then
          abandon(cp)
      else if frozen then
        () // paused where it stands; a held step of its own is retried by the retry round
      else
        cp.phase match
          case CheckpointPhase.Draining =>
            // Every journal-s batch is durable
            if cp.oldTail.isCompleted && journal.idle then
              writeNextHeader(cp)
          case CheckpointPhase.Applying =>
            // Step 2: every journal-s Delta is applied in place or cancelled. A cancelled one's slot may still be
            // mapped in the snapshot, so the record that cancelled it (appended to s+1 by now) must be durable too:
            // wait for everything appended so far.
            if !unappliedBySeq.keysIterator.exists(_ < cp.seq) then
              cp.phase = CheckpointPhase.Barrier
              cp.barrier = lastAppend
              if cp.barrier.isCompleted then finish(cp) else wake(cp.barrier)
          case CheckpointPhase.Barrier =>
            if cp.barrier.isCompleted then
              finish(cp)
          case CheckpointPhase.Header | CheckpointPhase.Finishing => ()

  /** Rest of step 1: header s+1 into the other journal file, forced; only then the new writer, which receives the
    * buffered records in order
    */
  private def writeNextHeader(cp: Checkpoint): Unit =
    cp.phase = CheckpointPhase.Header
    val name = FsifRecovery.JournalFiles.find(_ != activeJournal).get
    checkpointIo[(FsifFile, Long)](cp, (f, _) => f.close()) {
      val f = files.open(name)
      try (f, FsifRecovery.writeJournalHeader(f, cp.seq))
      catch
        case NonFatal(t) =>
          f.close()
          throw t
    } { (f, salt) =>
      cp.oldFile = Some(journalFile)
      journalFile = f
      activeJournal = name
      journal = newJournal(f, JournalFormat.HeaderSize, 0, salt)
      cp.phase = CheckpointPhase.Applying
      val buffered = switchBuffer.toList
      switchBuffer.clear()
      buffered.foreach((records, p) => p.completeWith(journal.append(records)))
    }

  /** Steps 2 (force data), 3 (index s+1 into the other index file) and 4 (empty journal s and index s). Each is
    * idempotent, so the whole sequence can be re-run from its start.
    */
  private def finish(cp: Checkpoint): Unit =
    cp.phase = CheckpointPhase.Finishing
    val oldIndex = activeIndex
    val newIndex = FsifRecovery.IndexFiles.find(_ != oldIndex).get
    val oldJournal = cp.oldFile.get
    checkpointIo(cp) {
      dataFile.force()
      withFile(newIndex): f =>
        f.truncate(0)
        f.write(0, IndexFile.encode(cp.seq, cp.snapshot))
        f.force()
      oldJournal.truncate(0)
      oldJournal.force()
      withFile(oldIndex): f =>
        f.truncate(0)
        f.force()
    } { _ =>
      cp.oldFile = None
      oldJournal.close()
      activeIndex = newIndex
      currentSeq = cp.seq
      checkpoint = None
      cp.done.success(())
    }

  private def abandon(cp: Checkpoint): Unit =
    checkpoint = None
    cp.oldFile.foreach(f => Try(f.close()))
    cp.done.tryFailure(new IOException(s"FSIF store $storeId has failed", fatal.orNull))

  private def withFile(name: String)(f: FsifFile => Unit): Unit =
    val file = files.open(name)
    try f(file)
    finally file.close()

  /** Runs one checkpoint step on io, then `next` on serial. ENOSPC holds the step, which the retry round re-runs
    * from its start; any other failure is fail-stop (Ruling 15). A success that arrives after a fatal error is handed
    * to `discard`.
    */
  private def checkpointIo[A](cp: Checkpoint, discard: A => Unit = (_: A) => ())(work: => A)(next: A => Unit): Unit =
    val step = () => work
    def run(): Unit =
      cp.ioInFlight = true
      def done(r: Try[A]): Unit =
        cp.ioInFlight = false
        r match
          case Failure(t) if fatal.isEmpty && FsifIo.isOutOfSpace(t) =>
            logger.warn(s"FSIF store $storeId: checkpoint to seq ${cp.seq} is out of space; holding it")
            hold(cp, "checkpoint", () => run(), () => ())
          case Failure(t) =>
            logger.error(s"FSIF store $storeId: checkpoint to seq ${cp.seq} failed: $t")
            failStop(t)
          case Success(a) =>
            released(cp)
            if fatal.isDefined then discard(a) else next(a)
      try
        io.execute(new IoTask(IoKind.Checkpoint, () => {
          val r = Try(step())
          post(done(r))
        }))
      catch
        case NonFatal(t) => done(Failure(t))
    run()

  // ---- Commit and repair ----

  private def update(state: CommitState, repairing: Boolean, reply0: Reply): Unit =
    var replied = false
    val wasThrottled = unappliedBytes > tuning.unappliedBound
    val reply: Reply = r =>
      if !replied then
        replied = true
        r match
          case Left(_) => ack(reply0, wasThrottled)
          case Right(_) => reply0(r)
    try
      if fatal.isDefined then
        reply(Right(CommitError.IOFailure))
      else
        decodeStorePointer(state.pointer.storePointer, layout.segmentCount) match
          case None => reply(Right(CommitError.InvalidPointer))
          case Some(x) =>
            if state.data.size > layout.segmentPayloadSize then
              reply(Right(CommitError.SliceTooLarge))
            else
              withExt(x)(replyOnError(reply)(updateExt(x, state, repairing, reply)))
    catch
      case NonFatal(t) =>
        reply(Right(CommitError.IOFailure))
        throw t

  /** The commit decision table, run once X has no copy-on-write in progress */
  private def updateExt(x: Int, state: CommitState, repairing: Boolean, reply: Reply): Unit =
    val id = state.objectId
    val slot = index.slots(x)
    val occupant = index.ids(x)
    // Ruling 18: Frontend repairs a missed allocation with allocating = false, so a repair allocates an unmapped X.
    // A repair never overwrites a different occupant: that missed delete heals first through repairDelete.
    val allocating = if repairing then slot < 0 else state.allocating
    if fatal.isDefined then
      reply(Right(CommitError.IOFailure))
    else if state.metadata.refcount.count == 0 then
      if slot >= 0 && occupant != id then
        logger.warn(s"FSIF store $storeId: delete of $id at ext $x, which holds $occupant; not deleting it")
        reply(Right(CommitError.ObjectMismatch))
      else
        deleteExt(x, reply)
    else if !allocating && (slot < 0 || occupant != id) then
      reply(Right(CommitError.ObjectMismatch))
    else
      if allocating && slot >= 0 && occupant != id then
        logger.warn(s"FSIF store $storeId: allocating $id at ext $x over $occupant (a missed delete); overwriting it")
      val header = SegmentHeader.forSlice(state.objectType, id, x, state.metadata, state.data)
      val bytes = state.data.getByteArray
      if state.form == UpdateForm.Full && bytes.length > tuning.cowThreshold then
        copyOnWrite(x, id, header, bytes, reply)
      else
        journaled(x, id, header, bytes, state.form, reply)

  private def journaled(x: Int, id: ObjectId, header: SegmentHeader, bytes: Array[Byte], form: UpdateForm,
                        reply: Reply): Unit =
    val slot = index.slots(x)
    if slot < 0 || index.ids(x) != id then
      // A new occupant: into the lowest free slot, or over a missed delete's occupant in place. Map and the full
      // Delta go in one append, so in one batch; replay drops everything for X before the Map.
      val target = if slot < 0 then allocateSlot() else slot
      def install(): Unit =
        if slot >= 0 then
          compaction.cancel(x)
          cancelQueued(slotState(slot))
        assign(x, target, id)
        val d: JournalRecord.Delta = JournalRecord.Delta(x, target, header, List(DeltaRange(0, bytes)))
        appendDelta(target, List(JournalRecord.Mapping(x, target, id), d), d, reply)
      if slot < 0 && tailCows.headOption.exists(_ < target) then
        // X waits, like behind a copy-on-write, so its later records still follow this Mapping
        gated(x) = mutable.ArrayDeque()
        afterTailCows(target):
          try
            if fatal.isDefined then reply(Right(CommitError.IOFailure)) else replyOnError(reply)(install())
          finally releaseExt(x)
      else
        install()
    else
      val d: JournalRecord.Delta = JournalRecord.Delta(x, slot, header, rangesOf(bytes, form))
      appendDelta(slot, List(d), d, reply)

  private def rangesOf(bytes: Array[Byte], form: UpdateForm): List[DeltaRange] =
    val full = List(DeltaRange(0, bytes))
    form match
      case UpdateForm.Full => full
      case UpdateForm.MetadataOnly => Nil
      case UpdateForm.Ranges(_, spans) =>
        val valid = spans.forall((o, l) => o >= 0 && l >= 0 && o.toLong + l <= bytes.length)
        if valid then
          spans.map((o, l) => DeltaRange(o, java.util.Arrays.copyOfRange(bytes, o, o + l)))
        else
          logger.warn(s"FSIF store $storeId: update spans $spans do not fit a ${bytes.length} byte slice; " +
            "journaling the whole slice")
          full

  private def appendDelta(slot: Int, records: List[JournalRecord], d: JournalRecord.Delta, reply: Reply): Unit =
    val pd = addDelta(slot, d)
    onDurable(appendRecords(records)):
      case Success(_) =>
        pd.durable = true
        slotStates.get(slot).foreach(st => kick(st))
        reply(Left(()))
      case Failure(_) =>
        reply(Right(CommitError.IOFailure))

  /** Unmaps X. An already unmapped X appends nothing: it completes at once, or once the record that unmapped it is
    * durable, so the acknowledgement never precedes the delete it relies on.
    */
  private def deleteExt(x: Int, reply: Reply): Unit =
    val slot = index.slots(x)
    if slot < 0 then
      unmapping.get(x) match
        case Some(f) => onDurable(f)(r => reply(if r.isSuccess then Left(()) else Right(CommitError.IOFailure)))
        case None => reply(Left(()))
    else
      compaction.cancel(x)
      assign(x, -1, Zero)
      val dead = kill(slot)
      val f = appendRecords(List(JournalRecord.Mapping(x, -1, Zero)))
      unmapping(x) = f
      onDurable(f):
        case Success(_) =>
          if unmapping.get(x).contains(f) then unmapping.remove(x)
          unmapDurable(dead)
          reply(Left(()))
        case Failure(_) =>
          reply(Right(CommitError.IOFailure))

  /** Durable-before-map: the whole segment is written to a free slot f and forced before Mapping(X, f) is appended.
    * X is gated meanwhile, so every later record for X follows the Mapping and targets f.
    */
  private def copyOnWrite(x: Int, id: ObjectId, header: SegmentHeader, bytes: Array[Byte], reply: Reply): Unit =
    compaction.cancel(x)
    val before = slotTotal
    val f = allocateSlot()
    if f == before then
      tailCows += f
    gated(x) = mutable.ArrayDeque()
    val segment = SegmentHeader.encode(header) ++ bytes
    val key = new Object
    def issue(): Unit =
      try
        io.execute(new IoTask(IoKind.Cow, () => {
          val r = Try {
            dataFile.write(layout.slotOffset(f), segment)
            dataFile.force()
          }
          post(cowWritten(key, () => issue(), x, id, f, r, reply))
        }))
      catch
        // A rejected task fails like a failed write: fail-stop, reply, and release the gate and tail slot
        case NonFatal(t) => cowWritten(key, () => issue(), x, id, f, Failure(t), reply)
    issue()

  /** ENOSPC holds the copy-on-write with X still gated and f reserved; the retry rewrites and forces the segment */
  private def cowWritten(key: AnyRef, reissue: () => Unit, x: Int, id: ObjectId, f: Int, r: Try[Unit],
                         reply: Reply): Unit =
    r match
      case Failure(t) if fatal.isEmpty && FsifIo.isOutOfSpace(t) =>
        logger.warn(s"FSIF store $storeId: copy-on-write to slot $f is out of space; holding it")
        hold(key, "copy-on-write", reissue,
          () => cowWritten(key, reissue, x, id, f, Failure(fatal.getOrElse(t)), reply))
      case _ =>
        released(key)
        try
          r match
            case Failure(t) =>
              logger.error(s"FSIF store $storeId: copy-on-write to slot $f failed: $t")
              failStop(t)
              reply(Right(CommitError.IOFailure))
              releaseExt(x)
            case Success(_) =>
              // No wait for lower tail slots: forcing f made the durable data size reach past f and any gap below it
              cowMap(x, id, f, reply)
        finally tailCowDone(f)

  /** Appends Mapping(X, f) for a forced copy-on-write segment, then lets X's held operations run */
  private def cowMap(x: Int, id: ObjectId, f: Int, reply: Reply): Unit =
    try
      if fatal.isDefined then
        reply(Right(CommitError.IOFailure))
      else
        replyOnError(reply):
          val old = index.slots(x)
          assign(x, f, id)
          val dead = if old >= 0 then Some(kill(old)) else None
          onDurable(appendRecords(List(JournalRecord.Mapping(x, f, id)))):
            case Success(_) =>
              dead.foreach(unmapDurable)
              reply(Left(()))
            case Failure(_) =>
              reply(Right(CommitError.IOFailure))
    finally releaseExt(x)

  private def startRepairDelete(objectId: ObjectId, storePointer: Array[Byte], reply: Reply): Unit =
    if fatal.isDefined then
      reply(Right(CommitError.IOFailure))
    else if storePointer.isEmpty then
      logger.warn(s"FSIF store $storeId: repairDelete of $objectId without a store pointer; nothing to delete")
      reply(Left(()))
    else
      decodeStorePointer(storePointer, layout.segmentCount) match
        case None =>
          logger.warn(s"FSIF store $storeId: repairDelete of $objectId with an invalid store pointer")
          reply(Left(()))
        case Some(x) =>
          withExt(x)(replyOnError(reply):
            if fatal.isDefined then
              reply(Right(CommitError.IOFailure))
            else if index.slots(x) >= 0 && index.ids(x) == objectId then
              deleteExt(x, reply)
            else
              reply(Left(())))

  // ---- Reads ----

  private def completeRead(id: ObjectId, result: Either[ReadState, ReadError.Value]): Unit =
    handler.foreach(_.complete(Read(storeId, id, result)))

  private def startRead(pointer: ObjectPointer): Unit =
    if fatal.isDefined then
      completeRead(pointer.id, Right(ReadError.CorruptedObject))
    else
      decodeStorePointer(pointer.storePointer, layout.segmentCount) match
        case None => completeRead(pointer.id, Right(ReadError.ObjectNotFound))
        case Some(x) =>
          // Not behind X's gate: while X is gated its index entry is unchanged (the gated operation assigns it in the
          // task that releases the gate), and a copy-on-write held by ENOSPC must not hold reads
          try readExt(pointer, x)
          catch
            case NonFatal(t) =>
              logger.error(s"FSIF store $storeId: read of ${pointer.id} failed: $t")
              completeRead(pointer.id, Right(ReadError.CorruptedObject))

  private def readExt(pointer: ObjectPointer, x: Int): Unit =
    val slot = index.slots(x)
    if fatal.isDefined then
      completeRead(pointer.id, Right(ReadError.CorruptedObject))
    else if slot < 0 || index.ids(x) != pointer.id then
      completeRead(pointer.id, Right(ReadError.ObjectNotFound))
    else
      val st = slotState(slot)
      st.reads += 1
      readsInFlight += 1
      // Any Delta that grows the slice past this length arrives later, so stays in the list until this read ends
      val issueLength = st.newestHeader.map(_.sliceLength).getOrElse(-1)
      val issueVersion = st.version
      val base = layout.slotOffset(slot)
      try
        io.execute(new IoTask(IoKind.Read, () => {
          val r = Try {
            val disk = SegmentHeader.decode(dataFile.read(base, SegmentHeader.Size))
              .filter(h => h.sliceLength >= 0 && h.sliceLength <= layout.segmentPayloadSize)
            val length = math.max(issueLength, disk.map(_.sliceLength).getOrElse(0))
            (disk, if length > 0 then dataFile.read(base + SegmentHeader.Size, length) else Array.emptyByteArray)
          }
          post(finishRead(pointer, x, st, issueVersion, r))
        }))
      catch
        case NonFatal(t) =>
          finishRead(pointer, x, st, issueVersion, Failure(t))

  private def finishRead(pointer: ObjectPointer, x: Int, st: SlotState, issueVersion: Long,
                         r: Try[(Option[SegmentHeader], Array[Byte])]): Unit =
    st.reads -= 1
    readsInFlight -= 1
    val moved = index.slots(x) != st.slot || index.ids(x) != pointer.id || !slotStates.get(st.slot).contains(st)
    val result: Option[Either[ReadState, ReadError.Value]] =
      if fatal.isDefined then
        Some(Right(ReadError.CorruptedObject))
      else if moved then
        None
      else
        val res = Try(assemble(pointer.id, x, st, r)).getOrElse(Right(ReadError.CorruptedObject))
        // A disk read racing an in-place write can tear the header (pread/pwrite are not atomic), e.g. its length
        // when no Delta was pending at issue. If the slot changed meanwhile, the failure may be that race: retry.
        val raced = res == Right(ReadError.CorruptedObject) && r.isSuccess && st.version != issueVersion
        if raced then None else Some(res)
    st.prune()
    if st.dead then
      tryFree(st)
    else
      dropIfUnused(st)
    result match
      case Some(res) => completeRead(pointer.id, res)
      case None => startRead(pointer) // X moved or changed meanwhile, or the read raced a write: read again

  private def assemble(id: ObjectId, x: Int, st: SlotState,
                       r: Try[(Option[SegmentHeader], Array[Byte])]): Either[ReadState, ReadError.Value] =
    r match
      case Failure(t) =>
        logger.error(s"FSIF store $storeId: read of $id from slot ${st.slot} failed: $t")
        Right(ReadError.CorruptedObject)
      case Success((disk, slice)) =>
        st.newestHeader.orElse(disk) match
          case None =>
            logger.error(s"FSIF store $storeId: slot ${st.slot} holding $id has no valid header")
            Right(ReadError.CorruptedObject)
          case Some(h) if h.sliceLength < 0 || h.sliceLength > layout.segmentPayloadSize =>
            Right(ReadError.CorruptedObject)
          case Some(h) =>
            val buf = SlotState.overlay(h, slice, st.deltas)
            if !SegmentHeader.verify(h, buf) then
              logger.error(s"FSIF store $storeId: CRC mismatch reading $id from slot ${st.slot}")
              Right(ReadError.CorruptedObject)
            else if h.objectId != id || h.externalIndex != x then
              Right(ReadError.ObjectNotFound)
            else
              Left(ReadState(id, h.metadata, h.objectType, DataBuffer(buf), Set()))

object FsifBackend:
  private val Zero = ObjectId(new UUID(0, 0))

  /** Runs a task once after a delay: the ENOSPC retry timer */
  trait RetryScheduler:
    def schedule(delay: FiniteDuration, task: Runnable): Unit

  /** Host-wide retry timer: one daemon thread named fsif-retry */
  object SharedRetryScheduler extends RetryScheduler:
    private lazy val executor: ScheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor: r =>
        val t = new Thread(r, "fsif-retry")
        t.setDaemon(true)
        t
    def schedule(delay: FiniteDuration, task: Runnable): Unit =
      executor.schedule(task, delay.toNanos, TimeUnit.NANOSECONDS)

  /** Draining: buffering, waiting for journal s to be durable. Header: buffering, header s+1 being written.
    * Applying: waiting for journal-s Deltas. Barrier: waiting for the records appended so far. Finishing: io for
    * steps 2-4.
    */
  private enum CheckpointPhase:
    case Draining, Header, Applying, Barrier, Finishing

  /** A checkpoint from seq - 1 to seq. `oldTail` is the last append to journal seq - 1. */
  private final class Checkpoint(val seq: Long, val snapshot: IndexMap, val oldTail: Future[Unit]):
    val done: Promise[Unit] = Promise()
    var phase: CheckpointPhase = CheckpointPhase.Draining
    var ioInFlight = false
    var barrier: Future[Unit] = Future.unit
    /** Journal s's handle once the writer has switched, until step 4 has emptied it */
    var oldFile: Option[FsifFile] = None
    def switching: Boolean = phase == CheckpointPhase.Draining || phase == CheckpointPhase.Header

  /** An operation held by ENOSPC: `retry` re-issues its I/O, `abort` ends it on fail-stop */
  private final class HeldOp(val retry: () => Unit, val abort: () => Unit)

  /** A repair or repairDelete that could not be applied */
  final class OperationFailed(msg: String) extends Exception(msg)

  private[fsif] enum IoKind:
    case Read, InPlace, Cow, Journal, Checkpoint, MoveRead, MoveWrite, MoveForce

  /** Test counters: moves started, moves whose Map(X, f) is durable, and moves cancelled */
  private[fsif] final case class CompactionStats(started: Int, mapped: Int, cancelled: Int)

  /** An io task, labelled so tests can hold one kind back */
  private[fsif] final class IoTask(val kind: IoKind, body: () => Unit) extends Runnable:
    def run(): Unit = body()

  /** Host-wide I/O pool: max(4, availableProcessors) daemon threads named fsif-io-N */
  lazy val sharedIo: ExecutionContext =
    val counter = new AtomicInteger(0)
    val factory: ThreadFactory = r =>
      val t = new Thread(r, s"fsif-io-${counter.getAndIncrement()}")
      t.setDaemon(true)
      t
    ExecutionContext.fromExecutorService(
      Executors.newFixedThreadPool(math.max(4, Runtime.getRuntime.availableProcessors), factory))

  /** The external index: exactly 4 bytes, a big-endian Int in [0, segmentCount) */
  def decodeStorePointer(sp: Array[Byte], segmentCount: Int): Option[Int] =
    if sp == null || sp.length != 4 then
      None
    else
      val x = ByteBuffer.wrap(sp).getInt
      if x >= 0 && x < segmentCount then Some(x) else None

  /** Creates and initializes a new store in `path`, then opens it */
  def create(storeId: StoreId, path: os.Path, layout: FsifLayout, io: ExecutionContext,
             onFatal: Throwable => Unit): FsifBackend =
    os.makeDir.all(path)
    val files = new NioFsifFiles(path)
    FsifRecovery.initialize(files, layout)
    new FsifBackend(storeId, path, layout, files, io, FsifTuning(), onFatal)
