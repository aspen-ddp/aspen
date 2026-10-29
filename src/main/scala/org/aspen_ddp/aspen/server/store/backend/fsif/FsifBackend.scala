package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
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
  */
class FsifBackend(val storeId: StoreId,
                  val path: os.Path,
                  layout: FsifLayout,
                  files: FsifFiles,
                  io: ExecutionContext,
                  tuning: FsifTuning = FsifTuning(),
                  onFatal: Throwable => Unit = _ => ()) extends Backend:
  import FsifBackend.*

  private type Reply = Either[Unit, CommitError.Value] => Unit

  private val serial = new SerialExecutor(io)
  private val recovered = FsifRecovery.recover(files, layout)
  private val dataFile = files.open(FsifRecovery.DataFile)
  /** The journal the current writer appends to: its handle and name follow a checkpoint's journal switch */
  @volatile private var journalFile: FsifFile = files.open(recovered.activeJournal)
  @volatile private var activeJournal: String = recovered.activeJournal
  private var journal = new JournalWriter(journalFile, recovered.journalLength, recovered.nextBatch, recovered.salt,
    io, serial, onJournalError)

  // ---- State, owned by serial ----
  private val index: IndexMap = recovered.index
  private var slotCount: Int = recovered.slotCount
  /** Slots that are mapped, reserved (copy-on-write or rebuild targets) or waiting for the slot-freeing rule */
  private val used = new java.util.BitSet
  index.slots.foreach(s => if s >= 0 then used.set(s))
  private val slotStates = mutable.HashMap[Int, SlotState]()
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

  // ---- Backend interface ----

  def isWriteFrozen: Boolean = false

  override def setCompletionHandler(h: CompletionHandler): Unit = handler = Some(h)

  /** Completes when no journal batch, in-place write, copy-on-write, read, commit or checkpoint is outstanding.
    * After a fatal error, held in-place writes no longer count.
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
    * or a slice larger than a segment.
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
      val slot = onSerialSync:
        throwIfFatal()
        val s = rebuildMaps.get(x).map(_._1).getOrElse:
          if index.slots(x) >= 0 then index.slots(x) else allocateSlot()
        rebuildMaps(x) = (s, pointer.id)
        s
      dataFile.write(layout.slotOffset(slot), segment)

  /** Forces `data`, then appends and forces the buffered Mappings (durable-before-map) */
  override def rebuildFlush(): Unit =
    rebuildLock.synchronized:
      dataFile.force()
      val appended = Promise[Unit]()
      onSerialSync:
        throwIfFatal()
        val maps = rebuildMaps.toList
        rebuildMaps.clear()
        maps.foreach:
          case (x, (slot, id)) =>
            index.slots(x) = slot
            index.ids(x) = id
        if maps.isEmpty then
          appended.success(())
        else
          val records = maps.map { case (x, (slot, id)) => JournalRecord.Mapping(x, slot, id) }
          onDurable(appendRecords(records))(appended.complete)
      Await.result(appended.future, Duration.Inf)
      Await.result(checkpointNow(), Duration.Inf)

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
      checkQuiesce()

  private def onSerialSync[A](f: => A): A =
    val p = Promise[A]()
    serial.execute(() => p.complete(Try(f)))
    Await.result(p.future, Duration.Inf)

  private def throwIfFatal(): Unit =
    fatal.foreach(t => throw new IOException(s"FSIF store $storeId has failed", t))

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

  private def idle: Boolean =
    gated.isEmpty && writesInFlight == 0 && readsInFlight == 0 && checkpoint.isEmpty &&
      (fatal.isDefined || (journal.idle && unappliedCount == 0))

  // ---- Failure handling ----

  /** The one place that decides what a journal write or force failure means. Task 12 adds the ENOSPC freeze and
    * retry here; for now every failure is fail-stop.
    */
  private def onJournalError(t: Throwable): Unit = failStop(t)

  /** Stops accepting writes: pending commits complete with IOFailure (through their failed journal futures), later
    * commits likewise, and reads with CorruptedObject. Calls onFatal once.
    */
  private def failStop(t: Throwable): Unit =
    if fatal.isEmpty then
      logger.error(s"FSIF store $storeId: fatal I/O error, failing the store: $t")
      fatal = Some(t)
      journal.failAll(t)
      val buffered = switchBuffer.toList
      switchBuffer.clear()
      buffered.foreach((_, p) => p.tryFailure(t))
      try onFatal(t)
      catch case NonFatal(e) => logger.error(s"FSIF store $storeId: onFatal threw $e")

  // ---- Slot allocation and freeing ----

  /** The lowest free slot, always: recovery bounds slots by assuming that allocation never skips one */
  private def allocateSlot(): Int =
    val s = used.nextClearBit(0)
    if s > slotCount then
      throw new IllegalStateException(s"lowest free slot $s is above the slot count $slotCount")
    used.set(s)
    if s == slotCount then
      slotCount += 1
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
    pd

  /** Starts the next coalesced in-place write for `st` if none is in flight. Physical writes per slot are serial. */
  private def kick(st: SlotState): Unit =
    if fatal.isEmpty && !st.dead && slotStates.get(st.slot).contains(st) then
      val batch = st.nextWritable()
      if batch.nonEmpty then
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

  private def inPlaceDone(st: SlotState, batch: List[PendingDelta], r: Try[Unit]): Unit =
    writesInFlight -= 1
    st.version += 1
    r match
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
    if checkpoint.isEmpty && fatal.isEmpty && journal.length > checkpointThreshold then
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
      journal = new JournalWriter(f, JournalFormat.HeaderSize, 0, salt, io, serial, onJournalError)
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

  /** Runs one checkpoint step on io, then `next` on serial. A failure is fail-stop (Ruling 15); a success that
    * arrives after a fatal error is handed to `discard`.
    */
  private def checkpointIo[A](cp: Checkpoint, discard: A => Unit = (_: A) => ())(work: => A)(next: A => Unit): Unit =
    cp.ioInFlight = true
    def done(r: Try[A]): Unit =
      cp.ioInFlight = false
      r match
        case Failure(t) =>
          logger.error(s"FSIF store $storeId: checkpoint to seq ${cp.seq} failed: $t")
          failStop(t)
        case Success(a) =>
          if fatal.isDefined then discard(a) else next(a)
    try
      io.execute(new IoTask(IoKind.Checkpoint, () => {
        val r = Try(work)
        post(done(r))
      }))
    catch
      case NonFatal(t) => done(Failure(t))

  // ---- Commit and repair ----

  private def update(state: CommitState, repairing: Boolean, reply0: Reply): Unit =
    var replied = false
    val reply: Reply = r =>
      if !replied then
        replied = true
        reply0(r)
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
          cancelQueued(slotState(slot))
        index.slots(x) = target
        index.ids(x) = id
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
        slotStates.get(slot).foreach(kick)
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
      index.slots(x) = -1
      index.ids(x) = Zero
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
    val before = slotCount
    val f = allocateSlot()
    if f == before then
      tailCows += f
    gated(x) = mutable.ArrayDeque()
    val segment = SegmentHeader.encode(header) ++ bytes
    try
      io.execute(new IoTask(IoKind.Cow, () => {
        val r = Try {
          dataFile.write(layout.slotOffset(f), segment)
          dataFile.force()
        }
        post(cowWritten(x, id, f, r, reply))
      }))
    catch
      // A rejected task fails like a failed write: fail-stop, reply, and release the gate and tail slot
      case NonFatal(t) => cowWritten(x, id, f, Failure(t), reply)

  private def cowWritten(x: Int, id: ObjectId, f: Int, r: Try[Unit], reply: Reply): Unit =
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
          index.slots(x) = f
          index.ids(x) = id
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
          withExt(x):
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

  /** A repair or repairDelete that could not be applied */
  final class OperationFailed(msg: String) extends Exception(msg)

  private[fsif] enum IoKind:
    case Read, InPlace, Cow, Journal, Checkpoint

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
