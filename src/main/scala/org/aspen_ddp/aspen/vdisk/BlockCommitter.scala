package org.aspen_ddp.aspen.vdisk

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectRangeState, InvalidObject, ObjectAllocator, RangedWriteState,
  StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyAlreadyExists, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.vdisk.VDiskError.*

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Runs a `VirtualDisk`'s commits: the data transaction loop of one block and the map transaction loop of one batch,
  * each retried under the client's backoff, with the resolution of a sent transaction that reported failure.
  *
  * The committer holds no disk state. Everything it reads or changes goes through `Host`, whose methods take the
  * disk's monitor themselves, so no lock is held across an Aspen call here.
  */
private[vdisk] final class BlockCommitter(client: AspenClient,
                                          info: DiskInfo,
                                          fenceRevision: ObjectRevision,
                                          blockMap: BlockMap,
                                          allocator: ObjectAllocator,
                                          host: BlockCommitter.Host):
  import BlockCommitter.*
  import VirtualDisk.covers

  private given ExecutionContext = client.clientContext

  // ---- Loop helpers ----

  /** Runs `attempt` under the client's backoff until it succeeds. Only `RetryCommit` retries: anything else, a
    * synchronous throw included, ends the loop with the error that stops the disk
    */
  private def commitLoop(context: String, attempt: () => Future[Unit])
                        (onSuccess: () => Unit, onFailure: VDiskError => Unit): Unit =
    val guarded = () => Future.delegate(attempt()).recoverWith:
      case RetryCommit => Future.failed(RetryCommit)
      case e => Future.failed(stopWith(e))
    client.retryStrategy.retryUntilSuccessful(guarded()).onComplete:
      case Success(_) => onSuccess()
      case Failure(e) => onFailure(asError(e, context))

  /** Turns any failure but StopRetrying into RetryCommit */
  private def retryOn[A](f: Future[A]): Future[A] = f.recoverWith:
    case e: StopRetrying => Future.failed(e)
    case _ => Future.failed(RetryCommit)

  /** Turns a successful commit into a failure when the test hook asks for it */
  private def injectFailure[A](result: A): Future[A] =
    if host.takeInjectedFailure() then Future.failed(InjectedFailure) else Future.successful(result)

  // ---- Data transactions ----

  /** Commits the block's dirty data to its object. A transaction that was sent and failed is resolved before the
    * next one is built: the block's revision shows whether it committed. Once the block has been unmapped since
    * `epoch` its object is to be deleted, so the loop ends at the next attempt without resolving anything
    */
  def commitBlock(b: Long, st: BlockState, epoch: Long): Unit =
    // Attempts run one after another, each starting once the previous one's future has completed
    var unresolved: Option[SentData] = None

    def attempt(): Future[Unit] = host.attemptGate().flatMap: _ =>
      val resolved = unresolved match
        case Some(sent) if !host.superseded(st, epoch) => resolveData(b, st, sent)
        case _ => Future.successful(false)
      resolved.flatMap: done =>
        unresolved = None
        if done then
          Future.unit
        else
          host.checkServing()
          host.dataInput(b, st, epoch) match
            case None => Future.unit
            case Some(input) =>
              @volatile var sent: Option[DataCommit] = None
              runDataTx(input, c => sent = Some(c)).transformWith:
                case Success(c) =>
                  host.dataCommitted(b, st, input, c, recovered = false)
                  Future.unit
                case Failure(e) =>
                  sent match
                    case Some(c) =>
                      unresolved = Some(SentData(input, c))
                      Future.failed(RetryCommit)
                    case None => Future.failed(stopWith(e))

    commitLoop(s"commit of block $b", () => attempt())(() => (), host.dataFailed)

  /** Whether the sent transaction committed, from the block's current revision. One that didn't, against a
    * revision learned from a read, makes the block learn its revision again. Anything else stops the disk
    */
  private def resolveData(b: Long, st: BlockState, sent: SentData): Future[Boolean] =
    retryOn(host.fenceHeld()).flatMap: _ =>
      client.readRange(sent.input.ptr, 0, Some(0)).transform:
        case Success(s) =>
          if s.revision == sent.commit.revision then
            host.dataCommitted(b, st, sent.input, sent.commit, recovered = true)
            Success(true)
          else if s.revision == sent.commit.base then
            Success(false)
          else if sent.input.learned then
            host.forgetRevision(b, st)
            Success(false)
          else
            Failure(StopRetrying(DiskFailed(s"block $b is at revision ${s.revision}, neither the committed " +
              s"${sent.commit.base} nor ${sent.commit.revision} of the failed transaction")))
        case Failure(e: InvalidObject) => Failure(StopRetrying(DiskFailed(s"block $b's object is gone: $e")))
        case Failure(_) => Failure(RetryCommit)

  /** Commits `snap` to a mapped block. Dirty data covering the whole block replaces it; anything else is a ranged
    * write, whose pre-image comes from the clean cache when it holds every byte written, and otherwise from a read.
    * The clean cache may have evicted the block, or hold only part of it. `onSent` gets the transaction's result
    * as the transaction is sent, before its outcome is known
    */
  private def runDataTx(input: DataInput, onSent: DataCommit => Unit): Future[DataCommit] =
    host.commitGate().flatMap: _ =>
      host.checkServing()
      val DataInput(ptr, revision, length, clean, snap, _, _) = input
      val blockSize = host.blockObjectSize
      val result =
        if covers(snap.pieces, 0, blockSize) then
          val content = new Array[Byte](blockSize)
          snap.overlay(content, 0)
          val frevision = revision match
            case Some(r) => Future.successful(r)
            case None => client.readMetadata(ptr).map(_.revision)
          frevision.flatMap: r =>
            client.transact: tx =>
              tx.sharedLockRevision(info.fencePointer, fenceRevision)
              tx.overwrite(ptr, r, DataBuffer(content))
              val c = DataCommit(tx.revision, blockSize, overwrote = true, base = r)
              onSent(c)
              Future.successful(c)
        else
          val writes = snap.writes
          val fstate = (revision, length) match
            case (Some(r), Some(len)) if writes.forall(w => covers(clean, w.offset, math.min(w.end, len).toInt)) =>
              val needed = clean.filter: (o, bytes) =>
                o + bytes.length <= len && writes.exists(w => o < w.end && w.offset < o + bytes.length)
              Future.successful(RangedWriteState.fromRanges(ptr, r, len, needed.toList))
            case _ =>
              host.preImageRead()
              client.readRangedWriteState(ptr, writes)
          fstate.flatMap: state =>
            client.transact: tx =>
              tx.sharedLockRevision(info.fencePointer, fenceRevision)
              tx.rangedWrites(state, writes, zeroFill = true)
              val c = DataCommit(tx.revision, math.max(state.length, snap.end), overwrote = false,
                base = state.revision)
              onSent(c)
              Future.successful(c)
      result.flatMap(injectFailure)

  // ---- Map transactions ----

  /** Runs one map batch: creates blocks, replaces the objects of unmapped blocks written again, and deletes those of
    * unmapped blocks. A create superseded by an unmap leaves the batch, and its deletion goes back to the disk.
    *
    * After a sent attempt fails, the fence is checked before the next. The next attempt then finds what a failed
    * attempt that did commit changed: a created block already in the map (`KeyAlreadyExists`), or an object to
    * delete that is gone. A created or replacing block adopts the object in the map and leaves its dirty data to a
    * data transaction; a deleted block is done. Finding either with no attempt sent means another writer changed
    * the map, so the fence is checked first
    */
  def commitMap(batch: List[MapEntry]): Unit =
    // Attempts run one after another, each starting once the previous one's future has completed
    var remaining = batch
    var checkFence = false
    // Set as an attempt is sent and read once its future has completed
    @volatile var anySent = false

    def leave(e: MapEntry): Unit = remaining = remaining.filterNot(_.index == e.index)

    /** Another writer's change is only possible once another owner has taken the disk */
    def foreignChangeChecked(): Future[Unit] = if anySent then Future.unit else retryOn(host.fenceHeld())

    def adopt(e: MapEntry, found: Option[DataObjectPointer]): Future[Unit] =
      val index = e.index
      val adopted = found match
        case None => Future.failed(StopRetrying(DiskFailed(s"block $index was found in the map, then not")))
        case Some(ptr) =>
          client.readRange(ptr, 0, Some(0)).transform:
            case Failure(e: InvalidObject) => Failure(StopRetrying(DiskFailed(s"block $index's object is gone: $e")))
            case r => r.map(s => (ptr, s))
      retryOn(adopted).map: (ptr, s) =>
        leave(e)
        host.adopted(e, ptr, s)

    /** The object in the map for an entry with a deletion: its pointer, with its refcount unless it is gone */
    def findOld(e: MapEntry): Future[Option[(DataObjectPointer, Option[ObjectRefcount])]] = e.delete match
      case None => Future.successful(None)
      case Some(pd) =>
        val mapped = host.deleteLookupGate().flatMap: _ =>
          if pd.refresh then blockMap.refresh(e.index) else blockMap.lookup(e.index)
        retryOn(mapped).flatMap:
          case None => Future.successful(None)
          case Some(ptr) =>
            retryOn(client.readMetadata(ptr).transform:
              case Success(md) => Success(Some((ptr, Some(md.refcount))))
              case Failure(_: InvalidObject) => Success(Some((ptr, None)))
              case Failure(err) => Failure(err))

    /** The entry's object `ptr` is gone, so an earlier attempt committed the entry: a create adopts the object that
      * attempt put in the map, and a deletion is done. A create finding nothing in the map stays, as an insert
      */
    def gone(e: MapEntry, ptr: DataObjectPointer): Future[Unit] =
      foreignChangeChecked().flatMap(_ => retryOn(blockMap.refresh(e.index))).flatMap:
        case Some(p) if p == ptr =>
          Future.failed(StopRetrying(DiskFailed(s"block ${e.index}'s object is gone but still mapped")))
        case Some(p) if e.create => adopt(e, Some(p))
        case Some(p) => Future.failed(StopRetrying(DiskFailed(s"block ${e.index} is mapped to $p, which it never had")))
        case None =>
          if !e.create then
            leave(e)
            host.mapEntryDone(e)
          Future.unit

    def attempt(): Future[Unit] = host.attemptGate().flatMap: _ =>
      val fenceChecked = if checkFence then retryOn(host.fenceHeld()) else Future.unit
      fenceChecked.flatMap: _ =>
        checkFence = false
        host.checkServing()
        remaining = host.dropSuperseded(remaining)
        val resolving = Future.traverse(remaining)(e => findOld(e).map(old => (e, old))).flatMap: found =>
          // One at a time, since entries leave the batch as they are resolved
          val gones = found.collect { case (e, Some((ptr, None))) => (e, ptr) }
          gones.foldLeft(Future.unit)((f, g) => f.flatMap(_ => gone(g._1, g._2))).map: _ =>
            found.map((e, old) => (e, old.collect { case (ptr, Some(refcount)) => OldObject(ptr, refcount) }))
        resolving.flatMap: resolved =>
          val live = resolved.filter((e, _) => remaining.exists(_.index == e.index))
          // A deletion with nothing mapped is done already
          val (empty, work) = live.partition((e, old) => !e.create && old.isEmpty)
          empty.foreach: (e, _) =>
            leave(e)
            host.mapEntryDone(e)
          val creates = work.filter(_._1.create)
          val snaps = host.mapInputs(creates.map(_._1))
          // The initial content runs to the end of the dirty data, with zeros in the gaps. The rest of the block is
          // beyond the object's end and reads as zeros without being stored
          val planned = creates.lazyZip(snaps).flatMap: (entry, osnap) =>
            osnap match
              case None =>
                leave(entry._1)
                None
              case Some(snap) =>
                val content = new Array[Byte](snap.end)
                snap.overlay(content, 0)
                Some(PlannedCreate(entry._1, snap, content, entry._2))
          val deletes = work.collect { case (e, Some(old)) if !e.create => (e, old) }
          if planned.isEmpty && deletes.isEmpty then
            Future.unit
          else
            @volatile var sent = false
            val onSent = () =>
              sent = true
              anySent = true
            runMapTx(planned, deletes, onSent).transformWith:
              case Success(result) =>
                host.mapCommitted(result)
                Future.unit
              case Failure(e: KeyAlreadyExists) if !sent =>
                remaining.find(_.index == e.key.longValue) match
                  case Some(entry) if entry.create =>
                    foreignChangeChecked()
                      .flatMap(_ => retryOn(blockMap.refresh(entry.index)))
                      .flatMap(found => adopt(entry, found))
                      .flatMap(_ => if remaining.isEmpty then Future.unit else Future.failed(RetryCommit))
                  case _ =>
                    Future.failed(StopRetrying(DiskFailed(
                      s"map transaction found block ${e.key.longValue}, which it does not add")))
              case Failure(_) if sent =>
                checkFence = true
                Future.failed(RetryCommit)
              case Failure(e) => Future.failed(stopWith(e))
          end if

    commitLoop("map transaction", () => attempt())(host.mapDone, host.mapFailed)

  private def runMapTx(creates: List[PlannedCreate], deletes: List[(MapEntry, OldObject)], onSent: () => Unit)
      : Future[MapResult] = host.commitGate().flatMap: _ =>
    host.checkServing()
    val result = client.transact: tx =>
      given Transaction = tx
      tx.sharedLockRevision(info.fencePointer, fenceRevision)

      val olds = creates.flatMap(_.old) ++ deletes.map(_._2)
      olds.foreach(o => tx.setRefcount(o.ptr, o.refcount, o.refcount.update(0)))
      // The disk's block objects all come from one allocator, so its deletions stay within one FSIF pool
      if olds.nonEmpty then
        tx.deferAllocationRelease()

      // One at a time: allocation strategies may keep per-transaction state
      def allocate(rest: List[PlannedCreate], acc: List[(PlannedCreate, DataObjectPointer)])
          : Future[List[(PlannedCreate, DataObjectPointer)]] = rest match
        case Nil => Future.successful(acc.reverse)
        case c :: tail =>
          allocator.allocateDataObject(DataBuffer(c.content)).flatMap(ptr => allocate(tail, (c, ptr) :: acc))

      for
        created <- allocate(creates, Nil)
        (replacing, inserting) = created.partition(_._1.old.nonEmpty)
        _ <- blockMap.prepare(inserting.map((c, p) => (c.entry.index, p)), replacing.map((c, p) => (c.entry.index, p)),
          deletes.map(_._1.index))
      yield
        onSent()
        MapResult(created, deletes.map(_._1), tx.revision, olds.size)
    result.flatMap(injectFailure)

private[vdisk] object BlockCommitter:

  /** The disk state a committer works on. Every method takes the disk's monitor itself, and none blocks */
  trait Host:
    /** Fails with StopRetrying(the stop error) once the disk has stopped, so no new transaction is started */
    def checkServing(): Unit
    /** Succeeds while the owner still holds the fence; see `VirtualDisk.fenceHeld` */
    def fenceHeld(): Future[Unit]
    def commitGate(): Future[Unit]
    /** Test hooks: see `VirtualDisk.attemptGate` and `deleteLookupGate` */
    def attemptGate(): Future[Unit]
    def deleteLookupGate(): Future[Unit]
    /** Whether the test hook asks for this successful commit to report failure. Clears the hook */
    def takeInjectedFailure(): Boolean
    def blockObjectSize: Int
    def preImageRead(): Unit

    /** Whether the block has been unmapped since `epoch` */
    def superseded(st: BlockState, epoch: Long): Boolean
    /** What the block's next data attempt commits, which also becomes its in-flight snapshot. None when the block has
      * been unmapped since `epoch`, which ends its commit loop
      */
    def dataInput(b: Long, st: BlockState, epoch: Long): Option[DataInput]
    /** The block's commit of `input` succeeded, which ends its commit loop. `recovered` when a resolution found it */
    def dataCommitted(b: Long, st: BlockState, input: DataInput, c: DataCommit, recovered: Boolean): Unit
    /** Forgets a revision learned from a read, and what was cached at it */
    def forgetRevision(b: Long, st: BlockState): Unit
    /** The block's commit loop ended with `err`, which stops the disk */
    def dataFailed(err: VDiskError): Unit

    /** The batch without its creates superseded by an unmap, whose deletions go back to the disk */
    def dropSuperseded(batch: List[MapEntry]): List[MapEntry]
    /** The current dirty snapshot of each created block, which also becomes its in-flight snapshot. None for a create
      * superseded by an unmap, which leaves the batch as `dropSuperseded` describes
      */
    def mapInputs(creates: List[MapEntry]): List[Option[SegmentSnapshot]]
    def mapCommitted(result: MapResult): Unit
    /** A failed attempt did create the block: it now has `ptr`, at the revision and length of `s` */
    def adopted(e: MapEntry, ptr: DataObjectPointer, s: DataObjectRangeState): Unit
    /** The entry's deletion needs no transaction: nothing is mapped to the block any more */
    def mapEntryDone(e: MapEntry): Unit
    /** The map transaction has nothing left to do */
    def mapDone(): Unit
    /** The map loop ended with `err`, which stops the disk */
    def mapFailed(err: VDiskError): Unit

  /** A committed data transaction: the revision it created and the one it replaced */
  final case class DataCommit(revision: ObjectRevision, length: Int, overwrote: Boolean, base: ObjectRevision)

  /** What a data transaction attempt commits and the block state it is built on. `learned` is true when the
    * revision was not set by one of this owner's commits
    */
  final case class DataInput(ptr: DataObjectPointer,
                             revision: Option[ObjectRevision],
                             length: Option[Int],
                             clean: Vector[(Int, Array[Byte])],
                             snap: SegmentSnapshot,
                             learned: Boolean,
                             epoch: Long)

  /** A deletion queued by UNMAP. `seq` orders it for FLUSH; `refresh` when the block's map entry held in memory may
    * be stale, after a map attempt with an unknown outcome
    */
  final case class PendingDelete(seq: Long, refresh: Boolean)

  /** A block in a map batch: `create` when its dirty data goes into a new object, `delete` when the object in the
    * map, if any, is to be deleted. `epoch` is the block's `unmaps` count when the batch began
    */
  final case class MapEntry(index: Long, st: BlockState, epoch: Long, create: Boolean, delete: Option[PendingDelete])

  final case class OldObject(ptr: DataObjectPointer, refcount: ObjectRefcount)

  final case class PlannedCreate(entry: MapEntry, snap: SegmentSnapshot, content: Array[Byte], old: Option[OldObject])

  /** A committed map transaction: each create with its new object, the deleted entries, and the objects deleted */
  final case class MapResult(created: List[(PlannedCreate, DataObjectPointer)],
                             deleted: List[MapEntry],
                             revision: ObjectRevision,
                             deletedObjects: Int)

  /** A data transaction that was sent and reported failure, so its outcome is unknown */
  private final case class SentData(input: DataInput, commit: DataCommit)

  /** Retries a commit loop with backoff */
  private object RetryCommit extends Exception("retry the commit"):
    override def fillInStackTrace(): Throwable = this

  private object InjectedFailure extends Exception("injected commit failure"):
    override def fillInStackTrace(): Throwable = this

  private def stopWith(e: Throwable): StopRetrying = e match
    case s: StopRetrying => s
    case other => StopRetrying(other)

  private def asError(e: Throwable, context: String): VDiskError = e match
    case err: VDiskError => err
    case StopRetrying(reason) => asError(reason, context)
    case other => DiskFailed(s"$context failed: $other")
