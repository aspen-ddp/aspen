package org.aspen_ddp.aspen.vdisk

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectRangeState, InvalidObject, ObjectAllocator, RangedWriteState,
  StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyAlreadyExists, ObjectRevision}
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
  private def commitLoop(context: String, attempt: () => Future[Unit])(onFailure: VDiskError => Unit): Unit =
    val guarded = () => Future.delegate(attempt()).recoverWith:
      case RetryCommit => Future.failed(RetryCommit)
      case e => Future.failed(stopWith(e))
    client.retryStrategy.retryUntilSuccessful(guarded()).onComplete:
      case Success(_) => ()
      case Failure(e) => onFailure(asError(e, context))

  /** Turns any failure but StopRetrying into RetryCommit */
  private def retryOn[A](f: Future[A]): Future[A] = f.recoverWith:
    case e: StopRetrying => Future.failed(e)
    case _ => Future.failed(RetryCommit)

  /** Turns a successful commit into a failure when the test hook asks for it */
  private def injectFailure[A](result: A): Future[A] =
    if host.takeInjectedFailure() then Future.failed(InjectedFailure) else Future.successful(result)

  // ---- Data transactions ----

  /** Commits the block's dirty data. A transaction that was sent and failed is resolved before the next one is
    * built: the block's revision shows whether it committed
    */
  def commitBlock(b: Long, st: BlockState): Unit =
    // Attempts run one after another, each starting once the previous one's future has completed
    var unresolved: Option[SentData] = None

    def attempt(): Future[Unit] =
      val resolved = unresolved match
        case Some(sent) => resolveData(b, st, sent)
        case None => Future.successful(false)
      resolved.flatMap: done =>
        unresolved = None
        if done then
          Future.unit
        else
          host.checkServing()
          val input = host.dataInput(b, st)
          @volatile var sent: Option[DataCommit] = None
          runDataTx(input, c => sent = Some(c)).transformWith:
            case Success(c) =>
              host.dataCommitted(b, st, input.snap, c, recovered = false)
              Future.unit
            case Failure(e) =>
              sent match
                case Some(c) =>
                  unresolved = Some(SentData(input, c))
                  Future.failed(RetryCommit)
                case None => Future.failed(stopWith(e))

    commitLoop(s"commit of block $b", () => attempt())(host.dataFailed)

  /** Whether the sent transaction committed, from the block's current revision. One that didn't, against a
    * revision learned from a read, makes the block learn its revision again. Anything else stops the disk
    */
  private def resolveData(b: Long, st: BlockState, sent: SentData): Future[Boolean] =
    retryOn(host.fenceHeld()).flatMap: _ =>
      client.readRange(sent.input.ptr, 0, Some(0)).transform:
        case Success(s) =>
          if s.revision == sent.commit.revision then
            host.dataCommitted(b, st, sent.input.snap, sent.commit, recovered = true)
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
      val DataInput(ptr, revision, length, clean, snap, _) = input
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

  /** Creates the batch's blocks. After a sent attempt fails, the fence is checked before the next. An attempt
    * finding a block already in the map (a failed attempt that did commit) adopts that block's object and leaves it
    * to a data transaction
    */
  def commitMap(batch: List[(Long, BlockState)]): Unit =
    // Attempts run one after another, each starting once the previous one's future has completed
    var remaining = batch
    var checkFence = false

    def adopt(index: Long): Future[Unit] =
      val adopted = blockMap.refresh(index).flatMap:
        case None => Future.failed(StopRetrying(DiskFailed(s"block $index was found in the map, then not")))
        case Some(ptr) =>
          client.readRange(ptr, 0, Some(0)).transform:
            case Failure(e: InvalidObject) => Failure(StopRetrying(DiskFailed(s"block $index's object is gone: $e")))
            case r => r.map(s => (ptr, s))
      retryOn(adopted).map: (ptr, s) =>
        remaining.find(_._1 == index) match
          case None => throw StopRetrying(DiskFailed(s"map transaction found block $index, which it does not add"))
          case Some((_, st)) =>
            remaining = remaining.filterNot(_._1 == index)
            host.adopted(index, st, ptr, s)
            if remaining.isEmpty then
              host.mapDone()

    def attempt(): Future[Unit] =
      val fenceChecked = if checkFence then retryOn(host.fenceHeld()) else Future.unit
      fenceChecked.flatMap: _ =>
        checkFence = false
        host.checkServing()
        val inputs = host.mapInputs(remaining)
        // The initial content runs to the end of the dirty data, with zeros in the gaps. The rest of the block is
        // beyond the object's end and reads as zeros without being stored
        val contents = inputs.map: (b, _, snap) =>
          val content = new Array[Byte](snap.end)
          snap.overlay(content, 0)
          (b, content)
        @volatile var sent = false
        runMapTx(contents, () => sent = true).transformWith:
          case Success((inserts, revision)) =>
            host.mapCommitted(inputs, contents, inserts, revision)
            Future.unit
          case Failure(e: KeyAlreadyExists) if !sent =>
            adopt(e.key.longValue).flatMap: _ =>
              if remaining.isEmpty then Future.unit else Future.failed(RetryCommit)
          case Failure(_) if sent =>
            checkFence = true
            Future.failed(RetryCommit)
          case Failure(e) => Future.failed(stopWith(e))

    commitLoop("map transaction", () => attempt())(host.mapFailed)

  private def runMapTx(contents: List[(Long, Array[Byte])], onSent: () => Unit)
      : Future[(List[(Long, DataObjectPointer)], ObjectRevision)] = host.commitGate().flatMap: _ =>
    host.checkServing()
    val result = client.transact: tx =>
      given Transaction = tx
      tx.sharedLockRevision(info.fencePointer, fenceRevision)

      // One at a time: allocation strategies may keep per-transaction state
      def allocate(rest: List[(Long, Array[Byte])],
                   acc: List[(Long, DataObjectPointer)]): Future[List[(Long, DataObjectPointer)]] = rest match
        case Nil => Future.successful(acc.reverse)
        case (b, content) :: tail =>
          allocator.allocateDataObject(DataBuffer(content)).flatMap(ptr => allocate(tail, (b, ptr) :: acc))

      for
        inserts <- allocate(contents, Nil)
        _ <- blockMap.prepare(inserts, Nil)
      yield
        onSent()
        (inserts, tx.revision)
    result.flatMap(injectFailure)

private[vdisk] object BlockCommitter:

  /** The disk state a committer works on. Every method takes the disk's monitor itself, and none blocks */
  trait Host:
    /** Fails with StopRetrying(the stop error) once the disk has stopped, so no new transaction is started */
    def checkServing(): Unit
    /** Succeeds while the owner still holds the fence; see `VirtualDisk.fenceHeld` */
    def fenceHeld(): Future[Unit]
    def commitGate(): Future[Unit]
    /** Whether the test hook asks for this successful commit to report failure. Clears the hook */
    def takeInjectedFailure(): Boolean
    def blockObjectSize: Int
    def preImageRead(): Unit

    /** What the block's next data attempt commits, which also becomes its in-flight snapshot */
    def dataInput(b: Long, st: BlockState): DataInput
    /** The block's commit of `snap` succeeded, which ends its commit loop. `recovered` when a resolution found it */
    def dataCommitted(b: Long, st: BlockState, snap: SegmentSnapshot, c: DataCommit, recovered: Boolean): Unit
    /** Forgets a revision learned from a read, and what was cached at it */
    def forgetRevision(b: Long, st: BlockState): Unit
    /** The block's commit loop ended with `err`, which stops the disk */
    def dataFailed(err: VDiskError): Unit

    /** The current dirty snapshot of each block, which also becomes its in-flight snapshot */
    def mapInputs(batch: List[(Long, BlockState)]): List[(Long, BlockState, SegmentSnapshot)]
    def mapCommitted(inputs: List[(Long, BlockState, SegmentSnapshot)],
                     contents: List[(Long, Array[Byte])],
                     inserts: List[(Long, DataObjectPointer)],
                     revision: ObjectRevision): Unit
    /** A failed attempt did create the block: it now has `ptr`, at the revision and length of `s` */
    def adopted(index: Long, st: BlockState, ptr: DataObjectPointer, s: DataObjectRangeState): Unit
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
                             learned: Boolean)

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
