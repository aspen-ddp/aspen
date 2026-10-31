package org.aspen_ddp.aspen.vdisk

import java.util.{Arrays, UUID}

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectRangeState, ObjectAllocator, StopRetrying}
import org.aspen_ddp.aspen.client.tkvl.KVObjectRootManager
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.vdisk.BlockCommitter.{DataCommit, DataInput, MapEntry, MapResult, PendingDelete}
import org.aspen_ddp.aspen.vdisk.VDiskError.*

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** A virtual disk served by its single owner, with write-back caching.
  *
  * Writes complete once they are in `BlockState.dirty`. Background writeback commits each block's dirty data after
  * `coalesceWindow`, or at once for FLUSH, FUA, waiting flushes and dirty data above `dirtySoftLimit`. Each block has
  * at most one commit in flight and blocks commit in parallel. Blocks without an object are created by map
  * transactions, one at a time per disk, each batching every such block waiting at the time. Every commit
  * shared-locks the fence at the revision this owner took ownership at.
  *
  * All state is guarded by this object's monitor. Aspen calls and promise completions are collected while holding
  * it (see `locked`) and run after it is released. In-flight commit data stays in `BlockState.dirty` until the
  * commit succeeds, so a read's overlay is the dirty snapshot alone.
  *
  * Committed bytes are held in a bounded `CleanCache`, always at the block's known revision. Reads fetch only what
  * neither cache holds. A read that `SequentialDetector` finds continuing a stream adds readahead, within the same
  * block object, to the same fetch.
  *
  * A failed commit is resolved before the block's next transaction is built. If the fence has moved, another owner
  * has taken the disk and it is `Fenced`. Otherwise the block's current revision shows whether the transaction
  * committed after all; if it didn't, the commit is retried with the client's backoff. A block at any other revision
  * than these two fails the disk, unless its known revision was only learned from a read. A failed map transaction
  * is retried, adopting the object of any block that an earlier attempt did add. Once the disk is no longer
  * `Serving`, every pending and later request fails with the stop error, and no transaction is started.
  *
  * UNMAP drops a block's dirty data and makes it read as zeros at once. Its object, if it has one, and its map entry
  * are deleted by a later map transaction, once any commit in flight on the block has ended: a data commit the unmap
  * supersedes is not retried, and an object a superseded map attempt created is deleted too. A block written again
  * first gets a new object in the same transaction that deletes the old one. FLUSH waits for the deletions.
  *
  * Writes wait only while the dirty total is at `dirtyHardLimit`, until writeback brings it below.
  */
final class VirtualDisk private (val client: AspenClient,
                                 val info: DiskInfo,
                                 fenceRevision: ObjectRevision,
                                 blockMap: BlockMap,
                                 allocator: ObjectAllocator,
                                 config: VDiskConfig) extends BlockDevice:
  import VirtualDisk.*

  private given ExecutionContext = client.clientContext

  @volatile private var currentGeometry: DiskGeometry = info.geometry

  def geometry: DiskGeometry = currentGeometry

  private val blocks = mutable.HashMap[Long, BlockState]()
  private var nextSeq = 0L
  /** Sum of every block's dirty bytes */
  private var dirtyTotal = 0L
  private var stopped: Option[VDiskError] = None
  private var pendingFlushes = 0
  private var mapTxRunning = false
  /** Set under the monitor to start a map transaction once the current monitor section is done, so that every
    * block it makes ready joins one batch
    */
  private var mapKickRequested = false
  /** Blocks without an object whose dirty data waits for the next map transaction */
  private val awaitingMap = mutable.TreeSet[Long]()
  /** Mapped blocks waiting for a free data transaction slot */
  private val awaitingData = mutable.LinkedHashSet[Long]()
  private var dataTxRunning = 0
  private var counters = Stats(0, 0, 0, 0, 0, 0, 0, 0, 0)
  private val cleanCache = CleanCache(config.cleanCacheBytes)
  private val detector =
    SequentialDetector(initialWindow = 4 * info.geometry.logicalBlockSize, maxWindow = config.maxReadahead)
  private var fetches = 0L
  /** Reads waiting on I/O. Stopping the disk fails them */
  private val pendingReads = mutable.HashSet[Promise[Array[Byte]]]()
  /** Unmapped blocks whose map entry, if they have one, waits for a map transaction to delete it with its object */
  private val pendingDeletes = mutable.TreeMap[Long, PendingDelete]()
  private var nextDeleteSeq = 0L
  /** Seqs of the deletions not yet done, whether pending or in the running map transaction */
  private val outstandingDeletes = mutable.TreeSet[Long]()
  /** FLUSHes waiting for every deletion up to a seq */
  private val deleteWaiters = ListBuffer[(Long, Promise[Unit])]()
  /** Writes waiting for the dirty total to drop below `dirtyHardLimit` */
  private val throttled = ListBuffer[Promise[Unit]]()

  private[vdisk] def dirtyBytes: Long = synchronized(dirtyTotal)
  private[vdisk] def mapTxInFlight: Boolean = synchronized(mapTxRunning)
  private[vdisk] def stats: Stats = synchronized(counters)
  /** Number of `readRanges` calls made by reads */
  private[vdisk] def fetchCount: Long = synchronized(fetches)
  private[vdisk] def cleanBytes(index: Long): Long = synchronized(cleanCache.get(index).map(_.bytes).getOrElse(0L))
  private[vdisk] def dropClean(index: Long): Unit = synchronized(cleanCache.invalidate(index))
  /** Performs every read fetch. Tests replace it to simulate a store serving an older revision */
  @volatile private[vdisk] var rangeReader: RangeReader = (ptr, ranges) => client.readRanges(ptr, ranges)
  /** Called as each map or data transaction starts, which proceeds once the result completes. Tests use it to
    * hold commits in flight and to see them start
    */
  @volatile private[vdisk] var commitGate: () => Future[Unit] = () => Future.unit
  /** Called by each read that needs I/O, once its overlay is taken. Tests use it to hold a read */
  @volatile private[vdisk] var readGate: () => Future[Unit] = () => Future.unit
  /** Makes the next successful data or map transaction report a failure, as a commit whose outcome was lost would */
  @volatile private[vdisk] var failNextCommitAfterSuccess: Boolean = false
  private[vdisk] def blockPointer(index: Long): Option[DataObjectPointer] =
    synchronized(blocks.get(index).flatMap(_.pointer))
  /** The block's map entry, as this owner's map knows it */
  private[vdisk] def lookupBlock(index: Long): Future[Option[DataObjectPointer]] = blockMap.lookup(index)

  def state: DiskState = synchronized:
    stopped match
      case None => DiskState.Serving
      case Some(Fenced) => DiskState.Fenced
      case Some(DiskFailed(reason)) => DiskState.Failed(reason)
      case Some(other) => DiskState.Failed(other.getMessage)

  // ---- BlockDevice ----

  def read(offset: Long, length: Int): Future[Array[Byte]] =
    val geom = currentGeometry
    geom.validate(offset, length) match
      case Some(err) => Future.failed(err)
      case None =>
        val readahead = if length == 0 then 0 else synchronized(detector.onRead(offset, length))
        // The readahead stays within the block object holding the request's end, and within the disk
        def fetchEnd(seg: BlockSegment): Int =
          val end = seg.offsetInBlock + seg.length
          if seg.offsetInRequest + seg.length < length || readahead == 0 then
            end
          else
            val blockEnd = math.min(geom.blockObjectSize.toLong, geom.size - seg.blockIndex * geom.blockObjectSize)
            math.min(end.toLong + readahead, blockEnd).toInt
        geom.segments(offset, length) match
          case Nil => Future.successful(Array.emptyByteArray)
          case seg :: Nil => readSegment(seg, fetchEnd(seg))
          case segs =>
            val result = new Array[Byte](length)
            Future.sequence(segs.map: seg =>
              readSegment(seg, fetchEnd(seg)).map: bytes =>
                System.arraycopy(bytes, 0, result, seg.offsetInRequest, seg.length)
            ).map(_ => result)

  def write(offset: Long, data: Array[Byte], fua: Boolean): Future[Unit] =
    val geom = currentGeometry
    geom.validate(offset, data.length) match
      case Some(err) => Future.failed(err)
      case None => locked:
        stopped match
          case Some(err) => Future.failed(err)
          case None =>
            nextSeq += 1
            val seq = nextSeq
            val segs = geom.segments(offset, data.length)
            val waits = segs.map: seg =>
              val st = block(seg.blockIndex)
              // put copies the bytes, so a single segment can hand over the caller's array
              val bytes =
                if segs.size == 1 then data
                else Arrays.copyOfRange(data, seg.offsetInRequest, seg.offsetInRequest + seg.length)
              if st.inFlight.nonEmpty then
                counters = counters.copy(writesDuringCommit = counters.writesDuringCommit + 1)
              val before = st.dirty.bytes
              st.dirty.put(seg.offsetInBlock, bytes, seq)
              dirtyTotal += st.dirty.bytes - before
              val wait = if fua then st.awaitCommitted(seq) else Future.unit
              dirtied(seg.blockIndex, st, urgent = fua)
              wait
            val throttle =
              if dirtyTotal >= config.dirtyHardLimit then
                val p = Promise[Unit]()
                throttled += p
                // Writeback has to make room now, so no block waits out its coalescing window
                blocks.foreach((b, st) => if st.timer.nonEmpty then kick(b, st))
                p.future
              else
                Future.unit
            if fua then Future.sequence(waits).flatMap(_ => throttle) else throttle

  def flush(): Future[Unit] = locked:
    stopped match
      case Some(err) => Future.failed(err)
      case None =>
        // dirty keeps an in-flight commit's pieces (or newer ones over them), so its maxSeq covers that commit too
        val targets = blocks.toList.filter((_, st) => !st.dirty.isEmpty)
        val deletes = awaitDeletes(nextDeleteSeq)
        if targets.isEmpty && deletes.isEmpty then
          Future.unit
        else
          pendingFlushes += 1
          val waits = targets.map((_, st) => st.awaitCommitted(st.dirty.maxSeq)) ++ deletes
          targets.foreach((b, st) => kick(b, st))
          if pendingDeletes.nonEmpty then
            mapKickRequested = true
          val all = Future.sequence(waits).map(_ => ())
          all.onComplete(_ => synchronized { pendingFlushes -= 1 })
          all

  /** Completes at once. Each block the ranges cover entirely reads as zeros from now on, and its object is deleted
    * by a later map transaction, which FLUSH waits for. Parts of blocks are left alone
    */
  def unmap(ranges: List[(Long, Long)]): Future[Unit] =
    val geom = currentGeometry
    ranges.view.flatMap((offset, length) => geom.validate(offset, length)).headOption match
      case Some(err) => Future.failed(err)
      case None => locked:
        stopped match
          case Some(err) => Future.failed(err)
          case None =>
            ranges.foreach((offset, length) => geom.blocksFullyCovered(offset, length).foreach(unmapBlock))
            releaseThrottled()
            Future.unit

  /** Commits the new size to the disk's config, then serves it. Requests in progress are unaffected */
  def grow(newSize: Long): Future[Unit] =
    synchronized(stopped) match
      case Some(err) => Future.failed(err)
      case None =>
        DiskCatalog.grow(client, info.name, newSize).map: grown =>
          synchronized:
            if grown.geometry.size > currentGeometry.size then
              currentGeometry = grown.geometry

  def close(): Unit = locked(stop(Fenced))

  // ---- Monitor helpers ----

  /** Work collected under the monitor and run once it is released */
  private final class Deferred:
    private val actions = ListBuffer[() => Unit]()
    def add(f: => Unit): Unit = actions += (() => f)
    def run(): Unit = actions.foreach(_())

  private def locked[A](f: Deferred ?=> A): A =
    val d = Deferred()
    val r = synchronized:
      val r = f(using d)
      if mapKickRequested then
        mapKickRequested = false
        kickMap()(using d)
      r
    d.run()
    r

  private def later(f: => Unit)(using d: Deferred): Unit = d.add(f)

  /** Starts Aspen work after the monitor is released, on the client's context rather than the caller's thread, so
    * that a write's acknowledgment never waits on building a transaction
    */
  private def launch(f: => Unit)(using Deferred): Unit = later(summon[ExecutionContext].execute(() => f))

  private def block(index: Long): BlockState = blocks.getOrElseUpdate(index, BlockState())

  private def stop(err: VDiskError)(using Deferred): Unit =
    if stopped.isEmpty then
      stopped = Some(err)
      blocks.values.foreach: st =>
        st.timer.foreach(_.cancel())
        st.timer = None
        st.takeAllWaiters().foreach(p => later(p.failure(err)))
      blocks.clear()
      pendingReads.foreach(p => later(p.tryFailure(err)))
      pendingReads.clear()
      cleanCache.clear()
      awaitingMap.clear()
      awaitingData.clear()
      dirtyTotal = 0
      throttled.foreach(p => later(p.tryFailure(err)))
      throttled.clear()
      deleteWaiters.foreach((_, p) => later(p.tryFailure(err)))
      deleteWaiters.clear()
      pendingDeletes.clear()
      outstandingDeletes.clear()

  /** A failed writeback lookup stops the disk */
  private def fail(reason: String)(using Deferred): Unit = stop(DiskFailed(reason))

  // ---- Writeback scheduling (all under the monitor) ----

  /** A commit, lookup or queued map/data transaction will reschedule the block when it completes */
  private def busy(b: Long, st: BlockState): Boolean =
    st.inFlight.nonEmpty || st.lookupInFlight || awaitingMap.contains(b) || awaitingData.contains(b)

  private def urgent(st: BlockState): Boolean =
    config.coalesceWindow.length == 0 || pendingFlushes > 0 || st.hasWaiters || dirtyTotal > config.dirtySoftLimit ||
      dirtyTotal >= config.dirtyHardLimit

  /** The block has new dirty data. Its map lookup, if still needed, runs during the coalescing window */
  private def dirtied(b: Long, st: BlockState, urgent: Boolean)(using Deferred): Unit =
    if !st.lookedUp then
      startLookup(b, st)
    else if urgent || this.urgent(st) then
      kick(b, st)
    else
      armTimer(b, st)

  /** Looks the block up, then treats its dirty data as new. FUA and flush waiters keep it urgent meanwhile */
  private def startLookup(b: Long, st: BlockState)(using Deferred): Unit =
    if !st.lookupInFlight then
      st.lookupInFlight = true
      launch:
        Future.delegate(ensureLookedUp(b, st)).onComplete: r =>
          locked:
            st.lookupInFlight = false
            if stopped.isEmpty then
              r match
                case Success(_) => if !st.dirty.isEmpty then dirtied(b, st, urgent = false)
                case Failure(e) => fail(s"map lookup of block $b failed: $e")

  private def armTimer(b: Long, st: BlockState)(using Deferred): Unit =
    if st.timer.isEmpty && !busy(b, st) then
      st.timerSeq += 1
      val mine = st.timerSeq
      val task = client.backgroundTaskManager.schedule(config.coalesceWindow) {
        locked:
          if st.timerSeq == mine && st.timer.nonEmpty then
            st.timer = None
            kick(b, st)
      }
      st.timer = Some(task)

  /** Starts writing the block back now, unless something already in progress will */
  private def kick(b: Long, st: BlockState)(using Deferred): Unit =
    st.timer.foreach(_.cancel())
    st.timer = None
    if stopped.isEmpty && !busy(b, st) && !st.dirty.isEmpty then
      if !st.lookedUp then
        startLookup(b, st)
      else if st.pointer.isEmpty then
        awaitingMap += b
        mapKickRequested = true
      else if dataTxRunning < config.maxInFlightDataTx then
        startDataTx(b, st)
      else
        awaitingData += b

  private def ensureLookedUp(b: Long, st: BlockState): Future[Unit] =
    if synchronized(st.lookedUp) then
      Future.unit
    else
      blockMap.lookup(b).map: ptr =>
        synchronized:
          if !st.lookedUp then
            st.lookedUp = true
            st.pointer = ptr

  /** The commit of `snap` succeeded */
  private def committed(b: Long, st: BlockState, snap: SegmentSnapshot)(using Deferred): Unit =
    val before = st.dirty.bytes
    st.dirty.removeUpTo(snap.maxSeq)
    dirtyTotal -= before - st.dirty.bytes
    st.committedSeq = math.max(st.committedSeq, snap.maxSeq)
    st.generation += 1
    st.inFlight = None
    st.takeSatisfied().foreach(p => later(p.success(())))
    releaseThrottled()
    if !st.dirty.isEmpty then
      dirtied(b, st, urgent = false)

  private def releaseThrottled()(using Deferred): Unit =
    if throttled.nonEmpty && dirtyTotal < config.dirtyHardLimit then
      throttled.foreach(p => later(p.trySuccess(())))
      throttled.clear()

  // ---- UNMAP ----

  /** Drops the block's dirty data, which completes its waiters, and makes it read as zeros. Unless it is known to
    * have no object, the next map transaction that can deletes the one in the map, waiting for a commit in flight
    * on the block to end first. Under the monitor
    */
  private def unmapBlock(b: Long)(using Deferred): Unit =
    val st = block(b)
    st.unmaps += 1
    st.timer.foreach(_.cancel())
    st.timer = None
    dirtyTotal -= st.dirty.bytes
    st.dirty.removeUpTo(Long.MaxValue)
    // Its writes are superseded, so FLUSH and FUA waiting for them are done
    st.committedSeq = math.max(st.committedSeq, nextSeq)
    st.takeSatisfied().foreach(p => later(p.success(())))
    awaitingMap.remove(b)
    awaitingData.remove(b)
    // A commit in flight may yet create or change an object, which then has to be deleted too
    val mayBeMapped = !st.lookedUp || st.pointer.nonEmpty || st.inFlight.nonEmpty
    st.lookedUp = true
    st.pointer = None
    forgetRevision(b, st)
    // A fetch in progress caches nothing
    st.generation += 1
    if mayBeMapped && !pendingDeletes.contains(b) then
      queueDelete(b, refresh = false)
    if pendingDeletes.contains(b) then
      mapKickRequested = true

  private def queueDelete(b: Long, refresh: Boolean): Unit =
    nextDeleteSeq += 1
    pendingDeletes(b) = PendingDelete(nextDeleteSeq, refresh)
    outstandingDeletes += nextDeleteSeq

  /** Completes once every deletion up to `seq` is done, or None if they all are */
  private def awaitDeletes(seq: Long): Option[Future[Unit]] =
    if outstandingDeletes.headOption.exists(_ <= seq) then
      val p = Promise[Unit]()
      deleteWaiters += ((seq, p))
      Some(p.future)
    else
      None

  private def deleteDone(pd: PendingDelete)(using Deferred): Unit =
    outstandingDeletes -= pd.seq
    val first = outstandingDeletes.headOption
    val (done, waiting) = deleteWaiters.partition((seq, _) => first.forall(_ > seq))
    done.foreach((_, p) => later(p.trySuccess(())))
    deleteWaiters.clear()
    deleteWaiters ++= waiting

  /** A map entry superseded by an unmap leaves its batch. An attempt may have created an object for it, so its
    * deletion goes back to the pending ones, reading the map entry again. Under the monitor
    */
  private def requeueDelete(e: MapEntry): Unit =
    pendingDeletes.get(e.index).toList ++ e.delete match
      case Nil => queueDelete(e.index, refresh = true)
      case ds =>
        val seq = ds.map(_.seq).min
        ds.foreach(d => if d.seq != seq then outstandingDeletes -= d.seq)
        pendingDeletes(e.index) = PendingDelete(seq, refresh = true)

  /** Succeeds while this owner still holds the fence. Once another owner has taken it, the disk stops and this fails
    * with StopRetrying(Fenced). A failed read of the fence fails with that read's error
    */
  private def fenceHeld(): Future[Unit] =
    client.read(info.fencePointer).flatMap: fence =>
      if fence.revision != fenceRevision then
        locked(stop(Fenced))
        Future.failed(StopRetrying(Fenced))
      else
        Future.unit

  /** Forgets a revision learned from a read, and what was cached at it, so the block learns it again. Under the
    * monitor
    */
  private def forgetRevision(b: Long, st: BlockState): Unit =
    st.revision = None
    st.length = None
    st.revisionLearned = false
    cleanCache.invalidate(b)

  // ---- Data transactions ----

  private def startDataTx(b: Long, st: BlockState)(using Deferred): Unit =
    // Marks the block busy until the commit loop is done. Each attempt takes its own snapshot
    st.inFlight = Some(st.dirty.snapshot)
    dataTxRunning += 1
    counters = counters.copy(maxConcurrentDataTx = math.max(counters.maxConcurrentDataTx, dataTxRunning))
    launch(committer.commitBlock(b, st, st.unmaps))

  /** What an attempt commits and the block state it is built on, unless the block has been unmapped since `epoch`.
    * That ends the commit loop. Under the monitor
    */
  private def dataInput(b: Long, st: BlockState, epoch: Long)(using Deferred): Option[DataInput] =
    if st.unmaps != epoch then
      dataSuperseded(b, st)
      None
    else
      val snap = st.dirty.snapshot
      st.inFlight = Some(snap)
      Some(DataInput(st.pointer.get, st.revision, st.length, cleanPieces(b), snap,
        learned = st.revision.isEmpty || st.revisionLearned, epoch))

  /** The block's commit of `input` succeeded, which ends its commit loop */
  private def dataCommitted(b: Long, st: BlockState, input: DataInput, c: DataCommit)(using Deferred): Unit =
    dataTxRunning -= 1
    if stopped.isEmpty then
      counters =
        if c.overwrote then counters.copy(overwriteTxCount = counters.overwriteTxCount + 1)
        else counters.copy(rangedTxCount = counters.rangedTxCount + 1)
      if st.unmaps == input.epoch then
        // Clean bytes are only known to be at the revision the commit replaced when that is the revision they were
        // held at
        if !st.revision.contains(c.base) then
          cleanCache.invalidate(b)
        st.revision = Some(c.revision)
        st.length = Some(c.length)
        st.revisionLearned = false
        input.snap.pieces.foreach((o, bytes) => cleanCache.merge(b, o, bytes))
      else
        // Unmapped during the commit: the object's deletion was waiting for it
        mapKickRequested = true
      committed(b, st, input.snap)
    startQueued()

  /** The block's commit loop ended without committing, since the block was unmapped */
  private def dataSuperseded(b: Long, st: BlockState)(using Deferred): Unit =
    dataTxRunning -= 1
    st.inFlight = None
    st.generation += 1
    if stopped.isEmpty then
      mapKickRequested = true
      if !st.dirty.isEmpty then
        dirtied(b, st, urgent = false)
      startQueued()

  private def startQueued()(using Deferred): Unit =
    while stopped.isEmpty && dataTxRunning < config.maxInFlightDataTx && awaitingData.nonEmpty do
      val b = awaitingData.head
      awaitingData.remove(b)
      blocks.get(b).foreach(st => kick(b, st))

  // ---- Map transactions ----

  /** Starts a map transaction for the waiting blocks, in index order, and for the pending deletions of blocks with
    * no commit in flight, unless one is already running. A waiting block with a pending deletion replaces its
    * object
    */
  private def kickMap()(using Deferred): Unit =
    if stopped.isEmpty && !mapTxRunning && (awaitingMap.nonEmpty || pendingDeletes.nonEmpty) then
      val batch = ListBuffer[(Long, BlockState, SegmentSnapshot)]()
      var bytes = 0L
      val it = awaitingMap.iterator
      var full = false
      while it.hasNext && !full do
        val b = it.next()
        val st = blocks(b)
        val snap = st.dirty.snapshot
        if batch.nonEmpty && bytes + snap.end > config.maxMapTxBytes then
          full = true
        else
          batch += ((b, st, snap))
          bytes += snap.end
      val creates = batch.toList.map: (b, st, snap) =>
        awaitingMap.remove(b)
        st.inFlight = Some(snap)
        MapEntry(b, st, st.unmaps, create = true, pendingDeletes.remove(b))
      // A block waiting for a map transaction it did not fit in replaces its object in a later one
      val deletes = pendingDeletes.iterator
        .filter((b, _) => blocks(b).inFlight.isEmpty && !awaitingMap.contains(b))
        .take(MaxMapTxDeletes)
        .toList
        .map: (b, pd) =>
          pendingDeletes.remove(b)
          MapEntry(b, blocks(b), blocks(b).unmaps, create = false, Some(pd))
      if creates.nonEmpty || deletes.nonEmpty then
        mapTxRunning = true
        launch(committer.commitMap(creates ++ deletes))

  /** The map transaction has nothing left to do. Under the monitor */
  private def mapDone(): Unit =
    mapTxRunning = false
    mapKickRequested = true

  private def mapCommitted(result: MapResult)(using Deferred): Unit =
    if stopped.isEmpty then
      counters = counters.copy(mapTxCount = counters.mapTxCount + 1,
        deletedObjects = counters.deletedObjects + result.deletedObjects)
      blockMap.recordCommitted(result.created.map((c, ptr) => (c.entry.index, ptr)), result.deleted.map(_.index))
      result.created.foreach: (c, ptr) =>
        val b = c.entry.index
        val st = c.entry.st
        // A block unmapped meanwhile keeps reading as zeros, and the deletion that queued waits for this object
        if st.unmaps == c.entry.epoch then
          st.pointer = Some(ptr)
          st.revision = Some(result.revision)
          st.length = Some(c.snap.end)
          st.revisionLearned = false
          cleanCache.merge(b, 0, c.content)
        c.entry.delete.foreach(deleteDone)
        committed(b, st, c.snap)
      result.deleted.foreach(_.delete.foreach(deleteDone))

  /** A failed map attempt did create or replace the block's object, so the block adopts it and its dirty data goes
    * through a data transaction. One unmapped meanwhile instead waits for the object's deletion
    */
  private def adopted(e: MapEntry, ptr: DataObjectPointer, s: DataObjectRangeState)(using Deferred): Unit =
    if stopped.isEmpty then
      val st = e.st
      e.delete.foreach(deleteDone)
      st.inFlight = None
      if st.unmaps == e.epoch then
        counters = counters.copy(adoptedBlocks = counters.adoptedBlocks + 1)
        st.pointer = Some(ptr)
        st.revision = Some(s.revision)
        st.length = s.objectLength
        st.revisionLearned = true
        cleanCache.invalidate(e.index)
        kick(e.index, st)
      else
        mapKickRequested = true
        if !st.dirty.isEmpty then
          dirtied(e.index, st, urgent = false)

  private object host extends BlockCommitter.Host:
    def checkServing(): Unit = synchronized(stopped).foreach(err => throw StopRetrying(err))
    def fenceHeld(): Future[Unit] = VirtualDisk.this.fenceHeld()
    def commitGate(): Future[Unit] = VirtualDisk.this.commitGate()
    def takeInjectedFailure(): Boolean = VirtualDisk.this.synchronized:
      val f = failNextCommitAfterSuccess
      failNextCommitAfterSuccess = false
      f
    def blockObjectSize: Int = currentGeometry.blockObjectSize
    def preImageRead(): Unit = VirtualDisk.this.synchronized:
      counters = counters.copy(preImageReads = counters.preImageReads + 1)
    def superseded(st: BlockState, epoch: Long): Boolean = VirtualDisk.this.synchronized(st.unmaps != epoch)
    def dataInput(b: Long, st: BlockState, epoch: Long): Option[DataInput] =
      locked(VirtualDisk.this.dataInput(b, st, epoch))
    def dataCommitted(b: Long, st: BlockState, input: DataInput, c: DataCommit, recovered: Boolean): Unit =
      locked:
        if recovered then
          counters = counters.copy(recoveredCommits = counters.recoveredCommits + 1)
        VirtualDisk.this.dataCommitted(b, st, input, c)
    def forgetRevision(b: Long, st: BlockState): Unit = VirtualDisk.this.synchronized:
      VirtualDisk.this.forgetRevision(b, st)
    def dataFailed(err: VDiskError): Unit = locked:
      dataTxRunning -= 1
      stop(err)
    def dropSuperseded(batch: List[MapEntry]): List[MapEntry] = locked:
      val (superseded, kept) = batch.partition(e => e.create && e.st.unmaps != e.epoch)
      superseded.foreach: e =>
        e.st.inFlight = None
        if stopped.isEmpty then
          requeueDelete(e)
          if !e.st.dirty.isEmpty then
            dirtied(e.index, e.st, urgent = false)
      kept
    def mapInputs(creates: List[MapEntry]): List[SegmentSnapshot] = VirtualDisk.this.synchronized:
      creates.map: e =>
        val snap = e.st.dirty.snapshot
        e.st.inFlight = Some(snap)
        snap
    def mapCommitted(result: MapResult): Unit = locked(VirtualDisk.this.mapCommitted(result))
    def adopted(e: MapEntry, ptr: DataObjectPointer, s: DataObjectRangeState): Unit =
      locked(VirtualDisk.this.adopted(e, ptr, s))
    def mapEntryDone(e: MapEntry): Unit = locked:
      if stopped.isEmpty then
        blockMap.recordCommitted(Nil, List(e.index))
        e.delete.foreach(deleteDone)
    def mapDone(): Unit = locked(VirtualDisk.this.mapDone())
    def mapFailed(err: VDiskError): Unit = locked:
      mapTxRunning = false
      stop(err)

  private val committer = BlockCommitter(client, info, fenceRevision, blockMap, allocator, host)

  // ---- Reads ----

  /** The block's clean pieces. Under the monitor, they are at `revision` */
  private def cleanPieces(b: Long): Vector[(Int, Array[Byte])] =
    cleanCache.get(b).map(_.pieces).getOrElse(Vector.empty)

  /** Bytes of one segment: the dirty overlay over committed content, which comes from the clean cache or a ranged
    * read. The overlay is taken before any I/O, so every write completed before the read is reflected. A fetch also
    * reads `[segment end, fetchEnd)` ahead into the clean cache
    */
  private def readSegment(seg: BlockSegment, fetchEnd: Int): Future[Array[Byte]] =
    val from = seg.offsetInBlock
    val to = from + seg.length
    val b = seg.blockIndex
    val start: Either[Future[Array[Byte]], (BlockState, SegmentSnapshot, Promise[Array[Byte]])] = synchronized:
      stopped match
        case Some(err) => Left(Future.failed(err))
        case None =>
          val st = block(b)
          val overlay = st.dirty.snapshot
          if covers(overlay.pieces, from, to) then
            Left(Future.successful(assemble(seg, Nil, overlay)))
          else
            val p = Promise[Array[Byte]]()
            pendingReads += p
            Right((st, overlay, p))
    start match
      case Left(result) => result
      case Right((st, overlay, p)) =>
        Future.delegate(readGate().flatMap(_ => ensureLookedUp(b, st)).flatMap(_ =>
          fetchSegment(seg, st, overlay, fetchEnd))).onComplete: r =>
          synchronized(pendingReads -= p)
          r match
            case Success(bytes) => p.trySuccess(bytes)
            case Failure(e: VDiskError) => p.tryFailure(e)
            case Failure(e) => p.tryFailure(DiskFailed(s"read of block $b failed: $e"))
        p.future

  /** Fetches what the segment needs. A fetch at another revision than the known one is retried with the client's
    * backoff, unless the fence shows that another owner has taken the disk: the read then fails with `Fenced` and
    * the disk stops
    */
  private def fetchSegment(seg: BlockSegment,
                           st: BlockState,
                           overlay: SegmentSnapshot,
                           fetchEnd: Int): Future[Array[Byte]] =
    client.retryStrategy.retryUntilSuccessful:
      fetchOnce(seg, st, overlay, fetchEnd).recoverWith:
        case StaleFetch =>
          fenceHeld().transform:
            case Success(_) => Failure(StaleFetch)
            case Failure(e: StopRetrying) => Failure(e)
            case Failure(e) => Failure(StopRetrying(e))
        case e: StopRetrying => Future.failed(e)
        case e => Future.failed(StopRetrying(e))

  private def fetchOnce(seg: BlockSegment,
                        st: BlockState,
                        overlay: SegmentSnapshot,
                        fetchEnd: Int): Future[Array[Byte]] =
    val b = seg.blockIndex
    val from = seg.offsetInBlock
    val to = from + seg.length
    val (pointer, revision, length, clean, generation) =
      synchronized((st.pointer, st.revision, st.length, cleanPieces(b), st.generation))
    pointer match
      case None => Future.successful(assemble(seg, Nil, overlay))
      case Some(ptr) =>
        val held = clean ++ overlay.pieces
        // Bytes beyond the object's length are zeros. When the length is unknown a read past it comes back short
        def needed(end: Int): List[(Int, Int)] = gaps(from, end, held).flatMap: (a, e) =>
          length match
            case Some(len) => if a < len then Some((a, math.min(e, len))) else None
            case None => Some((a, e))
        if needed(to).isEmpty then
          Future.successful(assemble(seg, clean, overlay))
        else
          // Readahead joins a fetch the request needs anyway, so a read served from the caches costs no I/O
          val ranges = needed(fetchEnd)
          synchronized:
            fetches += 1
          rangeReader(ptr, ranges.map((a, e) => (a, e - a))).flatMap: states =>
            val fetchRevision = states.head.revision
            val fetched = states.map(s => (s.offset, s.data.getByteArray)).filter(_._2.nonEmpty)
            val outcome: Either[VDiskError, Boolean] = synchronized:
              stopped match
                case Some(err) => Left(err)
                case None =>
                  val current = st.revision
                  // The owner is the only writer, so a known revision is authoritative. Bytes from any other
                  // revision (a store still serving an older state) are neither cached nor returned. An unknown
                  // revision is learned only while no commit is in flight, since that commit's pre-image may be at
                  // another revision
                  val cacheable = current match
                    case Some(r) => r == fetchRevision
                    case None => st.inFlight.isEmpty
                  if st.generation == generation && cacheable then
                    if current.isEmpty then
                      st.revision = Some(fetchRevision)
                      st.revisionLearned = true
                    if st.length.isEmpty then
                      st.length = states.flatMap(_.objectLength).headOption
                    fetched.foreach((o, bytes) => cleanCache.merge(b, o, bytes))
                  else if current.exists(_ != fetchRevision) && st.revisionLearned && st.inFlight.isEmpty then
                    // The learned revision may have come from a lagging store. Only this owner's commits are
                    // authoritative, so forget it and what was cached at it, and learn the block again
                    forgetRevision(b, st)
                  // A commit completed during the fetch is covered by the overlay or newer than the read
                  Right(revision.exists(_ != fetchRevision) && !current.contains(fetchRevision))
            outcome match
              case Left(err) => Future.failed(StopRetrying(err))
              case Right(true) => Future.failed(StaleFetch)
              case Right(false) => Future.successful(assemble(seg, clean ++ fetched, overlay))

/** Whether a disk is served. Once it is not, it never is again: a new owner has to acquire the disk */
enum DiskState:
  case Serving
  /** Another owner has taken the disk, or it was closed */
  case Fenced
  /** A commit could not be resolved, or an invariant broke */
  case Failed(reason: String)

object VirtualDisk:

  /** Test introspection */
  private[vdisk] final case class Stats(mapTxCount: Long,
                                        rangedTxCount: Long,
                                        overwriteTxCount: Long,
                                        maxConcurrentDataTx: Int,
                                        writesDuringCommit: Long,
                                        preImageReads: Long,
                                        recoveredCommits: Long,
                                        adoptedBlocks: Long,
                                        deletedObjects: Long)

  private[vdisk] type RangeReader = (DataObjectPointer, List[(Int, Int)]) => Future[List[DataObjectRangeState]]

  /** Most deletions a single map transaction makes */
  private val MaxMapTxDeletes = 256

  /** A fetch returned another revision than the block's known one */
  private object StaleFetch extends Exception("fetch returned a revision other than the known one"):
    override def fillInStackTrace(): Throwable = this

  /** Loads the disk and takes ownership of it, fencing any previous owner */
  def acquire(client: AspenClient, name: String, config: VDiskConfig = VDiskConfig()): Future[VirtualDisk] =
    given ExecutionContext = client.clientContext

    for
      info <- DiskCatalog.load(client, name)
      (_, fenceRevision) <- DiskCatalog.takeOwnership(client, info, UUID.randomUUID())
      tree <- KVObjectRootManager(client, DiskConfig.MapRootKey, info.configPointer).getDataObjectTree()
      allocator <- client.getAllocator(info.allocatorId)
    yield
      new VirtualDisk(client, info, fenceRevision, BlockMap(tree), allocator, config)

  /** Sub-ranges of [from, to) that no piece covers */
  private def gaps(from: Int, to: Int, pieces: Seq[(Int, Array[Byte])]): List[(Int, Int)] =
    val result = ListBuffer[(Int, Int)]()
    var pos = from
    pieces.map((o, b) => (o, o + b.length)).sortBy(_._1).foreach: (s, e) =>
      if e > pos && s < to then
        if s > pos then
          result += ((pos, s))
        pos = math.max(pos, e)
    if pos < to then
      result += ((pos, to))
    result.toList

  private[vdisk] def covers(pieces: Seq[(Int, Array[Byte])], from: Int, to: Int): Boolean = gaps(from, to, pieces).isEmpty

  /** The segment's bytes: zeros, then `base` (non-overlapping committed content), then the overlay on top */
  private def assemble(seg: BlockSegment, base: Seq[(Int, Array[Byte])], overlay: SegmentSnapshot): Array[Byte] =
    val from = seg.offsetInBlock
    val buf = new Array[Byte](seg.length)
    base.foreach: (o, bytes) =>
      val s = math.max(o, from)
      val e = math.min(o + bytes.length, from + seg.length)
      if s < e then
        System.arraycopy(bytes, s - o, buf, s - from, e - s)
    overlay.overlay(buf, from)
    buf
