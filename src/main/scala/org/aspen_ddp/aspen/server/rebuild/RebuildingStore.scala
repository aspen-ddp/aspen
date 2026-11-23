package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, KeyValueObjectState, MetadataObjectState, ObjectState, ReadTimeout}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.tkvl.KeyValueListNode
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceState}
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateMarker.MissedUpdate
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, Metadata, ObjectId, ObjectPointer, ObjectType}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.network.PendingAllocation
import org.aspen_ddp.aspen.common.transaction.{TransactionHorizon, TransactionId}
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}
import org.aspen_ddp.aspen.server.host.{CopyLayout, Phase}
import org.aspen_ddp.aspen.server.store.{Frontend, StoreIncarnationFile}
import org.aspen_ddp.aspen.server.host.DeviceIo
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendOpener, BackendStateLost, StoreCreation}
import scribe.Logging

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.util.control.NonFatal

object RebuildingStore:
  /** The ErrorTree mark for `objectId`'s slice of `ptr` on a store that may lack it. Missed at HLCTimestamp.Zero: the
   *  rebuild does not know the transaction, and the mark never displaces a newer one. Repair settles it */
  private[rebuild] def missMark(storeId: StoreId, objectId: ObjectId, ptr: ObjectPointer): MissedUpdate =
    MissedUpdate(storeId, objectId, Some(ptr.objectType), ptr.storePointer, HLCTimestamp.Zero)

  /** Staging directory under the device root. Invisible to Host.tryLoadStore for the
   *  same reason transferring-in/ is: there is no StoreConfig at this level, only one level
   *  down. That is what keeps a half-rebuilt store from being loaded and answering reads. */
  val RebuildDirectory = "rebuilding"

  /** Device-root directory a retired copy is moved into from lost/ (T11, StoreCopies.retire step 4), whole and under
   *  its copy name, with its former device-CRL entries in LostCrlStateFile. It is kept until the store is Active again
   *  on its owner device (StoreCopies.release): a rebuild that cannot finish destroys nothing. Never loaded: the
   *  loader is offered only live/ copies. */
  val PendingDeleteDirectory = "pending-delete"

  /** Writes the rebuilt store's whole transaction state, which is empty, as the crl_save.log of the store directory
   *  at `storePath` (finish, in staging before its episode's phase-param.rebuilt). A rebuilt store has never served, so
   *  whatever the device CRL holds under its id was left by an earlier copy of the store on this device whose
   *  directory left its path without a retire (removed by hand, or a fail-stopped copy's close saving behind the
   *  retire's drop). Its first load, wherever it comes from (the flip, the Completed Rebuilds pass, a restart, a
   *  reinstated transfer out, an adoption), is a save-file load, which drops those entries durably before loading the
   *  file (StoreLoader.awaitCrlLoad); a transfer out of the unloaded copy ships the file and drops them likewise
   *  (StoreTransfersOut.extractUnloaded). Without it the copy would recover their votes, locks and applied stubs as its
   *  own under its new incarnation, and a stale commit vote could apply again. */
  def writeEmptyCrlState(storePath: os.Path, storeId: StoreId): Unit =
    CrashRecoveryLog.saveStoreState(storeId, StoreRecoveryState.Empty, Backend.crlSaveFile(storePath))

  /** The parameter a Rebuilt copy of `episode` carries (phase-param.rebuilt): the episode, "" for None */
  def rebuildCompleteContent(episode: Option[UUID]): String = episode.map(_.toString).getOrElse("")

  /** Whether a copy in `phase` is the finished rebuild of `episode`: Rebuilt (in rebuilt/), with that episode */
  def isRebuiltFor(phase: Phase, episode: Option[UUID]): Boolean = phase == Phase.Rebuilt(rebuildCompleteContent(episode))

  /** In a store directory being discarded: the store's device-CRL entries, saved by CrashRecoveryLog.saveStoreState
   *  before they are dropped from the device CRL, which a rebuild onto the same device would otherwise recover */
  val LostCrlStateFile = "lost-crl-state.log"

  /** In a store directory being discarded whose crl_save.log became its LostCrlStateFile: the entries the device CRL
   *  also held for the store, saved before they are dropped. A crash between a transfer out's save file and the
   *  deletion of its entries leaves both, and the save file may be the one found unusable (systemic-loss D3) */
  val LostCrlEntriesFile = "lost-crl-entries.log"

  /** In a rebuilt store's directory: the HLC timestamp from which its Paxos acceptor state is complete
   *  (Frontend.acceptorSince). Written by finish before the store moves into place, and kept for good (a network
   *  transfer carries it with the directory). The old incarnation stopped before the rebuild began, and joined only
   *  transactions started at most Frontend.MaxStartLead ahead of its host's clock, so every transaction it took part
   *  in started earlier, whatever the clients' clocks, unless the two hosts' clocks differ by more than
   *  AcceptorSinceMargin less MaxStartLead */
  val AcceptorSinceFile = "acceptor-since"

  /** Added to the finish time. A client stamps its transactions' start with its own clock, which a store accepts up to
   *  Frontend.MaxStartLead ahead of its host's: the margin covers that lead plus clock skew between the old
   *  incarnation's host and the rebuilding one */
  val AcceptorSinceMargin: FiniteDuration = FiniteDuration(10, SECONDS)

  private def acceptorSinceAfter(now: HLCTimestamp): HLCTimestamp =
    HLCTimestamp(now.asLong + (AcceptorSinceMargin.toMillis << 16))

  def writeAcceptorSince(storePath: os.Path): Unit = writeAcceptorSince(storePath, acceptorSinceAfter(HLCTimestamp.now))

  /** Writes `at` as the store's acceptor start (finish passes now plus AcceptorSinceMargin) */
  def writeAcceptorSince(storePath: os.Path, at: HLCTimestamp): Unit =
    DurableFiles.write(storePath / AcceptorSinceFile, at.asLong.toString)

  /** The store's acceptor start, if it was rebuilt. A file that cannot be read counts as now plus the margin: later
   *  than the truth, which only mutes more transactions. A stat that fails throws (FileStat): None only for a file known
   *  absent, never for one that may be there (StoreLoader.handleLoad holds the load back) */
  def readAcceptorSince(storePath: os.Path): Option[HLCTimestamp] =
    val file = storePath / AcceptorSinceFile
    if !FileStat.exists(file) then
      None
    else
      Some(scala.util.Try(HLCTimestamp(os.read(file).trim.toLong)).getOrElse(acceptorSinceAfter(HLCTimestamp.now)))

  /** In a rebuilt store's directory until its inherited votes are released: the wall-clock time, in milliseconds,
   *  from which its peers' held votes are complete (Frontend.inheritedVotesAfter, inheritedVoteLock). Written by
   *  finish beside AcceptorSinceFile and carried by transfers the same way; removed by the Frontend once no inherited
   *  vote is held. Until a query after that time completes, the store votes abort on every object */
  val InheritedVotesFile = "inherited-votes"

  /** From when a rebuild pass that started at `passStartMillis` (its host's wall clock) sees every vote its store's
   *  lost incarnation's transactions can still get. The old incarnation voted only before the pass began, in
   *  transactions started at most MaxStartLead after its host's clock; no store joins a transaction past its start
   *  plus `horizon` by its own clock. The query is sent, and the peers answer, on clocks that may each differ from the
   *  old and rebuilding hosts' by the inter-host skew: one AcceptorSinceMargin covers the lead and one skew, a second
   *  the other skew */
  def inheritedVotesAfter(passStartMillis: Long, horizon: FiniteDuration): Long =
    passStartMillis + horizon.toMillis + 2 * AcceptorSinceMargin.toMillis

  def writeInheritedVotes(storePath: os.Path, afterMillis: Long): Unit =
    DurableFiles.write(storePath / InheritedVotesFile, afterMillis.toString)

  /** The store's inherited-votes gate, if it holds inherited votes. A file that cannot be read counts as now plus the
   *  horizon and both margins: later than the truth, as the store loads after its rebuild's pass began */
  def readInheritedVotes(storePath: os.Path, horizon: FiniteDuration): Option[Long] =
    val file = storePath / InheritedVotesFile
    // A stat that fails throws (FileStat), as readAcceptorSince's does
    if !FileStat.exists(file) then
      None
    else
      Some(scala.util.Try(os.read(file).trim.toLong)
        .getOrElse(inheritedVotesAfter(System.currentTimeMillis(), horizon)))

  /** No inherited vote is held any more. Not forced: a reload that still finds the file queries again */
  def removeInheritedVotes(storePath: os.Path): Unit =
    try os.remove(storePath / InheritedVotesFile)
    catch case NonFatal(_) => ()

  /** How long the sweep waits on listed allocations before it records what is still unsettled for repair
   *  (sweepPending). Every listed transaction started before the query, at most MaxStartLead ahead of its stores'
   *  clocks, so by then its stores' own expiry resolution has had its chance. Liveness only: what is recorded is
   *  repaired whatever its transaction's outcome */
  val PendingWaitLimit: FiniteDuration =
    TransactionHorizon.Horizon + Frontend.DefaultExpiryMargin + TransactionHorizon.MaxStartLead

  /** Objects between checkpoints. Small enough that a crash costs little, large enough that
   *  the flush is not the dominant cost. */
  val CheckpointInterval: Int = 1000

  /** Length of an allocation-tree key that names an object: the raw bytes of an ObjectId. */
  val ObjectIdKeyLength: Int = 16

  /** How long the rebuild waits on one object read. A read that has not completed by then fails with ReadTimeout and
   *  sends nothing more, and its object is a failed one, as for a read the peers answer with errors: retried at the
   *  end of the pass (or, in the sweep, at the next round). Long enough for any healthy read, so only an object that
   *  cannot be restored now (its pool below the read threshold, an allocation parked past expiry) times out */
  val ObjectReadTimeout: FiniteDuration = FiniteDuration(30, SECONDS)

  /** How long the peer query (queryPeers) waits for `width - writeThreshold + 1` answers before the pass fails into
   *  the host's backoff retry. A pool short of that many live peers cannot be rebuilt now (its objects are below the
   *  read threshold too), and the pass must not hold a host rebuild slot while it waits. The query notices it
   *  at its next resend, at most 10 s later */
  val PeerQueryTimeout: FiniteDuration = FiniteDuration(60, SECONDS)

  /** Read timeouts in a row, with no object restored between them, after which the walk aborts: every read timing out
   *  means the pool is below its read threshold, and a sequential walk at ObjectReadTimeout per object would hold the
   *  host's rebuild slot for days before MaxFailedObjects. The checkpoint is kept and the pass retried with backoff */
  val MaxConsecutiveReadTimeouts: Int = 100

  /** The failure of a pass whose peer query got too few answers within its timeout (PeerQueryTimeout) */
  final class PeerQueryTimedOut(storeId: StoreId, timeout: FiniteDuration)
    extends Exception(s"Rebuild of $storeId: too few of its pool's peers answered the pending-allocations query " +
                      s"within $timeout")

  /** The failure of a rebuild stopped by `cancel()`. */
  case class RebuildCancelled(storeId: StoreId) extends Exception(s"Rebuild of $storeId was cancelled")


/** Reconstructs one store by walking its pool's allocation tree, reading each object through
 *  the client, and writing this store's slice of it into a fresh backend.
 *
 *  The store is not Active while this runs, so it participates in no transactions and receives
 *  no writes. An object written after the walk copied it is stale here, as on a store that missed
 *  writes while offline: the silent store is marked in the ErrorTree by the writer's finalizer. A
 *  copy read while a reply reported a write lock may predate a commit the lost store applied, which
 *  no finalizer marks, so `restore` marks it. Repair scans reconcile both once the store is loaded.
 */
class RebuildingStore(client: AspenClient,
                      val storeId: StoreId,
                      storageDeviceId: StorageDeviceId,
                      devicePath: os.Path,
                      backends: BackendOpener,
                      /** The destination device's I/O: the staging store opens and runs on its lanes */
                      io: DeviceIo,
                      /** The copy of the store on the device outside pending-delete/, if any, with its phase and path,
                        * from the copy registry (StoreRebuilds.startRebuild): start takes a Rebuilt copy of its own
                        * episode for done and fails on any other */
                      existingCopy: Option[(Phase, os.Path)],
                      /** The Rebuilding episode this rebuild runs for (StoreEntry.rebuildEpisode) */
                      episode: Option[UUID] = None,
                      checkpointInterval: Int = RebuildingStore.CheckpointInterval,
                      testingOnlyFailKeys: Set[Key] = Set(),
                      testingOnlyOutOfSpaceKeys: Set[Key] = Set(),
                      testingOnlyTrackRestoredKeys: Boolean = false,
                      maxFailedObjects: Int = RebuildState.MaxFailedObjects,
                      /** Read timeouts in a row after which the walk aborts (MaxConsecutiveReadTimeouts) */
                      maxConsecutiveReadTimeouts: Int = RebuildingStore.MaxConsecutiveReadTimeouts,
                      testingOnlyOnRestored: () => Unit = () => (),
                      testingOnlyOnFinishClosed: () => Unit = () => (),
                      testingOnlyAfterWalk: () => Future[Unit] = () => Future.unit,
                      testingOnlyAfterQuery: () => Future[Unit] = () => Future.unit,
                      /** How long after its start a transaction may be joined (inheritedVotesAfter). The default
                        * horizon is the longest any host uses */
                      horizon: FiniteDuration = TransactionHorizon.Horizon,
                      /** How long the sweep waits before recording what is unsettled (PendingWaitLimit) */
                      pendingWaitLimit: FiniteDuration = RebuildingStore.PendingWaitLimit,
                      /** How long one object read may take before the object counts as failed (ObjectReadTimeout) */
                      objectReadTimeout: FiniteDuration = RebuildingStore.ObjectReadTimeout,
                      /** How long the peer query waits for its quorum before the pass fails (PeerQueryTimeout) */
                      peerQueryTimeout: FiniteDuration = RebuildingStore.PeerQueryTimeout,
                      /** How finish's rename into rebuilt/ forces its directories. Replaced only by tests */
                      syncDirectory: os.Path => Unit = DurableFiles.syncDirectory)
  extends StoreRebuild with Logging:

  import RebuildingStore.*

  private given ExecutionContext = client.clientContext

  /** Every object read of the walk, the sweep and the retries: a read that never completes would hold the walk, the
   *  host's rebuild slot and a cancel (ObjectReadTimeout) */
  private val reader = client.withReadTimeout(objectReadTimeout)

  private val stagingPath = devicePath / RebuildDirectory / storeId.directoryName

  // The copy id of the Rebuilt copy, once finish has placed it (or start found it placed): `complete` reports it, and
  // the completion's handler records the copy with it
  private val completionPromise: Promise[UUID] = Promise()
  def complete: Future[UUID] = completionPromise.future

  // Set by completeStaging, before the completion, for a rename into rebuilt/ that landed unforced
  @volatile private var unforcedPlacement: Option[DurableFiles.MoveNotForced] = None
  override def placedUnforced: Option[DurableFiles.MoveNotForced] = unforcedPlacement

  /** Keys handed to rebuildWrite, in walk order. Test hook, and off unless a test asks for it:
   *  a real store holds millions of objects, and accumulating a Key per object for the life of
   *  the rebuild would cost that memory for no production purpose. */
  private val restoredKeys = mutable.ListBuffer[Key]()
  private[rebuild] def testingOnlyRestoredKeys: List[Key] = synchronized(restoredKeys.toList)

  /** "flush" / "checkpoint" in the order they happened. Test hook: the ordering between them
   *  is the one thing that cannot be observed from disk after a successful pass. */
  private val checkpointTrace = mutable.ListBuffer[String]()
  private[rebuild] def testingOnlyCheckpointTrace: List[String] =
    synchronized(checkpointTrace.toList)

  private var backend: Backend = null
  private var restoredSinceCheckpoint = 0
  private var lastKey: Option[Key] = None
  private var failed: List[ObjectId] = Nil
  /** The walk's read timeouts since it last restored an object (MaxConsecutiveReadTimeouts) */
  private var consecutiveReadTimeouts = 0
  /** Every allocation the peers listed at the query of this pass or an earlier one, kept across resumes by the
   *  checkpoint (rebuild-process 5.3). Set by runPass before the walk */
  private var listedAllocations: Map[TransactionId, PendingAllocation] = Map()

  /** Abort latch: when a fatal condition fires (out of space or too many failures), we record
   *  the cause here. The TKVL walks swallow a failing fn by design -- they log and
   *  continues -- so a fatal condition has to be carried out of band. Once set, the walk stops
   *  (its stopWhen is `aborted`), and restoreObject returns Future.unit for anything already
   *  handed to it. The latched cause is checked after the walk returns, and if set, the pass
   *  fails before retryFailures() and finish() run, leaving the checkpoint in staging so the
   *  next device check resumes. */
  private var abortCause: Option[Throwable] = None

  private val aborted: () => Boolean = () => synchronized(abortCause).isDefined

  /** Sets the abort cause unless one is already latched: the first fatal condition owns the abort */
  private def latchAbort(cause: Throwable): Unit =
    synchronized:
      if abortCause.isEmpty then
        abortCause = Some(cause)

  /** Latches `RebuildCancelled` as the abort cause, so the walk stops and the pass fails with the
   *  checkpoint left in staging, exactly as for any other fatal condition.
   *
   *  The walk checks the latch before every entry and before reading each node (`aborted`), so it
   *  stops reading once the object in flight, if any, has been read; the backend is then closed
   *  and `complete` fails. Prompt matters: a device shutdown waits for its cancelled rebuilds
   *  before closing the device's stores, and a walk that ran on through the rest of the
   *  allocation tree would hold the staging RocksDB open on a disk about to be pulled. The
   *  backend is never closed under a write still in progress: the walk hands out one object at
   *  a time, and the close waits for the pass to end. */
  def cancel(): Unit = latchAbort(RebuildCancelled(storeId))

  // Key's equality is not something to bet a test seam on; compare the bytes directly.
  // Declared here, before start(), so it is initialized before the walk begins.
  private val failBytes: Set[List[Byte]] = testingOnlyFailKeys.map(_.bytes.toList)
  private val outOfSpaceBytes: Set[List[Byte]] = testingOnlyOutOfSpaceKeys.map(_.bytes.toList)

  private val pendingSweep = PendingAllocationSweep(storeId, client, peerQueryTimeout, pendingWaitLimit, aborted,
    () => synchronized(abortCause), restorePending, checkpointBestEffort)

  // Started from the constructor, which runs on the host's control thread (StoreRebuilds.startRebuild): start reads
  // only `existingCopy`, already in hand, before its pass runs in futures.
  start()

  private def start(): Unit =
    val f: Future[UUID] =
      try
        existingCopy match
          case Some((phase, path)) if isRebuiltFor(phase, episode) =>
            // A crash between the move and the flip. The store is already whole; there is nothing to
            // rebuild, only the flip left, and that is Host's job on completion.
            logger.info(s"Rebuild of $storeId: store already in place at $path")
            cleanupStaging()
            CopyLayout.parseName(path.last) match
              case Some((_, copyId)) => Future.successful(copyId)
              case None => Future.failed(new IllegalStateException(s"Rebuild of $storeId: $path is no copy name"))
          case Some((phase, path)) =>
            // Not this rebuild's output (any other phase, or Rebuilt for an earlier Rebuilding episode).
            // StoreRebuilds.startRebuild retires such a copy before it starts a rebuild, so this is only a defence:
            // the copy is left alone and the rebuild fails
            Future.failed(new IllegalStateException(
              s"Rebuild of $storeId: $path ($phase) holds a store directory that no rebuild finished"))
          case None =>
            runPass()
      catch
        case err: Throwable => Future.failed(err)

    f.onComplete: outcome =>
      closeBackend().onComplete(_ => completionPromise.tryComplete(outcome))

  private def cleanupStaging(): Unit =
    try
      if FileStat.exists(stagingPath) then os.remove.all(stagingPath)
    catch
      case t: Throwable => logger.warn(s"Rebuild of $storeId: failed to remove $stagingPath: $t")

  /** Idempotent: finish() closes on the happy path, start()'s onComplete closes on every path. */
  private def closeBackend(): Future[Unit] =
    val b = synchronized:
      val prev = backend
      backend = null
      prev

    if b == null then
      Future.unit
    else
      b.close().recover:
        case t => logger.warn(s"Rebuild of $storeId: backend close failed: $t")

  /** When the current pass began, on this host's wall clock (inheritedVotesAfter) */
  @volatile private var passStartMillis: Long = System.currentTimeMillis()

  /** One full pass: open (or reopen) the staging area, walk, retry failures, complete. */
  private def runPass(): Future[UUID] =
    passStartMillis = System.currentTimeMillis()
    // The checkpoint is never deleted on restart -- reading it is what makes a restart a
    // resume.
    // Only this episode's: a checkpoint left by an earlier Rebuilding episode of the store (re-placed away and
    // back since) describes a copy that missed everything written while the store lived elsewhere
    val resume = RebuildState.load(stagingPath).filter: st =>
      val own = st.episode == episode
      if !own then
        logger.warn(s"Rebuild of $storeId: discarding the staging checkpoint of an earlier Rebuilding episode " +
                    s"(${st.episode}, now $episode); the rebuild starts over")
      own

    resume.foreach: st =>
      synchronized:
        lastKey = st.lastRestoredKey
        failed = st.failedObjects
        listedAllocations = st.pendingAllocations.map(p => p.transactionId -> p).toMap
      logger.info(s"Rebuild of $storeId: resuming from ${st.lastRestoredKey}, " +
                  s"${st.failedObjects.size} objects to retry")

    for
      pstate <- client.getStoragePoolState(storeId.poolId)
      stagingCfg = StoreConfig.forPool(storeId, pstate)
      // A first pass (no checkpoint) starts from a fresh staging store, created atomically with its config, so
      // the directory is loadable the instant it is moved into place. A resume opens the one it checkpointed
      _ <- if resume.isEmpty then createStaging(stagingCfg) else Future.unit
      opened <- openStaging(stagingCfg)
      _ = synchronized:
            backend = opened
      // Before the walk: an allocation the lost store acknowledged is either listed by a peer at the first pass's
      // query, or was finalized before it, so its entry is in the tree ahead of every walk (rebuild-process 5.3). What
      // an earlier pass listed is swept again: finalized since, its entry may have landed behind the checkpoint. Every
      // checkpoint carries the union, so the walk never advances durably past what was listed
      queried <- pendingSweep.queryPeers(pstate.ida)
      pending = synchronized:
                  listedAllocations = PendingAllocationSweep.mergeListed(listedAllocations, queried)
                  listedAllocations
      _ <- testingOnlyAfterQuery()
      pool <- client.getStoragePool(storeId.poolId)
      tree = pool.allocationTree
      _ <- resume.flatMap(_.lastRestoredKey) match
             // The resume range is inclusive of the checkpointed key. rebuildWrite is an
             // overwrite, so re-restoring that one object is free, and inclusive is the only
             // bound that cannot skip.
             case Some(k) => tree.foreachFrom(k, restoreObject(_, _, _), stopWhen = aborted)
             case None => tree.foreach(restoreObject(_, _, _), stopWhen = aborted)
      _ <- testingOnlyAfterWalk()
      _ <- pendingSweep.sweepPending(pstate.ida, pending, 0, System.currentTimeMillis() + pendingWaitLimit.toMillis)
      // Check the abort latch after the walks and the sweep complete. If it's set, fail the pass before
      // retryFailures() and finish() run, leaving the checkpoint in staging.
      _ <- synchronized(abortCause) match
             case Some(cause) => Future.failed(cause)
             case None => Future.unit
      _ <- retryFailures()
      copyId <- finish()
    yield copyId

  /** Creates an empty staging store for a first pass, through the BackendOpener (StoreCreation: scratch under the
   *  device's `creating/`, then renamed to the staging path), after discarding whatever a pass that never
   *  checkpointed left there: without a checkpoint none of it is used. */
  private def createStaging(cfg: StoreConfig): Future[Unit] =
    try
      if FileStat.exists(stagingPath) then os.remove.all(stagingPath)
      backends.create(cfg, stagingPath, devicePath / StoreCreation.CreatingDirectory, io)
    catch case NonFatal(t) => Future.failed(t)

  /** Opens the staging store through the BackendOpener, which decides where the open runs: created just before on
   *  a first pass (createStaging), recovered on a resume. A fail-stop latches the
   *  abort, so the pass fails with the checkpoint left for the next one. A staging store whose state is lost
   *  (BackendStateLost) is discarded, checkpoint and all, so the next pass starts over rather than failing on it
   *  forever.
   */
  private def openStaging(cfg: StoreConfig): Future[Backend] =
    def onFatal(err: Throwable): Unit =
      logger.error(s"Rebuild of $storeId: the staging store failed. Abandoning this pass. Error: $err")
      latchAbort(err)
    val fopen =
      try backends.open(cfg, stagingPath, onFatal, io)
      catch case NonFatal(t) => Future.failed(t)
    fopen.recoverWith:
      case e: BackendStateLost =>
        logger.error(s"Rebuild of $storeId: the staging store cannot be recovered. Discarding it to start " +
                     s"over. Error: ${e.getMessage}")
        cleanupStaging()
        Future.failed(e)

  /** Restores one object a peer listed as pending. None once restored (or when the restore cannot be attempted for a
   *  reason the abort latch already carries), else the read failure */
  private def restorePending(ptr: ObjectPointer): Future[Option[Throwable]] =
    if aborted() then
      Future.successful(None)
    else
      val key = Key(ptr.id.toBytes)
      attemptRestore(key, ptr).transformWith:
        case scala.util.Success(_) =>
          recordRestored(key, advance = false).map(_ => None)
        case scala.util.Failure(err) if isOutOfSpace(err) =>
          latchOutOfSpace(err)
          Future.successful(None)
        case scala.util.Failure(err) =>
          logger.debug(s"Rebuild of $storeId: pending allocation ${ptr.id} not restored yet: $err")
          Future.successful(Some(err))

  /** Copy one object's slice of this store into the backend.
   *
   *  foreach swallows a failing fn -- it logs and continues -- so read failures are recorded
   *  here instead. The checkpoint advances past them, so a read failure cannot wedge the walk
   *  forever, and they are retried at the end of the pass. A read that gets no answer fails with
   *  ReadTimeout after `objectReadTimeout` and is one such failure, so it holds the walk, the
   *  Host rebuild slot and a cancel for at most that long.
   */
  private def restoreObject(node: KeyValueListNode,
                            key: Key,
                            value: ValueState): Future[Unit] =
    // If the abort latch is set, return immediately to make the remainder of the walk a cheap
    // no-op rather than thousands of repeated failures.
    if aborted() then
      Future.unit
    else
      // Everything below is inside the try, not just the decode: the walk logs a synchronous
      // throw out of this method and steps past it, which would skip the object without
      // recording it as failed. ObjectPointer decoding is the reachable
      // way to get one: it throws BufferUnderflowException out of the Varint readers and
      // MatchError out of its non-exhaustive typeCode match. Treating an undecodable pointer as
      // one more failed object subjects it to the existing cap and retry machinery instead.
      try
        val ptr = ObjectPointer(value.value.bytes)

        if ptr.poolId != storeId.poolId then
          // The allocation tree is per-pool, but a pointer stored in it is only authoritative for
          // its own pool. Skip anything foreign rather than writing it into the wrong store.
          Future.unit
        else
          attemptRestore(key, ptr).transformWith:
            case scala.util.Success(_) =>
              recordRestored(key, advance = true)
            case scala.util.Failure(err) if isOutOfSpace(err) =>
              // Distinct from a per-object read failure, and not something to accumulate 10,000
              // of: selectDeviceForRebuild checked free space at placement time against the
              // pool's recorded store size, which can be stale and can grow. There is no
              // automatic recovery in this scope -- the operator's remedy is to add capacity --
              // so log it loudly enough to alert on and set the abort latch.
              // Latch first, then checkpoint. The checkpoint is expected to throw when the disk
              // is full, and if it does, the throw must not unwind past the latch. A best-effort
              // checkpoint on a full disk is fine -- losing it costs a restart from the last good
              // one, which is bounded. Losing the latch costs the store.
              latchOutOfSpace(err)
              try checkpoint()
              catch case t: Throwable => logger.warn(s"Rebuild of $storeId: checkpoint after ENOSPC failed: $t")
              Future.unit
            case scala.util.Failure(err) =>
              recordFailedKey(key, err)
      catch
        case NonFatal(t) =>
          recordFailedKey(key, t)

  /** Latch a full destination as the abort cause and raise the operator alert.
   *
   *  Idempotent in the cause: the first fatal condition to fire owns the abort. The alert is
   *  emitted every time, because a disk that fills during the wind-down is still news.
   */
  private def latchOutOfSpace(err: Throwable): Unit =
    latchAbort(err)
    logger.error(s"REBUILD OUT OF SPACE: store $storeId cannot fit on device " +
                 s"$storageDeviceId at $devicePath. The store will remain Rebuilding " +
                 s"until capacity is added or it is re-placed on another device with transfer-store. " +
                 s"Underlying error: $err")

  /** recordFailure keyed by an allocation-tree key rather than by an ObjectId.
   *
   *  Deriving the id is itself a throw site -- ObjectId.apply reads two longs and throws
   *  BufferUnderflowException on anything shorter -- and on restoreObject's synchronous failure
   *  path that throw would escape the very catch that exists to keep the walk from wedging. A
   *  key that is not sixteen bytes is not an object entry at all: there is nothing to restore
   *  and nothing a retry could name, so it is logged and skipped rather than recorded.
   */
  private def recordFailedKey(key: Key, err: Throwable): Future[Unit] =
    if key.bytes.length == ObjectIdKeyLength then
      recordFailure(ObjectId(key.bytes), err)
    else
      logger.warn(s"Rebuild of $storeId: allocation tree entry whose ${key.bytes.length}-byte " +
                  s"key is not an object id; skipping. Underlying error: $err")
      Future.unit

  /** Best-effort detection of a full destination. Backends surface it differently -- RocksDB
   *  wraps it, the JDK throws IOException -- so this matches on the message as well as the
   *  type. A false negative only costs the slower path through recordFailure. The matching is
   *  Backend.isOutOfSpace's, which also counts a quota error (EDQUOT) as a full device. */
  private def isOutOfSpace(err: Throwable): Boolean = Backend.isOutOfSpace(err)

  private def attemptRestore(key: Key, ptr: ObjectPointer): Future[Unit] =
    if failBytes.contains(key.bytes.toList) then
      Future.failed(new Exception(s"injected read failure for $key"))
    else if outOfSpaceBytes.contains(key.bytes.toList) then
      Future.failed(new Exception(s"injected out of space error for $key"))
    else
      restore(ptr)

  /** Reads `ptr` through the client and writes this store's slice of it into staging. A read whose replies report a
   *  write lock also marks the object in the ErrorTree against this store (rebuild-process.md 5.2, locked copies):
   *  the read ignores locks of transactions not known committed, so the copy may predate one the lost store applied */
  private def restore(ptr: ObjectPointer): Future[Unit] =
    def metadataOf(os: ObjectState): (ObjectType.Value, Metadata) = os match
      case kvos: KeyValueObjectState =>
        (ObjectType.KeyValue, Metadata(kvos.revision, kvos.refcount, kvos.timestamp))
      case dos: DataObjectState =>
        (ObjectType.Data, Metadata(dos.revision, dos.refcount, dos.timestamp))
      case _: MetadataObjectState =>
        throw new Exception(s"Unsupported object type for rebuild: $ptr")

    val fos = ptr match
      case p: KeyValueObjectPointer => reader.read(p)
      case p: DataObjectPointer => reader.read(p)

    fos.flatMap: os =>
      val (objectType, metadata) = metadataOf(os)
      val localData = os.getRebuildDataForStore(storeId)
      backend.rebuildWrite(ptr, objectType, metadata, localData.getOrElse(DataBuffer()))
      if os.readLocks.keys.isEmpty && !os.readLocks.objectLevel then
        Future.unit
      else
        // A reply held a write lock: a committed transaction its holder has not applied yet, which the lost store may
        // have applied and acknowledged (so its finalizer marks nothing), may be missing from this copy. Recorded for
        // repair before the restore completes, so the walk never checkpoints past an unrecorded copy. Missed at
        // HLCTimestamp.Zero: the walk does not know the transaction, and repair defers while the lock is reported
        client.missedUpdateMarker.mark(List(
          RebuildingStore.missMark(storeId, ptr.id, ptr)))

  /** `advance`: the walk's position moves to `key` (false for the sweep, which leaves the checkpoint alone) */
  private def recordRestored(key: Key, advance: Boolean): Future[Unit] =
    val checkpointNow = synchronized:
      if testingOnlyTrackRestoredKeys then
        restoredKeys += key
      if !advance then
        false
      else
        consecutiveReadTimeouts = 0
        lastKey = Some(key)
        restoredSinceCheckpoint += 1
        if restoredSinceCheckpoint >= checkpointInterval then
          restoredSinceCheckpoint = 0
          true
        else
          false

    if checkpointNow then checkpointBestEffort("during the walk")
    testingOnlyOnRestored()
    Future.unit

  private def recordFailure(objectId: ObjectId, err: Throwable): Future[Unit] =
    val (over, timedOut) = synchronized:
      // A resumed walk can fail an object an earlier pass already failed: it is one object to retry
      if !failed.contains(objectId) then
        failed = objectId :: failed
      if err.isInstanceOf[ReadTimeout] then
        consecutiveReadTimeouts += 1
      (failed.size > maxFailedObjects, consecutiveReadTimeouts >= maxConsecutiveReadTimeouts)

    logger.warn(s"Rebuild of $storeId: failed to read object $objectId: $err")

    if timedOut && !over then
      // Every read timing out: the pool is below its read threshold, and the sequential walk would hold the host's
      // rebuild slot for days before the cap below. Aborted as the cap aborts, the checkpoint kept
      val cause = new Exception(
        s"Rebuild of $storeId aborted: $maxConsecutiveReadTimeouts object reads in a row timed out")
      latchAbort(cause)
      checkpointBestEffort("after consecutive read timeouts")
      Future.unit
    else if over then
      // Something systemic is wrong -- the pool below its read threshold, most likely -- and
      // continuing only burns I/O. Abort with the checkpoint intact; the next device check
      // retries. Set the abort latch so the rest of the walk is a no-op.
      // Latch first, then checkpoint. If the checkpoint throws, the throw must not unwind past
      // the latch.
      val cause = new Exception(
        s"Rebuild of $storeId aborted: more than $maxFailedObjects unreadable objects")
      latchAbort(cause)
      checkpointBestEffort("after max failures")
      Future.unit
    else
      Future.unit

  /** Flush FIRST, then write the checkpoint. The reverse order would let a crash between the
   *  two produce a checkpoint claiming objects that never reached stable storage. */
  private def checkpoint(): Unit =
    flushBackend()
    saveCheckpoint()

  /** Checkpoint without letting the throw out, classifying it the way a failed object write is
   *  classified.
   *
   *  The flush and the checkpoint write both touch the disk, so a full device surfaces here just
   *  as readily as it does from rebuildWrite -- and when it does it has to produce the abort
   *  latch and the operator alert, not a generic log line. Losing a checkpoint costs a restart
   *  from the last good one, which is bounded; letting the throw escape costs more. On the walk
   *  path it would become a failed Future that walkFrom logs and steps past with
   *  restoredSinceCheckpoint already reset and lastKey already advanced; on the abort paths it
   *  would unwind past a latch that has just been set.
   */
  private def checkpointBestEffort(context: String): Unit =
    try
      checkpoint()
    catch
      case t: Throwable if isOutOfSpace(t) =>
        latchOutOfSpace(t)
      case t: Throwable =>
        logger.warn(s"Rebuild of $storeId: checkpoint $context failed: $t")

  private def flushBackend(): Unit =
    backend.rebuildFlush()
    synchronized(checkpointTrace += "flush")

  private def saveCheckpoint(): Unit =
    val (k, f, l) = synchronized((lastKey, failed, listedAllocations))
    RebuildState.save(stagingPath, RebuildState(storeId, episode, k, f, l.values.toList))
    synchronized(checkpointTrace += "checkpoint")

  /** Retry everything the walk could not read. Anything still failing leaves the store
   *  Rebuilding for the next device check to pick up, unless an operator has accepted its loss (recordAcceptedLoss). */
  private def retryFailures(): Future[Unit] =
    val pending = synchronized:
      val f = failed
      failed = Nil
      f

    if pending.isEmpty then
      Future.unit
    else
      logger.info(s"Rebuild of $storeId: retrying ${pending.size} unreadable objects")
      val pool = client.getStoragePool(storeId.poolId)
      val retries = pending.map: objectId =>
        val key = Key(objectId.toBytes)
        pool.flatMap(_.allocationTree.get(key)).flatMap:
          case Some(vs) => attemptRestore(key, ObjectPointer(vs.value.bytes))
          case None => Future.unit // deleted since the walk saw it; nothing to restore
        .recover:
          case err =>
            synchronized:
              failed = objectId :: failed
            // A full destination is not an unreadable object: it latches the abort and raises its own alert, as on
            // the walk, so the pass fails as out of space rather than "incomplete", and never as an accepted loss
            if isOutOfSpace(err) then
              latchOutOfSpace(err)
            else
              logger.warn(s"Rebuild of $storeId: retry of $objectId failed: $err")

      Future.sequence(retries).flatMap: _ =>
        synchronized((failed, abortCause)) match
          case (_, Some(cause)) =>
            // Latched during the retries (out of space, a cancel): the pass fails with the latched cause before
            // lossAccepted is consulted, so no object a peer can supply is recorded as an accepted loss
            Future.failed(cause)
          case (Nil, None) =>
            Future.unit
          case (remaining, None) =>
            lossAccepted().flatMap: accepted =>
              if accepted then
                recordAcceptedLoss(remaining)
              else
                // Best-effort for the same reason the walk path is: a device that fills up here has to
                // produce the operator alert and the latch, not a generic warning. The pass fails
                // either way, so the classification is the only thing at stake.
                checkpointBestEffort("after retries")
                Future.failed(new Exception(
                  s"Rebuild of $storeId incomplete: ${remaining.size} objects still unreadable. If the pool cannot " +
                  "supply them, `accept-rebuild-loss` lets the rebuild finish without them"))

  /** Whether an operator has accepted the loss of the objects this rebuild cannot read: the store's entry on this
   *  device is Rebuilding with `acceptLoss` (AspenClient.acceptRebuildLoss). A failed read counts as no. */
  private def lossAccepted(): Future[Boolean] =
    client.getStorageDeviceState(storageDeviceId)
      .map(_.stores.get(storeId).exists(e => e.status == StorageDeviceState.StoreStatus.Rebuilding &&
                                             e.rebuildEpisode == episode && e.acceptLoss))
      .recover:
        case NonFatal(t) =>
          logger.warn(s"Rebuild of $storeId: could not read whether its losses are accepted: $t")
          false

  /** Finishes past objects no peer can supply, once an operator has accepted their loss. Each is
   *  recorded in the pool's ErrorTree as missed by this store (missed at HLCTimestamp.Zero, so it never displaces a
   *  newer mark), durably before the store is promoted: repair restores it if it ever becomes readable, and the
   *  entry is the record of the loss until then. An object no longer in the allocation tree was deleted and needs
   *  none; an entry whose pointer cannot be decoded names nothing to restore and is logged. A failed mark fails the
   *  pass, which is retried (rebuild-process 5.5). */
  private def recordAcceptedLoss(ids: List[ObjectId]): Future[Unit] =
    val pool = client.getStoragePool(storeId.poolId)
    val fmarks = Future.sequence(ids.map: objectId =>
      pool.flatMap(_.allocationTree.get(Key(objectId.toBytes))).map:
        case None => None
        case Some(vs) =>
          scala.util.Try(ObjectPointer(vs.value.bytes)).toOption.filter(_.poolId == storeId.poolId) match
            case Some(ptr) =>
              Some(RebuildingStore.missMark(storeId, objectId, ptr))
            case None =>
              logger.error(s"Rebuild of $storeId: the allocation tree entry of lost object $objectId does not " +
                           "decode to a pointer into this pool. Nothing to record")
              None)
    fmarks.flatMap: marks =>
      client.missedUpdateMarker.mark(marks.flatten).map: _ =>
        synchronized:
          failed = Nil
        logger.error(s"REBUILD ACCEPTED LOSS: store $storeId finishes without ${ids.size} objects its pool could " +
                     s"not supply, as an operator accepted. ${marks.flatten.size} are recorded in the ErrorTree for " +
                     s"repair: ${ids.mkString(", ")}")
    .recoverWith:
      case NonFatal(t) =>
        checkpointBestEffort("after a failed loss record")
        Future.failed(new Exception(s"Rebuild of $storeId: failed to record its accepted losses: $t", t))

  /** Flush, close, write the completion files (its episode's phase-param.rebuilt last), drop the checkpoint, rename
   *  staging into rebuilt/ as a fresh copy id (T2), which the completion reports.
   *
   *  The flush and the file writes touch the disk as the walk's writes do, so a full device here is classified the
   *  same way (latchOutOfSpace): the operator alert, and a failed pass. The checkpoint is dropped only after the last
   *  write, so a pass failing on any of them resumes from it rather than walking the pool again. */
  private def finish(): Future[UUID] =
    val done =
      try
        backend.rebuildFlush()
        closeBackend().map(_ => completeStaging())
      catch case NonFatal(t) => Future.failed(t)
    done.transform:
      case scala.util.Failure(err) if isOutOfSpace(err) =>
        latchOutOfSpace(err)
        scala.util.Failure(err)
      case other => other

  /** finish's work once the staging backend is closed. Returns the fresh copy id the copy took in rebuilt/ */
  private def completeStaging(): UUID =
    testingOnlyOnFinishClosed()
    // An abort latched while the backend closed (a fail-stop of the staging store's last I/O, a cancel)
    // means the staging copy must not become the live store: the pass fails with the checkpoint left in place
    synchronized(abortCause).foreach(cause => throw cause)
    // The rebuilt store has no Paxos state: it takes no part in transactions started before now (plus the margin)
    writeAcceptorSince(stagingPath)
    // Its old incarnation's commit votes may still count, with none of their locks held here: the store votes abort
    // on what they may lock until its peers' held votes, complete from this time on, release them
    writeInheritedVotes(stagingPath, inheritedVotesAfter(passStartMillis, horizon))
    // A new life for the store, replacing any incarnation the staging directory carried
    StoreIncarnationFile.writeFresh(stagingPath)
    // Its whole transaction state: its first load drops whatever the device CRL holds under its id
    writeEmptyCrlState(stagingPath, storeId)
    // Last write before the move: the copy's episode, which its location in rebuilt/ reads (T2). Kept out of service
    // there until its flip commits
    val rebuilt = Phase.Rebuilt(rebuildCompleteContent(episode))
    for
      file <- CopyLayout.paramFile(rebuilt)
      p <- CopyLayout.param(rebuilt)
    do DurableFiles.write(stagingPath / file, p)
    // The checkpoint, and a temp file a crash caught between its write and its rename, are kept out of the
    // directory that is about to become a live store
    RebuildState.removeAll(stagingPath)
    // A fresh copy id, as every copy takes when it first enters a phase directory (P4)
    val copyId = UUID.randomUUID()
    val finalPath = CopyLayout.path(devicePath, storeId, copyId, rebuilt)
    val dir = finalPath / os.up
    if !FileStat.exists(dir) then
      os.makeDir.all(dir)
      DurableFiles.syncDirectory(devicePath)
    // Durable before the store is reported in place: a device shut down and moved after this must come back with the
    // store in rebuilt/, or an adopting host refuses it. A failed rename leaves staging whole for the next pass. One
    // that landed but was not forced is in place all the same: reported (placedUnforced), so the host records it
    // unsynced and flips it once a device check has forced it, rather than rebuild the store over it
    try
      DurableFiles.move(stagingPath, finalPath, syncDirectory)
      logger.info(s"Rebuild of $storeId: store in place at $finalPath")
    catch case t: DurableFiles.MoveNotForced =>
      logger.error(s"Rebuild of $storeId: store in place at $finalPath, but its move could not be forced: $t")
      unforcedPlacement = Some(t)
    copyId
