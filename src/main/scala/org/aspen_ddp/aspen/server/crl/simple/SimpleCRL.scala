package org.aspen_ddp.aspen.server.crl.simple

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.{DurableFiles, FilePreallocation, StorageDeviceConfig}
import org.aspen_ddp.aspen.server.store.backend.BackendTiming
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, CrlAdmission, CrlConfigInvalid,
  CrlClosed, CrlFailed, CrlLocked, CrlMissing, CrlSizingDefaults, CrlSizingMismatch, StoreRecoveryState, TransactionRecoveryState}
import scribe.Logging

import java.util.UUID
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, Executors, LinkedBlockingQueue, Semaphore, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.immutable.HashMap
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** When a SimpleCRL may grow by one stream (CrashRecoveryLog.requestGrowth): while its ring is below `maxStreams`,
  * and only if `freeSpace()`, less the new stream, stays at or above `minFreePercent` of `capacity()`. Both are
  * functions of the file system holding the CRL, read at each attempt. After a refused or failed attempt, requests
  * are ignored for `retryAfter` */
case class GrowthPolicy(maxStreams: Int,
                        minFreePercent: Int,
                        capacity: () => Long,
                        freeSpace: () => Long,
                        retryAfter: FiniteDuration = 10.seconds)

object GrowthPolicy:
  /** No stream beyond the ring a CRL was opened with */
  val Disabled: GrowthPolicy = GrowthPolicy(0, 100, () => 0L, () => 0L)

object SimpleCRL extends Logging:

  /** What maxTransactionBytes keeps back from half a stream for the entry's own headers and record framing */
  val EntryOverheadReserve: Long = 8192

  sealed abstract class ActionRequest()

  case class SaveTransaction(txid: TransactionId,
                             state: TransactionRecoveryState,
                             completionHandler: () => Unit) extends ActionRequest

  case class DropTransactionData(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class DeleteTransaction(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  case class DeleteTransactions(storeId: StoreId, transactionIds: List[TransactionId],
                                completionHandler: () => Unit) extends ActionRequest

  case class SaveAppliedStub(storeId: StoreId, transactionId: TransactionId, start: HLCTimestamp,
                             completionHandler: () => Unit) extends ActionRequest

  case class DeleteAppliedStub(storeId: StoreId, transactionId: TransactionId) extends ActionRequest

  /** Drops every entry of the store held when it is processed (CrashRecoveryLog.dropStore) */
  case class DropStore(storeId: StoreId, completionHandler: () => Unit) extends ActionRequest

  case class WriteComplete(serialNumber: Long) extends ActionRequest

  /** Queued when the writer thread dies: the CRL thread dies of it */
  case class WriterDied(cause: Throwable) extends ActionRequest

  /** Queued when a completion handler throws on the completion thread: the CRL thread dies of it, as of a writer's
    * death (fail-stop) */
  case class HandlerFailed(cause: Throwable) extends ActionRequest

  case class Shutdown(completionHandler: () => Unit) extends ActionRequest

  case class GetRecoveryState(storeId: StoreId,
                              completion: StoreRecoveryState => Unit
                             ) extends ActionRequest

  case class GetStoresWithEntries(completion: Set[StoreId] => Unit) extends ActionRequest

  /** Runs `f` on the CRL thread, in order with the other requests: for tests that hold the thread */
  case class RunOnCrlThread(f: () => Unit) extends ActionRequest

  /** Runs the handler once every request queued before it is durable (CrashRecoveryLog.whenDurable) */
  case class Barrier(handler: () => Unit) extends ActionRequest

  /** Queued by the growth thread once stream `streamNumber` is preallocated, renamed into place and its directory
    * forced: the CRL thread commits it into the ring */
  case class GrowthReady(streamNumber: Int) extends ActionRequest


  case class InitialCRLState(crl: SimpleCRL,
                             trsList: List[TransactionRecoveryState],
                             appliedStubs: Map[TxId, HLCTimestamp])

  /** Opens each device's SimpleCRL in `CrashRecoveryLog.deviceCrlDir`, creating the directory if absent. A new CRL is
   *  sized by the device's config (CrlConfigInvalid if the config cannot size it); an existing one keeps the sizing it
   *  recorded (SizingFile). `onThreadDeath` is called if a CRL thread dies: that device's transactions stop. */
  case class DeviceFactory(onThreadDeath: Throwable => Unit = _ => ()) extends CrashRecoveryLogFactory:
    def openCRL(devicePath: os.Path, config: StorageDeviceConfig, holdsStores: Boolean): CrashRecoveryLog =
      val dir = CrashRecoveryLog.deviceCrlDir(devicePath)
      os.makeDir.all(dir)
      val capacity = devicePath.toIO.getTotalSpace
      // Only a CRL being created is held to the config: an existing one opens as recorded whatever it says. Stream
      // files without a sizing file are damage, which SimpleCRL.apply refuses (CrlSizingMismatch): a config refusal
      // must not hide that
      val sizing =
        if os.exists(dir / SizingFile) || streamFiles(dir).nonEmpty then
          CrlSizingDefaults.resolveExisting(config, capacity)
        else
          try CrlSizingDefaults.resolve(config, capacity)
          catch case e: IllegalArgumentException => throw CrlConfigInvalid(dir, e)
      // Free space is read at each growth attempt, from the file system holding the CRL
      val growth = GrowthPolicy(sizing.maxStreams, sizing.minFreePercent,
        () => devicePath.toIO.getTotalSpace, () => devicePath.toIO.getUsableSpace)
      SimpleCRL(dir, sizing.numStreams, sizing.streamBytes, onThreadDeath,
        mustExist = holdsStores, growth = growth).crl

  /** The CRL's own record of its sizing and id, written durably when it is created, before any stream file. The
   *  sizing is fixed then: an existing CRL is always opened with the sizing it recorded, whatever it is asked for, so
   *  a change of the device config or of the code defaults cannot make it unopenable */
  val SizingFile = "sizing"

  /** The sizing a CRL was created with, and its id: a random UUID chosen then, which every entry header bears
   *  (LogEntry). Recovery takes only headers bearing it for entries, so object data in the streams, which nobody
   *  outside the device can make bear an id it never sees, cannot pass for one.
   *
   *  `ring` is the order the streams are written in: rotation and recovery's next-stream check follow it, not the
   *  stream numbers. Stream `<n>.crl` exists for each `n` in `0 until ring.size`; a growth inserts a new number after
   *  the active stream, so the ring need not be in numeric order. Rewriting this file is a growth's commit point */
  case class Sizing(ring: Vector[StreamId], maxSizeInBytes: Long, crlId: UUID):
    def numStreams: Int = ring.size

    /** The stream written after `s` */
    def successor(s: StreamId): StreamId = StreamId.successor(ring, s)

    def encode: String = s"num-streams: $numStreams\nmax-size-in-bytes: $maxSizeInBytes\ncrl-id: $crlId\n" +
      s"ring: ${ring.map(_.number).mkString(",")}\n"

  object Sizing:
    /** A sizing whose ring is in numeric order, as a CRL is created */
    def inOrder(numStreams: Int, maxSizeInBytes: Long, crlId: UUID): Sizing =
      Sizing(Vector.tabulate(numStreams)(StreamId(_)), maxSizeInBytes, crlId)

    /** None if `text` is not a sizing file's content, or its ring is not a permutation of `0 until num-streams` */
    def decode(text: String): Option[Sizing] =
      val fields = text.linesIterator.map(_.split(":", 2)).collect:
        case Array(k, v) => k.trim -> v.trim
      .toMap
      for
        n <- fields.get("num-streams").flatMap(_.toIntOption)
        m <- fields.get("max-size-in-bytes").flatMap(_.toLongOption)
        id <- fields.get("crl-id").flatMap(v => scala.util.Try(UUID.fromString(v)).toOption)
        numbers <- fields.get("ring").map(_.split(",", -1).toVector.map(_.trim.toIntOption))
        if numbers.forall(_.isDefined) && numbers.flatten.sorted == (0 until n)
      yield Sizing(numbers.flatten.map(StreamId(_)), m, id)

  /** @param numStreams     the stream count of a CRL created by this call. An existing CRL keeps its recorded count
    * @param maxSizeInBytes the stream size of a CRL created by this call. An existing CRL keeps its recorded size
    * @param onThreadDeath called if a CRL thread dies. No transaction of the CRL's stores can make progress after
    *                      that: stores vote only once a save completes.
    * @throws CrlLocked if the CRL is open elsewhere (CrlLock). Nothing in `streamsDir` is read or written then.
    * @throws CrlSizingMismatch if `streamsDir` holds stream files but no readable SizingFile, or stream files other
    *                           than the recorded count of the recorded size each, or `.crl.tmp` files that are not an
    *                           unfinished creation (createStreamsIfNew). Recovering with another sizing, or creating
    *                           the CRL afresh, would silently ignore or delete files holding live entries, so the CRL
    *                           is not opened. Nothing in `streamsDir` is written then.
    * @param mustExist the CRL must already exist (its device holds stores). With no stream files in `streamsDir`
    *                  (only a SizingFile or unfinished `.crl.tmp` files at most), CrlMissing is thrown instead of a
    *                  new CRL being created, and nothing in `streamsDir` is written.
    * @param ringForTest the ring of a CRL created by this call, a permutation of `0 until numStreams`, in place of
    *                    numeric order: lets tests reach rings only a growth makes. An existing CRL keeps its ring
    * @param growth when the CRL may grow (requestGrowth). A `maxStreams` at or below the ring size grows nothing
    */
  def apply(streamsDir: os.Path,
            numStreams: Int,
            maxSizeInBytes: Long,
            onThreadDeath: Throwable => Unit = _ => (),
            mustExist: Boolean = false,
            ringForTest: Option[Vector[StreamId]] = None,
            growth: GrowthPolicy = GrowthPolicy.Disabled): InitialCRLState =
    // Held from before the files are read until the writer has closed them. A failed open releases it
    val lock = CrlLock.acquire(streamsDir)
    try
      if mustExist && streamFiles(streamsDir).isEmpty then
        throw CrlMissing(streamsDir)
      val sizing = recordedSizing(streamsDir, numStreams, maxSizeInBytes, ringForTest)
      val files: List[(StreamId, os.Path)] = (0 until sizing.numStreams).map(i =>
        (StreamId(i), streamsDir / s"$i.crl")
      ).toList

      // Before createStreamsIfNew, which would refuse a growth's unlisted `.crl.tmp` as damage
      removeUncommittedGrowth(streamsDir, files.map(_._2).toSet)
      createStreamsIfNew(streamsDir, files.map(_._2), sizing.maxSizeInBytes, mustExist)
      checkSizing(streamsDir, files.map(_._2).toSet, sizing.maxSizeInBytes)

      val r = Recovery.recover(files, sizing.crlId, sizing.ring)

      InitialCRLState(
        new SimpleCRL(
          streamsDir,
          sizing,
          files,
          r,
          onThreadDeath,
          lock,
          growth),
        r.trsList,
        r.appliedStubs)
    catch case t: Throwable =>
      lock.release()
      throw t

  private def streamFiles(streamsDir: os.Path): Set[os.Path] =
    if os.isDir(streamsDir) then os.list(streamsDir).filter(_.ext == "crl").toSet else Set[os.Path]()

  /** The sizing `streamsDir`'s CRL recorded. A directory with neither a SizingFile nor stream files is a new CRL: it
    * records the requested sizing and a new random id first. A SizingFile with no stream files is a creation that
    * crashed before them: its sizing and id stand. Stream files without a readable SizingFile throw CrlSizingMismatch */
  private def recordedSizing(streamsDir: os.Path, numStreams: Int, maxSizeInBytes: Long,
                             ringForTest: Option[Vector[StreamId]]): Sizing =
    val file = streamsDir / SizingFile
    if os.exists(file) then
      val recorded = Sizing.decode(os.read(file)).filter(s => s.numStreams >= 3 && s.maxSizeInBytes % 4096 == 0)
        .getOrElse:
          throw CrlSizingMismatch(streamsDir, s"its $SizingFile file is unreadable")
      // A ring grown past the configured count is normal (requestGrowth): only a smaller ring or another stream
      // size departs from the config
      if recorded.numStreams < numStreams || recorded.maxSizeInBytes != maxSizeInBytes then
        logger.warn(s"The CRL in $streamsDir was created with ${recorded.numStreams} streams of " +
                    s"${recorded.maxSizeInBytes} bytes, not the $numStreams of $maxSizeInBytes configured. " +
                    s"Opening it with the sizing it was created with")
      recorded
    else if streamFiles(streamsDir).nonEmpty then
      throw CrlSizingMismatch(streamsDir, s"it holds stream files but no $SizingFile file")
    else
      // The admission limit, (n − 2) · S/2 − S/16, needs a third stream (crash-recovery-log 6.1). The proof that every
      // entry is written rests on whole 4 KiB pages: S and every entry are multiples of 4096 (S1)
      require(numStreams >= 3, s"A CRL needs at least 3 streams, not $numStreams")
      require(maxSizeInBytes % 4096 == 0, s"A CRL's stream size must be a multiple of 4096, not $maxSizeInBytes")
      val created = ringForTest match
        case None => Sizing.inOrder(numStreams, maxSizeInBytes, UUID.randomUUID())
        case Some(ring) =>
          require(ring.map(_.number).sorted == (0 until numStreams), s"$ring is not a ring of $numStreams streams")
          Sizing(ring, maxSizeInBytes, UUID.randomUUID())
      DurableFiles.write(file, created.encode)
      created

  private val warnedCopyOnWrite = new AtomicBoolean(false)

  /** Creates the stream files of a new CRL, preallocated at their full size (FilePreallocation), so no CRL write ever
    * allocates a block and a full device cannot fail one with ENOSPC (crash-recovery-log 5.1). Each is written as
    * `<name>.tmp` and renamed into place, and the directory is forced once all are: only then does the writer start.
    * So a crashed creation leaves each listed stream at most once, as `<n>.crl` or as `<n>.crl.tmp`, and no other
    * `.crl.tmp`; it wrote no entry, and it starts over. It never leaves one under `mustExist`: stores exist only on a
    * device whose CRL creation finished. Any other `.crl.tmp` (a stray beside its stream, an unlisted name, or any
    * under `mustExist`) is damage beside a CRL that may hold entries: CrlSizingMismatch, and nothing is removed.
    * A copy-on-write file system allocates on every overwrite, which no preallocation prevents: warned once. */
  private def createStreamsIfNew(streamsDir: os.Path, streams: List[os.Path], maxSizeInBytes: Long,
                                 mustExist: Boolean): Unit =
    def tmpOf(f: os.Path): os.Path = f / os.up / s"${f.last}.tmp"
    val leftoverTmps = os.list(streamsDir).filter(_.last.endsWith(".crl.tmp"))
    if leftoverTmps.nonEmpty then
      val present = streamFiles(streamsDir)
      val unfinishedCreation = !mustExist && leftoverTmps.forall: t =>
        val stream = streamsDir / t.last.stripSuffix(".tmp")
        streams.contains(stream) && !present.contains(stream)
      if !unfinishedCreation then
        throw CrlSizingMismatch(streamsDir, s"it holds ${leftoverTmps.map(_.last).sorted.mkString(", ")} beside " +
          s"${present.map(_.last).toList.sorted.mkString(", ")}: not an unfinished creation " +
          s"${if mustExist then "(the device holds stores)" else s"of its ${streams.size} streams"}")
      logger.warn(s"The creation of the CRL in $streamsDir did not finish. Creating its stream files again")
      (leftoverTmps ++ present).foreach(os.remove(_))
    if streamFiles(streamsDir).isEmpty then
      if FilePreallocation.isCopyOnWrite(streamsDir) && warnedCopyOnWrite.compareAndSet(false, true) then
        logger.warn(s"The CRL in $streamsDir is on a copy-on-write file system: its writes allocate, so a full " +
                    s"device can stop it. Use a file system that overwrites in place, or disable copy-on-write " +
                    s"for the directory (chattr +C)")
      streams.foreach(f => FilePreallocation.preallocate(tmpOf(f), maxSizeInBytes))
      streams.foreach(f => os.move(tmpOf(f), f, atomicMove = true))
      DurableFiles.syncDirectory(streamsDir)

  /** A growth creates stream `<ringSize>` as `<ringSize>.crl.tmp`, renames it to `<ringSize>.crl`, and only then
    * commits it by rewriting the SizingFile with the new ring. Either file beside a ring that does not list it is a
    * growth that crashed before its commit: it holds no entry, and is removed. A growth runs only on a CRL whose
    * `listed` streams all exist, so a leftover beside any other set of files is damage, not a crash: it is
    * CrlSizingMismatch, and nothing is removed. Removing it there could leave no stream files, and a new, empty CRL
    * would be created under the device's stores */
  private def removeUncommittedGrowth(streamsDir: os.Path, listed: Set[os.Path]): Unit =
    val ringSize = listed.size
    val leftovers = List(s"$ringSize.crl", s"$ringSize.crl.tmp").map(streamsDir / _).filter(os.exists(_))
    if leftovers.nonEmpty then
      val others = os.list(streamsDir).filter(f => f.ext == "crl" || f.last.endsWith(".crl.tmp")).toSet --
        leftovers -- listed
      if others.nonEmpty || !listed.forall(os.exists(_)) then
        throw CrlSizingMismatch(streamsDir, s"it holds ${leftovers.map(_.last).mkString(", ")}, an uncommitted " +
          s"growth, beside ${(streamFiles(streamsDir) -- leftovers).map(_.last).toList.sorted.mkString(", ")}, " +
          s"not the $ringSize streams its ring lists")
      leftovers.foreach: f =>
        logger.warn(s"The CRL in $streamsDir holds ${f.last}, a growth that never committed. Removing it")
        os.remove(f)
      DurableFiles.syncDirectory(streamsDir)

  /** A directory with no stream files is a new CRL. Otherwise its files must be exactly `expected`, each
    * `maxSizeInBytes` long (createStreamsIfNew creates them at that size) */
  private def checkSizing(streamsDir: os.Path, expected: Set[os.Path], maxSizeInBytes: Long): Unit =
    val present = streamFiles(streamsDir)
    if present.nonEmpty then
      if present != expected then
        throw CrlSizingMismatch(streamsDir, s"found ${present.map(_.last).toList.sorted.mkString(", ")}, expected " +
          s"${expected.size} streams")
      present.foreach: f =>
        if os.size(f) != maxSizeInBytes then
          throw CrlSizingMismatch(streamsDir, s"${f.last} is ${os.size(f)} bytes, expected $maxSizeInBytes")

class SimpleCRL private (streamsDir: os.Path,
                         initialSizing: SimpleCRL.Sizing,
                         files: List[(StreamId, os.Path)],
                         r: Recovery.Result,
                         onThreadDeath: Throwable => Unit,
                         lock: CrlLock,
                         growth: GrowthPolicy) extends CrashRecoveryLog with Logging:

  import SimpleCRL._

  val maxSizeInBytes: Long = initialSizing.maxSizeInBytes
  private val crlId: UUID = initialSizing.crlId

  /** The ring as recorded in the SizingFile. Owned by the CRL thread: only a committed growth changes it */
  private var sizing: Sizing = initialSizing

  /** `sizing`'s ring size, for other threads: set by the CRL thread once a growth is committed */
  @volatile private var ringSize: Int = initialSizing.numStreams

  def numStreams: Int = ringSize

  /** The most an entry other than an evacuation may hold (crash-recovery-log 6.1, W1): a request that would push the
    * newest unwritten entry past it seals that entry. A lone transaction, at most maxTransactionBytes, always fits */
  val maxEntryBytes: Long = maxSizeInBytes / 2

  /** `(n − 2) · S/2 − S/16` for `n` streams of `S` bytes (crash-recovery-log 6.1, "The limit"): with it, every
    * evacuation fits an empty stream and every sealed entry is written within `n − 1` rotations, as long as the
    * unoffset stubs outstanding stay within the `S/16` reserve's margin. Past that the CRL fails (W6) */
  override def admissionLimitBytes: Long = limitAt(numStreams)

  /** The limit at `CrlSizingDefaults.MaxMaxStreams` streams, the most any device config can let the CRL grow to.
    * Not the limit at this open's `crl-max-streams`: the config can raise that cap, or restore one an invalid bound
    * disabled, so a state above it is one that does not fit yet, never one that cannot (crash-recovery-log 6.1, A3) */
  override def maxAdmissionLimitBytes: Long = limitAt(math.max(numStreams, CrlSizingDefaults.MaxMaxStreams))

  private def limitAt(streams: Int): Long = (streams - 2) * maxSizeInBytes / 2 - maxSizeInBytes / 16

  /** Half a stream, less EntryOverheadReserve for the headers of the entry that holds it, so one transaction's entry
    * always fits in a stream. A larger one would kill the CRL at writeHead's require (crash-recovery-log 10.20)
    */
  override def maxTransactionBytes: Long = maxSizeInBytes / 2 - EntryOverheadReserve

  private val ioQueue = new LinkedBlockingQueue[ActionRequest]()

  // Reported through the CRL thread, which dies of it: the CRL has one death path
  private val writer = StreamWriter(maxSizeInBytes, files, t => ioQueue.put(WriterDied(t)))
  private var currentStream: Int = r.activeStreamId.number

  /** The serials of the entries holding every piece of the image: its oldest is oldestEntryNeeded, which each entry
    * stamps when its buffers are built (LogEntry.createEntryBuffers) */
  private val livePieces = new LivePieces

  /** The durable image (crash-recovery-log 6.1, W3): what evacuations carry */
  private val image = new Image(livePieces)

  /** The newest unwritten entry, not yet sealed: requests are added to it. `currentEntryNumber` counts the entries
    * opened, so a record can tell whether its newest form is in this one (LiveTx.latestEntry) without holding on to
    * an entry once it is written */
  private var currentLogEntry: LogEntry = new LogEntry(image)
  private var currentEntryNumber: Long = 0

  /** Seals the newest entry: it queues behind the other sealed ones, and a new one takes requests */
  private def seal(): Unit =
    sealedEntries.enqueue(currentLogEntry)
    currentLogEntry = new LogEntry(image)
    currentEntryNumber += 1

  /** Sealed entries, oldest first, written in order one at a time (W1). The head is the entry written next */
  private val sealedEntries = scala.collection.mutable.Queue[LogEntry]()

  /** The entry whose write is in flight, if any: unwritten, so its deletions are final for later saves (W1b) */
  private var inFlight: Option[LogEntry] = None

  /** The serial number and previous-entry link the next entry built takes */
  private var nextSerial: Long = r.initialNextEntrySerialNumber
  private var previousEntryLocation: StreamLocation = r.initialPreviousEntryLocation

  /** Rotations since a head was last written (W6) */
  private var rotationsWithoutHead: Int = 0

  /** By stream number: a growth appends the next one */
  private val streams = scala.collection.mutable.ArrayBuffer.fill[Stream](files.length)(null)

  /** The live transactions as saved: readStoreState's view. A deleted one leaves at once, though the image keeps
    * it until its deletion's entry is built */
  private var transactions: HashMap[TxId, LiveTx] = new HashMap()

  /** Applied stubs, with their start timestamps: entries apart from transactions, under the same ids
    * (StoreRecoveryState) */
  private var stubs: HashMap[TxId, HLCTimestamp] = new HashMap()

  private var writeInProgress = false
  /** Barriers and skipped saves queued while no unwritten entry but the one in flight held anything: they run once
    * its write completes */
  private var afterInFlight: List[() => Unit] = Nil

  /** The completion thread: runs every save, stub, deletion and barrier handler, in the order the CRL thread hands
    * them over (crash-recovery-log C2). Kept off the writer and CRL threads, whose next write and next entry's build
    * would otherwise wait for the handlers' network sends. Shut down with the writer: handlers already handed
    * over, all of durable entries, still run */
  private val completions = Executors.newSingleThreadExecutor: r =>
    val t = new Thread(r, "crl-completions")
    t.setDaemon(true)
    t

  /** Hands `handlers` to the completion thread, to run in order. One that throws fails the CRL, and the rest of
    * them are not run */
  private def complete(handlers: () => Unit): Unit =
    completions.execute: () =>
      try handlers()
      catch case t: Throwable =>
        logger.error("CRL completion handler failed", t)
        ioQueue.put(HandlerFailed(t))

  // ---- Growth (crl/CLAUDE.md): preallocated on the growth thread, committed on the CRL thread ----

  private val limitListeners = new CopyOnWriteArrayList[Long => Unit]()

  /** Held by the growth thread across each step that touches the directory, which runs only while `growthStopped` is
    * unset. stopGrowth sets it and then takes the lock, so once stopGrowth returns no growth step runs or will run */
  private val growthFileLock = new Object
  @volatile private var growthStopped = false

  /** An attempt is under way: from the request that starts it to its refusal, failure or commit (endGrowthAttempt) */
  private val growthBusy = new AtomicBoolean(false)
  private val growthRequests = new Semaphore(0)
  @volatile private var growthAllowedAt: Long = System.nanoTime()
  private val growthAttemptsEnded = new AtomicInteger(0)
  private val loggedGrowthCap = new AtomicBoolean(false)

  /** Tests only: runs inside the preallocation step, before the file is created */
  @volatile private[simple] var inPreallocationForTest: () => Unit = () => ()

  /** Tests only: runs on the writer thread once an entry's write has returned, before its WriteComplete is queued.
    * Blocking in it holds the entry durable but unacknowledged: no handler of it runs and nothing more is issued */
  @volatile private[simple] var entryWrittenForTest: LogEntry => Unit = _ => ()

  /** A GrowthReady waiting for the rotation in progress to place its head (commitGrowthWhenQuiescent). CRL thread */
  private var pendingGrowth: Option[Int] = None

  /** The CRL's own thread for growth's slow steps, off the CRL thread (a preallocation may zero-fill) and off the
    * writer (DSYNC writes are not delayed). None when the policy allows no stream beyond the ring */
  private val growthThread: Option[Thread] =
    if growth.maxStreams <= initialSizing.numStreams then None
    else
      val t = new Thread(() => growthLoop(), s"crl-growth-${streamsDir.last}")
      t.setDaemon(true)
      Some(t)

  /** Stops the growth thread: no further step runs, and the step in progress is interrupted. Then waits for that
    * step to unwind, which the interrupt makes prompt: a zero-fill or a force through a FileChannel aborts with
    * ClosedByInterruptException (the preallocation removes its file), a wait throws InterruptedException. A
    * fallocate is one system call and not interruptible, but allocates without writing */
  private def stopGrowth(): Unit =
    growthStopped = true
    growthThread.foreach(_.interrupt())
    growthFileLock.synchronized(())

  /** Shuts the writer down, releasing the lock once it has closed the stream files and before `completionHandler`.
    * Growth stops first, so no growth step runs after the lock is released: a growth in flight is abandoned, and
    * leaves at most a `.tmp` or an unlisted stream file, which the next open removes */
  private def shutdownWriter(completionHandler: () => Unit): Unit =
    stopGrowth()
    completions.shutdown()
    writer.shutdown: () =>
      lock.release()
      completionHandler()

  // Indexed by stream number below: a ring lists `0 until ringSize`
  files.foreach: tpl =>
    if tpl._1 == r.activeStreamId then
      streams(tpl._1.number) = new Stream(r.activeStreamId, crlId, writer,
        r.currentStreamUUID, r.initialNextEntryOffset)
    else
      streams(tpl._1.number) = new Stream(tpl._1, crlId, writer, UUID.randomUUID(), 0)

  // The open writes nothing (crash-recovery-log 5.7): each recovered record's newest durable form is its last-built
  // form, its pieces pinned in livePieces by the verified entry holding each (Recovery.entryHolding), as if this
  // instance had written them. So the first entry's oldestEntryNeeded is no newer than any of them: every entry
  // recovery read them from is walked again. The last durable entry kept the ring's invariant (the stream after the
  // active one holds nothing needed), so the next rotation's evacuation finds and moves them as the instance before
  // the crash would have. A failed open shuts its writer down: the caller may open the same files again
  try
    r.txs.foreach: t =>
      require(t.updatePieces.size == t.state.objectUpdates.size)
      val built = BuiltTx(TxForm(t.state, keepUpdates = true), t.txdPiece +: t.updatePieces)
      built.pieces.foreach(p => livePieces.add(p.serial))
      image.txs += t.id -> built
      val tx = new LiveTx(t.state)
      tx.latest = built.form
      transactions += (t.id -> tx)
    r.stubs.foreach: s =>
      livePieces.add(s.anchor.serial)
      image.stubs += s.id -> BuiltStub(s.start, s.anchor)
      stubs += (s.id -> s.start)
  catch case t: Throwable =>
    val closed = new CountDownLatch(1)
    shutdownWriter(() => closed.countDown())
    closed.await()
    throw t

  override val recoveredBytesByStore: Map[StoreId, Long] =
    val byStore = transactions.iterator.map((id, tx) => id.storeId -> CrlAdmission.recoveredBytes(tx.state)) ++
      stubs.keysIterator.map(id => id.storeId -> CrlAdmission.AppliedStubBytes)
    byStore.toList.groupMapReduce(_._1)(_._2)(_ + _)

  // Before the thread starts: its catch counts shutdownComplete down
  private val shutdownRequested = new AtomicBoolean(false)
  private val shutdownComplete = new CountDownLatch(1)

  // The CRL has failed: its thread is dead (of its own error or the writer's), and nothing it is asked from now on is
  // answered. `cause` is set before `dead`
  @volatile private var cause: Throwable = null
  @volatile private var dead = false
  private val death = Promise[Throwable]()

  // The CRL thread has taken the Shutdown: nothing it is asked from now on is answered, and the unwritten entries are
  // discarded. Set before the Shutdown fails `pending`
  @volatile private var closed = false

  // The promises of the Future-returning calls not answered yet. A death fails them with CrlFailed, a shutdown with
  // CrlClosed, so no caller waits on a CRL that will never answer (crash-recovery-log 5.10). Handler-style completions
  // (save, saveAppliedStub, whenDurable) are never run after either: nothing that waits on one, a vote above all, may
  // proceed without the write
  private val pending = java.util.concurrent.ConcurrentHashMap.newKeySet[Promise[?]]()

  private def failPending(error: () => Throwable): Unit =
    pending.forEach(p => p.tryFailure(error()))

  /** Queues `request(p)` and returns `p`'s future, failed with CrlFailed if the CRL is or becomes dead first, or with
    * CrlClosed if it is or becomes shut down first. Added before `dead` and `closed` are checked: either the dying or
    * shutting-down thread's failPending finds `p`, or this check sees the death or the shutdown */
  private def tracked[T](request: Promise[T] => ActionRequest): Future[T] =
    val p = Promise[T]()
    pending.add(p)
    p.future.onComplete(_ => pending.remove(p))(using scala.concurrent.ExecutionContext.parasitic)
    ioQueue.put(request(p))
    if dead then
      p.tryFailure(CrlFailed(cause))
    else if closed then
      p.tryFailure(CrlClosed())
    p.future

  private val ioThread = new Thread {
    override def run(): Unit =
      try crlThread()
      catch case t: Throwable =>
        logger.error("CRL thread died", t)
        cause = t
        dead = true
        failPending(() => CrlFailed(cause))
        // Nothing takes the Shutdown that shutdown() queues now, so the shutdown is completed here: the files are
        // closed and the latch counted down once the writer finishes what it holds. Without it the device could not
        // be loaded again (its path stays in closingCrls) and the host's stop would wait forever.
        shutdownWriter(() => shutdownComplete.countDown())
        death.trySuccess(t)
        onThreadDeath(t)
  }
  ioThread.start()
  growthThread.foreach(_.start())

  /** The serial number the next entry built takes */
  def currentEntrySerialNumber: Long = nextSerial

  def currentStreamNumber: Int = currentStream

  /** As the next entry would stamp it if built now, its own pieces aside (they are no older) */
  def oldestEntryNeeded: Long = livePieces.oldest.getOrElse(nextSerial)

  /** Tests only, on the CRL thread (runOnCrlThread): how many sealed entries wait to be written */
  private[simple] def sealedEntryCount: Int = sealedEntries.size

  /** The newest unwritten entry holding anything, which handlers and barriers attach to: it is written after every
    * request queued before them */
  private def newestUnwritten: Option[LogEntry] =
    if !currentLogEntry.isEmpty then Some(currentLogEntry) else sealedEntries.lastOption

  /** The newest unwritten entry, sealed first if `size` (its size with the request added) would take it past
    * maxEntryBytes. An empty entry takes any request: a lone record always fits a stream (maxTransactionBytes) */
  private def entryFor(size: LogEntry => Long): LogEntry =
    if !currentLogEntry.isEmpty && size(currentLogEntry) > maxEntryBytes then
      seal()
    currentLogEntry

  /** Runs `handler` once every request queued before it is durable */
  private def whenWritten(handler: () => Unit): Unit =
    newestUnwritten match
      case Some(entry) => entry.addCompletionHandler(handler)
      case None if writeInProgress => afterInFlight = handler :: afterInFlight
      case None => complete(handler)

  private def startWrite(): Unit =
    if !writeInProgress then
      if sealedEntries.isEmpty && !currentLogEntry.isEmpty then
        seal()
      if sealedEntries.nonEmpty then
        writeHead()

  /** Writes the head in the active stream if it fits there. Otherwise rotates (W2): enters the successor at offset 0
    * and first writes an evacuation entry carrying every record of the image lying in the stream after it, the next
    * to be entered. The head waits for that write and tries again: beside the evacuation, or after another rotation */
  private def writeHead(): Unit =
    val head = sealedEntries.head
    // Can never fit: only a lone record past maxTransactionBytes, which admission drops, is this large
    // (crash-recovery-log 10.20)
    require(head.entrySize <= maxSizeInBytes, s"CRL entry of ${head.entrySize} bytes is larger than a stream")
    if streams(currentStream).canWriteEntry(head) then
      sealedEntries.dequeue()
      rotationsWithoutHead = 0
      issue(head)
    else
      // W6: within the admission limit every head is written within n − 1 rotations (crash-recovery-log 6.1, Lemma
      // B). More means its assumption broke; rotating on would only spin, with every deletion stuck behind the head.
      // The ring does not change while the count runs: a growth waits for the head (commitGrowthWhenQuiescent)
      val n = sizing.numStreams
      if rotationsWithoutHead >= n - 1 then
        throw IllegalStateException(s"CRL entry of ${head.entrySize} bytes not written after ${n - 1} " +
          s"rotations in a row (n − 1 for $n streams): the live data exceeds what the ring holds, the " +
          s"bound of crash-recovery-log 6.1")
      rotationsWithoutHead += 1
      currentStream = sizing.successor(StreamId(currentStream)).number

      // The stream entered now holds nothing of the image: the previous rotation evacuated it. This rotation's
      // evacuation carries the image's records lying in the next stream to be entered, so a torn write here, or there
      // at the next rotation, overwrites only entries no recovery needs. Only the pieces in those streams move; a
      // record's others stay where they are, in streams written after the next one to be entered, and its static
      // record is rewritten here naming both. Once this entry is built no piece of the image is in the next stream,
      // so its stamped oldestEntryNeeded never points there. Until it is durable the entry before it, stamped while
      // the moved pieces were pinned, is the one a recovery starts from
      val entered = StreamId(currentStream)
      val nextToEnter = sizing.successor(entered)
      streams(currentStream).recycleStream()
      val moved = Set(entered, nextToEnter)
      val evacuation = new LogEntry(image)
      def lyingIn(id: TxId, pieces: Iterable[Piece]): Boolean =
        if pieces.exists(_.location.streamId == entered) then
          logger.error(s"CRL entry $id still had data in entered stream $entered. Evacuating it")
        pieces.exists(p => moved.contains(p.location.streamId))
      image.txs.foreach: (id, built) =>
        if lyingIn(id, built.pieces) then
          evacuation.addTx(id, TxEvacuation(built, moved), () => ())
      image.stubs.foreach: (id, built) =>
        if lyingIn(id, List(built.anchor)) then
          evacuation.addStub(id, built.start, () => ())
      if evacuation.isEmpty then
        writeHead()
      else
        // An evacuation is no larger than what the stream it empties held (crash-recovery-log 6.1, Lemma A)
        issue(evacuation)

  private def issue(entry: LogEntry): Unit =
    writeInProgress = true
    inFlight = Some(entry)
    val serial = nextSerial
    logger.trace(s"CRL beginning write for entry $serial")
    previousEntryLocation = streams(currentStream).writeEntry(entry, serial, previousEntryLocation, () => {
      entryWrittenForTest(entry)
      ioQueue.put(WriteComplete(serial))
    })
    nextSerial += 1

  private def crlThread(): Unit =
    while true do
      ioQueue.take() match
        case SaveTransaction(transactionId, state, completionHandler) => onSaveTransaction(transactionId, state, completionHandler)

        case DropTransactionData(storeId, transactionId) => onDropTransactionObjectData(storeId, transactionId)

        case DeleteTransaction(storeId, transactionId) => onDeleteTransaction(storeId, transactionId)

        case DeleteTransactions(storeId, transactionIds, completionHandler) =>
          onDeleteTransactions(storeId, transactionIds, resavable = false, completionHandler)

        case DropStore(storeId, completionHandler) =>
          // Read and deleted in one step: what any earlier request saved is dropped, nothing a later one saves
          // (crash-recovery-log C14). A save in this same entry cancels the deletion, as it is the store loaded again
          val ids = (transactions.keysIterator ++ stubs.keysIterator).filter(_.storeId == storeId)
            .map(_.transactionId).toList.distinct
          onDeleteTransactions(storeId, ids, resavable = true, completionHandler)

        case SaveAppliedStub(storeId, transactionId, start, completionHandler) =>
          onSaveAppliedStub(storeId, transactionId, start, completionHandler)

        case DeleteAppliedStub(storeId, transactionId) => onDeleteAppliedStub(storeId, transactionId)

        case WriterDied(t) => throw t // Nothing more can be written: the CRL dies with its writer

        case HandlerFailed(t) => throw t

        case WriteComplete(serialNumber) =>
          logger.trace(s"CRL write complete for entry serial number: ${serialNumber}")

          val written = inFlight
          writeInProgress = false
          inFlight = None
          val barriers = afterInFlight.reverse
          afterInFlight = Nil
          // Nothing is in flight: if that was a head, the ring may take a stream before the next write
          commitGrowthWhenQuiescent()
          // The next entry is issued at once, and the written one's handlers run on the completion thread meanwhile:
          // every entry up to the written one is durable, which is all a handler relies on (C2)
          startWrite()
          complete: () =>
            val start = System.nanoTime()
            written.foreach(_.runCompletionHandlers())
            barriers.foreach(_())
            BackendTiming.report.record("crl handlers", System.nanoTime() - start,
              written.fold(0)(_.completionHandlerCount) + barriers.size)

        case GrowthReady(streamNumber) =>
          pendingGrowth = Some(streamNumber)
          commitGrowthWhenQuiescent()

        case Barrier(handler) =>
          // Every earlier request is in an unwritten entry, in the entry being written, or already durable
          whenWritten(handler)
          startWrite()

        case GetRecoveryState(storeId, completion) =>
          onGetRecoveryState(storeId, completion)

        case GetStoresWithEntries(completion) =>
          completion((transactions.keysIterator ++ stubs.keysIterator).map(_.storeId).toSet)

        case RunOnCrlThread(f) => f()

        case Shutdown(completionHandler) =>
          // Every handler already handed over runs first (they must not block), so what is durable is answered as
          // such; everything else, unwritten or in flight, is never answered and fails with CrlClosed
          completions.shutdown()
          completions.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
          closed = true
          failPending(() => CrlClosed())
          shutdownWriter(completionHandler)
          return

  /** Commits a pending growth unless a rotation is under way: between a rotation's evacuation entry and its head
    * (rotationsWithoutHead counts from the one to the other, W6), the stream after the active one may still hold
    * pieces the evacuation in flight is moving, and the count of rotations must run over one ring */
  private def commitGrowthWhenQuiescent(): Unit =
    pendingGrowth.foreach: n =>
      if rotationsWithoutHead == 0 then
        pendingGrowth = None
        commitGrowth(n)

  /** Inserts stream `n`, preallocated and renamed into place by the growth thread, right after the active stream A.
    * The rewrite of the SizingFile is the commit point (crl/CLAUDE.md, growth) and the last step that touches the
    * files: only once it is durable does the writer know of the stream, and only then is the limit raised. So a crash
    * before it leaves an unlisted `<n>.crl`, which the next open removes (removeUncommittedGrowth), and a crash after
    * it a listed, all-zero stream, which recovery reads as holding no entry.
    *
    * The invariant holds after the insert: the stream after A is now `n`, which holds no entry at all, and A's old
    * successor, now after `n`, holds nothing of the image: A's entry evacuated it, the rotation is complete
    * (commitGrowthWhenQuiescent), and pieces are written only into A. So the rotation into `n` carries nothing,
    * and the rotation after it enters a stream holding nothing needed, as every rotation does. Lemma A does not
    * depend on the ring size. Lemma B, for the n + 1 streams, needs the live bytes within n · (S/2 − H): they are
    * within the old ring's (n − 1) · (S/2 − H), as admitted under the old limit, until the listeners raise it, after
    * the commit. W6 counts n rotations from the next head on, over the new ring */
  private def commitGrowth(n: Int): Unit =
    if n != sizing.numStreams then
      // One growth at a time, numbered by the ring it was started from: never another number
      logger.error(s"CRL growth of stream $n does not match the ring of ${sizing.numStreams} streams. Ignoring it")
      endGrowthAttempt(backOff = true)
    else
      val file = streamsDir / s"$n.crl"
      val sizingFile = streamsDir / SizingFile
      val active = StreamId(currentStream)
      val grown = sizing.copy(ring = sizing.ring.patch(sizing.ring.indexOf(active) + 1, List(StreamId(n)), 0))
      val committed =
        try
          DurableFiles.write(sizingFile, grown.encode)
          true
        catch case NonFatal(e) =>
          // Before its rename the old file stands, and the stream is unlisted: removed, the CRL as it was. After it,
          // only the directory's force failed: the new ring is named but may not be durable, and nothing may be
          // written to the stream until it is. One more force decides; if it fails the CRL fails (it throws here),
          // and the next open finds either ring, both of which it opens
          val onDisk = Sizing.decode(os.read(sizingFile))
          if onDisk.contains(sizing) then
            logger.warn(s"CRL growth in $streamsDir failed writing its $SizingFile. Removing ${file.last}", e)
            try DurableFiles.remove(file)
            catch case NonFatal(r) => logger.warn(s"CRL growth could not remove ${file.last}: it is removed at open", r)
            endGrowthAttempt(backOff = true)
            false
          else if onDisk.contains(grown) then
            DurableFiles.syncDirectory(streamsDir)
            true
          else
            throw IllegalStateException(s"CRL $SizingFile in $streamsDir is unreadable after a failed growth", e)
      if committed then
        writer.addStream(StreamId(n), file)
        streams += new Stream(StreamId(n), crlId, writer, UUID.randomUUID(), 0)
        sizing = grown
        ringSize = grown.numStreams
        val limit = admissionLimitBytes
        logger.info(s"CRL in $streamsDir grew to ${grown.numStreams} streams " +
          s"(ring ${grown.ring.map(_.number).mkString(",")}): admission limit $limit bytes")
        limitListeners.forEach: f =>
          try f(limit)
          catch case NonFatal(e) => logger.error("CRL admission limit listener failed", e)
        endGrowthAttempt(backOff = false)

  /** Ends a growth attempt: the next request starts another, no sooner than `retryAfter` after a refusal or failure */
  private def endGrowthAttempt(backOff: Boolean): Unit =
    if backOff then
      growthAllowedAt = System.nanoTime() + growth.retryAfter.toNanos
    growthAttemptsEnded.incrementAndGet()
    growthBusy.set(false)

  private def growthLoop(): Unit =
    try
      while !growthStopped do
        growthRequests.acquire()
        if !growthStopped then
          attemptGrowth()
    catch
      case _: InterruptedException => () // Stopped
      case t: Throwable =>
        if !growthStopped then
          logger.error(s"CRL growth thread of $streamsDir died: the CRL no longer grows", t)

  /** Runs `step` unless growth has stopped, under growthFileLock: false, and nothing done, once it has */
  private def growthStep(step: => Unit): Boolean =
    growthFileLock.synchronized:
      if growthStopped then
        false
      else
        step
        true

  /** Steps 1 and 2 of a growth (crl/CLAUDE.md, growth), on the growth thread: checks the policy, preallocates
    * `<n>.crl.tmp`, forced, and renames it to `<n>.crl` with the directory forced. The CRL thread commits it.
    * A refusal or failure leaves the CRL as it was; a failure removes what it created, and never fails the CRL */
  private def attemptGrowth(): Unit =
    val n = ringSize
    val tmp = streamsDir / s"$n.crl.tmp"
    val file = streamsDir / s"$n.crl"
    try
      if n >= growth.maxStreams then
        endGrowthAttempt(backOff = false) // requestGrowth refuses these: it read the ring size before a commit
      else
        val capacity = growth.capacity()
        val free = growth.freeSpace()
        val floor = capacity * growth.minFreePercent / 100
        if free - maxSizeInBytes < floor then
          logger.warn(s"CRL in $streamsDir is full, but a stream of $maxSizeInBytes bytes would leave $free bytes " +
            s"free below ${growth.minFreePercent}% of $capacity (crl-min-free-percent): not growing")
          endGrowthAttempt(backOff = true)
        else if growthStep({ inPreallocationForTest(); FilePreallocation.preallocate(tmp, maxSizeInBytes) }) &&
                growthStep({ os.move(tmp, file, atomicMove = true); DurableFiles.syncDirectory(streamsDir) }) then
          ioQueue.put(GrowthReady(n))
        // Otherwise growth stopped: the attempt is abandoned, and the next open removes what it left
    catch
      case _: InterruptedException => () // Stopped: abandoned
      case NonFatal(_) if growthStopped => () // Interrupted by the stop (ClosedByInterruptException): abandoned
      case NonFatal(e) =>
        logger.warn(s"CRL growth in $streamsDir failed. Retrying no sooner than ${growth.retryAfter}", e)
        // Nothing was handed to the CRL thread, so `<n>.crl` is unlisted
        try
          growthStep({
            List(tmp, file).foreach(f => os.remove(f, checkExists = false))
            DurableFiles.syncDirectory(streamsDir)
          })
        catch case NonFatal(r) => logger.warn(s"CRL growth could not remove its files: they are removed at open", r)
        endGrowthAttempt(backOff = true)

  override def requestGrowth(): Unit =
    if growthThread.nonEmpty && !growthStopped then
      if ringSize >= growth.maxStreams then
        if loggedGrowthCap.compareAndSet(false, true) then
          logger.warn(s"CRL in $streamsDir is full and at its stream cap ($ringSize, crl-max-streams): not growing")
      else if System.nanoTime() - growthAllowedAt >= 0 && growthBusy.compareAndSet(false, true) then
        growthRequests.release()

  override def onAdmissionLimitRaised(f: Long => Unit): Unit = limitListeners.add(f)

  /** Tests only: whether a growth attempt is under way, how many have ended, and whether the growth thread runs */
  private[simple] def growthBusyForTest: Boolean = growthBusy.get
  private[simple] def growthAttemptsEndedForTest: Int = growthAttemptsEnded.get
  private[simple] def growthThreadAliveForTest: Boolean = growthThread.exists(_.isAlive)

  /** Tests only, on the CRL thread: the room left in the active stream, and the number of requests queued */
  private[simple] def activeStreamRoomForTest: Long = streams(currentStream).room
  private[simple] def queuedRequestsForTest: Int = ioQueue.size
  /** Tests only, on the CRL thread: nothing is unwritten, and the last write's completion has been processed */
  private[simple] def idleForTest: Boolean = !writeInProgress && sealedEntries.isEmpty && currentLogEntry.isEmpty

  /** Each id's deletions go to the newest unwritten entry, which is sealed when they would overflow it (W1), and the
    * handler to the last: it runs once every deletion is durable. `resavable` deletions are cancelled by a save of the
    * same id later in their entry (LogEntry.deleteTxUnlessResaved) */
  private def onDeleteTransactions(storeId: StoreId, transactionIds: List[TransactionId], resavable: Boolean,
                                   completionHandler: () => Unit): Unit =
    if transactionIds.isEmpty then
      complete(completionHandler)
    else
      transactionIds.foreach: transactionId =>
        val txid = TxId(storeId, transactionId)
        val entry = entryFor(_.entrySizeWith(2 * TxId.StaticSize))
        if resavable then
          entry.deleteTxUnlessResaved(txid)
        else
          entry.deleteTx(txid)
        transactions -= txid
        entry.deleteStub(txid)
        stubs -= txid
      currentLogEntry.addCompletionHandler(completionHandler)
      startWrite()

  /** Leaves the transactions live: only a deletion, which is written, takes them out of the log */
  private def onGetRecoveryState(storeId: StoreId,
                                 completion: StoreRecoveryState => Unit
                                ): Unit =
    completion(StoreRecoveryState(
      transactions.valuesIterator.filter(_.state.storeId == storeId).map(_.state).toList,
      stubs.iterator.filter(_._1.storeId == storeId).map((id, start) => id.transactionId -> start).toMap))

  /** Replaces the transaction state held for the id, if any, in the same entry: its deletion and the stub are durable
    * together (W1) */
  private def onSaveAppliedStub(storeId: StoreId, transactionId: TransactionId, start: HLCTimestamp,
                                completionHandler: () => Unit): Unit =
    val txid = TxId(storeId, transactionId)
    val replaces = transactions.contains(txid)
    val entry = entryFor(_.entrySizeWith(Stub.StaticSize + (if replaces then TxId.StaticSize else 0)))
    if replaces then
      transactions -= txid
      entry.deleteTxUnlessResaved(txid)
    val held = stubs.getOrElse(txid, start)
    stubs += (txid -> held)
    entry.addStub(txid, held, completionHandler)
    startWrite()

  private def onDeleteAppliedStub(storeId: StoreId, transactionId: TransactionId): Unit =
    val txid = TxId(storeId, transactionId)
    if stubs.contains(txid) then
      stubs -= txid
      entryFor(_.entrySizeWith(TxId.StaticSize)).deleteStub(txid)
      startWrite()

  /** Whether an unwritten entry, the one in flight included, deletes `txid` finally (W1b). The save is then skipped,
    * creating no record, and `completionHandler` runs once that deletion is durable */
  private def skippedAfterFinalDeletion(txid: TxId, completionHandler: () => Unit): Boolean =
    if currentLogEntry.deletesFinally(txid) || sealedEntries.exists(_.deletesFinally(txid)) then
      whenWritten(completionHandler)
      true
    else if inFlight.exists(_.deletesFinally(txid)) then
      afterInFlight = completionHandler :: afterInFlight
      true
    else
      false

  private def onSaveTransaction(transactionId: TransactionId,
                      state: TransactionRecoveryState,
                      completionHandler: () => Unit): Unit =
    val txid = TxId(state.storeId, transactionId)
    // Deleted in an unwritten entry: the save is skipped but still reports success once the deletion is durable. No
    // record is created for it: readStoreState would report it live, and it would save a state whose admission was
    // already released (crash-recovery-log C9, W1b). Every deletion removes the id from `transactions` too, so no
    // record exists here
    if skippedAfterFinalDeletion(txid, completionHandler) then
      startWrite()
      return
    val existing = transactions.get(txid)
    val form = TxForm(state, keepUpdates = existing.forall(_.keepObjectUpdates))

    // The entry takes this form as of now (W1a). It writes the pieces when the record has none yet, when the form it
    // replaces in the same entry was to write them, or when the update count changes (W4: the txd too, so every
    // record's pieces lie in one entry). Otherwise it names those an earlier entry wrote or will write
    def saveInCurrent(): TxSave = existing match
      case None => TxSave(form, writesPieces = true)
      case Some(tx) =>
        val inherits = tx.latestEntry == currentEntryNumber && tx.latestWritesPieces
        val countChanged = form.keepUpdates && form.updateCount != tx.latest.updateCount
        TxSave(form, inherits || countChanged)

    val entry = entryFor(_.entrySizeWith(txid, saveInCurrent()))
    val save = saveInCurrent()

    val tx = existing.getOrElse:
      val tx = new LiveTx(state)
      transactions += (txid -> tx)
      tx
    tx.state = state
    tx.latest = form
    tx.latestWritesPieces = save.writesPieces
    tx.latestEntry = currentEntryNumber

    logger.trace(s"CRL adding transaction $txid to the newest unwritten entry")
    entry.addTx(txid, save, completionHandler)
    startWrite()

  private def onDeleteTransaction(storeId: StoreId, transactionid: TransactionId): Unit =
    val txid = TxId(storeId, transactionid)
    entryFor(_.entrySizeWith(TxId.StaticSize)).deleteTx(txid)
    transactions -= txid
    startWrite()

  /** Writes nothing: forms taken from now on leave the data out. Forms already taken keep it, and so does the
    * last-built form, which an evacuation carries, until the record's next form is built (W1a, W5) */
  private def onDropTransactionObjectData(storeId: StoreId, transactionid: TransactionId): Unit =
    transactions.get(TxId(storeId, transactionid)).foreach(_.keepObjectUpdates = false)

  def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = blockingRead(storeId).transactions

  def getAppliedStubs(storeId: StoreId): Map[TransactionId, HLCTimestamp] = blockingRead(storeId).appliedStubs

  private def blockingRead(storeId: StoreId): StoreRecoveryState =
    scala.concurrent.Await.result(readStoreState(storeId), scala.concurrent.duration.Duration.Inf)

  def save(transactionId: TransactionId,
           state: TransactionRecoveryState,
           completionHandler: () => Unit): Unit =
    ioQueue.put(SaveTransaction(transactionId, state, completionHandler))

  def deleteTransaction(storeId: StoreId, transactionid: TransactionId): Unit =
    ioQueue.put(DeleteTransaction(storeId, transactionid))

  def dropTransactionObjectData(storeId: StoreId, transactionid: TransactionId): Unit =
    ioQueue.put(DropTransactionData(storeId, transactionid))

  def saveAppliedStub(storeId: StoreId, txid: TransactionId, start: HLCTimestamp, completionHandler: () => Unit): Unit =
    ioQueue.put(SaveAppliedStub(storeId, txid, start, completionHandler))

  override def whenDurable(storeId: StoreId, handler: () => Unit): Unit = ioQueue.put(Barrier(handler))

  def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit =
    ioQueue.put(DeleteAppliedStub(storeId, txid))

  override def deleteTransactions(storeId: StoreId, txids: List[TransactionId]): Future[Unit] =
    tracked(p => DeleteTransactions(storeId, txids, () => p.trySuccess(())))

  override def readStoreState(storeId: StoreId): Future[StoreRecoveryState] =
    tracked(p => GetRecoveryState(storeId, state => p.trySuccess(state)))

  /** One request on the CRL thread, so it drops exactly what was saved before it (crash-recovery-log C14) */
  override def dropStore(storeId: StoreId): Future[Unit] =
    tracked(p => DropStore(storeId, () => p.trySuccess(())))

  override def storesWithEntries(): Future[Set[StoreId]] =
    tracked(p => GetStoresWithEntries(stores => p.trySuccess(stores)))

  /** As the default, with every save tracked, so a death fails the load rather than leaving it waiting */
  override def loadStore(storeId: StoreId, state: StoreRecoveryState): Future[Unit] =
    given scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.parasitic
    // Stubs first: a stub's save replaces a transaction's state of the same id, which a later save then keeps
    Future.sequence(state.appliedStubs.toList.map: (txid, start) =>
      tracked[Unit](p => SaveAppliedStub(storeId, txid, start, () => p.trySuccess(())))
    ).flatMap: _ =>
      Future.sequence(state.transactions.map: trs =>
        tracked[Unit](p => SaveTransaction(trs.txd.transactionId, trs, () => p.trySuccess(())))
      ).map(_ => ())

  /** Tests only: runs `f` on the CRL thread once every request queued before it is processed */
  private[simple] def runOnCrlThread(f: () => Unit): Unit = ioQueue.put(RunOnCrlThread(f))

  override def failed: Future[Throwable] = death.future

  /** Idempotent: a later call waits for the first one's shutdown, as a second Shutdown would find no thread. Every
    * Future not answered by then, and every later one, fails with CrlClosed (CRL thread, `Shutdown`) */
  override def shutdown(): Unit =
    if shutdownRequested.compareAndSet(false, true) then
      ioQueue.put(Shutdown(() => shutdownComplete.countDown()))
    shutdownComplete.await()
