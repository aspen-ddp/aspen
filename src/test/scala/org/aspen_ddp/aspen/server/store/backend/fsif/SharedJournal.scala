package org.aspen_ddp.aspen.server.store.backend.fsif

import java.util.UUID

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.Random

import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.host.DeviceIo
import org.aspen_ddp.aspen.server.store.backend.{RetryScheduler, SharedRetryScheduler}

/** One simulated machine (a SimulatedFsifFiles) holding a device journal under `fj/` and FSIF stores under `s<idx>/`,
  * so one `crash()` covers the journal and every store, as a power cut covers a device.
  *
  * The journal is built on first use after construction, `crash()` or `reload()`: DeviceJournalRecovery runs over
  * every store directory that holds an index (a device load), and `lastRecovery` keeps its result. Its writes run
  * through `run`, read at each build (default: at once, on the appending thread); a write task handed to `run` by an
  * earlier build is dropped, so a crashed incarnation's journal writes nothing more.
  */
final class SharedJournal(rnd: Random, val sizing: JournalSizing = SharedJournal.TestSizing):
  private var sim = new SimulatedFsifFiles(rnd)
  private var current: Option[FsifDeviceJournal] = None
  private var build = 0
  private val known = mutable.SortedSet[Int]()
  private val layouts = mutable.Map[Int, FsifLayout]()

  var run: Runnable => Unit = r => r.run()
  /** Wraps the journal's files at each build (a test probing the machine's I/O) */
  var wrapJournal: FsifFiles => FsifFiles = f => f
  var lastRecovery: Option[DeviceJournalRecovery.Result] = None

  def this(seed: Long) = this(new Random(seed))

  def machine: SimulatedFsifFiles = sim

  def journalFiles: FsifFiles = new PrefixedFsifFiles(sim, "fj/")

  def storeFiles(idx: Int): SharedJournal.StoreFiles = new SharedJournal.StoreFiles(sim, s"s$idx/")

  /** The directory a store registers with the journal (only a key: the files are simulated) */
  def path(idx: Int): os.Path = os.root / "shared-journal" / s"s$idx"

  def initialize(idx: Int, layout: FsifLayout): Unit =
    this.layout(idx, layout)
    FsifRecovery.initialize(storeFiles(idx), layout)

  /** The running journal, recovering the machine first if none is */
  def journal: FsifDeviceJournal = current.getOrElse:
    val targets = known.toSeq.filter(i => FsifRecovery.IndexFiles.exists(storeFiles(i).exists)).map: i =>
      DeviceJournalRecovery.Target(path(i), storeFiles(i), layouts.getOrElse(i, SharedJournal.DefaultLayout))
    val jfiles = wrapJournal(journalFiles)
    val result = DeviceJournalRecovery.recover(jfiles, sizing, targets)
    lastRecovery = Some(result)
    build += 1
    val mine = build
    val runner = run
    val j = new FsifDeviceJournal(jfiles, sizing, result.start, r => if build == mine then runner(r),
      forceAfterWrite = true)
    current = Some(j)
    j

  /** Records `layout` for store `idx` (the device load replays it with that layout) */
  def layout(idx: Int, layout: FsifLayout): Unit =
    known += idx
    layouts(idx) = layout

  def open(idx: Int, layout: FsifLayout, io: ExecutionContext, tuning: FsifTuning = FsifTuning(),
           onFatal: Throwable => Unit = _ => (), scheduler: RetryScheduler = SharedRetryScheduler,
           serialEc: Option[ExecutionContext] = None, readGate: Option[DeviceIo] = None,
           wrap: FsifFiles => FsifFiles = f => f, storeId: StoreId = SharedJournal.storeIdOf(0)): FsifBackend =
    this.layout(idx, layout)
    new FsifBackend(storeId, path(idx), layout, wrap(storeFiles(idx)), io, journal, tuning, onFatal, scheduler,
      serialEc, readGate)

  /** Power cut: the machine keeps its durable image (SimulatedFsifFiles.crash), and the next use of `journal` is a
    * device load. Backends of the earlier incarnation must not be used again.
    */
  def crash(): Unit =
    sim = sim.crash()
    current = None
    build += 1

  /** A copy of the machine's durable image with the same stores, no journal running yet and no faults; this one stays
    * usable (SimulatedFsifFiles.snapshot)
    */
  def snapshot(): SharedJournal =
    val copy = new SharedJournal(new Random(0), sizing)
    copy.sim = sim.snapshot()
    copy.known ++= known
    copy.layouts ++= layouts
    copy

  /** A device reload without a crash: the journal is closed and the next use of `journal` recovers it */
  def reload(): Unit =
    current.foreach(_.close())
    current = None
    build += 1

object SharedJournal:
  /** 3 files of 256 KiB: room for any test layout's commit, small enough to wrap in a test */
  val TestSizing: JournalSizing = JournalSizing(new UUID(0x5a, 0x4a), 3, 256 * 1024)
  val DefaultLayout: FsifLayout = FsifLayout(1024, 64)

  def storeIdOf(idx: Int): StoreId = StoreId(PoolId(new UUID(7, 7)), idx.toByte)

  /** One store's directory on the machine: its files, and the machine's fault and crash hooks with names relative to
    * it
    */
  final class StoreFiles(sim: SimulatedFsifFiles, prefix: String) extends FsifFiles:
    def open(name: String): FsifFile = sim.open(prefix + name)
    def size(name: String): Long = sim.size(prefix + name)
    def exists(name: String): Boolean = sim.exists(prefix + name)
    private def mine(file: String => Boolean): String => Boolean =
      n => n.startsWith(prefix) && file(n.drop(prefix.length))
    def failNext(fault: Fault, file: String => Boolean = _ => true,
                 ops: Set[FaultOp] = FaultOp.values.toSet): Unit = sim.failNext(fault, mine(file), ops)
    def failWhile(fault: Fault, file: String => Boolean = _ => true,
                  ops: Set[FaultOp] = FaultOp.values.toSet)(p: => Boolean): Unit = sim.failWhile(fault, mine(file), ops)(p)
    def failReads(n: Int, file: String => Boolean = _ => true): Unit = sim.failReads(n, mine(file))
    def faultCount(file: String, op: FaultOp): Int = sim.faultCount(prefix + file, op)
    /** Machine-wide, as SimulatedFsifFiles' */
    def crashAfterOps(n: Int): Unit = sim.crashAfterOps(n)
    def opCount: Long = sim.opCount

  /** A journal over real files in `dir` (created there if absent), writing at once on the caller's thread; for suites
    * whose stores are real directories
    */
  def nio(dir: os.Path, sizing: JournalSizing = TestSizing): FsifDeviceJournal =
    if !os.exists(dir) then
      DeviceJournalFiles.create(dir, sizing)
    val s = DeviceJournalFiles.readSizing(dir)
    val files = new NioFsifFiles(dir)
    val start = DeviceJournalScan.scan(files, s) match
      case Scan.Valid(_, tail) => Some(tail)
      case _ => None
    new FsifDeviceJournal(files, s, start, r => r.run(), forceAfterWrite = true)
