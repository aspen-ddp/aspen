package org.aspen_ddp.aspen.server.host

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicReference, LongAdder}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.backend.fsif.FsifDeviceJournal

enum JobKind:
  case Scan, ReadConfig, Markers, CopyLayout, Move, Retire, Release, Delete, Sweep, SaveFile, CrlOpen, CrlCreate,
    BackendOpen, BackendCreate, Usage, Staging, ReconcileInputs,
    /** A store load's reads before its Store is built (StoreLoader.handleLoad, LoadDisk.readLoadInputs) */
    LoadInputs,
    /** StoreRebuilds' bookkeeping around a rebuild (RebuildBookkeepingDisk): staging removals, a checkpoint's drop, the
     *  listing of a store's copies at its rebuild's finish */
    RebuildBookkeeping
  /** Device discovery's own jobs (DeviceDiscovery), used by nothing else: the listing of storage-devices/ on the host
   *  root's lane, a path's probe, and a load's sweep and P2. A load's device CRL open (DeviceCrlDisk.open) is
   *  `CrlOpen`, used by discovery only; its FSIF device journal open (FsifJournalDisk.openAtLoad) is `FsifJournalOpen` */
  case DeviceListing, DeviceProbe, LoadSweep, LoadDuplicates, FsifJournalOpen,
    /** Ownership resolution's reads (DeviceStateDisk.storesIn, ownershipUnchanged) */
    DeviceOwnership,
    /** Adoption's reads and moves, and bringIntoService's steps (DeviceStateDisk) */
    Adoption
  case Other(name: String)

  def label: String = this match
    case Other(n) => n
    case k => k.toString

trait JobHandle[T]:
  def future: Future[T]
  /** True only if the job had not started. The future then fails with `DeviceIo.Cancelled`. */
  def cancel(): Boolean

/** Per-device I/O isolation: a serial management lane plus data and serial pools, all on threads owned by this
  * device so a hung device stalls only its own work.
  */
trait DeviceIo:
  def path: os.Path
  def label: String
  def setDeviceId(id: StorageDeviceId): Unit
  def submit[T](kind: JobKind, store: Option[StoreId])(job: => T): JobHandle[T]
  def dataEc: ExecutionContext
  def serialEc: ExecutionContext
  /** The device's FSIF journal thread (FsifDeviceJournal's `run`): one thread, each write of the journal one task on
   *  it, in order. A task counts toward `idle` as a pool task does, and the thread stops at `close` */
  def journalRun: Runnable => Unit
  /** The device's open FSIF journal, None while none is open. Set by the device load (FsifJournalDisk.openAtLoad,
   *  applied by DeviceDiscovery) or the lazy creation ahead of the first FSIF store on a device without one
   *  (FsifJournalDisk.ensure); cleared at the device's unload, wherever its CRL is shut, and closed with the CRL
   *  (DeviceShutdown.shutDownCrl) */
  def fsifJournal: AtomicReference[Option[FsifDeviceJournal]]
  def observer: DeviceIoObserver
  /** The device's health probe (HealthProbe): armed while a storage device is loaded at this path, disarmed at its
   *  unload, `closeLane` and `close`; never armed for the host root. Its attempts are recorded in `observer` but are
   *  no lane job or pool task, so they never keep `idle` false */
  def probe: HealthProbe
  def beginRead(): Unit
  def endRead(): Unit
  def readsOverBound: Boolean
  /** No management job queued or running, and no data or serial task queued or running: closing now drops nothing
   *  (DeviceIos.forget). A snapshot: true only while nothing submits to this DeviceIo, which holds for a path with no
   *  device loaded and no control step outstanding on it */
  def idle: Boolean
  /** Closes the management lane only (DeviceIos.forget): every lane job queued fails with `Cancelled`, and every later
   *  submit at once. The data, serial and journal pools keep running whatever is still executed on them (a rebuild's staging
   *  backend, an FSIF serial executor, a late close that outlives its device's listing), on this device's threads,
   *  which time out once idle; so no backend task is ever rejected and no Future chained on one left incomplete */
  def closeLane(): Unit
  /** Host shutdown (DeviceIos.closeAll): `closeLane`, and every pool stopped without waiting */
  def close(): Unit

object DeviceIo:
  object Cancelled extends Exception("device io job cancelled")

  /** Data threads per device by default: 4. With the FSIF device journal the data pool carries no journal writes, only
   *  reads, in-place writes, data forces and index writes. Measured with FsifDeviceBench (16 FSIF stores, depth 8, six
   *  10 s runs each, 2026-11-24): mean ops/s 2231 at 1 thread, 1532 at 2, 2638 at 4, 2409 at 8, 2246 at 16, 2644 at
   *  24; 1 and 2 threads fell to 288-578 in some runs as reads queued (22-50 ms), 4 never below 2449. The per-store
   *  journals before it needed 24 threads for 767 (docs/host-threading-measurement.md, "FSIF device journal"). */
  def defaultDataThreads: Int = 4

  /** `poolKeepAliveMillis`: how long an idle data or serial thread lives before it exits (a device with no backend
   *  work holds no pool thread). `probe`: the health probe's interval and hang threshold */
  final case class Config(dataThreads: Int = defaultDataThreads, serialThreads: Int = 1, readBound: Int = 4096,
                          poolKeepAliveMillis: Long = 30000, probe: HealthProbe.Config = HealthProbe.Config())

/** Thread-safe record of the jobs a device is running, each stamped with `clock` (nanoseconds; injectable so tests
 *  control every age). */
final class DeviceIoObserver(clock: () => Long = () => System.nanoTime()):
  private final case class Entry(kind: String, store: Option[StoreId], bytes: Long, startNanos: Long)

  private val tokens = new AtomicLong(0)
  private val running = new ConcurrentHashMap[Long, Entry]()
  private val done = new ConcurrentHashMap[String, LongAdder]()

  def started(kind: String, store: Option[StoreId], bytes: Long): Long =
    val token = tokens.incrementAndGet()
    running.put(token, Entry(kind, store, bytes, clock()))
    token

  def finished(token: Long, ok: Boolean): Unit =
    val e = running.remove(token)
    if e != null then
      done.computeIfAbsent(e.kind, _ => new LongAdder).increment()

  /** The observer's clock now, against which `inFlight`'s start times are aged */
  def now: Long = clock()

  /** kind -> (count, oldest start by `clock`) */
  def inFlight: Map[String, (Int, Long)] =
    running.values.asScala.groupBy(_.kind).map: (k, es) =>
      k -> (es.size, es.map(_.startNanos).min)

  def completed: Map[String, Long] =
    done.asScala.map((k, v) => k -> v.sum).toMap
