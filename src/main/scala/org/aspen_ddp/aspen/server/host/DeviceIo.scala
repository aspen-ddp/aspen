package org.aspen_ddp.aspen.server.host

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

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
   *  `CrlOpen`, used by discovery only */
  case DeviceListing, DeviceProbe, LoadSweep, LoadDuplicates,
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
  def observer: DeviceIoObserver
  def beginRead(): Unit
  def endRead(): Unit
  def readsOverBound: Boolean
  /** No management job queued or running, and no data or serial task queued or running: closing now drops nothing
   *  (DeviceIos.forget). A snapshot: true only while nothing submits to this DeviceIo, which holds for a path with no
   *  device loaded and no control step outstanding on it */
  def idle: Boolean
  /** Closes the management lane only (DeviceIos.forget): every lane job queued fails with `Cancelled`, and every later
   *  submit at once. The data and serial pools keep running whatever is still executed on them (a rebuild's staging
   *  backend, an FSIF serial executor, a late close that outlives its device's listing), on this device's threads,
   *  which time out once idle; so no backend task is ever rejected and no Future chained on one left incomplete */
  def closeLane(): Unit
  /** Host shutdown (DeviceIos.closeAll): `closeLane`, and both pools stopped without waiting */
  def close(): Unit

object DeviceIo:
  object Cancelled extends Exception("device io job cancelled")

  /** Data threads per device by default: half the CPUs, at least 4. Measured against the host-wide FSIF pool this
   *  replaced (max(4, CPUs) shared threads): 4 per device lost 15 to 25 percent on a single busy device, CPUs/2 matched
   *  or beat it (docs/host-threading-measurement.md, "Device I/O isolation"). */
  def defaultDataThreads: Int = math.max(4, Runtime.getRuntime.availableProcessors / 2)

  /** `poolKeepAliveMillis`: how long an idle data or serial thread lives before it exits (a device with no backend
   *  work holds no pool thread) */
  final case class Config(dataThreads: Int = defaultDataThreads, serialThreads: Int = 1, readBound: Int = 4096,
                          poolKeepAliveMillis: Long = 30000)

/** Thread-safe record of the jobs a device is running. */
final class DeviceIoObserver:
  private final case class Entry(kind: String, store: Option[StoreId], bytes: Long, startNanos: Long)

  private val tokens = new AtomicLong(0)
  private val running = new ConcurrentHashMap[Long, Entry]()
  private val done = new ConcurrentHashMap[String, LongAdder]()

  def started(kind: String, store: Option[StoreId], bytes: Long): Long =
    val token = tokens.incrementAndGet()
    running.put(token, Entry(kind, store, bytes, System.nanoTime()))
    token

  def finished(token: Long, ok: Boolean): Unit =
    val e = running.remove(token)
    if e != null then
      done.computeIfAbsent(e.kind, _ => new LongAdder).increment()

  /** kind -> (count, oldest start in nanoTime) */
  def inFlight: Map[String, (Int, Long)] =
    running.values.asScala.groupBy(_.kind).map: (k, es) =>
      k -> (es.size, es.map(_.startNanos).min)

  def completed: Map[String, Long] =
    done.asScala.map((k, v) => k -> v.sum).toMap
