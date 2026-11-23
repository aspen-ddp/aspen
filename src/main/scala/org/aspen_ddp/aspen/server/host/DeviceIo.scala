package org.aspen_ddp.aspen.server.host

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

enum JobKind:
  case Scan, ReadConfig, Markers, CopyLayout, Move, Retire, Release, Delete, Sweep, SaveFile, CrlOpen, CrlCreate,
    BackendOpen, BackendCreate, Usage, Staging, ReconcileInputs
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
  def close(): Unit

object DeviceIo:
  object Cancelled extends Exception("device io job cancelled")

  final case class Config(dataThreads: Int = 4, serialThreads: Int = 1, readBound: Int = 4096)

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
