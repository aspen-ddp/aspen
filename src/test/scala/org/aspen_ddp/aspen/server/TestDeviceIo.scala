package org.aspen_ddp.aspen.server

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.host.{DeviceIo, DeviceIoObserver, HealthProbe, JobHandle, JobKind}
import org.aspen_ddp.aspen.server.store.backend.fsif.FsifDeviceJournal

/** A deterministic DeviceIo for tests that drive a BackendOpener or a RebuildingStore without a host (a host owns its
 *  own, `DeviceControl.deviceIos`): `submit` runs the job in the caller before returning, so the Future is completed
 *  and a yield loop needs no real-time wait. It owns no threads, so nothing leaks; `dataEc`, `serialEc` and `journalRun` are
 *  the global pool. */
final class TestDeviceIo(val path: os.Path, readBound: Int = Int.MaxValue) extends DeviceIo:
  /** Reads begun and not ended, as ThreadedDeviceIo counts them; over the bound once it reaches `readBound` */
  val reads = new java.util.concurrent.atomic.AtomicInteger(0)
  val observer: DeviceIoObserver = new DeviceIoObserver
  /** Never armed: no host loads a device on it */
  val probe: HealthProbe = new HealthProbe(path, HealthProbe.Config(), observer, () => "test-probe")
  def label: String = "test"
  def setDeviceId(id: StorageDeviceId): Unit = ()
  def dataEc: ExecutionContext = ExecutionContext.global
  def serialEc: ExecutionContext = ExecutionContext.global
  /** The global pool: the journal's writes are one at a time by its own rule, so order needs no single thread */
  def journalRun: Runnable => Unit = r => ExecutionContext.global.execute(r)
  val fsifJournal: java.util.concurrent.atomic.AtomicReference[Option[FsifDeviceJournal]] =
    new java.util.concurrent.atomic.AtomicReference(None)
  def beginRead(): Unit = reads.incrementAndGet()
  def endRead(): Unit = reads.decrementAndGet()
  def readsOverBound: Boolean = reads.get() >= readBound
  def idle: Boolean = true
  def close(): Unit = ()
  def closeLane(): Unit = ()

  def submit[T](kind: JobKind, store: Option[StoreId])(job: => T): JobHandle[T] =
    val result = Future.fromTry(Try(job))
    new JobHandle[T]:
      def future: Future[T] = result
      def cancel(): Boolean = false

object TestDeviceIo:
  def of(path: os.Path): DeviceIo = new TestDeviceIo(path)
