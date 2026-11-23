package org.aspen_ddp.aspen.server

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.host.{DeviceIo, DeviceIoObserver, JobHandle, JobKind}

/** A deterministic DeviceIo for tests that drive a BackendOpener or a RebuildingStore without a host (a host owns its
 *  own, `DeviceControl.deviceIos`): `submit` runs the job in the caller before returning, so the Future is completed
 *  and a yield loop needs no real-time wait. It owns no threads, so nothing leaks; `dataEc` and `serialEc` are the
 *  global pool. */
final class TestDeviceIo(val path: os.Path) extends DeviceIo:
  val observer: DeviceIoObserver = new DeviceIoObserver
  def label: String = "test"
  def setDeviceId(id: StorageDeviceId): Unit = ()
  def dataEc: ExecutionContext = ExecutionContext.global
  def serialEc: ExecutionContext = ExecutionContext.global
  def beginRead(): Unit = ()
  def endRead(): Unit = ()
  def readsOverBound: Boolean = false
  def close(): Unit = ()

  def submit[T](kind: JobKind, store: Option[StoreId])(job: => T): JobHandle[T] =
    val result = Future.fromTry(Try(job))
    new JobHandle[T]:
      def future: Future[T] = result
      def cancel(): Boolean = false

object TestDeviceIo:
  def of(path: os.Path): DeviceIo = new TestDeviceIo(path)
