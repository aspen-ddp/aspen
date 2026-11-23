package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision,
  ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.host.{DeviceIo, JobKind, ThreadedDeviceIo}

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.CountDownLatch
import scala.concurrent.{Await, ExecutionContext, Promise}
import scala.concurrent.duration.*

/** Every open and create runs on the device's management lane, and an FSIF backend's I/O on the device's data pool */
class BackendOpenerSuite extends FileBasedTests:

  private val storeId = StoreId(PoolId(new UUID(9, 1)), 0)
  private val oid = ObjectId(new UUID(9, 2))
  private val ptr = DataObjectPointer(oid, storeId.poolId, ByteBuffer.allocate(4).putInt(3).array())
  private val txid = TransactionId(new UUID(9, 3))
  private val metadata = Metadata(ObjectRevision(txid), ObjectRefcount(1, 1), HLCTimestamp(1))
  private val Timeout = 20.seconds
  private val opener = new DefaultBackendOpener(ExecutionContext.global)

  /** The names of the threads that ran a task of the device, in order (the DeviceIo's gate sees every task) */
  private val ran = new java.util.concurrent.ConcurrentLinkedQueue[String]()

  private def withIo[A](f: DeviceIo => A): A =
    val io = new ThreadedDeviceIo(tdir, DeviceIo.Config(), () => ran.add(Thread.currentThread.getName))
    try f(io)
    finally io.close()

  test("an FSIF open runs on the device lane and its I/O on the device data pool"):
    withIo: io =>
      val cfg = StoreConfig(storeId, StoreConfig.Fsif(1024, 64), None, Replication(3, 2))
      val path = tdir / "fsif"
      DefaultBackendOpener.createStore(cfg, path, tdir / StoreCreation.CreatingDirectory)
      // Which thread the open runs on: the backend is built inside the lane job
      val openThread = Promise[String]()
      val probe = new DefaultBackendOpener(ExecutionContext.global):
        override def openFsifBackend(id: StoreId, p: os.Path, layout: org.aspen_ddp.aspen.server.store.backend.fsif.FsifLayout,
                                     onFatal: Throwable => Unit, dio: DeviceIo): Backend =
          openThread.trySuccess(Thread.currentThread.getName)
          super.openFsifBackend(id, p, layout, onFatal, dio)
      val backend = Await.result(probe.open(cfg, path, _ => (), io), Timeout)
      val name = Await.result(openThread.future, Timeout)
      name should startWith("dev-")
      name should endWith("-mgmt")

      val done = Promise[Unit]()
      backend.setCompletionHandler:
        new CompletionHandler:
          def complete(op: Completion): Unit = done.trySuccess(())
      backend.commit(CommitState(ptr, metadata, ObjectType.Data, DataBuffer(Array[Byte](1, 2, 3)), allocating = true), txid)
      Await.result(done.future, Timeout)
      // The commit's writes and forces ran on the device's data pool
      ran.toArray.map(_.toString).filter(_.contains("-io-")) should not be empty
      ran.toArray.map(_.toString).foreach(_ should startWith("dev-"))
      Await.result(backend.close(), Timeout)

  test("a RocksDB open never completes inline"):
    withIo: io =>
      val cfg = StoreConfig(storeId, StoreConfig.RocksDB(None), None, Replication(3, 2))
      val path = tdir / "rocks"
      DefaultBackendOpener.createStore(cfg, path, tdir / StoreCreation.CreatingDirectory)
      // A job blocking the lane keeps the open queued behind it
      val started = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val blocker = io.submit(JobKind.Scan, None):
        started.countDown()
        release.await()
      started.await()
      val opened = opener.open(cfg, path, _ => (), io)
      val created = opener.create(cfg, tdir / "rocks2", tdir / StoreCreation.CreatingDirectory, io)
      opened.isCompleted should be(false)
      created.isCompleted should be(false)
      release.countDown()
      Await.result(blocker.future, Timeout)
      Await.result(created, Timeout)
      Await.result(Await.result(opened, Timeout).close(), Timeout)
