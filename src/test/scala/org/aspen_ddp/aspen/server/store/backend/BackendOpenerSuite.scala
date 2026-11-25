package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.FileBasedTests
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision,
  ObjectType}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.server.{FileStat, StorageDeviceConfig, StoreConfig}
import org.aspen_ddp.aspen.server.host.{DeviceIo, JobKind, ThreadedDeviceIo}
import org.aspen_ddp.aspen.server.store.backend.fsif.{DeviceJournalFiles, DeviceJournalFormat, FsifBackend,
  JournalSizing}

import java.io.IOException

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
    finally
      io.fsifJournal.get.foreach(_.close())
      io.close()

  /** The device config a create reads from the device directory (`tdir`, the parent of its creating/) */
  private def writeDeviceConfig(): Unit =
    os.write.over(tdir / StorageDeviceConfig.configFilename,
      StorageDeviceConfig(StorageDeviceId(new UUID(9, 4)), new UUID(9, 5)).yamlConfig)

  test("opening an FSIF store with no journal open fails with an IOException, not BackendStateLost"):
    withIo: io =>
      val cfg = StoreConfig(storeId, StoreConfig.Fsif(1024, 64), None, Replication(3, 2))
      val path = tdir / "fsif"
      DefaultBackendOpener.createStore(cfg, path, tdir / StoreCreation.CreatingDirectory)
      // A journal directory whose sizing cannot be read: no journal can be opened for the device
      os.makeDir.all(tdir / DeviceJournalFiles.DirName)
      os.write(tdir / DeviceJournalFiles.DirName / DeviceJournalFiles.SizingName, "garbage")
      val e = intercept[Throwable](Await.result(opener.open(cfg, path, _ => (), io), Timeout))
      e shouldBe a[IOException]
      e should not be a[BackendStateLost]
      io.fsifJournal.get should be(None)

  test("Review Focus 3: an FSIF create whose slice cannot fit a journal file fails with nothing at the path; an open of such a store fails with an IOException naming both sizes"):
    withIo: io =>
      writeDeviceConfig()
      val fileSize = 64L * 1024
      DeviceJournalFiles.create(tdir / DeviceJournalFiles.DirName, JournalSizing(new UUID(9, 6), 3, fileSize))
      val slice = 128 * 1024
      val cfg = StoreConfig(storeId, StoreConfig.Fsif(slice, 4), None, Replication(3, 2))
      val path = tdir / "big"
      val e = intercept[Throwable](
        Await.result(opener.create(cfg, path, tdir / StoreCreation.CreatingDirectory, io), Timeout))
      e shouldBe an[IllegalArgumentException]
      FileStat.exists(path) should be(false)
      // Made without the check, as a store of an earlier journal or a transfer in would be
      DefaultBackendOpener.createStore(cfg, path, tdir / StoreCreation.CreatingDirectory)
      val o = intercept[Throwable](Await.result(opener.open(cfg, path, _ => (), io), Timeout))
      o shouldBe an[IOException]
      o should not be a[BackendStateLost]
      val needed = FsifBackend.appendBound(slice) + FsifBackend.MappingAppendSize
      o.getMessage should include(needed.toString)
      o.getMessage should include((fileSize - DeviceJournalFormat.HeaderSize).toString)

  test("an FSIF create opens the device journal first, and a fitting store is then opened against it"):
    withIo: io =>
      writeDeviceConfig()
      val cfg = StoreConfig(storeId, StoreConfig.Fsif(1024, 64), None, Replication(3, 2))
      val path = tdir / "fsif"
      Await.result(opener.create(cfg, path, tdir / StoreCreation.CreatingDirectory, io), Timeout)
      val journal = io.fsifJournal.get
      journal should not be empty
      FileStat.isDir(tdir / DeviceJournalFiles.DirName) should be(true)
      val backend = Await.result(opener.open(cfg, path, _ => (), io), Timeout)
      io.fsifJournal.get should be(journal)
      Await.result(backend.close(), Timeout)

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
