package org.aspen_ddp.aspen.vdisk.bridge

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.{IDA, RandomAccessReedSolomon}
import org.aspen_ddp.aspen.vdisk.*

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.*

class BridgeIntegrationSuite extends IntegrationTestSuite:

  import TestBridgeClient.{Ops, Statuses}

  override def testIda: IDA = RandomAccessReedSolomon(5, 3, 4, 64)
  override def testPoolMaxSliceSize: Option[Int] =
    Some(RandomAccessReedSolomon(5, 3, 4, 64).minSliceSizeFor(16384).toInt)

  private val DiskSize = 8L * 1024 * 1024
  private val BlockSize = 4096

  /** A window long enough that writeback never runs before the test flushes */
  private val Held = VDiskConfig(coalesceWindow = 10.seconds)

  private val handles = ListBuffer[DiskHandle]()
  private val clients = ListBuffer[TestBridgeClient]()
  private var server: Option[BridgeServer] = None
  private var dir: Option[os.Path] = None

  override def subFixtureTeardown(): Unit =
    clients.foreach(_.close())
    clients.clear()
    server.foreach(_.stop())
    server = None
    dir.foreach(os.remove.all)
    dir = None
    handles.foreach(_.close())
    handles.clear()

  private def createDisk(): Future[DiskInfo] =
    client.getStoragePool(Radicle.poolId).flatMap: pool =>
      DiskCatalog.create(client, "d", new PoolObjectAllocator(client, pool), DiskSize)

  private def startServer(): os.Path =
    val d = os.temp.dir(prefix = "vdb")
    dir = Some(d)
    val path = d / "s.sock"
    val s = new BridgeServer(path, name => DiskHandle.open(client, name, Held).map: h =>
      synchronized(handles += h)
      h)
    s.start()
    server = Some(s)
    path

  private def connect(path: os.Path): TestBridgeClient =
    val c = new TestBridgeClient(path)
    synchronized(clients += c)
    c

  /** OPEN then LOCK on a fresh connection */
  private def session(path: os.Path): Future[TestBridgeClient] =
    val c = connect(path)
    for
      o <- c.open("d", DiskSize, BlockSize)
      _ = o.status should be (Statuses.Ok)
      l <- c.request(Ops.Lock, c.nextId())
    yield
      l.status should be (Statuses.Ok)
      c

  private def pattern(len: Int, seed: Int): Array[Byte] =
    Array.tabulate(len)(i => ((i * 7 + seed * 13) % 251 + 1).toByte)

  atest("4 MiB write round trip"):
    val data = pattern(4 * 1024 * 1024, 1)
    for
      _ <- createDisk()
      path = startServer()
      c <- session(path)
      w <- c.write(0, data)
      r <- c.read(0, data.length)
    yield
      w.status should be (Statuses.Ok)
      r.status should be (Statuses.Ok)
      java.util.Arrays.equals(r.payload, data) should be (true)

  atest("FUA data survives a dropped connection"):
    val data = pattern(8192, 2)
    for
      _ <- createDisk()
      path = startServer()
      c1 <- session(path)
      w <- c1.write(4096, data, fua = true)
      _ = c1.close()
      c2 <- session(path)
      r <- c2.read(4096, data.length)
    yield
      w.status should be (Statuses.Ok)
      r.status should be (Statuses.Ok)
      r.payload.toList should be (data.toList)

  atest("FLUSH makes earlier writes survive a dropped connection"):
    val a = pattern(4096, 3)
    val b = pattern(8192, 4)
    for
      _ <- createDisk()
      path = startServer()
      c1 <- session(path)
      w1 <- c1.write(0, a)
      w2 <- c1.write(1024 * 1024, b)
      f <- c1.request(Ops.Flush, c1.nextId())
      _ = c1.close()
      c2 <- session(path)
      r1 <- c2.read(0, a.length)
      r2 <- c2.read(1024 * 1024, b.length)
    yield
      List(w1, w2, f, r1, r2).map(_.status) should be (List.fill(5)(Statuses.Ok))
      r1.payload.toList should be (a.toList)
      r2.payload.toList should be (b.toList)

  atest("RESIZE commits the grow"):
    for
      _ <- createDisk()
      path = startServer()
      c <- session(path)
      r <- c.request(Ops.Resize, c.nextId(), offset = DiskSize * 2)
      info <- DiskCatalog.load(client, "d")
    yield
      r.status should be (Statuses.Ok)
      info.geometry.size should be (DiskSize * 2)
