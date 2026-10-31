package org.aspen_ddp.aspen.vdisk.bridge

import io.netty.util.ResourceLeakDetector
import org.aspen_ddp.aspen.vdisk.*
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.channels.ServerSocketChannel
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future, Promise}

class BridgeServerSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll:

  import TestBridgeClient.{Ops, Response, Statuses}

  private var previousLevel: ResourceLeakDetector.Level = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    previousLevel = ResourceLeakDetector.getLevel
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)

  override def afterAll(): Unit =
    ResourceLeakDetector.setLevel(previousLevel)

  private val Size = 1L << 20
  private val Block = 4096
  private val Geometry = DiskGeometry(Size, Block, 65536)

  /** Every call returns a Promise the test controls, unless `auto` is set, in which case calls complete at once */
  private final class FakeDevice(var auto: Boolean = true, geo: DiskGeometry = Geometry)
      extends LockableBlockDevice:

    final case class Call[A](name: String, args: List[Any], promise: Promise[A])

    @volatile var geometryFails = false
    def geometry: DiskGeometry = if geometryFails then throw new RuntimeException("no geometry") else geo

    val calls = new LinkedBlockingQueue[Call[?]]()
    val log = ListBuffer[(String, List[Any])]()
    val closes = new AtomicInteger(0)
    val releases = new AtomicInteger(0)
    val closed = new CountDownLatch(1)
    @volatile var state: LockState = LockState.Unlocked
    @volatile var readData: Array[Byte] => Array[Byte] = _ => Array.empty

    private def record[A](name: String, args: List[Any], default: => A): Future[A] =
      val p = Promise[A]()
      synchronized(log += ((name, args)))
      calls.add(Call(name, args, p))
      if auto then
        p.success(default)
      p.future

    def logged: List[(String, List[Any])] = synchronized(log.toList)

    def next[A](): Call[A] =
      val c = calls.poll(5, TimeUnit.SECONDS)
      c should not be null
      c.asInstanceOf[Call[A]]

    def read(offset: Long, length: Int): Future[Array[Byte]] =
      record("read", List(offset, length), Array.tabulate(length)(i => (i % 251).toByte))
    def write(offset: Long, data: Array[Byte], fua: Boolean): Future[Unit] =
      record("write", List(offset, data.toList, fua), ())
    def flush(): Future[Unit] = record("flush", Nil, ())
    def unmap(ranges: List[(Long, Long)]): Future[Unit] = record("unmap", List(ranges), ())
    def grow(newSize: Long): Future[Unit] = record("grow", List(newSize), ())
    def acquire(): Future[Unit] = record("acquire", Nil, ())
    def release(): Future[Unit] =
      releases.incrementAndGet()
      record("release", Nil, ())
    def lockState: LockState = state
    def close(): Unit =
      closes.incrementAndGet()
      closed.countDown()

  private def await[A](f: Future[A]): A = Await.result(f, 10.seconds)

  /** Runs `body` against a server whose every OPEN yields `dev`, with a short socket path */
  private def withServer(open: String => Future[LockableBlockDevice])(body: (os.Path, BridgeServer) => Unit): Unit =
    val dir = os.temp.dir(prefix = "vdb")
    val path = dir / "s.sock"
    val server = new BridgeServer(path, open)
    server.start()
    try
      body(path, server)
    finally
      server.stop()
      os.remove.all(dir)

  private def withDevice(dev: FakeDevice)(body: (os.Path, FakeDevice) => Unit): Unit =
    withServer(_ => Future.successful(dev))((path, _) => body(path, dev))

  private def opened(path: os.Path): TestBridgeClient =
    val c = new TestBridgeClient(path)
    val r = await(c.open("d", Size, Block))
    r.status should be (Statuses.Ok)
    c

  private def u32(b: Array[Byte], off: Int): Int = java.nio.ByteBuffer.wrap(b).getInt(off)

  test("open then lock then I/O"):
    val dev = new FakeDevice()
    withDevice(dev): (path, _) =>
      val c = opened(path)
      try
        c.call(Ops.Lock).status should be (Statuses.Ok)
        val r = c.call(Ops.Read, offset = 4096, length = 8)
        r.status should be (Statuses.Ok)
        r.op should be (Ops.Read)
        r.payload.toList should be (List.tabulate(8)(i => i.toByte))
        val data = Array.tabulate(16)(i => (i + 100).toByte)
        c.call(Ops.Write, flags = 1, offset = 8192, length = 16, payload = data).status should be (Statuses.Ok)
        c.call(Ops.Write, flags = 0, offset = 0, length = 16, payload = data).status should be (Statuses.Ok)
        c.call(Ops.Flush).status should be (Statuses.Ok)
        c.call(Ops.Resize, offset = Size * 2).status should be (Statuses.Ok)
        dev.logged should be (List(
          ("acquire", Nil),
          ("read", List(4096L, 8)),
          ("write", List(8192L, data.toList, true)),
          ("write", List(0L, data.toList, false)),
          ("flush", Nil),
          ("grow", List(Size * 2))))
      finally
        c.close()

  test("open reply carries blockObjectSize and logicalBlockSize"):
    withDevice(new FakeDevice()): (path, _) =>
      val c = new TestBridgeClient(path)
      try
        val r = await(c.open("d", Size, Block))
        r.status should be (Statuses.Ok)
        r.op should be (Ops.Open)
        r.payload.length should be (8)
        u32(r.payload, 0) should be (65536)
        u32(r.payload, 4) should be (Block)
      finally
        c.close()

  test("the disk name reaches openDisk"):
    val names = new LinkedBlockingQueue[String]()
    withServer(n => { names.add(n); Future.successful(new FakeDevice()) }): (path, _) =>
      val c = opened(path)
      try
        names.poll(5, TimeUnit.SECONDS) should be ("d")
      finally
        c.close()

  test("size mismatch closes the device and replies SizeMismatch"):
    for (size, bs) <- List((Size * 2, Block), (Size, 512)) do
      val dev = new FakeDevice()
      withDevice(dev): (path, _) =>
        val c = new TestBridgeClient(path)
        try
          val r = await(c.open("d", size, bs))
          r.status should be (Statuses.SizeMismatch)
          r.message should not be empty
          dev.closed.await(5, TimeUnit.SECONDS) should be (true)
          dev.closes.get should be (1)
          // Not open, so I/O is refused
          c.call(Ops.Flush).status should be (Statuses.ProtocolError)
        finally
          c.close()

  test("open of a missing disk replies NotFound"):
    withServer(n => Future.failed(VDiskError.DiskNotFound(n))): (path, _) =>
      val c = new TestBridgeClient(path)
      try
        val r = await(c.open("nope", Size, Block))
        r.status should be (Statuses.NotFound)
        r.message should include ("nope")
      finally
        c.close()

  test("an openDisk that throws replies Internal"):
    withServer(_ => throw new RuntimeException("boom")): (path, _) =>
      val c = new TestBridgeClient(path)
      try
        await(c.open("x", Size, Block)).status should be (Statuses.Internal)
      finally
        c.close()

  test("requests before OPEN and a second OPEN reply ProtocolError"):
    withDevice(new FakeDevice()): (path, _) =>
      val c = new TestBridgeClient(path)
      try
        for op <- List(Ops.Read, Ops.Write, Ops.Flush, Ops.Unmap, Ops.Resize, Ops.Lock, Ops.Unlock, Ops.LockState) do
          val r = c.call(op)
          r.status should be (Statuses.ProtocolError)
          r.op should be (op)
        await(c.open("d", Size, Block)).status should be (Statuses.Ok)
        await(c.open("d", Size, Block)).status should be (Statuses.ProtocolError)
        // The session is still usable
        c.call(Ops.Flush).status should be (Statuses.Ok)
      finally
        c.close()

  test("an unknown op replies ProtocolError"):
    withDevice(new FakeDevice()): (path, _) =>
      val c = opened(path)
      try
        for op <- List(0, 10, 200) do
          val r = c.call(op)
          r.status should be (Statuses.ProtocolError)
          r.op should be (op)
        c.call(Ops.Flush).status should be (Statuses.Ok)
      finally
        c.close()

  test("completions return in completion order"):
    val dev = new FakeDevice(auto = false)
    withDevice(dev): (path, _) =>
      val c = opened(path)
      try
        val f1 = c.request(Ops.Read, 101, offset = 0, length = 4)
        val f2 = c.request(Ops.Read, 102, offset = 4096, length = 4)
        val first = dev.next[Array[Byte]]()
        val second = dev.next[Array[Byte]]()
        first.args should be (List(0L, 4))
        second.args should be (List(4096L, 4))
        second.promise.success(Array[Byte](2, 2, 2, 2))
        val r2 = await(f2)
        // Only now, after the reply to 102 has arrived, does 101 complete
        first.promise.success(Array[Byte](1, 1, 1, 1))
        val r1 = await(f1)
        r2.payload.toList should be (List[Byte](2, 2, 2, 2))
        r1.payload.toList should be (List[Byte](1, 1, 1, 1))
        c.arrival.toArray.toList.drop(1) should be (List[AnyRef](Long.box(102L), Long.box(101L)))
      finally
        c.close()

  test("error mapping"):
    val table: List[(Throwable, Int)] = List(
      (VDiskError.InvalidRequest("bad request"), Statuses.InvalidRequest),
      (VDiskError.InvalidGeometry("bad geometry"), Statuses.InvalidRequest),
      (VDiskError.OutOfRange("too far"), Statuses.OutOfRange),
      (VDiskError.Fenced, Statuses.Fenced),
      (VDiskError.NotOwner, Statuses.NotOwner),
      (VDiskError.DiskFailed("gone"), Statuses.DiskFailed),
      (VDiskError.DiskNotFound("zzz"), Statuses.NotFound),
      (VDiskError.DiskAlreadyExists("yyy"), Statuses.Internal),
      (new RuntimeException("surprise"), Statuses.Internal))
    val dev = new FakeDevice(auto = false)
    withDevice(dev): (path, _) =>
      val c = opened(path)
      try
        for (err, status) <- table do
          val f = c.request(Ops.Read, c.nextId(), offset = 0, length = 4)
          dev.next[Array[Byte]]().promise.failure(err)
          val r = await(f)
          r.status should be (status)
          r.message should be (err.getMessage)
        val f = c.request(Ops.Flush, c.nextId())
        dev.next[Unit]().promise.failure(VDiskError.Fenced)
        await(f).status should be (Statuses.Fenced)
      finally
        c.close()

  test("an oversize error message is truncated to 1024 bytes"):
    val dev = new FakeDevice(auto = false)
    withDevice(dev): (path, _) =>
      val c = opened(path)
      try
        val f = c.request(Ops.Flush, c.nextId())
        dev.next[Unit]().promise.failure(VDiskError.InvalidRequest("x" * 5000))
        await(f).payload.length should be (1024)
      finally
        c.close()

  test("WRITE length mismatch, bad UNMAP payloads and stray payloads reply InvalidRequest"):
    val dev = new FakeDevice()
    withDevice(dev): (path, _) =>
      val c = opened(path)
      try
        val four = Array[Byte](1, 2, 3, 4)
        c.call(Ops.Write, length = 8, payload = four).status should be (Statuses.InvalidRequest)
        c.call(Ops.Write, length = 0, payload = four).status should be (Statuses.InvalidRequest)
        c.call(Ops.Write, length = 4).status should be (Statuses.InvalidRequest)
        c.call(Ops.Unmap).status should be (Statuses.InvalidRequest)
        c.call(Ops.Unmap, payload = new Array[Byte](15)).status should be (Statuses.InvalidRequest)
        c.call(Ops.Unmap, payload = new Array[Byte](17)).status should be (Statuses.InvalidRequest)
        for op <- List(Ops.Read, Ops.Flush, Ops.Resize, Ops.Lock, Ops.Unlock, Ops.LockState) do
          c.call(op, length = 4, payload = four).status should be (Statuses.InvalidRequest)
        dev.logged should be (Nil)
        c.call(Ops.Flush).status should be (Statuses.Ok)
      finally
        c.close()

  test("UNMAP passes every range in order"):
    val dev = new FakeDevice()
    withDevice(dev): (path, _) =>
      val c = opened(path)
      try
        val ranges = List((0L, 4096L), (8192L, 8192L), (1L << 33, 4096L))
        val b = java.nio.ByteBuffer.allocate(16 * ranges.size)
        ranges.foreach((o, l) => b.putLong(o).putLong(l))
        c.call(Ops.Unmap, payload = b.array()).status should be (Statuses.Ok)
        dev.logged should be (List(("unmap", List(ranges))))
      finally
        c.close()

  test("LOCK, UNLOCK and LOCK_STATE call acquire, release and lockState"):
    val dev = new FakeDevice()
    withDevice(dev): (path, _) =>
      val c = opened(path)
      try
        val states = List(
          (LockState.Unlocked, 0), (LockState.Locked, 1), (LockState.Fenced, 2), (LockState.Failed("why"), 3))
        for (s, byte) <- states do
          dev.state = s
          val r = c.call(Ops.LockState)
          r.status should be (Statuses.Ok)
          r.payload.toList should be (List(byte.toByte))
        c.call(Ops.Lock).status should be (Statuses.Ok)
        c.call(Ops.Unlock).status should be (Statuses.Ok)
        dev.logged should be (List(("acquire", Nil), ("release", Nil)))
      finally
        c.close()

  test("disconnect closes the device without release"):
    val dev = new FakeDevice()
    withDevice(dev): (path, _) =>
      val c = opened(path)
      c.call(Ops.Lock).status should be (Statuses.Ok)
      c.close()
      dev.closed.await(5, TimeUnit.SECONDS) should be (true)
      dev.releases.get should be (0)
      dev.closes.get should be (1)

  test("UNLOCK then disconnect calls release once and close once"):
    val dev = new FakeDevice()
    withDevice(dev): (path, _) =>
      val c = opened(path)
      c.call(Ops.Lock).status should be (Statuses.Ok)
      c.call(Ops.Unlock).status should be (Statuses.Ok)
      c.close()
      dev.closed.await(5, TimeUnit.SECONDS) should be (true)
      dev.releases.get should be (1)
      dev.closes.get should be (1)

  test("a disconnect while OPEN is pending still closes the device"):
    val dev = new FakeDevice()
    val gate = Promise[LockableBlockDevice]()
    val asked = new CountDownLatch(1)
    withServer(_ => { asked.countDown(); gate.future }): (path, _) =>
      val c = new TestBridgeClient(path)
      c.open("d", Size, Block)
      asked.await(5, TimeUnit.SECONDS) should be (true)
      c.close()
      Await.ready(c.eof.future, 5.seconds)
      // The server may not yet have processed the disconnect; the device is closed either way once it arrives
      gate.success(dev)
      dev.closed.await(5, TimeUnit.SECONDS) should be (true)
      dev.closes.get should be (1)

  test("stop closes the devices of live sessions"):
    val dev = new FakeDevice()
    withServer(_ => Future.successful(dev)): (path, server) =>
      val c = opened(path)
      try
        server.stop()
        dev.closed.await(5, TimeUnit.SECONDS) should be (true)
        Await.ready(c.eof.future, 5.seconds)
      finally
        c.close()

  test("bad magic closes the connection and the server accepts the next one"):
    val dev = new FakeDevice()
    withDevice(dev): (path, _) =>
      val bad = new TestBridgeClient(path)
      await(bad.sendRaw(new Array[Byte](32)))
      Await.ready(bad.eof.future, 5.seconds)
      bad.close()
      val good = opened(path)
      try
        good.call(Ops.Flush).status should be (Statuses.Ok)
      finally
        good.close()

  test("an oversize payload length closes the connection"):
    withDevice(new FakeDevice()): (path, _) =>
      val c = new TestBridgeClient(path)
      try
        val b = java.nio.ByteBuffer.allocate(32)
        b.putShort(0xA5D1.toShort).put(1.toByte).put(2.toByte).putInt(0).putLong(1).putLong(0).putInt(0)
          .putInt(64 * 1024 * 1024 + 1)
        await(c.sendRaw(b.array()))
        Await.ready(c.eof.future, 5.seconds)
      finally
        c.close()

  test("200 outstanding writes are all answered"):
    val dev = new FakeDevice(auto = false)
    withDevice(dev): (path, _) =>
      val c = opened(path)
      try
        val futures = (0 until 200).map(i => c.write(i * 512L, Array.fill(512)(i.toByte)))
        val pending = (0 until 200).map(_ => dev.next[Unit]())
        pending.map(_.args(0).asInstanceOf[Long]).toSet should be ((0 until 200).map(_ * 512L).toSet)
        pending.reverse.foreach(_.promise.success(()))
        val replies = futures.map(await)
        replies.forall(_.status == Statuses.Ok) should be (true)
        replies.map(_.requestId).toSet.size should be (200)
      finally
        c.close()

  test("stale socket: a socket file left behind is replaced"):
    val dir = os.temp.dir(prefix = "vdb")
    val path = dir / "s.sock"
    val leftover = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
    leftover.bind(UnixDomainSocketAddress.of(path.toNIO))
    leftover.close()
    os.exists(path, followLinks = false) should be (true)
    val dev = new FakeDevice()
    val server = new BridgeServer(path, _ => Future.successful(dev))
    try
      server.start()
      val c = opened(path)
      c.close()
    finally
      server.stop()
      os.remove.all(dir)

  test("live socket: a second start on the same path throws"):
    withDevice(new FakeDevice()): (path, _) =>
      val second = new BridgeServer(path, _ => Future.successful(new FakeDevice()))
      val e = intercept[IllegalStateException](second.start())
      e.getMessage should be (s"socket in use: $path")
      // The first server is unharmed
      val c = opened(path)
      c.close()

  test("the socket file is mode 0600"):
    withDevice(new FakeDevice()): (path, _) =>
      os.perms(path).toString() should be ("rw-------")
      // The private bind directory is gone
      os.list(path / os.up).map(_.last) should be (Seq("s.sock"))

  test("stop racing a pending OPEN closes the device that arrives later"):
    val dev = new FakeDevice()
    val gate = Promise[LockableBlockDevice]()
    val asked = new CountDownLatch(1)
    withServer(_ => { asked.countDown(); gate.future }): (path, server) =>
      val c = new TestBridgeClient(path)
      try
        c.open("d", Size, Block)
        asked.await(5, TimeUnit.SECONDS) should be (true)
        server.stop()
        gate.success(dev)
        dev.closed.await(5, TimeUnit.SECONDS) should be (true)
        dev.closes.get should be (1)
      finally
        c.close()

  test("an OPEN completion that throws closes the device and replies Internal"):
    val dev = new FakeDevice()
    dev.geometryFails = true
    withDevice(dev): (path, _) =>
      val c = new TestBridgeClient(path)
      try
        await(c.open("d", Size, Block)).status should be (Statuses.Internal)
        dev.closed.await(5, TimeUnit.SECONDS) should be (true)
        dev.closes.get should be (1)
        c.call(Ops.Flush).status should be (Statuses.ProtocolError)
      finally
        c.close()

  test("a result that cannot be encoded replies Internal"):
    val dev = new FakeDevice(auto = false)
    withDevice(dev): (path, _) =>
      val c = opened(path)
      try
        val f = c.request(Ops.Read, c.nextId(), offset = 0, length = 4)
        dev.next[Array[Byte]]().promise.success(null)
        await(f).status should be (Statuses.Internal)
        dev.auto = true
        c.call(Ops.Flush).status should be (Statuses.Ok)
      finally
        c.close()

  test("a regular file at the path makes start fail and survives"):
    val dir = os.temp.dir(prefix = "vdb")
    val path = dir / "s.sock"
    os.write(path, "precious")
    val server = new BridgeServer(path, _ => Future.successful(new FakeDevice()))
    try
      intercept[IllegalStateException](server.start())
      os.read(path) should be ("precious")
    finally
      os.remove.all(dir)
