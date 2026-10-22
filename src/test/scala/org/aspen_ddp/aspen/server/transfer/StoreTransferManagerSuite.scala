package org.aspen_ddp.aspen.server.transfer

import org.aspen_ddp.aspen.TestTLS
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.implementations.quic.TlsCredentials
import org.aspen_ddp.aspen.common.network.tls.{CertificateAuthority, TLSContext}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import java.io.InputStream
import java.net.{InetSocketAddress, ServerSocket, SocketException}
import java.util.UUID
import javax.net.ssl.{SSLException, SSLServerSocket, SSLSocket}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.util.{Random, Try}

class StoreTransferManagerSuite extends AnyFunSuite with Matchers with BeforeAndAfterEach with Eventually:

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  private val managers = ArrayBuffer.empty[StoreTransferManager]
  private val servers = ArrayBuffer.empty[RecordingServer]
  private val sockets = ArrayBuffer.empty[SSLSocket]
  private val tempDirs = ArrayBuffer.empty[os.Path]

  override def afterEach(): Unit =
    sockets.foreach(s => Try(s.close()))
    sockets.clear()
    managers.foreach(_.close())
    managers.clear()
    servers.foreach(_.close())
    servers.clear()
    tempDirs.foreach(d => Try(os.remove.all(d)))
    tempDirs.clear()

  private def credsFrom(ca: CertificateAuthority): TlsCredentials =
    val kp = CertificateAuthority.generateKeyPair()
    TlsCredentials(ca.certificate, ca.signCertificate(UUID.randomUUID(), kp.getPublic), kp.getPrivate)

  private def newManager(ioTimeout: FiniteDuration = 1.second): StoreTransferManager =
    val m = new StoreTransferManager(0, credsFrom(TestTLS.ca), ioTimeout = ioTimeout)
    managers += m
    m

  private def tempDir(): os.Path =
    val d = os.temp.dir(prefix = "store-transfer-manager-suite")
    tempDirs += d
    d

  private def rawSender(port: Int, creds: TlsCredentials): SSLSocket =
    val ctx = TLSContext.create(creds.caCertificate, creds.certificate, creds.privateKey)
    val s = ctx.getSocketFactory.createSocket("127.0.0.1", port).asInstanceOf[SSLSocket]
    s.setSoTimeout(5000)
    s.startHandshake()
    sockets += s
    s

  private def connect(m: StoreTransferManager): SSLSocket = rawSender(m.boundPort, credsFrom(TestTLS.ca))

  private val bigFile: Array[Byte] =
    val arr = new Array[Byte](3 * 1024 * 1024)
    new Random(42).nextBytes(arr)
    arr

  private def mkTree(dir: os.Path): Unit =
    os.write(dir / "a", bigFile)
    os.write(dir / "sub" / "b", "hello", createFolders = true)
    os.makeDir.all(dir / "sub" / "empty")

  private def jarBytes(dir: os.Path): Array[Byte] =
    os.proc("jar", "--create", "--no-manifest", ".").call(cwd = dir).out.bytes

  private lazy val archive: Array[Byte] =
    val src = tempDir()
    mkTree(src)
    jarBytes(src)

  /** A source store holding `size` bytes of incompressible data: far more than the socket buffers hold */
  private def bigTree(size: Int = 32 * 1024 * 1024): os.Path =
    val dir = tempDir()
    val arr = new Array[Byte](size)
    new Random(7).nextBytes(arr)
    os.write(dir / "big", arr)
    dir

  /** Mutual-TLS listener that records each connection's header and then never reads again */
  private class RecordingServer:
    private val ss =
      val creds = credsFrom(TestTLS.ca)
      val sctx = TLSContext.create(creds.caCertificate, creds.certificate, creds.privateKey)
      val s = sctx.getServerSocketFactory.createServerSocket().asInstanceOf[SSLServerSocket]
      s.setNeedClientAuth(true)
      s.bind(new InetSocketAddress("127.0.0.1", 0))
      s
    private var conns = Vector.empty[(SSLSocket, Option[(StoreId, UUID)])]

    def port: Int = ss.getLocalPort

    private val acceptThread = new Thread(() =>
      try
        while true do
          val sock = ss.accept().asInstanceOf[SSLSocket]
          val t = new Thread(() =>
            val hdr = Try(sock.getInputStream.readNBytes(StoreTransferManager.HeaderSize)).toOption
            val parsed = hdr.filter(_.length == StoreTransferManager.HeaderSize).map: h =>
              val bb = java.nio.ByteBuffer.wrap(h)
              (StoreId(PoolId(new UUID(bb.getLong, bb.getLong)), bb.get), new UUID(bb.getLong, bb.getLong))
            synchronized { conns = conns :+ (sock, parsed) }
          )
          t.setDaemon(true)
          t.start()
      catch case _: Throwable => ()
    )
    acceptThread.setDaemon(true)
    acceptThread.start()

    def connections: Vector[(SSLSocket, Option[(StoreId, UUID)])] = synchronized(conns)

    def headers: Vector[(StoreId, UUID)] = connections.flatMap(_._2)

    def close(): Unit =
      Try(ss.close())
      connections.foreach((s, _) => Try(s.close()))

  private def recordingServer(): RecordingServer =
    val r = new RecordingServer
    servers += r
    r

  private def freePort(): Int =
    val s = new ServerSocket(0)
    try s.getLocalPort finally s.close()

  /** Reads `s` until EOF or error; the number of bytes read before it ended */
  private def drain(s: SSLSocket): Long =
    s.setSoTimeout(10000)
    val in: InputStream = s.getInputStream
    val buf = new Array[Byte](64 * 1024)
    var total = 0L
    var n = 0
    while n != -1 do
      n = try in.read(buf) catch
        case _: SSLException => -1
        case _: SocketException => -1
      if n > 0 then
        total += n
    total

  private def newStoreId(): StoreId = StoreId(PoolId(UUID.randomUUID()), 2)

  private def send(s: SSLSocket, parts: Array[Byte]*): Unit =
    val out = s.getOutputStream
    parts.foreach(p => out.write(p))
    out.flush()

  /** True once the peer has closed the connection without sending anything */
  private def isClosedByPeer(s: SSLSocket): Boolean =
    try
      s.getInputStream.read() == -1
    catch
      case _: SSLException => true
      case _: SocketException => true

  private def await(f: Future[Unit], d: FiniteDuration = 10.seconds): Try[Unit] =
    Try(Await.result(f, d))

  private def assertExtracted(storeDir: os.Path): Unit =
    os.read.bytes(storeDir / "a") should equal(bigFile)
    os.read(storeDir / "sub" / "b") should be("hello")
    os.isDir(storeDir / "sub" / "empty") should be(true)
    os.exists(storeDir / "META-INF") should be(false)

  test("beginTransferIn completes when header, archive and trailer arrive"):
    val m = newManager()
    val dest = tempDir()
    val sid = newStoreId()
    val u = UUID.randomUUID()
    val f = m.beginTransferIn(sid, u, dest)
    val s = connect(m)
    send(s, StoreTransferManager.encodeHeader(sid, u), archive, sid.toBytes)
    s.close()
    await(f).isSuccess should be(true)
    assertExtracted(dest / sid.directoryName)

  test("existing destination store directory is replaced"):
    val m = newManager()
    val dest = tempDir()
    val sid = newStoreId()
    val u = UUID.randomUUID()
    os.write(dest / sid.directoryName / "stale", "old", createFolders = true)
    val f = m.beginTransferIn(sid, u, dest)
    val s = connect(m)
    send(s, StoreTransferManager.encodeHeader(sid, u), archive, sid.toBytes)
    s.close()
    await(f).isSuccess should be(true)
    os.exists(dest / sid.directoryName / "stale") should be(false)
    assertExtracted(dest / sid.directoryName)

  test("truncated stream without trailer fails and cleans up"):
    val m = newManager()
    val dest = tempDir()
    val sid = newStoreId()
    val u = UUID.randomUUID()
    val f = m.beginTransferIn(sid, u, dest)
    val s = connect(m)
    send(s, StoreTransferManager.encodeHeader(sid, u), archive)
    s.close()
    await(f).isFailure should be(true)
    os.exists(dest / sid.directoryName) should be(false)

  test("wrong trailer fails"):
    val m = newManager()
    val dest = tempDir()
    val sid = newStoreId()
    val u = UUID.randomUUID()
    val f = m.beginTransferIn(sid, u, dest)
    val s = connect(m)
    send(s, StoreTransferManager.encodeHeader(sid, u), archive, newStoreId().toBytes)
    s.close()
    await(f).isFailure should be(true)
    os.exists(dest / sid.directoryName) should be(false)

  test("extractor failure fails the transfer"):
    val m = newManager()
    val dest = tempDir()
    val sid = newStoreId()
    val u = UUID.randomUUID()
    val f = m.beginTransferIn(sid, u, dest)
    val s = connect(m)
    // A cut in the middle of the 3 MiB entry makes `jar --extract` exit 1
    send(s, StoreTransferManager.encodeHeader(sid, u), archive.take(archive.length / 2), sid.toBytes)
    s.close()
    await(f).isFailure should be(true)
    os.exists(dest / sid.directoryName) should be(false)

  test("unknown StoreId is dropped"):
    val m = newManager()
    val dest = tempDir()
    val bad = connect(m)
    send(bad, StoreTransferManager.encodeHeader(newStoreId(), UUID.randomUUID()))
    isClosedByPeer(bad) should be(true)

    val sid = newStoreId()
    val u = UUID.randomUUID()
    val f = m.beginTransferIn(sid, u, dest)
    val s = connect(m)
    send(s, StoreTransferManager.encodeHeader(sid, u), archive, sid.toBytes)
    s.close()
    await(f).isSuccess should be(true)

  test("stale UUID is rejected and the current one succeeds"):
    val m = newManager()
    val dest = tempDir()
    val sid = newStoreId()
    val u1 = UUID.randomUUID()
    val u2 = UUID.randomUUID()
    val f = m.beginTransferIn(sid, u2, dest)
    val stale = connect(m)
    send(stale, StoreTransferManager.encodeHeader(sid, u1))
    isClosedByPeer(stale) should be(true)

    val s = connect(m)
    send(s, StoreTransferManager.encodeHeader(sid, u2), archive, sid.toBytes)
    s.close()
    await(f).isSuccess should be(true)
    assertExtracted(dest / sid.directoryName)

  test("reused UUID is rejected while first transfer proceeds"):
    val m = newManager()
    val dest = tempDir()
    val sid = newStoreId()
    val u = UUID.randomUUID()
    val f = m.beginTransferIn(sid, u, dest)
    val half = archive.length / 2
    val first = connect(m)
    send(first, StoreTransferManager.encodeHeader(sid, u), archive.take(half))
    // The store directory is created only once the first connection has been matched
    eventually { os.exists(dest / sid.directoryName) should be(true) }

    val second = connect(m)
    send(second, StoreTransferManager.encodeHeader(sid, u))
    isClosedByPeer(second) should be(true)

    send(first, archive.drop(half), sid.toBytes)
    first.close()
    await(f).isSuccess should be(true)
    assertExtracted(dest / sid.directoryName)

  test("receiver times out on stalled sender"):
    val m = newManager()
    val dest = tempDir()
    val sid = newStoreId()
    val u = UUID.randomUUID()
    val f = m.beginTransferIn(sid, u, dest)
    val s = connect(m)
    send(s, StoreTransferManager.encodeHeader(sid, u), archive.take(1024))
    await(f, 5.seconds).isFailure should be(true)
    f.isCompleted should be(true)
    os.exists(dest / sid.directoryName) should be(false)

  test("re-registering a store fails the previous future"):
    val m = newManager()
    val dest = tempDir()
    val sid = newStoreId()
    val f1 = m.beginTransferIn(sid, UUID.randomUUID(), dest)
    val u2 = UUID.randomUUID()
    val f2 = m.beginTransferIn(sid, u2, dest)
    await(f1).isFailure should be(true)

    val s = connect(m)
    send(s, StoreTransferManager.encodeHeader(sid, u2), archive, sid.toBytes)
    s.close()
    await(f2).isSuccess should be(true)
    f1.value.exists(_.isFailure) should be(true)
    assertExtracted(dest / sid.directoryName)

  test("round trip between two managers"):
    val a = newManager()
    val b = newManager()
    val src = tempDir()
    mkTree(src)
    val dest = tempDir()
    val sid = newStoreId()
    val u = UUID.randomUUID()
    val f = b.beginTransferIn(sid, u, dest)
    a.beginTransferOut(sid, u, src, "127.0.0.1", b.boundPort, HLCTimestamp.now)
    await(f).isSuccess should be(true)
    assertExtracted(dest / sid.directoryName)
    eventually(timeout(Span(5, Seconds))) { a.isTransferringOut(sid) should be(false) }
    a.transferringOutStores should be(Set.empty)

  test("isTransferringOut is true while sending"):
    val a = newManager(ioTimeout = 30.seconds)
    val r = recordingServer()
    val sid = newStoreId()
    val u = UUID.randomUUID()
    a.beginTransferOut(sid, u, bigTree(), "127.0.0.1", r.port, HLCTimestamp.now)
    eventually { r.headers should be(Vector((sid, u))) }
    a.isTransferringOut(sid) should be(true)
    a.transferringOutStores should be(Set(sid))

  test("sender times out on stalled receiver"):
    val a = newManager()
    val r = recordingServer()
    val sid = newStoreId()
    a.beginTransferOut(sid, UUID.randomUUID(), bigTree(), "127.0.0.1", r.port, HLCTimestamp.now)
    a.isTransferringOut(sid) should be(true)
    eventually(timeout(Span(15, Seconds))) { a.isTransferringOut(sid) should be(false) }
    r.headers.size should be(1)

  test("newer timestamp supersedes the active attempt"):
    val a = newManager(ioTimeout = 30.seconds)
    val r = recordingServer()
    val sid = newStoreId()
    val src = bigTree()
    val u1 = UUID.randomUUID()
    val u2 = UUID.randomUUID()
    val ts1 = HLCTimestamp.now
    val ts2 = HLCTimestamp.happensAfter(ts1)
    a.beginTransferOut(sid, u1, src, "127.0.0.1", r.port, ts1)
    eventually { r.headers should be(Vector((sid, u1))) }
    a.beginTransferOut(sid, u2, src, "127.0.0.1", r.port, ts2)
    eventually { r.headers should be(Vector((sid, u1), (sid, u2))) }
    // An aborted attempt ends long before the whole 32 MiB archive could have been sent
    drain(r.connections.head._1) should be < (32L * 1024 * 1024)
    a.isTransferringOut(sid) should be(true)

  test("same or older timestamp is ignored after completion"):
    val a = newManager()
    val b = newManager()
    val src = tempDir()
    mkTree(src)
    val sid = newStoreId()
    val u1 = UUID.randomUUID()
    val ts1 = HLCTimestamp.now
    val f = b.beginTransferIn(sid, u1, tempDir())
    a.beginTransferOut(sid, u1, src, "127.0.0.1", b.boundPort, ts1)
    await(f).isSuccess should be(true)
    eventually { a.isTransferringOut(sid) should be(false) }

    val r = recordingServer()
    a.beginTransferOut(sid, u1, src, "127.0.0.1", r.port, ts1)
    a.beginTransferOut(sid, UUID.randomUUID(), src, "127.0.0.1", r.port, HLCTimestamp(ts1.asLong - (1L << 16)))
    a.isTransferringOut(sid) should be(false)
    Thread.sleep(2000)
    r.connections should be(empty)

  test("same timestamp is ignored while active"):
    val a = newManager(ioTimeout = 30.seconds)
    val r = recordingServer()
    val sid = newStoreId()
    val src = bigTree()
    val u = UUID.randomUUID()
    val ts = HLCTimestamp.now
    a.beginTransferOut(sid, u, src, "127.0.0.1", r.port, ts)
    eventually { r.headers should be(Vector((sid, u))) }
    a.beginTransferOut(sid, u, src, "127.0.0.1", r.port, ts)
    a.beginTransferOut(sid, UUID.randomUUID(), src, "127.0.0.1", r.port, ts)
    Thread.sleep(1000)
    r.connections.size should be(1)
    a.isTransferringOut(sid) should be(true)

  test("failed jar create sends no trailer"):
    val a = newManager()
    val b = newManager()
    val missing = tempDir() / "does-not-exist"
    val sid = newStoreId()
    val u = UUID.randomUUID()
    val f = b.beginTransferIn(sid, u, tempDir())
    a.beginTransferOut(sid, u, missing, "127.0.0.1", b.boundPort, HLCTimestamp.now)
    await(f).isFailure should be(true)
    eventually { a.isTransferringOut(sid) should be(false) }

  test("unreachable destination"):
    val a = newManager()
    val src = tempDir()
    mkTree(src)
    val sid = newStoreId()
    noException should be thrownBy
      a.beginTransferOut(sid, UUID.randomUUID(), src, "127.0.0.1", freePort(), HLCTimestamp.now)
    eventually { a.isTransferringOut(sid) should be(false) }

  test("same timestamp is retried after a failed attempt"):
    val a = newManager()
    val b = newManager()
    val src = tempDir()
    mkTree(src)
    val dest = tempDir()
    val sid = newStoreId()
    val u = UUID.randomUUID()
    val ts1 = HLCTimestamp.now
    // Fails before any receiver accepts it, so the receiver never fails and never mints a newer
    // timestamp: the retransmit of this same request is the only thing that can retry it.
    a.beginTransferOut(sid, u, src, "127.0.0.1", freePort(), ts1)
    eventually { a.isTransferringOut(sid) should be(false) }

    val f = b.beginTransferIn(sid, u, dest)
    a.beginTransferOut(sid, u, src, "127.0.0.1", b.boundPort, ts1)
    await(f).isSuccess should be(true)
    assertExtracted(dest / sid.directoryName)
