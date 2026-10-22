package org.aspen_ddp.aspen.server.transfer

import org.aspen_ddp.aspen.TestTLS
import org.aspen_ddp.aspen.common.network.implementations.quic.TlsCredentials
import org.aspen_ddp.aspen.common.network.tls.{CertificateAuthority, TLSContext}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import java.net.SocketException
import java.util.UUID
import javax.net.ssl.{SSLException, SSLSocket}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*
import scala.util.{Random, Try}

class StoreTransferManagerSuite extends AnyFunSuite with Matchers with BeforeAndAfterEach with Eventually:

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  private val managers = ArrayBuffer.empty[StoreTransferManager]
  private val sockets = ArrayBuffer.empty[SSLSocket]
  private val tempDirs = ArrayBuffer.empty[os.Path]

  override def afterEach(): Unit =
    sockets.foreach(s => Try(s.close()))
    sockets.clear()
    managers.foreach(_.close())
    managers.clear()
    tempDirs.foreach(d => Try(os.remove.all(d)))
    tempDirs.clear()

  private def credsFrom(ca: CertificateAuthority): TlsCredentials =
    val kp = CertificateAuthority.generateKeyPair()
    TlsCredentials(ca.certificate, ca.signCertificate(UUID.randomUUID(), kp.getPublic), kp.getPrivate)

  private def newManager(): StoreTransferManager =
    val m = new StoreTransferManager(0, credsFrom(TestTLS.ca), ioTimeout = 1.second)
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
    m.beginTransferIn(sid, UUID.randomUUID(), dest)
    await(f1).isFailure should be(true)
