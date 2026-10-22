package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.util.ResourceLeakDetector
import org.aspen_ddp.aspen.TestTLS
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.network.{HostHeartbeat, OpportunisticRebuild}
import org.aspen_ddp.aspen.common.network.tls.CertificateAuthority
import io.netty.handler.codec.quic.{QuicSslContext, QuicSslContextBuilder}
import org.aspen_ddp.aspen.common.network.tls.{AspenTrustManager, CertificateVerifier, InvalidPeerCertificate, Pem}
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.{BasicConstraints, Extension, GeneralName, GeneralNames, KeyUsage}
import org.bouncycastle.cert.jcajce.{JcaX509CertificateConverter, JcaX509v3CertificateBuilder}
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.math.BigInteger
import java.security.{PrivateKey, PublicKey}
import java.security.cert.{CertificateExpiredException, X509Certificate}
import java.time.{Duration as JDuration, Instant}
import java.util.{Date, UUID}
import scala.collection.mutable.ArrayBuffer

class QuicTlsSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach:

  private var previousLevel: ResourceLeakDetector.Level = scala.compiletime.uninitialized
  private val peers = ArrayBuffer.empty[QuicTestPeer]
  private var leakWatch: QuicTestPeer.LeakWatch = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    previousLevel = ResourceLeakDetector.getLevel
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)
    leakWatch = QuicTestPeer.watchLeaks()

  override def afterAll(): Unit =
    leakWatch.stop()
    ResourceLeakDetector.setLevel(previousLevel)

  override def afterEach(): Unit =
    peers.foreach(_.close())
    peers.clear()

  private def track(p: QuicTestPeer): QuicTestPeer =
    peers += p
    p

  private def credsFrom(ca: CertificateAuthority): TlsCredentials =
    val kp = CertificateAuthority.generateKeyPair()
    TlsCredentials(ca.certificate, ca.signCertificate(UUID.randomUUID(), kp.getPublic), kp.getPrivate)

  private def goodCreds(): TlsCredentials = credsFrom(TestTLS.ca)

  /** Credentials from a different CA, so neither side trusts the other */
  private def foreignCreds(): TlsCredentials = credsFrom(CertificateAuthority.create(UUID.randomUUID()))

  /** A client context that trusts the test CA but presents `chain` (or nothing, if empty) as its own identity */
  private def presenting(key: PrivateKey, chain: X509Certificate*): QuicSslContext =
    val b = QuicSslContextBuilder.forClient()
    if chain.nonEmpty then
      b.keyManager(key, null, chain*)
    b.trustManager(new AspenTrustManager(TestTLS.ca.certificate))
      .endpointIdentificationAlgorithm(null)
      .applicationProtocols(QuicTls.ApplicationProtocol)
      .build()

  /** An entity (or, with `ca`, CA-flagged) certificate signed by the test CA, valid from 30 days ago to `notAfter` */
  private def signedByTestCa(key: PublicKey, id: UUID, notAfter: Instant, ca: Boolean = false): X509Certificate =
    val builder = new JcaX509v3CertificateBuilder(
      TestTLS.ca.certificate,
      BigInteger.valueOf(Instant.now.toEpochMilli),
      Date.from(Instant.now.minus(JDuration.ofDays(30))),
      Date.from(notAfter),
      new X500Name(s"CN=$id"),
      key)
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca))
    val usage = if ca then KeyUsage.digitalSignature | KeyUsage.keyCertSign else KeyUsage.digitalSignature
    builder.addExtension(Extension.keyUsage, true, new KeyUsage(usage))
    builder.addExtension(Extension.subjectAlternativeName, false,
      new GeneralNames(new GeneralName(GeneralName.uniformResourceIdentifier, s"urn:uuid:$id")))
    val signer = new JcaContentSignerBuilder("SHA256withECDSA").build(TestTLS.ca.privateKey)
    new JcaX509CertificateConverter().getCertificate(builder.build(signer))

  private def clientWith(ctx: QuicSslContext): QuicTestPeer = track(QuicTestPeer.client(ctx))

  private def server(creds: TlsCredentials): QuicTestPeer = track(QuicTestPeer.server(QuicTls.serverContext(creds)))

  private def client(creds: TlsCredentials): QuicTestPeer = track(QuicTestPeer.client(QuicTls.clientContext(creds)))

  /** Server rejected the client: the server records a failed handshake and the client's connection closes */
  private def assertServerRejects(srv: QuicTestPeer, cli: QuicTestPeer): Unit =
    // Under TLS 1.3 the client may consider the handshake done before the server has checked its certificate
    cli.connect(srv.localAddress) match
      case Left(_) => ()
      case Right(ch) => ch.closeFuture().await(QuicTestPeer.AwaitMillis) shouldBe true
    srv.awaitHandshake() shouldBe a [QuicTestPeer.HandshakeOutcome.Failed]
    srv.handshakes.isEmpty shouldBe true
    srv.received.isEmpty shouldBe true

  /** Client rejected the server: connecting fails with a TLS error and the server never completes a handshake */
  private def assertClientRejects(srv: QuicTestPeer, cli: QuicTestPeer): Unit =
    cli.connect(srv.localAddress) match
      case Left(t) => t shouldBe a [javax.net.ssl.SSLHandshakeException]
      case Right(_) => fail("client accepted an untrusted server")
    srv.handshakes.poll() should not be QuicTestPeer.HandshakeOutcome.Succeeded

  test("peers with certificates from the same CA connect"):
    val srv = server(goodCreds())
    val cli = client(goodCreds())
    val ch = cli.connect(srv.localAddress).fold(t => fail(t), identity)
    cli.awaitHandshake() shouldBe QuicTestPeer.HandshakeOutcome.Succeeded
    srv.awaitHandshake() shouldBe QuicTestPeer.HandshakeOutcome.Succeeded

    val small = HostHeartbeat(HostId(UUID.randomUUID()))
    QuicTestPeer.sendDatagram(ch, small)
    val d = srv.awaitReceived()
    d.message shouldBe small
    d.transport shouldBe QuicTestPeer.Transport.Datagram

    val payload = Array.tabulate[Byte](64 * 1024)(i => (i * 31).toByte)
    val big = QuicNetFixture.rebuildOf(StoreId(PoolId.BootstrapPoolId, 0), payload)
    QuicTestPeer.sendStream(ch, big)
    val s = srv.awaitReceived()
    s.transport shouldBe a [QuicTestPeer.Transport.Stream]
    s.message match
      case m: OpportunisticRebuild => m.data.getByteArray shouldBe payload
      case other => fail(s"unexpected $other")
    srv.protocolErrors.isEmpty shouldBe true

  test("server with a certificate from a different CA is rejected by the client"):
    assertClientRejects(server(foreignCreds()), client(goodCreds()))

  test("client with a certificate from a different CA is rejected by the server"):
    // The client must trust the server, else it aborts before the server ever sees its certificate
    val foreign = foreignCreds()
    val cli = clientWith(presenting(foreign.privateKey, foreign.certificate, foreign.caCertificate))
    assertServerRejects(server(goodCreds()), cli)

  test("client presenting no certificate is rejected"):
    assertServerRejects(server(goodCreds()), clientWith(presenting(null)))

  test("CA certificate presented as an entity certificate is rejected"):
    // QuicTls would append the CA again, which the key store refuses as an invalid chain. BoringSSL itself refuses
    // to sign with the test CA's key (no digitalSignature key usage), so the connection fails on the client
    val srv = server(goodCreds())
    clientWith(presenting(TestTLS.ca.privateKey, TestTLS.ca.certificate)).connect(srv.localAddress) match
      case Left(t) => t shouldBe a [javax.net.ssl.SSLHandshakeException]
      case Right(_) => fail("client presenting the CA certificate connected")
    srv.handshakes.isEmpty shouldBe true

    // A CA-flagged certificate BoringSSL will sign with reaches AspenTrustManager, which rejects it
    val kp = CertificateAuthority.generateKeyPair()
    val subCa = signedByTestCa(kp.getPublic, UUID.randomUUID(), notAfter = Instant.now.plus(JDuration.ofDays(1)),
      ca = true)
    the [InvalidPeerCertificate] thrownBy CertificateVerifier.verify(subCa, TestTLS.ca.certificate) should have message
      "peer certificate must not be a CA certificate"
    assertServerRejects(srv, clientWith(presenting(kp.getPrivate, subCa, TestTLS.ca.certificate)))

  test("expired certificate is rejected"):
    val kp = CertificateAuthority.generateKeyPair()
    val cert = signedByTestCa(kp.getPublic, UUID.randomUUID(), notAfter = Instant.now.minus(JDuration.ofDays(1)))
    val expired = TlsCredentials(TestTLS.ca.certificate, cert, kp.getPrivate)
    the [InvalidPeerCertificate] thrownBy CertificateVerifier.verify(cert, TestTLS.ca.certificate) should have message
      s"certificate verification failed: ${(the [CertificateExpiredException] thrownBy cert.checkValidity()).getMessage}"

    assertServerRejects(server(goodCreds()), client(expired))
    assertClientRejects(server(expired), client(goodCreds()))

  test("two clients sharing one certificate both connect"):
    val srv = server(goodCreds())
    val shared = goodCreds()
    val chans = Seq(client(shared), client(shared)).map: c =>
      val ch = c.connect(srv.localAddress).fold(t => fail(t), identity)
      c.awaitHandshake() shouldBe QuicTestPeer.HandshakeOutcome.Succeeded
      ch
    srv.awaitHandshake() shouldBe QuicTestPeer.HandshakeOutcome.Succeeded
    srv.awaitHandshake() shouldBe QuicTestPeer.HandshakeOutcome.Succeeded
    val msgs = chans.map: ch =>
      val m = HostHeartbeat(HostId(UUID.randomUUID()))
      QuicTestPeer.sendDatagram(ch, m)
      m
    Set(srv.awaitReceived().message, srv.awaitReceived().message) shouldBe msgs.toSet

  test("TlsCredentials.fromPem decodes PEM credentials"):
    val (certPem, keyPem) = TestTLS.credentials(UUID.randomUUID())
    val creds = TlsCredentials.fromPem(TestTLS.caCertificatePem, certPem, keyPem)
    creds.caCertificate shouldBe TestTLS.ca.certificate
    creds.certificate shouldBe Pem.decodeCertificate(certPem)
    creds.privateKey.getEncoded shouldBe Pem.decodePrivateKey(keyPem).getEncoded

  test("stallStreams leaves inbound stream data unread so flow control fills"):
    val window = 64L * 1024
    val config = QuicConfig(initialMaxData = window * 4, initialMaxStreamDataUnidirectional = window)
    val srv = track(QuicTestPeer.server(QuicTls.serverContext(goodCreds()), config, stallStreams = true))
    val cli = track(QuicTestPeer.client(QuicTls.clientContext(goodCreds()), config))
    val ch = cli.connect(srv.localAddress).fold(t => fail(t), identity)
    val msg = QuicNetFixture.rebuildOf(StoreId(PoolId.BootstrapPoolId, 0), new Array[Byte]((window * 2).toInt))
    val stream = QuicTestPeer.openStream(ch)
    val write = QuicTestPeer.writeFrame(stream, msg)
    write.await(500) shouldBe false
    srv.received.isEmpty shouldBe true
    stream.isWritable shouldBe false

  // Declared last so it covers every test above
  test("no ByteBuf leaks"):
    leakWatch.leaks() shouldBe Nil
