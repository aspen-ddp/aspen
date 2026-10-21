package org.aspen_ddp.aspen.common.network.tls

import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.{BasicConstraints, Extension}
import org.bouncycastle.cert.jcajce.{JcaX509CertificateConverter, JcaX509v3CertificateBuilder}
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.math.BigInteger
import java.nio.ByteBuffer
import java.security.cert.X509Certificate
import java.time.{Duration, Instant}
import java.util.{Date, UUID}
import javax.net.ssl.{SSLContext, SSLEngine, SSLEngineResult, SSLException, SSLSession}

class TLSSuite extends AnyFunSuite with Matchers:

  val sysId = UUID.randomUUID()
  val ca = CertificateAuthority.create(sysId)

  private def contextFor(authority: CertificateAuthority, trusted: X509Certificate, id: UUID): SSLContext =
    val kp = CertificateAuthority.generateKeyPair()
    val cert = authority.signCertificate(id, kp.getPublic)
    TLSContext.create(trusted, cert, kp.getPrivate)

  private def runTasks(e: SSLEngine): Unit =
    var t = e.getDelegatedTask
    while t != null do
      t.run()
      t = e.getDelegatedTask

  private def handshake(clientCtx: SSLContext, serverCtx: SSLContext): (SSLSession, SSLSession) =
    val client = clientCtx.createSSLEngine()
    client.setUseClientMode(true)
    val server = serverCtx.createSSLEngine()
    server.setUseClientMode(false)
    server.setNeedClientAuth(true)
    client.beginHandshake()
    server.beginHandshake()

    val cToS = ByteBuffer.allocate(64 * 1024)
    val sToC = ByteBuffer.allocate(64 * 1024)
    val empty = ByteBuffer.allocate(0)
    val cApp = ByteBuffer.allocate(64 * 1024)
    val sApp = ByteBuffer.allocate(64 * 1024)

    def step(e: SSLEngine, out: ByteBuffer, in: ByteBuffer, app: ByteBuffer): Unit =
      e.getHandshakeStatus match
        case SSLEngineResult.HandshakeStatus.NEED_WRAP =>
          e.wrap(empty, out)
        case SSLEngineResult.HandshakeStatus.NEED_UNWRAP =>
          in.flip()
          if in.hasRemaining then
            e.unwrap(in, app)
          in.compact()
        case SSLEngineResult.HandshakeStatus.NEED_TASK =>
          runTasks(e)
        case _ =>

    def done(e: SSLEngine) =
      val s = e.getHandshakeStatus
      s == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING || s == SSLEngineResult.HandshakeStatus.FINISHED

    var iterations = 0
    while !(done(client) && done(server)) && iterations < 1000 do
      step(client, cToS, sToC, cApp)
      step(server, sToC, cToS, sApp)
      iterations += 1
    if !(done(client) && done(server)) then
      throw new IllegalStateException("handshake did not complete")
    (client.getSession, server.getSession)

  test("CA certificate is a self-signed CA"):
    ca.certificate.getBasicConstraints should be >= 0
    ca.certificate.getSubjectX500Principal shouldBe ca.certificate.getIssuerX500Principal
    ca.certificate.getSubjectX500Principal.getName should include (s"Aspen CA $sysId")
    ca.certificate.verify(ca.certificate.getPublicKey)

  test("signed certificate carries the UUID and verifies against the CA"):
    val id = UUID.randomUUID()
    val cert = ca.signCertificate(id, CertificateAuthority.generateKeyPair().getPublic)
    cert.getBasicConstraints shouldBe -1
    cert.getSubjectX500Principal.getName shouldBe s"CN=$id"
    CertificateVerifier.extractId(cert) shouldBe Some(id)
    CertificateVerifier.verify(cert, ca.certificate) shouldBe id
    cert.getExtendedKeyUsage should contain allOf ("1.3.6.1.5.5.7.3.1", "1.3.6.1.5.5.7.3.2")
    cert.getNotAfter.after(Date.from(Instant.now.plus(Duration.ofDays(365L * 99)))) shouldBe true

  test("verify rejects a certificate from a foreign CA"):
    val other = CertificateAuthority.create(UUID.randomUUID())
    val cert = other.signCertificate(UUID.randomUUID(), CertificateAuthority.generateKeyPair().getPublic)
    an [InvalidPeerCertificate] should be thrownBy CertificateVerifier.verify(cert, ca.certificate)

  test("verify rejects a certificate without a urn:uuid SAN"):
    CertificateVerifier.extractId(ca.certificate) shouldBe None
    // Non-CA entity cert, validly signed by ca, but with no SAN extension
    val kp = CertificateAuthority.generateKeyPair()
    val now = Instant.now
    val builder = new JcaX509v3CertificateBuilder(
      ca.certificate,
      BigInteger.valueOf(12345),
      Date.from(now.minus(Duration.ofHours(1))),
      Date.from(now.plus(Duration.ofDays(365))),
      new X500Name("CN=" + UUID.randomUUID()),
      kp.getPublic)
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false))
    val signer = new JcaContentSignerBuilder("SHA256withECDSA").build(ca.privateKey)
    val cert = new JcaX509CertificateConverter().getCertificate(builder.build(signer))
    cert.getBasicConstraints shouldBe -1
    cert.verify(ca.certificate.getPublicKey)
    CertificateVerifier.extractId(cert) shouldBe None
    an [InvalidPeerCertificate] should be thrownBy CertificateVerifier.verify(cert, ca.certificate)
    // The CA cert itself is also rejected
    an [InvalidPeerCertificate] should be thrownBy CertificateVerifier.verify(ca.certificate, ca.certificate)

  test("PEM round-trips certificates and private keys"):
    val kp = CertificateAuthority.generateKeyPair()
    val cert = ca.signCertificate(UUID.randomUUID(), kp.getPublic)
    val certPem = Pem.encodeCertificate(cert)
    val keyPem = Pem.encodePrivateKey(kp.getPrivate)
    certPem should startWith ("-----BEGIN CERTIFICATE-----\n")
    keyPem should startWith ("-----BEGIN PRIVATE KEY-----\n")
    certPem should endWith ("-----END CERTIFICATE-----\n")
    certPem should not include ("\r")
    Pem.decodeCertificate(certPem) shouldBe cert
    Pem.decodePrivateKey(keyPem).getEncoded shouldBe kp.getPrivate.getEncoded
    Pem.encodeCertificate(Pem.decodeCertificate(certPem)) shouldBe certPem

  test("PEM decode rejects the wrong PEM type"):
    val kp = CertificateAuthority.generateKeyPair()
    an [IllegalArgumentException] should be thrownBy Pem.decodeCertificate(Pem.encodePrivateKey(kp.getPrivate))
    an [IllegalArgumentException] should be thrownBy Pem.decodePrivateKey(Pem.encodeCertificate(ca.certificate))

  test("TLSContext completes a mutually authenticated handshake"):
    val (clientId, serverId) = (UUID.randomUUID(), UUID.randomUUID())
    val clientCtx = contextFor(ca, ca.certificate, clientId)
    val serverCtx = contextFor(ca, ca.certificate, serverId)
    val (clientSession, serverSession) = handshake(clientCtx, serverCtx)
    val serverSeenPeer = serverSession.getPeerCertificates()(0).asInstanceOf[X509Certificate]
    val clientSeenPeer = clientSession.getPeerCertificates()(0).asInstanceOf[X509Certificate]
    CertificateVerifier.verify(serverSeenPeer, ca.certificate) shouldBe clientId
    CertificateVerifier.verify(clientSeenPeer, ca.certificate) shouldBe serverId

  test("TLSContext handshake fails against a foreign CA"):
    val other = CertificateAuthority.create(UUID.randomUUID())
    val serverCtx = contextFor(ca, ca.certificate, UUID.randomUUID())
    val clientCtx = contextFor(other, other.certificate, UUID.randomUUID())
    an [SSLException] should be thrownBy handshake(clientCtx, serverCtx)
