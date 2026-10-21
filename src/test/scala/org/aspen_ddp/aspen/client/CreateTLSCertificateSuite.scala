package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestTLS}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.network.tls.{CertificateAuthority, CertificateVerifier, Pem}
import org.aspen_ddp.aspen.common.objects.DataObjectPointer

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.concurrent.ExecutionContext

class CreateTLSCertificateSuite extends IntegrationTestSuite:

  atest("bootstrap stores the CA certificate and private key in the radicle"):
    given ExecutionContext = executionContext
    for
      kvos <- client.read(radicle)
      certPtr = DataObjectPointer(kvos.contents(Radicle.CACertificateKey).value.bytes)
      keyPtr = DataObjectPointer(kvos.contents(Radicle.CAPrivateKeyKey).value.bytes)
      certDos <- client.read(certPtr)
      keyDos <- client.read(keyPtr)
    yield
      new String(certDos.data.getByteArray, UTF_8) should be(Pem.encodeCertificate(TestTLS.ca.certificate))
      Pem.decodePrivateKey(new String(keyDos.data.getByteArray, UTF_8)).getEncoded should be(TestTLS.ca.privateKey.getEncoded)

  atest("createTLSCertificate signs a certificate for the requested UUID"):
    given ExecutionContext = executionContext
    val id = UUID.randomUUID()
    client.createTLSCertificate(id, CertificateAuthority.generateKeyPair().getPublic).map: cert =>
      CertificateVerifier.verify(cert, TestTLS.ca.certificate) should be(id)
