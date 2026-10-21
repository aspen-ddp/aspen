package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.common.metadata.BootstrapConfig
import org.aspen_ddp.aspen.common.network.tls.{CertificateAuthority, Pem}

import java.util.UUID

/** Shared TLS fixture for tests. A single CA is created once per JVM; credentials are
  * generated on demand as fresh key pairs signed by that CA.
  */
object TestTLS:
  val ca: CertificateAuthority = CertificateAuthority.create(new UUID(0, 0))

  val caCertificatePem: String = Pem.encodeCertificate(ca.certificate)

  val cli: BootstrapConfig.CLICredentials = BootstrapConfig.CLICredentials.generate(ca)

  /** Returns (certificatePem, privateKeyPem) for a fresh key pair signed by `ca` */
  def credentials(id: UUID): (String, String) =
    val kp = CertificateAuthority.generateKeyPair()
    val cert = ca.signCertificate(id, kp.getPublic)
    (Pem.encodeCertificate(cert), Pem.encodePrivateKey(kp.getPrivate))
