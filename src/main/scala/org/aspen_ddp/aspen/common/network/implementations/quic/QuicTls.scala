package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.handler.codec.quic.{QuicSslContext, QuicSslContextBuilder}
import io.netty.handler.ssl.ClientAuth
import org.aspen_ddp.aspen.common.network.tls.{AspenTrustManager, Pem}

import java.security.PrivateKey
import java.security.cert.X509Certificate

final case class TlsCredentials(caCertificate: X509Certificate,
                                certificate: X509Certificate,
                                privateKey: PrivateKey)

object TlsCredentials:
  def fromPem(caCertificatePem: String, certificatePem: String, privateKeyPem: String): TlsCredentials =
    TlsCredentials(Pem.decodeCertificate(caCertificatePem),
      Pem.decodeCertificate(certificatePem),
      Pem.decodePrivateKey(privateKeyPem))

/** BoringSSL contexts for mutual TLS against the Aspen CA.
  *
  * Peer verification is delegated entirely to `AspenTrustManager`, which BoringSSL accepts directly because it is an
  * `X509ExtendedTrustManager`. The certificate UUID is not compared to any routing identity. Hostname verification is
  * disabled on the client since Aspen identities are UUIDs, not host names.
  */
object QuicTls:
  val ApplicationProtocol: String = "aspen/1"

  def serverContext(creds: TlsCredentials): QuicSslContext =
    QuicSslContextBuilder.forServer(creds.privateKey, null, creds.certificate, creds.caCertificate)
      .trustManager(new AspenTrustManager(creds.caCertificate))
      .clientAuth(ClientAuth.REQUIRE)
      .applicationProtocols(ApplicationProtocol)
      .build()

  def clientContext(creds: TlsCredentials): QuicSslContext =
    QuicSslContextBuilder.forClient()
      .keyManager(creds.privateKey, null, creds.certificate, creds.caCertificate)
      .trustManager(new AspenTrustManager(creds.caCertificate))
      .endpointIdentificationAlgorithm(null)
      .applicationProtocols(ApplicationProtocol)
      .build()
