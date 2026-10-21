package org.aspen_ddp.aspen.common.network.tls

import java.net.Socket
import java.security.cert.{CertificateException, X509Certificate}
import java.security.{KeyStore, PrivateKey}
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLEngine, X509ExtendedTrustManager}

/** Trusts exactly the certificates issued by the Aspen system's CA. No hostname verification is performed;
  * a peer's identity is the UUID in its certificate. Network plugins obtain it by calling
  * CertificateVerifier.verify on session.getPeerCertificates()(0).
  */
class AspenTrustManager(caCertificate: X509Certificate) extends X509ExtendedTrustManager:

  private def check(chain: Array[X509Certificate]): Unit =
    if chain == null || chain.isEmpty then
      throw new CertificateException("empty certificate chain")
    try
      CertificateVerifier.verify(chain(0), caCertificate)
    catch
      case e: InvalidPeerCertificate => throw new CertificateException(e.getMessage, e)

  override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = check(chain)

  override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = check(chain)

  override def checkClientTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit =
    check(chain)

  override def checkServerTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit =
    check(chain)

  override def checkClientTrusted(chain: Array[X509Certificate], authType: String, engine: SSLEngine): Unit =
    check(chain)

  override def checkServerTrusted(chain: Array[X509Certificate], authType: String, engine: SSLEngine): Unit =
    check(chain)

  override def getAcceptedIssuers: Array[X509Certificate] = Array(caCertificate)

object TLSContext:

  def create(caCertificate: X509Certificate, certificate: X509Certificate, privateKey: PrivateKey): SSLContext =
    val password = Array.emptyCharArray
    val ks = KeyStore.getInstance("PKCS12")
    ks.load(null, null)
    ks.setKeyEntry("aspen", privateKey, password, Array(certificate, caCertificate))
    val kmf = KeyManagerFactory.getInstance("PKIX")
    kmf.init(ks, password)
    val ctx = SSLContext.getInstance("TLSv1.3")
    ctx.init(kmf.getKeyManagers, Array(new AspenTrustManager(caCertificate)), null)
    ctx
