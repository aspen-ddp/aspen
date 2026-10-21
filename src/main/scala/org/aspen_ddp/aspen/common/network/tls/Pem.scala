package org.aspen_ddp.aspen.common.network.tls

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.openssl.PEMParser
import org.bouncycastle.openssl.jcajce.{JcaPEMKeyConverter, JcaPEMWriter, JcaPKCS8Generator}

import java.io.{StringReader, StringWriter}
import java.security.PrivateKey
import java.security.cert.X509Certificate

/** PEM encoding of certificates and PKCS#8 private keys. Output always uses '\n' line endings. */
object Pem:

  private def write(obj: AnyRef): String =
    val sw = new StringWriter()
    val w = new JcaPEMWriter(sw)
    w.writeObject(obj)
    w.close()
    sw.toString.replace("\r\n", "\n")

  def encodeCertificate(cert: X509Certificate): String = write(cert)

  def encodePrivateKey(key: PrivateKey): String = write(new JcaPKCS8Generator(key, null))

  private def parse(pem: String): AnyRef =
    val parser = new PEMParser(new StringReader(pem))
    try
      parser.readObject()
    finally
      parser.close()

  def decodeCertificate(pem: String): X509Certificate =
    parse(pem) match
      case h: X509CertificateHolder => new JcaX509CertificateConverter().getCertificate(h)
      case _ => throw new IllegalArgumentException("PEM does not contain a certificate")

  def decodePrivateKey(pem: String): PrivateKey =
    parse(pem) match
      case i: PrivateKeyInfo => new JcaPEMKeyConverter().getPrivateKey(i)
      case _ => throw new IllegalArgumentException("PEM does not contain a PKCS#8 private key")
