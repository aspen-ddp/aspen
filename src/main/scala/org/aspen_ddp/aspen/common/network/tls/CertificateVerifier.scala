package org.aspen_ddp.aspen.common.network.tls

import java.security.GeneralSecurityException
import java.security.cert.X509Certificate
import java.util.UUID

class InvalidPeerCertificate(reason: String) extends Exception(reason)

object CertificateVerifier:

  private val UuidPrefix = "urn:uuid:"
  private val UriNameType = 6

  /** Returns the UUID from the certificate's urn:uuid URI SAN, or None if absent or unparseable */
  def extractId(cert: X509Certificate): Option[UUID] =
    try
      Option(cert.getSubjectAlternativeNames) match
        case None => None
        case Some(names) =>
          var result: Option[UUID] = None
          val it = names.iterator()
          while result.isEmpty && it.hasNext do
            val entry = it.next()
            if entry.get(0) == Integer.valueOf(UriNameType) then
              entry.get(1) match
                case s: String if s.startsWith(UuidPrefix) =>
                  try
                    result = Some(UUID.fromString(s.substring(UuidPrefix.length)))
                  catch
                    case _: IllegalArgumentException => ()
                case _ => ()
          result
    catch
      case _: GeneralSecurityException => None

  /** Verifies that the certificate is a valid, non-CA entity certificate issued by the CA and returns the
    * UUID it identifies.
    */
  def verify(cert: X509Certificate, caCertificate: X509Certificate): UUID =
    try
      cert.checkValidity()
      if cert.getIssuerX500Principal != caCertificate.getSubjectX500Principal then
        throw new InvalidPeerCertificate("certificate was not issued by the Aspen CA")
      cert.verify(caCertificate.getPublicKey)
      if cert.getBasicConstraints != -1 then
        throw new InvalidPeerCertificate("peer certificate must not be a CA certificate")
      extractId(cert).getOrElse(throw new InvalidPeerCertificate("certificate has no urn:uuid subject alternative name"))
    catch
      case e: InvalidPeerCertificate => throw e
      case e: GeneralSecurityException =>
        throw new InvalidPeerCertificate(s"certificate verification failed: ${e.getMessage}")
