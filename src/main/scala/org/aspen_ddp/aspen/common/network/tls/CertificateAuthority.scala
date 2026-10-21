package org.aspen_ddp.aspen.common.network.tls

import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.{BasicConstraints, ExtendedKeyUsage, Extension, GeneralName, GeneralNames,
  KeyPurposeId, KeyUsage}
import org.bouncycastle.cert.jcajce.{JcaX509CertificateConverter, JcaX509ExtensionUtils, JcaX509v3CertificateBuilder}
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder

import java.math.BigInteger
import java.security.spec.ECGenParameterSpec
import java.security.{KeyPair, KeyPairGenerator, PrivateKey, PublicKey, SecureRandom}
import java.security.cert.X509Certificate
import java.time.{Duration, Instant, ZoneOffset}
import java.util.{Date, UUID}

case class CertificateAuthority(certificate: X509Certificate, privateKey: PrivateKey):

  /** Signs an entity certificate whose identity is the supplied UUID (subject CN and urn:uuid SAN) */
  def signCertificate(id: UUID, publicKey: PublicKey): X509Certificate =
    val utils = new JcaX509ExtensionUtils()
    val builder = new JcaX509v3CertificateBuilder(
      certificate,
      CertificateAuthority.randomSerial(),
      CertificateAuthority.notBefore(),
      CertificateAuthority.notAfter(),
      new X500Name(s"CN=$id"),
      publicKey)
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false))
    builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature))
    builder.addExtension(Extension.extendedKeyUsage, false,
      new ExtendedKeyUsage(Array(KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth)))
    builder.addExtension(Extension.subjectAlternativeName, false,
      new GeneralNames(new GeneralName(GeneralName.uniformResourceIdentifier, s"urn:uuid:$id")))
    builder.addExtension(Extension.authorityKeyIdentifier, false, utils.createAuthorityKeyIdentifier(certificate))
    builder.addExtension(Extension.subjectKeyIdentifier, false, utils.createSubjectKeyIdentifier(publicKey))
    val signer = new JcaContentSignerBuilder(CertificateAuthority.SignatureAlgorithm).build(privateKey)
    new JcaX509CertificateConverter().getCertificate(builder.build(signer))

object CertificateAuthority:

  val ValidityYears: Int = 100

  private val SignatureAlgorithm = "SHA256withECDSA"
  private val random = new SecureRandom()

  def generateKeyPair(): KeyPair =
    val gen = KeyPairGenerator.getInstance("EC")
    gen.initialize(new ECGenParameterSpec("secp256r1"))
    gen.generateKeyPair()

  private def randomSerial(): BigInteger = new BigInteger(127, random).add(BigInteger.ONE)

  // Backdated by an hour to tolerate clock skew between hosts
  private def notBefore(): Date = Date.from(Instant.now.minus(Duration.ofHours(1)))

  private def notAfter(): Date =
    Date.from(Instant.now.atZone(ZoneOffset.UTC).plusYears(ValidityYears).toInstant)

  def create(aspenSystemId: UUID): CertificateAuthority =
    val kp = generateKeyPair()
    val name = new X500Name(s"CN=Aspen CA $aspenSystemId")
    val utils = new JcaX509ExtensionUtils()
    val builder = new JcaX509v3CertificateBuilder(
      name, randomSerial(), notBefore(), notAfter(), name, kp.getPublic)
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true))
    builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign))
    builder.addExtension(Extension.subjectKeyIdentifier, false, utils.createSubjectKeyIdentifier(kp.getPublic))
    val signer = new JcaContentSignerBuilder(SignatureAlgorithm).build(kp.getPrivate)
    CertificateAuthority(new JcaX509CertificateConverter().getCertificate(builder.build(signer)), kp.getPrivate)
