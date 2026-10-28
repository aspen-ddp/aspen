package org.aspen_ddp.aspen.common.config

import org.aspen_ddp.aspen.common.network.tls.{CertificateAuthority, Pem}
import org.aspen_ddp.aspen.common.util.YamlFormat.*
import org.aspen_ddp.aspen.common.util.atomicWrite

import java.util.UUID
import scala.util.control.NonFatal

/*
aspen-system-id: BF1049AD-D2A8-4D17-8080-E01A4678C8B3
certificate: |
  -----BEGIN CERTIFICATE-----
  ...
  -----END CERTIFICATE-----
private-key: |
  -----BEGIN PRIVATE KEY-----
  ...
  -----END PRIVATE KEY-----
bootstrap-hosts:
  - "127.0.0.1:5001"
  - "[fd00::5]:5001"
*/

/** The small, per-client file holding a client's identity and the addresses it should contact to
 *  fetch the bootstrap config. The bootstrap config itself is fetched from the system and holds
 *  nothing client-specific.
 */
object ClientConfig:

  val configFilename: String = "aspen-client-config.yaml"

  /** Credentials a client uses to authenticate to the system. */
  case class ClientCredentials(certificate: String, privateKey: String)

  object ClientCredentials:
    /** The certificate subject is a random UUID. */
    def generate(ca: CertificateAuthority): ClientCredentials =
      val clientId = UUID.randomUUID()
      val keyPair = CertificateAuthority.generateKeyPair()
      val cert = ca.signCertificate(clientId, keyPair.getPublic)
      ClientCredentials(Pem.encodeCertificate(cert), Pem.encodePrivateKey(keyPair.getPrivate))

  /** The HTTP endpoint of a bootstrap host. IPv6 literals keep their brackets. */
  case class BootstrapEndpoint(address: String, port: Int):
    override def toString: String = s"$address:$port"

  object BootstrapEndpoint:
    private def parseWith(s: String, defaultPort: Option[Int]): Either[String, BootstrapEndpoint] =
      def bad(why: String): Either[String, BootstrapEndpoint] =
        Left(s"Invalid bootstrap endpoint '$s': $why")

      def build(address: String, portText: Option[String]): Either[String, BootstrapEndpoint] =
        if address.isEmpty || address == "[]" then
          bad("empty address")
        else if address.exists(_.isWhitespace) then
          bad("address contains whitespace")
        else
          portText match
            case Some(p) =>
              p.toIntOption match
                case Some(port) if port >= 1 && port <= 65535 => Right(BootstrapEndpoint(address, port))
                case _ => bad("port must be an integer from 1 to 65535")
            case None =>
              defaultPort match
                case Some(port) => Right(BootstrapEndpoint(address, port))
                case None => bad("a port is required")

      if s.startsWith("[") then
        val close = s.indexOf(']')
        if close < 0 then
          bad("missing closing bracket")
        else
          val rest = s.substring(close + 1)
          if rest.isEmpty then
            build(s.substring(0, close + 1), None)
          else if rest.startsWith(":") then
            build(s.substring(0, close + 1), Some(rest.substring(1)))
          else
            bad("unexpected text after the closing bracket")
      else
        val colon = s.lastIndexOf(':')
        if colon < 0 then
          build(s, None)
        else
          val address = s.substring(0, colon)
          if address.contains(':') then
            bad("IPv6 literals must be bracketed, e.g. [fd00::5]")
          else
            build(address, Some(s.substring(colon + 1)))

    /** Accepts `name`, `name:port`, `[ipv6]` and `[ipv6]:port`. A missing port uses `defaultPort`. */
    def parse(s: String, defaultPort: Int): Either[String, BootstrapEndpoint] =
      parseWith(s, Some(defaultPort))

    /** The yaml form always carries an explicit port */
    object YEndpoint extends Format[BootstrapEndpoint]:
      def format(o: Object): BootstrapEndpoint =
        parseWith(YString.format(o), None) match
          case Right(e) => e
          case Left(err) => throw new FormatError(err)

  case class Config(aspenSystemId: UUID,
                    credentials: ClientCredentials,
                    bootstrapHosts: List[BootstrapEndpoint]):
    try
      Pem.decodeCertificate(credentials.certificate)
    catch
      case NonFatal(t) => throw new FormatError(s"certificate is not a valid PEM certificate: $t")

    try
      Pem.decodePrivateKey(credentials.privateKey)
    catch
      case NonFatal(t) => throw new FormatError(s"private-key is not a valid PEM private key: $t")

    if bootstrapHosts.isEmpty then
      throw new FormatError("bootstrap-hosts must contain at least one endpoint")

  object Config extends YObject[Config]:
    val aspenSystemId: Required[UUID]                     = Required("aspen-system-id", YUUID)
    val certificate: Required[String]                     = Required("certificate", YString)
    val privateKey: Required[String]                      = Required("private-key", YString)
    val bootstrapHosts: Required[List[BootstrapEndpoint]] =
      Required("bootstrap-hosts", YList(BootstrapEndpoint.YEndpoint))

    val attrs: List[Attr] = aspenSystemId :: certificate :: privateKey :: bootstrapHosts :: Nil

    def create(o: Object): Config = Config(
      aspenSystemId.get(o),
      ClientCredentials(certificate.get(o), privateKey.get(o)),
      bootstrapHosts.get(o))

  def generate(cfg: Config): String =
    val sb = StringBuilder()
    sb.append(s"aspen-system-id: ${cfg.aspenSystemId}\n")
    sb.append(yamlBlockScalar("certificate", cfg.credentials.certificate))
    sb.append(yamlBlockScalar("private-key", cfg.credentials.privateKey))
    sb.append("bootstrap-hosts:\n")
    cfg.bootstrapHosts.foreach: e =>
      sb.append(s"  - ${yamlQuote(e.toString)}\n")
    sb.toString

  def parse(yaml: String): Config =
    Config.create(loadYamlString(yaml))

  def load(path: os.Path): Config =
    Config.create(loadYamlFile(path))

  /** Atomic and owner-only: atomicWrite's temp file is created 0600 and then renamed into place. */
  def write(path: os.Path, cfg: Config): Unit =
    atomicWrite(path, generate(cfg))

  /** The HTTP endpoints of the hosts in a bootstrap config, in the order given. */
  def endpointsOf(bs: BootstrapConfig.Config): List[BootstrapEndpoint] =
    bs.hosts.map(h => BootstrapEndpoint(h.address, h.httpPort)).distinct

  enum CacheUpdate:
    case Unchanged
    case Updated
    case Failed(error: Throwable)

  /** Refreshes the cached bootstrap host list from a freshly fetched bootstrap config. A host set
   *  that differs only in order is Unchanged. Never throws NonFatal: a failed write is reported
   *  as Failed so a client can carry on with the config it already has.
   */
  def updateCachedHosts(path: os.Path, current: Config, fetched: BootstrapConfig.Config): CacheUpdate =
    val endpoints = endpointsOf(fetched)

    if endpoints.toSet == current.bootstrapHosts.toSet then
      CacheUpdate.Unchanged
    else
      try
        // atomicWrite renames over the target, which would silently replace a read-only file
        if os.exists(path) && !path.toIO.canWrite then
          CacheUpdate.Failed(new java.io.IOException(s"$path is not writable"))
        else
          write(path, current.copy(bootstrapHosts = endpoints))
          CacheUpdate.Updated
      catch
        case NonFatal(t) => CacheUpdate.Failed(t)
