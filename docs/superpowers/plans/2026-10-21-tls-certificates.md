# TLS Certificate Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Aspen runs its own CA and issues UUID-identified, TLS-compatible X.509 certificates to hosts and the CLI, with plugin-agnostic verification code ready for future network plugins.

**Architecture:** A new shared package `org.aspen_ddp.aspen.common.network.tls` holds key generation, CA creation/signing, PEM I/O, peer verification and `SSLContext` construction. Bootstrap creates the CA, stores its cert and private key as PEM DataObjects referenced from the Radicle, and writes certs/keys into `aspen-bootstrap-config.yaml` and `aspen-host-config.yaml`. `AspenClient.createTLSCertificate` signs certs for later hosts.

**Tech Stack:** Scala 3.7.3, JDK 25, Bouncy Castle (`bcprov-jdk18on`, `bcpkix-jdk18on` 1.80) for building/signing certificates and PEM; JDK `java.security` / `javax.net.ssl` for verification and TLS; ScalaTest.

**Spec:** `docs/superpowers/specs/2026-10-21-tls-certificates-design.md`

## Global Constraints

- Bouncy Castle is used only to build/sign certificates and for PEM encode/decode; verification and `SSLContext` use JDK built-ins.
- Keys: ECDSA P-256 (`secp256r1`); signatures `SHA256withECDSA`.
- All keys and certs are stored as PEM text. Private keys are PKCS#8 (`-----BEGIN PRIVATE KEY-----`). PEM strings use `\n` line endings and end with exactly one `\n`.
- Entity certs: subject `CN=<uuid>`, SAN URI `urn:uuid:<uuid>` (the SAN is authoritative), BasicConstraints CA=false, KeyUsage `digitalSignature`, EKU `serverAuth` + `clientAuth`.
- CA cert: self-signed, subject `CN=Aspen CA <aspen-system-id>`, BasicConstraints CA=true (critical), KeyUsage `keyCertSign` + `cRLSign`.
- Validity for CA and entity certs: 100 years from now (notBefore = now minus 1 hour to tolerate clock skew). Serial: random positive 128-bit `BigInteger`.
- All new YAML fields are **required**; no backward compatibility handling.
- The ZeroMQ network implementation (`common/network/implementations/zmqnet/`) is not modified.
- Scala 3 quiet syntax; indented if/then/else; match surrounding comment density.
- Run tests with `sbt 'testOnly *SuiteName'`.

## Review Focus

- A peer cert signed by a different Aspen system's CA must be rejected by `CertificateVerifier.verify` and the trust manager (Task 1 test "verify rejects a certificate from a foreign CA").
- PEM inside YAML block scalars must round-trip byte-exact, including the trailing newline, or a regenerated config silently changes the CLI cert (Tasks 2, 3 round-trip tests use `shouldBe` on the PEM strings).
- `prepRadicleUpdate` regenerating the Radicle's config on a store move must keep the CA cert and CLI credentials unchanged (Task 5 test).
- A failure in `createTLSCertificate` must abort host creation before the registration transaction runs, leaving no registered host and no host config (Task 6 test "createHost registers nothing when certificate signing fails").
- A cert whose SAN is missing or not a `urn:uuid:` URI must be rejected, not crash with a parse exception (Task 1 test).

---

## File Structure

Create (package `org.aspen_ddp.aspen.common.network.tls`, dir `src/main/scala/org/aspen_ddp/aspen/common/network/tls/`):
- `Pem.scala` — PEM encode/decode for certificates and private keys.
- `CertificateAuthority.scala` — key-pair generation, CA creation, entity cert signing.
- `CertificateVerifier.scala` — chain-to-CA verification and UUID extraction; `InvalidPeerCertificate`.
- `TLSContext.scala` — `AspenTrustManager` and `SSLContext` construction.

Create (tests):
- `src/test/scala/org/aspen_ddp/aspen/common/network/tls/TLSSuite.scala`
- `src/test/scala/org/aspen_ddp/aspen/TestTLS.scala` — shared fixed test CA and credentials.
- `src/test/scala/org/aspen_ddp/aspen/client/CreateTLSCertificateSuite.scala`

Modify: `build.sbt`, `common/util/YamlFormat.scala`, `server/HostConfig.scala`, `common/metadata/BootstrapConfig.scala`, `common/Radicle.scala`, `server/store/Bootstrap.scala`, `client/AspenClient.scala`, `server/HostManager.scala`, `cmdline/Main.scala`, `Backlog.txt`, `Operation.md`, and the tests that construct `HostConfig` or call `generateBootstrapConfig` / `Bootstrap.initialize`.

---

### Task 1: Shared TLS module

**Files:**
- Modify: `build.sbt` (add to `libraryDependencies`)
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/tls/{Pem,CertificateAuthority,CertificateVerifier,TLSContext}.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/tls/TLSSuite.scala`

**Interfaces:**
- Produces:
  - `object Pem`:
    `encodeCertificate(cert: X509Certificate): String`, `decodeCertificate(pem: String): X509Certificate`,
    `encodePrivateKey(key: PrivateKey): String`, `decodePrivateKey(pem: String): PrivateKey`
  - `case class CertificateAuthority(certificate: X509Certificate, privateKey: PrivateKey)` with
    `def signCertificate(id: UUID, publicKey: PublicKey): X509Certificate`
  - `object CertificateAuthority`: `def create(aspenSystemId: UUID): CertificateAuthority`,
    `def generateKeyPair(): java.security.KeyPair`, `val ValidityYears: Int = 100`
  - `class InvalidPeerCertificate(reason: String) extends Exception(reason)`
  - `object CertificateVerifier`: `def verify(cert: X509Certificate, caCertificate: X509Certificate): UUID` (throws `InvalidPeerCertificate`), `def extractId(cert: X509Certificate): Option[UUID]`
  - `class AspenTrustManager(caCertificate: X509Certificate) extends X509TrustManager`
  - `object TLSContext`: `def create(caCertificate: X509Certificate, certificate: X509Certificate, privateKey: PrivateKey): SSLContext`

- [ ] **Step 1: Add dependencies to `build.sbt`**

```scala
"org.bouncycastle"                 %  "bcprov-jdk18on"          % "1.80",
"org.bouncycastle"                 %  "bcpkix-jdk18on"          % "1.80",
```

Run: `sbt update` — Expected: success.

- [ ] **Step 2: Write the failing tests in `TLSSuite` (`AnyFunSuite with Matchers`)**

```scala
val sysId = UUID.randomUUID()
val ca = CertificateAuthority.create(sysId)

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
  cert.getNotAfter.after(java.util.Date.from(java.time.Instant.now.plus(java.time.Duration.ofDays(365L * 99)))) shouldBe true

test("verify rejects a certificate from a foreign CA"):
  val other = CertificateAuthority.create(UUID.randomUUID())
  val cert = other.signCertificate(UUID.randomUUID(), CertificateAuthority.generateKeyPair().getPublic)
  an [InvalidPeerCertificate] should be thrownBy CertificateVerifier.verify(cert, ca.certificate)

test("verify rejects a certificate without a urn:uuid SAN"):
  // The CA's own cert has no SAN; it chains to itself, so only the SAN check can reject it.
  CertificateVerifier.extractId(ca.certificate) shouldBe None
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

test("TLSContext completes a mutually authenticated handshake"):
  // Two SSLEngines wired back to back in memory; client requires the server's cert and the
  // server sets needClientAuth. After the handshake each side's peer cert UUID is checked.
  val (clientId, serverId) = (UUID.randomUUID(), UUID.randomUUID())
  // build clientCtx / serverCtx with TLSContext.create from fresh key pairs signed by ca
  val (clientSession, serverSession) = handshake(clientCtx, serverCtx)
  val serverSeenPeer = serverSession.getPeerCertificates()(0).asInstanceOf[X509Certificate]
  val clientSeenPeer = clientSession.getPeerCertificates()(0).asInstanceOf[X509Certificate]
  CertificateVerifier.verify(serverSeenPeer, ca.certificate) shouldBe clientId
  CertificateVerifier.verify(clientSeenPeer, ca.certificate) shouldBe serverId

test("TLSContext handshake fails against a foreign CA"):
  // Same harness; the client's cert is signed by a different CA. Handshake throws SSLException.
```

The handshake harness is a private helper in the suite: `handshake(clientCtx: SSLContext, serverCtx: SSLContext): (SSLSession, SSLSession)` that loops `wrap`/`unwrap` over `ByteBuffer`s, running delegated tasks inline, until both engines report `NOT_HANDSHAKING`/`FINISHED`. Use `TLSv1.3`.

- [ ] **Step 3: Run tests to verify they fail**

Run: `sbt 'testOnly *TLSSuite'` — Expected: compilation failure (types not defined).

- [ ] **Step 4: Implement the four files**

- `Pem`: BC `JcaPEMWriter` over a `StringWriter`; private keys written via `new JcaPKCS8Generator(key, null)` so the output is PKCS#8. Decode with `PEMParser` + `JcaX509CertificateConverter` / `JcaPEMKeyConverter` (the parser yields `PrivateKeyInfo` for PKCS#8). Normalize output to `\n` line endings. Throw `IllegalArgumentException` on input that is not the expected PEM type.
- `CertificateAuthority.create`: `generateKeyPair()` (`KeyPairGenerator.getInstance("EC")` with `ECGenParameterSpec("secp256r1")`), then a self-signed `JcaX509v3CertificateBuilder` with the CA extensions from Global Constraints plus `SubjectKeyIdentifier`. `signCertificate` builds the entity cert with the Global Constraints extensions plus `AuthorityKeyIdentifier`, SAN `GeneralName.uniformResourceIdentifier` = `s"urn:uuid:$id"`, signed with `JcaContentSignerBuilder("SHA256withECDSA")`. Convert with `JcaX509CertificateConverter`. No BC provider registration is needed (JDK providers handle EC).
- `CertificateVerifier.verify`: `cert.checkValidity()`; issuer principal equals CA subject; `cert.verify(caCertificate.getPublicKey)`; `cert.getBasicConstraints == -1`; `extractId` must be `Some`. Every failure (including `GeneralSecurityException`) is rethrown as `InvalidPeerCertificate` with a reason. `extractId` reads `getSubjectAlternativeNames`, finds type 6 (URI) entries starting with `urn:uuid:`, parses the remainder; `None` if absent or unparseable.
- `AspenTrustManager`: `checkClientTrusted`/`checkServerTrusted` call `CertificateVerifier.verify(chain(0), caCertificate)` and translate `InvalidPeerCertificate` into `java.security.cert.CertificateException`; `getAcceptedIssuers = Array(caCertificate)`. No hostname verification. Scaladoc: peer identity is the UUID from the cert; plugins call `CertificateVerifier.verify` on `session.getPeerCertificates()(0)` to obtain it.
- `TLSContext.create`: in-memory `KeyStore` (`PKCS12`, empty password) holding the key + `Array(certificate, caCertificate)` chain, `KeyManagerFactory` (`PKIX`), `SSLContext.getInstance("TLSv1.3")` initialised with those key managers and `Array(AspenTrustManager(caCertificate))`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `sbt 'testOnly *TLSSuite'` — Expected: all 7 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add build.sbt src/main/scala/org/aspen_ddp/aspen/common/network/tls src/test/scala/org/aspen_ddp/aspen/common/network/tls
git commit -m "Add shared TLS certificate module"
```

---

### Task 2: Host config carries TLS certificate and private key

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/util/YamlFormat.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/HostConfig.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/TestTLS.scala`
- Modify tests constructing `HostConfig`: `server/HostConfigSuite.scala`, `server/StorageDeviceManagerSuite.scala:67`, `client/DeviceCheckNudgeSuite.scala:48`
- Modify (to compile only): `server/HostManager.scala:104`, `cmdline/Main.scala:1154` — temporarily pass the cert/key described below; Tasks 6 and 7 replace these.

**Interfaces:**
- Consumes: Task 1 `CertificateAuthority`, `Pem`.
- Produces:
  - `YamlFormat.yamlBlockScalar(key: String, text: String, indent: String = ""): String` — emits `s"$indent$key: |\n"` followed by every line of `text` prefixed with `indent + "  "`, each ending in `\n`. `text` must end with `\n` (`require`).
  - `HostConfig(..., crl: HostConfig.CRLBackend, tlsCertificate: String, tlsPrivateKey: String)` — PEM strings, new required YAML keys `tls-certificate` and `tls-private-key` (emitted after `crl`).
  - `object TestTLS` (test scope): `val ca: CertificateAuthority` (created once for `UUID(0,0)`), `def credentials(id: UUID): (String, String)` returning `(certPem, keyPem)` for a fresh key pair signed by `ca`.

- [ ] **Step 1: Write failing tests**

In `HostConfigSuite`, build `sample` with `TestTLS.credentials(hostUuid)` and add:

```scala
test("round trip preserves the TLS certificate and private key exactly"):
  // write sample.yamlConfig to a temp file, load it
  loaded.tlsCertificate shouldBe sample.tlsCertificate
  loaded.tlsPrivateKey shouldBe sample.tlsPrivateKey
  CertificateVerifier.verify(Pem.decodeCertificate(loaded.tlsCertificate), TestTLS.ca.certificate) shouldBe sample.hostId.uuid

test("loadHostConfig rejects a config without tls-certificate"):
  // sample.yamlConfig with the tls-certificate block removed
  a [FormatError] should be thrownBy HostConfig.loadHostConfig(f.toFile)
```

Update the existing doc-example test and `yamlConfig` format test for the new keys.

- [ ] **Step 2: Run to verify failure**

Run: `sbt 'testOnly *HostConfigSuite'` — Expected: compile failure.

- [ ] **Step 3: Implement**

Add `yamlBlockScalar` to `YamlFormat`. Add the two fields, `Required("tls-certificate", YString)`/`Required("tls-private-key", YString)` attrs, and append them in `yamlConfig` via `yamlBlockScalar`. Extend the doc comment example with abbreviated PEM blocks. Update the other `HostConfig(...)` call sites: tests use `TestTLS.credentials`; `HostManager` and `Main.bootstrap` temporarily call `CertificateAuthority.create(aspenSystemId)` to sign a throwaway cert (marked `// replaced in a later task`).

- [ ] **Step 4: Run to verify pass**

Run: `sbt 'testOnly *HostConfigSuite *StorageDeviceManagerSuite *DeviceCheckNudgeSuite'` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/TestTLS.scala
git commit -am "Add TLS certificate and private key to host config"
```

---

### Task 3: Bootstrap config carries CA certificate and CLI credentials

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala`
- Modify call sites to compile: `server/store/Bootstrap.scala:31`, `cmdline/Main.scala:1220`, `BootstrapConfig.prepRadicleUpdate` (line 220), `src/test/.../common/network/MetadataManagerFixture.scala:320`, `src/test/.../server/HostManagerSuite.scala:58`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigSuite.scala`

**Interfaces:**
- Consumes: Task 1 (`CertificateAuthority`, `Pem`), Task 2 (`yamlBlockScalar`, `TestTLS`).
- Produces:
  - `case class BootstrapConfig.CLICredentials(cliId: UUID, certificate: String, privateKey: String)` with `object CLICredentials { def generate(ca: CertificateAuthority): CLICredentials }` (random UUID, fresh key pair, PEM).
  - `Config(aspenSystemId: UUID, bootstrapIDA: IDA, hosts: List[BootstrapHost], caCertificate: String, cli: CLICredentials)`; required top-level YAML keys `ca-certificate`, `cli-id`, `cli-certificate`, `cli-private-key`.
  - `generateBootstrapConfig(aspenSystemId: UUID, ida: IDA, hostStates: List[HostState], storeMap: List[(StoreId, HostId)], caCertificate: String, cli: CLICredentials): String` — new keys emitted after `bootstrap-hosts`.
  - `TestTLS.cli: BootstrapConfig.CLICredentials` (generated once from `TestTLS.ca`) and `TestTLS.caCertificatePem: String`.

- [ ] **Step 1: Write failing tests**

Pass `TestTLS.caCertificatePem` / `TestTLS.cli` through the suite's `generated` helper and existing `generateBootstrapConfig` calls, then add:

```scala
test("round trip preserves the CA certificate and CLI credentials exactly"):
  val cfg = BootstrapConfig.parseBootstrapConfig(generated)
  cfg.caCertificate shouldBe TestTLS.caCertificatePem
  cfg.cli shouldBe TestTLS.cli

test("parseBootstrapConfig rejects a config without cli-private-key"):
  // generated with the cli-private-key block removed
  a [FormatError] should be thrownBy BootstrapConfig.parseBootstrapConfig(stripped)

test("CLICredentials.generate signs a certificate for cliId"):
  val c = BootstrapConfig.CLICredentials.generate(TestTLS.ca)
  CertificateVerifier.verify(Pem.decodeCertificate(c.certificate), TestTLS.ca.certificate) shouldBe c.cliId
```

Update the doc-comment example test (and the doc comment at the top of `BootstrapConfig.scala`) to include the new keys.

- [ ] **Step 2: Run to verify failure** — `sbt 'testOnly *BootstrapConfigSuite'`, expected compile failure.

- [ ] **Step 3: Implement**

Add the case class, attrs (`Required("ca-certificate", YString)`, `Required("cli-id", YUUID)`, `Required("cli-certificate", YString)`, `Required("cli-private-key", YString)`), generator output via `yamlBlockScalar`. In `prepRadicleUpdate`, parse the Radicle's current YAML with `parseBootstrapConfig` and pass its `caCertificate` and `cli` through unchanged (Task 5 tests this). In `Bootstrap.initialize` and `Main.bootstrap`, add temporary arguments generated from a throwaway CA (replaced in Tasks 4 and 7). Fixtures use `TestTLS`.

- [ ] **Step 4: Run to verify pass** — `sbt 'testOnly *BootstrapConfigSuite *HostManagerSuite *MetadataManagerBootstrapRefreshSuite'`, expected PASS.

- [ ] **Step 5: Commit** — `git commit -am "Add CA certificate and CLI credentials to bootstrap config"`

---

### Task 4: CA objects in the Radicle, created by Bootstrap

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/Radicle.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Bootstrap.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala:128`
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:1228` (compile only; Task 7 finalises)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/CreateTLSCertificateSuite.scala` (first test)

**Interfaces:**
- Consumes: Task 1, Task 3 (`CLICredentials`, new `generateBootstrapConfig`).
- Produces:
  - `Radicle.CACertificateKey = Key(Array[Byte](10))`, `Radicle.CAPrivateKeyKey = Key(Array[Byte](11))`; each value is `DataObjectPointer.toArray` of a bootstrap-pool DataObject holding UTF-8 PEM.
  - `Bootstrap.initialize(aspenSystemId: UUID, ida: IDA, bootstrapHostState: HostState, bootstrapStorageDeviceState: StorageDeviceState, stores: List[Backend], ca: CertificateAuthority, cli: BootstrapConfig.CLICredentials): KeyValueObjectPointer`
  - `TestNetwork` passes `TestTLS.ca` and `TestTLS.cli`.

- [ ] **Step 1: Write the failing test (`CreateTLSCertificateSuite extends IntegrationTestSuite`)**

```scala
atest("bootstrap stores the CA certificate and private key in the radicle"):
  for
    kvos <- client.read(radicle)
    certPtr = DataObjectPointer(kvos.contents(Radicle.CACertificateKey).value.bytes)
    keyPtr  = DataObjectPointer(kvos.contents(Radicle.CAPrivateKeyKey).value.bytes)
    certDos <- client.read(certPtr)
    keyDos  <- client.read(keyPtr)
  yield
    new String(certDos.data.getByteArray, UTF_8) shouldBe Pem.encodeCertificate(TestTLS.ca.certificate)
    Pem.decodePrivateKey(new String(keyDos.data.getByteArray, UTF_8)).getEncoded shouldBe TestTLS.ca.privateKey.getEncoded
```

(Use whatever `DataBuffer` accessor the codebase uses for bytes.)

- [ ] **Step 2: Run to verify failure** — `sbt 'testOnly *CreateTLSCertificateSuite'`.

- [ ] **Step 3: Implement**

Add the keys. In `Bootstrap.initialize`, allocate the two DataObjects with `allocateData(Pem.encode...(…).getBytes(UTF_8))` and add both keys to `radicleContent`; pass `Pem.encodeCertificate(ca.certificate)` and `cli` to `generateBootstrapConfig`.

- [ ] **Step 4: Run to verify pass** — `sbt 'testOnly *CreateTLSCertificateSuite *SystemTaskBootstrapSuite'`.

- [ ] **Step 5: Commit** — `git commit -am "Store CA certificate and private key objects in the radicle"` (plus `git add` the new suite).

---

### Task 5: `AspenClient.createTLSCertificate` and credential preservation

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (next to `getBootstrapConfig`)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/CreateTLSCertificateSuite.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigRadicleUpdateSuite.scala`

**Interfaces:**
- Consumes: Task 4 Radicle keys, Task 1.
- Produces: `def createTLSCertificate(id: UUID, publicKey: PublicKey): Future[X509Certificate]` — concrete method on the `AspenClient` trait.

- [ ] **Step 1: Write failing tests**

```scala
atest("createTLSCertificate signs a certificate for the requested UUID"):
  val id = UUID.randomUUID()
  client.createTLSCertificate(id, CertificateAuthority.generateKeyPair().getPublic).map: cert =>
    CertificateVerifier.verify(cert, TestTLS.ca.certificate) shouldBe id
```

In `BootstrapConfigRadicleUpdateSuite`, add a test that runs `prepRadicleUpdate` for a bootstrap store move (follow the existing tests' setup) and asserts the parsed post-update config has `caCertificate == TestTLS.caCertificatePem` and `cli == TestTLS.cli`.

- [ ] **Step 2: Run to verify failure** — `sbt 'testOnly *CreateTLSCertificateSuite *BootstrapConfigRadicleUpdateSuite'`.

- [ ] **Step 3: Implement** — read radicle, read both DataObjects, decode PEM, `CertificateAuthority(cert, key).signCertificate(id, publicKey)`. Scaladoc notes there is no access control: any client that can read the radicle can sign.

- [ ] **Step 4: Run to verify pass** — same command, expected PASS.

- [ ] **Step 5: Commit** — `git commit -am "Add AspenClient.createTLSCertificate"`

---

### Task 6: New hosts get a CA-signed certificate

Signing happens **before** the host-registration transaction (user requirement): if signing fails, host creation aborts and nothing is registered. That requires the HostId to be known before `client.createHost` runs, so the caller now supplies it.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:174` and `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala:274` (`createHost` signature)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/HostManager.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (`add_host`'s `reportError`)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/HostManagerSuite.scala`, `src/test/scala/org/aspen_ddp/aspen/common/metadata/HostCreationSuite.scala`

**Interfaces:**
- Consumes: Task 5 `createTLSCertificate`, Task 2 `HostConfig` fields.
- Produces:
  - `AspenClient.createHost(name: String, address: String, dataPort: Int, cncPort: Int, storeTransferPort: Int, hostId: HostId = HostId(UUID.randomUUID())): Future[HostId]` — default declared on the trait only; `BaseAspenClient` uses the supplied id instead of minting one (it is still fixed outside `runCreate`, so retries reuse it). Existing callers are unchanged.
  - `class HostManager.CertificateCreationFailed(cause: Throwable) extends Exception(...)` — raised when signing fails; no host was registered.

- [ ] **Step 1: Write failing tests**

```scala
test("createHost writes a host config with a CA-signed certificate for the new host id"):
  // existing happy-path setup
  val cfg = HostConfig.loadHostConfig(...)
  CertificateVerifier.verify(Pem.decodeCertificate(cfg.tlsCertificate), TestTLS.ca.certificate) shouldBe hostId.uuid
  // private key matches the certificate's public key: sign/verify a nonce with SHA256withECDSA
```

```scala
test("createHost registers nothing when certificate signing fails"):
  // client whose createTLSCertificate returns Future.failed(...)
  // expect CertificateCreationFailed; the host name is not registered, the hosts tree has no
  // new entry, and no host config file was written
```

Follow the suite's existing client-stub approach for the failure case.

In `HostCreationSuite`, add:

```scala
atest("createHost uses a caller-supplied host id"):
  val id = HostId(UUID.randomUUID())
  client.createHost("node_b", "10.0.0.7", 5750, 5751, 5752, id).flatMap: created =>
    created shouldBe id
    client.getHostState(id).map(_.name shouldBe "node_b")
```

- [ ] **Step 2: Run to verify failure** — `sbt 'testOnly *HostManagerSuite *HostCreationSuite'`.

- [ ] **Step 3: Implement**

Add the `hostId` parameter to `createHost` (trait + `BaseAspenClient`, and any test stub that implements it). In `HostManager.createHost`, before registration: mint `HostId(UUID.randomUUID())`, `CertificateAuthority.generateKeyPair()`, `client.createTLSCertificate(hostId.uuid, kp.getPublic)` — failures recover into `CertificateCreationFailed(t)`; then `client.createHost(..., hostId)` and write the config as today with `Pem` strings. Remove the Task 2 throwaway CA. Add a `reportError` case in `Main.add_host` saying signing failed and no host was registered, so the command can simply be re-run. Update the `createHost` scaladoc to describe the new ordering.

- [ ] **Step 4: Run to verify pass** — `sbt 'testOnly *HostManagerSuite *HostCreationSuite'`.

- [ ] **Step 5: Commit** — `git commit -am "Issue CA-signed TLS certificates to new hosts"`

---

### Task 7: CLI bootstrap creates the CA; CLI uses the fixed client id

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (`bootstrap`, `createAspenClient`)
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala:191`

**Interfaces:**
- Consumes: Tasks 1–4.

- [ ] **Step 1: Extend the existing bootstrap test**

```scala
val ca = Pem.decodeCertificate(bsCfg.caCertificate)
CertificateVerifier.verify(Pem.decodeCertificate(hostCfg.tlsCertificate), ca) shouldBe hostCfg.hostId.uuid
CertificateVerifier.verify(Pem.decodeCertificate(bsCfg.cli.certificate), ca) shouldBe bsCfg.cli.cliId
ca.getSubjectX500Principal.getName should include (bsCfg.aspenSystemId.toString)
```

- [ ] **Step 2: Run to verify failure** — `sbt 'testOnly *MainSuite'` (the throwaway CAs from Tasks 2–3 differ, so the verify fails).

- [ ] **Step 3: Implement**

In `bootstrap`, after `aspenSystemId`: `val ca = CertificateAuthority.create(aspenSystemId)`, `val cli = BootstrapConfig.CLICredentials.generate(ca)`; generate the host id first, sign the host cert, put it in `HostConfig`; pass `Pem.encodeCertificate(ca.certificate)`/`cli` to `generateBootstrapConfig` and `ca`/`cli` to `Bootstrap.initialize`. Remove the throwaway CAs. In `createAspenClient`, when `onnet` is `None`, load the bootstrap config once and call `createNetwork(bootstrapConfigFile, None, Some(ClientId(cfg.cli.cliId)))`; reuse that loaded config for `bootstrapIda`. The `host` command path (which passes `onnet`) is unchanged.

- [ ] **Step 4: Run to verify pass** — `sbt 'testOnly *MainSuite'`.

- [ ] **Step 5: Commit** — `git commit -am "Create Aspen CA at bootstrap and give the CLI a fixed certificate identity"`

---

### Task 8: Backlog, docs, full test run

**Files:**
- Modify: `Backlog.txt` (replace line 46 `Add TLS Cert generation for hosts`)
- Modify: `Operation.md` (bootstrap/add-host sections near lines 37 and 51)

- [ ] **Step 1: Edit `Backlog.txt`** — replace the bare entry with, in the file's existing style:

```
Design proper certificate management for the CLI and other client applications
  - Intentional technical debt: the CLI certificate and private key live in
    aspen-bootstrap-config.yaml, which is copied to every host and stored in the
    Radicle. They are generated once at bootstrap and carried forward unchanged
    whenever the config is regenerated.
  - Every CLI process uses the same fixed client id taken from that certificate,
    so concurrent CLI invocations share one identity.

TLS certificate rotation, renewal and revocation
  - CA and entity certificates are issued with ~100 year lifetimes and are never
    renewed or revoked.
```

- [ ] **Step 2: Edit `Operation.md`** — one short paragraph: bootstrap creates the Aspen CA; host configs contain each host's certificate and private key; the bootstrap config contains the CA certificate and the CLI credentials and must therefore be treated as secret.

- [ ] **Step 3: Full test run**

Run: `sbt test` — Expected: all suites pass. Also `git diff main --stat -- src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet` — Expected: empty.

- [ ] **Step 4: Commit** — `git commit -am "Document TLS certificate support and record follow-up backlog items"`
