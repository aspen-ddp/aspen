# TLS Certificate Support — Design

> Note: the CLI identity described here has since moved out of the bootstrap yaml into `aspen-client-config.yaml`; see the HTTP bootstrap and client config design (`2026-10-21-http-bootstrap-and-client-config-design.md`).

## Intent

Lay the groundwork for authenticated, encrypted communication between every entity in an Aspen
system (hosts and clients). Hosts and clients have no fixed IP addresses or DNS names; the only
stable, unique identity they have is a UUID. Aspen therefore runs its own Certificate Authority
and issues TLS-compatible X.509 certificates that embed the entity's UUID, so a network plugin
can identify the entity on the other end of an incoming connection.

This change is plumbing only. No network plugin uses the certificates yet. The existing ZeroMQ
network implementation ignores them and remains unchanged.

## Goals

- Bootstrap creates the Aspen CA (key pair + self-signed CA certificate).
- The CA certificate and CA private key are stored in Aspen as two separate DataObjects, in PEM
  text form, referenced from the Radicle object.
- `aspen-bootstrap-config.yaml` carries the CA certificate.
- Every host's `aspen-host-config.yaml` carries that host's private key and its CA-signed
  certificate, including the initial bootstrap host.
- `AspenClient.createTLSCertificate(UUID, PublicKey)` reads the CA private key from Aspen and
  returns a signed certificate for that UUID.
- New hosts created via `add-host` get their certificate through `createTLSCertificate`.
- The CLI has a certificate + private key it can present when connecting to hosts.
- Certificate creation and verification logic lives in a plugin-agnostic shared module so all
  future network plugins reuse it.

## Non-goals

- Any network plugin that actually performs TLS. ZeroMQ is untouched.
- Certificate rotation, renewal, revocation, or expiry handling (backlog).
- A proper certificate-management design for CLIs and other client applications (backlog).
- Access control over the CA private key. Any client able to read the CA private-key object can
  sign certificates. This is accepted and **not** planned to change; no backlog item.
- Backwards compatibility. There are no existing deployed systems or config files.

## Anti-goals

- Plugin-specific copies of verification logic. Verification is invoked by plugins but
  implemented once, in the shared module.
- Identifying peers by hostname or IP. Identity is the UUID in the certificate, only.

## Decisions

### Library and algorithms
- **Bouncy Castle** (`bcprov` + `bcpkix`) is added as a dependency, used only to *build and sign*
  certificates and for PEM encode/decode. JDK 25 has no public certificate-generation API (only
  internal `sun.security.x509`, which needs `--add-exports`). *(my call, user agreed)*
- Verification, `SSLContext`, `KeyManager`/`TrustManager` construction use JDK built-ins
  (`java.security`, `javax.net.ssl`) where practical. *(my call, user agreed)*
- Key algorithm: **ECDSA P-256**, signature `SHA256withECDSA`. *(my call, user agreed)*
- All keys and certificates are stored as **PEM text** everywhere (DataObjects and YAML files).
  Private keys as PKCS#8 PEM (`-----BEGIN PRIVATE KEY-----`).

### Certificate contents
- Entity certificates: subject `CN=<uuid>`, plus a Subject Alternative Name of type URI with value
  `urn:uuid:<uuid>`. The SAN is the authoritative identity source; extraction reads the SAN.
  Issuer is the Aspen CA. Not a CA (`BasicConstraints` false). Key usage suitable for TLS
  client *and* server authentication (`digitalSignature`; EKU `serverAuth` + `clientAuth`),
  since every entity may both accept and initiate connections.
- CA certificate: self-signed, `BasicConstraints` CA=true, `keyCertSign`. Subject should include
  the Aspen system id (e.g. `CN=Aspen CA <aspen-system-id>`).
- "The CA's public key" in the original request is realized as the **self-signed CA
  certificate**, since TLS trust stores need a certificate, not a bare key. *(my call, user agreed)*
- Validity: very long (~100 years) for both CA and entity certificates, because nothing can rotate
  or renew them yet. *(my call, user agreed)*
- Serial numbers: random positive 128-bit values (builder's discretion on exact method).

### Shared module (plugin-agnostic)
A new package, e.g. `org.aspen_ddp.aspen.common.network.tls` (exact name is the builder's call),
containing pure functions with no dependency on any network plugin:
- generate an ECDSA P-256 key pair
- create the CA (key pair + self-signed cert) for a given aspen system id
- sign an entity certificate for `(UUID, PublicKey)` using CA cert + CA private key
- PEM encode/decode for certificates and private keys
- verify a peer certificate chains to the Aspen CA certificate (signature + validity) and extract
  its UUID from the `urn:uuid:` SAN; reject certs from a foreign CA or with missing/malformed SAN
- build a JDK `SSLContext` (and/or `KeyManager`/`TrustManager`) from: CA cert + this entity's
  cert + private key. The trust manager must validate against the Aspen CA only and must **not**
  perform hostname verification; peer identity is the extracted UUID.

Both `Bootstrap` (which has no client available) and `AspenClient.createTLSCertificate` call the
same signing function.

### Radicle and CA objects
- Two new Radicle keys: `CACertificateKey = Key(10)` and `CAPrivateKeyKey = Key(11)` (names are
  the builder's call). Each value is the binary-encoded `DataObjectPointer` (`ptr.toArray`) of a
  DataObject in the bootstrap pool.
- DataObject contents are the UTF-8 PEM text of the CA certificate and CA private key,
  respectively.
- Created in `Bootstrap.initialize` using the existing direct-allocation path (`allocateData`),
  pointers added to the allocation tree like every other bootstrap object, and both keys written
  into `radicleContent`. Readers may treat both keys as required.

### `aspen-bootstrap-config.yaml`
New **required** top-level fields (no optional/compat parsing):
- `ca-certificate`: PEM text of the CA certificate.
- `cli-id`: the CLI's fixed UUID. *(field name is the builder's call; the UUID may alternatively
  be read from the cert's SAN, but it must equal the cert's UUID)*
- `cli-certificate`: PEM text of the CLI's CA-signed certificate.
- `cli-private-key`: PEM text of the CLI's private key.

PEM values are multi-line; use YAML block scalars (`|`) in the hand-built writer and make sure
parsing round-trips exactly.

The CLI credentials are generated **once, during bootstrap**, and never change. Every code path
that regenerates the bootstrap config must carry the existing CA cert and CLI credentials forward
unchanged:
- `BootstrapConfig.generateBootstrapConfig` gains parameters for them.
- `Bootstrap.initialize` stores the full config (including CLI credentials) in the Radicle's
  `BootstrapConfigKey`, as today.
- `BootstrapConfig.prepRadicleUpdate` (store moves) reads the existing config and reuses its CA
  cert and CLI credentials.
- `MetadataManager.refreshBootstrapConfig`/`applyBootstrapConfig` write the Radicle copy, which
  therefore already contains them.

This is **intentional technical debt**: the CLI private key lives in a shared file copied to every
host and stored in the Radicle.

### CLI identity
*(Revised after implementation.)* The certificate's UUID is not used as the client id. Mapping
certificate identities to connections is left to the network plugin. All clients, including the
CLI and `amoebafs`, keep using a randomly generated client id for each process.

### `aspen-host-config.yaml`
New **required** fields on `HostConfig`:
- `tls-certificate`: PEM text of the host's CA-signed certificate (UUID = host id).
- `tls-private-key`: PEM text of the host's private key.
*(field names are the builder's call)*

### Bootstrap flow (`Main.bootstrap` + `Bootstrap.initialize`)
1. Generate the CA (key pair + self-signed cert).
2. Generate the bootstrap host's key pair; sign its cert for its HostId with the CA directly
   (shared module; no client exists yet). Write both into `aspen-host-config.yaml`.
3. Generate the CLI UUID + key pair; sign its cert. Include CA cert and CLI credentials in the
   generated `aspen-bootstrap-config.yaml`.
4. `Bootstrap.initialize` receives the CA cert and CA private key (PEM) and allocates the two CA
   DataObjects, referencing them from the Radicle.

How the CA material is threaded from `Main.bootstrap` into `Bootstrap.initialize` (new parameters
vs. generated inside `initialize` and returned) is the builder's call; the test harness
(`TestNetwork`) must also bootstrap a working CA.

### Adding hosts (`add-host` → `HostManager.createHost`)
1. `client.createHost(...)` creates the host and returns its HostId, as today.
2. Generate the host key pair locally.
3. `client.createTLSCertificate(hostId.uuid, publicKey)`.
4. Write certificate + private key into the new host's `aspen-host-config.yaml`.

### `AspenClient.createTLSCertificate(id: UUID, publicKey: PublicKey): Future[X509Certificate]`
Declared on the `AspenClient` trait. Reads the Radicle, follows the CA cert and CA private-key
pointers, reads both DataObjects, parses PEM, and signs via the shared module. Return type may be
the PEM string instead of `X509Certificate` if that composes better — builder's call. It can be a
concrete trait method (like `getBootstrapConfig`) since it only needs `read`.

### Backlog.txt
- Replace the existing bare entry "Add TLS Cert generation for hosts" (line ~46).
- Add an entry: design a proper certificate-management solution for CLIs and other client
  applications. Note the intentional technical debt: CLI cert/private key stored in
  `aspen-bootstrap-config.yaml` (copied to every host, stored in the Radicle), generated once at
  bootstrap.
- Add an entry for certificate rotation / renewal / revocation (certs are currently ~100-year,
  never renewed).
- No entry for CA private-key access control.

## Testing *(my call, user agreed)*
- Shared module unit tests: CA creation; sign + verify; UUID extraction; rejection of a cert
  signed by a different CA; rejection of a cert without the `urn:uuid:` SAN; PEM round trips;
  an `SSLContext` built from the material can complete a TLS handshake between two in-process
  endpoints with mutual auth (good evidence the certs are genuinely TLS-compatible).
- `BootstrapConfigSuite`: round trip with the new fields; missing fields fail to parse.
- `BootstrapConfigRadicleUpdateSuite` / `MetadataManagerBootstrapRefreshSuite`: CA cert and CLI
  credentials survive regeneration unchanged.
- `HostConfigSuite`: round trip with cert + key.
- `HostManagerSuite`: created host config contains a cert whose UUID is the new host id and which
  verifies against the CA.
- `MainSuite` bootstrap test: both yaml files contain valid, verifying credentials; the bootstrap
  host cert's UUID equals its host id.
- An integration test (via `TestNetwork`) for `createTLSCertificate`.
- Existing tests and doc-comment YAML examples updated for the new required fields.

## Left to the builder
- Package and class names, Radicle key names, YAML field names (except those fixed by the user:
  `cli-certificate`, `cli-private-key`).
- Bouncy Castle version (a current stable `bcprov-jdk18on`/`bcpkix-jdk18on`).
- Exact signature of the shared-module API and of `createTLSCertificate`'s return type.
- How CA material is passed into `Bootstrap.initialize`.
- Documentation updates (`Operation.md`, `Architecture.md`) as appropriate.
