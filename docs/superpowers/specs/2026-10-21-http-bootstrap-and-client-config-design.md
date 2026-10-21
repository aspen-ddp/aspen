# HTTP Bootstrap Endpoint and Client Config — Design

Date: 2026-10-21

## Intent

Today every Aspen client (the CLI and each `amoebafs` mount) needs an offline copy of the full
`aspen-bootstrap-config.yaml`. That file has two problems:

1. It goes stale as the cluster changes and has to be kept current by hand (`Operation.md`
   already calls for "an API to obtain a copy of the current config from a running host").
2. It carries the shared CLI identity (`cli-id`, `cli-certificate`, `cli-private-key`), so the
   CLI private key is copied into every host directory and stored in the Radicle.

This change separates the two concerns:

- **Cluster layout** is fetched at startup over a small HTTP endpoint served by every host.
  Clients no longer keep a bootstrap yaml at all.
- **Client identity** lives in a small `aspen-client-config.yaml` that holds the client's TLS
  certificate and private key plus a cache of bootstrap host:port pairs to fetch the layout from.

At the same time the network Command-and-Control (CnC) machinery is deleted. It is dead code: no
request types are defined and the only listener is commented out (`cmdline/Main.scala` in
`run_host`). Its port is repurposed for the HTTP server.

Users: the author at the CLI against test clusters, and `amoebafs` mounts on other machines.

## Goals

- Remove all network CnC code and configuration.
- Every host runs a plain HTTP server on the former CnC port exposing `GET /bootstrap`, which
  returns the current `aspen-bootstrap-config.yaml` content.
- Remove `cli-id`, `cli-certificate` and `cli-private-key` from the bootstrap yaml (and therefore
  from host directories and the Radicle).
- `aspen bootstrap` generates an `aspen-client-config.yaml` containing the client certificate,
  private key, and the bootstrap host's `address:http-port` as the initial cached host.
- Every CLI command that takes `<bootstrap-config-file>` today, `amoebafs` included, takes
  `<client-config-file>` instead.
- An optional CLI argument `--bootstrap-host host[:port]` names the host to fetch the bootstrap
  yaml from. Without a port, the default HTTP port is used.
- If the bootstrap yaml cannot be fetched from any cached host, the command fails with an error
  stating that no cached hosts could be reached.
- When the fetched bootstrap yaml's host:port set differs from the cache, rewrite the client
  config if it is writable; warn if it is not.

## Non-goals

- **Securing the HTTP endpoint.** Plain, unauthenticated HTTP is accepted for now. After this
  change the bootstrap yaml contains nothing secret (the CA certificate is public). The accepted
  risk is that something impersonating a host could hand a client a forged layout or CA
  certificate. Nothing enforces TLS yet (ZeroMQ ignores the certificates), so this adds no new
  exposure today.
- **Per-client identities.** One client config is generated at bootstrap and the same file is
  copied to each machine that needs it. A command to mint additional client certificates from
  the cluster CA is future work; the `Backlog.txt` entry on client certificate management stays
  open but is narrowed to that.
- **Compatibility with existing clusters.** A fresh bootstrap is required. No support for the old
  yaml fields, the old `--cnc-port` flag, or the `cncPort` field in stored `HostState` records.
- **Additional HTTP endpoints.** `/bootstrap` is the only one.
- Changing how hosts obtain their own bootstrap yaml at startup (they keep reading it from their
  host directory).

## Anti-goals

- Adding a third-party HTTP library dependency.
- Leaving any CnC remnant (types, protobuf messages, config keys, flags, docs) behind.
- Having clients silently fall back to some stale local bootstrap yaml when no host answers. The
  failure must be loud.

## Decisions made with the user

- Remove CnC completely; repurpose its port for HTTP.
- `/bootstrap` is the only endpoint; plain HTTP is fine for now.
- The client config holds the cert, the key, and a cache of known bootstrap hosts.
- Bootstrap generates the client config with the initial bootstrap host:port cached.
- The CLI accepts the client config instead of the bootstrap yaml.
- On no reachable cached host: fail with an error saying no cached hosts could be reached.
- An optional command-line argument specifies `host[:port]` to fetch from.
- Update the cache when the bootstrap hosts/ports change and the file is writable; warn otherwise.
- One shared client config copied between machines; no per-client minting.
- Fresh bootstrap required; no backward compatibility.
- Clients fetch the bootstrap yaml fresh on every start and keep no local copy of it.

## Design

### 1. CnC removal

Delete:

- `common/network/CnCMessage.scala`, `CnCFrontend.scala`, `CnCBackend.scala`,
  `CnCMessageReceiver.scala`.
- `implementations/zmqnet/ZCnCBackend.scala`, `ZCnCFrontend.scala`.
- The `CnCRequest`/`CnCOk`/`CnCError`/`CnCReply` messages in `src/main/protobuf/codec.proto` and
  their `Codec.scala` encode/decode functions.
- The CnC round-trip test in `CodecRoundTripSuite`.
- The commented-out `ZCnCBackend` block in `run_host`, and stale comments mentioning
  `ZCnCFrontend`.
- `CLAUDE.md`'s "Command and Control (CnC)" section, and the "non-CnC messages" wording in
  `common/network/CLAUDE.md`.

### 2. Port rename: `cnc-port` → `http-port`

Rename everywhere the CnC port is threaded through, keeping the default `4751`:

- `HostConfig` (`cnc-port` yaml key → `http-port`), `HostState.cncPort` → `httpPort`,
  protobuf `HostState.cncPort` → `httpPort` (field number may be reused since no compatibility
  is required), `Codec`.
- `BootstrapConfig.BootstrapHost` (`cnc-port` → `http-port`).
- `MetadataManager.HostEntry`/`createHostEntry`, `ZMQNet`, `AspenClient`/`BaseAspenClient`
  `createHost`, `HostManager`.
- `cmdline/Main.scala`: `DefaultCnCPort` → `DefaultHttpPort`, `--cnc-port` → `--http-port`, the
  distinct-port check, `bootstrap`, `add-host`, `show-host` ("HTTP Port"), and the address error
  message.
- All affected tests.

### 3. Host HTTP server

- Implemented with the JDK's `com.sun.net.httpserver.HttpServer`, bound to `http-port` on all
  interfaces (matching how ZeroMQ binds the data port). Started in `run_host` where the CnC
  backend used to be. Runs on daemon threads.
- `GET /bootstrap` → `200`, `Content-Type: application/yaml; charset=utf-8`, body = the
  bootstrap yaml. Any other path → `404`. Any other method on `/bootstrap` → `405`.
- **Content source (my call):** the authoritative copy in the Radicle via
  `AspenClient.getBootstrapConfig()`, with a 2-second timeout, falling back to the host's
  on-disk `aspen-bootstrap-config.yaml` if the read fails or times out. The Radicle copy is what
  `prepRadicleUpdate` keeps current as the bootstrap pool moves; the on-disk copy is only
  refreshed lazily. If both fail, `503`.
- **Startup (my call):** the server starts in `run_host` after the host's `AspenClient` has been
  constructed and `network.startIoThread` called, and does not wait for the Radicle to be
  readable. A host that is starting, or a cluster without quorum, therefore serves the disk copy
  after the timeout. That copy may be older than the Radicle's; this is accepted. The worst
  outcome is a client caching a slightly stale host list, which the next successful fetch or
  `MetadataManager` refresh corrects.
- Lives in its own small class (e.g. `server/BootstrapHttpServer.scala`) so it can be tested
  without a full host: give it a `() => Future[String]` content source.

### 4. Bootstrap yaml changes

- `BootstrapConfig.Config` loses `cli-id`, `cli-certificate`, `cli-private-key`.
  `CLICredentials` moves out of `BootstrapConfig` (into the new client-config module, renamed
  e.g. `ClientCredentials`).
- `generateBootstrapConfig` and `prepRadicleUpdate` stop taking/carrying the credentials.
- `Bootstrap.initialize` (`server/store/Bootstrap.scala`) and `Radicle` stop storing them.
- It keeps `aspen-system-id`, `bootstrap-ida`, `bootstrap-hosts`, `ca-certificate`.
- Host directories still get an `aspen-bootstrap-config.yaml` (it is no longer secret, but
  writing it owner-only via `SecretFile` is harmless; builder may switch to a plain write).

### 5. `aspen-client-config.yaml`

Schema (builder may adjust key names to match the existing yaml style):

```yaml
aspen-system-id: <uuid>
certificate: |
  -----BEGIN CERTIFICATE-----
  ...
private-key: |
  -----BEGIN PRIVATE KEY-----
  ...
bootstrap-hosts:
  - 192.168.1.10:4751
  - 192.168.1.11:4751
```

- `aspen-system-id` (my call): lets the client reject a fetched bootstrap yaml from a
  different Aspen system, e.g. when `--bootstrap-host` points at the wrong cluster. A mismatch
  is a hard error naming both ids.
- No client id field: since commit `7055092` the network client id is random per process, and the
  certificate carries its own UUID.
- No CA certificate: it arrives in the fetched bootstrap yaml.
- **Certificate and key have no runtime consumer yet (my call).** Nothing reads the CLI
  credentials at connection time today, and ZeroMQ ignores TLS. Client startup loads and
  PEM-parses them, so a corrupt file fails early, and makes them available on the parsed
  `ClientConfig`. It does not wire them into `ZMQNet` or `AspenClient`. That wiring belongs to
  the future TLS-enforcement work.
- Written owner-only with `SecretFile` (it holds a private key).
- New module, e.g. `common/metadata/ClientConfig.scala`, with load, parse, generate and an
  update-hosts function, following the `BootstrapConfig`/`HostConfig` style (snakeyaml).

**Generation:** `aspen bootstrap` writes it to `<base-directory>/aspen-client-config.yaml`,
alongside the `bootstrap-host/` directory (my call), with `bootstrap-hosts` containing only the
bootstrap host's `address:http-port`. It prints the path.

### 6. Client startup

Replaces `createAspenClient(bootstrapConfigFile)` for every client command:

1. Load the client config.
2. Build the candidate list: the `--bootstrap-host` value first if given (port defaults to
   `4751`), then the cached hosts in file order.
3. Try `GET http://host:port/bootstrap` on each in turn, with a short connect/read timeout
   (builder's call, a few seconds), until one returns `200` and the body parses as a bootstrap
   yaml whose `aspen-system-id` matches.
4. If none succeed: exit nonzero with an error that says no cached hosts could be reached and
   lists each host tried with its failure reason. If `--bootstrap-host` was given and failed,
   the message mentions it too.
5. Compare the fetched yaml's bootstrap hosts, as the set of `address:http-port`, against the
   cached set. If they differ, rewrite the client config with the new list (atomic write,
   owner-only). If the file is not writable or the write fails, print a warning to stderr and
   continue. A `--bootstrap-host` value that is not one of the fetched bootstrap hosts is never
   added to the cache. Addresses are compared as the literal strings in the yaml, with no DNS
   resolution or normalization (my call). Concurrent rewrites by several processes sharing one
   file are last-writer-wins. They all write the same fetched content, so that is harmless.
6. Hand the parsed bootstrap config to the network and client.

A bootstrap yaml whose host entry lacks `http-port` is a parse error. No default is applied,
consistent with requiring a fresh bootstrap.

Every command that takes `<bootstrap-config-file>` today takes only the client config. No
command needs both a host directory and a client config.

Use the JDK's `java.net.http.HttpClient`.

### 7. MetadataManager without a client-side file

`MetadataManager` (and `ZMQNet`, which constructs it) currently take a bootstrap yaml *path*,
load it at construction, and rewrite it on refresh. Clients no longer have that file.

- Change the constructor to take the initial bootstrap config content (or parsed `Config`) plus
  a callback invoked with each successfully parsed refreshed config, instead of a path.
- **Hosts:** the callback writes the host-directory file, preserving today's behaviour.
- **Clients:** the callback runs the same cache comparison as step 5 of client startup, so a
  long-running `amoebafs` mount keeps its client config current when the bootstrap pool moves.
- Preserve `applyBootstrapConfig`'s documented ordering: parse, then map, then best-effort write.

### 8. CLI changes

- The positional `<bootstrap-config-file>` becomes `<client-config-file>` for every command that
  takes it today (all except `bootstrap` and `host`), including `amoebafs`.
- Add the optional `--bootstrap-host host[:port]` to those same commands.
- `add-host` / `create-storage-device`: `HostManager.createHost` currently copies the CLI's
  bootstrap yaml file into the new host directory. It must instead write the fetched bootstrap
  yaml content into it.
- The CLI warning before printing secrets (`Main.scala`, host private key) stays.

### 9. Documentation

- `Operation.md`: describe the client config, the HTTP endpoint, `--bootstrap-host`, and copying
  the client config to other machines. Replace the "API to obtain a copy of the current config"
  note with a description of `/bootstrap`. Update the TLS/CLI identity paragraph.
- `README.md` and `Notes.txt`: update the `create-filesystem`/`amoebafs` examples to use the
  client config.
- `Backlog.txt`: narrow the client certificate management entry to per-client certificate
  minting; note that the CLI key no longer lives in the bootstrap yaml or the Radicle.
- `CLAUDE.md`: remove the CnC section; mention that clients start from
  `aspen-client-config.yaml` and fetch the bootstrap yaml over HTTP.
- `docs/superpowers/specs/2026-10-21-tls-certificates-design.md`: add a note pointing to this
  design for the CLI identity change.

## Testing expectations

- `ClientConfig`: generate/parse round trip; host-list update; unwritable-file warning path.
- `BootstrapConfig`: the suites drop the credential and `cnc-port` cases and use `http-port`.
- HTTP server: `/bootstrap` returns the content; `404` on other paths; fallback to disk when the
  primary source fails; `503` when both fail.
- Client startup: tries `--bootstrap-host` first; falls through unreachable cached hosts;
  fails with the "no cached hosts could be reached" message when all fail; rejects a mismatched
  `aspen-system-id`; rewrites the cache only when the host set changes.
- `MainSuite`: bootstrap writes the client config with the expected cached host; CLI parsing of
  `<client-config-file>` and `--bootstrap-host`; `--http-port` replaces `--cnc-port`.
- Existing integration suites that build a bootstrap yaml (`MetadataManagerBootstrapRefresh*`,
  `HostManagerSuite`, `FailedStorageDeviceSuite`, `ReedSolomonIntegrationSuite`, `TestTLS`)
  updated for the new constructor and fields.

## Left to the builder

- Exact class and file names, and yaml key spellings, within existing conventions.
- HTTP timeouts and the server's thread pool size.
- Whether host-directory bootstrap yaml stays owner-only.
- Task ordering and how to split the work into commits.
