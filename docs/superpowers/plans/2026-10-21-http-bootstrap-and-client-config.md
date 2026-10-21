# HTTP Bootstrap Endpoint and Client Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete network CnC, serve the bootstrap yaml over HTTP on the former CnC port, and have clients start from a small `aspen-client-config.yaml` (cert, key, cached bootstrap hosts) instead of an offline bootstrap yaml.

**Architecture:** A JDK `HttpServer` on each host serves `GET /bootstrap`. A new `ClientConfig` module owns the client file. A `BootstrapFetcher` tries `--bootstrap-host` and then the cached hosts, and updates the cache. `MetadataManager`/`ZMQNet` stop taking a file path. They take a parsed config plus a refresh callback: hosts write their host-dir file from it, clients update their cache.

**Tech Stack:** Scala 3, sbt, scopt, snakeyaml (via `YamlFormat`), JDK `com.sun.net.httpserver.HttpServer` and `java.net.http.HttpClient`, scalatest `AnyFunSuite` + `Matchers`.

**Spec:** `docs/superpowers/specs/2026-10-21-http-bootstrap-and-client-config-design.md`

## Global Constraints

- No new library dependencies in `build.sbt`.
- Default HTTP port is `4751` (`Main.DefaultHttpPort`); the yaml key is `http-port` everywhere; the CLI flag is `--http-port`.
- Client config filename: `aspen-client-config.yaml`; bootstrap writes it to `<target-directory>/aspen-client-config.yaml`.
- CLI override flag: `--bootstrap-host <host[:port]>`.
- The failure message when nothing answers must start with `No cached hosts could be reached`.
- The HTTP server's Radicle read times out after 2 seconds and then falls back to the on-disk yaml. `503` if both fail, `404` for other paths, `405` for non-GET on `/bootstrap`. `Content-Type: application/yaml; charset=utf-8`.
- No backward compatibility: no reading of `cnc-port` or `cli-*` keys, no defaulting a missing `http-port`.
- Files holding private keys are written owner-only (0600).
- Scala 3 quiet syntax; indented if/then/else (see `CLAUDE.md`).
- Run single suites with `sbt 'testOnly *SuiteName'`.

## Review Focus

1. **A read-only client config** (copied to another machine, owned by root). The command must still run and print a warning naming the file. Pinned in Task 5.
2. **A cached host:port that answers with something other than Aspen** (another HTTP service, a 500, a garbage body). The fetcher should count it as a failed attempt and move on, not crash. Pinned in Task 5.
3. **A host that accepts the connection but never responds.** The per-host timeout must bound the wait, and the fetcher falls through. Pinned in Task 5.
4. **`--bootstrap-host` values `[fd00::5]:4751`, `node-a`, `node-a:abc`, `node-a:0`.** The IPv6 literal and the bare name parse; a bad port is a usage error at parse time. Pinned in Tasks 3 and 7.
5. **A host whose HTTP port is already in use.** `run_host` must fail at startup instead of running without an endpoint. Pinned in Task 4.

---

### Task 1: Delete network CnC

**Files:**
- Delete: `src/main/scala/org/aspen_ddp/aspen/common/network/CnCMessage.scala`, `CnCFrontend.scala`, `CnCBackend.scala`, `CnCMessageReceiver.scala`
- Delete: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZCnCBackend.scala`, `ZCnCFrontend.scala`
- Modify: `src/main/protobuf/codec.proto:600-624` (remove the `CnC Messages` section)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala:1208-1230` (remove CnC encode/decode and imports)
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:1112-1115` (remove the commented `ZCnCBackend` block); `Main.scala:~1865` (drop the `ZCnCFrontend` mention from the scaladoc)
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala:632-637` (remove the CnCReply test)
- Modify: `CLAUDE.md` (remove the "Command and Control (CnC)" subsection); `src/main/scala/org/aspen_ddp/aspen/common/network/CLAUDE.md` ("defines all non-CnC messages" → "defines all network messages")

**Interfaces:** Consumes nothing. Produces nothing (pure removal).

- [ ] **Step 1: Delete the files and sections listed above.** `cncPort` plumbing stays for now; Task 2 renames it.
- [ ] **Step 2: Verify nothing references CnC types**

Run: `grep -rnE "CnC(Request|Reply|Ok|Error|Frontend|Backend|MessageReceiver)|ZCnC" src CLAUDE.md`
Expected: no output.

- [ ] **Step 3: Compile and run the codec suite**

Run: `sbt 'testOnly *CodecRoundTripSuite'`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A src CLAUDE.md
git commit -m "Remove obsolete network CnC messaging"
```

---

### Task 2: Rename the CnC port to the HTTP port

A mechanical rename across production and test code. `cncPort` → `httpPort`, `cnc-port` → `http-port`, `DefaultCnCPort` → `DefaultHttpPort`, value `4751`.

**Files:**
- Modify: `src/main/protobuf/codec.proto:598` (`uint32 httpPort = 5;`)
- Modify: `common/network/Codec.scala:1192,1202-1206`
- Modify: `common/metadata/HostState.scala:41`
- Modify: `common/metadata/BootstrapConfig.scala:31,91,100,104,111,207` (sample comment, `BootstrapHost.httpPort`, `Required("http-port", YInt)`, generator line `http-port:`)
- Modify: `server/HostConfig.scala:16,36,47,87,100,113`
- Modify: `common/network/MetadataManager.scala:20,28,348,399` (`HostEntry.httpPort`, `createHostEntry(..., httpPort: Int, ...)`)
- Modify: `common/network/implementations/zmqnet/ZMQNet.scala:73-74,190-193`
- Modify: `client/AspenClient.scala:183`, `client/internal/BaseAspenClient.scala:277-282` (`createHost(name, address, dataPort, httpPort, storeTransferPort, hostId)`)
- Modify: `server/HostManager.scala:81,119,129`
- Modify: `cmdline/Main.scala`. Change `DefaultHttpPort`, `Args.httpPort`, and the `--http-port` option with text `Port for the HTTP bootstrap endpoint (default: 4751)`. The `checkConfig` message becomes `data-port, http-port, and store-transfer-port must all be different`. Also update `bootstrap`/`add_host` params, `show-host` (`  HTTP Port:           ${s.httpPort}`), and the `validateHostAddress` embedded-port message (`use --data-port/--http-port/--store-transfer-port`).
- Test: `HostManagerSuite:117`, `HostCreationSuite:22`, `BootstrapConfigSuite`, `HostConfigSuite:76,102`, `MainSuite:220,251,501-505`, `CodecRoundTripSuite:628`, `MetadataManagerFixture:227-235`

**Interfaces:**
- Produces: `HostState.httpPort: Int`, `BootstrapConfig.BootstrapHost.httpPort: Int`, `HostConfig.httpPort: Int`, `MetadataManager.HostEntry.httpPort: Int`, `Main.DefaultHttpPort: Int = 4751`, `Main.Args.httpPort: Int`.

- [ ] **Step 1: Update the tests first.** In `MainSuite` "the shared port options reach a command that takes them", pass `--http-port 5001` and assert `parsed.httpPort shouldBe 5001`. In `MainSuite`, update the embedded-port expectation to `"Address must not include a port; use --data-port/--http-port/--store-transfer-port"`. In `BootstrapConfigSuite.assertMatchesGenerated`, assert `h.httpPort should be(5001)`, and add `generated should include("http-port: 5001")` and `generated should not include "cnc-port"`. Rename the other listed uses.
- [ ] **Step 2: Run them to see them fail to compile**

Run: `sbt 'testOnly *MainSuite *BootstrapConfigSuite'`
Expected: compilation errors on `httpPort`.

- [ ] **Step 3: Rename across the production files listed.**
- [ ] **Step 4: Verify no stragglers**

Run: `grep -rniE "cnc[-_]?port|DefaultCnCPort|CnC Port" src docs/Operation.md README.md Notes.txt 2>/dev/null`
Expected: no output.

- [ ] **Step 5: Run the affected suites**

Run: `sbt 'testOnly *MainSuite *BootstrapConfigSuite *HostConfigSuite *HostManagerSuite *HostCreationSuite *CodecRoundTripSuite *MetadataManager*'`
Expected: PASS.

- [ ] **Step 6: Commit** — `git commit -am "Rename the CnC port to the HTTP port"`

---

### Task 3: `ClientConfig` module, and CLI credentials out of the bootstrap yaml

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/metadata/ClientConfig.scala`
- Modify: `common/metadata/BootstrapConfig.scala`. Delete `CLICredentials` and the `cli-*` attrs. `Config` loses `cli`, and `generateBootstrapConfig` loses its `cli` param. `prepRadicleUpdate` stops carrying `current.cli`, and its comment says "The CA certificate is generated once…". Delete the `cli-*` lines from the sample comment.
- Modify: `server/store/Bootstrap.scala:24-41` (drop the `cli` param)
- Modify: `cmdline/Main.scala:1158,1239,1250` (stop creating/passing `cli`; Task 7 adds the client config write)
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestTLS.scala`. Replace `cli` with `val client: ClientConfig.ClientCredentials = ClientConfig.ClientCredentials.generate(ca)`.
- Modify: every test that passes `TestTLS.cli` to `generateBootstrapConfig` (`MetadataManagerFixture:321`, `BootstrapConfigSuite`, `BootstrapConfigRadicleUpdateSuite`, `FailedStorageDeviceSuite:292-306`, `ReedSolomonIntegrationSuite:107`) and `MainSuite:231`, which drops the `bsCfg.cli` assertion.
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/ClientConfigSuite.scala`

**Interfaces:**
- Consumes: `BootstrapConfig.BootstrapHost.httpPort` (Task 2); `CertificateAuthority`, `Pem`, `SecretFile`, `atomicWrite`, `YamlFormat.*`.
- Produces (all in `object ClientConfig`):
  - `val configFilename: String = "aspen-client-config.yaml"`
  - `case class ClientCredentials(certificate: String, privateKey: String)`; `object ClientCredentials { def generate(ca: CertificateAuthority): ClientCredentials }`. The subject is a random UUID, as `CLICredentials.generate` did.
  - `case class BootstrapEndpoint(address: String, port: Int)` with `override def toString: String = s"$address:$port"`
  - `object BootstrapEndpoint { def parse(s: String, defaultPort: Int): Either[String, BootstrapEndpoint] }`
  - `case class Config(aspenSystemId: UUID, credentials: ClientCredentials, bootstrapHosts: List[BootstrapEndpoint])`. The constructor PEM-decodes the certificate and key (`Pem.decodeCertificate`/`Pem.decodePrivateKey`), and throws `YamlFormat.FormatError` if either fails or `bootstrapHosts` is empty.
  - `def generate(cfg: Config): String`. Keys: `aspen-system-id`, `certificate` (block scalar), `private-key` (block scalar), `bootstrap-hosts` (list of `"address:port"` strings).
  - `def parse(yaml: String): Config`, `def load(path: java.nio.file.Path): Config`
  - `def write(path: java.nio.file.Path, cfg: Config): Unit`. Atomic and owner-only: write via `atomicWrite` (its temp file is created 0600 by `Files.createTempFile`), and assert the mode in the test.
  - `def endpointsOf(bs: BootstrapConfig.Config): List[BootstrapEndpoint]`. Takes `bs.hosts.map(h => BootstrapEndpoint(h.address, h.httpPort)).distinct` in the order given.
  - `enum CacheUpdate { case Unchanged; case Updated; case Failed(error: Throwable) }`
  - `def updateCachedHosts(path: java.nio.file.Path, current: Config, fetched: BootstrapConfig.Config): CacheUpdate`. It returns `Unchanged` when `endpointsOf(fetched).toSet == current.bootstrapHosts.toSet`. Otherwise it writes `current.copy(bootstrapHosts = endpointsOf(fetched))` and returns `Updated`, or `Failed(t)` on any NonFatal write error. It never throws NonFatal.

`BootstrapEndpoint.parse` rules: a leading `[` means a bracketed IPv6 literal (`[fd00::5]` or `[fd00::5]:4751`). Otherwise the last `:` separates the port, and a name without `:` uses `defaultPort`. The port must be an integer from 1 to 65535. The address is non-empty and has no whitespace. Errors are `Left` with a message that contains the input.

- [ ] **Step 1: Write `ClientConfigSuite` with these tests**

```scala
test("generate/parse round-trips and keeps host order")
  // Config(sysId, TestTLS.client, List(BootstrapEndpoint("10.0.0.5", 4751), BootstrapEndpoint("[fd00::5]", 4800)))
  // parse(generate(cfg)) shouldBe cfg
test("parse rejects a certificate that is not PEM")          // intercept[YamlFormat.FormatError]
test("parse rejects an empty bootstrap-hosts list")          // intercept[YamlFormat.FormatError]
test("BootstrapEndpoint.parse handles names, ports and bracketed IPv6")
  // parse("node-a", 4751) shouldBe Right(BootstrapEndpoint("node-a", 4751))
  // parse("node-a:5000", 4751) shouldBe Right(BootstrapEndpoint("node-a", 5000))
  // parse("[fd00::5]:4800", 4751) shouldBe Right(BootstrapEndpoint("[fd00::5]", 4800))
  // parse("[fd00::5]", 4751) shouldBe Right(BootstrapEndpoint("[fd00::5]", 4751))
test("BootstrapEndpoint.parse rejects bad ports and empty hosts")
  // "node-a:abc", "node-a:0", "node-a:70000", "", ":4751" are all Left
test("write creates an owner-only file")                     // PosixFilePermissions rw-------
test("updateCachedHosts leaves the file alone when the host set matches in a different order")
  // returns Unchanged; file bytes identical
test("updateCachedHosts rewrites the file when the host set differs")
  // returns Updated; ClientConfig.load(path).bootstrapHosts shouldBe endpointsOf(fetched)
test("updateCachedHosts reports Failed rather than throwing when the file is not writable")
  // make the parent dir read-only (setPosixFilePermissions r-x); result is CacheUpdate.Failed(_); restore perms in finally
```

Build a fetched `BootstrapConfig.Config` with `BootstrapConfig.parseBootstrapConfig(generateBootstrapConfig(...))` using `TestTLS.caCertificatePem`.

- [ ] **Step 2: Run to verify failure** — `sbt 'testOnly *ClientConfigSuite'` → compile error, `ClientConfig` not found.
- [ ] **Step 3: Implement `ClientConfig.scala`** in the `BootstrapConfig`/`HostConfig` style (`YObject` with `Required` attrs; `yamlBlockScalar` for the PEMs). Then remove the CLI credentials from `BootstrapConfig`, `Bootstrap.initialize` and `Main.bootstrap`, and update `TestTLS` and the listed tests.
- [ ] **Step 4: Verify** — `grep -rnE "cli-(id|certificate|private-key)|CLICredentials|TestTLS\.cli\b" src` → no output.
- [ ] **Step 5: Run** — `sbt 'testOnly *ClientConfigSuite *BootstrapConfig* *MainSuite *MetadataManager* *FailedStorageDeviceSuite *ReedSolomonIntegrationSuite'` → PASS.
- [ ] **Step 6: Commit** — `git add -A src && git commit -m "Add ClientConfig and remove CLI credentials from the bootstrap config"`

---

### Task 4: `BootstrapHttpServer`, started by `run_host`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/BootstrapHttpServer.scala`
- Modify: `cmdline/Main.scala` `run_host` (start the server where the CnC block was)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/BootstrapHttpServerSuite.scala`

**Interfaces:**
- Produces:

```scala
class BootstrapHttpServer(port: Int,
                          primary: () => Future[String],
                          fallback: () => String,
                          primaryTimeout: Duration = Duration(2, SECONDS)) extends Logging:
  def start(): Unit      // binds 0.0.0.0:port; throws java.net.BindException if the port is taken
  def boundPort: Int     // actual port (tests pass 0)
  def stop(): Unit       // server.stop(0)
```

Handler: `GET /bootstrap` → `Await.result(primary(), primaryTimeout)`. On any NonFatal failure, log a warning and use `fallback()`. If that also throws, return `503`. Other path → `404`; non-GET on `/bootstrap` → `405`. Use a small fixed daemon-thread executor (`DaemonThreads.factory("bootstrap-http")`), size the builder's call.

- [ ] **Step 1: Write `BootstrapHttpServerSuite`.** Use `java.net.http.HttpClient` against `http://127.0.0.1:${server.boundPort}`, with servers built on port `0` and stopped in `finally`.

```scala
test("GET /bootstrap returns the primary content as yaml")
  // status 200; body == "primary"; header Content-Type == "application/yaml; charset=utf-8"
test("falls back to the on-disk copy when the primary read fails")
  // primary = () => Future.failed(Exception("x")); fallback = () => "disk"; body == "disk"
test("falls back when the primary read times out")
  // primary = () => Promise[String]().future; primaryTimeout = 200.millis; body == "disk"; elapsed < 2s
test("returns 503 when both sources fail")
test("returns 404 for other paths")              // GET /other
test("returns 405 for a POST to /bootstrap")
test("start fails when the port is already in use")
  // val ss = new java.net.ServerSocket(0); new BootstrapHttpServer(ss.getLocalPort, ...).start() throws BindException
```

- [ ] **Step 2: Run to verify failure** — `sbt 'testOnly *BootstrapHttpServerSuite'` → compile error.
- [ ] **Step 3: Implement `BootstrapHttpServer`.**
- [ ] **Step 4: Wire into `run_host`.** After `network.startIoThread(client)` and before `host.start()`, run `new BootstrapHttpServer(hostCfg.httpPort, () => client.getBootstrapConfig(), () => Files.readString(bsCfgFile)).start()`. A `BindException` propagates and ends the command with an error, which is Review Focus 5.
- [ ] **Step 5: Run** — `sbt 'testOnly *BootstrapHttpServerSuite'` → PASS.
- [ ] **Step 6: Commit** — `git add -A src && git commit -m "Serve the bootstrap config over HTTP from every host"`

---

### Task 5: `BootstrapFetcher`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/BootstrapFetcher.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/BootstrapFetcherSuite.scala`

**Interfaces:**
- Consumes: `ClientConfig.{Config, BootstrapEndpoint, updateCachedHosts, CacheUpdate}` (Task 3), `BootstrapHttpServer` (Task 4, tests only), `BootstrapConfig.parseBootstrapConfig`.
- Produces:

```scala
object BootstrapFetcher:
  case class Fetched(from: ClientConfig.BootstrapEndpoint, yaml: String, config: BootstrapConfig.Config)

  class NoReachableHosts(val attempts: List[(ClientConfig.BootstrapEndpoint, String)]) extends Exception(...)
    // getMessage: "No cached hosts could be reached:" then one line per attempt "  <endpoint>: <reason>"

  class SystemIdMismatch(val from: ClientConfig.BootstrapEndpoint, val expected: UUID, val actual: UUID) extends Exception(...)
    // getMessage names the endpoint and both ids

  def fetch(clientConfig: ClientConfig.Config,
            bootstrapHost: Option[ClientConfig.BootstrapEndpoint],
            timeout: Duration = Duration(5, SECONDS)): Fetched
```

Algorithm: build the candidates as `bootstrapHost.toList ++ clientConfig.bootstrapHosts`, then `.distinct`. For each one, `GET http://<address>:<port>/bootstrap` with `connectTimeout` and request `timeout` both set to `timeout`. A non-200 status, an I/O error, a timeout, or a body that fails `parseBootstrapConfig` records `(endpoint, reason)` and moves on. A parsed config whose `aspenSystemId` differs from `clientConfig.aspenSystemId` throws `SystemIdMismatch` immediately: this is the spec's hard error, and it never falls through. The first good config returns `Fetched`. If every candidate fails, throw `NoReachableHosts`.

- [ ] **Step 1: Write `BootstrapFetcherSuite`.** Serve real yaml from `BootstrapHttpServer(0, ...)` instances. Build the client config with `TestTLS.client` and the same system id as the served yaml. For an unreachable endpoint, use a port from a closed `ServerSocket(0)`.

```scala
test("tries --bootstrap-host before the cached hosts")
  // two servers A (override) and B (cached), different yaml; result.from == A
test("falls through an unreachable cached host to the next one")
  // cached = List(dead, good); result.from == good
test("treats a non-Aspen HTTP response as a failed attempt")                     // Review Focus 2
  // first server returns "<html>not aspen</html>" (primary = Future.successful(...)); second good; result.from == second
test("a host that accepts but never answers is bounded by the timeout")          // Review Focus 3
  // a ServerSocket that accepts and never writes; timeout = 300.millis; falls through to good; elapsed < 3s
test("reports every attempt when no cached host can be reached")
  // all dead; NoReachableHosts; getMessage should startWith("No cached hosts could be reached"); includes each endpoint.toString
test("rejects a bootstrap config from a different Aspen system")
  // served yaml with another systemId; intercept[SystemIdMismatch]; message includes both ids
```

- [ ] **Step 2: Run to verify failure** — `sbt 'testOnly *BootstrapFetcherSuite'` → compile error.
- [ ] **Step 3: Implement `BootstrapFetcher`.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** — `git add -A src && git commit -m "Fetch the bootstrap config from cached hosts over HTTP"`

---

### Task 6: `MetadataManager` and `ZMQNet` take a config and a refresh callback

**Files:**
- Modify: `common/network/MetadataManager.scala:56-71,260-349`
- Modify: `common/network/implementations/zmqnet/ZMQNet.scala:137-141,215`
- Modify: `cmdline/Main.scala:831-873` (`createNetwork`, `createAspenClient`) and `run_host` (host callback)
- Test: `MetadataManagerFixture.scala:315-350`, `MetadataManagerBootstrapRefreshSuite.scala`

**Interfaces:**
- Produces:

```scala
class MetadataManager[T <: MetadataManager.HostEntry](
    initialBootstrapConfig: BootstrapConfig.Config,
    onBootstrapConfigRefreshed: (String, BootstrapConfig.Config) => Unit,
    val networkImplInterface: MetadataManager.NetworkImplInterface[T],
    val pendingStoreLookupQueueSize: Int = 20,
    val pendingHostLookupQueueSize: Int = 100)

class ZMQNet(initialBootstrapConfig: BootstrapConfig.Config,
             onBootstrapConfigRefreshed: (String, BootstrapConfig.Config) => Unit,
             val oclientId: Option[ClientId],
             val ohost: Option[(HostId, Int)],
             val heartbeatPeriod: Duration,
             val messageHandler: MessageHandler)

// Main
def createNetwork(bootstrapConfig: BootstrapConfig.Config,
                  onRefresh: (String, BootstrapConfig.Config) => Unit,
                  ohost: Option[(HostId, Int)],
                  oclientId: Option[ClientId]): (NetworkBridge, ZMQNet)
def createAspenClient(bootstrapConfig: BootstrapConfig.Config,
                      onRefresh: (String, BootstrapConfig.Config) => Unit,
                      onnet: Option[(NetworkBridge, ZMQNet)] = None): (AspenClient, ZMQNet, KeyValueObjectPointer)
```

`loadInitialBootstrapConfig` uses `initialBootstrapConfig` and no longer reads a file. `applyBootstrapConfig(cfg)` keeps its order: parse, then map under the monitor, then call `onBootstrapConfigRefreshed(cfg, parsed)` inside `try … catch NonFatal` with an error log. Rewrite the scaladoc so it describes a best-effort callback rather than a file write; keep the ordering argument.

The host callback, in `run_host`: `(yaml, _) => atomicWrite(bsCfgFile, yaml)` plus an info log. The file write and its logging move here from `MetadataManager`. Until Task 7, client commands may pass `(_, _) => ()`, temporarily.

- [ ] **Step 1: Update the fixture and suite.** `newManagerWithConfigFile` becomes `newManagerWithRecorder()`. It returns a `ListBuffer[String]` that records each callback's yaml, and the manager is built from `BootstrapConfig.parseBootstrapConfig(templateConfig)`. Rewrite the suite's file assertions as recorder assertions:
  - "a fetched config that parses is written through to disk" becomes "a fetched config that parses is passed to the refresh callback" (`recorded shouldBe List(fresh)`).
  - "a fetched config that does not parse leaves the file unchanged…" becomes "…is not passed to the callback" (`recorded shouldBe empty`).
  - "a config that cannot be written is still applied in memory" becomes "a callback that throws does not prevent the mapping". The callback throws `RuntimeException`; assert the remap took effect and the refresh is retryable.
- [ ] **Step 2: Run to verify failure** — `sbt 'testOnly *MetadataManager*'` → compile errors.
- [ ] **Step 3: Implement the constructor changes in `MetadataManager`, `ZMQNet` and `Main`.**
- [ ] **Step 4: Run** — `sbt 'testOnly *MetadataManager* *MainSuite'` → PASS; `sbt compile` clean.
- [ ] **Step 5: Commit** — `git commit -am "Give MetadataManager a parsed bootstrap config and refresh callback"`

---

### Task 7: CLI uses the client config

**Files:**
- Modify: `cmdline/Main.scala`. Covers `Args`, `bootstrapConfigArg` (now `clientConfigArg`), the new `bootstrapHostOption`, every command's children, the dispatch at 767-804, every `def` taking `bootstrapConfigFile: os.Path`, `add_host`, `create_storage_device`, `bootstrap`, and `commandErrorMessage`.
- Modify: `server/HostManager.scala:52-104` (`createHost` takes the yaml content)
- Test: `MainSuite.scala`, `HostManagerSuite.scala`

**Interfaces:**
- Consumes: `ClientConfig` (Task 3), `BootstrapFetcher` (Task 5), and the `createAspenClient(bootstrapConfig, onRefresh, …)` signature (Task 6).
- Produces:

```scala
// Main
case class ClientSession(clientConfigFile: Path,
                         clientConfig: ClientConfig.Config,
                         bootstrapYaml: String,
                         bootstrapConfig: BootstrapConfig.Config)
def openClientSession(clientConfigFile: Path, bootstrapHost: Option[ClientConfig.BootstrapEndpoint]): ClientSession
def createAspenClient(session: ClientSession): (AspenClient, ZMQNet, KeyValueObjectPointer)  // overload used by every client command
// Args: clientConfigFile: File = null, bootstrapHost: Option[ClientConfig.BootstrapEndpoint] = None (replaces bootstrapConfigFile)

// HostManager
def createHost(client: AspenClient, hostDirectory: Path, bootstrapYaml: String, aspenSystemId: UUID,
               name: String, address: String, dataPort: Int, httpPort: Int, storeTransferPort: Int)
              (using ExecutionContext): Future[HostId]
```

Behaviour:
- `openClientSession` calls `ClientConfig.load`, then `BootstrapFetcher.fetch`, then `ClientConfig.updateCachedHosts`. On `CacheUpdate.Failed(t)`, print to stderr: `Warning: could not update cached bootstrap hosts in <file>: <t.getMessage>`.
- `createAspenClient(session)` passes an `onRefresh` that runs the same `updateCachedHosts` and warning against the latest `ClientConfig`. Keep it in a `@volatile var` or `AtomicReference`, so a long-running amoebafs compares against what it last wrote.
- Every command that took `<bootstrap-config-file>` now takes `<client-config-file>` (text `Client configuration file (aspen-client-config.yaml)`, same exists-validation) plus `--bootstrap-host`. That is all commands except `bootstrap` and `host`.
- `--bootstrap-host <host[:port]>` (text `Host to fetch the bootstrap config from, tried before the cached hosts (default port: 4751)`) validates with `BootstrapEndpoint.parse(_, DefaultHttpPort)`, so a bad port is a usage error.
- `commandErrorMessage` maps `BootstrapFetcher.NoReachableHosts` and `SystemIdMismatch` to `"Error: " + e.getMessage`, and the command exits nonzero.
- `HostManager.createHost` writes `bootstrapYaml` into the new host directory with `SecretFile.write` (replacing the `SecretFile.copy`; keep the REPLACE_EXISTING reasoning in the comment, drop "holds the CLI private key"). `add_host`/`create_storage_device` take a `ClientSession` and pass `session.bootstrapYaml` and `session.bootstrapConfig.aspenSystemId`.
- `bootstrap` writes `ClientConfig.write(targetDirectory.resolve(ClientConfig.configFilename), Config(aspenSystemId, ClientCredentials.generate(ca), List(BootstrapEndpoint(address, httpPort))))`, then prints `Client configuration written to <path>`.
- `fail_storage_device`'s follow-up hint at `Main.scala:~1510` prints `<client-config-file>` in place of the old path.

- [ ] **Step 1: Write the tests in `MainSuite`.**

```scala
test("bootstrap writes an owner-only client config caching the bootstrap host")
  // extend the existing bootstrap test: tmp.resolve("aspen-client-config.yaml") exists, perms rw-------,
  // ClientConfig.load(...).bootstrapHosts shouldBe List(BootstrapEndpoint("10.0.0.5", 4751)),
  // aspenSystemId == hostCfg.aspenSystemId, and CertificateVerifier.verify(cert, ca) succeeds
test("client commands take a client config and --bootstrap-host")
  // parse Seq("list-pools", cfgFile, "--bootstrap-host", "[fd00::5]:4800") -> bootstrapHost == Some(BootstrapEndpoint("[fd00::5]", 4800))
test("--bootstrap-host with a bad port is a usage error")                       // Review Focus 4
  // parse Seq("list-pools", cfgFile, "--bootstrap-host", "node-a:abc") -> None
test("commandErrorMessage reports unreachable cached hosts")
  // commandErrorMessage(new BootstrapFetcher.NoReachableHosts(List(BootstrapEndpoint("h",1) -> "refused"))) should include("No cached hosts could be reached")
test("openClientSession warns but succeeds when the client config is read-only")  // Review Focus 1
  // serve yaml (different host list than cached) via BootstrapHttpServer(0,...); client config in a dir chmod r-x;
  // capture Console.err; session.bootstrapConfig.aspenSystemId == expected; stderr includes "Warning: could not update cached bootstrap hosts"
```

In `HostManagerSuite`, change the setup to pass yaml content, and assert the new host dir's `aspen-bootstrap-config.yaml` equals that content byte for byte.

- [ ] **Step 2: Run to verify failure** — `sbt 'testOnly *MainSuite *HostManagerSuite'` → compile errors.
- [ ] **Step 3: Implement the changes above.** Remove the temporary `(_, _) => ()` callbacks from Task 6.
- [ ] **Step 4: Verify** — `grep -n "bootstrapConfigFile\|bootstrap-config-file\|BootstrapConfig.loadBootstrapConfig" src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` shows only `run_host`'s host-directory read.
- [ ] **Step 5: Run the full suite** — `sbt test` → PASS.
- [ ] **Step 6: Commit** — `git add -A src && git commit -m "CLI starts from aspen-client-config.yaml and fetches the bootstrap config over HTTP"`

---

### Task 8: Documentation

**Files:**
- Modify: `Operation.md`. Add a client config section: what it holds, that it's written by `bootstrap`, copied to other machines, and kept owner-only. Cover `--bootstrap-host` and the `/bootstrap` endpoint on `http-port`. Replace the "API to obtain a copy of the current config" note (`:51-57`). Update the CLI identity paragraph (`:59-65`) so the credentials live in the client config, not the bootstrap yaml or the Radicle.
- Modify: `README.md:263-282`, `Notes.txt:26-27`. Examples use `aspen-client-config.yaml`.
- Modify: `Backlog.txt:46-54`. Narrow "Design proper certificate management for the CLI…" to minting per-client certificates. Note that the shared client key no longer lives in the bootstrap yaml, host directories, or the Radicle.
- Modify: `CLAUDE.md`. In Key Design Aspects, replace the bullet "Creating an Aspen Client requires an offline configuration of the bootstrap StoragePool and network endpoints" with one saying that clients start from `aspen-client-config.yaml` and fetch the bootstrap yaml from any host's `GET /bootstrap`.
- Modify: `docs/superpowers/specs/2026-10-21-tls-certificates-design.md`. Add a one-line note pointing to the HTTP bootstrap design for the CLI identity change.

- [ ] **Step 1: Make the edits.**
- [ ] **Step 2: Verify** — `grep -rniE "cnc|cli-private-key|bootstrap-config-file" Operation.md README.md Notes.txt CLAUDE.md Backlog.txt` → only intentional historical mentions, if any.
- [ ] **Step 3: Commit** — `git commit -am "Document the client config and HTTP bootstrap endpoint"`
