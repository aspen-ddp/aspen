# MetadataManager Bootstrap Config Refresh Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `MetadataManager.refreshBootstrapConfig` release its `refreshingBootstrapConfig` guard on every failure path, and make it apply the config it actually fetched rather than re-reading the file.

**Architecture:** `refreshBootstrapConfig` sets a Boolean guard, calls `AspenClient.getBootstrapConfig()`, and today releases the guard only inside a `Future.foreach` callback — which runs on `Success` only. Four paths leave the guard stuck `true` forever, permanently disabling `dropStoreMapping`'s correction for bootstrap stores. The fix mirrors the pattern already established in `StoreManager.startDeviceCheck:846`: wrap the lookup call alone in `try Success(...) catch case t: Throwable => Failure(t)`, switch `.foreach` to `.onComplete`, and release the guard in a `finally`. Alongside that, the fetched YAML string is parsed *before* it is written (so a bad config never lands on disk) and the resulting `Config` — not a re-read of the file — drives the in-memory remap, so a write failure no longer discards a good config.

**Tech Stack:** Scala 3 (quiet/indented syntax), ScalaTest `AnyFunSuite` + `Matchers`, SnakeYAML, `ExecutionContext.parasitic` for inline test continuations, sbt.

**Spec:** `docs/superpowers/specs/2026-08-06-bootstrap-config-refresh-guard-design.md`

---

## File Structure

**Production — modify:**

- `src/main/scala/org/aspen_ddp/aspen/common/util/YamlFormat.scala`
  Add `loadYamlString(yaml: String): Object`, a sibling of the existing `loadYamlFile`. Parsing a string is the capability the whole fix rests on: the fetched config must be validated before it is written.

- `src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala`
  Add `parseBootstrapConfig(yaml: String): Config`, a one-liner over `loadYamlString` mirroring the existing `loadBootstrapConfig(file: File)`.

- `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`
  Rewrite `refreshBootstrapConfig`; add three private helpers — `applyBootstrapConfig(cfg: String)`, `mapBootstrapStores(config: BootstrapConfig.Config)`, `loadInitialBootstrapConfig()`. `mapBootstrapStores` is the single place both the constructor and a refresh install store→host mappings, which is what stops the two from disagreeing about `bootstrapStores`.

**Tests — modify:**

- `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigSuite.scala`
  Two tests for `parseBootstrapConfig`.

- `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala`
  Add the `getBootstrapConfig` seam to `LookupRecordingClient`; give each manager its own config file in its own temp directory; add `newManagerWithConfigFile()` and `bootstrapConfigYaml(...)`.

**Tests — create:**

- `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala`
  Six tests, all driven through the one public entry point that reaches the refresh: `dropStoreMapping(bootstrapStoreId)`.

**Docs — modify:**

- `TODO.txt` — remove the fixed entry, add the stale-`HostEntry` finding.

---

## Background the implementer needs

Read these before starting. They are short and every task depends on them.

**Why `.foreach` is the bug.** `Future.foreach` registers a callback that runs on `Success` only. The guard release lives in that callback's `finally`, so a failed `Future` never releases it.

**Why the `try`/`catch` must wrap the lookup call *alone*.** Under `ExecutionContext.parasitic` the callback runs inline on the completing thread. A wider `catch` would also swallow throws coming back out of the callback body. `StoreManager.startDeviceCheck:846` documents this at length; the same reasoning applies here verbatim.

**Why tests cannot assert on an escaping exception.** `Future`'s `Transformation.run` absorbs non-fatal throws from callbacks and routes them to `ExecutionContext.reportFailure`. A promise completion therefore returns normally even when the continuation it ran threw. Every test below proves guard release *indirectly*, by making a second `dropStoreMapping` call and asserting a second fetch was attempted.

**Lock ordering.** `MetadataManagerFixture` documents the invariant: never complete a promise while holding the client's monitor, because parasitic runs the manager's continuation inline and that would take the manager's lock while holding the client's, inverting the established manager → client order.

**`BootstrapConfig.Config` validates store count against IDA width** and throws `FormatError` when they differ. Every test config below must satisfy `sum(host.stores.length) == ida.width`. This is also what Task 4 exploits to build a well-formed-but-rejected config.

---

## Task 1: Parse a bootstrap config from a string

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/util/YamlFormat.scala:30`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala:115`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigSuite.scala`

- [ ] **Step 1: Write the failing tests**

Append to `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigSuite.scala` (at the end of the class, same two-space indentation as the existing tests):

```scala
  test("parseBootstrapConfig round-trips generateBootstrapConfig"):
    val cfg = BootstrapConfig.parseBootstrapConfig(generated)

    cfg.aspenSystemId should be(systemId)
    cfg.bootstrapIDA should be(Replication(1, 1))
    cfg.hosts.length should be(1)

    val h = cfg.hosts.head
    h.hostId should be(hostId)
    h.name should be("node_a")
    h.address should be("127.0.0.1")
    h.dataPort should be(5000)
    h.cncPort should be(5001)
    h.storeTransferPort should be(5002)
    h.stores should be(List(storeId))

  test("parseBootstrapConfig rejects an empty document"):
    val ex = intercept[FormatError]:
      BootstrapConfig.parseBootstrapConfig("")
    ex.getMessage should include("Empty YAML document")
```

Note: `generated`, `systemId`, `hostId`, `storeId` and the `FormatError` import already exist in this file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *BootstrapConfigSuite'`
Expected: compilation failure — `value parseBootstrapConfig is not a member of object BootstrapConfig`.

- [ ] **Step 3: Add `loadYamlString`**

In `src/main/scala/org/aspen_ddp/aspen/common/util/YamlFormat.scala`, immediately after `loadYamlFile` (which ends at line 30) and before `trait Format[T]`, insert:

```scala
  /** Parses a YAML document already held in memory.
    *
    * The string overload exists so a config fetched over the network can be validated before it
    * is written to disk. Kept alongside loadYamlFile rather than having that method read the file
    * and delegate: SnakeYAML streams from the InputStream, and routing the file path through a
    * String would give up that streaming for no gain.
    */
  def loadYamlString(yaml: String): Object = {
    val doc = new Yaml(new SafeConstructor).load[java.util.AbstractMap[Object, Object]](yaml)

    if (doc == null)
      throw new FormatError("Empty YAML document")

    doc
  }
```

This file is legacy Scala 2 brace style — match it, do not convert it to quiet syntax. `Yaml`, `SafeConstructor` and `FormatError` are already imported/defined in this file.

- [ ] **Step 4: Add `parseBootstrapConfig`**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala`, immediately after `loadBootstrapConfig` (line 115), insert:

```scala
  /** Parses a bootstrap config held in memory, applying exactly the validation
   *  loadBootstrapConfig applies to a file. */
  def parseBootstrapConfig(yaml: String): Config =
    Config.create(loadYamlString(yaml))
```

`import org.aspen_ddp.aspen.common.util.YamlFormat.*` is already present, so `loadYamlString` needs no new import.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *BootstrapConfigSuite'`
Expected: PASS — 8 tests, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/util/YamlFormat.scala \
        src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigSuite.scala
git commit -m "Add YamlFormat.loadYamlString and BootstrapConfig.parseBootstrapConfig"
```

---

## Task 2: Fixture seams for driving a bootstrap refresh

No behaviour changes here — this task only builds the levers Tasks 3-5 pull. It is verified by the existing MetadataManager suites continuing to pass.

Three separate changes:

1. `LookupRecordingClient` gains a `getBootstrapConfig` override in the same shape as its existing `getHostState` override: record the attempt, throw if armed, otherwise hand back a test-controlled `Promise`.
2. Each manager gets its *own* config file in its *own* temp directory. Today every manager shares the one file written in `beforeAll`; once a refresh can actually succeed, one test's write would leak into the next.
3. A `bootstrapConfigYaml` helper so tests can build a refreshed config without repeating `systemId`.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala:54-57` (fields), `:240-268` (fixture trait tail)

- [ ] **Step 1: Add the `getBootstrapConfig` seam to `LookupRecordingClient`**

In `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala`, find the private field block at lines 54-57:

```scala
  private var promises: Map[HostId, Promise[HostState]] = Map()
  private var poolPromises: Map[PoolId, Promise[StoragePoolState]] = Map()
  private var lookupFailures: Map[HostId, Throwable] = Map()
  private var poolLookupFailures: Map[PoolId, Throwable] = Map()
```

and append three fields to it, so the block reads:

```scala
  private var promises: Map[HostId, Promise[HostState]] = Map()
  private var poolPromises: Map[PoolId, Promise[StoragePoolState]] = Map()
  private var lookupFailures: Map[HostId, Throwable] = Map()
  private var poolLookupFailures: Map[PoolId, Throwable] = Map()
  private var bootstrapConfigAttempts: Int = 0
  private var bootstrapConfigPromises: Map[Int, Promise[String]] = Map()
  private var bootstrapConfigFailure: Option[Throwable] = None
```

Then add the accessors and the override at the very end of the `LookupRecordingClient` class body — that is, immediately before the blank lines preceding `class RecordingNetworkImpl`. Keep the two-space class-body indentation:

```scala
  /** How many times getBootstrapConfig has been called.
   *
   *  Counting attempts rather than inspecting refreshingBootstrapConfig is what makes guard
   *  release observable from outside the manager: a released guard lets the next
   *  dropStoreMapping start a second fetch, a wedged one does not.
   */
  def bootstrapConfigFetches: Int = synchronized:
    bootstrapConfigAttempts

  /** The Promise backing the nth fetch, 1-based, created on first use. Callable before or after
   *  the fetch itself so a test can complete it either way round. */
  def bootstrapConfigPromise(attempt: Int): Promise[String] = synchronized:
    bootstrapConfigPromises.get(attempt) match
      case Some(p) => p
      case None =>
        val p = Promise[String]()
        bootstrapConfigPromises += attempt -> p
        p

  /** Makes getBootstrapConfig throw rather than return a Future. Still counts as an attempt --
   *  the manager did call it. */
  def failBootstrapConfigWith(err: Throwable): Unit = synchronized:
    bootstrapConfigFailure = Some(err)

  def clearBootstrapConfigFailure(): Unit = synchronized:
    bootstrapConfigFailure = None

  override def getBootstrapConfig(): Future[String] = synchronized:
    bootstrapConfigAttempts += 1
    bootstrapConfigFailure match
      case Some(err) => throw err
      case None => bootstrapConfigPromise(bootstrapConfigAttempts).future
```

`Future` and `Promise` are already imported at line 18. The nested `synchronized` in `getBootstrapConfig` → `bootstrapConfigPromise` is safe: Java monitors are reentrant.

- [ ] **Step 2: Give each manager its own config file**

Replace lines 240-268 of the same file — everything from `private var tempDir` to the end — with:

```scala
  private var tempDir: Path = scala.compiletime.uninitialized
  private var templateConfig: String = scala.compiletime.uninitialized

  override protected def beforeAll(): Unit =
    super.beforeAll()
    tempDir = Files.createTempDirectory("aspen-metadata-manager")
    templateConfig = bootstrapConfigYaml(
      Replication(1, 1),
      List(HostState(bootstrapHostId, "bootstrap_host", "127.0.0.1", 5000, 5001, 5002, Set())),
      List(StoreId(poolId, 0.toByte) -> bootstrapHostId))

  override protected def afterAll(): Unit =
    try os.remove.all(os.Path(tempDir))
    catch case _: Throwable => ()
    finally super.afterAll()

  /** A bootstrap config over the fixture's systemId.
   *
   *  BootstrapConfig.Config rejects a config whose total store count differs from the IDA width,
   *  so callers must keep `ida.width` equal to the number of entries in `storeMap`.
   */
  protected def bootstrapConfigYaml(ida: Replication,
                                    hostStates: List[HostState],
                                    storeMap: List[(StoreId, HostId)]): String =
    BootstrapConfig.generateBootstrapConfig(systemId, ida, hostStates, storeMap)

  /** newManager, plus the path of the config file that manager was handed.
   *
   *  Every manager gets its own copy in its own directory. Sharing one file across managers was
   *  harmless while nothing rewrote it, but a refresh that succeeds writes through
   *  atomicWrite -- one test's refreshed config would then be the next test's starting state.
   *  The private directory also gives a test a way to make the write fail: remove the directory
   *  and atomicWrite's Files.createTempFile(parentDir, ...) raises NoSuchFileException, which
   *  works regardless of the privileges the suite runs under.
   */
  protected def newManagerWithConfigFile(): (MetadataManager[MetadataManager.HostEntry], LookupRecordingClient, RecordingNetworkImpl, os.Path) =
    val dir = Files.createTempDirectory(tempDir, "manager-")
    val f = dir.resolve(BootstrapConfig.configFilename)
    Files.write(f, templateConfig.getBytes(StandardCharsets.UTF_8))

    val impl = new RecordingNetworkImpl
    val client = new LookupRecordingClient
    val mgr = new MetadataManager[MetadataManager.HostEntry](os.Path(f), impl)
    mgr.setAspenClient(client)
    (mgr, client, impl, os.Path(f))

  /** A MetadataManager over a bootstrap config naming exactly one host, wired to a fresh
   *  recording client and a fresh RecordingNetworkImpl. Returning the impl is what lets a suite
   *  assert that a message actually reached a host rather than merely that a lookup was started. */
  protected def newManager(): (MetadataManager[MetadataManager.HostEntry], LookupRecordingClient, RecordingNetworkImpl) =
    val (mgr, client, impl, _) = newManagerWithConfigFile()
    (mgr, client, impl)
```

The `bootstrapConfigFile` field is gone; `newManager` keeps its exact signature, so all twenty existing call sites across `MetadataManagerDrainSuite`, `MetadataManagerExceptionSafetySuite`, `MetadataManagerPoolLookupSuite` and `MetadataManagerPeekHostEntrySuite` are untouched.

- [ ] **Step 3: Run the existing suites to verify nothing regressed**

Run: `sbt 'testOnly *MetadataManager*'`
Expected: PASS — the four existing MetadataManager suites, 0 failures. (No new suite exists yet.)

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala
git commit -m "Give each test MetadataManager its own bootstrap config and a getBootstrapConfig seam"
```

---

## Task 3: Release the guard whenever the fetch fails

Covers the two wedge paths that lose the guard before any config exists — a failed `Future` and a synchronous throw from `getBootstrapConfig` — plus a regression guard on the coalescing the flag is there to provide.

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala:193-215`

- [ ] **Step 1: Write the failing tests**

Create `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** What a bootstrap config refresh costs when it goes wrong.
 *
 *  refreshingBootstrapConfig is a plain Boolean, so a refresh that ends without clearing it
 *  disables every later refresh for the life of the process. dropStoreMapping routes bootstrap
 *  stores to refreshBootstrapConfig and non-bootstrap stores to `stores -= storeId`, so a wedged
 *  flag does not degrade to the fallback -- it drops the correction entirely.
 *
 *  The flag is private and ExecutionContext.parasitic reports a throw from an onComplete callback
 *  rather than rethrowing it, so nothing here can observe either the flag or an escaping
 *  exception directly. Each test instead makes a second dropStoreMapping call and asserts a
 *  second fetch was attempted: a released guard permits one, a wedged guard does not.
 *
 *  Every test drives the refresh through dropStoreMapping(bootstrapStoreId), the only public
 *  path that reaches it.
 */
class MetadataManagerBootstrapRefreshSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  /** The one store the fixture's bootstrap config names. */
  private val bootstrapStoreId: StoreId = StoreId(poolId, 0.toByte)

  /** Absent from the fixture's bootstrap config; a refreshed config can introduce it. */
  private val secondStoreId: StoreId = StoreId(poolId, 1.toByte)

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  test("a bootstrap config fetch that fails leaves the refresh retryable"):
    val (mgr, client, _) = newManager()

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(1)

    // Pre-fix the release lives in a Future.foreach, which runs on Success only, so this
    // completion never reaches it and the guard stays true forever.
    client.bootstrapConfigPromise(1).failure(new RuntimeException("read failed"))

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(2)

  test("a getBootstrapConfig call that throws leaves the refresh retryable"):
    val (mgr, client, _) = newManager()

    client.failBootstrapConfigWith(new RuntimeException("getBootstrapConfig exploded"))

    // Pre-fix this throw propagates straight out of dropStoreMapping -- past the guard it just
    // set -- and into whatever called it, typically the network IO thread.
    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(1)

    client.clearBootstrapConfigFailure()

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(2)

  test("a refresh already in flight coalesces a second request"):
    val (mgr, client, _) = newManager()

    // This passes against the unfixed code: coalescing is the flag's actual purpose and the one
    // behaviour it gets right. It is here so the rewrite cannot quietly drop it.
    mgr.dropStoreMapping(bootstrapStoreId)
    mgr.dropStoreMapping(bootstrapStoreId)

    client.bootstrapConfigFetches should be(1)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *MetadataManagerBootstrapRefreshSuite'`
Expected: 3 tests, 2 failures.
- "a bootstrap config fetch that fails..." — FAIL, `2 was not equal to 1` on the final assertion (only one fetch happened).
- "a getBootstrapConfig call that throws..." — FAIL, the `RuntimeException("getBootstrapConfig exploded")` escapes `dropStoreMapping` and aborts the test.
- "a refresh already in flight coalesces..." — PASS.

- [ ] **Step 3: Rewrite `refreshBootstrapConfig`**

In `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`, replace the whole of `refreshBootstrapConfig` (lines 193-215) with:

```scala
  /** Refetches the bootstrap config and reinstalls the store -> host mappings it names.
   *
   *  refreshingBootstrapConfig coalesces concurrent requests: a bootstrap store whose host has
   *  moved is usually noticed by several sends at once, and one refetch answers all of them.
   *  Because it is a plain Boolean rather than an entry in a map, a path that leaves it set
   *  disables refresh permanently -- and dropStoreMapping has no fallback for a bootstrap store,
   *  so the correction is dropped rather than degraded. Every exit therefore releases it.
   *
   *  The try/catch wraps the getBootstrapConfig call and nothing else. Widening it to cover
   *  onComplete would, under an inline or parasitic ExecutionContext, also catch a throw coming
   *  back out of the callback body -- which the callback's own catch already handles, and which
   *  would be released twice. The narrow scope leaves onComplete itself unguarded: an
   *  ExecutionContext that rejects the submission still strands the flag.
   *
   *  clientContext is an abstract def, so binding it can throw. It is evaluated before the flag
   *  is set, so such a throw propagates with nothing acquired. Unlike
   *  StoreManager.startDeviceCheck this method has no caller in a finally, so a propagating
   *  throw masks nothing.
   */
  private def refreshBootstrapConfig(): Unit =
    if !refreshingBootstrapConfig then
      logger.info("Refreshing bootstrap config file")
      oClient match
        case None => logger.error(s"Refreshing bootstrap config before AspenClient initialized!")
        case Some(client) =>
          given ExecutionContext = client.clientContext
          refreshingBootstrapConfig = true

          val fetch =
            try Success(client.getBootstrapConfig())
            catch case t: Throwable => Failure(t)

          fetch match
            case Success(fCfg) =>
              fCfg.onComplete: result =>
                try
                  result match
                    case Success(cfg) =>
                      try
                        atomicWrite(bootstrapConfigFile.toNIO, cfg)
                        logger.info(s"Updated bootstrap config written to $bootstrapConfigFile")
                      catch
                        case NonFatal(t) =>
                          logger.error(s"Failed to update bootstrap config file $bootstrapConfigFile. Error: $t", t)

                      synchronized:
                        BootstrapConfig.loadBootstrapConfig(bootstrapConfigFile.toIO).hosts.foreach: bsHost =>
                          bsHost.stores.foreach: storeId =>
                            stores += storeId -> bsHost.hostId

                    case Failure(err) =>
                      logger.error(s"Failed to fetch the bootstrap config. Error: $err", err)
                catch
                  case NonFatal(t) =>
                    logger.error(s"Failed to apply the fetched bootstrap config. Error: $t", t)
                finally
                  synchronized:
                    refreshingBootstrapConfig = false

            case Failure(t) =>
              // Repair first, log second: a throwing logger must not be able to wedge the flag.
              refreshingBootstrapConfig = false
              logger.error(s"The getBootstrapConfig call threw. Error: $t", t)
```

Four things changed and one deliberately has not:
- `given ExecutionContext` now precedes `refreshingBootstrapConfig = true`, so a `clientContext` throw cannot strand a flag it never set.
- `.foreach` became `.onComplete`, so a failed `Future` reaches the release.
- The synchronous throw is captured and handled rather than propagating past the set flag.
- A callback-body throw is caught, and the release sits in a `finally` below it.
- The body still writes the file and then re-reads it. That is Task 4's problem; leaving it here keeps this task's diff to the guard.

`Success`, `Failure`, `NonFatal` and `ExecutionContext` are all already imported (lines 10-12).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `sbt 'testOnly *MetadataManagerBootstrapRefreshSuite'`
Expected: PASS — 3 tests, 0 failures.

- [ ] **Step 5: Run the whole MetadataManager set**

Run: `sbt 'testOnly *MetadataManager*'`
Expected: PASS — 5 suites, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala
git commit -m "Release the bootstrap refresh guard when the config fetch fails"
```

---

## Task 4: Apply the config that was fetched

Two defects share one cause: the callback writes the fetched string to disk and then re-reads the file. A config that does not parse is written before anyone discovers it is garbage, and a write that fails leaves the re-read returning the *stale* config — so a good fetch is discarded because of a disk problem.

Both go away by parsing the string first and mapping from the parsed result.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala` (`refreshBootstrapConfig` callback body; add `applyBootstrapConfig`)

- [ ] **Step 1: Write the failing tests**

Append to the class in `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala`:

```scala
  test("a fetched config that does not parse leaves the file unchanged and the refresh retryable"):
    val (mgr, client, _, configFile) = newManagerWithConfigFile()

    val before = os.read(configFile)

    mgr.dropStoreMapping(bootstrapStoreId)

    // Well-formed YAML that BootstrapConfig.Config rejects: two stores against an IDA of width
    // one. A syntactically broken string would do too, but this exercises the same validation a
    // real mis-generated config would trip.
    val unparseable = bootstrapConfigYaml(
      Replication(1, 1),
      List(remoteHostState),
      List(bootstrapStoreId -> remoteHostId, secondStoreId -> remoteHostId))

    client.bootstrapConfigPromise(1).success(unparseable)

    // Pre-fix the string is written before anything parses it, so the on-disk config is replaced
    // by one the process cannot read back -- and a restart then fails at construction.
    os.read(configFile) should be(before)

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(2)

  test("a config that cannot be written is still applied in memory"):
    val (mgr, client, _, configFile) = newManagerWithConfigFile()

    mgr.dropStoreMapping(bootstrapStoreId)

    // atomicWrite creates its temp file in the target's parent directory, so removing the
    // directory makes the write fail with NoSuchFileException whatever the suite's privileges.
    os.remove.all(configFile / os.up)

    client.bootstrapConfigPromise(1).success(bootstrapConfigYaml(
      Replication(1, 1),
      List(remoteHostState),
      List(bootstrapStoreId -> remoteHostId)))

    // The store now routes to remoteHostId, which has no HostEntry, so reaching it starts a
    // lookup and parks the message. Pre-fix the failed write leaves the re-read serving the old
    // config -- the store still maps to bootstrapHostId and this returns Some.
    mgr.getHostEntryOrQueueMessage(bootstrapStoreId, nudge()) should be(None)
    client.lookups.toList should be(List(remoteHostId))

    mgr.dropStoreMapping(bootstrapStoreId)
    client.bootstrapConfigFetches should be(2)
```

`Replication` needs an import. Add it to the file's import block so it reads:

```scala
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
```

`remoteHostState`, `remoteHostId`, `bootstrapConfigYaml` and `newManagerWithConfigFile` all come from `MetadataManagerFixture`. `os` is available without an import — os-lib's package object is a project-wide dependency and `MetadataManagerFixture` already uses `os.Path` unqualified.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *MetadataManagerBootstrapRefreshSuite'`
Expected: 5 tests, 2 failures.
- "a fetched config that does not parse..." — FAIL on `os.read(configFile) should be(before)`; the garbage config is already on disk.
- "a config that cannot be written..." — FAIL on `should be(None)`; the mapping still points at `bootstrapHostId`, so a `HostEntry` comes back.

- [ ] **Step 3: Extract `applyBootstrapConfig`**

In `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`, inside `refreshBootstrapConfig`, replace the `case Success(cfg) =>` arm — everything from `try` through the `stores += storeId -> bsHost.hostId` line — with a single call:

```scala
                    case Success(cfg) => applyBootstrapConfig(cfg)
```

so the `onComplete` body reads:

```scala
              fCfg.onComplete: result =>
                try
                  result match
                    case Success(cfg) => applyBootstrapConfig(cfg)
                    case Failure(err) =>
                      logger.error(s"Failed to fetch the bootstrap config. Error: $err", err)
                catch
                  case NonFatal(t) =>
                    logger.error(s"Failed to apply the fetched bootstrap config. Error: $t", t)
                finally
                  synchronized:
                    refreshingBootstrapConfig = false
```

Then add the new method immediately after `refreshBootstrapConfig`:

```scala
  /** Installs a freshly fetched bootstrap config, writing it through to disk on the way.
   *
   *  Parse first. The config is written only once it is known to be readable, so a bad fetch
   *  cannot replace a good file with one that fails at the next construction. A parse failure
   *  propagates to the caller's catch, which logs it; the guard release is in that caller's
   *  finally, so a rejected config leaves the refresh retryable.
   *
   *  A failed write does not stop the mapping being applied. The mapping comes from the parsed
   *  config rather than from a re-read of the file, so the process picks up the new placements
   *  immediately and only loses them across a restart -- strictly better than discarding a good
   *  config because the disk is full or read-only.
   */
  private def applyBootstrapConfig(cfg: String): Unit =
    val config = BootstrapConfig.parseBootstrapConfig(cfg)

    try
      atomicWrite(bootstrapConfigFile.toNIO, cfg)
      logger.info(s"Updated bootstrap config written to $bootstrapConfigFile")
    catch
      case NonFatal(t) =>
        logger.error(s"Failed to update bootstrap config file $bootstrapConfigFile. Error: $t", t)

    synchronized:
      config.hosts.foreach: bsHost =>
        bsHost.stores.foreach: storeId =>
          stores += storeId -> bsHost.hostId
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `sbt 'testOnly *MetadataManagerBootstrapRefreshSuite'`
Expected: PASS — 5 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala
git commit -m "Parse the fetched bootstrap config before writing it and map from the parsed result"
```

---

## Task 5: Keep `bootstrapStores` in step with a refresh

`bootstrapStores` is written once, in the constructor, and never again. A refreshed config that names a store the original did not leaves that store outside the set, so `dropStoreMapping` sends it down the `stores -= storeId` branch instead of refreshing — and the next send starts a pool lookup, which must read the bootstrap pool's state through the very stores that are unreachable.

The fix routes both the constructor and a refresh through one method.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala:71-84` (constructor block), plus `applyBootstrapConfig`

- [ ] **Step 1: Write the failing test**

Append to the class in `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala`:

```scala
  test("a successful refresh remaps the store and adopts new bootstrap stores"):
    val (mgr, client, _) = newManager()

    mgr.isBootstrapStore(bootstrapStoreId) should be(true)
    mgr.isBootstrapStore(secondStoreId) should be(false)

    mgr.dropStoreMapping(bootstrapStoreId)

    // The refreshed config moves the existing store to remoteHostId and adds a second one there.
    // Two stores means the IDA width must be two -- BootstrapConfig.Config rejects a mismatch.
    client.bootstrapConfigPromise(1).success(bootstrapConfigYaml(
      Replication(2, 2),
      List(remoteHostState),
      List(bootstrapStoreId -> remoteHostId, secondStoreId -> remoteHostId)))

    mgr.getHostEntryOrQueueMessage(bootstrapStoreId, nudge()) should be(None)
    client.lookups.toList should be(List(remoteHostId))

    // Pre-fix bootstrapStores is written once, by the constructor, so the newly named store is
    // treated as an ordinary store: dropStoreMapping would drop its mapping and the next send
    // would start a pool lookup against the bootstrap pool it cannot reach.
    mgr.isBootstrapStore(secondStoreId) should be(true)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *MetadataManagerBootstrapRefreshSuite'`
Expected: 6 tests, 1 failure — "a successful refresh remaps the store and adopts new bootstrap stores" fails on the last assertion, `false was not true`. The remap assertions above it already pass.

- [ ] **Step 3: Extract `mapBootstrapStores` and route the constructor through it**

In `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`, replace the tail of `applyBootstrapConfig` — the `synchronized:` block and the two nested `foreach`es under it — with:

```scala
    synchronized:
      mapBootstrapStores(config)
```

Add `mapBootstrapStores` immediately after `applyBootstrapConfig`:

```scala
  /** Installs every store -> host mapping the config names and marks each store as a bootstrap
   *  store. Caller holds this object's monitor.
   *
   *  Add-only, mirroring `stores`. A store the refreshed config omits keeps both its old mapping
   *  and its bootstrap status, which is the conservative reading: the config is the only source
   *  that can name a bootstrap store, and forgetting one would send it down dropStoreMapping's
   *  non-bootstrap branch -- a pool lookup against a pool whose state can only be read through
   *  the bootstrap stores themselves.
   *
   *  The constructor and a refresh share this method so the two can never disagree about which
   *  stores are bootstrap stores.
   */
  private def mapBootstrapStores(config: BootstrapConfig.Config): Unit =
    config.hosts.foreach: bsHost =>
      bsHost.stores.foreach: storeId =>
        stores += storeId -> bsHost.hostId
        bootstrapStores += storeId
```

- [ ] **Step 4: Route the constructor through it too**

Replace the constructor block at lines 71-84 — the whole `BootstrapConfig.loadBootstrapConfig(...).hosts.foreach` statement — with a single call:

```scala
  loadInitialBootstrapConfig()
```

and add the method after `mapBootstrapStores`:

```scala
  /** Loads the on-disk bootstrap config at construction: the store mappings, plus a HostEntry
   *  for each named host so those hosts are reachable before any lookup completes.
   *
   *  Only the mapping half is shared with a refresh. Creating a HostEntry is a constructor-only
   *  concern: a refresh runs against a manager whose hosts map is already populated and whose
   *  entries may hold parked messages, so recreating them would discard queued work.
   *
   *  No synchronized: this runs during construction, before the instance escapes.
   */
  private def loadInitialBootstrapConfig(): Unit =
    val config = BootstrapConfig.loadBootstrapConfig(bootstrapConfigFile.toIO)

    mapBootstrapStores(config)

    config.hosts.foreach: bsHost =>
      hosts += bsHost.hostId -> Right(networkImplInterface.createHostEntry(
        bsHost.hostId, bsHost.name, bsHost.address, bsHost.dataPort,
        bsHost.cncPort, bsHost.storeTransferPort, new EvictingQueue[Message](1)))
```

Calling a private `def` from a constructor statement that appears earlier in the class body is fine — methods are not initialization-ordered state.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *MetadataManagerBootstrapRefreshSuite'`
Expected: PASS — 6 tests, 0 failures.

- [ ] **Step 6: Run the whole MetadataManager set**

Run: `sbt 'testOnly *MetadataManager*'`
Expected: PASS — 5 suites, 0 failures. `MetadataManagerDrainSuite` is the one that would catch a mistake in Step 4: it asserts on messages parked against bootstrap `HostEntry`s created at construction.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala
git commit -m "Refresh bootstrapStores alongside the store mappings"
```

---

## Task 6: TODO.txt and full verification

**Files:**
- Modify: `TODO.txt`

- [ ] **Step 1: Read TODO.txt and find the refresh entry**

Run: `grep -n "refreshingBootstrapConfig\|refreshBootstrapConfig" TODO.txt`
Expected: the entry describing the guard that can stick `true`.

- [ ] **Step 2: Remove the fixed entry and record the finding this work surfaced**

Delete the `refreshBootstrapConfig` guard entry in full — it is fixed.

Add a new entry in its place, matching the surrounding entries' format:

```
MetadataManager never updates a bootstrap host's HostEntry.

  loadInitialBootstrapConfig creates one HostEntry per bootstrap host at construction, and
  applyBootstrapConfig only ever touches `stores` and `bootstrapStores`. A refreshed config that
  changes a bootstrap host's address or any of its three ports is therefore ignored: the manager
  keeps sending to the old endpoint forever. Only a restart picks the change up.

  Refreshing the entry is not a one-line change -- an existing entry may hold parked messages and
  may be the subject of an in-flight lookup, so replacing one needs a rule for what happens to
  both. Deferred for that reason, not overlooked.
```

- [ ] **Step 3: Run the full test suite**

Run: `sbt test`
Expected: PASS — 0 failures across all suites. `MetadataManager` is broadly depended on, and `YamlFormat` is used by `StoreConfigSuite` and `MainSuite` as well.

- [ ] **Step 4: Commit**

```bash
git add TODO.txt
git commit -m "Retire the bootstrap refresh guard entry; record the stale HostEntry finding"
```

---

## Out of scope

Recorded in the spec with reasons; do not implement:

- **Retry pacing.** A released guard means every failed send against a moved bootstrap store starts a fresh fetch. Pacing that needs a policy, and the wedge is the worse failure.
- **Removal semantics for `stores` / `bootstrapStores`.** Both stay add-only. Deciding when a store may be forgotten is a separate design question.
- **Stale bootstrap `HostEntry`s.** Task 6 files it; the fix is not attempted.
- **Other guards.** `pendingPoolLookups` and `hosts` use map presence rather than a Boolean and were hardened on 2026-07-30. `refreshingBootstrapConfig` is the only remaining Boolean guard.

---

## Verification summary

| Test | Task | Pre-fix result |
|---|---|---|
| a bootstrap config fetch that fails leaves the refresh retryable | 3 | FAIL — one fetch, not two |
| a getBootstrapConfig call that throws leaves the refresh retryable | 3 | FAIL — the throw escapes `dropStoreMapping` |
| a refresh already in flight coalesces a second request | 3 | PASS — regression guard only |
| a fetched config that does not parse leaves the file unchanged and the refresh retryable | 4 | FAIL — garbage is written to disk |
| a config that cannot be written is still applied in memory | 4 | FAIL — the stale mapping survives |
| a successful refresh remaps the store and adopts new bootstrap stores | 5 | FAIL — `isBootstrapStore` stays false |

Each failing test must be confirmed red against the code as it stands at the start of its own task, before that task's production change is applied.
