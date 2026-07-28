# `create-storage-device` CLI Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a CLI command `create-storage-device` that registers a new storage device on an existing host in one atomic transaction and writes the device's on-disk config file so the host's `StoreManager` discovers it.

**Architecture:** Three layers. `AspenClient.createStorageDevice` (implemented in `BaseAspenClient`) performs the metadata transaction — allocate a `StorageDeviceState` KV object, register it in `storageDevicesTree`, add the id to the owning `HostState`, add the id to the target level-0 `StorageDeviceSetState`. `server.StorageDeviceManager` validates the target directory and host config, calls the client, then writes `aspen-storage-device-config.yaml` onto the device. `Main.create_storage_device` does argument plumbing and error reporting only. The device directory is provisioned by the operator (a real mount point, or a symlink under `storage-devices/` pointing at one) — the command never creates it.

**Tech Stack:** Scala 3 (quiet syntax, indented `if/then/else`), sbt, ScalaTest (`AnyFunSuite` for pure units, `IntegrationTestSuite`/`AsyncFunSuite` for anything touching a client), SnakeYAML via `YamlFormat`, scopt for CLI parsing, `java.nio.file` for filesystem work in the `server` package.

**Source spec:** `docs/superpowers/specs/2026-07-27-create-storage-device-design.md`

---

## Deviations from the spec

Two, both flagged deliberately:

1. **The spec's `MainSuite` parser test is dropped.** The spec's testing section calls for a `MainSuite` test asserting that `create-storage-device` parses into the correct mode with all four fields populated. The scopt `OParser` is constructed *inside* `def main` in `Main.scala` and is not reachable from a test. (The prior `move-device-to-set` spec made the same claim and commit `425fbff` correctly never added such a test.) Extracting the parser is a worthwhile refactor but is a separate, larger change. Instead, the CLI's one piece of real logic — resolving a bare `<device-name>` to `<host-dir>/storage-devices/<name>` — is extracted into the pure helper `StorageDeviceManager.deviceDirectory` and tested directly (Task 8).

2. **Three latent config-file defects surface as soon as the spec's round-trip tests are written, and are fixed here.** All three are in code this branch already touches:
   - `HostConfig.yamlConfig` interpolates `$hostId`, a `case class HostId(uuid: UUID)` with no `toString` override, so it emits `hostState-id: HostId(1111…)` rather than a bare UUID. `HostConfig.loadHostConfig` cannot read its own output.
   - `StorageDeviceConfig.yamlConfig` has the identical defect with `$storageDeviceId`. This one matters directly: `StoreManager.tryLoadDevice` reads that file, and the new command writes it.
   - `BootstrapConfig.generateBootstrapConfig` appends `"bootstrap-hostStates:"` with no trailing `\n`, so the first host entry is concatenated onto the key line.

   The spec's own testing section requires round-trip tests for `HostConfig`, `BootstrapConfig`, and the written device config, so these fixes are prerequisites, not scope creep. They are kept in their own commits (Tasks 1–3), separate from the feature.

---

## File Structure

**Modified**

| File | Change |
|---|---|
| `src/main/scala/org/aspen_ddp/aspen/server/HostConfig.scala` | yaml key `hostState-id` → `host-id`; filename `aspen-hostState-config.yaml` → `aspen-host-config.yaml`; emit bare UUID; doc comment |
| `src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceConfig.scala` | emit bare UUID |
| `src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala` | yaml keys `bootstrap-hostStates` → `bootstrap-hosts`, `hostState-id` → `host-id`; missing newline; doc comment; error message |
| `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` | declare `createStorageDevice` on the trait |
| `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` | implement `createStorageDevice` |
| `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` | use `StorageDeviceManager.StorageDevicesDirName` |
| `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` | `Args.deviceName`; `<hoste-directory>` typo; parser entry; dispatch; `create_storage_device`; use `StorageDevicesDirName` |
| `TODO.txt` | remove the completed item |

**Created**

| File | Responsibility |
|---|---|
| `src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceManager.scala` | Validate a device directory, drive the creation transaction, write the device config file. The shared `storage-devices` directory-name constant lives here. |
| `src/test/scala/org/aspen_ddp/aspen/server/HostConfigSuite.scala` | Host config yaml format and round-trip |
| `src/test/scala/org/aspen_ddp/aspen/server/StorageDeviceConfigSuite.scala` | Device config yaml round-trip |
| `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigSuite.scala` | Bootstrap config generate/load round-trip |
| `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceCreationSuite.scala` | `AspenClient.createStorageDevice` against a `TestNetwork` |
| `src/test/scala/org/aspen_ddp/aspen/server/StorageDeviceManagerSuite.scala` | `StorageDeviceManager` validation, path resolution, and config-file write |

---

## Task 1: Host config file format

Fixes the `hostState` naming corruption in `HostConfig` and makes the generated file readable by its own loader.

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/server/HostConfigSuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/HostConfig.scala:12-19` (doc comment), `:29` (yaml key + UUID), `:51` (filename), `:67` (parser key)
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:155` (arg label typo)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/server/HostConfigSuite.scala`:

```scala
package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.HostId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

class HostConfigSuite extends AnyFunSuite with Matchers:

  private val sample = HostConfig(
    HostId(UUID.fromString("11111111-1111-1111-1111-111111111111")),
    UUID.fromString("22222222-2222-2222-2222-222222222222"),
    "node_a",
    "127.0.0.1",
    4750,
    4751,
    4752,
    HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300))

  test("configFilename is aspen-host-config.yaml"):
    HostConfig.configFilename should be("aspen-host-config.yaml")

  test("yamlConfig uses the host-id key and a bare UUID"):
    sample.yamlConfig should include("host-id: 11111111-1111-1111-1111-111111111111")
    sample.yamlConfig should not include "hostState"

  test("loadHostConfig round-trips yamlConfig"):
    val f = Files.createTempFile("aspen-host-config", ".yaml")
    try
      Files.write(f, sample.yamlConfig.getBytes(StandardCharsets.UTF_8))
      HostConfig.loadHostConfig(f.toFile) should be(sample)
    finally
      Files.deleteIfExists(f)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *HostConfigSuite'`

Expected: FAIL. Three failures — `"aspen-hostState-config.yaml" was not equal to "aspen-host-config.yaml"`, the `include` assertion on `host-id:` (the emitted line is `hostState-id: HostId(11111111-…)`), and the round-trip, which raises `org.aspen_ddp.aspen.common.util.YamlFormat$FormatError` for the missing `host-id` key.

- [ ] **Step 3: Fix `HostConfig.scala`**

Replace the doc comment at lines 12-19:

```scala
/*
host-id: 00000000-0000-0000-0000-000000000000
name: bootstrap-host
aspen-system-id: 00000000-0000-0000-0000-000000000000
data-port: 4750
cnc-port: 4751
store-transfer-port: 4752
*/
```

Change line 29 (yaml key and bare UUID):

```scala
    val base = s"""host-id: ${hostId.uuid}
```

Change line 51:

```scala
  val configFilename = "aspen-host-config.yaml"
```

Change line 67:

```scala
  val hostId: Required[HostId]         = Required("host-id", HostId.YHostId)
```

- [ ] **Step 4: Fix the `<hoste-directory>` typo in `Main.scala`**

At `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:155`, change:

```scala
          arg[File]("<hoste-directory>").text("Host Directory").
```

to:

```scala
          arg[File]("<host-directory>").text("Host Directory").
```

This is a display label in the scopt parser, which is built inside `def main` and has no test. It is part of the same rename table in the spec.

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *HostConfigSuite'`
Expected: PASS — `Tests: succeeded 3, failed 0`

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/HostConfig.scala \
        src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala \
        src/test/scala/org/aspen_ddp/aspen/server/HostConfigSuite.scala
git commit -m "fix: correct host config yaml keys and UUID rendering

The generated host config used 'hostState-id' (an accidental
rename) and interpolated the HostId case class rather than its
UUID, so HostConfig.loadHostConfig could not read its own output.
Also renames the config file to aspen-host-config.yaml and fixes
the '<hoste-directory>' CLI argument label."
```

---

## Task 2: Bootstrap config file format

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigSuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala:13-31` (doc comment), `:77`, `:100`, `:105`, `:138`, `:144`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

class BootstrapConfigSuite extends AnyFunSuite with Matchers:

  private val systemId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val hostId   = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
  private val poolId   = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
  private val storeId  = StoreId(poolId, 0.toByte)

  private val hostState =
    HostState(hostId, "node_a", "127.0.0.1", 5000, 5001, 5002, Set())

  private def generated: String =
    BootstrapConfig.generateBootstrapConfig(
      systemId,
      Replication(1, 1),
      List(hostState),
      List(storeId -> hostId))

  test("generateBootstrapConfig uses the bootstrap-hosts and host-id keys"):
    generated should include("bootstrap-hosts:")
    generated should include("- host-id: 11111111-1111-1111-1111-111111111111")
    generated should not include "hostState"

  test("loadBootstrapConfig round-trips generateBootstrapConfig"):
    val f = Files.createTempFile("aspen-bootstrap-config", ".yaml")
    try
      Files.write(f, generated.getBytes(StandardCharsets.UTF_8))
      val cfg = BootstrapConfig.loadBootstrapConfig(f.toFile)

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
    finally
      Files.deleteIfExists(f)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *BootstrapConfigSuite'`

Expected: FAIL. The first test fails on `bootstrap-hosts:` / `host-id:` not being present. The second fails when SnakeYAML parses the run-together line `bootstrap-hostStates:  - hostState-id: …` — the value is not the expected list, so loading raises a `FormatError` (or a SnakeYAML scanner error).

- [ ] **Step 3: Fix `BootstrapConfig.scala`**

Replace lines 20-23 of the doc comment:

```scala
bootstrap-hosts:
  - host-id: AA1049AD-D2A8-4D17-8080-E01A4678C8B3
    name: node_a
    address: 127.0.0.1
```

Change line 77:

```scala
    val hostId: Required[HostId]         = Required("host-id", HostId.YHostId)
```

Change line 100:

```scala
      throw new FormatError("Number of hosts must exactly match the Bootstrap IDA width")
```

Change line 105:

```scala
    val hosts: Required[List[BootstrapHost]] = Required("bootstrap-hosts", YList(BootstrapHost))
```

Change line 138 (key rename **and** the missing newline, without which the first host entry lands on the key's line):

```scala
    sb.append("bootstrap-hosts:\n")
```

Change line 144:

```scala
      sb.append(f"  - host-id: ${host.hostId.uuid}\n")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *BootstrapConfigSuite'`
Expected: PASS — `Tests: succeeded 2, failed 0`

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfigSuite.scala
git commit -m "fix: correct bootstrap config yaml keys and host list newline

Renames 'bootstrap-hostStates'/'hostState-id' back to
'bootstrap-hosts'/'host-id' and adds the newline after the
'bootstrap-hosts:' key, without which the first host entry was
appended to the key's own line and the file would not parse."
```

---

## Task 3: Storage device config file round-trip

`StorageDeviceConfig` has the same UUID-rendering defect as `HostConfig`. The new command writes this file and `StoreManager.tryLoadDevice` reads it, so it must round-trip before the feature is built on top of it.

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/server/StorageDeviceConfigSuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceConfig.scala:17`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/server/StorageDeviceConfigSuite.scala`:

```scala
package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

class StorageDeviceConfigSuite extends AnyFunSuite with Matchers:

  private val sample = StorageDeviceConfig(
    StorageDeviceId(UUID.fromString("33333333-3333-3333-3333-333333333333")),
    UUID.fromString("22222222-2222-2222-2222-222222222222"))

  test("yamlConfig emits a bare device UUID"):
    sample.yamlConfig should include(
      "storage-device-id: 33333333-3333-3333-3333-333333333333")

  test("loadHostConfig round-trips yamlConfig"):
    val f = Files.createTempFile("aspen-storage-device-config", ".yaml")
    try
      Files.write(f, sample.yamlConfig.getBytes(StandardCharsets.UTF_8))
      StorageDeviceConfig.loadHostConfig(f.toFile) should be(sample)
    finally
      Files.deleteIfExists(f)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *StorageDeviceConfigSuite'`

Expected: FAIL. The emitted line is `storage-device-id: StorageDeviceId(33333333-…)`, so the `include` assertion fails and the round-trip raises `FormatError: Invalid UUID: …`.

- [ ] **Step 3: Fix `StorageDeviceConfig.scala`**

Change line 17:

```scala
    s"""storage-device-id: ${storageDeviceId.uuid}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *StorageDeviceConfigSuite'`
Expected: PASS — `Tests: succeeded 2, failed 0`

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceConfig.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StorageDeviceConfigSuite.scala
git commit -m "fix: emit a bare UUID in the storage device config file

yamlConfig interpolated the StorageDeviceId case class, producing
'storage-device-id: StorageDeviceId(...)', which
StorageDeviceConfig.loadHostConfig rejects."
```

---

## Task 4: Declare `AspenClient.createStorageDevice`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:118` (insert after `createStorageDeviceSet`)

- [ ] **Step 1: Add the trait method**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, immediately after this existing line 118:

```scala
  def createStorageDeviceSet(name: String, level: Int, parent: Option[StorageDeviceSetId]): Future[StorageDeviceSetId]
```

insert:

```scala

  /** Register a new storage device on `hostId` and place it in the level-0 device set
   *  `deviceSetId`, in a single atomic transaction: a new StorageDeviceState object is
   *  allocated and registered in the storage devices tree, the owning HostState gains the
   *  device id, and the set's memberDevices gains the device id.
   *
   *  Fails with NoSuchElementException if the host or the set is unknown, or with
   *  StorageDeviceSetState.NotLevelZero if the set is not level 0.
   *
   *  No store is created. Stores appear on the device when a pool is assigned to it. */
  def createStorageDevice(hostId: HostId, deviceSetId: StorageDeviceSetId): Future[StorageDeviceId]
```

- [ ] **Step 2: Verify it does not compile yet**

Run: `sbt compile`
Expected: FAIL — `error: class BaseAspenClient needs to be abstract, since def createStorageDevice … is not defined`. Task 5 supplies the implementation; do not commit at this point.

---

## Task 5: Implement `createStorageDevice` in `BaseAspenClient`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala:225` (insert after `createStorageDeviceSet`)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceCreationSuite.scala`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceCreationSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.IntegrationTestSuite

import java.util.UUID
import scala.concurrent.ExecutionContext

class StorageDeviceCreationSuite extends IntegrationTestSuite:

  private val bootstrapHost = HostId.BootstrapHostId
  private val bootstrapSet  = StorageDeviceSetId.BootstrapStorageDeviceSetId

  atest("createStorageDevice registers a device with empty usage and no stores"):
    given ExecutionContext = executionContext
    for
      deviceId <- client.createStorageDevice(bootstrapHost, bootstrapSet)
      _        <- waitForTransactionsToComplete()
      ds       <- client.getStorageDeviceState(deviceId)
    yield
      ds.storageDeviceId should be(deviceId)
      ds.hostId should be(bootstrapHost)
      ds.storageDeviceSet should be(bootstrapSet)
      ds.stores should be(Map.empty)
      ds.currentUsage should be(0L)
      ds.totalSize should be(0L)

  atest("createStorageDevice adds the device to the host"):
    given ExecutionContext = executionContext
    for
      deviceId <- client.createStorageDevice(bootstrapHost, bootstrapSet)
      _        <- waitForTransactionsToComplete()
      hs       <- client.getHostState(bootstrapHost)
    yield
      hs.storageDevices should contain(deviceId)

  atest("createStorageDevice adds the device to the target set"):
    given ExecutionContext = executionContext
    for
      setId    <- client.createStorageDeviceSet("dev-target-set", level = 0, parent = None)
      _        <- waitForTransactionsToComplete()
      deviceId <- client.createStorageDevice(bootstrapHost, setId)
      _        <- waitForTransactionsToComplete()
      sds      <- client.getStorageDeviceSetState(setId)
    yield
      sds.memberDevices should be(List(deviceId))

  atest("two calls produce distinct devices, both registered on the host and set"):
    given ExecutionContext = executionContext
    for
      first  <- client.createStorageDevice(bootstrapHost, bootstrapSet)
      _      <- waitForTransactionsToComplete()
      second <- client.createStorageDevice(bootstrapHost, bootstrapSet)
      _      <- waitForTransactionsToComplete()
      hs     <- client.getHostState(bootstrapHost)
      sds    <- client.getStorageDeviceSetState(bootstrapSet)
    yield
      first should not be second
      hs.storageDevices should contain(first)
      hs.storageDevices should contain(second)
      sds.memberDevices should contain(first)
      sds.memberDevices should contain(second)

  atest("createStorageDevice fails with NotLevelZero when the set is not level 0"):
    given ExecutionContext = executionContext
    for
      level1Id <- client.createStorageDeviceSet("higher-set", level = 1, parent = None)
      _        <- waitForTransactionsToComplete()
      err      <- client.createStorageDevice(bootstrapHost, level1Id).failed
    yield
      err shouldBe a[StorageDeviceSetState.NotLevelZero]

  atest("createStorageDevice fails with NoSuchElementException for an unknown host"):
    given ExecutionContext = executionContext
    recoverToSucceededIf[NoSuchElementException](
      client.createStorageDevice(HostId(UUID.randomUUID()), bootstrapSet))
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *StorageDeviceCreationSuite'`
Expected: FAIL to compile — `error: class BaseAspenClient needs to be abstract, since def createStorageDevice … is not defined` (from Task 4).

- [ ] **Step 3: Write the implementation**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`, insert immediately after the end of `createStorageDeviceSet` (the line `        sds.setId`, currently line 225) and before `override protected def createStoragePool`:

```scala

  override def createStorageDevice(hostId: HostId,
                                   deviceSetId: StorageDeviceSetId): Future[StorageDeviceId] =
    // The id is generated once, outside runCreate, so a retried attempt reuses it rather
    // than leaking a fresh UUID per attempt. createStorageDeviceSet does the same with setId.
    val deviceId = StorageDeviceId(UUID.randomUUID())
    val state = StorageDeviceState(deviceId, hostId, 0L, 0L, Map(), deviceSetId)

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)
      case e: StorageDeviceSetState.NotLevelZero => throw StopRetrying(e)
      case _ => Future.unit

    runCreate(onFail): tx =>
      given Transaction = tx
      for
        bsPool   <- getStoragePool(PoolId.BootstrapPoolId)
        devPtr   <- bsPool.allocator.allocateKeyValueObject(
                      Map(StorageDeviceState.StateKey -> Value(state.encode())))
        _        <- storageDevicesTree.preparePut(deviceId.uuid, devPtr)

        hostPtr  <- getHostPointer(hostId)
        hostKvos <- read(hostPtr)
        setPtr   <- getStorageDeviceSetPointer(deviceSetId)
        setDos   <- read(setPtr)
      yield
        val hostState = HostState(hostKvos)
        tx.update(hostPtr, None, None,
          List(KeyRevision(HostState.StateKey,
                           hostKvos.contents(HostState.StateKey).revision)),
          List(Insert(HostState.StateKey,
                      hostState.addStorageDevice(deviceId).encode())))

        val setState = StorageDeviceSetState(setDos)

        if setState.level != 0 then
          throw new StorageDeviceSetState.NotLevelZero(deviceSetId)

        tx.overwrite(setPtr, setDos.revision,
          DataBuffer(setState.copy(memberDevices = deviceId :: setState.memberDevices).toBytes))

        deviceId
```

Devices have no name, so nothing is entered in `namespacedRegistry`; the UUID-keyed `storageDevicesTree` is the only registration.

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *StorageDeviceCreationSuite'`
Expected: PASS — `Tests: succeeded 6, failed 0`

- [ ] **Step 5: Run the full suite to check for regressions**

Run: `sbt test`
Expected: PASS — no new failures. (`TestNetwork.TClient` inherits the concrete `BaseAspenClient` implementation, so no test stub needs updating.)

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceCreationSuite.scala
git commit -m "feat: add AspenClient.createStorageDevice

Allocates a StorageDeviceState object, registers it in the storage
devices tree, adds the device to its host's HostState, and adds it
to a level-0 StorageDeviceSetState -- all in one transaction."
```

---

## Task 6: `StorageDeviceManager` skeleton and the shared directory-name constant

`"storage-devices"` is a bare literal in two places today and this feature would add a third. The constant lands first, on its own, so the later tasks can reference it.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceManager.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:106`
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:851`

- [ ] **Step 1: Create the file with the constant and the exception types**

Create `src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceManager.scala`:

```scala
package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId

import java.nio.file.Path
import java.util.UUID

/** Creation and on-disk setup of storage devices.
 *
 *  Stateless by design: StoreManager will call the same functions when device creation is
 *  automated, so nothing here may depend on running inside the CLI.
 */
object StorageDeviceManager:

  /** Name of the directory under a host's root directory that holds its storage devices.
   *  StoreManager scans the direct children of this directory at startup. */
  val StorageDevicesDirName = "storage-devices"

  class WrongAspenSystem(val expected: UUID, val found: UUID)
    extends Exception(s"Host config belongs to Aspen system $found, not $expected")

  class DeviceDirectoryNotUnderHost(val directory: Path, val expectedParent: Path)
    extends Exception(s"$directory is not a direct child of $expectedParent")

  class DeviceDirectoryNotFound(val directory: Path)
    extends Exception(s"Storage device directory does not exist: $directory")

  class DeviceAlreadyConfigured(val directory: Path)
    extends Exception(s"$directory already contains ${StorageDeviceConfig.configFilename}")

  /** Raised when the device metadata committed but the config file could not be written.
   *  Carries the device id because it is the only handle an operator has for recovery. */
  class ConfigWriteFailed(val storageDeviceId: StorageDeviceId,
                          val configFile: Path,
                          cause: Throwable)
    extends Exception(
      s"Storage device ${storageDeviceId.uuid} was registered but writing $configFile failed: ${cause.getMessage}",
      cause)

  /** Resolve the conventional location of a named storage device directory:
   *  `<host-directory>/storage-devices/<device-name>`.
   *
   *  `deviceName` is deliberately not sanitised here. A name containing a separator, a
   *  `..`, or an absolute path resolves outside the expected parent and is rejected by
   *  createStorageDevice's containment check. */
  def deviceDirectory(hostDirectory: Path, deviceName: String): Path =
    hostDirectory.resolve(StorageDevicesDirName).resolve(deviceName)
```

- [ ] **Step 2: Use the constant in `StoreManager`**

At `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:106`, change:

```scala
  val storageDevicesDir: Path = rootDir.resolve("storage-devices")
```

to:

```scala
  val storageDevicesDir: Path = rootDir.resolve(StorageDeviceManager.StorageDevicesDirName)
```

(`StoreManager` is in the same package; no import is needed.)

- [ ] **Step 3: Use the constant in `Main.bootstrap`**

At `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:851`, change:

```scala
    val storageDevicesDir = hostDirectory.resolve("storage-devices")
```

to:

```scala
    val storageDevicesDir = hostDirectory.resolve(StorageDeviceManager.StorageDevicesDirName)
```

(`Main.scala` already has `import org.aspen_ddp.aspen.server.*`; no import is needed.)

- [ ] **Step 4: Verify it compiles**

Run: `sbt compile`
Expected: PASS — `[success]`

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceManager.scala \
        src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "refactor: introduce StorageDeviceManager.StorageDevicesDirName

Replaces the duplicated 'storage-devices' literal in StoreManager
and Main.bootstrap with a single constant, and adds the
StorageDeviceManager object that device creation will live in."
```

---

## Task 7: `StorageDeviceManager.createStorageDevice`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceManager.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/StorageDeviceManagerSuite.scala`

- [ ] **Step 1: Write the failing test**

The suite extends `IntegrationTestSuite` so the success cases have a real `AspenClient`. The validation cases pass the same client; they never reach it because every check runs before the transaction.

Create `src/test/scala/org/aspen_ddp/aspen/server/StorageDeviceManagerSuite.scala`:

```scala
package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceSetId}

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class StorageDeviceManagerSuite extends IntegrationTestSuite:

  private val systemId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val bootstrapSet = StorageDeviceSetId.BootstrapStorageDeviceSetId

  /** A HostConfig for the TestNetwork's bootstrap host. Only hostId and aspenSystemId
   *  are consulted by createStorageDevice; the rest is filler. */
  private def hostConfig(sysId: UUID = systemId) = HostConfig(
    HostId.BootstrapHostId,
    sysId,
    "testhost",
    "localhost",
    1234,
    1235,
    1236,
    HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300))

  /** Creates `<tmp>/host/storage-devices` and returns the host directory. */
  private def newHostDir(): Path =
    val base = Files.createTempDirectory("aspen-sdm-test")
    val hostDir = base.resolve("host")
    Files.createDirectories(hostDir.resolve(StorageDeviceManager.StorageDevicesDirName))
    hostDir

  atest("deviceDirectory resolves <host-dir>/storage-devices/<name>"):
    Future.successful(
      StorageDeviceManager.deviceDirectory(Paths.get("/srv/aspen/node_a"), "dev2") should be(
        Paths.get("/srv/aspen/node_a/storage-devices/dev2")))

  atest("rejects a host config from a different Aspen system"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    recoverToSucceededIf[StorageDeviceManager.WrongAspenSystem](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(UUID.randomUUID()), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device directory that is a grandchild of storage-devices"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "foo/bar")
    Files.createDirectories(dir)
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotUnderHost](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device name that escapes upward"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "..")
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotUnderHost](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects an absolute device name"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val elsewhere = Files.createTempDirectory("aspen-elsewhere")
    val dir = StorageDeviceManager.deviceDirectory(hostDir, elsewhere.toString)
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotUnderHost](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device directory that does not exist"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "nope")
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotFound](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device directory that already holds a device config"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    Files.write(dir.resolve(StorageDeviceConfig.configFilename),
                "storage-device-id: x\n".getBytes(StandardCharsets.UTF_8))
    recoverToSucceededIf[StorageDeviceManager.DeviceAlreadyConfigured](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("creates the device and writes its config into a plain directory"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    for
      deviceId <- StorageDeviceManager.createStorageDevice(
                    client, hostConfig(), hostDir, dir, bootstrapSet, systemId)
      _        <- waitForTransactionsToComplete()
      ds       <- client.getStorageDeviceState(deviceId)
    yield
      val written = dir.resolve(StorageDeviceConfig.configFilename)
      Files.isRegularFile(written) should be(true)
      val cfg = StorageDeviceConfig.loadHostConfig(written.toFile)
      cfg.storageDeviceId should be(deviceId)
      cfg.aspenSystemId should be(systemId)
      ds.hostId should be(HostId.BootstrapHostId)
      ds.storageDeviceSet should be(bootstrapSet)

  atest("accepts a symlink under storage-devices and writes the config through it"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val elsewhere = Files.createTempDirectory("aspen-real-device")
    val link = hostDir.resolve(StorageDeviceManager.StorageDevicesDirName).resolve("dev2")
    Files.createSymbolicLink(link, elsewhere)
    for
      deviceId <- StorageDeviceManager.createStorageDevice(
                    client, hostConfig(), hostDir, link, bootstrapSet, systemId)
      _        <- waitForTransactionsToComplete()
    yield
      val written = elsewhere.resolve(StorageDeviceConfig.configFilename)
      Files.isRegularFile(written) should be(true)
      StorageDeviceConfig.loadHostConfig(written.toFile).storageDeviceId should be(deviceId)

  atest("reports ConfigWriteFailed, naming the device, when the write fails"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-xr-xr-x"))
    // A process running as root can still write to a read-only directory.
    assume(!Files.isWritable(dir))
    StorageDeviceManager
      .createStorageDevice(client, hostConfig(), hostDir, dir, bootstrapSet, systemId)
      .failed
      .map: err =>
        err shouldBe a[StorageDeviceManager.ConfigWriteFailed]
        val cwf = err.asInstanceOf[StorageDeviceManager.ConfigWriteFailed]
        err.getMessage should include(cwf.storageDeviceId.uuid.toString)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *StorageDeviceManagerSuite'`
Expected: FAIL to compile — `error: value createStorageDevice is not a member of object StorageDeviceManager`.

- [ ] **Step 3: Write the implementation**

Append to `src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceManager.scala`, and extend its import block to:

```scala
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
```

Then add, inside `object StorageDeviceManager`:

```scala

  /** Register `deviceDirectory` as a new storage device on `hostConfig`'s host and place it
   *  in the level-0 set `deviceSetId`, then write the device's config file into the
   *  directory so the host's StoreManager will discover it.
   *
   *  The directory must already exist. In production it is a mount point, or a symlink
   *  under `<host-directory>/storage-devices/` pointing at one; the config file has to land
   *  on the device itself, so this function never creates the directory.
   *
   *  Ordering: the transaction commits before the file is written, and the two cannot be
   *  made atomic. A crash in between leaves registered metadata with no on-disk device --
   *  inert, and recoverable by writing the file or removing the device. The reverse order
   *  would let a host load a device whose StorageDeviceState object does not exist.
   */
  def createStorageDevice(client: AspenClient,
                          hostConfig: HostConfig,
                          hostDirectory: Path,
                          deviceDirectory: Path,
                          deviceSetId: StorageDeviceSetId,
                          aspenSystemId: UUID)
                         (using ExecutionContext): Future[StorageDeviceId] =

    // Deliberately lexical: normalize but do not call toRealPath, so a symlink at
    // storage-devices/<name> pointing at a mount elsewhere still counts as contained.
    // Only direct children are accepted, because StoreManager scans only direct children.
    val expectedParent = hostDirectory.resolve(StorageDevicesDirName).toAbsolutePath.normalize
    val devDir = deviceDirectory.toAbsolutePath.normalize
    val configFile = devDir.resolve(StorageDeviceConfig.configFilename)

    if hostConfig.aspenSystemId != aspenSystemId then
      Future.failed(new WrongAspenSystem(aspenSystemId, hostConfig.aspenSystemId))

    else if devDir.getParent != expectedParent then
      Future.failed(new DeviceDirectoryNotUnderHost(devDir, expectedParent))

    else if !Files.isDirectory(devDir) then
      Future.failed(new DeviceDirectoryNotFound(devDir))

    else if Files.exists(configFile) then
      Future.failed(new DeviceAlreadyConfigured(devDir))

    else
      client.createStorageDevice(hostConfig.hostId, deviceSetId).map: deviceId =>
        val cfg = StorageDeviceConfig(deviceId, aspenSystemId)
        try
          Files.write(configFile, cfg.yamlConfig.getBytes(StandardCharsets.UTF_8))
        catch
          case t: Throwable => throw new ConfigWriteFailed(deviceId, configFile, t)
        deviceId
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *StorageDeviceManagerSuite'`
Expected: PASS — `Tests: succeeded 10, failed 0` (the `ConfigWriteFailed` test is cancelled rather than run if the JVM can write to a read-only directory, e.g. running as root).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StorageDeviceManagerSuite.scala
git commit -m "feat: add StorageDeviceManager.createStorageDevice

Validates the target directory and host config, commits the device
metadata transaction, then writes the device config file onto the
device. The containment check is lexical so a symlink under
storage-devices/ pointing at a mount elsewhere is accepted."
```

---

## Task 8: The `create-storage-device` CLI command

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:60` (`Args`), `:309` (parser), `:490` (dispatch), `:1211` (new function after `move_device_to_set`)
- Modify: `TODO.txt:1-16`

- [ ] **Step 1: Add the `deviceName` field to `Args`**

In `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, in the `Args` case class, insert `deviceName` immediately after the existing `deviceId` field (line 60):

```scala
                  deviceId:String="",
                  deviceName:String="",
```

- [ ] **Step 2: Add the parser entry**

Insert the following immediately **before** the existing line:

```scala
      cmd("transfer-store").text("Transfers a store to a different storage device").
```

New block:

```scala
      cmd("create-storage-device").text("Registers a new storage device on a host").
        action((_, c) => c.copy(mode = "create-storage-device")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[File]("<host-directory>").text("Host Directory").
            action((x, c) => c.copy(hostDirectory = x)).
            validate(x => if (x.exists()) success else failure(s"Host directory does not exist: $x")),

          arg[String]("<device-name>").text("Name of the already-provisioned device directory under <host-directory>/storage-devices").
            action((x, c) => c.copy(deviceName = x)),

          arg[String]("<set-name-or-uuid>").text("Name or UUID of the target level-0 device set").
            action((x, c) => c.copy(deviceSetName = x)),
        )

```

- [ ] **Step 3: Add the dispatch case**

Immediately after the existing line:

```scala
            case "move-device-to-set" => move_device_to_set(bootstrapConfigPath, cfg.deviceId, cfg.deviceSetName)
```

insert:

```scala
            case "create-storage-device" => create_storage_device(bootstrapConfig, bootstrapConfigPath,
                                                                  cfg.hostDirectory.toPath, cfg.deviceName,
                                                                  cfg.deviceSetName)
```

`bootstrapConfig` (the loaded config, not the path) is passed because the `aspenSystemId` check needs it. The `host` command does the same.

- [ ] **Step 4: Add the `create_storage_device` function**

Insert immediately after the closing brace of `move_device_to_set` and before `def transfer_store`:

```scala

  def create_storage_device(bootstrapCfg: BootstrapConfig.Config,
                            bootstrapConfigFile: os.Path,
                            hostDirectory: Path,
                            deviceName: String,
                            setRef: String): Unit = {

    configureLogging()

    val hostConfigFile = hostDirectory.resolve(HostConfig.configFilename)

    if !Files.isRegularFile(hostConfigFile) then
      println(s"Error: host configuration file not found: $hostConfigFile")
    else
      val hostCfg = HostConfig.loadHostConfig(hostConfigFile.toFile)

      // The device directory is required to sit at a fixed location, so the CLI takes the
      // bare name. Anything path-shaped resolves outside the expected parent and is
      // rejected by StorageDeviceManager's containment check.
      val deviceDirectory = StorageDeviceManager.deviceDirectory(hostDirectory, deviceName)

      val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

      network.startIoThread(client)

      given ExecutionContext = client.clientContext

      val f = for
        setId    <- resolveRef(setRef, StorageDeviceSetId(_), client.getStorageDeviceSetId)
        deviceId <- StorageDeviceManager.createStorageDevice(
                      client, hostCfg, hostDirectory, deviceDirectory,
                      setId, bootstrapCfg.aspenSystemId)
      yield deviceId

      def reportError(cause: Throwable): Unit = cause match
        case e: StorageDeviceManager.WrongAspenSystem =>
          println(s"Error: host config belongs to Aspen system ${e.found}, not ${e.expected}")
        case e: StorageDeviceManager.DeviceDirectoryNotUnderHost =>
          println(s"Error: ${e.directory} must be a direct child of ${e.expectedParent}")
        case e: StorageDeviceManager.DeviceDirectoryNotFound =>
          println(s"Error: storage device directory does not exist: ${e.directory}")
          println("The directory must be provisioned before this command is run.")
        case e: StorageDeviceManager.DeviceAlreadyConfigured =>
          println(s"Error: ${e.directory} already contains ${StorageDeviceConfig.configFilename}")
        case e: StorageDeviceManager.ConfigWriteFailed =>
          println(s"Error: device ${e.storageDeviceId.uuid} was registered but writing ${e.configFile} failed: ${e.getCause.getMessage}")
          println("Write that file by hand, or remove the device metadata and re-run.")
        case _: StorageDeviceSetState.NotLevelZero =>
          println(s"Error: device set '$setRef' must be a level-0 (tier-0) set")
        case _: NoSuchElementException =>
          println(s"Error: host or device set '$setRef' not found")
        case e =>
          println(s"Error creating storage device: ${e.getMessage}")

      f.onComplete:
        case scala.util.Success(deviceId) =>
          println(s"Created storage device ${deviceId.uuid} at $deviceDirectory")
        case scala.util.Failure(err) => reportError(err)

      Await.ready(f, Duration(30, SECONDS))
  }

`Main.scala` imports `org.aspen_ddp.aspen.server.crl.simple.SimpleCRL`, so a bare `SimpleCRL` in that file is the CRL backend, not `HostConfig.SimpleCRL`. The code above never names either, so there is no conflict — but the test suites in Tasks 1 and 7 must write `HostConfig.SimpleCRL(...)` explicitly.
```

- [ ] **Step 5: Verify it compiles and nothing regressed**

Run: `sbt compile`
Expected: PASS — `[success]`

Run: `sbt test`
Expected: PASS — no new failures.

- [ ] **Step 6: Exercise the command end to end**

```bash
rm -rf demo/bootstrap-host
sbt 'runMain org.aspen_ddp.aspen.cmdline.Main bootstrap'
mkdir -p demo/bootstrap-host/storage-devices/dev2
```

Then, in one terminal start the host and, in another, run the command (adjust the generated bootstrap config path if `bootstrap` reports a different one):

```bash
./t create-storage-device demo/bootstrap-host/bootstrap_config.yaml demo/bootstrap-host dev2 bootstrap
```

Expected: `Created storage device <uuid> at demo/bootstrap-host/storage-devices/dev2`, and `demo/bootstrap-host/storage-devices/dev2/aspen-storage-device-config.yaml` exists containing that uuid.

Then check the failure paths:

```bash
./t create-storage-device demo/bootstrap-host/bootstrap_config.yaml demo/bootstrap-host dev2 bootstrap
```

Expected: `Error: .../dev2 already contains aspen-storage-device-config.yaml`

```bash
./t create-storage-device demo/bootstrap-host/bootstrap_config.yaml demo/bootstrap-host missing bootstrap
```

Expected: `Error: storage device directory does not exist: .../storage-devices/missing`

- [ ] **Step 7: Remove the completed item from `TODO.txt`**

Delete the entire first block, lines 2-16 (from `Add CLI command "create-storage-device"` through the `- minimize the logic going into the cli code` line), plus the blank line that follows it, leaving `Reassignment of a pool to a new set:` as the first entry.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala TODO.txt
git commit -m "feat: add 'create-storage-device' CLI command

Takes a bootstrap config, a host directory, the bare name of an
already-provisioned device directory under <host-dir>/storage-devices,
and the target level-0 device set."
```

---

## Out of scope

Carried over from the spec, and deliberately not addressed by any task above:

- Creating or symlinking the device directory. That is the operator's job; the separate `add-storage-device` TODO item covers automating the link.
- Any CnC message telling the host to rescan. The host picks the device up on its next scan or at restart.
- Creating stores on the new device; that happens when a pool is assigned.
- The `hostState` corruption in TKVL and transaction test comments and test names, where the replaced word was usually `node` (`"Join on delete to empty hostState"`) and in one case a verb (`TransactionDriverSuite:152`, `"poolIndex doesn't hostState a slice"`). Correcting those needs per-site judgement.
- `HostState.removeStorageDevice`, which uses `+` where it means `-`. A real bug, unrelated, with no current caller.
- Extracting the scopt parser out of `def main` so CLI parsing becomes testable.
