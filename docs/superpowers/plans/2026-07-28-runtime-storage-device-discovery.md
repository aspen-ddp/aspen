# Runtime Storage Device Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a `StoreManager` discover and load storage devices added to `<hostRoot>/storage-devices/` while it is running, instead of only at construction, and have the `create-storage-device` CLI command notify the owning host so the device comes online in seconds rather than after the one-hour poll.

**Architecture:** All directory-scanning logic collapses into one new private method, `StoreManager.checkForNewDevices()`, called from the constructor and from the event loop. `tryLoadDevice` gains an idempotency guard so rescanning is safe. The push path reuses the existing `CheckStorageDevice` `HostMessage` — no wire-protocol change — and `create-storage-device` sends one after a successful registration, then briefly drains ZMQNet's send path so the short-lived CLI process does not abandon the message on exit. Polling remains the correctness guarantee; the message is only a latency optimization.

**Tech Stack:** Scala 3 (quiet/indented syntax per `CLAUDE.md`), sbt, ScalaTest `AsyncFunSuite` via the project's `IntegrationTestSuite`, jeromq (ZeroMQ), SnakeYAML.

**Source spec:** `docs/superpowers/specs/2026-07-28-runtime-storage-device-discovery-design.md`

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` | Modify | Adds `checkForNewDevices()`; makes `tryLoadDevice` idempotent; wires discovery into the `CheckAllDevices` and `CheckStorageDevice` handlers; adds two testing hooks; `tryLoadStore` becomes `protected` |
| `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala` | Modify | Adds side-effect-free `peekHostEntry` |
| `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala` | Modify | Tracks connected dealer sockets; adds `awaitHostMessagesSent` and `shutdown` |
| `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` | Modify | `create_storage_device` sends `CheckStorageDevice`, drains, and reports honestly instead of telling the operator to restart the host |
| `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala` | Create | All device-discovery tests plus the recording `StoreManager` subclass they share |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPeekHostEntrySuite.scala` | Create | `peekHostEntry` tests (added during execution; see Task 8) |
| `TODO.txt` | Modify | Removes the two items this work completes; records the known gaps execution deliberately left open |

---

## Deviations from the spec

Three, all recorded here so the spec and plan do not silently disagree:

1. **Testing hooks are synchronous, not queue-based.** The spec proposed
   `testingOnlyEnqueueCheckAllDevices()` plus `testingOnlyHandleEvents()`. That would also
   drain the `InitializeTaskExecutor()` event the constructor queues, which runs
   `initializeTaskExecutor()` — a real task-executor bootstrap that may issue a
   `transactUntilSuccessful` against the shared `TestNetwork`, with a `DoesNotExist`
   requirement that can never be satisfied if the network's own `StoreManager` already
   created the key. Instead this plan adds `testingOnlyCheckAllDevices()` and
   `testingOnlyHandleHostMessage(msg)`, which call `handleEvent` directly. The handler code
   under test is identical — `receiveHostMessage` does nothing but `events.put(HostMsg(msg))`.
2. **`ZMQNet.shutdown` iterates a ZMQNet-owned list of dealer sockets** rather than asking
   `MetadataManager` to enumerate host entries. This keeps the new `MetadataManager` surface
   to the single `peekHostEntry` method the spec calls for.
3. **The CLI command is `create-storage-device`, not `add-storage-device`.** The spec and
   `TODO.txt` both use the older name; the scopt command (`Main.scala:380`) and its handler
   (`def create_storage_device`, `Main.scala:1368`) use `create-storage-device`. This plan uses
   the real name. Its arguments are
   `create-storage-device <bootstrap-config-file> <host-directory> <device-name> <set-name-or-uuid>`.

---

## Task 1: Test harness and characterization test

Establishes the suite, the recording `StoreManager` subclass every later test uses, and a test
pinning the behaviour that already works (the constructor scan). This test is expected to pass
as soon as it compiles — it is the baseline that later tasks must not break.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:254`
- Create: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala`

- [x] **Step 1: Make `tryLoadStore` overridable**

In `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`, change line 254 from:

```scala
  private def tryLoadStore(sds: LocalStorageDeviceState, potentialStoreFile: File): Unit =
```

to:

```scala
  // protected so tests can subclass with a recording implementation instead of opening a
  // real RocksDB backend. `stores` and `storageDevices` are already protected.
  protected def tryLoadStore(sds: LocalStorageDeviceState, potentialStoreFile: File): Unit =
```

- [x] **Step 2: Write the harness and the characterization test**

Create `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala`:

```scala
package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer}

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}

/** A StoreManager that records tryLoadStore calls instead of opening RocksDB backends, and
 *  exposes the protected device map to assertions.
 *
 *  Constructor parameters are deliberately named differently from StoreManager's own members
 *  (`client`, `ec`, `net`, `rootDir`, ...) so the superclass constructor call cannot
 *  accidentally resolve to an inherited, not-yet-initialized member.
 */
class RecordingStoreManager(mgrClient: AspenClient,
                            systemId: UUID,
                            hostRootDir: Path,
                            execCtx: ExecutionContext,
                            cacheFactory: () => ObjectCache,
                            messenger: ServerMessenger,
                            finalizers: TransactionFinalizer.Factory)
  extends StoreManager(
    mgrClient,
    HostId.BootstrapHostId,
    systemId,
    hostRootDir,
    execCtx,
    cacheFactory,
    messenger,
    BackgroundTaskManager.NoBackgroundTaskManager,
    TestNetwork.TestCRL,
    finalizers,
    TransactionDriver.noErrorRecoveryFactory,
    Duration(5, SECONDS),
    Duration(60, SECONDS)):

  /** (deviceId, store directory) for every tryLoadStore call, in call order. */
  val storeLoadAttempts: mutable.ListBuffer[(StorageDeviceId, Path)] =
    mutable.ListBuffer[(StorageDeviceId, Path)]()

  override protected def tryLoadStore(sds: StoreManager.LocalStorageDeviceState,
                                      potentialStoreFile: File): Unit =
    synchronized:
      storeLoadAttempts += ((sds.storageDeviceId, potentialStoreFile.toPath))

  def loadedDevices: Map[StorageDeviceId, StoreManager.LocalStorageDeviceState] =
    synchronized(storageDevices)


class StoreManagerDeviceDiscoverySuite extends IntegrationTestSuite:

  /** Must match the aspenSystemId the RecordingStoreManager is constructed with. */
  private val systemId = UUID.fromString("33333333-3333-3333-3333-333333333333")
  private val foreignSystemId = UUID.fromString("44444444-4444-4444-4444-444444444444")

  private val tempRoots = mutable.ListBuffer[Path]()

  override def subFixtureTeardown(): Unit =
    tempRoots.foreach(deleteTree)
    tempRoots.clear()

  /** Removes a tree bottom-up. Failures are ignored; these are temp dirs. */
  private def deleteTree(root: Path): Unit =
    try
      Files.walkFileTree(root, new SimpleFileVisitor[Path]:
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult =
          Files.deleteIfExists(file)
          FileVisitResult.CONTINUE

        override def visitFileFailed(file: Path, exc: IOException): FileVisitResult =
          FileVisitResult.CONTINUE

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult =
          Files.deleteIfExists(dir)
          FileVisitResult.CONTINUE
      )
    catch
      case _: Throwable => ()

  /** Creates `<tmp>/host/storage-devices` and returns the host root directory. */
  private def newHostDir(): Path =
    val base = Files.createTempDirectory("aspen-device-discovery")
    tempRoots += base
    val hostDir = base.resolve("host")
    Files.createDirectories(hostDir.resolve(StorageDeviceManager.StorageDevicesDirName))
    hostDir

  /** Creates `<hostRoot>/storage-devices/<name>` with a device config naming `deviceId`. */
  private def writeDevice(hostRoot: Path,
                          name: String,
                          deviceId: StorageDeviceId,
                          sysId: UUID = systemId): Path =
    val dir = hostRoot.resolve(StorageDeviceManager.StorageDevicesDirName).resolve(name)
    Files.createDirectories(dir)
    writeDeviceConfig(dir, deviceId, sysId)
    dir

  /** Writes only the config file into an existing device directory. */
  private def writeDeviceConfig(deviceDir: Path,
                                deviceId: StorageDeviceId,
                                sysId: UUID = systemId): Unit =
    Files.write(
      deviceDir.resolve(StorageDeviceConfig.configFilename),
      StorageDeviceConfig(deviceId, sysId).yamlConfig.getBytes(StandardCharsets.UTF_8))

  /** Creates an empty device directory with no config file. */
  private def newDeviceDirWithoutConfig(hostRoot: Path, name: String): Path =
    val dir = hostRoot.resolve(StorageDeviceManager.StorageDevicesDirName).resolve(name)
    Files.createDirectories(dir)
    dir

  private def newManager(hostRoot: Path): RecordingStoreManager =
    new RecordingStoreManager(client, systemId, hostRoot, executionContext,
                              net.objectCacheFactory, net, net.FinalizerFactory)

  private val deviceA = StorageDeviceId(UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001"))
  private val deviceB = StorageDeviceId(UUID.fromString("bbbbbbbb-0000-0000-0000-000000000002"))

  atest("constructor loads a device that already exists on disk"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))
```

- [x] **Step 3: Run the test**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite'`

Expected: PASS, 1 test. This is a characterization test — the constructor scan already works.
If it fails to compile, the harness is wrong; fix that before continuing.

- [x] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Add StoreManager device discovery test harness

Makes tryLoadStore protected so tests can record store loads rather than
opening real RocksDB backends."
```

---

## Task 2: `checkForNewDevices()` and the `CheckAllDevices` handler

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:143-147` (constructor scan), `:235` (new method above `tryLoadDevice`), `:743-749` (testing hooks), `:841-843` (`CheckAllDevices` handler)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala`

- [x] **Step 1: Write the failing test**

Append to `StoreManagerDeviceDiscoverySuite`:

```scala
  atest("a device created after construction is discovered on the next CheckAllDevices"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    mgr.loadedDevices should be(empty)

    writeDevice(hostRoot, "dev0", deviceA)
    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))

  atest("a missing storage-devices directory does not throw"):
    val base = Files.createTempDirectory("aspen-device-discovery")
    tempRoots += base
    val hostRoot = base.resolve("host-with-no-storage-devices-dir")
    Files.createDirectories(hostRoot)

    val mgr = newManager(hostRoot)
    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices should be(empty))
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite'`

Expected: compile error — `value testingOnlyCheckAllDevices is not a member of RecordingStoreManager`.

- [x] **Step 3: Add `checkForNewDevices()`**

In `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`, insert immediately above
`private def tryLoadDevice` (currently line 235):

```scala
  /** Scans storage-devices/ and loads any device not already loaded.
   *
   *  Called at construction and from the event loop on every CheckAllDevices, so it must be
   *  idempotent -- tryLoadDevice skips devices already in storageDevices. Callers hold the
   *  instance lock: handleEvent is synchronized, and the constructor runs before start().
   */
  private def checkForNewDevices(): Unit =
    if ! Files.isDirectory(storageDevicesDir) then
      logger.warn(s"Invalid storage devices directory: $storageDevicesDir")
    else
      // listFiles returns null on an IO error even when isDirectory just succeeded. Left
      // unguarded, the NPE escapes handleEvent and silently kills the event loop thread.
      storageDevicesDir.toFile.listFiles() match
        case null  => logger.warn(s"Failed to list storage devices directory: $storageDevicesDir")
        case files => files.foreach(tryLoadDevice)
```

- [x] **Step 4: Replace the constructor scan**

Replace lines 143-147 of `StoreManager.scala`:

```scala
  if ! Files.isDirectory(storageDevicesDir) then
    logger.warn(s"Invalid storage devices directory: $storageDevicesDir")
  else
    storageDevicesDir.toFile.listFiles().foreach: sdFile =>
      tryLoadDevice(sdFile)
```

with:

```scala
  checkForNewDevices()
```

The two `events.put(...)` lines that follow are unchanged.

- [x] **Step 5: Discover before iterating in the `CheckAllDevices` handler**

Replace lines 841-843 of `StoreManager.scala`:

```scala
      case CheckAllDevices() =>
        storageDevices.valuesIterator.foreach: sds =>
          checkStorageDevice(sds.storageDeviceId)
```

with:

```scala
      case CheckAllDevices() =>
        // Discover first so a device found by this scan is checked within the same event
        // and its Initializing stores are created now rather than a period from now.
        checkForNewDevices()
        storageDevices.valuesIterator.foreach: sds =>
          checkStorageDevice(sds.storageDeviceId)
```

- [x] **Step 6: Add the testing hooks**

In `StoreManager.scala`, immediately after `testingOnlyHandleEvents()` (which currently ends at
line 749), add:

```scala
  /** Testing hook: runs a CheckAllDevices event synchronously.
   *
   *  Direct rather than events.put + testingOnlyHandleEvents so a test does not also drain
   *  the InitializeTaskExecutor event queued by the constructor, which would attempt a real
   *  task-executor bootstrap transaction.
   */
  private[aspen] def testingOnlyCheckAllDevices(): Unit = handleEvent(CheckAllDevices())

  /** Testing hook: runs receiveHostMessage's handler synchronously. See
   *  testingOnlyCheckAllDevices for why this bypasses the event queue. */
  private[aspen] def testingOnlyHandleHostMessage(msg: HostMessage): Unit =
    handleEvent(HostMsg(msg))
```

- [x] **Step 7: Run the tests to verify they pass**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite'`

Expected: PASS, 3 tests.

- [x] **Step 8: Run the full suite for regressions**

Run: `sbt test`

Expected: no new failures. The constructor path now goes through `checkForNewDevices()`;
`TestNetwork` constructs its `StoreManager` with `rootDir = Path.of("/")`, so the
"Invalid storage devices directory" warning is expected and harmless.

- [x] **Step 9: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Add StoreManager.checkForNewDevices and rescan on CheckAllDevices

The constructor scan is now expressed in terms of the same method, so the
device loading logic exists in one place. Guards against listFiles returning
null, which from the event loop would kill the manager thread."
```

---

## Task 3: Make `tryLoadDevice` idempotent

Without this, every rescan replaces the `LocalStorageDeviceState` (losing
`loadedStores`/`offlineStores` tracking) and re-runs `tryLoadStore` on every child, which in
production constructs a second `RocksDBBackend` over a live RocksDB directory.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:243-250`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala`

- [x] **Step 1: Write the failing tests**

Append to `StoreManagerDeviceDiscoverySuite`:

```scala
  atest("an already-loaded device is not reloaded"):
    val hostRoot = newHostDir()
    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    Files.createDirectories(deviceDir.resolve("some-store-dir"))

    val mgr = newManager(hostRoot)
    val firstState = mgr.loadedDevices(deviceA)
    val attemptsAfterConstruction = mgr.storeLoadAttempts.toList

    // tryLoadDevice offers every child of the device directory to tryLoadStore: the store
    // directory and the device config file. The real implementation rejects the latter.
    attemptsAfterConstruction.size should be(2)

    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyCheckAllDevices()

    // Same instance: the device's loadedStores/offlineStores tracking survives a rescan.
    mgr.loadedDevices(deviceA) should be theSameInstanceAs firstState
    Future.successful(mgr.storeLoadAttempts.toList should be(attemptsAfterConstruction))

  atest("a second directory claiming a loaded device id is ignored"):
    val hostRoot = newHostDir()
    val originalDir = writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    val originalState = mgr.loadedDevices(deviceA)
    originalState.devicePath should be(originalDir)

    // A duplicate mount or a copied config file: same device id, different directory.
    writeDevice(hostRoot, "dev0-copy", deviceA)
    mgr.testingOnlyCheckAllDevices()

    mgr.loadedDevices.keySet should be(Set(deviceA))
    Future.successful(mgr.loadedDevices(deviceA) should be theSameInstanceAs originalState)
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite'`

Expected: both new tests FAIL. "an already-loaded device is not reloaded" fails on the
`theSameInstanceAs` assertion (each rescan builds a new `LocalStorageDeviceState`); "a second
directory claiming a loaded device id is ignored" fails the same way, because the copy
overwrites the map entry.

- [x] **Step 3: Add the guard**

In `StoreManager.scala`, replace lines 243-250 (the `else` body of the system-id check inside
`tryLoadDevice`):

```scala
        if sdCfg.aspenSystemId != aspenSystemId then
          logger.warn(s"Storage Device found that does not belong to this Aspen system: $storageDevicePath. Ignoring")
        else
          val sds = new LocalStorageDeviceState(sdCfg.storageDeviceId, storageDevicePath, configFile)
          storageDevices += sdCfg.storageDeviceId -> sds
          logger.info(s"Loading store $sdFile. StorageDeviceId ${sds.storageDeviceId}")
          sdFile.listFiles.foreach: potentialStoreFile =>
            tryLoadStore(sds, potentialStoreFile)
```

with:

```scala
        if sdCfg.aspenSystemId != aspenSystemId then
          logger.warn(s"Storage Device found that does not belong to this Aspen system: $storageDevicePath. Ignoring")
        else
          storageDevices.get(sdCfg.storageDeviceId) match
            case Some(existing) =>
              // Already loaded. A second directory claiming the same device id means a
              // duplicate mount or a copied config file -- loading it would open a second
              // backend on live store directories, so report it and keep the original.
              if existing.devicePath != storageDevicePath then
                logger.warn(s"Storage device ${sdCfg.storageDeviceId} is already loaded from " +
                            s"${existing.devicePath}; ignoring duplicate at $storageDevicePath")
            case None =>
              val sds = new LocalStorageDeviceState(sdCfg.storageDeviceId, storageDevicePath, configFile)
              storageDevices += sdCfg.storageDeviceId -> sds
              logger.info(s"Loading storage device $sdFile. StorageDeviceId ${sds.storageDeviceId}")
              sdFile.listFiles.foreach: potentialStoreFile =>
                tryLoadStore(sds, potentialStoreFile)
```

Note the drive-by wording fix in the `logger.info` line: it logs a *device*, not a store.

- [x] **Step 4: Run the tests to verify they pass**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite'`

Expected: PASS, 5 tests.

- [x] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Make StoreManager.tryLoadDevice idempotent

Rescanning must not replace a loaded device's LocalStorageDeviceState or
re-run tryLoadStore over live store directories. A second directory claiming
a loaded device id is reported and ignored."
```

---

## Task 4: Regression tests for the skip paths

No production change. These pin behaviour that must survive the move from a one-shot
constructor scan to a repeated one.

**Files:**
- Test: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala`

- [x] **Step 1: Write the tests**

Append to `StoreManagerDeviceDiscoverySuite`:

```scala
  atest("a device directory with no config is skipped, then loaded once the config appears"):
    val hostRoot = newHostDir()
    val deviceDir = newDeviceDirWithoutConfig(hostRoot, "dev0")

    val mgr = newManager(hostRoot)
    mgr.loadedDevices should be(empty)

    // createStorageDevice writes the config only after its transaction commits, so a
    // provisioned-but-unregistered directory is a normal transient state.
    writeDeviceConfig(deviceDir, deviceA)
    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))

  atest("a device belonging to another Aspen system is ignored"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA, foreignSystemId)
    writeDevice(hostRoot, "dev1", deviceB)

    val mgr = newManager(hostRoot)

    mgr.loadedDevices.keySet should be(Set(deviceB))

    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceB)))

  atest("an unparseable device config is skipped and retried on the next scan"):
    val hostRoot = newHostDir()
    val deviceDir = newDeviceDirWithoutConfig(hostRoot, "dev0")
    Files.write(deviceDir.resolve(StorageDeviceConfig.configFilename),
                "this is not: valid: device config yaml".getBytes(StandardCharsets.UTF_8))

    val mgr = newManager(hostRoot)
    mgr.loadedDevices should be(empty)

    Files.delete(deviceDir.resolve(StorageDeviceConfig.configFilename))
    writeDeviceConfig(deviceDir, deviceA)
    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))
```

- [x] **Step 2: Run the tests to verify they pass**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite'`

Expected: PASS, 8 tests.

If "an unparseable device config is skipped" fails because SnakeYAML happens to accept that
string, replace the bad-config bytes with something that parses as YAML but lacks the required
keys — `"unrelated-key: 1\n".getBytes(StandardCharsets.UTF_8)` — which `StorageDeviceConfig`'s
`Required` attrs reject.

- [x] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Add regression tests for storage device discovery skip paths"
```

---

## Task 5: `CheckStorageDevice` triggers discovery

This is the push path. It also closes a latent gap: today a `CheckStorageDevice` naming a
device the host has not loaded falls into `checkStorageDevice`'s `None` branch and needlessly
marks that device's stores offline.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:804`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala`

- [x] **Step 1: Write the failing test**

Append to `StoreManagerDeviceDiscoverySuite` (and add
`import org.aspen_ddp.aspen.common.network.CheckStorageDevice` to the file's imports):

```scala
  atest("CheckStorageDevice for an unknown but on-disk device triggers discovery"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    mgr.loadedDevices should be(empty)

    writeDevice(hostRoot, "dev0", deviceA)
    mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    Future.successful(mgr.loadedDevices.keySet should be(Set(deviceA)))
```

- [x] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite -- -z "triggers discovery"'`

Expected: FAIL — `Set() was not equal to Set(StorageDeviceId(aaaaaaaa-...))`. The handler calls
`checkStorageDevice` directly, which takes the `None` branch and never rescans.

- [x] **Step 3: Rescan when the named device is unknown**

In `StoreManager.scala`, replace line 804:

```scala
        case m: CheckStorageDevice => checkStorageDevice(m.deviceId)
```

with:

```scala
        case m: CheckStorageDevice =>
          // create-storage-device sends this after registering a device, so a name we do
          // not recognise may simply be one we have not scanned for yet.
          if ! storageDevices.contains(m.deviceId) then
            checkForNewDevices()
          checkStorageDevice(m.deviceId)
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite'`

Expected: PASS, 9 tests.

- [x] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Rescan for new devices when CheckStorageDevice names an unknown device

Lets create-storage-device use the existing CheckStorageDevice message as a
discovery nudge, and stops an unknown device id from needlessly marking its
stores offline before we have even looked for it."
```

---

## Task 6: Stores on a newly discovered device are loaded

**Files:**
- Test: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala`

No production change is expected — `tryLoadDevice` already iterates the device directory's
children. This test pins that discovery reaches store loading, which is the whole point of the
feature.

- [x] **Step 1: Write the test**

Append to `StoreManagerDeviceDiscoverySuite`:

```scala
  atest("stores on a newly discovered device are loaded"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    mgr.storeLoadAttempts should be(empty)

    val deviceDir = writeDevice(hostRoot, "dev0", deviceA)
    val storeDir = deviceDir.resolve("11111111-1111-1111-1111-111111111111:0")
    Files.createDirectories(storeDir)

    mgr.testingOnlyCheckAllDevices()

    mgr.loadedDevices.keySet should be(Set(deviceA))
    // The config file is a child of the device directory too, so it is offered to
    // tryLoadStore as well; the real implementation rejects it for having no store config.
    Future.successful(mgr.storeLoadAttempts.toList should contain((deviceA, storeDir)))
```

- [x] **Step 2: Run the test to verify it passes**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite'`

Expected: PASS, 10 tests.

- [x] **Step 3: Run the full suite**

Run: `sbt test`

Expected: no new failures.

- [x] **Step 4: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Assert stores on a newly discovered device reach tryLoadStore"
```

---

## Task 7: `MetadataManager.peekHostEntry`

`ZMQNet.awaitHostMessagesSent` needs to ask "is this host resolved yet?" in a loop.
`getHostEntry` cannot be used: it calls `startHostLookup` when the entry is absent, so a
polling caller would kick off repeated lookups.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala:100`

The plan originally called for no test here, on the grounds that there is no
`MetadataManagerSuite` and constructing one requires a bootstrap config file plus a
`NetworkImplInterface`. That turned out to be affordable, and the "starts no lookup" property is
the whole reason the method exists, so execution added
`src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPeekHostEntrySuite.scala`
(5 tests) rather than leaving it to the manual verification in Task 10.

- [x] **Step 1: Add the method**

In `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`, insert
immediately above `def getHostEntry` (currently line 100):

```scala
  /** Returns the host entry only if it is already resolved. Unlike getHostEntry, this never
   *  starts a host lookup, so it is safe to call from a polling loop. */
  def peekHostEntry(hostId: HostId): Option[T] =
    synchronized:
      hosts.get(hostId) match
        case Some(Right(hostEntry)) => Some(hostEntry)
        case _ => None

```

- [x] **Step 2: Verify it compiles**

Run: `sbt compile`

Expected: success.

- [x] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala
git commit -m "Add MetadataManager.peekHostEntry

Side-effect-free host entry lookup for polling callers. getHostEntry starts a
host lookup on a miss, which a poll loop must not do repeatedly."
```

---

## Task 8: `ZMQNet.awaitHostMessagesSent` and `shutdown`

`sendHostMessage` only appends to a `ConcurrentLinkedQueue` and pokes an inproc socket; the
`zmq-io` daemon thread does the real work, and may first need an async host lookup and a TCP
connect. Since commit `31c5f39` the CLI exits promptly, so a fire-and-forget send immediately
before exit is very likely dropped — and the fallback is a one-hour poll.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala:97` (new field), `:324-328` (record the dealer), end of class (new methods)

No automated test: the only suite under `zmqnet/` is `ProtobufMessageCodecSuite`, and
exercising the drain requires binding real sockets. Verified manually in Task 10.

- [x] **Step 1: Track connected dealer sockets**

In `ZMQNet.scala`, immediately after the `sendQueue` declaration (line 97):

```scala
  private val sendQueue = new ConcurrentLinkedQueue[SendQueueMsg]()
```

add:

```scala
  // Dealers created by the IO thread, recorded so shutdown can set their linger. Only ever
  // appended to; a ZMQNet's host set is small and lives for the process lifetime.
  private val connectedDealers = new ConcurrentLinkedQueue[ZMQ.Socket]()
```

- [x] **Step 2: Record each dealer as it is created**

In the `NewHostAvailable` case of `ioThread` (lines 323-328), after
`entry.opollItem = Some(new PollItem(dealer, ZMQ.Poller.POLLIN))`, add
`connectedDealers.add(dealer)` so the block reads:

```scala
          case NewHostAvailable(entry) =>
            val dealer = context.createSocket(SocketType.DEALER)
            dealer.setIdentity(clientId.toBytes)
            dealer.connect(s"tcp://${entry.address}:${entry.dataPort}")
            entry.odealer = Some(dealer)
            entry.opollItem = Some(new PollItem(dealer, ZMQ.Poller.POLLIN))
            connectedDealers.add(dealer)
```

The rest of the `NewHostAvailable` block (initial heartbeat, pending drain,
`connectedHosts += entry`, `rebuildPoller()`) is unchanged.

- [x] **Step 3: Add the drain and shutdown methods**

In `ZMQNet.scala`, immediately after `private def wakeIoThread()` (line 169-170), add:

```scala
  /** Blocks until messages queued for `hostId` have been handed to their ZMQ socket, or
   *  `timeout` elapses. Returns true if the queues drained.
   *
   *  This is not a delivery guarantee -- ZMQ buffers internally and the peer may be down. It
   *  establishes only that nothing is left waiting inside ZMQNet, which is exactly what a
   *  short-lived process would otherwise abandon on exit.
   */
  def awaitHostMessagesSent(hostId: HostId, timeout: Duration): Boolean =
    val deadline = System.nanoTime() + timeout.toNanos
    val pollInterval = 25L

    def drained: Boolean =
      sendQueue.isEmpty && (metadataManager.peekHostEntry(hostId) match
        case Some(entry) => entry.odealer.isDefined && entry.pendingMessages.isEmpty
        case None => false)

    while ! drained && System.nanoTime() < deadline do
      Thread.sleep(pollInterval)

    drained

  /** Closes the ZMQ context, allowing buffered outbound data up to `linger` to flush.
   *
   *  Intended for short-lived processes on their way out; a long-running host keeps its
   *  network up for the life of the process. setLinger runs on the caller's thread rather
   *  than the IO thread, which is acceptable only because the process is terminating.
   */
  def shutdown(linger: Duration): Unit =
    val lingerMillis = linger.toMillis.toInt
    connectedDealers.forEach: dealer =>
      try
        dealer.setLinger(lingerMillis)
      catch
        case t: Throwable => logger.debug(s"Failed to set linger during shutdown: $t")
    context.close()
```

- [x] **Step 4: Verify it compiles**

Run: `sbt compile`

Expected: success. `HostId` and `Duration` are already imported in this file
(`org.aspen_ddp.aspen.common.metadata.HostId`, `scala.concurrent.duration.Duration`).

- [x] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala
git commit -m "Add ZMQNet.awaitHostMessagesSent and shutdown

sendHostMessage only enqueues; the IO thread does the real send. A CLI command
that exits immediately afterwards drops the message. These give short-lived
processes a bounded flush window before exit."
```

---

## Task 9: `create-storage-device` notifies the host

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:1446-1454` (the success branch of
  `def create_storage_device`, which backs the `create-storage-device` CLI command)

No automated test: the command builds a real `ZMQNet` and an `AspenClient` against a bootstrap
config file. Verified manually in Task 10.

- [x] **Step 1: Replace the success branch**

In `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, replace lines 1446-1454:

```scala
      awaitAndReport(f):
        case Success(deviceId) =>
          println(s"Created storage device ${deviceId.uuid} at $deviceDirectory")
          // StoreManager scans storage-devices/ only in its constructor; the periodic
          // CheckAllDevices event iterates already-loaded devices and never rescans. A
          // running host therefore ignores the new device, and any pool created on it
          // before the restart has its stores marked offline rather than instantiated.
          println(s"Restart host '${hostCfg.name}' to bring the device online -- a running host does not detect new storage devices.")
        case Failure(err) => reportError(err)
```

with:

```scala
      awaitAndReport(f):
        case Success(deviceId) =>
          println(s"Created storage device ${deviceId.uuid} at $deviceDirectory")
          // Best-effort nudge so the host loads the device now rather than on its next
          // periodic storage-device check. StoreManager rescans storage-devices/ when a
          // CheckStorageDevice names a device it has not loaded, so no new message type is
          // needed. Losing this costs at most one check period; it is never a requirement.
          // The drain matters because sendHostMessage only enqueues -- see ZMQNet.
          client.sendHostMessage(CheckStorageDevice(hostCfg.hostId, client.clientId, deviceId))
          val flushed = network.awaitHostMessagesSent(hostCfg.hostId, Duration(5, SECONDS))
          network.shutdown(Duration(1, SECONDS))
          if flushed then
            println(s"Sent a device-check notification to host '${hostCfg.name}'. If it does not " +
                    "arrive, the host will load the device on its next periodic storage-device check.")
          else
            println(s"Could not reach host '${hostCfg.name}'. It will load the device on its next " +
                    "periodic storage-device check.")
        case Failure(err) => reportError(err)
```

`CheckStorageDevice` is already in scope via `import org.aspen_ddp.aspen.common.network.*`
(Main.scala:17); `Duration` and `SECONDS` via Main.scala:50.

- [x] **Step 2: Verify it compiles**

Run: `sbt compile`

Expected: success.

- [x] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "create-storage-device notifies the owning host

Sends a CheckStorageDevice message so a running host picks up the new device
within seconds instead of after the one-hour poll, then drains ZMQNet's send
path so the short-lived CLI process does not abandon it on exit."
```

---

## Task 9b: Event-loop hardening

**Added during the Task 2 code review, not present in the original spec.** Two pre-existing
defects that this feature makes load-bearing. Neither is caused by the discovery work; both are
reached by it.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` — `checkStorageDevice`'s
  future completion, and `start()`'s event loop

**Defect 1: `checkStorageDevice` leaks `activeDeviceChecks` on failure.**
`client.getStorageDeviceState` returns a *failed* future for a device that has no registered
`StorageDeviceState` (`MetadataTree` throws `NoSuchElementException` on a missing key).
`checkStorageDevice` consumes that future with `.foreach`, so on failure
`activeDeviceChecks -= storageDeviceId` never runs and the id is stuck for the lifetime of the
process — every subsequent check of that device is silently a no-op.

Before this feature, every entry in `storageDevices` was present from construction, so this was
mostly a startup-only edge. Now a device directory can be discovered *before* its
`StorageDeviceState` is registered — exactly the race this feature targets — and both the
`CheckAllDevices` and `CheckStorageDevice` handlers call `checkStorageDevice` on it immediately.
The intended benefit ("stores are created now rather than a period from now") inverts to *never*.

Fix: complete the future with `onComplete` (or `andThen`) so the `activeDeviceChecks -=` cleanup
runs on the failure path too. Log the failure rather than swallowing it.

**Defect 2: an exception escaping `handleEvent` silently kills the event loop.**
`start()`'s loop has no `try`/`catch` around `handleEvent(event)`, and the `Future` returned by
`threadPool.submit` is discarded — so any escape terminates the storage host's event loop with no
log line at all. The host then appears alive while processing nothing.

Fix: wrap the `handleEvent(event)` call in a `try`/`catch` that logs the throwable and continues
the loop. This is the durable version of the narrower `listFiles`-null guard added in Task 2.

- [x] **Step 1: Write the failing test for defect 1**

Append to `StoreManagerDeviceDiscoverySuite`:

```scala
  atest("a device check that fails does not wedge later checks of the same device"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    mgr.loadedDevices.keySet should be(Set(deviceA))

    // deviceA has no StorageDeviceState registered in the TestNetwork, so getStorageDeviceState
    // fails. The failure must still release the activeDeviceChecks entry.
    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyCheckAllDevices()

    Future.successful(mgr.testingOnlyActiveDeviceChecks should be(empty))
```

This needs a `private[aspen] def testingOnlyActiveDeviceChecks: Set[StorageDeviceId]` accessor on
`StoreManager` exposing the `activeDeviceChecks` set, placed with the other testing hooks.

The future completes asynchronously, so the accessor may need to be polled with a short bounded
wait rather than read once. Use the same bounded-poll idiom as elsewhere in the test suite; if no
such idiom exists, a loop with a deadline and a 25 ms sleep is acceptable in a test.

- [x] **Step 2: Run it to verify it fails**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite -- -z "wedge"'`

Expected: FAIL — the set still contains `deviceA` because the failed future never ran the cleanup.

- [x] **Step 3: Fix defect 1**

In `checkStorageDevice`, change the `.foreach` that consumes the `getStorageDeviceState` future so
that the `activeDeviceChecks -= storageDeviceId` cleanup runs on both success and failure, and the
failure is logged. Keep the success path's behaviour exactly as it is.

- [x] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly org.aspen_ddp.aspen.server.StoreManagerDeviceDiscoverySuite'`

Expected: PASS.

- [x] **Step 5: Fix defect 2**

In `start()`, wrap the `handleEvent(event)` call so a throwable is logged at error and the loop
continues:

```scala
        try
          handleEvent(event)
        catch
          case t: Throwable =>
            // Without this, any escape from handleEvent terminates the event loop and the host
            // goes silently deaf while still appearing to run.
            logger.error(s"Unhandled exception processing event $event: $t", t)
```

Match the surrounding code's exact loop structure and variable names; the snippet above shows the
intent, not necessarily the literal surrounding lines.

- [x] **Step 6: Run the full suite**

Run: `sbt test`

Expected: no new failures.

- [x] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Harden the StoreManager event loop

checkStorageDevice leaked its activeDeviceChecks entry when the device state
lookup failed, permanently wedging later checks of that device -- reachable now
that a device directory can be discovered before its state is registered. An
exception escaping handleEvent also killed the event loop with no log line."
```

---

## Task 10: Manual verification and TODO cleanup

**Files:**
- Modify: `TODO.txt:22-24`, `TODO.txt:45-47`

- [x] **Step 1: Run the full test suite**

Run: `sbt test`

Expected: no failures.

- [x] **Step 2: Verify end-to-end against a running host**

`bootstrap` creates `<target>/bootstrap-host/` holding `aspen-host-config.yaml`,
`aspen-bootstrap-config.yaml`, and `storage-devices/bootstrap-device/`
(`Main.scala:988-1070`). The bootstrap device set is named `bootstrap`
(`Bootstrap.scala:136-138`).

In terminal 1:

```bash
rm -rf /tmp/aspen-test
./t bootstrap /tmp/aspen-test replication 2 3 3
./t host /tmp/aspen-test/bootstrap-host
```

In terminal 2, with the host still running:

```bash
mkdir -p /tmp/aspen-test/bootstrap-host/storage-devices/dev1
./t create-storage-device /tmp/aspen-test/bootstrap-host/aspen-bootstrap-config.yaml \
   /tmp/aspen-test/bootstrap-host dev1 bootstrap
```

Expected (the exact notification wording quoted in this step and the next was revised during
and after execution to stop over-claiming delivery; `Main.scala` is authoritative):
- The CLI prints `Created storage device <uuid> at /tmp/aspen-test/bootstrap-host/storage-devices/dev1`
  followed by `Sent a device-check notification to host 'bootstrap-host'. ...`
- Within a few seconds — not an hour — terminal 1 logs
  `Loading storage device /tmp/aspen-test/bootstrap-host/storage-devices/dev1. StorageDeviceId <uuid>`
- The CLI process exits rather than hanging.

Confirm the device is registered:

```bash
./t list-devices /tmp/aspen-test/bootstrap-host/aspen-bootstrap-config.yaml bootstrap-host
```

- [x] **Step 3: Verify the polling fallback and the constructor path**

Stop the host in terminal 1 (Ctrl-C). Then, with the host down:

```bash
mkdir -p /tmp/aspen-test/bootstrap-host/storage-devices/dev2
./t create-storage-device /tmp/aspen-test/bootstrap-host/aspen-bootstrap-config.yaml \
   /tmp/aspen-test/bootstrap-host dev2 bootstrap
```

Expected: the CLI prints `Could not reach host 'bootstrap-host'. It will load the device on its
next periodic storage-device check.` and exits — it must not hang for longer than the 5-second
drain window.

Restart the host:

```bash
./t host /tmp/aspen-test/bootstrap-host
```

Expected: the constructor scan logs `Loading storage device ...` for `bootstrap-device`, `dev1`,
and `dev2`, and each is logged exactly once — the idempotency guard must not produce a duplicate
load when the initial `CheckAllDevices` event runs immediately after construction.

If a device is added while the host is up and the notification is lost, the device is loaded on
the next `CheckAllDevices`, up to one hour later. That is the intended fallback and is not
practical to sit through here; step 2 covers the notification path and this step covers the scan
itself.

- [x] **Step 4: Remove the completed TODO items**

In `TODO.txt`, delete lines 22-24:

```
Add a CnC message to instruct a host to call checkStorageDevices
  - Send this message when a new device is created so we don't have to wait
    for a polling period to elapse before the host notices
```

and lines 45-47:

```
Update add-storage-device to send a new NewDeviceAdded HostMessage
  - Send to host after new device successfully created
  - Host immediately invokes checkForNewDevices() upon reception
```

Remove the blank line each leaves behind so the file keeps its one-blank-line-between-items
shape.

- [x] **Step 5: Commit**

```bash
git add TODO.txt
git commit -m "Remove TODO items completed by runtime storage device discovery"
```

---

## Verification checklist

- [x] `sbt test` passes with 18 new tests across two new suites. 13 in
      `StoreManagerDeviceDiscoverySuite` (10 from the original plan, the one added by Task 9b,
      and two more added during execution: a partially-failed device load being retried, and an
      unusable entry under `storage-devices/` not stopping its siblings), plus 5 in
      `MetadataManagerPeekHostEntrySuite`, which the original plan did not call for (see Task 8)
- [x] Device loading logic exists in exactly one place (`tryLoadDevice`, reached only through
      `checkForNewDevices`); `grep -n "listFiles" src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`
      shows the directory scan only inside `checkForNewDevices` and the per-device child scan
      inside `tryLoadDevice`
- [x] `codec.proto`, `Codec.scala`, and `Message.scala` are untouched — the whole point of
      reusing `CheckStorageDevice`. Against the pre-work commit `e2b6ebd`:
      ```bash
      git diff --stat e2b6ebd -- src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala
      ```
      must print nothing
- [x] A device added under a running host comes online in seconds (Task 10, step 2)
- [x] The `create-storage-device` CLI still exits rather than hanging, both when the host is up
      and when it is down (Task 10, steps 2 and 3)
