# Store Rebuild — Consumer Half Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A store marked `Rebuilding` on a device is reconstructed from the rest of its pool, crash-resumably, and flipped to `Active` when it is whole.

**Architecture:** A new `org.aspen_ddp.aspen.server.rebuild` package holding `RebuildState` (the `rebuild-state.yaml` checkpoint model), `StoreRebuild`/`StoreRebuildFactory` (a test seam mirroring `StoreTransferFactory`), and `RebuildingStore` (the real implementation). `StoreManager` grows a fourth clause in `reconcileDeviceState` that starts a rebuild for each `Rebuilding` entry, bounded by `maxConcurrentRebuilds` and guarded by a `rebuildingStores` map exactly as `creatingStores` guards `createNewStore`. The walk stages into `<device>/rebuilding/<storeId.directoryName>/`, which `tryLoadStore` cannot see, and moves into place on completion — the same shape as `transferring-in/`.

**Tech Stack:** Scala 3, sbt, ScalaTest, RocksDB backend, os-lib for filesystem work, `YamlFormat` for the checkpoint file.

**Spec:** `docs/superpowers/specs/2026-09-01-failed-store-rebuild-design.md` (sections 4, 5, 6; "Error Handling"; the "Consumer", "`StoreRebuild` itself", and "End-to-end" testing sections)

## Global Constraints

- Scala 3 "quiet mode" syntax. Braces only where they clearly enhance readability.
- Indented `if/then/else`. Avoid single-line `if a == 1 then value` unless it clearly reads better.
- os-lib for all filesystem operations.
- `cmdline` is a work-in-progress; its interfaces are unstable and may be changed freely.
- Run one test by substring: `sbt 'testOnly *SuiteName -- -z "test substring"'`.
- Every commit message ends with the trailer `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- Checkpoint ordering is `rebuildFlush()` **then** write `rebuild-state.yaml`, never the reverse:
  a crash between the two must lose work, not claim work that never reached stable storage.

## Prerequisites

This plan depends on **both** of the other two:

- `docs/superpowers/plans/2026-09-01-tkvl-foreach-fixes.md` — the walk uses
  `TieredKeyValueList.foreach` over more than one tier-0 node, which is broken until that plan
  lands, and `foreachFrom`, which does not exist until then.
- `docs/superpowers/plans/2026-09-01-failed-storage-device-producer.md` — nothing here runs until
  something writes a `Rebuilding` entry.

Land both before starting Task 1.

---

### Task 1: `RebuildState` — the checkpoint file

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/rebuild/RebuildState.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/rebuild/RebuildStateSuite.scala`

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```scala
  case class RebuildState(storeId: StoreId,
                          lastRestoredKey: Option[Key],
                          failedObjects: List[ObjectId]):
    def yamlConfig: String

  object RebuildState extends YObject[RebuildState]:
    val stateFilename: String                      // "rebuild-state.yaml"
    val MaxFailedObjects: Int                      // 10000
    def loadRebuildState(file: File): RebuildState
    def save(path: os.Path, state: RebuildState): Unit
    def load(path: os.Path): Option[RebuildState]
  ```
  Task 3 consumes all of it.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/server/rebuild/RebuildStateSuite.scala`:

```scala
package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class RebuildStateSuite extends AnyFunSuite with Matchers:

  private val storeId = StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")),
                                3.toByte)

  private def objectId(u: String): ObjectId = ObjectId(UUID.fromString(u))

  private def tmpDir(): os.Path =
    os.temp.dir(prefix = "rebuild-state-suite")

  test("a state with no progress round-trips"):
    val dir = tmpDir()
    val state = RebuildState(storeId, None, Nil)
    RebuildState.save(dir, state)
    RebuildState.load(dir) should be(Some(state))

  test("a state with a checkpoint and failures round-trips"):
    val dir = tmpDir()
    // An arbitrary non-UTF8 byte sequence: allocation-tree keys are raw ObjectId bytes, so the
    // encoding has to survive bytes that are not printable characters.
    val key = Key(Array[Byte](0, 1, -1, 127, -128, 42))
    val state = RebuildState(storeId, Some(key),
      List(objectId("22222222-2222-2222-2222-222222222222"),
           objectId("33333333-3333-3333-3333-333333333333")))
    RebuildState.save(dir, state)
    val loaded = RebuildState.load(dir).get
    loaded.storeId should be(storeId)
    loaded.lastRestoredKey.map(_.bytes.toList) should be(Some(key.bytes.toList))
    loaded.failedObjects should be(state.failedObjects)

  test("load returns None when no checkpoint exists"):
    RebuildState.load(tmpDir()) should be(None)

  test("save overwrites an existing checkpoint"):
    val dir = tmpDir()
    RebuildState.save(dir, RebuildState(storeId, Some(Key(Array[Byte](1))), Nil))
    RebuildState.save(dir, RebuildState(storeId, Some(Key(Array[Byte](2))), Nil))
    RebuildState.load(dir).get.lastRestoredKey.map(_.bytes.toList) should be(Some(List[Byte](2)))

  test("the failed-object cap is 10000"):
    RebuildState.MaxFailedObjects should be(10000)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *RebuildStateSuite'`
Expected: compilation failure — `not found: value RebuildState`.

- [ ] **Step 3: Write `RebuildState`**

Create `src/main/scala/org/aspen_ddp/aspen/server/rebuild/RebuildState.scala`:

```scala
package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.common.objects.{Key, ObjectId}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.*

import java.io.File
import java.util.{HexFormat, UUID}

/** The `rebuild-state.yaml` checkpoint, written into a rebuild's staging directory.
 *
 *  Its presence is what makes a restart a resume, so it is deleted only on successful
 *  completion -- never on startup.
 *
 *  `lastRestoredKey` is an allocation-tree key (the raw bytes of an ObjectId), hex-encoded
 *  because those bytes are not text. `failedObjects` are objects the walk could not read; the
 *  checkpoint advances past them so one unreadable object cannot wedge the walk forever, and
 *  they are retried at the end of the pass.
 *
 *  ```
 *  store-id: 00000000-0000-0000-0000-000000000000:0
 *  last-restored-key: 0a1b2c
 *  failed-objects:
 *    - 22222222-2222-2222-2222-222222222222
 *  ```
 */
case class RebuildState(storeId: StoreId,
                        lastRestoredKey: Option[Key],
                        failedObjects: List[ObjectId]):

  def yamlConfig: String =
    val hex = HexFormat.of()
    val keyLine = lastRestoredKey match
      case Some(k) => s"last-restored-key: ${hex.formatHex(k.bytes)}\n"
      case None => ""
    val failedLines =
      if failedObjects.isEmpty then
        "failed-objects: []\n"
      else
        "failed-objects:\n" + failedObjects.map(o => s"  - ${o.uuid}\n").mkString

    s"store-id: $storeId\n" + keyLine + failedLines


object RebuildState extends YObject[RebuildState]:

  val stateFilename = "rebuild-state.yaml"

  /** Beyond this many unreadable objects something systemic is wrong -- the pool below its read
   *  threshold, most likely -- and continuing only burns I/O. The pass aborts with the
   *  checkpoint intact and the next device check retries. */
  val MaxFailedObjects: Int = 10000

  private val hex = HexFormat.of()

  val storeId: Required[StoreId] = Required("store-id", StoreId.YStoreId)
  val lastRestoredKey: Optional[String] = Optional("last-restored-key", YString)
  val failedObjects: Required[List[UUID]] = Required("failed-objects", YList(YUUID))

  val attrs: List[Attr] = storeId :: lastRestoredKey :: failedObjects :: Nil

  def create(o: Object): RebuildState =
    RebuildState(
      storeId.get(o),
      lastRestoredKey.get(o).map(s => Key(hex.parseHex(s))),
      failedObjects.get(o).map(ObjectId.apply))

  def loadRebuildState(file: File): RebuildState =
    create(loadYamlFile(file))

  /** Write the checkpoint into `path`, replacing any existing one. */
  def save(path: os.Path, state: RebuildState): Unit =
    os.write.over(path / stateFilename, state.yamlConfig)

  /** Read the checkpoint from `path`, or None if there is none. */
  def load(path: os.Path): Option[RebuildState] =
    val f = path / stateFilename
    if os.exists(f) then
      Some(loadRebuildState(f.toIO))
    else
      None
```

`YList(YUUID)` on an empty YAML list parses to `Nil`; the `failed-objects: []` branch above is
what keeps the attribute `Required` rather than `Optional`, which in turn keeps a truncated file
from silently reading as "no failures".

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *RebuildStateSuite'`
Expected: PASS, 5 tests.

If `ObjectId.apply(uuid: UUID)` does not exist, use whatever constructor `ObjectId` offers (check
`src/main/scala/org/aspen_ddp/aspen/common/objects/ObjectId.scala`) and adjust both the model and
the test's `objectId` helper to match. Likewise confirm `StoreId.YStoreId` is the YAML format used
by `StoreConfig` — it is, at `StoreConfig.scala:38`.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/rebuild/RebuildState.scala \
        src/test/scala/org/aspen_ddp/aspen/server/rebuild/RebuildStateSuite.scala
git commit -m "$(cat <<'EOF'
feat: add the RebuildState checkpoint model

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: The `StoreRebuild` seam and the `StoreManager` trigger

The trait, the factory, and the whole `StoreManager` side — the `Rebuilding` clause, the
double-start guard, the concurrency bound, and the flip-to-`Active` transaction — land here
against a recording factory. The real walk arrives in Task 3.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/rebuild/StoreRebuild.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` (constructor params,
  `rebuildingStores`, `startStoreRebuild`, the fourth `reconcileDeviceState` clause)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/StoreRebuildTriggerSuite.scala` (create)

**Interfaces:**
- Consumes: `StorageDeviceState.StoreStatus.Rebuilding` (already in the enum).
- Produces:
  ```scala
  trait StoreRebuild:
    val storeId: StoreId
    /** Resolves once the rebuilt store is in place at <device>/<storeId.directoryName>. */
    def complete: Future[Unit]

  trait StoreRebuildFactory:
    def createRebuild(client: AspenClient,
                      storeId: StoreId,
                      storageDeviceId: StorageDeviceId,
                      devicePath: Path): StoreRebuild

  object StoreRebuildFactory:
    object Filesystem extends StoreRebuildFactory
  ```
  and two new `StoreManager` constructor parameters, both defaulted and both last so existing
  call sites (including `RecordingStoreManager`) compile unchanged:
  ```scala
  val storeRebuildFactory: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
  val maxConcurrentRebuilds: Int = 2
  ```
  Task 3 implements `StoreRebuildFactory.Filesystem`.

- [ ] **Step 1: Write the failing tests**

Create `src/test/scala/org/aspen_ddp/aspen/server/StoreRebuildTriggerSuite.scala`. It reuses the
device-discovery suite's harness shape — a `StoreManager` subclass with a recording
`tryLoadStore` and a recording `lookupStorageDeviceState` — plus a recording rebuild factory:

```scala
package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.rebuild.{StoreRebuild, StoreRebuildFactory}

import java.nio.file.Path
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/** A StoreRebuild that does nothing until the test resolves it. */
private class RecordedRebuild(val storeId: StoreId,
                              val storageDeviceId: StorageDeviceId) extends StoreRebuild:
  val promise: Promise[Unit] = Promise()
  def complete: Future[Unit] = promise.future

private class RecordingRebuildFactory extends StoreRebuildFactory:
  val created: mutable.ListBuffer[RecordedRebuild] = mutable.ListBuffer()

  def createRebuild(client: AspenClient,
                    storeId: StoreId,
                    storageDeviceId: StorageDeviceId,
                    devicePath: Path): StoreRebuild =
    val r = new RecordedRebuild(storeId, storageDeviceId)
    synchronized:
      created += r
    r
```

Write the tests using the same `newHostDir()` / `writeDevice()` / `armLookup()` /
`injectLoadedDevice` / `yieldUntil` harness that `StoreManagerDeviceDiscoverySuite` uses. Rather
than duplicating that harness, **extract it**: move `RecordingStoreManager`, `newHostDir`,
`writeDevice`, `newManager`, `deviceState`, `yieldUntil`, `deviceA`, `deviceB`, `storeId` and
`deviceSetId` out of `StoreManagerDeviceDiscoverySuite` into a new
`src/test/scala/org/aspen_ddp/aspen/server/StoreManagerTestHarness.scala` that both suites mix in,
and add a `storeRebuildFactory` / `maxConcurrentRebuilds` parameter pair to `newManager` and to
`RecordingStoreManager`'s constructor (defaulted, so the discovery suite's calls are unchanged).
`StoreManagerDeviceDiscoverySuite` keeps only its tests.

Then the tests:

```scala
class StoreRebuildTriggerSuite extends IntegrationTestSuite with StoreManagerTestHarness:

  private def rebuilding(storeId: StoreId): (StoreId, StorageDeviceState.StoreEntry) =
    storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Rebuilding, None)

  private def storeN(n: Int): StoreId =
    StoreId(PoolId(UUID.fromString("11111111-1111-1111-1111-111111111111")), n.toByte)

  atest("a Rebuilding entry starts a rebuild"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory)

    val armed = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    armed.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    yieldUntil(factory.created.nonEmpty).map: _ =>
      factory.created.map(_.storeId).toList should be(List(storeN(0)))
      factory.created.head.storageDeviceId should be(deviceA)

  atest("a rebuild already running is not started twice"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory)

    val first = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    first.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      // A second check of the same device sees the same Rebuilding entry.
      second = mgr.armLookup(deviceA)
      _ = mgr.testingOnlyCheckAllDevices()
      _ = second.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))
      _ <- yieldUntil(!mgr.testingOnlyActiveDeviceChecks.contains(deviceA))
    yield
      factory.created.size should be(1)

  atest("maxConcurrentRebuilds bounds how many start at once"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 2)

    val armed = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    armed.success(deviceState(deviceA,
      Map(rebuilding(storeN(0)), rebuilding(storeN(1)), rebuilding(storeN(2)))))

    for
      _ <- yieldUntil(factory.created.size == 2)
      _ <- yieldUntil(!mgr.testingOnlyActiveDeviceChecks.contains(deviceA))
    yield
      // Three entries, a bound of two: the third waits. A rebuild reads a whole store's worth
      // of data through the client, so an eight-store device must not start eight at once.
      factory.created.size should be(2)

  atest("a completed rebuild releases its slot and the next one starts"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    val first = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    val twoRebuilding = deviceState(deviceA, Map(rebuilding(storeN(0)), rebuilding(storeN(1))))
    first.success(twoRebuilding)

    for
      _ <- yieldUntil(factory.created.size == 1)
      // Completion re-checks the device, which starts the next queued rebuild.
      second = mgr.armLookup(deviceA)
      _ = factory.created.head.promise.success(())
      _ = second.success(twoRebuilding)
      _ <- yieldUntil(factory.created.size == 2)
    yield
      factory.created.map(_.storeId).toList.distinct.size should be(2)

  atest("a failed rebuild releases its slot"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)
    val factory = new RecordingRebuildFactory
    val mgr = newManager(hostRoot, storeRebuildFactory = factory, maxConcurrentRebuilds = 1)

    val first = mgr.armLookup(deviceA)
    mgr.testingOnlyCheckAllDevices()
    first.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))

    for
      _ <- yieldUntil(factory.created.size == 1)
      second = mgr.armLookup(deviceA)
      _ = factory.created.head.promise.failure(new Exception("disk on fire"))
      _ = second.success(deviceState(deviceA, Map(rebuilding(storeN(0)))))
      // The same store is retried rather than being stuck behind a slot that never frees.
      _ <- yieldUntil(factory.created.size == 2)
    yield
      factory.created.map(_.storeId).toList should be(List(storeN(0), storeN(0)))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *StoreRebuildTriggerSuite'`
Expected: compilation failure — `not found: object StoreRebuildFactory`.

- [ ] **Step 3: Write the trait and factory**

Create `src/main/scala/org/aspen_ddp/aspen/server/rebuild/StoreRebuild.scala`:

```scala
package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId

import java.nio.file.Path
import scala.concurrent.Future

/** Reconstruction of one store from the rest of its pool.
 *
 *  `complete` resolves once the rebuilt store is in place at
 *  `<device>/<storeId.directoryName>`. Flipping the device entry from Rebuilding to Active and
 *  loading the store are StoreManager's job, exactly as they are for a transfer in.
 */
trait StoreRebuild:
  val storeId: StoreId
  def complete: Future[Unit]

/** Constructs a store rebuild. The default `Filesystem` factory produces the real
 *  implementation. Tests inject a factory that records the request and resolves on command. */
trait StoreRebuildFactory:
  def createRebuild(client: AspenClient,
                    storeId: StoreId,
                    storageDeviceId: StorageDeviceId,
                    devicePath: Path): StoreRebuild

object StoreRebuildFactory:
  object Filesystem extends StoreRebuildFactory:
    def createRebuild(client: AspenClient,
                      storeId: StoreId,
                      storageDeviceId: StorageDeviceId,
                      devicePath: Path): StoreRebuild =
      new RebuildingStore(client, storeId, storageDeviceId, devicePath)
```

`RebuildingStore` does not exist yet. To keep this task independently testable, add a minimal
placeholder in the same file for now — Task 3 replaces it with the real class in its own file:

```scala
/** Placeholder until the walk lands. Replaced in full by
 *  server/rebuild/RebuildingStore.scala. */
private class RebuildingStore(client: AspenClient,
                              val storeId: StoreId,
                              storageDeviceId: StorageDeviceId,
                              devicePath: Path) extends StoreRebuild:
  def complete: Future[Unit] =
    Future.failed(new NotImplementedError("store rebuild is not implemented yet"))
```

- [ ] **Step 4: Wire up `StoreManager`**

In `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`, add the two constructor
parameters after `storeTransferFactory` (line 88):

```scala
                   val storeTransferFactory: StoreTransferFactory = StoreTransferFactory.Filesystem,
                   val storeRebuildFactory: StoreRebuildFactory = StoreRebuildFactory.Filesystem,
                   /** A rebuild reads a whole store's worth of data through the client, so an
                    *  eight-store device must not start eight at once. Entries beyond this bound
                    *  wait; every completion re-checks the device and starts the next. */
                   val maxConcurrentRebuilds: Int = 2) extends Logging {
```

Add the guard map alongside `creatingStores` (line 116):

```scala
  private var rebuildingStores: Map[StoreId, StoreRebuild] = Map()
```

Add `startStoreRebuild` immediately after `createNewStore` (which ends at line 692):

```scala
  /** Begins reconstruction of one store staged into `<device>/rebuilding/<store>/`.
   *
   *  Runs under the instance lock, so it does no more than construct the rebuild and register
   *  its continuation; the walk itself runs on the client's execution context.
   *
   *  On success the store is already in place on disk, so this loads it and flips the device
   *  entry from Rebuilding to Active -- re-reading inside the transaction and writing only if
   *  the status is still Rebuilding, exactly as createNewStore does for Initializing. Either
   *  outcome re-checks the device, which releases the concurrency slot and starts whatever was
   *  queued behind it.
   */
  private def startStoreRebuild(local: LocalStorageDeviceState, storeId: StoreId): Unit =
    synchronized {
      if !rebuildingStores.contains(storeId) && rebuildingStores.size < maxConcurrentRebuilds then
        val sr = storeRebuildFactory.createRebuild(
          client, storeId, local.storageDeviceId, local.devicePath)

        rebuildingStores += storeId -> sr

        def release(): Unit = synchronized:
          rebuildingStores -= storeId

        sr.complete.onComplete:
          case Success(_) =>
            release()
            loadStoreById(local.storageDeviceId, storeId)
            markRebuiltStoreActive(local.storageDeviceId, storeId)

          case Failure(err) =>
            // The checkpoint survives in the staging directory, so the next check resumes
            // rather than restarting. Nothing here retries directly: a rebuild that fails
            // immediately and repeatedly would otherwise spin.
            logger.warn(s"Rebuild of store $storeId failed: $err")
            release()
            checkStorageDevice(local.storageDeviceId)
    }

  /** Flips a rebuilt store's device entry from Rebuilding to Active, then re-checks the device
   *  so the next queued rebuild starts. */
  private def markRebuiltStoreActive(storageDeviceId: StorageDeviceId, storeId: StoreId): Unit =
    client.transactUntilSuccessful: tx =>
      for
        ptr <- client.getStorageDevicePointer(storageDeviceId)
        kvos <- client.read(ptr)
        state = StorageDeviceState(kvos)
      yield
        state.stores.get(storeId).foreach: entry =>
          if entry.status == StorageDeviceState.StoreStatus.Rebuilding then
            val newState = state.setStoreEntry(
              storeId, StorageDeviceState.StoreStatus.Active, None)

            val reqs = List(KeyRevision(StorageDeviceState.StateKey,
              kvos.contents(StorageDeviceState.StateKey).revision))
            val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))

            logger.info(s"Updating device state to mark rebuilt store $storeId as Active")
            tx.update(ptr, None, None, reqs, ops)

            tx.result.foreach: _ =>
              logger.info(s"Rebuild of store $storeId complete")
              checkStorageDevice(storageDeviceId)
```

Add the fourth clause at the end of `reconcileDeviceState`, after the Transferring In block
(which ends at line 761):

```scala
      //----------------------
      // Rebuilding Stores
      //
      remote.stores.filter((_, entry) =>
        entry.status == StorageDeviceState.StoreStatus.Rebuilding
      ).keys.toList.sortBy(_.poolIndex).foreach: storeId =>
        startStoreRebuild(local, storeId)
```

Sorted so the bound admits a deterministic subset rather than whichever two the map iterates
first — a resumed manager then picks up the same rebuilds it was running.

Add the imports:

```scala
import org.aspen_ddp.aspen.server.rebuild.{StoreRebuild, StoreRebuildFactory}
```

`Success`/`Failure` are already imported (used by `startStoreTransferIn`).

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *StoreRebuildTriggerSuite'`
Expected: PASS, 5 tests.

- [ ] **Step 6: Confirm the harness extraction did not break the discovery suite**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite'`
Expected: PASS, unchanged.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/rebuild/StoreRebuild.scala \
        src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerTestHarness.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreRebuildTriggerSuite.scala
git commit -m "$(cat <<'EOF'
feat: start a bounded store rebuild for each Rebuilding device entry

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: `RebuildingStore` — the walk, the checkpoint, the completion

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/rebuild/RebuildingStore.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/rebuild/StoreRebuild.scala` (delete the
  placeholder)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/rebuild/RebuildingStoreSuite.scala` (create)

**Interfaces:**
- Consumes: `RebuildState` (Task 1), the `StoreRebuild` trait (Task 2),
  `TieredKeyValueList.foreach` / `foreachFrom` (TKVL plan),
  `Backend.rebuildWrite` / `rebuildFlush`, `ObjectState.getRebuildDataForStore`.
- Produces:
  ```scala
  object RebuildingStore:
    val RebuildDirectory: String                   // "rebuilding"
    val CheckpointInterval: Int                    // 1000

  class RebuildingStore(client: AspenClient,
                        storeId: StoreId,
                        storageDeviceId: StorageDeviceId,
                        devicePath: Path,
                        checkpointInterval: Int = RebuildingStore.CheckpointInterval,
                        testingOnlyFailKeys: Set[Key] = Set()) extends StoreRebuild
  ```
  The last two are test seams and are named as such. They are constructor parameters rather
  than mutable fields because the walk starts from the constructor, so anything set afterwards
  would race it.

- [ ] **Step 1: Write the failing tests**

Create `src/test/scala/org/aspen_ddp/aspen/server/rebuild/RebuildingStoreSuite.scala`:

```scala
package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.{Key, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig

import scala.concurrent.{ExecutionContext, Future}

/** Exercises the walk against a real RocksDB backend on a temp directory, driven by the
 *  bootstrap pool's real allocation tree. */
class RebuildingStoreSuite extends IntegrationTestSuite:

  private val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)

  /** A bare device directory: no StorageDeviceConfig, since nothing here loads it as a device. */
  private def deviceDir(): os.Path =
    os.temp.dir(prefix = "rebuilding-store-suite")

  /** Allocate `n` data objects into the bootstrap pool so the allocation tree has content, and
   *  split it across more than one tier-0 node. */
  private def populate(n: Int): Future[List[org.aspen_ddp.aspen.common.objects.ObjectId]] =
    given ExecutionContext = executionContext
    val allocated = (0 until n).toList.map: i =>
      client.transactUntilSuccessful: tx =>
        given org.aspen_ddp.aspen.client.Transaction = tx
        client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
          pool.createAllocator(client.defaultObjectIDA).flatMap: alloc =>
            alloc.allocateDataObject(ObjectRevision.Null, DataBuffer(Array[Byte](i.toByte)))
              .map(_.id)
    Future.sequence(allocated)

  atest("a full walk restores every object and moves the store into place"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, net.bootstrapDeviceId, dev.toNIO)
      _ <- rebuild.complete
    yield
      val finalPath = dev / storeId.directoryName
      os.exists(finalPath) should be(true)
      // The staging directory and its checkpoint are gone once the store is whole.
      os.exists(dev / RebuildingStore.RebuildDirectory / storeId.directoryName) should be(false)
      os.exists(finalPath / RebuildState.stateFilename) should be(false)
      // A StoreConfig is written up front, so the moved directory is loadable.
      StoreConfig.loadStoreConfig((finalPath / StoreConfig.configFilename).toIO).storeId should
        be(storeId)

  atest("a pre-existing final directory skips straight to completion"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    // A crash between the move and the flip leaves the store in place with no staging
    // directory. The rebuild must not start over, and must not fail.
    os.makeDir.all(dev / storeId.directoryName)
    val rebuild = new RebuildingStore(client, storeId, net.bootstrapDeviceId, dev.toNIO)
    for
      _ <- rebuild.complete
    yield
      os.exists(dev / RebuildingStore.RebuildDirectory / storeId.directoryName) should be(false)

  atest("a resume skips the objects already restored"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- waitForTransactionsToComplete()

      // Stage a checkpoint by hand, as a crashed pass would have left behind.
      staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      _ = os.makeDir.all(staging)
      sorted = ids.map(id => Key(id.toBytes)).sortBy(_.bytes.toList)
      _ = RebuildState.save(staging, RebuildState(storeId, Some(sorted(9)), Nil))

      rebuild = new RebuildingStore(client, storeId, net.bootstrapDeviceId, dev.toNIO)
      _ <- rebuild.complete
      restored = rebuild.testingOnlyRestoredKeys
    yield
      // The resume range is inclusive of the checkpointed key -- rebuildWrite is an overwrite,
      // so re-restoring it is free -- and everything before it is skipped.
      restored.map(_.bytes.toList).toSet should be(sorted.drop(9).map(_.bytes.toList).toSet)

  atest("the checkpoint is written after the flush, not before"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      _ <- populate(20)
      _ <- waitForTransactionsToComplete()
      // A checkpoint every 5 objects rather than every 1000, so the walk actually checkpoints
      // without needing a thousand allocations.
      rebuild = new RebuildingStore(client, storeId, net.bootstrapDeviceId, dev.toNIO,
                                    checkpointInterval = 5)
      _ <- rebuild.complete
    yield
      // A checkpoint claiming objects that never reached stable storage is the one ordering
      // that loses data, so the order is recorded and asserted rather than inferred.
      val ops = rebuild.testingOnlyCheckpointTrace
      ops should not be empty
      ops.grouped(2).foreach(pair => pair should be(List("flush", "checkpoint")))

  atest("an object that cannot be read is recorded and the rebuild does not complete"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- waitForTransactionsToComplete()
      doomed = Key(ids.head.toBytes)
      rebuild = new RebuildingStore(client, storeId, net.bootstrapDeviceId, dev.toNIO,
                                    checkpointInterval = 5,
                                    testingOnlyFailKeys = Set(doomed))
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      // The pass must not report success with an object missing -- that would flip the store to
      // Active while it is short a slice.
      outcome.isFailure should be(true)
      // The staging directory and its checkpoint survive, so the next device check resumes.
      val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      os.exists(dev / storeId.directoryName) should be(false)
      val saved = RebuildState.load(staging).get
      saved.failedObjects should be(List(ids.head))
      // The checkpoint advanced past it: one unreadable object cannot wedge the walk forever.
      saved.lastRestoredKey should not be None
```

`testingOnlyRestoredKeys` and `testingOnlyCheckpointTrace` are named test hooks on
`RebuildingStore`, added in Step 3 — the ordering and the resume range are not observable from
disk after a successful pass.

Check `net.bootstrapDeviceId` exists on `TestNetwork`; if not, use
`StorageDeviceId.BootstrapStorageDeviceId`. Check `client.defaultObjectIDA` and the allocator API
against an existing allocation test (`grep -rn "allocateDataObject" src/test/`) and match it — the
exact allocation idiom matters less than getting 20 objects into the tree.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *RebuildingStoreSuite -- -z "full walk"'`
Expected: FAIL with `NotImplementedError: store rebuild is not implemented yet`.

- [ ] **Step 3: Write `RebuildingStore`**

Create `src/main/scala/org/aspen_ddp/aspen/server/rebuild/RebuildingStore.scala`:

```scala
package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, KeyValueObjectState, MetadataObjectState, ObjectState}
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.tkvl.KeyValueListNode
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, Metadata, ObjectId, ObjectPointer, ObjectType}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.StoreConfig
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBBackend, RocksDBConfig}
import scribe.Logging

import java.nio.file.Path
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

object RebuildingStore:
  /** Staging directory under the device root. Invisible to StoreManager.tryLoadStore for the
   *  same reason transferring-in/ is: there is no StoreConfig at this level, only one level
   *  down. That is what keeps a half-rebuilt store from being loaded and answering reads. */
  val RebuildDirectory = "rebuilding"

  /** Objects between checkpoints. Small enough that a crash costs little, large enough that
   *  the flush is not the dominant cost. */
  val CheckpointInterval: Int = 1000


/** Reconstructs one store by walking its pool's allocation tree, reading each object through
 *  the client, and writing this store's slice of it into a fresh backend.
 *
 *  The store is not Active while this runs, so it participates in no transactions and receives
 *  no writes. Objects the walk has not yet reached are therefore read at their current value.
 *  The gap is the other side: an object already copied and then written before the rebuild
 *  finishes leaves a stale slice here. That is the same condition as a store that missed writes
 *  while briefly offline, and the repair-on-read path heals it for anything something reads.
 *  Cold objects are not healed; a background scrubber is deliberately out of scope.
 */
class RebuildingStore(client: AspenClient,
                      val storeId: StoreId,
                      storageDeviceId: StorageDeviceId,
                      devicePath: Path,
                      checkpointInterval: Int = RebuildingStore.CheckpointInterval,
                      testingOnlyFailKeys: Set[Key] = Set()) extends StoreRebuild with Logging:

  import RebuildingStore.*

  private given ExecutionContext = client.clientContext

  private val stagingPath = os.Path(devicePath) / RebuildDirectory / storeId.directoryName
  private val finalPath = os.Path(devicePath) / storeId.directoryName

  private val completionPromise: Promise[Unit] = Promise()
  def complete: Future[Unit] = completionPromise.future

  /** Keys handed to rebuildWrite, in walk order. Test hook. */
  private val restoredKeys = mutable.ListBuffer[Key]()
  private[rebuild] def testingOnlyRestoredKeys: List[Key] = synchronized(restoredKeys.toList)

  /** "flush" / "checkpoint" in the order they happened. Test hook: the ordering between them
   *  is the one thing that cannot be observed from disk after a successful pass. */
  private val checkpointTrace = mutable.ListBuffer[String]()
  private[rebuild] def testingOnlyCheckpointTrace: List[String] =
    synchronized(checkpointTrace.toList)

  private var backend: Backend = null
  private var restoredSinceCheckpoint = 0
  private var lastKey: Option[Key] = None
  private var failed: List[ObjectId] = Nil

  // Started from the constructor, mirroring TransferringIn. StoreManager holds its instance
  // lock across the call, so everything expensive is inside the future.
  start()

  private def start(): Unit =
    if os.exists(finalPath) then
      // A crash between the move and the flip. The store is already whole; there is nothing to
      // rebuild, only the flip left, and that is StoreManager's job on completion.
      logger.info(s"Rebuild of $storeId: store already in place at $finalPath")
      cleanupStaging()
      completionPromise.success(())
    else
      val f =
        try runPass()
        catch
          case err: Throwable => Future.failed(err)

      f.onComplete: outcome =>
        closeBackend().onComplete(_ => completionPromise.tryComplete(outcome))

  private def cleanupStaging(): Unit =
    try
      if os.exists(stagingPath) then os.remove.all(stagingPath)
    catch
      case t: Throwable => logger.warn(s"Rebuild of $storeId: failed to remove $stagingPath: $t")

  /** Idempotent: finish() closes on the happy path, start()'s onComplete closes on every path. */
  private def closeBackend(): Future[Unit] =
    val b = synchronized:
      val prev = backend
      backend = null
      prev

    if b == null then
      Future.unit
    else
      b.close().recover:
        case t => logger.warn(s"Rebuild of $storeId: backend close failed: $t")

  /** One full pass: open (or reopen) the staging area, walk, retry failures, complete. */
  private def runPass(): Future[Unit] =
    // The checkpoint is never deleted on restart -- reading it is what makes a restart a
    // resume.
    val resume = RebuildState.load(stagingPath)

    os.makeDir.all(stagingPath)

    if !os.exists(stagingPath / StoreConfig.configFilename) then
      // Written up front so the directory is loadable the instant it is moved into place.
      os.write.over(stagingPath / StoreConfig.configFilename,
        StoreConfig(storeId, StoreConfig.RocksDB()).yamlConfig)

    resume.foreach: st =>
      synchronized:
        lastKey = st.lastRestoredKey
        failed = st.failedObjects
      logger.info(s"Rebuild of $storeId: resuming from ${st.lastRestoredKey}, " +
                  s"${st.failedObjects.size} objects to retry")

    for
      pstate <- client.getStoragePoolState(storeId.poolId)
      _ = synchronized:
            backend = pstate.backendConfig match
              case _: RocksDBConfig => new RocksDBBackend(stagingPath.toNIO, storeId, summon)
      pool <- client.getStoragePool(storeId.poolId)
      tree = pool.allocationTree
      _ <- resume.flatMap(_.lastRestoredKey) match
             // The resume range is inclusive of the checkpointed key. rebuildWrite is an
             // overwrite, so re-restoring that one object is free, and inclusive is the only
             // bound that cannot skip.
             case Some(k) => tree.foreachFrom(k, restoreObject)
             case None => tree.foreach(restoreObject)
      _ <- retryFailures()
      _ <- finish()
    yield ()

  /** Copy one object's slice of this store into the backend.
   *
   *  foreach swallows a failing fn -- it logs and continues -- so read failures are recorded
   *  here instead. The checkpoint advances past them, so one unreadable object cannot wedge the
   *  walk forever, and they are retried at the end of the pass.
   */
  private def restoreObject(node: KeyValueListNode,
                            key: Key,
                            value: ValueState): Future[Unit] =
    val ptr = ObjectPointer(value.value.bytes)

    if ptr.poolId != storeId.poolId then
      // The allocation tree is per-pool, but a pointer stored in it is only authoritative for
      // its own pool. Skip anything foreign rather than writing it into the wrong store.
      Future.unit
    else
      attemptRestore(key, ptr).transformWith:
        case scala.util.Success(_) =>
          recordRestored(key)
        case scala.util.Failure(err) if isOutOfSpace(err) =>
          // Distinct from a per-object read failure, and not something to accumulate 10,000 of:
          // selectDeviceForRebuild checked free space at placement time against the pool's
          // recorded store size, which can be stale and can grow. There is no automatic
          // recovery in this scope -- the operator's remedy is to add capacity -- so log it
          // loudly enough to alert on and abort with the checkpoint intact.
          checkpoint()
          logger.error(s"REBUILD OUT OF SPACE: store $storeId cannot fit on device " +
                       s"$storageDeviceId at $devicePath. The store will remain Rebuilding " +
                       s"until capacity is added. Underlying error: $err")
          Future.failed(err)
        case scala.util.Failure(err) =>
          recordFailure(ObjectId(key.bytes), err)

  /** Best-effort detection of a full destination. Backends surface it differently -- RocksDB
   *  wraps it, the JDK throws IOException -- so this matches on the message as well as the
   *  type. A false negative only costs the slower path through recordFailure. */
  private def isOutOfSpace(err: Throwable): Boolean =
    def matches(t: Throwable): Boolean =
      val msg = Option(t.getMessage).getOrElse("").toLowerCase
      msg.contains("no space left") || msg.contains("disk full") ||
        msg.contains("insufficient space")

    Iterator.iterate(err)(_.getCause).takeWhile(_ != null).take(8).exists(matches)

  // Key's equality is not something to bet a test seam on; compare the bytes directly.
  private val failBytes: Set[List[Byte]] = testingOnlyFailKeys.map(_.bytes.toList)

  private def attemptRestore(key: Key, ptr: ObjectPointer): Future[Unit] =
    if failBytes.contains(key.bytes.toList) then
      Future.failed(new Exception(s"injected read failure for $key"))
    else
      restore(ptr)

  private def restore(ptr: ObjectPointer): Future[Unit] =
    def metadataOf(os: ObjectState): (ObjectType.Value, Metadata) = os match
      case kvos: KeyValueObjectState =>
        (ObjectType.KeyValue, Metadata(kvos.revision, kvos.refcount, kvos.timestamp))
      case dos: DataObjectState =>
        (ObjectType.Data, Metadata(dos.revision, dos.refcount, dos.timestamp))
      case _: MetadataObjectState =>
        throw new Exception(s"Unsupported object type for rebuild: $ptr")

    val fos = ptr match
      case p: KeyValueObjectPointer => client.read(p)
      case p: DataObjectPointer => client.read(p)

    fos.map: os =>
      val (objectType, metadata) = metadataOf(os)
      val localData = os.getRebuildDataForStore(storeId)
      backend.rebuildWrite(os.id, objectType, metadata, localData.getOrElse(DataBuffer()))

  private def recordRestored(key: Key): Future[Unit] =
    val checkpointNow = synchronized:
      restoredKeys += key
      lastKey = Some(key)
      restoredSinceCheckpoint += 1
      if restoredSinceCheckpoint >= checkpointInterval then
        restoredSinceCheckpoint = 0
        true
      else
        false

    if checkpointNow then checkpoint()
    Future.unit

  private def recordFailure(objectId: ObjectId, err: Throwable): Future[Unit] =
    val over = synchronized:
      failed = objectId :: failed
      failed.size > RebuildState.MaxFailedObjects

    logger.warn(s"Rebuild of $storeId: failed to read object $objectId: $err")

    if over then
      // Something systemic is wrong -- the pool below its read threshold, most likely -- and
      // continuing only burns I/O. Abort with the checkpoint intact; the next device check
      // retries.
      checkpoint()
      Future.failed(new Exception(
        s"Rebuild of $storeId aborted: more than ${RebuildState.MaxFailedObjects} unreadable objects"))
    else
      Future.unit

  /** Flush FIRST, then write the checkpoint. The reverse order would let a crash between the
   *  two produce a checkpoint claiming objects that never reached stable storage. */
  private def checkpoint(): Unit =
    backend.rebuildFlush()
    synchronized(checkpointTrace += "flush")
    val (k, f) = synchronized((lastKey, failed))
    RebuildState.save(stagingPath, RebuildState(storeId, k, f))
    synchronized(checkpointTrace += "checkpoint")

  /** Retry everything the walk could not read. Anything still failing leaves the store
   *  Rebuilding for the next device check to pick up. */
  private def retryFailures(): Future[Unit] =
    val pending = synchronized:
      val f = failed
      failed = Nil
      f

    if pending.isEmpty then
      Future.unit
    else
      logger.info(s"Rebuild of $storeId: retrying ${pending.size} unreadable objects")
      val pool = client.getStoragePool(storeId.poolId)
      val retries = pending.map: objectId =>
        val key = Key(objectId.toBytes)
        pool.flatMap(_.allocationTree.get(key)).flatMap:
          case Some(vs) => attemptRestore(key, ObjectPointer(vs.value.bytes))
          case None => Future.unit // deleted since the walk saw it; nothing to restore
        .recover:
          case err =>
            synchronized(failed = objectId :: failed)
            logger.warn(s"Rebuild of $storeId: retry of $objectId failed: $err")

      Future.sequence(retries).flatMap: _ =>
        val remaining = synchronized(failed)
        if remaining.isEmpty then
          Future.unit
        else
          checkpoint()
          Future.failed(new Exception(
            s"Rebuild of $storeId incomplete: ${remaining.size} objects still unreadable"))

  /** Flush, close, drop the checkpoint, move into place. */
  private def finish(): Future[Unit] =
    backend.rebuildFlush()
    closeBackend().map: _ =>
      val checkpointFile = stagingPath / RebuildState.stateFilename
      if os.exists(checkpointFile) then
        os.remove(checkpointFile)

      os.move(stagingPath, finalPath)
      logger.info(s"Rebuild of $storeId: store in place at $finalPath")
```

Delete the placeholder `RebuildingStore` from `StoreRebuild.scala` — the real class now supplies
it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `sbt 'testOnly *RebuildingStoreSuite'`
Expected: PASS, 5 tests.

Two API details already confirmed, called out because getting either wrong compiles into a
subtle bug rather than an error:
- `Backend.close(): Future[Unit]` is asynchronous. `closeBackend` above returns its future and
  every caller sequences on it — do not drop it, or `os.move` can race the backend's last write.
- `ObjectId(bytes: Array[Byte])` and `objectId.toBytes` both exist (`ObjectId.scala:7,16`), so
  the allocation-tree key and the `ObjectId` convert in both directions.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/rebuild/RebuildingStore.scala \
        src/main/scala/org/aspen_ddp/aspen/server/rebuild/StoreRebuild.scala \
        src/test/scala/org/aspen_ddp/aspen/server/rebuild/RebuildingStoreSuite.scala
git commit -m "$(cat <<'EOF'
feat: implement the store rebuild walk with crash-resumable checkpointing

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: End-to-end — fail a device, drive both halves, compare slices

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` (add `completeRebuilds`)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/rebuild/RebuildEndToEndSuite.scala` (create)

**Interfaces:**
- Consumes: everything from the producer plan and Tasks 1-3.
- Produces: `TestNetwork.completeRebuilds(): Future[Unit]`, alongside the existing
  transfer-completion helpers.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/server/rebuild/RebuildEndToEndSuite.scala`:

```scala
package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceState}
import org.aspen_ddp.aspen.common.objects.ObjectRevision
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

/** Both halves against each other: an operator fails a device, the producer moves its stores
 *  onto a live one marked Rebuilding, and the consumer reconstructs them. */
class RebuildEndToEndSuite extends IntegrationTestSuite:

  atest("a failed device's stores are reconstructed on a live device"):
    given ExecutionContext = executionContext
    val failedId = StorageDeviceId.BootstrapStorageDeviceId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId

    for
      // Content to reconstruct, and a live destination to reconstruct it onto.
      ids <- Future.sequence((0 until 20).toList.map: i =>
               client.transactUntilSuccessful: tx =>
                 given org.aspen_ddp.aspen.client.Transaction = tx
                 client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
                   pool.createAllocator(client.defaultObjectIDA).flatMap: alloc =>
                     alloc.allocateDataObject(ObjectRevision.Null,
                       DataBuffer(Array[Byte](i.toByte))).map(_.id))
      _ <- waitForTransactionsToComplete()
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()

      // Producer.
      _ <- client.failStorageDevice(failedId)
      _ <- waitForTransactionsToComplete()
      _ <- net.driveFailedDeviceTask(failedId, setId, Duration(30000, MILLISECONDS))
      _ <- waitForTransactionsToComplete()

      staged <- client.getStorageDeviceState(net.secondDeviceId)
      _ = staged.stores.size should be(3)

      // Consumer.
      _ <- net.completeRebuilds()
      _ <- waitForTransactionsToComplete()

      rebuilt <- client.getStorageDeviceState(net.secondDeviceId)
      // Every object is still readable through the pool, which is the only thing an
      // application can observe -- and the only thing worth asserting.
      reads <- Future.sequence(ids.map(id => client.readObject(id)))
    yield
      rebuilt.stores.size should be(3)
      rebuilt.stores.values.foreach: entry =>
        entry.status should be(StorageDeviceState.StoreStatus.Active)
      reads.size should be(20)
```

`net.driveFailedDeviceTask` and `client.readObject` are placeholders for whatever the harness
actually offers. Before writing this test, check what exists:
- Reuse `FailedStorageDeviceSuite`'s `taskForEnrolled` + `awaitCompletion` for the producer half
  rather than inventing a `TestNetwork` helper — lift them into a shared trait if both suites need
  them.
- For the read-back assertion, use the pool's allocation tree and `client.read(ptr)` on each
  pointer, matching `RebuildingStoreSuite`'s idiom, rather than a `readObject` that may not exist.

Adjust the test to the real APIs; keep the assertions.

- [ ] **Step 2: Add `completeRebuilds` to `TestNetwork`**

In `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`, add alongside the existing
transfer-completion helpers:

```scala
  /** Drives every StoreManager's device check until no store anywhere is Rebuilding.
   *
   *  Rebuilds are started by the device-check machinery, which in production runs on an hourly
   *  timer. Tests do not wait an hour: this pumps checks and yields until the flips land.
   */
  def completeRebuilds(timeout: Duration = Duration(30, SECONDS)): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos

    def anyRebuilding(): Future[Boolean] =
      Future.sequence(storeManagers.toList.map(_ => Future.unit)).flatMap: _ =>
        // Read every device's state and look for a Rebuilding entry.
        Future.sequence(knownDeviceIds.toList.map(client.getStorageDeviceState))
          .map(_.exists(_.stores.values.exists(
            _.status == StorageDeviceState.StoreStatus.Rebuilding)))

    def loop(): Future[Unit] =
      anyRebuilding().flatMap: rebuilding =>
        if !rebuilding then
          Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError("stores are still Rebuilding after $timeout"))
        else
          storeManagers.foreach(_.testingOnlyCheckAllDevices())
          storeManagers.foreach(_.testingOnlyHandleEvents())
          Future(()).flatMap(_ => loop())

    loop()
```

Adapt `storeManagers` and `knownDeviceIds` to whatever `TestNetwork` actually exposes — read the
existing `completeInFlightTransfers` and copy its access pattern exactly.

- [ ] **Step 3: Run the test to verify it fails, then passes**

Run: `sbt 'testOnly *RebuildEndToEndSuite'`

Expect failures on the first runs while the harness details settle. Fix them in the test and the
helper; do not change the production code to accommodate the harness unless the failure is a real
defect — if it is, that is what this test is for, so fix the defect and note it in the commit
message.

Expected once settled: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala \
        src/test/scala/org/aspen_ddp/aspen/server/rebuild/RebuildEndToEndSuite.scala
git commit -m "$(cat <<'EOF'
test: drive a device failure through both halves end to end

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Delete the obsolete CLI `rebuild`

`Main.rebuild` hardcodes `demo/bootstrap-host/.../bootstrap-device`, is unreachable from the
parser, and is now superseded outright. `RebuildingStore` was written from its body; nothing is
lost by removing it.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:1253-1323` (delete `rebuild`),
  the commented-out parser entry, and the commented-out dispatch case at line 735

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

- [ ] **Step 1: Delete the method and its dead references**

Delete `def rebuild(storeName: String, bootstrapConfigFile: os.Path): Unit` in its entirety,
including the `/** OBSOLETE: ... */` scaladoc above it.

Delete the commented-out dispatch line:

```scala
            //case "rebuild" => rebuild(cfg.storeName, bootstrapConfigPath)
```

Search for any commented-out `cmd("rebuild")` parser block and delete that too:

```bash
grep -n "rebuild" src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
```

Everything the grep turns up should now be either gone or part of `fail-storage-device`'s help
text.

- [ ] **Step 2: Remove orphaned imports**

The deletion likely orphans imports used only by `rebuild` — candidates include
`RocksDBBackend`, `Backend`, `StorageDeviceManager`, `KeyValueListNode`, `MetadataObjectState`,
`ObjectType`, `Metadata`. Compile and let the warnings guide it:

Run: `sbt compile`
Expected: no errors. Remove any import the compiler now flags as unused; leave anything still in
use by another command.

- [ ] **Step 3: Verify the CLI still builds and runs**

Run: `sbt 'runMain org.aspen_ddp.aspen.cmdline.Main --help'`
Expected: help output, with `fail-storage-device` present and no `rebuild`.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "$(cat <<'EOF'
refactor: delete the obsolete CLI rebuild command

Its logic now lives in server/rebuild, driven by the device-check
machinery rather than by hardcoded demo paths.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Full-suite regression pass and TODO cleanup

**Files:**
- No source changes expected. Fix anything this pass turns up.
- Modify: `TODO.txt`

**Interfaces:**
- Consumes: everything above.
- Produces: nothing.

- [ ] **Step 1: Run the full suite**

Run: `sbt test`
Expected: PASS.

- [ ] **Step 2: Fix and re-run**

If anything fails, fix it and re-run `sbt test` until green. Commit each fix separately with a
message naming what broke:

```bash
git commit -m "$(cat <<'EOF'
fix: <what broke and why>

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3: Remove the entry from TODO.txt**

Every bullet under "Need official mechanism for rebuilding a failed store" is now done. Delete
the whole entry.

One deliberate deviation to record rather than silently drop: TODO.txt proposed
`rebuild-state.yaml` in the device root as a do-not-load marker. The staging directory subsumes
that role — it is invisible to `tryLoadStore` for the same reason `transferring-in/` is — and the
file keeps only its checkpoint job, inside the staging directory. If `TODO.txt` is the only record
of that intent, note the resolution in the commit message rather than leaving a stale bullet.

```bash
git add TODO.txt
git commit -m "$(cat <<'EOF'
docs: remove the completed failed-store rebuild entry from TODO.txt

rebuild-state.yaml ended up inside the rebuilding/ staging directory as
the checkpoint only; the staging directory itself is the do-not-load
marker, since tryLoadStore cannot see into it.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

## Definition of Done

- `sbt test` passes.
- A store entered as `Rebuilding` on a device is reconstructed and flipped to `Active` with no
  operator action beyond the initial `fail-storage-device`.
- At most `maxConcurrentRebuilds` rebuilds run per `StoreManager` at once, and every completion
  — success or failure — releases its slot and re-checks the device.
- A half-rebuilt store is never loadable: it lives under `rebuilding/`, where `tryLoadStore`
  cannot see it, until the move.
- Killing the process mid-rebuild and restarting resumes from the checkpoint rather than
  restarting the walk, and the checkpoint never names an object that had not been flushed.
- A rebuild that cannot read every object leaves the store `Rebuilding` rather than flipping it
  to `Active` short a slice.
- The obsolete `Main.rebuild` is gone and `TODO.txt` no longer carries this entry.
