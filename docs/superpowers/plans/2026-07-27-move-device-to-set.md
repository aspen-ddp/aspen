# move-device-to-set Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `move-device-to-set` CLI command that atomically moves a storage device from its current level-0 device set into a different level-0 device set.

**Architecture:** A static `moveDevice` helper on `StorageDeviceSetState` builds a single transaction updating three objects (the device's KV state, the target set, and the old set); a thin `AspenClient.moveDeviceToSet` delegates to it; and a CLI command in `Main.scala` parses arguments and resolves the target set by name-or-UUID. Follows the existing `add-group-to-group` / `transfer-store` pattern.

**Tech Stack:** Scala 3, ScalaTest (`IntegrationTestSuite`), scopt CLI parser, Aspen transaction API.

---

## Background / Key Facts (read before starting)

- **`StorageDeviceState`** (`src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala`) is a **KeyValue object**. Its serialized state lives under `StorageDeviceState.StateKey` and it carries `storageDeviceSet: StorageDeviceSetId`. It exposes `.encode(): Array[Byte]` and `.copy(...)`. Updated in a transaction via `tx.update(ptr, None, None, List(KeyRevision(StateKey, rev)), List(Insert(StateKey, bytes)))` (see `AspenClient.transferStore`).
- **`StorageDeviceSetState`** (same directory) is a **DataObject** with `memberDevices: List[StorageDeviceId]` and `level: Int` (level 0 holds devices). It exposes `.toBytes` and `.copy(...)`. Updated via `tx.overwrite(ptr, dos.revision, DataBuffer(bytes))` (see `AllocationGroupState.addGroup`).
- The device already knows its current set via `deviceState.storageDeviceSet`; the "old set" is derived, never passed in.
- Pointer lookups (`getStorageDevicePointer`, `getStorageDeviceSetPointer`) fail with `NoSuchElementException` when the id is unknown (via `MetadataTree.get`).
- `Main.resolveRef[A](ref, byUuid, byName)` (already in `Main.scala`) resolves a name-or-UUID string: it parses the string as a UUID and calls `byUuid`, or falls back to `byName` on parse failure.
- Each `atest` in `IntegrationTestSuite` runs against a fresh `TestNetwork`, so the bootstrap device/set can be mutated freely per test. Helpers available: `waitForTransactionsToComplete()`, `recoverToSucceededIf[T](future)`, `client`, `executionContext`.
- Bootstrap fixtures: `StorageDeviceId.BootstrapStorageDeviceId` starts in `StorageDeviceSetId.BootstrapStorageDeviceSetId` (a level-0 set).

## File Structure

- **Modify** `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala` — add `NotLevelZero` error type and the `moveDevice` static helper (the transaction logic).
- **Modify** `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` — add the thin `moveDeviceToSet` method.
- **Modify** `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — add the CLI command (config field, `cmd` block, dispatch case, `move_device_to_set` function).
- **Modify** `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala` — add integration tests exercising `client.moveDeviceToSet`.
- **Modify** `TODO.txt` — remove the completed top item.

### Note on CLI testing

The scopt parser is a local `val` inside `Main.main` and is not exposed for unit testing. The precedent command `add-group-to-group` (commit a9ee063) added no CLI test. This plan follows that precedent: the transaction logic is fully covered by integration tests through `client.moveDeviceToSet`, and the CLI wiring is verified by `sbt compile`. No `MainSuite` test is added.

---

## Task 1: State helper, error type, and client method

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala`

- [ ] **Step 1: Write the failing happy-path test**

Add these imports to the top of `StorageDeviceSetIntegrationSuite.scala` (below the existing imports):

```scala
import org.aspen_ddp.aspen.common.DataBuffer
```

Append this test to `StorageDeviceSetIntegrationSuite`:

```scala
  atest("moveDeviceToSet moves the device and updates both sets"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    val oldSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      destId <- client.createStorageDeviceSet("dest-set", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      _ <- client.moveDeviceToSet(deviceId, destId)
      _ <- waitForTransactionsToComplete()

      device <- client.getStorageDeviceState(deviceId)
      dest <- client.getStorageDeviceSetState(destId)
      old <- client.getStorageDeviceSetState(oldSetId)
    yield
      device.storageDeviceSet should be(destId)
      dest.memberDevices should contain(deviceId)
      old.memberDevices should not contain deviceId
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite -- -z "moveDeviceToSet moves the device"'`
Expected: FAIL — compilation error, `value moveDeviceToSet is not a member of ... AspenClient`.

- [ ] **Step 3: Add the `NotLevelZero` error type and `moveDevice` helper**

In `StorageDeviceSetState.scala`, add these imports to the existing import block:

```scala
import org.aspen_ddp.aspen.client.{ReadError, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
```

In the `object StorageDeviceSetState:` body (e.g. just after the `apply` overloads), add:

```scala
  /** Thrown when attempting to move a device into a set whose level is not 0.
   *  Only level-0 sets hold devices. */
  class NotLevelZero(val setId: StorageDeviceSetId)
      extends Throwable(s"Storage device set ${setId.uuid} is not level 0")

  /** Move `deviceId` from its current level-0 set into `targetSetId` (which must also be
   *  level 0), in a single atomic transaction updating three objects: the device KV
   *  state's `storageDeviceSet` field, the target set's `memberDevices` (device added),
   *  and the old set's `memberDevices` (device removed).
   *
   *  No-op if the device is already in the target set. Fails with NoSuchElementException
   *  if the device or a set object cannot be found, or NotLevelZero if the target set is
   *  not level 0. The old set is updated even if it did not actually list the device
   *  (self-healing toward the correct final state). */
  def moveDevice(client: AspenClient,
                 deviceId: StorageDeviceId,
                 targetSetId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = client.clientContext

    def prep(tx: Transaction): Future[Unit] =
      for
        devPtr <- client.getStorageDevicePointer(deviceId)
        devKvos <- client.read(devPtr)
        deviceState = StorageDeviceState(devKvos)
        oldSetId = deviceState.storageDeviceSet
        targetPtr <- client.getStorageDeviceSetPointer(targetSetId)
        targetDos <- client.read(targetPtr)
        target = StorageDeviceSetState(targetDos)
        _ <-
          if target.level != 0 then
            throw new NotLevelZero(targetSetId)
          else if oldSetId == targetSetId then
            Future.unit
          else
            for
              oldPtr <- client.getStorageDeviceSetPointer(oldSetId)
              oldDos <- client.read(oldPtr)
              oldSet = StorageDeviceSetState(oldDos)
            yield
              // Device KV state -> point at the target set
              val newDeviceState = deviceState.copy(storageDeviceSet = targetSetId)
              val devReqs = List(KeyRevision(StorageDeviceState.StateKey,
                devKvos.contents(StorageDeviceState.StateKey).revision))
              val devOps = List(Insert(StorageDeviceState.StateKey, newDeviceState.encode()))
              tx.update(devPtr, None, None, devReqs, devOps)

              // Target set -> add the device (dedup-guarded)
              val newTarget = target.copy(
                memberDevices = deviceId :: target.memberDevices.filter(_ != deviceId))
              tx.overwrite(targetPtr, targetDos.revision, DataBuffer(newTarget.toBytes))

              // Old set -> remove the device (self-healing if absent)
              val newOld = oldSet.copy(
                memberDevices = oldSet.memberDevices.filter(_ != deviceId))
              tx.overwrite(oldPtr, oldDos.revision, DataBuffer(newOld.toBytes))
      yield ()

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)
      case e: NotLevelZero => throw StopRetrying(e)

    client.transactUntilSuccessfulWithRecovery(onFail): tx =>
      prep(tx)
```

Then add the thin client method. In `AspenClient.scala`, just after the `addGroupToGroup` method (around line 143), add:

```scala
  /** Move a storage device (by id) from its current level-0 device set into
   *  `targetSetId` (which must also be level 0), in a single atomic transaction.
   *  Fails with NoSuchElementException if the device or target set is unknown, or
   *  StorageDeviceSetState.NotLevelZero if the target set is not level 0. */
  def moveDeviceToSet(deviceId: StorageDeviceId, targetSetId: StorageDeviceSetId): Future[Unit] =
    StorageDeviceSetState.moveDevice(this, deviceId, targetSetId)
```

(The types `StorageDeviceId`, `StorageDeviceSetId`, and `StorageDeviceSetState` are already imported at `AspenClient.scala:9`.)

- [ ] **Step 4: Run the happy-path test to verify it passes**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite -- -z "moveDeviceToSet moves the device"'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala \
        src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala
git commit -m "feat: add StorageDeviceSetState.moveDevice and AspenClient.moveDeviceToSet

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

- [ ] **Step 6: Add the edge-case tests**

Append these four tests to `StorageDeviceSetIntegrationSuite`:

```scala
  atest("moveDeviceToSet is a no-op when the device is already in the target set"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- client.moveDeviceToSet(deviceId, setId)
      _ <- waitForTransactionsToComplete()

      device <- client.getStorageDeviceState(deviceId)
      set <- client.getStorageDeviceSetState(setId)
    yield
      device.storageDeviceSet should be(setId)
      set.memberDevices should contain(deviceId)
      set.memberDevices.count(_ == deviceId) should be(1)

  atest("moveDeviceToSet fails with NotLevelZero when the target set is not level 0"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    for
      level1Id <- client.createStorageDeviceSet("higher-set", level = 1, parent = None)
      _ <- waitForTransactionsToComplete()

      err <- client.moveDeviceToSet(deviceId, level1Id).failed
    yield
      err shouldBe a[StorageDeviceSetState.NotLevelZero]

  atest("moveDeviceToSet fails with NoSuchElementException for an unknown device"):
    given ExecutionContext = executionContext
    val unknownDevice = StorageDeviceId(java.util.UUID.randomUUID())
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    recoverToSucceededIf[NoSuchElementException](
      client.moveDeviceToSet(unknownDevice, setId)
    )

  atest("moveDeviceToSet self-heals when the old set does not list the device"):
    given ExecutionContext = executionContext
    val deviceId = StorageDeviceId.BootstrapStorageDeviceId
    val oldSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      destId <- client.createStorageDeviceSet("selfheal-dest", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      // Corrupt state: remove the device from the old set's memberDevices while the
      // device's storageDeviceSet still points at the old set.
      oldPtr <- client.getStorageDeviceSetPointer(oldSetId)
      oldDos <- client.read(oldPtr)
      oldSet = StorageDeviceSetState(oldDos)
      _ <- client.transactUntilSuccessful: tx =>
             tx.overwrite(oldPtr, oldDos.revision,
               DataBuffer(oldSet.copy(memberDevices = oldSet.memberDevices.filter(_ != deviceId)).toBytes))
             Future.unit
      _ <- waitForTransactionsToComplete()

      _ <- client.moveDeviceToSet(deviceId, destId)
      _ <- waitForTransactionsToComplete()

      device <- client.getStorageDeviceState(deviceId)
      dest <- client.getStorageDeviceSetState(destId)
      old <- client.getStorageDeviceSetState(oldSetId)
    yield
      device.storageDeviceSet should be(destId)
      dest.memberDevices should contain(deviceId)
      old.memberDevices should not contain deviceId
```

- [ ] **Step 7: Run the full suite to verify all tests pass**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite'`
Expected: PASS (all tests, including the four new ones and the pre-existing ones).

- [ ] **Step 8: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala
git commit -m "test: cover moveDeviceToSet no-op, NotLevelZero, not-found, and self-heal

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: CLI command wiring

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
- Modify: `TODO.txt`

- [ ] **Step 1: Add the config field**

In `Main.scala`, in the `case class Args(...)` (around line 58-80), add a `deviceId` field. The target set reuses the existing `deviceSetName` field. Add:

```scala
                  deviceId:String="",
```

(Place it alongside the other `String=""` fields, e.g. after `storeName:String="",`.)

- [ ] **Step 2: Add the `cmd` block**

In `Main.scala`, immediately after the `cmd("add-group-to-group")...` block (ends around line 285), add:

```scala
      cmd("move-device-to-set").text("Moves a storage device into a different level-0 device set").
        action((_, c) => c.copy(mode = "move-device-to-set")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<device-uuid>").text("UUID of the storage device to move").
            action((x, c) => c.copy(deviceId = x)).
            validate { x =>
              try
                UUID.fromString(x)
                success
              catch
                case _: Throwable => failure("Storage device id must be a valid UUID")
            },

          arg[String]("<set-name-or-uuid>").text("Name or UUID of the target level-0 device set").
            action((x, c) => c.copy(deviceSetName = x)),
        )
```

- [ ] **Step 3: Add the dispatch case**

In `Main.scala`, in the mode-dispatch `match` (near the other `case "add-group-to-group" => ...` around line 467), add:

```scala
            case "move-device-to-set" => move_device_to_set(bootstrapConfigPath, cfg.deviceId, cfg.deviceSetName)
```

- [ ] **Step 4: Add the command function**

In `Main.scala`, immediately after the `add_group_to_group(...)` function (ends around line 1151), add:

```scala
  def move_device_to_set(bootstrapConfigFile: os.Path,
                         deviceIdStr: String,
                         setRef: String): Unit = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val deviceId = StorageDeviceId(UUID.fromString(deviceIdStr))

    val f = for
      targetSetId <- resolveRef(setRef, StorageDeviceSetId(_), client.getStorageDeviceSetId)
      _           <- client.moveDeviceToSet(deviceId, targetSetId)
    yield ()

    // getStorageDevicePointer / getStorageDeviceSetPointer / getStorageDeviceSetId throw
    // NoSuchElementException for unknown ids/names; moveDevice throws NotLevelZero when the
    // target set is not level 0. Translate both into precise messages.
    def reportError(cause: Throwable): Unit = cause match
      case _: NoSuchElementException =>
        println(s"Error: device '$deviceIdStr' or set '$setRef' not found")
      case _: StorageDeviceSetState.NotLevelZero =>
        println(s"Error: target set '$setRef' must be a level-0 (tier-0) set")
      case e =>
        println(s"Error moving device to set: ${e.getMessage}")

    f.onComplete:
      case scala.util.Success(_) =>
        println(s"Device '$deviceIdStr' moved to set '$setRef'")
      case scala.util.Failure(err) => reportError(err)

    Await.ready(f, Duration(30, SECONDS))
  }
```

(`StorageDeviceId`, `StorageDeviceSetId`, `StorageDeviceSetState`, and `resolveRef` are all already available in `Main.scala` — metadata types via the `common.metadata.*` import at line 14, `resolveRef` as a sibling method.)

- [ ] **Step 5: Compile to verify the wiring**

Run: `sbt compile`
Expected: SUCCESS (no errors).

- [ ] **Step 6: Remove the completed item from TODO.txt**

In `TODO.txt`, delete the top block (lines 2-8):

```
Add CLI command "move-device-to-set"
  - takes device UUID and device set name or UUID
    - Set must be tier 0
  - In one Transaction
    - Update device state to point to new set
    - Update new set to point to the device
    - Update the old device set to no longer point to the device
```

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala TODO.txt
git commit -m "feat: add 'move-device-to-set' CLI command

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Final verification

- [ ] **Run the full test suite**

Run: `sbt test`
Expected: PASS (no regressions).

---

## Self-Review Notes (author)

- **Spec coverage:** 3-object atomic tx (Task 1 Step 3); level-0 validation → `NotLevelZero` (Step 3 + test Step 6); no-op when already in target (Step 3 + test); device/set not-found → `NoSuchElementException` (pointer lookups + test); self-heal on missing old-set membership (`.filter` + test); CLI command with name-or-UUID set resolution and error messages (Task 2); TODO removal (Task 2 Step 6).
- **Deviation from spec (testing section):** No `MainSuite` CLI parse test — the scopt parser is not exposed for testing and the `add-group-to-group` precedent added none. CLI logic is thin and covered by `sbt compile`; all transaction logic is covered by integration tests through `client.moveDeviceToSet`.
- **Deviation from spec (API shape):** `AspenClient.moveDeviceToSet` takes a resolved `StorageDeviceSetId` (not a raw name-or-UUID string); name/UUID resolution is done in the CLI via the existing `Main.resolveRef` helper. This keeps resolution logic in the CLI layer where it already lives and avoids duplicating it in the client.
- **Type consistency:** `moveDevice(client, deviceId, targetSetId)` and `moveDeviceToSet(deviceId, targetSetId)` signatures match across definition, call sites, and tests. `NotLevelZero` referenced consistently as `StorageDeviceSetState.NotLevelZero`.
