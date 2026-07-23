# create-device-set CLI command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `create-device-set` command-line command that creates a new storage device set, and fix the missing parent-level validation in the client method it relies on.

**Architecture:** Reuse the existing `AspenClient.createStorageDeviceSet(name, level, parent)` (implemented in `BaseAspenClient`), which already allocates the state object, inserts it into the device-sets TKVL tree, registers the name in the namespaced registry, and rejects duplicate names. Two changes: (1) fix `createStorageDeviceSet` to enforce the `child.level < parent.level` hierarchy invariant, mirroring the existing `AllocationGroupState.addGroup` / `InvalidLevel` convention; (2) add the `create-device-set` CLI command in `Main.scala`, modeled on the existing `create-pool` command.

**Tech Stack:** Scala 3, ScalaTest `AsyncFunSuite` (integration tests via `IntegrationTestSuite`/`TestNetwork`), scopt-style CLI parser in `Main.scala`, sbt.

---

## File Structure

- **Modify** `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` — add the `InvalidDeviceSetLevel` exception to the `AspenClient` companion object (alongside `InvalidDestination`).
- **Modify** `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` — enforce the parent-level invariant inside `createStorageDeviceSet`.
- **Modify** `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala` — add a test that an invalid parent level is rejected.
- **Modify** `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — add `Args` fields, the `create-device-set` command registration, the dispatch case, and the `create_device_set` handler.

---

## Task 1: Enforce parent-level invariant in `createStorageDeviceSet`

**Files:**
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:23-25`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala:166-203`

- [ ] **Step 1: Write the failing test**

Add this import near the other imports at the top of `StorageDeviceSetIntegrationSuite.scala` (after line 4, `import org.aspen_ddp.aspen.client.internal.MetadataTree`):

```scala
import org.aspen_ddp.aspen.client.AspenClient
```

Then add this test at the end of the class (after the existing `atest("createStorageDeviceSet links the new set into its parent")` block, which ends at line 81):

```scala
  atest("createStorageDeviceSet rejects a parent at an equal or lower level"):
    given ExecutionContext = executionContext
    for
      parentId <- client.createStorageDeviceSet("bad-parent", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- recoverToSucceededIf[AspenClient.InvalidDeviceSetLevel](
             client.createStorageDeviceSet("bad-child", level = 0, parent = Some(parentId))
           )
    yield succeed
```

Rationale: `bad-child` has `level == 0` and its parent `bad-parent` also has `level == 0`, so `child.level >= parent.level` and creation must be rejected. `recoverToSucceededIf` is inherited from ScalaTest's async suite (the same helper `AllocationGroupStateSuite` uses).

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite -- -z "equal or lower level"'`

Expected: FAIL — the test will not compile yet (`AspenClient.InvalidDeviceSetLevel` does not exist). If it is made to compile without the validation, it fails because the second `createStorageDeviceSet` succeeds instead of raising `InvalidDeviceSetLevel`.

- [ ] **Step 3: Add the `InvalidDeviceSetLevel` exception**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, extend the companion object (currently lines 23-25):

```scala
object AspenClient:
  class InvalidDestination extends Exception("Source and destination devices must be different")
  class StoreNotActive(storeId: StoreId) extends Exception(s"Store $storeId is not in the Active state")
  class InvalidDeviceSetLevel(childLevel: Int, parentLevel: Int)
    extends Exception(s"Device set level $childLevel must be less than parent level $parentLevel")
```

- [ ] **Step 4: Enforce the invariant in `createStorageDeviceSet`**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`, update the `addToParent` helper inside `createStorageDeviceSet` (currently lines 179-186) to check the level before linking:

```scala
    def addToParent(parentId: StorageDeviceSetId)(using tx: Transaction): Future[Unit] =
      for
        parentPtr <- getStorageDeviceSetPointer(parentId)
        parentDos <- read(parentPtr)
      yield
        val parentState = StorageDeviceSetState(parentDos)
        if sds.level >= parentState.level then
          throw AspenClient.InvalidDeviceSetLevel(sds.level, parentState.level)
        val updated = parentState.copy(memberSets = sds.setId :: parentState.memberSets)
        tx.overwrite(parentPtr, parentDos.revision, DataBuffer(updated.toBytes))
```

Then add the new exception to the method's `onFail` recovery handler (currently lines 188-190) so an invalid request is not retried:

```scala
    def onFail(err: Throwable): Future[Unit] = err match
      case e: DuplicateRegistration => throw StopRetrying(e)
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: AspenClient.InvalidDeviceSetLevel => throw StopRetrying(e)
```

Note: `InvalidDeviceSetLevel` lives in the `AspenClient` companion object; reference it qualified as `AspenClient.InvalidDeviceSetLevel` (BaseAspenClient imports `org.aspen_ddp.aspen.client.*` but does not `import AspenClient.*`). `StopRetrying` unwraps to its inner cause when the retry loop stops, so the test observes `InvalidDeviceSetLevel` — consistent with how `AllocationGroupState.InvalidLevel` surfaces in `AllocationGroupStateSuite`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite -- -z "equal or lower level"'`

Expected: PASS.

- [ ] **Step 6: Run the full suite to confirm no regressions**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite'`

Expected: PASS — all existing tests (including "createStorageDeviceSet links the new set into its parent", which uses `level = 0` child under a `level = 1` parent) still pass.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala
git commit -m "Enforce parent-level invariant in createStorageDeviceSet

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Add the `create-device-set` CLI command

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (imports, `Args` case class ~line 56-69, command registration ~line 208, dispatch ~line 281, new handler ~line 811)

CLI handler functions in this codebase are thin and are not unit-tested (see `create_pool`, `transfer_store`, `rebalance`); this task is verified by a clean compile and by exercising the parser's `--help`.

- [ ] **Step 1: Add the duplicate-registration import**

In `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, add this import alongside the other `org.aspen_ddp.aspen.client` imports (e.g. after `import org.aspen_ddp.aspen.client.tkvl.KeyValueListNode`):

```scala
import org.aspen_ddp.aspen.client.registries.Registry.DuplicateRegistration
```

(`StopRetrying`, `AspenClient`, `StorageDeviceSetId` are already in scope via the existing `import org.aspen_ddp.aspen.client.*` and `import org.aspen_ddp.aspen.common.metadata.*` wildcards.)

- [ ] **Step 2: Add fields to the `Args` case class**

Change the end of the `Args` case class (currently ends `setId:String="")` at line 69) to add three fields:

```scala
                  setId:String="",
                  newSetName:String="",
                  newSetLevel:Int=0,
                  parentSetName:String="")
```

- [ ] **Step 3: Register the `create-device-set` command**

In the `parser` block, insert this command immediately after the `create-pool` command's `children(...)` closes (after line 208, before `cmd("transfer-store")`):

```scala
      cmd("create-device-set").text("Creates a new storage device set").
        action((_, c) => c.copy(mode = "create-device-set")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name>").text("Name of the new storage device set").
            action((x, c) => c.copy(newSetName = x)),

          arg[Int]("<level>").text("Hierarchy level (0 = set of physical devices, 1+ = set of sets)").
            action((x, c) => c.copy(newSetLevel = x)).
            validate(x => if (x >= 0) success else failure("Level must be >= 0")),

          arg[String]("[parent-set-name]").optional().text("Optional name of the parent device set to link into").
            action((x, c) => c.copy(parentSetName = x)),
        )
```

- [ ] **Step 4: Add the dispatch case**

In the `cfg.mode match` block, add this case after the `create-pool` case (line 281):

```scala
            case "create-device-set" => create_device_set(bootstrapConfigPath, cfg.newSetName, cfg.newSetLevel, cfg.parentSetName)
```

- [ ] **Step 5: Add the `create_device_set` handler**

Add this method immediately after the `create_pool` method (after its closing brace at line 811, before `def transfer_store`):

```scala
  def create_device_set(bootstrapConfigFile: os.Path,
                        name: String,
                        level: Int,
                        parentSetName: String): Unit = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    // Resolve the optional parent set name to an id before creating the new set. An empty
    // parentSetName means "no parent" (a top-level set).
    val fParent: Future[Option[StorageDeviceSetId]] =
      if parentSetName.isEmpty then
        Future.successful(None)
      else
        client.getStorageDeviceSetId(parentSetName).map(Some(_))

    val f = for
      parentOpt <- fParent
      setId <- client.createStorageDeviceSet(name, level, parentOpt)
    yield setId

    // Translate the known failure modes into human-readable messages. The client unwraps
    // StopRetrying to its underlying cause, but we match StopRetrying defensively as well.
    def reportError(cause: Throwable): Unit = cause match
      case _: DuplicateRegistration =>
        println(s"Error: a device set named '$name' already exists")
      case _: NoSuchElementException =>
        println(s"Error: parent device set '$parentSetName' not found")
      case e: AspenClient.InvalidDeviceSetLevel =>
        println(s"Error: ${e.getMessage}")
      case e =>
        println(s"Error creating device set: ${e.getMessage}")

    f.onComplete:
      case scala.util.Success(setId) =>
        println("******************************************")
        println(s"* New Device Set Created: ${setId.uuid}")
        println("******************************************")
      case scala.util.Failure(StopRetrying(cause)) => reportError(cause)
      case scala.util.Failure(err) => reportError(err)

    Await.ready(f, Duration(30, SECONDS))
  }
```

(`Await`, `Duration`, `SECONDS`, `Future`, `ExecutionContext` are already imported/used by the neighboring handlers such as `create_pool`.)

- [ ] **Step 6: Compile**

Run: `sbt compile`

Expected: SUCCESS with no errors.

- [ ] **Step 7: Verify the command is wired into the parser**

Run: `sbt 'runMain org.aspen_ddp.aspen.cmdline.Main create-device-set --help'`

Expected: usage output listing the `<bootstrap-config-file>`, `<name>`, `<level>`, and optional `[parent-set-name]` arguments. (Exit/usage text is produced by the parser without contacting a live cluster.)

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "Add create-device-set CLI command

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full metadata test package**

Run: `sbt 'testOnly org.aspen_ddp.aspen.common.metadata.*'`

Expected: PASS — confirms the client change did not regress device-set, allocation-group, or codec suites.

- [ ] **Step 2: Confirm a clean build**

Run: `sbt compile`

Expected: SUCCESS.

---

## Self-Review

**Spec coverage:**
- "CLI command create-device-set, takes name and level" → Task 2 (Steps 3-5), with `<name>` and `<level>` args.
- "fail if name already in use" → reused `createStorageDeviceSet` (registry `DoesNotExist` guard) + friendly CLI message in Task 2 Step 5; existing behavior preserved (Task 1 Step 6).
- "Reuse existing method (no new createNewDeviceSet)" → Task 2 handler calls `client.createStorageDeviceSet` directly; no new client method added.
- "Optional --parent-set-name (positional)" → Task 2 Step 3 optional `[parent-set-name]` arg; resolved in Task 2 Step 5.
- "Parent-level validation belongs in createStorageDeviceSet (bug fix)" → Task 1 (Steps 3-4).
- Testing (spec): reject `child.level >= parent.level` → Task 1 Step 1; valid child links into parent → existing test preserved (Task 1 Step 6); duplicate name rejected → existing behavior, covered by unchanged registry guard.

**Placeholder scan:** No TBD/TODO/"add error handling" placeholders; every code step shows complete code.

**Type/name consistency:** `InvalidDeviceSetLevel(childLevel: Int, parentLevel: Int)` defined in Task 1 Step 3 and referenced as `AspenClient.InvalidDeviceSetLevel` in Task 1 Steps 1/4 and Task 2 Steps 1/5. `Args` fields `newSetName`/`newSetLevel`/`parentSetName` defined in Task 2 Step 2 and used consistently in Steps 3-4. Handler signature `create_device_set(os.Path, String, Int, String)` matches the dispatch call in Task 2 Step 4.
