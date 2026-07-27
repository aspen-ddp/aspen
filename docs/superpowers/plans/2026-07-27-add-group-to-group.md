# add-group-to-group Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a CLI command `add-group-to-group` that nests one allocation group inside another, backed by a new `AspenClient.addGroupToGroup` convenience method.

**Architecture:** The core transaction logic already exists in `AllocationGroupState.addGroup` (atomically updates the child group's `parentGroups` and the parent group's `members`, enforces the level invariant, and enrolls the usage-cascade task). This plan adds a thin `AspenClient.addGroupToGroup` method that resolves two group names to ids and delegates to `addGroup`, plus CLI wiring in `Main.scala` that mirrors the existing `add-pool-to-group` command. No protobuf/codec changes are needed.

**Tech Stack:** Scala 3, ScalaTest (async integration suites via `IntegrationTestSuite`), scopt-style CLI parser in `Main.scala`, sbt.

**Spec:** `docs/superpowers/specs/2026-07-27-add-group-to-group-design.md`

---

## Background facts (verified against the codebase)

- `AllocationGroupState.addGroup(client, childId, parentId, taskExecutor)` exists at
  `src/main/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupState.scala:140`.
  - Signature: `def addGroup(client: AspenClient, childId: AllocationGroupId, parentId: AllocationGroupId, taskExecutor: Option[TaskExecutor] = None): Future[Unit]`
  - `child` = source (lower level), `parent` = destination (higher level).
  - Throws `AllocationGroupState.InvalidLevel` when `child.level >= parent.level`
    (`AllocationGroupState.scala:153`). `InvalidLevel` is `class InvalidLevel extends Throwable`
    at `AllocationGroupState.scala:30`. It is wrapped in `StopRetrying` inside `modifyGroup`'s
    `onFail` (line 221), so it surfaces directly on the returned `Future`.
  - Idempotent: no-ops when membership already exists (`AllocationGroupState.scala:149`).
- `AspenClient.addPoolToGroup` (the template) is at
  `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:123`.
- `getAllocationGroupId(groupName: String): Future[AllocationGroupId]` exists at
  `AspenClient.scala:100` and throws `NoSuchElementException` for unknown names.
- CLI template `add_pool_to_group` is at `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
  (parser cmd, dispatch case, handler function). The `Args` case class holds `poolName` and
  `newGroupName` fields already.
- `Main.scala:14` imports `org.aspen_ddp.aspen.common.metadata.*`, so `AllocationGroupState`
  and `AllocationGroupState.InvalidLevel` are already in scope — **no new import required**.
- Test suite `src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala`
  extends `IntegrationTestSuite`, uses `atest(...)` (async), a `client` fixture,
  `waitForTransactionsToComplete()`, and a private helper
  `readGroupState(groupId): Future[AllocationGroupState]` (line 41). `AllocationGroupState.addGroup`
  already has coverage there (system-task-path test near line 337), so this plan only adds
  **client-level `addGroupToGroup`** coverage.

---

## File structure

- **Modify** `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` — add `addGroupToGroup`
  method to the `AspenClient` trait (concrete default body, next to `addPoolToGroup`).
- **Modify** `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — add `srcGroupName` to `Args`,
  register the `add-group-to-group` command in the parser, add the dispatch case, add the
  `add_group_to_group` handler function.
- **Modify** `src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala` —
  add a happy-path test and a level-violation test for `addGroupToGroup`.

---

## Task 1: Add `AspenClient.addGroupToGroup` (client method + tests, TDD)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (add method after `addPoolToGroup`, currently ending at line 129)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala` (add tests after the `addPoolToGroup` test, currently ending near line 374)

- [ ] **Step 1: Write the failing happy-path test**

Append this test to `AllocationGroupStateSuite.scala` (after the `"addPoolToGroup resolves names and adds the pool"` test):

```scala
  atest("addGroupToGroup resolves names and nests the source group"):
    given ExecutionContext = executionContext
    for
      childId <- client.createAllocationGroup("child-named", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-named", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- client.addGroupToGroup("child-named", "parent-named")
      _ <- waitForTransactionsToComplete()

      childState <- readGroupState(childId)
      parentState <- readGroupState(parentId)
    yield
      childState.parentGroups.exists(_.uuid == parentId.uuid) should be(true)
      parentState.members.exists(_.uuid == childId.uuid) should be(true)
```

- [ ] **Step 2: Write the failing level-violation test**

Append this test immediately after the previous one:

```scala
  atest("addGroupToGroup fails with InvalidLevel when source level is not lower"):
    given ExecutionContext = executionContext
    for
      _ <- client.createAllocationGroup("src-same", level = 1)
      _ <- waitForTransactionsToComplete()
      _ <- client.createAllocationGroup("dst-same", level = 1)
      _ <- waitForTransactionsToComplete()

      err <- client.addGroupToGroup("src-same", "dst-same").failed
    yield
      err shouldBe a[AllocationGroupState.InvalidLevel]
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `sbt 'testOnly *AllocationGroupStateSuite -- -z addGroupToGroup'`
Expected: FAIL — compilation error, `value addGroupToGroup is not a member of ... AspenClient` (the method does not exist yet).

- [ ] **Step 4: Implement `addGroupToGroup`**

In `AspenClient.scala`, add this method directly after `addPoolToGroup` (after line 129, before `def transact`):

```scala
  /** Nest a source allocation group inside a destination allocation group, both
   *  identified by name. The destination group's level must be strictly greater than
   *  the source group's level (enforced by AllocationGroupState.addGroup, which throws
   *  AllocationGroupState.InvalidLevel otherwise). Fails with NoSuchElementException if
   *  either name is not registered. Uses the system durable task path for any usage
   *  cascade (no local TaskExecutor). */
  def addGroupToGroup(sourceGroupName: String, destGroupName: String): Future[Unit] =
    given ExecutionContext = this.clientContext
    for
      childId  <- getAllocationGroupId(sourceGroupName)
      parentId <- getAllocationGroupId(destGroupName)
      _        <- AllocationGroupState.addGroup(this, childId, parentId, None)
    yield ()
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *AllocationGroupStateSuite -- -z addGroupToGroup'`
Expected: PASS — both `addGroupToGroup` tests green.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala
git commit -m "feat: add AspenClient.addGroupToGroup

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Add `add-group-to-group` CLI command

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (`Args` case class, parser, dispatch `match`, and a new handler function)

There is no unit test for the CLI layer (consistent with `add-pool-to-group`, which shipped without one). Verification is by successful compilation. The command's behavior is exercised through the `AspenClient.addGroupToGroup` tests from Task 1.

- [ ] **Step 1: Add the `srcGroupName` field to `Args`**

In the `Args` case class (the block containing `poolName:String="",` and `newGroupName:String="",`), add a `srcGroupName` field. Locate:

```scala
                  newGroupName:String="",
```

and add the new field right after `poolName`:

```scala
                  poolName:String="",
                  srcGroupName:String="",
                  newGroupName:String="",
```

(Adjust to match the exact surrounding lines; the goal is a new `srcGroupName:String=""` field in `Args`.)

- [ ] **Step 2: Register the `add-group-to-group` command in the parser**

Directly after the existing `cmd("add-pool-to-group")....children(...)` block, add:

```scala
      cmd("add-group-to-group").text("Nests one allocation group inside another").
        action((_, c) => c.copy(mode = "add-group-to-group")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<source-group-name>").text("Name of the source (lower-level) allocation group").
            action((x, c) => c.copy(srcGroupName = x)),

          arg[String]("<destination-group-name>").text("Name of the destination allocation group (level must be strictly greater than the source)").
            action((x, c) => c.copy(newGroupName = x)),
        )
```

- [ ] **Step 3: Add the dispatch case**

In the `mode` dispatch `match` block, directly after the line:

```scala
            case "add-pool-to-group" => add_pool_to_group(bootstrapConfigPath, cfg.poolName, cfg.newGroupName)
```

add:

```scala
            case "add-group-to-group" => add_group_to_group(bootstrapConfigPath, cfg.srcGroupName, cfg.newGroupName)
```

- [ ] **Step 4: Add the `add_group_to_group` handler function**

Directly after the existing `add_pool_to_group` handler function (it ends with `Await.ready(f, Duration(30, SECONDS))` followed by `}`), add:

```scala
  def add_group_to_group(bootstrapConfigFile: os.Path,
                         sourceGroupName: String,
                         destGroupName: String): Unit = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = client.addGroupToGroup(sourceGroupName, destGroupName)

    // getAllocationGroupId throws NoSuchElementException when a name is not registered;
    // addGroup throws AllocationGroupState.InvalidLevel when the destination level is not
    // strictly greater than the source level. Translate both into precise messages.
    def reportError(cause: Throwable): Unit = cause match
      case _: NoSuchElementException =>
        println(s"Error: allocation group '$sourceGroupName' or '$destGroupName' not found")
      case _: AllocationGroupState.InvalidLevel =>
        println(s"Error: destination group '$destGroupName' must have a higher level than source group '$sourceGroupName'")
      case e =>
        println(s"Error adding group to group: ${e.getMessage}")

    f.onComplete:
      case scala.util.Success(_) =>
        println(s"Allocation group '$sourceGroupName' added to allocation group '$destGroupName'")
      case scala.util.Failure(err) => reportError(err)

    Await.ready(f, Duration(30, SECONDS))
  }
```

- [ ] **Step 5: Compile to verify the wiring**

Run: `sbt compile`
Expected: SUCCESS — no errors. (`AllocationGroupState` is already in scope via the
`org.aspen_ddp.aspen.common.metadata.*` import at `Main.scala:14`.)

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "feat: add 'add-group-to-group' CLI command

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Full test run and TODO cleanup

**Files:**
- Modify: `TODO.txt` (remove the completed `add-group-to-group` item)

- [ ] **Step 1: Run the full test suite**

Run: `sbt test`
Expected: PASS — all suites green, including the new `AllocationGroupStateSuite` tests.

If anything fails, fix it before proceeding (do not mark this task complete with failing tests).

- [ ] **Step 2: Remove the completed item from `TODO.txt`**

Delete the top entry in `TODO.txt`:

```
Add CLI command "add-group-to-group"
  - Takes source and destination group names/uuids
  - Destination group level must be greater than the source group's level
  - Updates the group state object accordingly
```

(Delete these lines and the trailing blank line, matching how the `add-pool-to-group` item was
removed in commit `1a662f5`.)

- [ ] **Step 3: Commit**

```bash
git add TODO.txt
git commit -m "chore: remove completed add-group-to-group item from TODO

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-review notes

- **Spec coverage:** CLI command (Task 2), `AspenClient.addGroupToGroup` name-only resolution
  (Task 1), strict level invariant surfaced as a clear CLI error (Task 2 handler + Task 1
  violation test), single-transaction state update (delegated to existing `addGroup`), happy-path
  + level-violation tests (Task 1). Out-of-scope items (UUID args, remove op, codec changes) are
  intentionally excluded.
- **Type consistency:** `addGroupToGroup(sourceGroupName, destGroupName)` is used identically in
  the trait method, the CLI handler, and the tests. `AllocationGroupState.InvalidLevel` and
  `getAllocationGroupId` / `AllocationGroupState.addGroup` match their verified source signatures.
- **No placeholders:** every step contains concrete code or an exact command with expected output.
