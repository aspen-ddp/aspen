# create-allocation-group CLI Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `create-allocation-group` CLI command that creates a named allocation group at a given level, failing if the name is already in use.

**Architecture:** Pure CLI-layer addition in `Main.scala`. The client API (`AspenClient.createAllocationGroup`) already exists and enforces name-uniqueness atomically via the `Namespaces.Group` registry (a duplicate fails with `Registry.DuplicateRegistration`). The new command mirrors the existing `create-device-set` command.

**Tech Stack:** Scala 3, scopt (CLI arg parsing), sbt, Scala Futures.

**Testing note:** CLI commands in `Main.scala` have no unit-test harness; sibling commands (`create-device-set`, `transfer-store`) are verified manually against the demo cluster. This plan verifies via `sbt compile` and argument-parser behavior. No new test infrastructure is added.

---

### Task 1: Add `create-allocation-group` command to Main.scala

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
  - `Args` case class (lines 58-76)
  - Parser block (insert after `create-device-set` block, currently ending at line 238)
  - Dispatch `match` (after line 417)
  - New function (insert after `create_device_set`, currently ending at line 1005)

Reference template throughout: the existing `create-device-set` command and `create_device_set` function.

- [ ] **Step 1: Add fields to the `Args` case class**

In the `Args` case class, add two fields. Change the last field line from:

```scala
                  parentSetName:String="",
                  entityRef:String="")
```

to:

```scala
                  parentSetName:String="",
                  newGroupName:String="",
                  newGroupLevel:Int=0,
                  entityRef:String="")
```

- [ ] **Step 2: Add the parser `cmd` block**

Immediately after the `create-device-set` `cmd(...)` block (the one ending with the `[parent-set-name]` arg, closing `)` at line 238), insert:

```scala
      cmd("create-allocation-group").text("Creates a new allocation group").
        action((_, c) => c.copy(mode = "create-allocation-group")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name>").text("Name of the new allocation group").
            action((x, c) => c.copy(newGroupName = x)),

          arg[Int]("<level>").text("Hierarchy level (0 = group of pools, 1+ = group of groups)").
            action((x, c) => c.copy(newGroupLevel = x)).
            validate(x => if (x >= 0) success else failure("Level must be >= 0")),
        )
```

- [ ] **Step 3: Add the dispatch case**

In the `cfg.mode match` block, immediately after the `create-device-set` case (line 417), insert:

```scala
            case "create-allocation-group" => create_allocation_group(bootstrapConfigPath, cfg.newGroupName, cfg.newGroupLevel)
```

- [ ] **Step 4: Add the `create_allocation_group` function**

Immediately after the `create_device_set` function (closing `}` at line 1005), insert:

```scala
  def create_allocation_group(bootstrapConfigFile: os.Path,
                              name: String,
                              level: Int): Unit = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = client.createAllocationGroup(name, level)

    // Translate the known failure modes into human-readable messages. The client's retry
    // strategy unwraps StopRetrying, so the future fails with the underlying cause.
    def reportError(cause: Throwable): Unit = cause match
      case _: DuplicateRegistration =>
        println(s"Error: an allocation group named '$name' already exists")
      case e =>
        println(s"Error creating allocation group: ${e.getMessage}")

    f.onComplete:
      case scala.util.Success(groupId) =>
        println("******************************************")
        println(s"* New Allocation Group Created: ${groupId.uuid}")
        println("******************************************")
      case scala.util.Failure(err) => reportError(err)

    Await.ready(f, Duration(30, SECONDS))
  }
```

Note: `DuplicateRegistration`, `ExecutionContext`, `Await`, `Duration`, `SECONDS`, and `Future` are already imported in `Main.scala` (used by `create_device_set`). No new imports required. `AllocationGroupId` (the type returned by `createAllocationGroup`) exposes `.uuid`; it is already referenced elsewhere in `Main.scala` (e.g. line 1379).

- [ ] **Step 5: Compile**

Run: `sbt compile`
Expected: `[success]` with no errors. Watch specifically for: unresolved `DuplicateRegistration`, missing `.uuid` accessor, or scopt type-mismatch on the new `cmd` block. If `DuplicateRegistration` is unresolved, confirm the import at the top of `Main.scala` (`org.aspen_ddp.aspen.client.registries.Registry.DuplicateRegistration`).

- [ ] **Step 6: Verify argument parsing / help output**

Run: `sbt "runMain org.aspen_ddp.aspen.cmdline.Main --help"`
Expected: the command list includes `create-allocation-group` with its `<name>` and `<level>` arguments. (If `runMain` needs the class and it differs, use the project's `./t` wrapper: `./t --help`.)

- [ ] **Step 7: Verify level validation rejects a negative level**

Run: `sbt "runMain org.aspen_ddp.aspen.cmdline.Main create-allocation-group demo/bootstrap_config.yaml testgroup -1"`
Expected: parser prints `Error: Level must be >= 0` (or scopt's validation failure for the `<level>` arg) and does not attempt to contact the cluster.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "Add create-allocation-group CLI command

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Manual end-to-end verification (optional, requires a running demo cluster)

Not part of the automated flow, but the definitive check. Against a bootstrapped cluster:

```bash
./t create-allocation-group demo/bootstrap_config.yaml mygroup 0
# Expect: "* New Allocation Group Created: <uuid>"

./t list-allocation-groups demo/bootstrap_config.yaml
# Expect: mygroup listed with the same uuid

./t create-allocation-group demo/bootstrap_config.yaml mygroup 0
# Expect: "Error: an allocation group named 'mygroup' already exists"
```
