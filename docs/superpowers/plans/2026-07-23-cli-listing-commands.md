# CLI Listing Commands Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add four CLI commands — `list-pools`, `list-hosts`, `list-allocation-groups`, `list-device-sets` — that print a name + UUID listing of the named entities registered in an Aspen system.

**Architecture:** Add a prefix-scan primitive to the `Registry`/`NamespacedUUIDRegistry` layer, expose four typed listing methods on the `AspenClient` trait (implemented in `BaseAspenClient`), and wire four scopt subcommands in `cmdline/Main.scala` that call them through a single shared, generic helper. Registry keys are `"<namespace>.<name>"` and are byte-ordered, so a bounded range scan efficiently returns just one namespace's entries.

**Tech Stack:** Scala 3, ScalaTest (`AsyncFunSuite` via `IntegrationTestSuite`), scopt for CLI parsing, `TieredKeyValueList` for the underlying storage.

**Spec:** `docs/superpowers/specs/2026-07-23-cli-listing-commands-design.md`

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `src/main/scala/org/aspen_ddp/aspen/client/registries/Registry.scala` | Underlying registry; owns the private `tkvl` | Add `scan(prefix)` |
| `src/main/scala/org/aspen_ddp/aspen/client/registries/NamespacedUUIDRegistry.scala` | Namespaced name→UUID API | Add `getAllEntries(namespace)` |
| `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` | Client interface | Add four `list*` declarations |
| `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` | Client implementation | Add `Namespaces` constants, refactor existing lookups, implement four `list*` methods |
| `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` | CLI entry point | Add four subcommands, dispatch cases, `list_entries` helper |
| `src/test/scala/org/aspen_ddp/aspen/client/registries/NamespacedUUIDRegistrySuite.scala` | Registry tests | Add `getAllEntries` tests |
| `src/test/scala/org/aspen_ddp/aspen/client/ClientListingSuite.scala` | Client listing tests | New file |

---

## Task 1: Registry prefix scan + `getAllEntries`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/registries/Registry.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/registries/NamespacedUUIDRegistry.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/registries/NamespacedUUIDRegistrySuite.scala`

- [ ] **Step 1: Write the failing tests**

Add these three tests to `NamespacedUUIDRegistrySuite.scala` (the suite already defines `createRegistry()`). Insert them after the existing `atest("Duplicate namespace+name with different UUID fails")` test, before the final line:

```scala
  atest("getAllEntries returns only matching namespace, names stripped, sorted"):
    for
      registry <- createRegistry()
      poolA = UUID.randomUUID()
      poolB = UUID.randomUUID()
      host1 = UUID.randomUUID()

      // Register out of alphabetical order to prove sorting is by name.
      _ <- registry.registerObject("pool", "zebra", poolB)
      _ <- registry.registerObject("pool", "alpha", poolA)
      _ <- registry.registerObject("host", "node-a", host1)

      pools <- registry.getAllEntries("pool")
      hosts <- registry.getAllEntries("host")
    yield
      pools should be (List("alpha" -> poolA, "zebra" -> poolB))
      hosts should be (List("node-a" -> host1))

  atest("getAllEntries returns empty list for unused namespace"):
    for
      registry <- createRegistry()
      _ <- registry.registerObject("pool", "alpha", UUID.randomUUID())
      groups <- registry.getAllEntries("group")
    yield
      groups should be (Nil)

  atest("getAllEntries does not match a namespace that is a name substring"):
    for
      registry <- createRegistry()
      poolId = UUID.randomUUID()
      // A name in a different namespace whose text contains "pool" must not leak in.
      _ <- registry.registerObject("host", "pool-host", UUID.randomUUID())
      _ <- registry.registerObject("pool", "real", poolId)
      pools <- registry.getAllEntries("pool")
    yield
      pools should be (List("real" -> poolId))
```

Note: `getAllEntries` returns entries sorted by name. The suite's test tree uses `ByteArrayKeyOrdering`, so keys are byte-lexically ordered and the scan boundary trick (`.` → `/`) is valid.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *NamespacedUUIDRegistrySuite'`
Expected: FAIL — compilation error, `value getAllEntries is not a member of NamespacedUUIDRegistry`.

- [ ] **Step 3: Add `scan` to `Registry`**

In `Registry.scala`, add this method inside the `Registry` class (e.g. after `get`). `Key` and `Value` are already imported; `ExecutionContext`/`Future` are already in scope:

```scala
  def scan(prefix: String): Future[List[(Key, Value)]] =
    val minKey = Key(s"$prefix.")
    val maxKey = Key(s"$prefix/")   // '/' (0x2F) is the byte after '.' (0x2E)
    val buf = scala.collection.mutable.ListBuffer[(Key, Value)]()
    tkvl.foreachInRange(minKey, maxKey, (_, key, vs) => {
      buf += key -> vs.value
      Future.unit
    }).map(_ => buf.toList)
```

- [ ] **Step 4: Add `getAllEntries` to `NamespacedUUIDRegistry`**

In `NamespacedUUIDRegistry.scala`, add the import at the top with the other imports:

```scala
import java.nio.charset.StandardCharsets
```

Then add this method inside the `NamespacedUUIDRegistry` class (e.g. after `getRegisteredObject`):

```scala
  def getAllEntries(namespace: String): Future[List[(String, UUID)]] =
    val fullPrefix = s"$namespace."
    registry.scan(namespace).map(_.collect {
      case (key, value) =>
        val keyStr = new String(key.bytes, StandardCharsets.UTF_8)
        (keyStr, value)
    }.collect {
      case (keyStr, value) if keyStr.startsWith(fullPrefix) =>
        keyStr.substring(fullPrefix.length) -> decodeUUID(value)
    })
```

The `startsWith` filter is a correctness safety belt in case `foreachInRange`'s upper bound is inclusive of an adjacent key. Sorting is inherited from the byte-ordered TKVL scan.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *NamespacedUUIDRegistrySuite'`
Expected: PASS — all tests, including the three new ones.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/registries/Registry.scala \
        src/main/scala/org/aspen_ddp/aspen/client/registries/NamespacedUUIDRegistry.scala \
        src/test/scala/org/aspen_ddp/aspen/client/registries/NamespacedUUIDRegistrySuite.scala
git commit -m "Add prefix scan to registry: Registry.scan + NamespacedUUIDRegistry.getAllEntries

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Namespace constants in `BaseAspenClient`

This is a small refactor that removes the four magic-string literals so the register/lookup/list sides share one source of truth. No new behavior, so it is covered by re-running the existing suite.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`

- [ ] **Step 1: Add the `Namespaces` object**

`BaseAspenClient.scala` has a companion `object BaseAspenClient` (or add one if absent — check the file first; if it exists, add inside it). Add:

```scala
  private[internal] object Namespaces:
    val Pool = "pool"
    val Host = "host"
    val Group = "group"
    val DeviceSet = "device-set"
```

If there is no existing `object BaseAspenClient`, add this as a top-level `private object` in the same file instead:

```scala
private object Namespaces:
  val Pool = "pool"
  val Host = "host"
  val Group = "group"
  val DeviceSet = "device-set"
```

Use whichever placement matches the file's existing structure. Reference the values below as `Namespaces.Pool` etc. (adjust the qualifier if you nested it under `BaseAspenClient`).

- [ ] **Step 2: Replace the literals in the existing `getXxxId` lookups**

In `BaseAspenClient.scala`, replace the current bodies (around the `// ---- Id lookups ----` section):

```scala
  override def getStoragePoolId(poolName: String): Future[PoolId] =
    namespacedRegistry.getRegisteredObject(Namespaces.Pool, poolName).map(PoolId(_))

  override def getHostId(hostName: String): Future[HostId] =
    namespacedRegistry.getRegisteredObject(Namespaces.Host, hostName).map(HostId(_))

  override def getAllocationGroupId(groupName: String): Future[AllocationGroupId] =
    namespacedRegistry.getRegisteredObject(Namespaces.Group, groupName).map(AllocationGroupId(_))

  override def getStorageDeviceSetId(setName: String): Future[StorageDeviceSetId] =
    namespacedRegistry.getRegisteredObject(Namespaces.DeviceSet, setName).map(StorageDeviceSetId(_))
```

- [ ] **Step 3: Replace the literals in the `prepareRegisterObject` call sites**

There are three registration sites using string literals. Replace them:

- `namespacedRegistry.prepareRegisterObject("group", ags.name, ags.groupId.uuid)` → `namespacedRegistry.prepareRegisterObject(Namespaces.Group, ags.name, ags.groupId.uuid)`
- `namespacedRegistry.prepareRegisterObject("device-set", sds.name, sds.setId.uuid)` → `namespacedRegistry.prepareRegisterObject(Namespaces.DeviceSet, sds.name, sds.setId.uuid)`
- `namespacedRegistry.prepareRegisterObject("pool", config.name, config.poolId.uuid)` → `namespacedRegistry.prepareRegisterObject(Namespaces.Pool, config.name, config.poolId.uuid)`

(Locate them by searching the file for `prepareRegisterObject(`.)

- [ ] **Step 4: Compile and run the registry suite to confirm no regression**

Run: `sbt 'testOnly *NamespacedUUIDRegistrySuite'`
Expected: PASS — behavior unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala
git commit -m "Hoist registry namespace strings into Namespaces constants

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Client listing methods

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/ClientListingSuite.scala` (new)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/client/ClientListingSuite.scala`:

```scala
package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite

class ClientListingSuite extends IntegrationTestSuite:

  atest("listStorageDeviceSets returns created device sets by name, sorted"):
    for
      idB <- client.createStorageDeviceSet("set-b", 0, None)
      idA <- client.createStorageDeviceSet("set-a", 0, None)
      entries <- client.listStorageDeviceSets()
    yield
      // Only the two sets created here should be present; assert both are listed
      // with their typed ids, sorted by name.
      val relevant = entries.filter { (name, _) => name == "set-a" || name == "set-b" }
      relevant should be (List("set-a" -> idA, "set-b" -> idB))
```

Note: `createStorageDeviceSet(name, level, parent)` registers the set under the `device-set` namespace, so `listStorageDeviceSets` must return it. The test filters to the two names it created because the bootstrap environment may register other sets. The returned value type is `(String, StorageDeviceSetId)`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *ClientListingSuite'`
Expected: FAIL — compilation error, `value listStorageDeviceSets is not a member of AspenClient`.

- [ ] **Step 3: Add the trait declarations**

In `AspenClient.scala`, add these four declarations next to the existing `getStorageDeviceSetId` declaration (all four typed IDs — `PoolId`, `HostId`, `AllocationGroupId`, `StorageDeviceSetId` — are already imported in this file):

```scala
  def listStoragePools(): Future[List[(String, PoolId)]]
  def listHosts(): Future[List[(String, HostId)]]
  def listAllocationGroups(): Future[List[(String, AllocationGroupId)]]
  def listStorageDeviceSets(): Future[List[(String, StorageDeviceSetId)]]
```

- [ ] **Step 4: Implement in `BaseAspenClient`**

In `BaseAspenClient.scala`, add these implementations in the `// ---- Id lookups ----` region (right after the four `getXxxId` methods). An `ExecutionContext` is needed for `.map`; use `given ExecutionContext = clientContext` at the top of the region if one is not already in scope for these methods (check the surrounding methods — if they already have a `given`, reuse it):

```scala
  override def listStoragePools(): Future[List[(String, PoolId)]] =
    given ExecutionContext = clientContext
    namespacedRegistry.getAllEntries(Namespaces.Pool).map(_.map((n, u) => n -> PoolId(u)))

  override def listHosts(): Future[List[(String, HostId)]] =
    given ExecutionContext = clientContext
    namespacedRegistry.getAllEntries(Namespaces.Host).map(_.map((n, u) => n -> HostId(u)))

  override def listAllocationGroups(): Future[List[(String, AllocationGroupId)]] =
    given ExecutionContext = clientContext
    namespacedRegistry.getAllEntries(Namespaces.Group).map(_.map((n, u) => n -> AllocationGroupId(u)))

  override def listStorageDeviceSets(): Future[List[(String, StorageDeviceSetId)]] =
    given ExecutionContext = clientContext
    namespacedRegistry.getAllEntries(Namespaces.DeviceSet).map(_.map((n, u) => n -> StorageDeviceSetId(u)))
```

(If the file already declares a class-level `given ExecutionContext`, omit the per-method `given` lines.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *ClientListingSuite'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/client/ClientListingSuite.scala
git commit -m "Add client listing methods for pools, hosts, groups, device sets

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: CLI subcommands

The CLI command functions follow the existing untested-command convention (`create_device_set` and peers have no direct automated test). Verification is by compilation plus a manual smoke run.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`

- [ ] **Step 1: Add the four subcommand definitions**

In `Main.scala`, in the scopt parser block (the `OptionParser` where `cmd("create-device-set")` etc. are defined), add these four commands after the `cmd("rebalance")` block and before the `checkConfig(...)` line:

```scala
      cmd("list-pools").text("Lists all storage pools").
        action((_, c) => c.copy(mode = "list-pools")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
        )

      cmd("list-hosts").text("Lists all hosts").
        action((_, c) => c.copy(mode = "list-hosts")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
        )

      cmd("list-allocation-groups").text("Lists all allocation groups").
        action((_, c) => c.copy(mode = "list-allocation-groups")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
        )

      cmd("list-device-sets").text("Lists all storage device sets").
        action((_, c) => c.copy(mode = "list-device-sets")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
        )
```

- [ ] **Step 2: Add the dispatch cases**

In the `cfg.mode match` block (where `case "create-device-set" => ...` etc. live), add after the `case "rebalance"` line:

```scala
            case "list-pools"             => list_entries(bootstrapConfigPath, "Storage Pools",     _.listStoragePools(),      _.uuid)
            case "list-hosts"             => list_entries(bootstrapConfigPath, "Hosts",             _.listHosts(),             _.uuid)
            case "list-allocation-groups" => list_entries(bootstrapConfigPath, "Allocation Groups", _.listAllocationGroups(),  _.uuid)
            case "list-device-sets"       => list_entries(bootstrapConfigPath, "Device Sets",       _.listStorageDeviceSets(), _.uuid)
```

- [ ] **Step 3: Add the `list_entries` helper**

Add this method alongside the other command functions (e.g. right after `create_device_set`). `File`, `UUID`, `Await`, `Duration`, `SECONDS`, `ExecutionContext`, and `Future` are already imported:

```scala
  def list_entries[A](bootstrapConfigFile: os.Path,
                      title: String,
                      fetch: AspenClient => Future[List[(String, A)]],
                      idToUuid: A => UUID): Unit =

    configureLogging()

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = fetch(client)

    f.onComplete:
      case scala.util.Success(entries) =>
        if entries.isEmpty then
          println(s"No $title found")
        else
          // Sort by name explicitly for a deterministic listing order, independent
          // of the underlying registry's iteration order.
          val sorted = entries.sortBy(_._1)
          val width = sorted.map(_._1.length).max
          println(title)
          sorted.foreach { (name, id) => println(s"  ${name.padTo(width, ' ')}  ${idToUuid(id)}") }
      case scala.util.Failure(err) =>
        println(s"Error listing ${title.toLowerCase}: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))
```

- [ ] **Step 4: Compile**

Run: `sbt compile`
Expected: SUCCESS — no errors.

- [ ] **Step 5: Full test run**

Run: `sbt test`
Expected: PASS — all suites green (no existing behavior changed).

- [ ] **Step 6: Manual smoke check (optional but recommended)**

With a running demo cluster (see `TODO.txt` "Testing commands"), the commands are available via the `./t` launcher, e.g.:

```
./t list-device-sets demo/bootstrap_config.yaml
./t list-pools demo/bootstrap_config.yaml
```

Expected: a `Device Sets` / `Storage Pools` header followed by name-padded `name  uuid` rows (or `No <title> found` when empty).

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "Add list-pools, list-hosts, list-allocation-groups, list-device-sets CLI commands

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Update TODO.txt

**Files:**
- Modify: `TODO.txt`

- [ ] **Step 1: Remove the completed item**

Delete the "Add CLI listing commands" block (the `list-device-sets`/`list-pools`/`list-hosts`/`list-allocation-groups` entry and its `NamespacedUUIDRegistry.getAllEntries` sub-bullet) from the top of `TODO.txt`, matching the project's convention of pruning completed work (see commit `958f229 "Removed completed work from TODO.txt"`).

- [ ] **Step 2: Commit**

```bash
git add TODO.txt
git commit -m "Remove completed CLI listing commands from TODO.txt

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** `getAllEntries` (Task 1), typed-ID client methods + namespace constants (Tasks 2–3), four CLI commands + generic helper with explicit sort (Task 4), unit test for `getAllEntries` (Task 1) and client-layer coverage (Task 3) — all spec sections mapped. `list-devices` and `show-*` are explicitly out of scope per the spec.
- **Type consistency:** `getAllEntries(namespace: String): Future[List[(String, UUID)]]` (registry) → wrapped to `Future[List[(String, XId)]]` (client) → consumed by `list_entries[A](..., idToUuid: A => UUID)` with `_.uuid`. Namespace values referenced as `Namespaces.Pool/Host/Group/DeviceSet` consistently in Tasks 2 and 3.
- **Boundary safety:** the `startsWith(s"$namespace.")` filter in `getAllEntries` guards against `foreachInRange` upper-bound inclusivity; a dedicated test (Task 1, Step 1) covers the substring-collision case.
