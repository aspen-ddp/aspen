# list-devices CLI Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `list-devices` CLI command that, given a hostname, lists each of that host's storage devices with its UUID, device-set name, total capacity (human-readable), and percent utilization.

**Architecture:** All changes live in `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, following the pattern of the existing `list-*` commands. A dedicated `list_devices` function resolves hostname → `HostId` → `HostState` → per-device `StorageDeviceState` (+ set-name lookups) via `AspenClient`, then prints a four-column table. A small `formatBytes` helper (the only non-trivial pure logic) is unit-tested; the CLI wiring is verified by compilation and manual run.

**Tech Stack:** Scala 3, scopt (CLI parsing), scalatest (`AnyFunSuite` + `Matchers`), `AspenClient` async API (`scala.concurrent.Future`).

**Reference spec:** `docs/superpowers/specs/2026-07-23-list-devices-cli-design.md`

---

## File Structure

- **Modify:** `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
  - Add `private[cmdline] def formatBytes(n: Long): String` helper.
  - Add `cmd("list-devices")` block to the scopt parser.
  - Add `case "list-devices" => ...` to the `cfg.mode match` dispatch.
  - Add `def list_devices(bootstrapConfigFile: os.Path, hostname: String): Unit`.
- **Create:** `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`
  - Unit tests for `formatBytes`.

No other files change. No wire-protocol / codec changes.

---

## Task 1: `formatBytes` helper (TDD)

Formats a byte count as a human-readable binary-unit string: integer `B` below 1024, otherwise one decimal place with the largest fitting unit (`KiB`/`MiB`/`GiB`/`TiB`/`PiB`).

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (add helper inside `object Main`)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala` with exactly this content:

```scala
package org.aspen_ddp.aspen.cmdline

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MainSuite extends AnyFunSuite with Matchers:

  test("formatBytes renders sub-KiB values as integer bytes"):
    Main.formatBytes(0L) shouldBe "0 B"
    Main.formatBytes(512L) shouldBe "512 B"
    Main.formatBytes(1023L) shouldBe "1023 B"

  test("formatBytes renders KiB with one decimal"):
    Main.formatBytes(1024L) shouldBe "1.0 KiB"
    Main.formatBytes(1536L) shouldBe "1.5 KiB"

  test("formatBytes selects the largest fitting binary unit"):
    // 1.5 * 1024^4
    Main.formatBytes(1649267441664L) shouldBe "1.5 TiB"
    // 10 * 1024^4
    Main.formatBytes(10995116277760L) shouldBe "10.0 TiB"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: compilation failure — `formatBytes` is not a member of `object Main` (or method not found).

- [ ] **Step 3: Write minimal implementation**

In `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, add this method inside `object Main` (place it just above the existing `list_entries` method near the end of the object):

```scala
  /** Format a byte count using binary units (powers of 1024). Sub-KiB values are
   *  rendered as whole bytes; larger values use one decimal place and the largest
   *  unit that keeps the value >= 1.0. */
  private[cmdline] def formatBytes(n: Long): String =
    val units = Array("KiB", "MiB", "GiB", "TiB", "PiB")
    if n < 1024L then
      s"$n B"
    else
      var value = n.toDouble / 1024.0
      var idx = 0
      while value >= 1024.0 && idx < units.length - 1 do
        value /= 1024.0
        idx += 1
      f"$value%.1f ${units(idx)}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: PASS (3 tests succeed).

- [ ] **Step 5: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala \
        src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "Add formatBytes helper for human-readable byte sizes"
```

---

## Task 2: `list-devices` command wiring and implementation

Adds the scopt command, the dispatch case, and the `list_devices` function. This is CLI glue over already-tested client methods, so it is verified by compilation and manual run rather than a unit test (matching the existing `list-*` commands).

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
  - parser (after the `list-device-sets` `cmd` block, ~line 313)
  - dispatch (in the `cfg.mode match`, after the `list-device-sets` case, ~line 342)
  - new `list_devices` function (near the other command functions)

- [ ] **Step 1: Add the scopt command block**

In `main()`, immediately after the existing `cmd("list-device-sets")...` block (the one that ends just before `checkConfig(...)`), insert:

```scala
      cmd("list-devices").text("Lists all storage devices for a host").
        action((_, c) => c.copy(mode = "list-devices")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<hostname>").text("Name of the host whose devices to list").
            action((x, c) => c.copy(hostName = x)),
        )
```

- [ ] **Step 2: Add the dispatch case**

In the `cfg.mode match` block, immediately after the line:

```scala
            case "list-device-sets"       => list_entries(bootstrapConfigPath, "Device Sets",       _.listStorageDeviceSets(), _.uuid)
```

add:

```scala
            case "list-devices"           => list_devices(bootstrapConfigPath, cfg.hostName)
```

- [ ] **Step 3: Add the `list_devices` function**

Add this function inside `object Main`, immediately before the `list_entries` method:

```scala
  def list_devices(bootstrapConfigFile: os.Path, hostname: String): Unit =

    configureLogging()

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    // Resolve hostname -> HostId -> HostState, then fetch each device's state and
    // resolve the (deduplicated) set ids to names for display.
    val f = for
      hostId    <- client.getHostId(hostname)
      hostState <- client.getHostState(hostId)
      devStates <- Future.sequence(hostState.storageDevices.toList.map(client.getStorageDeviceState))
      setIds     = devStates.map(_.storageDeviceSet).distinct
      setStates <- Future.sequence(setIds.map(client.getStorageDeviceSetState))
      setNameMap = setStates.map(s => s.setId -> s.name).toMap
    yield
      devStates.map { ds =>
        val setName  = setNameMap.getOrElse(ds.storageDeviceSet, ds.storageDeviceSet.uuid.toString)
        val capacity = formatBytes(ds.totalSize)
        val pct      = if ds.totalSize > 0 then ds.currentUsage.toDouble / ds.totalSize * 100.0 else 0.0
        (ds.storageDeviceId.uuid.toString, setName, capacity, pct)
      }.sortBy(_._1)

    f.onComplete:
      case scala.util.Success(devices) =>
        if devices.isEmpty then
          println(s"No devices found for host '$hostname'")
        else
          val setWidth = devices.map(_._2.length).max
          val capWidth = devices.map(_._3.length).max
          println(s"Devices for host '$hostname'")
          devices.foreach { (uuid, setName, capacity, pct) =>
            val paddedCap = " " * (capWidth - capacity.length) + capacity
            println(f"  $uuid  ${setName.padTo(setWidth, ' ')}  $paddedCap  $pct%5.1f%%")
          }
      case scala.util.Failure(_: NoSuchElementException) =>
        println(s"Error: host '$hostname' not found")
      case scala.util.Failure(err) =>
        println(s"Error listing devices: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))
```

- [ ] **Step 4: Compile**

Run: `sbt compile`
Expected: `success` — no errors. (Confirms parser block, dispatch case, and function all type-check, and that `list-devices` is now exhaustively handled in the `cfg.mode match`.)

- [ ] **Step 5: Run the full test suite for the command's helper**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: PASS (the `formatBytes` tests from Task 1 still pass).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "Add list-devices CLI command"
```

---

## Task 3: Manual verification (optional, requires a running demo cluster)

This step confirms real behavior end-to-end. Skip if no demo cluster is available; the command is otherwise verified by compilation.

- [ ] **Step 1: Bring up the demo cluster** (per `TODO.txt` testing notes)

```bash
./t bootstrap demo/bootstrap_config.yaml
./t node demo/bootstrap_config.yaml demo/node_a.yaml
```

- [ ] **Step 2: List devices for a known host**

Run: `./t list-devices demo/bootstrap_config.yaml bootstrap-host`
Expected: a `Devices for host 'bootstrap-host'` header followed by one row per device, e.g.:

```
Devices for host 'bootstrap-host'
  <uuid>  <set-name>   0 B    0.0%
```

(Freshly bootstrapped devices report `totalSize == 0`, so capacity shows `0 B` and utilization `0.0%` — this exercises the `totalSize == 0` guard.)

- [ ] **Step 3: Verify the not-found path**

Run: `./t list-devices demo/bootstrap_config.yaml no-such-host`
Expected: `Error: host 'no-such-host' not found`

---

## Self-Review

**Spec coverage:**
- "Requires a hostname argument" → Task 2 Step 1 (`<hostname>` arg) + Step 3 (`getHostId`).
- "Lists each device UUID" → Task 2 Step 3 (`ds.storageDeviceId.uuid.toString`, sorted).
- "the device set name it belongs to" → Task 2 Step 3 (`setNameMap`, UUID fallback).
- "total capacity" (human-readable) → Task 1 (`formatBytes`) + Task 2 Step 3.
- "percent utilization" → Task 2 Step 3 (`currentUsage / totalSize * 100`, zero-guarded).
- Error handling (host not found / generic) → Task 2 Step 3 `onComplete`.
- Testing stance (thin wrapper + `formatBytes` unit test + manual) → Tasks 1 and 3.

**Placeholder scan:** No TBD/TODO/"handle edge cases"; all code shown in full.

**Type/name consistency:** `formatBytes(n: Long): String` defined in Task 1, called in Task 2 Step 3 — same signature. Client methods (`getHostId`, `getHostState`, `getStorageDeviceState`, `getStorageDeviceSetState`) and field names (`storageDeviceId`, `currentUsage`, `totalSize`, `storageDeviceSet`, `setId`, `name`, `storageDevices`) match the verified source. `Args.hostName` reused (already exists). Dispatch string `"list-devices"` matches the `mode` set in the parser action.
