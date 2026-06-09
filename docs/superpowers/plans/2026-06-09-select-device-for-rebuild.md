# selectDeviceForRebuild Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `StorageDeviceSetState.selectDeviceForRebuild`, which picks another device in a level-0 set to host a store rebuilt from scratch, requiring sufficient free space.

**Architecture:** A `private[metadata]` testable core (`selectRebuildDevice`) does the optimistic, lazy prefer-then-fallback scan over member devices using an injected `freeSpaceLookup`. A thin public method (`selectDeviceForRebuild`) does the client I/O: it reads the pool object's pointer directly to obtain both the config and the per-store size, then delegates to the core. This mirrors the existing `selectDevicesForPool` / `selectDevices` public-method + testable-core split.

**Tech Stack:** Scala 3, ScalaTest (`AnyFunSuite` + `Matchers`), `scala.concurrent.Future`.

**Spec:** `docs/superpowers/specs/2026-06-09-select-device-for-rebuild-design.md`

---

## File Structure

- **Modify** `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`
  - Add `import org.aspen_ddp.aspen.common.util.byte2long` and `import scala.util.Success`.
  - Add `private[metadata] def selectRebuildDevice(...)` (core, Task 1).
  - Add `def selectDeviceForRebuild(...)` (public I/O wrapper, Task 2).
- **Modify** `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala`
  - Add a `fixedLookup` helper and rebuild test cases (Task 1).

No new files. `StoragePoolState` and `StorageDeviceState` are in the same package (`common.metadata`), so they need no import.

---

## Task 1: Testable core `selectRebuildDevice` + unit tests

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala`

- [ ] **Step 1: Write the failing tests**

Add this helper to the `StorageDeviceSetSelectionSuite` companion object (after the existing `noLookup` val, around line 49):

```scala
  /** A free-space lookup backed by a fixed map. */
  def fixedLookup(free: Map[StorageDeviceId, Long]): StorageDeviceId => Future[Long] =
    id => Future.successful(free(id))
```

Add these tests inside `class StorageDeviceSetSelectionSuite` (after the last existing test, around line 171):

```scala
  test("rebuild: prefers a non-pool device with sufficient space"):
    val failed = dev()
    val poolDev = dev()
    val spare = dev()
    val set = leaf(List(failed, poolDev, spare))
    val lookup = fixedLookup(Map(failed -> 9999L, poolDev -> 9999L, spare -> 9999L))
    val r = Await.result(
      set.selectRebuildDevice(500L, failed, Set(failed, poolDev), lookup, new Random(1)), timeout)
    r should be(spare)

  test("rebuild: falls back to a pool device when no non-pool device fits"):
    val failed = dev()
    val poolDev = dev()
    val spare = dev() // non-pool but too small
    val set = leaf(List(failed, poolDev, spare))
    val lookup = fixedLookup(Map(failed -> 9999L, poolDev -> 1000L, spare -> 100L))
    val r = Await.result(
      set.selectRebuildDevice(500L, failed, Set(failed, poolDev), lookup, new Random(2)), timeout)
    r should be(poolDev)

  test("rebuild: failed device is never selected, even as a last resort"):
    val failed = dev()
    val other = dev()
    val set = leaf(List(failed, other))
    val lookup = fixedLookup(Map(failed -> 9999L, other -> 0L))
    assertThrows[AllocationError]:
      Await.result(
        set.selectRebuildDevice(500L, failed, Set(failed, other), lookup, new Random(3)), timeout)

  test("rebuild: AllocationError when no device has sufficient space"):
    val failed = dev()
    val a = dev()
    val b = dev()
    val set = leaf(List(failed, a, b))
    val lookup = fixedLookup(Map(failed -> 9999L, a -> 100L, b -> 200L))
    assertThrows[AllocationError]:
      Await.result(
        set.selectRebuildDevice(500L, failed, Set(failed), lookup, new Random(4)), timeout)

  test("rebuild: requiredSize 0 accepts any non-failed device with free space"):
    val failed = dev()
    val d1 = dev()
    val set = leaf(List(failed, d1))
    val lookup = fixedLookup(Map(failed -> 9999L, d1 -> 0L))
    val r = Await.result(
      set.selectRebuildDevice(0L, failed, Set(failed), lookup, new Random(5)), timeout)
    r should be(d1)

  test("rebuild: stops reading device states once a fitting device is found"):
    val failed = dev()
    val a = dev()
    val b = dev()
    val set = leaf(List(failed, a, b))
    val queried = scala.collection.mutable.ListBuffer[StorageDeviceId]()
    val free = Map(failed -> 9999L, a -> 9999L, b -> 9999L)
    val lookup: StorageDeviceId => Future[Long] = id =>
      queried += id
      Future.successful(free(id))
    val r = Await.result(
      set.selectRebuildDevice(500L, failed, Set(failed), lookup, new Random(6)), timeout)
    // a and b both qualify, so only the first candidate should be queried
    queried.toList.length should be(1)
    List(a, b) should contain(r)

  test("rebuild: skips a candidate whose free-space lookup fails"):
    val failed = dev()
    val bad = dev()  // non-pool, lookup fails
    val good = dev() // pool device, has space (fallback)
    val set = leaf(List(failed, bad, good))
    val lookup: StorageDeviceId => Future[Long] = id =>
      if id == bad then Future.failed(new RuntimeException("boom"))
      else Future.successful(9999L)
    val r = Await.result(
      set.selectRebuildDevice(500L, failed, Set(failed, good), lookup, new Random(7)), timeout)
    r should be(good)

  test("rebuild: level != 0 fails with AllocationError"):
    val upperSet = upper(1, List(leaf(List(dev()))))
    val lookup = fixedLookup(Map.empty[StorageDeviceId, Long].withDefaultValue(9999L))
    assertThrows[AllocationError]:
      Await.result(
        upperSet.selectRebuildDevice(0L, dev(), Set.empty, lookup, new Random(8)), timeout)

  test("rebuild: empty member devices fails with AllocationError"):
    val set = leaf(Nil)
    val lookup = fixedLookup(Map.empty[StorageDeviceId, Long].withDefaultValue(9999L))
    assertThrows[AllocationError]:
      Await.result(
        set.selectRebuildDevice(0L, dev(), Set.empty, lookup, new Random(9)), timeout)

  test("rebuild: identical seed produces identical selection"):
    val failed = dev()
    val devices = List.fill(6)(dev())
    val set = leaf(failed :: devices)
    val lookup = fixedLookup((failed :: devices).map(_ -> 9999L).toMap)
    val a = Await.result(
      set.selectRebuildDevice(500L, failed, Set(failed), lookup, new Random(123)), timeout)
    val b = Await.result(
      set.selectRebuildDevice(500L, failed, Set(failed), lookup, new Random(123)), timeout)
    a should be(b)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: FAIL — compilation error, `value selectRebuildDevice is not a member of ...StorageDeviceSetState` (the method does not exist yet).

- [ ] **Step 3: Add imports to `StorageDeviceSetState.scala`**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`, update the imports. The current relevant lines are:

```scala
import org.aspen_ddp.aspen.common.pool.PoolId

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random
```

Replace them with:

```scala
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.util.byte2long

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Success}
```

(`byte2long` is used by the public method in Task 2; adding it now keeps the import edit in one place. `Success` is used by the core scan below.)

- [ ] **Step 4: Implement the core `selectRebuildDevice`**

In the same file, add this method to the `StorageDeviceSetState` class, immediately after the existing `selectFromSets` method (after the current final line 110 `folded.map(_._1)` block):

```scala
  /** Select a device to host a store rebuilt from scratch (its data was lost).
   *
   *  Only valid for level-0 sets. The failed device is hard-excluded (never chosen).
   *  Devices already hosting a store in the pool (`poolDevices`) are soft-excluded:
   *  preferred candidates that are not in `poolDevices` are tried first, falling back
   *  to pool devices only if no preferred device has enough space. Free space is a hard
   *  requirement in both phases.
   *
   *  Selection is optimistic and lazy: candidates are tried in random order, reading
   *  each device's free space one at a time via `freeSpaceLookup`, stopping at the first
   *  that fits. A candidate whose lookup fails is skipped. `rng` is injectable so tests
   *  can be deterministic. Depends only on `freeSpaceLookup` so it is unit-testable
   *  without a full `AspenClient`.
   */
  private[metadata] def selectRebuildDevice(
      requiredSize: Long,
      failedDevice: StorageDeviceId,
      poolDevices: Set[StorageDeviceId],
      freeSpaceLookup: StorageDeviceId => Future[Long],
      rng: Random
  )(using ec: ExecutionContext): Future[StorageDeviceId] =
    if level != 0 then
      Future.failed(AllocationError(
        s"selectDeviceForRebuild only supports level-0 sets; set ${setId.uuid} is level $level"))
    else if memberDevices.isEmpty then
      Future.failed(AllocationError(
        s"StorageDeviceSet ${setId.uuid} (level 0) has no member devices"))
    else
      val eligible = memberDevices.filterNot(_ == failedDevice)
      val preferred = rng.shuffle(eligible.filterNot(poolDevices.contains))
      val fallback = rng.shuffle(eligible.filter(poolDevices.contains))
      val candidates = preferred ++ fallback

      def scan(remaining: List[StorageDeviceId]): Future[StorageDeviceId] =
        remaining match
          case Nil =>
            Future.failed(AllocationError(
              s"no device in set ${setId.uuid} has >= $requiredSize free bytes available for rebuild"))
          case head :: tail =>
            freeSpaceLookup(head).transformWith:
              case Success(free) if free >= requiredSize => Future.successful(head)
              case _ => scan(tail)

      scan(candidates)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: PASS — all existing tests plus the 10 new `rebuild:` tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala
git commit -m "Add selectRebuildDevice core to StorageDeviceSetState"
```

---

## Task 2: Public `selectDeviceForRebuild` wrapper

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`

**Note on testing:** This wrapper is pure client I/O (reads the pool pointer and device states) and is verified by `sbt compile` plus the core's unit tests, mirroring how the sibling `selectDevicesForPool` wrapper is left untested while its `selectDevices` core carries the test coverage. Constructing a full `AspenClient` for a unit test is out of scope per the spec.

- [ ] **Step 1: Implement the public method**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`, add this method to the `StorageDeviceSetState` class, immediately after `selectDevicesForPool` (after its current line 55 body) so the public methods sit together:

```scala
  /** Select a device to host a store that must be rebuilt from scratch because its
   *  data was lost. The store currently at `failedIndex` in pool `poolId` is the one
   *  being rebuilt; its current size (the pool's per-store usage record, or 0 if absent)
   *  is the amount of free space a candidate device must have.
   *
   *  Only valid for level-0 sets; level-1+ sets fail with `AllocationError` without any
   *  reads. The pool object is read directly via its pointer so the per-store size key
   *  and the config come from a single read. See `selectRebuildDevice` for the selection
   *  policy (failed device hard-excluded, other pool devices soft-excluded, free space
   *  required). All failures are `Future.failed(AllocationError(...))`.
   */
  def selectDeviceForRebuild(
      poolId: PoolId,
      failedIndex: Byte,
      client: AspenClient,
      rng: Random = new Random()
  ): Future[StorageDeviceId] =
    given ExecutionContext = client.clientContext
    if level != 0 then
      Future.failed(AllocationError(
        s"selectDeviceForRebuild only supports level-0 sets; set ${setId.uuid} is level $level"))
    else
      for
        poolPtr <- client.getStoragePoolPointer(poolId)
        poolKvos <- client.read(poolPtr)
        poolState = StoragePoolState(poolKvos)
        device <-
          if failedIndex < 0 || failedIndex >= poolState.stores.length then
            Future.failed(AllocationError(
              s"failedIndex $failedIndex out of range for pool ${poolId.uuid} (${poolState.stores.length} stores)"))
          else
            val requiredSize = poolKvos.contents
              .get(StoragePoolState.getStoreUsageKey(failedIndex))
              .map(vs => byte2long(vs.value.bytes))
              .getOrElse(0L)
            val failedDevice = poolState.stores(failedIndex).storageDeviceId
            val poolDevices = poolState.stores.map(_.storageDeviceId).toSet
            selectRebuildDevice(
              requiredSize,
              failedDevice,
              poolDevices,
              id => client.getStorageDeviceState(id).map(s => s.totalSize - s.currentUsage),
              rng)
      yield device
```

- [ ] **Step 2: Compile the project**

Run: `sbt compile`
Expected: SUCCESS — no errors. (Confirms `getStoragePoolPointer`, `read`, `getStorageDeviceState`, `getStoreUsageKey`, and `byte2long` are all resolved and the types line up.)

- [ ] **Step 3: Run the full selection suite**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: PASS — unchanged from Task 1 (the wrapper adds no new tests but must not break compilation of the suite).

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala
git commit -m "Add public selectDeviceForRebuild wrapper to StorageDeviceSetState"
```

---

## Task 3: Full build + test sweep

**Files:** none (verification only).

- [ ] **Step 1: Compile everything**

Run: `sbt compile`
Expected: SUCCESS.

- [ ] **Step 2: Run the full test suite**

Run: `sbt test`
Expected: PASS — no regressions across the project.

- [ ] **Step 3: Commit (only if any fixups were required)**

If Steps 1-2 surfaced and required fixes, commit them:

```bash
git add -A
git commit -m "Fix up build/test issues for selectDeviceForRebuild"
```

Otherwise nothing to commit.

---

## Self-Review Notes

- **Spec coverage:** Tier-0-only (Task 1 core + Task 2 early check) ✓; read pool pointer directly for store size (Task 2) ✓; required size = per-store usage, missing = 0 (Task 2) ✓; failed device hard-excluded (Task 1 `eligible` filter, tested) ✓; other pool devices soft-excluded with fallback (Task 1 `preferred`/`fallback`, tested) ✓; optimistic lazy scan, no upfront reads, short-circuit (Task 1 `scan`, tested) ✓; AllocationError for no-space and out-of-range and level-1+ (tested / Task 2) ✓; skip-on-lookup-failure (Task 1 `transformWith`, tested) ✓.
- **Type consistency:** core name `selectRebuildDevice` is used identically in all 10 tests and in the Task 2 delegation; `freeSpaceLookup: StorageDeviceId => Future[Long]` matches the Task 2 lambda `id => client.getStorageDeviceState(id).map(s => s.totalSize - s.currentUsage)`; `getStoreUsageKey(failedIndex: Byte)` and `byte2long(Array[Byte]): Long` match their definitions.
- **Placeholders:** none.
