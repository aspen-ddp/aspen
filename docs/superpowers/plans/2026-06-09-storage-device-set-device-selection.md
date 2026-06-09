# StorageDeviceSet Device Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `StorageDeviceSetState.selectDevicesForPool(numStores, client, rng)` that selects `StorageDeviceId`s for a pool by walking the device-set hierarchy with even distribution and best-effort deduplication.

**Architecture:** A public method on the `StorageDeviceSetState` case class takes an explicit `AspenClient` and a defaulted, injectable `Random`. It delegates to a package-private recursive helper, `selectDevices`, that depends only on a narrow `lookup: StorageDeviceSetId => Future[StorageDeviceSetState]` function (so it can be unit-tested without a real client). Level-0 sets draw from `memberDevices`; level-1+ sets round-robin over `memberSets` and recurse down with `numStores = 1`, threading an accumulating exclusion set.

**Tech Stack:** Scala 3, `scala.concurrent.Future`, `scala.util.Random`, ScalaTest (`AnyFunSuite` + `Matchers`), sbt.

---

## File Structure

- **Modify:** `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`
  - Add the public `selectDevicesForPool` method and the private helpers `selectDevices`, `selectFromDevices`, `selectFromSets` to the `StorageDeviceSetState` case class. Add required imports.
- **Create:** `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala`
  - Unit tests for the level-0 strategy (Task 1) and the level-1+ strategy (Task 2). Lives in the same `metadata` package so it can call the `private[metadata]` helper directly.

Both files change together and share one responsibility (device selection), so the implementation lives on the existing `StorageDeviceSetState` and its tests in a dedicated suite.

---

## Task 1: Level-0 selection (public method + flat-set strategy)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala` (create)

- [ ] **Step 1: Write the failing test suite for level-0 behavior**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.AllocationError
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.util.Random

object StorageDeviceSetSelectionSuite:
  given ExecutionContext = ExecutionContext.global
  val timeout: FiniteDuration = 5.seconds

  def dev(): StorageDeviceId = StorageDeviceId(UUID.randomUUID())

  /** A level-0 set containing the given devices. */
  def leaf(devices: List[StorageDeviceId]): StorageDeviceSetState =
    StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "leaf",
      level = 0,
      parent = None,
      memberDevices = devices,
      memberSets = Nil,
      assignedPools = Nil
    )

  /** A higher-level set over the given member sets. */
  def upper(level: Int, members: List[StorageDeviceSetState]): StorageDeviceSetState =
    StorageDeviceSetState(
      setId = StorageDeviceSetId(UUID.randomUUID()),
      name = "upper",
      level = level,
      parent = None,
      memberDevices = Nil,
      memberSets = members.map(_.setId),
      assignedPools = Nil
    )

  /** Build a lookup over every supplied state. */
  def lookupFor(states: StorageDeviceSetState*): StorageDeviceSetId => Future[StorageDeviceSetState] =
    val m = states.map(s => s.setId -> s).toMap
    id => Future.successful(m(id))

  /** Lookup that fails if ever called (level-0 tests never recurse). */
  val noLookup: StorageDeviceSetId => Future[StorageDeviceSetState] =
    _ => Future.failed(new RuntimeException("lookup should not be called"))

class StorageDeviceSetSelectionSuite extends AnyFunSuite with Matchers:
  import StorageDeviceSetSelectionSuite.*

  test("level 0: numStores < members yields distinct subset"):
    val devices = List.fill(5)(dev())
    val set = leaf(devices)
    val result = Await.result(set.selectDevices(3, Set.empty, noLookup, new Random(1)), timeout)
    result.length should be(3)
    result.distinct.length should be(3)
    result.toSet.subsetOf(devices.toSet) should be(true)

  test("level 0: numStores == members yields a full permutation"):
    val devices = List.fill(4)(dev())
    val set = leaf(devices)
    val result = Await.result(set.selectDevices(4, Set.empty, noLookup, new Random(2)), timeout)
    result.length should be(4)
    result.toSet should be(devices.toSet)

  test("level 0: numStores > members reuses minimally and evenly"):
    val devices = List.fill(3)(dev())
    val set = leaf(devices)
    val result = Await.result(set.selectDevices(7, Set.empty, noLookup, new Random(3)), timeout)
    result.length should be(7)
    val counts = result.groupBy(identity).view.mapValues(_.size).values.toList
    counts.max should be(3) // ceil(7/3)
    counts.min should be(2) // floor(7/3)
    counts.size should be(3) // every device used

  test("level 0: exclude is honored at numStores == 1"):
    val devices = List.fill(4)(dev())
    val set = leaf(devices)
    val excluded = devices.take(3).toSet
    val result = Await.result(set.selectDevices(1, excluded, noLookup, new Random(4)), timeout)
    result should be(List(devices(3)))

  test("level 0: falls back to a duplicate when all devices are excluded"):
    val devices = List.fill(3)(dev())
    val set = leaf(devices)
    val result = Await.result(set.selectDevices(1, devices.toSet, noLookup, new Random(5)), timeout)
    result.length should be(1)
    devices should contain(result.head)

  test("level 0: identical seed produces identical results"):
    val devices = List.fill(6)(dev())
    val set = leaf(devices)
    val a = Await.result(set.selectDevices(4, Set.empty, noLookup, new Random(123)), timeout)
    val b = Await.result(set.selectDevices(4, Set.empty, noLookup, new Random(123)), timeout)
    a should be(b)

  test("numStores <= 0 returns empty list"):
    val set = leaf(List.fill(3)(dev()))
    Await.result(set.selectDevices(0, Set.empty, noLookup, new Random(6)), timeout) should be(Nil)

  test("level 0: empty member devices fails with AllocationError"):
    val set = leaf(Nil)
    assertThrows[AllocationError]:
      Await.result(set.selectDevices(2, Set.empty, noLookup, new Random(7)), timeout)
```

- [ ] **Step 2: Run the suite to verify it fails to compile / fail**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: FAIL — compilation error, `value selectDevices is not a member of StorageDeviceSetState`.

- [ ] **Step 3: Add imports and the level-0 implementation to `StorageDeviceSetState`**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`, replace the existing import line:

```scala
import org.aspen_ddp.aspen.client.DataObjectState
```

with:

```scala
import org.aspen_ddp.aspen.client.{AllocationError, AspenClient, DataObjectState}
```

and add these imports alongside the others:

```scala
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random
```

Then, inside the `StorageDeviceSetState` case class, immediately after the existing `def toBytes` method, add:

```scala
  /** Select `numStores` device ids for a pool, walking this set's hierarchy.
   *
   *  Level-0 sets draw from their member devices. Level-1+ sets distribute the
   *  request evenly across their member sets and recurse. `rng` is injectable so
   *  tests can be deterministic.
   */
  def selectDevicesForPool(
      numStores: Int,
      client: AspenClient,
      rng: Random = new Random()
  ): Future[List[StorageDeviceId]] =
    given ExecutionContext = client.clientContext
    selectDevices(numStores, Set.empty, client.getStorageDeviceSetState, rng)

  /** Recursive core. Depends only on a narrow `lookup` so it is unit-testable
   *  without a full `AspenClient`. `exclude` carries device ids already chosen
   *  earlier in the overall selection, enabling best-effort deduplication.
   */
  private[metadata] def selectDevices(
      numStores: Int,
      exclude: Set[StorageDeviceId],
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
      rng: Random
  )(using ec: ExecutionContext): Future[List[StorageDeviceId]] =
    if numStores <= 0 then
      Future.successful(Nil)
    else if level == 0 then
      selectFromDevices(numStores, exclude, rng)
    else
      Future.failed(new NotImplementedError("level 1+ selection added in Task 2"))

  private def selectFromDevices(
      numStores: Int,
      exclude: Set[StorageDeviceId],
      rng: Random
  ): Future[List[StorageDeviceId]] =
    if memberDevices.isEmpty then
      Future.failed(AllocationError(s"StorageDeviceSet ${setId.uuid} (level 0) has no member devices"))
    else
      val shuffled = rng.shuffle(memberDevices)
      val preferred = shuffled.filterNot(exclude.contains)
      val ordered = preferred.iterator ++ Iterator.continually(shuffled).flatten
      Future.successful(ordered.take(numStores).toList)
```

- [ ] **Step 4: Run the suite to verify it passes**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: PASS — all 8 level-0 tests green. (The level-1+ `else` branch is a temporary placeholder, not yet exercised.)

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala
git commit -m "Add level-0 device selection to StorageDeviceSetState

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Level-1+ recursion (hierarchical distribution + dedup)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala`

- [ ] **Step 1: Add the failing level-1+ tests**

Append these tests to the body of `class StorageDeviceSetSelectionSuite` in `StorageDeviceSetSelectionSuite.scala` (after the last level-0 test):

```scala
  test("level 1: distributes evenly across member sets (counts differ by <= 1)"):
    val leaves = List.fill(3)(leaf(List.fill(4)(dev())))
    val root = upper(1, leaves)
    val deviceToLeaf: Map[StorageDeviceId, StorageDeviceSetId] =
      leaves.flatMap(l => l.memberDevices.map(_ -> l.setId)).toMap

    val result = Await.result(
      root.selectDevices(7, Set.empty, lookupFor(leaves*), new Random(11)), timeout)

    result.length should be(7)
    val perLeaf = result.groupBy(deviceToLeaf).view.mapValues(_.size).values.toList
    (perLeaf.max - perLeaf.min) should be <= 1

  test("level 1: no duplicates when each member set has enough distinct devices"):
    val leaves = List.fill(3)(leaf(List.fill(4)(dev())))
    val root = upper(1, leaves)

    // numStores=6, 3 member sets => ceil(6/3)=2 visits each, each leaf has 4 devices.
    val result = Await.result(
      root.selectDevices(6, Set.empty, lookupFor(leaves*), new Random(12)), timeout)

    result.length should be(6)
    result.distinct.length should be(6)

  test("level 1: duplicates only from an exhausted member set, distribution still even"):
    val a1 = dev()
    val small = leaf(List(a1))            // only 1 device
    val big = leaf(List.fill(5)(dev()))   // plenty
    val root = upper(1, List(small, big))
    val deviceToLeaf: Map[StorageDeviceId, StorageDeviceSetId] =
      (small.memberDevices ++ big.memberDevices).map { d =>
        d -> (if d == a1 then small.setId else big.setId)
      }.toMap

    // numStores=4, 2 member sets => 2 visits each. `small` is revisited but has 1 device.
    val result = Await.result(
      root.selectDevices(4, Set.empty, lookupFor(small, big), new Random(13)), timeout)

    result.length should be(4)
    result.count(_ == a1) should be(2)        // the only possible duplicate, from `small`
    result.distinct.length should be(3)
    val perLeaf = result.groupBy(deviceToLeaf).view.mapValues(_.size).toMap
    perLeaf(small.setId) should be(2)
    perLeaf(big.setId) should be(2)

  test("level 2: chains down through nested sets to leaf devices"):
    val leavesA = List.fill(2)(leaf(List.fill(3)(dev())))
    val leavesB = List.fill(2)(leaf(List.fill(3)(dev())))
    val branchA = upper(1, leavesA)
    val branchB = upper(1, leavesB)
    val root = upper(2, List(branchA, branchB))
    val allDevices = (leavesA ++ leavesB).flatMap(_.memberDevices).toSet
    val lookup = lookupFor((leavesA ++ leavesB ++ List(branchA, branchB))*)

    val result = Await.result(root.selectDevices(4, Set.empty, lookup, new Random(14)), timeout)

    result.length should be(4)
    result.toSet.subsetOf(allDevices) should be(true)

  test("level 1: empty member sets fails with AllocationError"):
    val root = upper(1, Nil)
    assertThrows[AllocationError]:
      Await.result(root.selectDevices(2, Set.empty, noLookup, new Random(15)), timeout)
```

- [ ] **Step 2: Run the suite to verify the new tests fail**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: FAIL — the new level-1+ tests fail (the `else` branch returns a failed `NotImplementedError`); the 8 level-0 tests still pass.

- [ ] **Step 3: Replace the placeholder with the level-1+ implementation**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`, change the `else` branch of `selectDevices` from:

```scala
    else
      Future.failed(new NotImplementedError("level 1+ selection added in Task 2"))
```

to:

```scala
    else
      selectFromSets(numStores, exclude, lookup, rng)
```

Then add the `selectFromSets` helper immediately after `selectFromDevices`:

```scala
  private def selectFromSets(
      numStores: Int,
      exclude: Set[StorageDeviceId],
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
      rng: Random
  )(using ec: ExecutionContext): Future[List[StorageDeviceId]] =
    if memberSets.isEmpty then
      Future.failed(AllocationError(s"StorageDeviceSet ${setId.uuid} (level $level) has no member sets"))
    else
      val shuffled = rng.shuffle(memberSets)
      // Round-robin visiting order gives per-member-set counts differing by <= 1.
      val visits = Iterator.continually(shuffled).flatten.take(numStores).toList

      // Sequential fold so each visit sees devices chosen by earlier visits,
      // threading the growing exclusion set for best-effort deduplication.
      val folded = visits.foldLeft(Future.successful((List.empty[StorageDeviceId], exclude))):
        (accF, memberSetId) =>
          accF.flatMap: (results, excludeSoFar) =>
            lookup(memberSetId).flatMap: subState =>
              subState.selectDevices(1, excludeSoFar, lookup, rng).map: chosen =>
                val device = chosen.head
                (results :+ device, excludeSoFar + device)

      folded.map(_._1)
```

- [ ] **Step 4: Run the suite to verify all tests pass**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: PASS — all 13 tests (8 level-0 + 5 level-1+) green.

- [ ] **Step 5: Confirm the whole project still compiles and the broader metadata tests pass**

Run: `sbt 'testOnly *StorageDeviceSet*'`
Expected: PASS — `StorageDeviceSetSelectionSuite`, `StorageDeviceSetStateSuite`, and `StorageDeviceSetIntegrationSuite` all green (the last two are unaffected).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala
git commit -m "Add level-1+ hierarchical device selection to StorageDeviceSetState

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** Public API (Task 1 Step 3) ✓; level-0 unified algorithm (Task 1) ✓; level-1+ round-robin + sequential exclude-threading (Task 2) ✓; empty-set → `AllocationError` (both tasks) ✓; injectable `Random` + determinism test ✓; level-0, level-1, dedup-fallback, even-distribution, and 2-level nesting tests ✓.
- **Dedup guarantee tested precisely:** the no-duplicate test uses `ceil(numStores/numMemberSets) <= per-set device count`; the fallback test forces a revisit of an exhausted small set, matching the spec's corrected guarantee.
- **Type consistency:** `selectDevices`/`selectFromDevices`/`selectFromSets` signatures and the `lookup: StorageDeviceSetId => Future[StorageDeviceSetState]` alias are identical across both tasks; `AllocationError(String)`, `StorageDeviceId(UUID)`, and `client.clientContext`/`getStorageDeviceSetState` match the existing codebase.
- **No placeholders** remain after Task 2 (the Task 1 `NotImplementedError` is explicitly replaced in Task 2 Step 3).
