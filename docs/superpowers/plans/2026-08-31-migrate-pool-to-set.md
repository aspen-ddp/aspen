# MigratePoolToSetDurableTask Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator move every store of a storage pool onto devices of a different `StorageDeviceSet`, driven by a crash-tolerant durable task.

**Architecture:** `StoragePoolState` gains an optional `migration` field. `AspenClient.migratePoolToSet` flips the pool's `storageDeviceSet` pointer, swaps `assignedPools` between the old and new sets, and enrolls a `MigratePoolToSetDurableTask` with the system task executor — all in one transaction. The task polls: it re-reads the pool each pass, computes the target set's device membership by walking downward, treats a store as migrated iff its device is in that membership (the pool's own `stores` array is the progress record, rewritten atomically by `StoreManager` at transfer completion), and moves one remaining store at a time via `transferStore`, gated by `TransferSafety.isSafe`. When nothing remains it marks the migration `Complete`.

**Tech Stack:** Scala 3 (quiet syntax, indented if/then/else), ScalaTest (`AnyFunSuite` + `Matchers` for unit tests, `IntegrationTestSuite`/`TestNetwork` for integration), ScalaPB / protobuf 3, scribe logging, sbt.

**Spec:** `docs/superpowers/specs/2026-08-31-migrate-pool-to-set-design.md`

## Global Constraints

- Scala 3 "quiet mode" syntax. Braces only where they clearly enhance readability.
- Indented `if/then/else`. Avoid single-line `if a == 1 then value` unless it enhances readability.
- Protobuf changes must stay in lockstep across three sites: `src/main/protobuf/codec.proto`, `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`, and `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala`.
- `StoragePoolState` field 11 is the only free protobuf field number on that message (1–10 are taken). Use exactly 11.
- Every transaction must stage at least one operation/requirement. Never let a code path commit an empty transaction — throw a sentinel and recover instead.
- `common/util.ignoreExtraCallsWhileRunning` clears its in-flight flag via `Future.foreach`, which does **not** fire on a failed Future. Any function wrapped in it must return a Future that never fails, or the wrapper wedges permanently.
- The durable task must never fail its own promise except from `onStop()` (with `TaskStopped`). All other errors log at warn and reschedule.
- A `transactUntilSuccessfulWithRecovery` `onFail` handler may be a non-exhaustive match. `ExponentialBackoffRetryStrategy` wraps the call in try/catch, so an unmatched error becomes a `MatchError` that falls through to `scheduleNextAttempt` — i.e. "keep retrying", the correct default. Do not add a catch-all that turns transient errors terminal.
- `StorageDeviceSetSelectionSuite` must pass **unchanged** after the `selectRebuildDevice` refactor — that refactor is behavior-preserving.
- Run a single test with: `sbt 'testOnly *SuiteName -- -z "test substring"'`

---

## File Structure

**Modified:**

| File | Responsibility after this change |
|---|---|
| `src/main/protobuf/codec.proto` | Adds `PoolMigrationStatus` enum and `PoolMigration` message; `StoragePoolState.migration = 11`. |
| `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala` | Adds `MigrationStatus` enum, `Migration` case class, and the optional `migration` field. |
| `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` | Encode/decode for the new enum, message, and field. |
| `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala` | Gains the level-aware `selectDeviceWithSpace` core, the public `selectDeviceForStore`, and the downward membership walk `collectMemberDevices`/`collectDevices`. `selectRebuildDevice` becomes a thin level-0-guarded wrapper over the core. |
| `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala` | Gains `getStateForPoolMigration` (pool-centric, level-agnostic). `getStateForRebalancePlanning` excludes `InProgress` pools. |
| `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceDurableTask.scala` | `drive` drops pending transfers whose pool has left planning state. |
| `src/main/scala/org/aspen_ddp/aspen/common/TypeFactories.scala` | Registers `MigratePoolToSetDurableTask`. |
| `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` | Declares `migratePoolToSet`. |
| `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` | Implements `migratePoolToSet`. |
| `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` | `migrate-pool` command; `show-pool` renders migration status. |

**Created:**

| File | Responsibility |
|---|---|
| `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetDurableTask.scala` | The factory object (type UUID, state key, poll-period seam, `prepareSystemTask`) and the task class that drives one pool's migration. |
| `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/PoolMigrationPlanningSuite.scala` | Integration tests for `getStateForPoolMigration`, the rebalance-planning exclusion, and the `SetRebalanceDurableTask` drop. |
| `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala` | Integration tests for factory registration, the `migratePoolToSet` entry point, and end-to-end migration. |

**Extended test files:** `CodecRoundTripSuite.scala`, `StorageDeviceSetSelectionSuite.scala`, `MainSuite.scala`.

**Task order rationale:** Task 7 (the factory object) must precede Task 8 (the entry point) because `migratePoolToSet` references `MigratePoolToSetDurableTask.typeUUID` and `PoolIdKey`. Tasks 2–6 are independent of each other and of 7–8, but 7 depends on 2, 3, and 4.

**Deviation from the spec's Testing section:** the spec lists `getStateForPoolMigration` under "Unit, no client". Its signature takes an `AspenClient`, so it is tested as an integration test (Task 4) instead. Everything else follows the spec's split.

---

### Task 1: `migration` field on `StoragePoolState` (model + protobuf + codec)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala:13-42`
- Modify: `src/main/protobuf/codec.proto:485-496`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala:1047-1076`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala:527-556`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `StoragePoolState.MigrationStatus` — Scala 3 `enum` with cases `InProgress`, `Complete`.
  - `StoragePoolState.Migration(targetSet: StorageDeviceSetId, status: StoragePoolState.MigrationStatus)` — case class.
  - `StoragePoolState.migration: Option[StoragePoolState.Migration]`, defaulting to `None`, positioned **last** in the case class parameter list so every existing positional construction still compiles.
  - `Codec.encode(o: StoragePoolState.Migration): codec.PoolMigration` and `Codec.decode(m: codec.PoolMigration): StoragePoolState.Migration`.

- [ ] **Step 1: Write the failing test**

Add to `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala`, immediately after the existing `"StoragePoolState round-trip"` test:

```scala
  test("StoragePoolState migration field round-trip"):
    val withMigration = StoragePoolState(
      PoolId(uuid(20)), "migrating-pool", Replication(3, 2), None,
      Array.empty, RocksDBConfig(), StorageDeviceSetId(uuid(21)),
      migration = Some(StoragePoolState.Migration(
        StorageDeviceSetId(uuid(22)), StoragePoolState.MigrationStatus.InProgress))
    )
    val decoded = Codec.decode(Codec.encode(withMigration))
    decoded.migration shouldBe Some(StoragePoolState.Migration(
      StorageDeviceSetId(uuid(22)), StoragePoolState.MigrationStatus.InProgress))

    val completed = withMigration.copy(migration = Some(StoragePoolState.Migration(
      StorageDeviceSetId(uuid(22)), StoragePoolState.MigrationStatus.Complete)))
    Codec.decode(Codec.encode(completed)).migration.get.status shouldBe
      StoragePoolState.MigrationStatus.Complete

    // Absent is the common case: every pool that has never been migrated.
    val noMigration = StoragePoolState(
      PoolId(uuid(23)), "plain-pool", Replication(3, 2), None,
      Array.empty, RocksDBConfig(), StorageDeviceSetId(uuid(24))
    )
    Codec.decode(Codec.encode(noMigration)).migration shouldBe None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *CodecRoundTripSuite -- -z "migration field round-trip"'`
Expected: FAIL — compile error, `value migration is not a member of StoragePoolState`.

- [ ] **Step 3: Add the model types**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala`, add to the `object StoragePoolState` body (after `case class StoreEntry`, before `getStoreUsageKey`):

```scala
  /** State of a pool-to-set migration. `Complete` persists rather than reverting to `None`,
   *  so a pool records its last migration. */
  enum MigrationStatus:
    case InProgress, Complete

  case class Migration(targetSet: StorageDeviceSetId, status: MigrationStatus)
```

Then add the field as the **last** parameter of the case class:

```scala
final case class StoragePoolState(
                                   poolId: PoolId,
                                   name: String,
                                   ida: IDA,
                                   maxObjectSize: Option[Int],
                                   stores: Array[StoragePoolState.StoreEntry],
                                   backendConfig: BackendConfig,
                                   storageDeviceSet: StorageDeviceSetId,
                                   currentUsage: Long = 0,
                                   maximumStoreSize: Long = 0,
                                   allocationGroups: List[UUID] = Nil,
                                   migration: Option[StoragePoolState.Migration] = None
                                 ):
```

- [ ] **Step 4: Add the protobuf definitions**

In `src/main/protobuf/codec.proto`, replace the `message StoragePoolState { ... }` block with:

```protobuf
enum PoolMigrationStatus {
  POOL_MIGRATION_STATUS_IN_PROGRESS = 0;
  POOL_MIGRATION_STATUS_COMPLETE = 1;
}

message PoolMigration {
  UUID targetSet = 1;
  PoolMigrationStatus status = 2;
}

message StoragePoolState {
  UUID poolId = 1;
  string name = 2;
  IDA ida = 3;
  uint32 maxObjectSize = 4;
  repeated PoolStoreEntry stores = 5;
  BackendConfig backendConfig = 6;
  int64 currentUsage = 7;
  int64 maximumStoreSize = 8;
  repeated UUID allocationGroups = 9;
  UUID storageDeviceSet = 10;
  PoolMigration migration = 11;
}
```

- [ ] **Step 5: Add the codec functions**

In `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`, insert immediately before `def encode(o: StoragePoolState): codec.StoragePoolState`:

```scala
  def encodePoolMigrationStatus(o: StoragePoolState.MigrationStatus): codec.PoolMigrationStatus = o match
    case StoragePoolState.MigrationStatus.InProgress => codec.PoolMigrationStatus.POOL_MIGRATION_STATUS_IN_PROGRESS
    case StoragePoolState.MigrationStatus.Complete   => codec.PoolMigrationStatus.POOL_MIGRATION_STATUS_COMPLETE

  def decodePoolMigrationStatus(m: codec.PoolMigrationStatus): StoragePoolState.MigrationStatus = m match
    case codec.PoolMigrationStatus.POOL_MIGRATION_STATUS_IN_PROGRESS => StoragePoolState.MigrationStatus.InProgress
    case codec.PoolMigrationStatus.POOL_MIGRATION_STATUS_COMPLETE => StoragePoolState.MigrationStatus.Complete
    case codec.PoolMigrationStatus.Unrecognized(v) => throw new EncodingError(f"Invalid PoolMigrationStatus: $v")

  def encode(o: StoragePoolState.Migration): codec.PoolMigration =
    codec.PoolMigration(
      targetSet = Some(encodeUUID(o.targetSet.uuid)),
      status = encodePoolMigrationStatus(o.status)
    )

  def decode(m: codec.PoolMigration): StoragePoolState.Migration =
    StoragePoolState.Migration(
      StorageDeviceSetId(decodeUUID(m.targetSet.get)),
      decodePoolMigrationStatus(m.status))
```

Then wire the field into the existing pair. In `encode`, add a final argument after `storageDeviceSet`:

```scala
      storageDeviceSet = Some(encodeUUID(o.storageDeviceSet.uuid)),
      migration = o.migration.map(encode)
    )
```

In `decode`, add the val and extend the construction:

```scala
    val storageDeviceSet = StorageDeviceSetId(decodeUUID(m.storageDeviceSet.get))
    val migration = m.migration.map(decode)

    StoragePoolState(poolId, name, ida, maxObjectSize, stores, backendConfig,
      storageDeviceSet, currentUsage, maximumStoreSize, allocationGroups, migration)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `sbt 'testOnly *CodecRoundTripSuite'`
Expected: PASS — the new test and all pre-existing round-trip tests.

- [ ] **Step 7: Commit**

```bash
git add src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala
git commit -m "feat: add optional migration field to StoragePoolState"
```

---

### Task 2: Level-aware `selectDeviceWithSpace` core

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala:245-274`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala` (append)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: on `StorageDeviceSetState`,
  ```scala
  private[metadata] def selectDeviceWithSpace(
      requiredSize: Long,
      hardExclude: Set[StorageDeviceId],
      softExclude: Set[StorageDeviceId],
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
      freeSpaceLookup: StorageDeviceId => Future[Long],
      rng: Random
  )(using ec: ExecutionContext): Future[StorageDeviceId]
  ```
  `selectRebuildDevice` keeps its existing signature and its level-0 guard, delegating its body to this core.

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala` (inside the class):

```scala
  test("selectDeviceWithSpace: level 0 prefers non-soft-excluded devices"):
    val preferredDev = dev()
    val softDev = dev()
    val set = leaf(List(softDev, preferredDev))
    val free = fixedLookup(Map(preferredDev -> 1000L, softDev -> 1000L))
    val chosen = Await.result(
      set.selectDeviceWithSpace(100L, Set.empty, Set(softDev), noLookup, free, new Random(1)),
      timeout)
    chosen should be(preferredDev)

  test("selectDeviceWithSpace: falls back to soft-excluded when preferred lack space"):
    val tightDev = dev()
    val softDev = dev()
    val set = leaf(List(tightDev, softDev))
    val free = fixedLookup(Map(tightDev -> 10L, softDev -> 1000L))
    val chosen = Await.result(
      set.selectDeviceWithSpace(100L, Set.empty, Set(softDev), noLookup, free, new Random(1)),
      timeout)
    chosen should be(softDev)

  test("selectDeviceWithSpace: never returns a hard-excluded device"):
    val banned = dev()
    val ok = dev()
    val set = leaf(List(banned, ok))
    val free = fixedLookup(Map(banned -> 1000L, ok -> 1000L))
    (0 until 20).foreach: i =>
      val chosen = Await.result(
        set.selectDeviceWithSpace(100L, Set(banned), Set.empty, noLookup, free, new Random(i)),
        timeout)
      chosen should be(ok)

  test("selectDeviceWithSpace: level 1 recurses into member sets"):
    val deepDev = dev()
    val child = leaf(List(deepDev))
    val parent = upper(1, List(child))
    val free = fixedLookup(Map(deepDev -> 1000L))
    val chosen = Await.result(
      parent.selectDeviceWithSpace(100L, Set.empty, Set.empty,
        lookupFor(child), free, new Random(1)),
      timeout)
    chosen should be(deepDev)

  test("selectDeviceWithSpace: level 1 tries the next member set when the first is exhausted"):
    val fullDev = dev()
    val roomyDev = dev()
    val childA = leaf(List(fullDev))
    val childB = leaf(List(roomyDev))
    val parent = upper(1, List(childA, childB))
    val free = fixedLookup(Map(fullDev -> 10L, roomyDev -> 1000L))
    (0 until 10).foreach: i =>
      val chosen = Await.result(
        parent.selectDeviceWithSpace(100L, Set.empty, Set.empty,
          lookupFor(childA, childB), free, new Random(i)),
        timeout)
      chosen should be(roomyDev)

  test("selectDeviceWithSpace: exhaustion fails with AllocationError"):
    val tiny = dev()
    val set = leaf(List(tiny))
    val free = fixedLookup(Map(tiny -> 10L))
    val err = intercept[AllocationError](
      Await.result(set.selectDeviceWithSpace(100L, Set.empty, Set.empty, noLookup, free,
        new Random(1)), timeout))
    err.getMessage should include("free bytes available")

  test("selectDeviceWithSpace: level 1 with no member sets fails with AllocationError"):
    val parent = upper(1, Nil)
    intercept[AllocationError](
      Await.result(parent.selectDeviceWithSpace(100L, Set.empty, Set.empty, noLookup,
        fixedLookup(Map.empty), new Random(1)), timeout))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite -- -z "selectDeviceWithSpace"'`
Expected: FAIL — compile error, `value selectDeviceWithSpace is not a member of StorageDeviceSetState`.

- [ ] **Step 3: Extract the core and rewrite `selectRebuildDevice` on top of it**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`, replace the whole `selectRebuildDevice` method (lines 231-274, doc comment included) with:

```scala
  /** Select a device with at least `requiredSize` free bytes, at any level.
   *
   *  Level 0 draws from `memberDevices`; level 1+ shuffles `memberSets` and recurses until
   *  one yields a device. `hardExclude` devices are never chosen. `softExclude` devices are
   *  tried only after every non-excluded candidate has been rejected — soft rather than hard
   *  so that a set narrower than the IDA width still works, for the same reason
   *  `selectDevicesForPool` cycles its device list.
   *
   *  Selection is optimistic and lazy: candidates are tried in random order, reading each
   *  device's free space one at a time via `freeSpaceLookup`, stopping at the first that fits.
   *  A candidate whose lookup fails is skipped. Depends only on `lookup` and `freeSpaceLookup`
   *  so it is unit-testable without a full `AspenClient`. `rng` is injectable so tests can be
   *  deterministic.
   */
  private[metadata] def selectDeviceWithSpace(
      requiredSize: Long,
      hardExclude: Set[StorageDeviceId],
      softExclude: Set[StorageDeviceId],
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
      freeSpaceLookup: StorageDeviceId => Future[Long],
      rng: Random
  )(using ec: ExecutionContext): Future[StorageDeviceId] =
    def exhausted: Future[StorageDeviceId] =
      Future.failed(AllocationError(
        s"no device in set ${setId.uuid} has >= $requiredSize free bytes available"))

    if level == 0 then
      if memberDevices.isEmpty then
        Future.failed(AllocationError(s"StorageDeviceSet ${setId.uuid} (level 0) has no member devices"))
      else
        val eligible = memberDevices.filterNot(hardExclude.contains)
        val preferred = rng.shuffle(eligible.filterNot(softExclude.contains))
        val fallback = rng.shuffle(eligible.filter(softExclude.contains))

        def scan(remaining: List[StorageDeviceId]): Future[StorageDeviceId] =
          remaining match
            case Nil => exhausted
            case head :: tail =>
              freeSpaceLookup(head).transformWith:
                case Success(free) if free >= requiredSize => Future.successful(head)
                case _ => scan(tail)

        scan(preferred ++ fallback)
    else
      if memberSets.isEmpty then
        Future.failed(AllocationError(s"StorageDeviceSet ${setId.uuid} (level $level) has no member sets"))
      else
        def scanSets(remaining: List[StorageDeviceSetId]): Future[StorageDeviceId] =
          remaining match
            case Nil => exhausted
            case head :: tail =>
              lookup(head)
                .flatMap(sub => sub.selectDeviceWithSpace(
                  requiredSize, hardExclude, softExclude, lookup, freeSpaceLookup, rng))
                .transformWith:
                  case Success(device) => Future.successful(device)
                  case Failure(_) => scanSets(tail)

        scanSets(rng.shuffle(memberSets))

  /** Select a device to host a store rebuilt from scratch (its data was lost).
   *
   *  Only valid for level-0 sets. The failed device is hard-excluded; devices already hosting
   *  a store in the pool (`poolDevices`) are soft-excluded. This is `selectDeviceWithSpace`
   *  with the level-0 restriction retained -- the refactor beneath it is behavior-preserving,
   *  not a widening of it.
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
    else
      selectDeviceWithSpace(
        requiredSize,
        hardExclude = Set(failedDevice),
        softExclude = poolDevices,
        lookup = _ => Future.failed(new IllegalStateException("a level-0 set must not recurse")),
        freeSpaceLookup = freeSpaceLookup,
        rng = rng)
```

Add `Failure` to the existing `scala.util` import at line 15:

```scala
import scala.util.{Failure, Random, Success}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: PASS — the seven new tests plus every pre-existing test in the suite, unchanged (the regression guard on the refactor).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala
git commit -m "refactor: extract level-aware selectDeviceWithSpace from selectRebuildDevice"
```

---

### Task 3: `selectDeviceForStore` and the target-set membership walk

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala` (append)

**Interfaces:**
- Consumes: `selectDeviceWithSpace` (Task 2).
- Produces: on `StorageDeviceSetState`,
  ```scala
  def selectDeviceForStore(requiredSize: Long,
                           exclude: Set[StorageDeviceId],
                           client: AspenClient,
                           rng: Random = new Random()): Future[StorageDeviceId]

  def collectMemberDevices(client: AspenClient): Future[Set[StorageDeviceId]]

  private[metadata] def collectDevices(
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState]
  )(using ec: ExecutionContext): Future[Set[StorageDeviceId]]
  ```
  and on the companion, `private[metadata] val MaxConcurrentSetReads: Int = 20`.

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala` (inside the class):

```scala
  test("collectDevices: level 0 returns its own member devices"):
    val a = dev()
    val b = dev()
    val set = leaf(List(a, b))
    Await.result(set.collectDevices(noLookup), timeout) should be(Set(a, b))

  test("collectDevices: level 0 with no devices returns the empty set"):
    Await.result(leaf(Nil).collectDevices(noLookup), timeout) should be(Set.empty)

  test("collectDevices: level 1 unions its children"):
    val a = dev()
    val b = dev()
    val childA = leaf(List(a))
    val childB = leaf(List(b))
    val parent = upper(1, List(childA, childB))
    Await.result(parent.collectDevices(lookupFor(childA, childB)), timeout) should be(Set(a, b))

  test("collectDevices: nested levels are walked to the leaves"):
    val deep = dev()
    val shallow = dev()
    val leafDeep = leaf(List(deep))
    val leafShallow = leaf(List(shallow))
    val mid = upper(1, List(leafDeep))
    val top = upper(2, List(mid, leafShallow))
    val members = Await.result(
      top.collectDevices(lookupFor(mid, leafDeep, leafShallow)), timeout)
    members should be(Set(deep, shallow))

  test("collectDevices: a device outside the tree is not a member"):
    val inside = dev()
    val outside = dev()
    val child = leaf(List(inside))
    val parent = upper(1, List(child))
    val members = Await.result(parent.collectDevices(lookupFor(child)), timeout)
    members should contain(inside)
    members should not contain outside

  test("collectDevices: level 1 with no member sets returns the empty set"):
    Await.result(upper(1, Nil).collectDevices(noLookup), timeout) should be(Set.empty)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite -- -z "collectDevices"'`
Expected: FAIL — compile error, `value collectDevices is not a member of StorageDeviceSetState`.

- [ ] **Step 3: Write the implementation**

In `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala`, add to the `object StorageDeviceSetState` body (after the `apply` overloads, before `class NotLevelZero`):

```scala
  /** Cap on concurrent member-set reads while walking a hierarchy. Bounds load on large trees. */
  private[metadata] val MaxConcurrentSetReads: Int = 20
```

Add to the class body, immediately after `selectDeviceForRebuild`:

```scala
  /** Select a device within this set (at any level) to receive a store being migrated here.
   *
   *  `requiredSize` is the store's current size and is a hard requirement. `exclude` holds the
   *  devices already hosting stores of the same pool; they are soft-excluded, so a target set
   *  narrower than the pool's IDA width still works. Fails with `AllocationError` when no
   *  device in the tree has room.
   */
  def selectDeviceForStore(
      requiredSize: Long,
      exclude: Set[StorageDeviceId],
      client: AspenClient,
      rng: Random = new Random()
  ): Future[StorageDeviceId] =
    given ExecutionContext = client.clientContext
    selectDeviceWithSpace(
      requiredSize,
      hardExclude = Set.empty,
      softExclude = exclude,
      lookup = client.getStorageDeviceSetState,
      freeSpaceLookup = id => client.getStorageDeviceState(id).map(s => s.totalSize - s.currentUsage),
      rng = rng)

  /** Every device reachable from this set: `memberDevices` at level 0, the union of the
   *  members' walks above. This is the "is this store already in the target set?" predicate
   *  used by pool migration.
   *
   *  The walk is downward. The cheaper upward walk (device -> its set -> follow `parent`) is
   *  deliberately not used: `parent` is documented as unenforced and `moveDevice` never
   *  maintains it, and -- more decisively -- selection walks downward, so an upward done-check
   *  could fail to recognize a device that selection had just placed a store on, which is an
   *  infinite migration loop rather than a stale read.
   */
  def collectMemberDevices(client: AspenClient): Future[Set[StorageDeviceId]] =
    given ExecutionContext = client.clientContext
    collectDevices(client.getStorageDeviceSetState)

  /** Recursive core of `collectMemberDevices`, depending only on a narrow `lookup` so it is
   *  unit-testable without a full `AspenClient`. */
  private[metadata] def collectDevices(
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState]
  )(using ec: ExecutionContext): Future[Set[StorageDeviceId]] =
    if level == 0 then
      Future.successful(memberDevices.toSet)
    else
      runBoundedParallel(memberSets, StorageDeviceSetState.MaxConcurrentSetReads): id =>
        lookup(id).flatMap(_.collectDevices(lookup))
      .map(_.foldLeft(Set.empty[StorageDeviceId])(_ ++ _))
```

Extend the `common.util` import at line 11:

```scala
import org.aspen_ddp.aspen.common.util.{byte2long, runBoundedParallel}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *StorageDeviceSetSelectionSuite'`
Expected: PASS — all tests, new and pre-existing.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetState.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetSelectionSuite.scala
git commit -m "feat: add selectDeviceForStore and target-set membership walk"
```

---

### Task 4: `State.getStateForPoolMigration`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/PoolMigrationPlanningSuite.scala`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  ```scala
  def getStateForPoolMigration(client: AspenClient,
                               poolId: PoolId,
                               destinationDevice: StorageDeviceId,
                               maxConcurrentReads: Int = DefaultMaxConcurrentReads
                              ): Future[State.PlanningState]
  ```
  Returns a `PlanningState` whose `pools` holds exactly one entry (`poolId`) and whose `devices` holds the pool's store devices plus `destinationDevice`. Consumed by `TransferSafety.isSafe` in Task 7.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/PoolMigrationPlanningSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetId, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.ExecutionContext

class PoolMigrationPlanningSuite extends IntegrationTestSuite:

  atest("getStateForPoolMigration holds one pool and the store devices plus the destination"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      planning <- State.getStateForPoolMigration(client, poolId, net.secondDeviceId)
      poolState <- client.getStoragePoolState(poolId)
    yield
      planning.pools.keySet should be(Set(poolId))
      planning.pools(poolId).ida should be(poolState.ida)
      planning.pools(poolId).stores.keySet should be(
        poolState.stores.indices.map(i => StoreId(poolId, i.toByte)).toSet)

      planning.devices.keySet should be(
        poolState.stores.map(_.storageDeviceId).toSet + net.secondDeviceId)

      // The destination is empty, so it carries none of the pool's stores.
      planning.devices(net.secondDeviceId).stores should be(empty)

  atest("getStateForPoolMigration attributes each store to its pool device of record"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      poolState <- client.getStoragePoolState(poolId)
      planning <- State.getStateForPoolMigration(client, poolId, net.secondDeviceId)
    yield
      poolState.stores.indices.foreach: i =>
        val storeId = StoreId(poolId, i.toByte)
        val deviceOfRecord = poolState.stores(i).storageDeviceId
        planning.devices(deviceOfRecord).stores.keySet should contain(storeId)
        planning.pools(poolId).stores(storeId).status should be(
          StorageDeviceState.StoreStatus.Active)

  atest("getStateForPoolMigration lets TransferSafety approve a transfer to a fresh device"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      poolState <- client.getStoragePoolState(poolId)
      planning <- State.getStateForPoolMigration(client, poolId, net.secondDeviceId)
    yield
      val storeId = StoreId(poolId, 0.toByte)
      val source = poolState.stores(0).storageDeviceId
      // Replication(3, 2) with three Active stores: 3 - 1 == 2 >= writeThreshold.
      TransferSafety.isSafe(planning, Set.empty,
        Plan.Transfer(storeId, source, net.secondDeviceId)) should be(true)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *PoolMigrationPlanningSuite'`
Expected: FAIL — compile error, `value getStateForPoolMigration is not a member of object State`.

- [ ] **Step 3: Write the implementation**

Append to `object State` in `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala`:

```scala
  /** Gather the planning state needed to decide whether one store of `poolId` may be moved to
   *  `destinationDevice` right now. Holds exactly one pool and the devices that matter: the
   *  pool's own store devices plus the candidate destination.
   *
   *  This is the level-agnostic counterpart to `getStateForRebalancePlanning`, which is
   *  level-0-only. `TransferSafety.isSafe` needs only `devices.get(toDevice)` for the
   *  offline-host check and `pools.get(poolId)` for the write-threshold count; it never
   *  touches set membership, so this works for a target set at any level.
   *
   *  No `TransferringIn` filter is needed here. Each store's device of record comes from
   *  `poolState.stores`, which StoreManager rewrites atomically at transfer completion, so
   *  every store maps to exactly one device by construction.
   */
  def getStateForPoolMigration(client: AspenClient,
                               poolId: PoolId,
                               destinationDevice: StorageDeviceId,
                               maxConcurrentReads: Int = DefaultMaxConcurrentReads): Future[PlanningState] =
    given ExecutionContext = client.clientContext

    for
      poolPtr <- client.getStoragePoolPointer(poolId)
      poolKvos <- client.read(poolPtr, "pool migration planning")
      poolState = StoragePoolState(poolKvos)
      deviceIds = (poolState.stores.map(_.storageDeviceId).toSet + destinationDevice).toSeq
      deviceStates <- runBoundedParallel(deviceIds, maxConcurrentReads)(client.getStorageDeviceState)
    yield
      val stateByDevice = deviceStates.map(ds => ds.storageDeviceId -> ds).toMap

      val deviceOfStore: Map[StoreId, StorageDeviceId] =
        poolState.stores.indices
          .map(i => StoreId(poolId, i.toByte) -> poolState.stores(i).storageDeviceId)
          .toMap

      def storeSize(storeId: StoreId): Long =
        poolKvos.contents
          .get(StoragePoolState.getStoreUsageKey(storeId.poolIndex))
          .map(vs => byte2long(vs.value.bytes))
          .getOrElse(0L)

      // Status comes from the device of record, not from whichever device happens to list the
      // store: mid-transfer the source says TransferringOut and the destination says
      // TransferringIn, and the source is the one that still owns it. A store its device does
      // not list yet reads as Initializing, which TransferSafety does not count as usable --
      // the conservative direction.
      def statusOf(storeId: StoreId): StorageDeviceState.StoreStatus =
        stateByDevice.get(deviceOfStore(storeId))
          .flatMap(_.stores.get(storeId))
          .map(_.status)
          .getOrElse(StorageDeviceState.StoreStatus.Initializing)

      val storesById: Map[StoreId, Store] =
        deviceOfStore.keys.map(sid => sid -> Store(sid, storeSize(sid), statusOf(sid))).toMap

      val devices: Map[StorageDeviceId, Device] =
        deviceStates.map: ds =>
          val itsStores = storesById.filter((sid, _) => deviceOfStore(sid) == ds.storageDeviceId)
          ds.storageDeviceId -> Device(ds.storageDeviceId, ds.hostId, ds.currentUsage, ds.totalSize, itsStores)
        .toMap

      PlanningState(devices, Map(poolId -> Pool(poolId, poolState.ida, storesById)))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *PoolMigrationPlanningSuite'`
Expected: PASS — three tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala \
        src/test/scala/org/aspen_ddp/aspen/common/rebalancing/PoolMigrationPlanningSuite.scala
git commit -m "feat: add pool-centric planning state for migration safety checks"
```

---

### Task 5: Exclude `InProgress` pools from rebalance planning

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala:68-96`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/PoolMigrationPlanningSuite.scala` (append)

**Interfaces:**
- Consumes: `StoragePoolState.migration` / `MigrationStatus` (Task 1).
- Produces: `getStateForRebalancePlanning` keeps its signature but now omits any pool whose `migration` is `InProgress`, and omits that pool's stores from every `Device.stores`.

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/PoolMigrationPlanningSuite.scala` (inside the class). Add these imports at the top of the file:

```scala
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision

import scala.concurrent.Future
```

Then the test plus a shared helper:

```scala
  /** Write `migration` directly into a pool's config. Task 8 adds the real entry point; these
   *  tests predate it and only need the recorded state. */
  private def setMigration(poolId: PoolId,
                           migration: Option[StoragePoolState.Migration]): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        poolPtr <- client.getStoragePoolPointer(poolId)
        kvos <- client.read(poolPtr, "test migration write")
      yield
        val updated = StoragePoolState(kvos).copy(migration = migration)
        val reqs = List(KeyRevision(StoragePoolState.ConfigKey,
          kvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, updated.encode()))
        tx.update(poolPtr, None, None, reqs, ops)

  atest("getStateForRebalancePlanning omits a pool whose migration is InProgress"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- net.createSecondDevice()
      setState <- client.getStorageDeviceSetState(setId)
      before <- State.getStateForRebalancePlanning(client, setState)

      _ <- setMigration(poolId, Some(StoragePoolState.Migration(
             StorageDeviceSetId(java.util.UUID.randomUUID()),
             StoragePoolState.MigrationStatus.InProgress)))
      _ <- waitForTransactionsToComplete()

      after <- State.getStateForRebalancePlanning(client, setState)
    yield
      before.pools.keySet should contain(poolId)
      after.pools.keySet should not contain poolId
      // Its stores also stop being movable candidates on every device.
      after.devices.values.flatMap(_.stores.keys).count(_.poolId == poolId) should be(0)
      // Device capacity is self-reported, so it is unaffected by the exclusion.
      after.devices.keySet should be(before.devices.keySet)

  atest("getStateForRebalancePlanning keeps a pool whose migration is Complete"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- setMigration(poolId, Some(StoragePoolState.Migration(
             StorageDeviceSetId(java.util.UUID.randomUUID()),
             StoragePoolState.MigrationStatus.Complete)))
      _ <- waitForTransactionsToComplete()
      setState <- client.getStorageDeviceSetState(setId)
      planning <- State.getStateForRebalancePlanning(client, setState)
    yield
      planning.pools.keySet should contain(poolId)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *PoolMigrationPlanningSuite -- -z "omits a pool whose migration"'`
Expected: FAIL — `after.pools.keySet` still contains the bootstrap pool.

- [ ] **Step 3: Write the implementation**

In `getStateForRebalancePlanning`, replace the `yield` block body (from `val poolKvos = poolPairs.toMap` through `PlanningState(devices, pools)`) with:

```scala
      yield
        val poolKvos = poolPairs.toMap

        // A pool with a migration in progress is owned by MigratePoolToSetDurableTask for the
        // duration; the rebalancer must not plan moves for its stores. Omitting it from `pools`
        // and its stores from every device makes Plan.computePlan skip them across all three
        // phases with no changes of its own. Device currentUsage/totalSize are self-reported by
        // StorageDeviceState rather than summed from stores, so fill ratios stay accurate.
        val migratingPools: Set[PoolId] =
          poolKvos.collect:
            case (poolId, kvos)
              if StoragePoolState(kvos).migration.exists(
                   _.status == StoragePoolState.MigrationStatus.InProgress) => poolId
          .toSet

        val plannedPoolIds = poolIds -- migratingPools

        def storeSize(storeId: StoreId): Long =
          poolKvos.get(storeId.poolId)
            .flatMap(_.contents.get(StoragePoolState.getStoreUsageKey(storeId.poolIndex)))
            .map(vs => byte2long(vs.value.bytes))
            .getOrElse(0L)

        // One Store per owned store id, shared between the device and pool views.
        val storesById: Map[StoreId, Store] =
          deviceStates.flatMap: ds =>
            ownedStores(ds)
              .filterNot((sid, _) => migratingPools.contains(sid.poolId))
              .map((sid, entry) => sid -> Store(sid, storeSize(sid), entry.status))
          .toMap

        val devices: Map[StorageDeviceId, Device] =
          deviceStates.map: ds =>
            val itsStores = ownedStores(ds).keys
              .filter(storesById.contains)
              .map(sid => sid -> storesById(sid)).toMap
            ds.storageDeviceId -> Device(ds.storageDeviceId, ds.hostId, ds.currentUsage, ds.totalSize, itsStores)
          .toMap

        val pools: Map[PoolId, Pool] =
          plannedPoolIds.map: poolId =>
            val ida = StoragePoolState(poolKvos(poolId)).ida
            val poolStores = storesById.filter((sid, _) => sid.poolId == poolId)
            poolId -> Pool(poolId, ida, poolStores)
          .toMap

        PlanningState(devices, pools)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *PoolMigrationPlanningSuite *RebalancingServiceSuite *RebalancePlanSuite'`
Expected: PASS — the new tests plus the existing rebalancing suites (the exclusion must not change behavior for non-migrating pools).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala \
        src/test/scala/org/aspen_ddp/aspen/common/rebalancing/PoolMigrationPlanningSuite.scala
git commit -m "feat: exclude migrating pools from rebalance planning"
```

---

### Task 6: `SetRebalanceDurableTask` drops transfers for pools that left planning state

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceDurableTask.scala:72-94`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/PoolMigrationPlanningSuite.scala` (append)

**Interfaces:**
- Consumes: the Task 5 exclusion.
- Produces: no signature change. `drive` gains a branch that removes an unstartable pending transfer whose pool is absent from `planningState.pools`.

**Why this is needed even though Task 8 strips the list:** `migratePoolToSet`'s strip and `RebalancingDurableService.enroll` are both revision-checked on the *set* object, so if `enroll` commits a fresh plan just after the strip, that plan can still contain migrating stores. Without this backstop, `TransferSafety.isSafe` would return `false` forever (`pools.get(poolId)` is `None`) and the set's entire rebalance would wedge.

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/PoolMigrationPlanningSuite.scala`. Add these imports:

```scala
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetState
import org.aspen_ddp.aspen.compute.{DurableTaskPointer, TaskStopped}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.Value

import scala.concurrent.duration.{Duration, MILLISECONDS}
```

Then:

```scala
  atest("SetRebalanceDurableTask drops a pending transfer whose pool left planning state"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId

    for
      _ <- net.createSecondDevice()
      poolState <- client.getStoragePoolState(poolId)
      storeId = StoreId(poolId, 0.toByte)
      source = poolState.stores(0).storageDeviceId

      // Enroll a transfer for the pool, then mark the pool as migrating -- the race the
      // backstop exists for.
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             for
               setPtr <- client.getStorageDeviceSetPointer(setId)
               dos <- client.read(setPtr)
             yield
               val updated = StorageDeviceSetState(dos).copy(
                 pendingTransfers = List((storeId, source, net.secondDeviceId)))
               tx.overwrite(setPtr, dos.revision, DataBuffer(updated.toBytes))
      _ <- setMigration(poolId, Some(StoragePoolState.Migration(
             StorageDeviceSetId(java.util.UUID.randomUUID()),
             StoragePoolState.MigrationStatus.InProgress)))
      _ <- waitForTransactionsToComplete()

      taskPtr <- allocateTaskStateObject()
      task = new SetRebalanceDurableTask(DurableTaskPointer(taskPtr), client, setId,
               Duration(50, MILLISECONDS))
      _ <- task.completed

      finalSet <- client.getStorageDeviceSetState(setId)
    yield
      finalSet.pendingTransfers shouldBe empty

  /** An empty KV object to serve as a durable task's state object. */
  private def allocateTaskStateObject(): Future[org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        pool <- client.getStoragePool(Radicle.poolId)
        allocator = new PoolObjectAllocator(client, pool)
        ptr <- allocator.allocateKeyValueObject(
                 Map(org.aspen_ddp.aspen.common.objects.Key(99) -> Value(Array[Byte](0))))
      yield ptr
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *PoolMigrationPlanningSuite -- -z "drops a pending transfer"'`
Expected: FAIL — timeout waiting on `task.completed`; the transfer is never safe and never removed.

- [ ] **Step 3: Write the implementation**

In `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceDurableTask.scala`, replace the `else` branch of `drive` (lines 81-94) with:

```scala
          else
            client.getStorageDeviceSetState(setId).flatMap: setState =>
              State.getStateForRebalancePlanning(client, setState).flatMap: planningState =>
                val notStarted = classified.collect { case (t, Phase.NotStarted) => t }

                // A pool that has left planning state (its migration is InProgress) is owned by
                // MigratePoolToSetDurableTask. isSafe would return false forever for its
                // transfers -- pools.get(poolId) is None -- wedging this set's whole rebalance.
                // Drop them instead. migratePoolToSet strips this list at migration start; this
                // is the backstop for a plan enrolled just after that strip committed.
                notStarted.find(t => !planningState.pools.contains(t._1.poolId)) match
                  case Some(t) =>
                    logger.info(s"Rebalance set $setId: dropping transfer for ${t._1}; " +
                      s"its pool is no longer in planning state")
                    removePending(t).map(_ => processNext())

                  case None =>
                    val offline = client.offlineHosts()
                    notStarted.find(t => TransferSafety.isSafe(planningState, offline,
                        Plan.Transfer(t._1, t._2, t._3))) match
                      case Some((storeId, _, toDevice)) =>
                        client.transferStore(storeId, toDevice).failed.foreach: err =>
                          logger.warn(s"Rebalance set $setId: transferStore for $storeId failed " +
                            s"(will retry via poll): $err")
                        scheduleRecheck()
                      case None =>
                        scheduleRecheck()
                    Future.unit
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *PoolMigrationPlanningSuite *RebalancingServiceSuite'`
Expected: PASS — the new test plus the existing rebalancing service suite.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/SetRebalanceDurableTask.scala \
        src/test/scala/org/aspen_ddp/aspen/common/rebalancing/PoolMigrationPlanningSuite.scala
git commit -m "fix: drop rebalance transfers for pools excluded from planning state"
```

---

### Task 7: `MigratePoolToSetDurableTask` and registration

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetDurableTask.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/TypeFactories.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala`

**Interfaces:**
- Consumes: `StoragePoolState.Migration`/`MigrationStatus` (Task 1); `StorageDeviceSetState.selectDeviceForStore` and `collectMemberDevices` (Task 3); `State.getStateForPoolMigration` (Task 4).
- Produces:
  - `object MigratePoolToSetDurableTask extends DurableTaskFactory` with
    - `val typeUUID: UUID = UUID.fromString("3c6e9a14-8b7d-4f02-a5e1-9d0b2c48f736")`
    - `private[aspen] val PoolIdKey: Key = Key(Array[Byte](0))`
    - `val DefaultPollPeriod: Duration = Duration(2, MINUTES)`
    - `@volatile var pollPeriod: Duration` (test seam)
    - `class MigrationSuperseded extends Throwable`
    - `def prepareSystemTask(client: AspenClient, poolId: PoolId)(using tx: Transaction): Future[Unit]` — consumed by Task 8.
  - `class MigratePoolToSetDurableTask(taskPointer: DurableTaskPointer, client: AspenClient, poolId: PoolId, pollPeriod: Duration) extends DurableTask` — constructed directly by the Task 9 tests.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.TypeFactories
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory}

import scala.concurrent.ExecutionContext

class MigratePoolToSetSuite extends IntegrationTestSuite:

  atest("MigratePoolToSetDurableTask is registered in the client type registry"):
    given ExecutionContext = executionContext
    // SystemTaskExecutorService reconstructs a task by looking its type UUID up here; an
    // unregistered factory makes prepareSystemDurableTask StopRetrying.
    val factory = client.typeRegistry.getType[DurableTaskFactory](
      MigratePoolToSetDurableTask.typeUUID)
    factory should be(Some(MigratePoolToSetDurableTask))
    TypeFactories.factories should contain(MigratePoolToSetDurableTask)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *MigratePoolToSetSuite'`
Expected: FAIL — compile error, `not found: MigratePoolToSetDurableTask`.

- [ ] **Step 3: Write the task**

Create `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetDurableTask.scala`:

```scala
package org.aspen_ddp.aspen.common.metadata.management

import org.aspen_ddp.aspen.client.{AspenClient, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.rebalancing.{Plan, State, TransferSafety}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.{NoTask, ScheduledTask}
import org.aspen_ddp.aspen.common.util.{byte2long, byte2uuid, ignoreExtraCallsWhileRunning, runBoundedParallel, uuid2byte}
import org.aspen_ddp.aspen.compute.{DurableTask, DurableTaskFactory, DurableTaskPointer, TaskExecutor, TaskStopped}
import scribe.Logging

import java.util.UUID
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object MigratePoolToSetDurableTask extends DurableTaskFactory:

  val typeUUID: UUID = UUID.fromString("3c6e9a14-8b7d-4f02-a5e1-9d0b2c48f736")

  /** The task's only state: the pool being migrated. Below DurableTask.ReservedFromKeyId and
   *  distinct from SimpleTaskExecutor.TaskTypeKey, which is where prepareSystemDurableTask
   *  writes the type UUID. The target set is not stored here -- it is re-read from pool state
   *  every pass, which is what makes retargeting free. */
  private[aspen] val PoolIdKey: Key = Key(Array[Byte](0))

  val DefaultPollPeriod: Duration = Duration(2, MINUTES)

  /** Overridable poll period (test seam; mirrors RebalancingDurableService.pollPeriod). */
  @volatile var pollPeriod: Duration = DefaultPollPeriod

  /** The recorded migration changed under us (retargeted, or already completed by another
   *  instance). Not an error: the completion write is simply skipped. */
  class MigrationSuperseded(poolId: PoolId)
      extends Throwable(s"migration of pool ${poolId.uuid} was retargeted or already completed")

  def createTask(client: AspenClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState],
                 taskExecutor: TaskExecutor): DurableTask =
    val poolId = PoolId(byte2uuid(state(PoolIdKey).value.bytes))
    new MigratePoolToSetDurableTask(pointer, client, poolId, pollPeriod)

  /** Stage enrollment of a migration task for `poolId` inside the caller's transaction. */
  def prepareSystemTask(client: AspenClient, poolId: PoolId)(using tx: Transaction): Future[Unit] =
    client.prepareSystemDurableTask(typeUUID, Map(PoolIdKey -> uuid2byte(poolId.uuid)))


/** Drives one pool's migration onto its target StorageDeviceSet, serially: one store transfer
 *  in flight at a time.
 *
 *  Progress needs no record of its own. StoreManager rewrites `poolState.stores(poolIndex)` to
 *  the new (host, device) in the same transaction that completes a transfer, so a store is
 *  migrated iff its device is a member of the target set. Crash recovery is therefore free: a
 *  fresh instance re-reads the pool and picks up exactly where the last one left off.
 *
 *  The task holds only the pool id; the target set is re-read every pass, so a retarget is
 *  picked up on the next tick with no task-side change at all.
 */
class MigratePoolToSetDurableTask(
    val taskPointer: DurableTaskPointer,
    client: AspenClient,
    poolId: PoolId,
    pollPeriod: Duration
) extends DurableTask with Logging:

  import MigratePoolToSetDurableTask.MigrationSuperseded

  private given ExecutionContext = client.clientContext

  private val promise = Promise[Option[AnyRef]]()
  def completed: Future[Option[AnyRef]] = promise.future

  private var pollTask: ScheduledTask = NoTask

  /** Single-flight guard: a tick arriving while a pass is still running is dropped
   *  (TODO.txt's "protect against pileups", on the async-body side; the cancel-then-schedule
   *  in scheduleRecheck covers the timer side).
   *
   *  processNext MUST never return a failed Future: ignoreExtraCallsWhileRunning clears its
   *  flag from `Future.foreach`, which does not run on failure, so one failure would wedge the
   *  wrapper permanently. */
  private val run: () => Unit = ignoreExtraCallsWhileRunning(processNext())

  // Begin driving immediately.
  run()

  override protected def onStop(): Unit = synchronized:
    pollTask.cancel()
    if !promise.isCompleted then promise.failure(new TaskStopped)

  private def scheduleRecheck(): Unit = synchronized:
    if !isStopped then
      pollTask.cancel()
      pollTask = client.backgroundTaskManager.schedule(pollPeriod):
        run()

  private def finishOk(): Unit = synchronized:
    if !promise.isCompleted then promise.success(None)

  private def processNext(): Future[Unit] =
    if isStopped then
      synchronized { if !promise.isCompleted then promise.failure(new TaskStopped) }
      Future.unit
    else
      driveMigration().recover:
        case err =>
          logger.warn(s"Pool migration ${poolId.uuid}: transient error, will retry: $err")
          scheduleRecheck()

  private def driveMigration(): Future[Unit] =
    for
      poolPtr <- client.getStoragePoolPointer(poolId)
      poolKvos <- client.read(poolPtr, "pool migration")
      poolState = StoragePoolState(poolKvos)
      result <- poolState.migration match
        // Nothing to do, and a re-dispatched task after completion is a no-op.
        case None =>
          Future.successful(finishOk())

        case Some(m) if m.status == StoragePoolState.MigrationStatus.Complete =>
          Future.successful(finishOk())

        case Some(m) =>
          for
            targetSet <- client.getStorageDeviceSetState(m.targetSet)
            members <- targetSet.collectMemberDevices(client)
            r <- step(poolPtr, poolKvos, poolState, m, targetSet, members)
          yield r
    yield result

  private def step(poolPtr: KeyValueObjectPointer,
                   poolKvos: KeyValueObjectState,
                   poolState: StoragePoolState,
                   migration: StoragePoolState.Migration,
                   targetSet: StorageDeviceSetState,
                   targetMembers: Set[StorageDeviceId]): Future[Unit] =

    val remaining: List[Byte] = poolState.stores.indices
      .filterNot(i => targetMembers.contains(poolState.stores(i).storageDeviceId))
      .map(_.toByte)
      .toList

    if remaining.isEmpty then
      markComplete(poolPtr, migration)
    else
      val storeDevices = poolState.stores.map(_.storageDeviceId).distinct.toSeq
      runBoundedParallel(storeDevices, State.DefaultMaxConcurrentReads)(client.getStorageDeviceState)
        .flatMap: deviceStates =>
          // One transfer at a time. The pool's device of record stays the source until the
          // transfer completes, so TransferringOut there is the signal; TransferringIn is
          // checked too in case a device serves as both.
          val inFlight = deviceStates.exists: ds =>
            ds.stores.exists: (sid, entry) =>
              sid.poolId == poolId &&
                (entry.status == StorageDeviceState.StoreStatus.TransferringIn ||
                 entry.status == StorageDeviceState.StoreStatus.TransferringOut)

          if inFlight then
            scheduleRecheck()
            Future.unit
          else
            tryStores(poolKvos, poolState, targetSet, remaining)

  /** Try each remaining store in poolIndex order until one transfer starts. */
  private def tryStores(poolKvos: KeyValueObjectState,
                        poolState: StoragePoolState,
                        targetSet: StorageDeviceSetState,
                        remaining: List[Byte]): Future[Unit] =

    val poolDevices = poolState.stores.map(_.storageDeviceId).toSet

    def attempt(candidates: List[Byte]): Future[Unit] = candidates match
      case Nil =>
        scheduleRecheck()
        Future.unit

      case poolIndex :: tail =>
        val storeId = StoreId(poolId, poolIndex)
        val sourceDevice = poolState.stores(poolIndex).storageDeviceId
        val requiredSize = poolKvos.contents
          .get(StoragePoolState.getStoreUsageKey(poolIndex))
          .map(vs => byte2long(vs.value.bytes))
          .getOrElse(0L)

        val started = for
          destination <- targetSet.selectDeviceForStore(requiredSize, poolDevices, client)
          ok <-
            if destination == sourceDevice then
              // Structurally unreachable -- selection only returns target-set devices and this
              // store's device is not one -- but the pool state driving that reasoning can be
              // stale, and transferStore would throw InvalidDestination. Skip instead.
              Future.successful(false)
            else
              State.getStateForPoolMigration(client, poolId, destination).flatMap: planning =>
                if TransferSafety.isSafe(planning, client.offlineHosts(),
                     Plan.Transfer(storeId, sourceDevice, destination)) then
                  client.transferStore(storeId, destination).map(_ => true)
                else
                  Future.successful(false)
        yield ok

        started.transformWith:
          case Success(true) =>
            scheduleRecheck()
            Future.unit

          case Success(false) =>
            attempt(tail)

          case Failure(err) =>
            // AllocationError (no device with room), a lost transferStore race, a read failure:
            // all transient from this task's point of view. Try the next store.
            logger.warn(s"Pool migration ${poolId.uuid}: could not start $storeId: $err")
            attempt(tail)

    attempt(remaining)

  /** Record the migration as Complete, then finish.
   *
   *  Re-reads inside the transaction and writes only if the migration we acted on is still the
   *  one recorded, which makes this idempotent and keeps it from clobbering a retarget that
   *  landed between passes. Throwing rather than staging nothing also avoids committing an
   *  empty transaction.
   */
  private def markComplete(poolPtr: KeyValueObjectPointer,
                           migration: StoragePoolState.Migration): Future[Unit] =

    def onFail(err: Throwable): Future[Unit] = err match
      case e: MigrationSuperseded => throw StopRetrying(e)

    val done = client.transactUntilSuccessfulWithRecovery(onFail): tx =>
      client.read(poolPtr, "pool migration completion").map: kvos =>
        val current = StoragePoolState(kvos)
        if !current.migration.contains(migration) then
          throw new MigrationSuperseded(poolId)

        val updated = current.copy(migration =
          Some(migration.copy(status = StoragePoolState.MigrationStatus.Complete)))
        val reqs = List(KeyRevision(StoragePoolState.ConfigKey,
          kvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, updated.encode()))
        tx.update(poolPtr, None, None, reqs, ops)

    done.transformWith:
      case Success(_) =>
        finishOk()
        Future.unit

      case Failure(_: MigrationSuperseded) =>
        // Retargeted or already complete. Re-drive rather than finish -- a retarget means there
        // is more work. scheduleRecheck, not run(), because run()'s single-flight flag is still
        // held by this very pass and the call would be dropped.
        scheduleRecheck()
        Future.unit

      case Failure(err) =>
        logger.warn(s"Pool migration ${poolId.uuid}: completion transaction failed: $err")
        scheduleRecheck()
        Future.unit
```

- [ ] **Step 4: Register the factory**

Replace `src/main/scala/org/aspen_ddp/aspen/common/TypeFactories.scala` with:

```scala
package org.aspen_ddp.aspen.common

import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.common.metadata.management.{MigratePoolToSetDurableTask, UpdateAllocationGroupUsageTask}

object TypeFactories:

  val factories: List[RegisteredTypeFactory] = List(
    UpdateAllocationGroupUsageTask,
    MigratePoolToSetDurableTask
  )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `sbt 'testOnly *MigratePoolToSetSuite'`
Expected: PASS — one test.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetDurableTask.scala \
        src/main/scala/org/aspen_ddp/aspen/common/TypeFactories.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala
git commit -m "feat: add MigratePoolToSetDurableTask"
```

---

### Task 8: `migratePoolToSet` entry point

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (declaration, near `moveDeviceToSet` at line 174)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` (implementation)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala` (append)

**Interfaces:**
- Consumes: `StoragePoolState.Migration`/`MigrationStatus` (Task 1); `MigratePoolToSetDurableTask.prepareSystemTask` (Task 7).
- Produces: `AspenClient.migratePoolToSet(poolId: PoolId, targetSetId: StorageDeviceSetId): Future[Unit]` — consumed by Task 9's tests and Task 10's CLI.

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala`. Add these imports:

```scala
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceSetId, StoragePoolState}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskServiceState}

import scala.concurrent.Future
```

Then the helper and tests:

```scala
  /** The task state objects currently enrolled with the system task executor. */
  private def enrolledTasks(): Future[List[(java.util.UUID, KeyValueObjectPointer)]] =
    given ExecutionContext = executionContext
    val tkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      vs <- tkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(vs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield enrolled

  atest("migratePoolToSet flips the pointer, swaps assignedPools, and enrolls one task"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val oldSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      targetSetId <- client.createStorageDeviceSet("migration-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      poolState <- client.getStoragePoolState(poolId)
      target <- client.getStorageDeviceSetState(targetSetId)
      old <- client.getStorageDeviceSetState(oldSetId)
      enrolled <- enrolledTasks()
    yield
      poolState.storageDeviceSet should be(targetSetId)
      poolState.migration should be(Some(StoragePoolState.Migration(
        targetSetId, StoragePoolState.MigrationStatus.InProgress)))
      target.assignedPools should contain(poolId)
      old.assignedPools should not contain poolId
      enrolled.size should be(1)

  atest("migratePoolToSet to the pool's current set is a no-op"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      _ <- client.migratePoolToSet(poolId, setId)
      _ <- waitForTransactionsToComplete()

      poolState <- client.getStoragePoolState(poolId)
      set <- client.getStorageDeviceSetState(setId)
      enrolled <- enrolledTasks()
    yield
      poolState.migration should be(None)
      poolState.storageDeviceSet should be(setId)
      set.assignedPools.count(_ == poolId) should be(1)
      enrolled shouldBe empty

  atest("migratePoolToSet fails with NoSuchElementException for an unknown target set"):
    given ExecutionContext = executionContext
    val unknown = StorageDeviceSetId(java.util.UUID.randomUUID())
    recoverToSucceededIf[NoSuchElementException](
      client.migratePoolToSet(PoolId.BootstrapPoolId, unknown))

  atest("retargeting swaps assignedPools to the new target and enrolls no second task"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      firstId <- client.createStorageDeviceSet("first-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      secondId <- client.createStorageDeviceSet("second-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, firstId)
      _ <- waitForTransactionsToComplete()
      _ <- client.migratePoolToSet(poolId, secondId)
      _ <- waitForTransactionsToComplete()

      poolState <- client.getStoragePoolState(poolId)
      first <- client.getStorageDeviceSetState(firstId)
      second <- client.getStorageDeviceSetState(secondId)
      enrolled <- enrolledTasks()
    yield
      poolState.storageDeviceSet should be(secondId)
      poolState.migration.get.targetSet should be(secondId)
      poolState.migration.get.status should be(StoragePoolState.MigrationStatus.InProgress)
      second.assignedPools should contain(poolId)
      first.assignedPools should not contain poolId
      // The running task holds only the pool id and re-reads its target, so it is reused.
      enrolled.size should be(1)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *MigratePoolToSetSuite -- -z "flips the pointer"'`
Expected: FAIL — compile error, `value migratePoolToSet is not a member of AspenClient`.

- [ ] **Step 3: Declare it on `AspenClient`**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, immediately after the `moveDeviceToSet` method, add:

```scala
  /** Begin migrating every store of `poolId` onto devices of `targetSetId`.
   *
   *  One transaction flips the pool's `storageDeviceSet`, records
   *  `Migration(targetSetId, InProgress)`, swaps the pool between the two sets'
   *  `assignedPools`, strips the pool's stores from both sets' `pendingTransfers`, and (on a
   *  first call) enrolls a MigratePoolToSetDurableTask with the system task executor. There is
   *  therefore no window in which a pool is InProgress with no task driving it.
   *
   *  Returns as soon as that transaction commits; progress is observable via the pool's
   *  `migration` field. A call naming the pool's current set is a silent no-op. A call against
   *  a pool that is already migrating retargets it, reusing the running task.
   *
   *  Fails with NoSuchElementException if the pool or the target set does not exist.
   */
  def migratePoolToSet(poolId: PoolId, targetSetId: StorageDeviceSetId): Future[Unit]
```

- [ ] **Step 4: Implement it on `BaseAspenClient`**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`, add (next to the other pool/device-set methods):

```scala
  /** Signals that the pool's set pointer already equals the target. Thrown from inside the
   *  transaction so nothing is staged: Aspen has no empty transactions. Recovered to unit. */
  private class SameSetNoOp extends Throwable("pool is already assigned to the target set")

  override def migratePoolToSet(poolId: PoolId,
                                targetSetId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = clientContext

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)
      case e: SameSetNoOp => throw StopRetrying(e)

    val migrated = transactUntilSuccessfulWithRecovery(onFail): tx =>
      given Transaction = tx

      for
        poolPtr <- getStoragePoolPointer(poolId)
        poolKvos <- read(poolPtr, "migrate pool to set")
        vs = poolKvos.contents(StoragePoolState.ConfigKey)
        poolState = StoragePoolState(vs.value.bytes)
        // `previousSet` is the pool's original set on a first call and the now-abandoned
        // target on a retarget. One write path covers both.
        previousSetId = poolState.storageDeviceSet
        _ <-
          if previousSetId == targetSetId then
            throw new SameSetNoOp
          else
            for
              targetPtr <- getStorageDeviceSetPointer(targetSetId)
              targetDos <- read(targetPtr, "migrate pool to set")
              previousPtr <- getStorageDeviceSetPointer(previousSetId)
              previousDos <- read(previousPtr, "migrate pool to set")
              _ <-
                val poolStoreIds =
                  poolState.stores.indices.map(i => StoreId(poolId, i.toByte)).toSet

                // Pool config: flip the pointer and record the migration. The pointer flips at
                // the start so a store lost mid-migration is rebuilt into the target set
                // directly (selectDeviceForRebuild uses the pool's set) rather than into the
                // old set and then migrated again.
                val updatedPool = poolState.copy(
                  storageDeviceSet = targetSetId,
                  migration = Some(StoragePoolState.Migration(
                    targetSetId, StoragePoolState.MigrationStatus.InProgress)))
                val poolReqs = List(KeyRevision(StoragePoolState.ConfigKey, vs.revision))
                val poolOps = List(Insert(StoragePoolState.ConfigKey, updatedPool.encode()))
                tx.update(poolPtr, None, None, poolReqs, poolOps)

                // Target set: gains the pool (dedup-guarded); defensively strip any pending
                // transfers for it.
                val target = StorageDeviceSetState(targetDos)
                val newTarget = target.copy(
                  assignedPools = poolId :: target.assignedPools.filter(_ != poolId),
                  pendingTransfers = target.pendingTransfers.filterNot:
                    (sid, _, _) => poolStoreIds.contains(sid))
                tx.overwrite(targetPtr, targetDos.revision, DataBuffer(newTarget.toBytes))

                // Previous set: loses the pool (self-healing if absent) and its pending
                // transfers, which can never become safe once the pool leaves planning state.
                val previous = StorageDeviceSetState(previousDos)
                val newPrevious = previous.copy(
                  assignedPools = previous.assignedPools.filter(_ != poolId),
                  pendingTransfers = previous.pendingTransfers.filterNot:
                    (sid, _, _) => poolStoreIds.contains(sid))
                tx.overwrite(previousPtr, previousDos.revision, DataBuffer(newPrevious.toBytes))

                // Enroll only on a first call. A retarget reuses the running task, which holds
                // only the pool id and re-reads its target every pass.
                val alreadyRunning = poolState.migration.exists(
                  _.status == StoragePoolState.MigrationStatus.InProgress)
                if alreadyRunning then
                  Future.unit
                else
                  MigratePoolToSetDurableTask.prepareSystemTask(this, poolId)
            yield ()
      yield ()

    migrated.recover:
      case _: SameSetNoOp => ()
```

Ensure these are imported in `BaseAspenClient.scala` (add whichever are missing):

```scala
import org.aspen_ddp.aspen.common.metadata.management.MigratePoolToSetDurableTask
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceSetId, StorageDeviceSetState, StoragePoolState}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.DataBuffer
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `sbt 'testOnly *MigratePoolToSetSuite'`
Expected: PASS — five tests.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala
git commit -m "feat: add AspenClient.migratePoolToSet entry point"
```

---

### Task 9: End-to-end migration tests

**Files:**
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala` (append)
- Modify: only if a defect surfaces (see Step 3)

**Interfaces:**
- Consumes: `MigratePoolToSetDurableTask` (Task 7), `migratePoolToSet` (Task 8), `TestNetwork.createSecondDevice` / `secondDeviceId` / `completeInFlightTransfers` (existing).
- Produces: no production interfaces. This task proves the loop, the level-agnosticism, the zero-transfer case, and crash resume.

**Context the implementer needs:** `TestNetwork` runs `Replication(3, 2)` with three bootstrap-pool stores on `StorageDeviceId.BootstrapStorageDeviceId`. `TransferSafety` allows a transfer when `usable - 1 >= writeThreshold`, i.e. `3 - 1 >= 2`, so each transfer is safe. `net.completeInFlightTransfers(setId)` scans **that set's `memberDevices`** for `TransferringIn` entries, so it must be given the level-0 set holding the destination device, not a level-1 parent. `SystemTaskExecutorService` is not claimed by any host in `IntegrationTestSuite`, so enrolled tasks do not run on their own — these tests construct the task directly from its enrolled state object, which is also what makes the crash-resume test possible.

- [ ] **Step 1: Write the failing tests**

Append to `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala`. Add these imports:

```scala
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetState}
import org.aspen_ddp.aspen.compute.DurableTaskPointer

import scala.concurrent.Promise
import scala.concurrent.duration.{Duration, MILLISECONDS}
```

Then the helpers and tests:

```scala
  private val fastPoll = Duration(50, MILLISECONDS)

  /** Build a task instance over the state object migratePoolToSet enrolled. */
  private def taskForEnrolled(poolId: PoolId): Future[MigratePoolToSetDurableTask] =
    given ExecutionContext = executionContext
    enrolledTasks().map: enrolled =>
      enrolled.size should be(1)
      new MigratePoolToSetDurableTask(
        DurableTaskPointer(enrolled.head._2), client, poolId, fastPoll)

  /** Repeatedly complete in-flight transfers landing on `destinationSetId`'s member devices
   *  until the task completes. Fails on timeout. */
  private def driveUntilComplete(task: MigratePoolToSetDurableTask,
                                 destinationSetId: StorageDeviceSetId,
                                 timeout: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      if task.completed.isCompleted then
        task.completed.map(_ => ())
      else if System.nanoTime() > deadline then
        client.getStoragePoolState(PoolId.BootstrapPoolId).flatMap: ps =>
          Future.failed(new AssertionError(
            s"migration did not complete; migration=${ps.migration} " +
              s"stores=${ps.stores.map(_.storageDeviceId.uuid).mkString(",")}"))
      else
        net.completeInFlightTransfers(destinationSetId).flatMap: _ =>
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(fastPoll)(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  /** Complete in-flight transfers for a bounded wall-clock window without waiting for the
   *  task to finish. Used to let a migration make partial progress. */
  private def pumpFor(destinationSetId: StorageDeviceSetId, window: Duration): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + window.toNanos
    def loop(): Future[Unit] =
      if System.nanoTime() > deadline then Future.unit
      else
        net.completeInFlightTransfers(destinationSetId).flatMap: _ =>
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(fastPoll)(p.success(()))
          p.future.flatMap(_ => loop())
    loop()

  atest("level-0 migration moves every store and marks the migration Complete"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      targetSetId <- client.createStorageDeviceSet("nvme", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, targetSetId)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      _ <- driveUntilComplete(task, targetSetId, Duration(30000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.secondDeviceId)
      poolState.migration should be(Some(StoragePoolState.Migration(
        targetSetId, StoragePoolState.MigrationStatus.Complete)))

  atest("a level-1 target set works by the same path"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      parentId <- client.createStorageDeviceSet("tier-1", level = 1, parent = None)
      _ <- waitForTransactionsToComplete()
      // createStorageDeviceSet links the child into the parent's memberSets.
      childId <- client.createStorageDeviceSet("tier-1-leaf", level = 0, parent = Some(parentId))
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, childId)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, parentId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      // Destinations land on the level-0 child's member devices.
      _ <- driveUntilComplete(task, childId, Duration(30000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.secondDeviceId)
      poolState.migration.get.status should be(StoragePoolState.MigrationStatus.Complete)

  atest("migration completes with zero transfers when the old set is already inside the target"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    val bootstrapSetId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      parentId <- client.createStorageDeviceSet("umbrella", level = 1, parent = None)
      _ <- waitForTransactionsToComplete()

      // Adopt the bootstrap set as a member of the level-1 set. There is no client API for
      // reparenting an existing set, so write memberSets directly.
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             for
               parentPtr <- client.getStorageDeviceSetPointer(parentId)
               dos <- client.read(parentPtr)
             yield
               val updated = StorageDeviceSetState(dos).copy(memberSets = List(bootstrapSetId))
               tx.overwrite(parentPtr, dos.revision, DataBuffer(updated.toBytes))
      _ <- waitForTransactionsToComplete()

      before <- client.getStoragePoolState(poolId)
      _ <- client.migratePoolToSet(poolId, parentId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      _ <- task.completed

      poolState <- client.getStoragePoolState(poolId)
    yield
      // Every store was already inside the target, so nothing moved.
      poolState.stores.map(_.storageDeviceId).toList should be(
        before.stores.map(_.storageDeviceId).toList)
      poolState.migration should be(Some(StoragePoolState.Migration(
        parentId, StoragePoolState.MigrationStatus.Complete)))

  atest("a fresh task instance resumes a migration after the first is stopped"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      targetSetId <- client.createStorageDeviceSet("resume-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, targetSetId)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      first <- taskForEnrolled(poolId)
      _ <- pumpFor(targetSetId, Duration(1000, MILLISECONDS))
      _ = first.stop()

      midPool <- client.getStoragePoolState(poolId)

      // Nothing was written to say where the migration got to -- the pool's own stores array
      // is the progress record, so a fresh instance simply picks up.
      second <- taskForEnrolled(poolId)
      _ <- driveUntilComplete(second, targetSetId, Duration(30000, MILLISECONDS))

      poolState <- client.getStoragePoolState(poolId)
    yield
      midPool.migration.get.status should be(StoragePoolState.MigrationStatus.InProgress)
      poolState.stores.foreach: entry =>
        entry.storageDeviceId should be(net.secondDeviceId)
      poolState.migration.get.status should be(StoragePoolState.MigrationStatus.Complete)

  atest("a completed migration re-dispatched as a task finishes immediately without changes"):
    given ExecutionContext = executionContext
    val poolId = PoolId.BootstrapPoolId
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      targetSetId <- client.createStorageDeviceSet("idempotent-target", level = 0, parent = None)
      _ <- waitForTransactionsToComplete()
      _ <- client.moveDeviceToSet(net.secondDeviceId, targetSetId)
      _ <- waitForTransactionsToComplete()

      _ <- client.migratePoolToSet(poolId, targetSetId)
      _ <- waitForTransactionsToComplete()

      task <- taskForEnrolled(poolId)
      _ <- driveUntilComplete(task, targetSetId, Duration(30000, MILLISECONDS))
      after <- client.getStoragePoolState(poolId)

      // Re-dispatch: SystemTaskExecutorService may restart a task whose state object is still
      // enrolled.
      again <- taskForEnrolled(poolId)
      _ <- again.completed
      afterAgain <- client.getStoragePoolState(poolId)
    yield
      afterAgain.migration should be(after.migration)
      afterAgain.stores.map(_.storageDeviceId).toList should be(
        after.stores.map(_.storageDeviceId).toList)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly *MigratePoolToSetSuite'`
Expected: FAIL — the five new tests fail (the earlier tests from Tasks 7 and 8 still pass). Compile errors are also possible if any helper name collides; fix those first, then look at real failures.

- [ ] **Step 3: Fix whatever the tests expose**

These tests are the first exercise of the Task 7 loop end to end. Expected failure shapes and where they point:

- Task never completes, pool stores unchanged → the done-check partition in `step`, or `collectMemberDevices` on the target set.
- Task completes but `migration.status` stays `InProgress` → `markComplete`'s guard (`current.migration.contains(migration)`) rejecting a still-valid migration.
- One store moves and then progress stalls → the in-flight detection in `step` never clearing, or `scheduleRecheck` not firing after a `transferStore`.
- Progress stalls permanently after a single logged warning → the `ignoreExtraCallsWhileRunning` wedge; `processNext` returned a failed Future. Its `.recover` must cover every path.

Make the minimal production fix in `MigratePoolToSetDurableTask.scala`. Do not weaken the tests.

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly *MigratePoolToSetSuite'`
Expected: PASS — ten tests.

- [ ] **Step 5: Run the full suite**

Run: `sbt test`
Expected: PASS — nothing regressed.

- [ ] **Step 6: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetSuite.scala \
        src/main/scala/org/aspen_ddp/aspen/common/metadata/management/MigratePoolToSetDurableTask.scala
git commit -m "test: end-to-end pool migration, level-1 targets, and crash resume"
```

---

### Task 10: `migrate-pool` CLI command and `show-pool` migration status

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — parser (after the `rebalance` block ending near line 556), dispatch (near line 711), a new `migrate_pool` function (after `move_device_to_set`, near line 1497), and `formatPoolState` (line 1848)
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala` (append)

**Interfaces:**
- Consumes: `AspenClient.migratePoolToSet` (Task 8), `StoragePoolState.migration` (Task 1).
- Produces: `Main.migrate_pool(bootstrapConfigFile: os.Path, poolRef: String, setRef: String): Int`, and a `Migration:` line in `Main.formatPoolState` output when the pool has one.

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala` (inside the class):

```scala
  test("formatPoolState renders migration status when a migration is recorded"):
    val poolId = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val target = StorageDeviceSetId(UUID.fromString("66666666-6666-6666-6666-666666666666"))
    val s = StoragePoolState(poolId, "mypool", Replication(3, 2), None, Array.empty,
      RocksDBConfig(), setId,
      migration = Some(StoragePoolState.Migration(
        target, StoragePoolState.MigrationStatus.InProgress)))
    val out = Main.formatPoolState(s, Some("fast-nvme"))
    out should include ("Migration:")
    out should include ("InProgress")
    out should include ("66666666-6666-6666-6666-666666666666")

  test("formatPoolState omits the migration line when there is no migration"):
    val poolId = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val s = StoragePoolState(poolId, "mypool", Replication(3, 2), None, Array.empty,
      RocksDBConfig(), setId)
    Main.formatPoolState(s, None) should not include "Migration:"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *MainSuite -- -z "migration status"'`
Expected: FAIL — output does not include `"Migration:"`.

- [ ] **Step 3: Add the `formatPoolState` line**

In `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, in `formatPoolState`, insert immediately after the `Device Set:` line:

```scala
    lines += s"  Device Set:   $set (${s.storageDeviceSet.uuid})"
    s.migration.foreach: m =>
      lines += s"  Migration:    ${m.status} -> ${m.targetSet.uuid}"
```

- [ ] **Step 4: Run the formatter tests to verify they pass**

Run: `sbt 'testOnly *MainSuite'`
Expected: PASS — the two new tests plus every pre-existing `MainSuite` test.

- [ ] **Step 5: Register the `migrate-pool` command**

In the scopt parser, immediately after the `cmd("rebalance")` block:

```scala
      cmd("migrate-pool").text("Migrates a storage pool to a different storage device set").
        action((_, c) => c.copy(mode = "migrate-pool")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<pool-name-or-uuid>").text("Name or UUID of the pool to migrate").
            action((x, c) => c.copy(poolName = x)),

          arg[String]("<set-name-or-uuid>").text("Name or UUID of the target device set").
            action((x, c) => c.copy(deviceSetName = x)),
        )
```

Add the dispatch case immediately after `case "rebalance" => ...`:

```scala
            case "migrate-pool" => migrate_pool(bootstrapConfigPath, cfg.poolName, cfg.deviceSetName)
```

- [ ] **Step 6: Add the `migrate_pool` function**

Insert after `move_device_to_set`:

```scala
  def migrate_pool(bootstrapConfigFile: os.Path,
                   poolRef: String,
                   setRef: String): Int =

    configureLogging()

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = for
      poolId      <- resolveRef(poolRef, PoolId(_), client.getStoragePoolId)
      targetSetId <- resolveRef(setRef, StorageDeviceSetId(_), client.getStorageDeviceSetId)
      _           <- client.migratePoolToSet(poolId, targetSetId)
    yield ()

    // getStoragePoolId / getStorageDeviceSetId / getStoragePoolPointer /
    // getStorageDeviceSetPointer all throw NoSuchElementException for unknown names and ids.
    def reportError(cause: Throwable): Unit = cause match
      case _: NoSuchElementException =>
        println(s"Error: pool '$poolRef' or set '$setRef' not found")
      case e =>
        println(s"Error starting pool migration: ${e.getMessage}")

    awaitAndReport(f):
      case Success(_) =>
        println(s"Migration of pool '$poolRef' to set '$setRef' started. " +
          s"Track progress with: show-pool $poolRef")
      case Failure(err) => reportError(err)
```

- [ ] **Step 7: Verify the CLI compiles and the command is reachable**

Run: `sbt compile && sbt 'testOnly *MainSuite'`
Expected: compilation succeeds; `MainSuite` passes.

Then check the command surfaces in help:

```bash
sbt 'runMain org.aspen_ddp.aspen.cmdline.Main --help' 2>&1 | grep -A 2 'migrate-pool'
```

Expected: the `migrate-pool` command and its description appear.

- [ ] **Step 8: Run the full suite**

Run: `sbt test`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala \
        src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala
git commit -m "feat: add migrate-pool command and show-pool migration status"
```

- [ ] **Step 10: Remove the completed TODO.txt entry**

Delete lines 2–17 of `TODO.txt` (the "Reassignment of a pool to a new set" block), leaving "Rebalancing as a passive background task" as the top entry.

```bash
git add TODO.txt
git commit -m "chore: drop completed pool-to-set reassignment TODO"
```

---

## Self-Review

**Spec coverage** — every section maps to a task:

| Spec section | Task |
|---|---|
| §1 Data model (`migration`, protobuf field 11, codec, round-trip) | 1 |
| §2 Entry point (single write path, same-set no-op, `StopRetrying`, conditional enrollment, flip-at-start) | 8 |
| §3 `selectDeviceWithSpace` + `selectDeviceForRebuild` refactor | 2 |
| §3 `selectDeviceForStore`, membership walk | 3 |
| §3 `getStateForPoolMigration` | 4 |
| §3 Source == destination defensive skip | 7 (`tryStores`), 9 (zero-transfer test) |
| §4 The task (poll loop, pileup protection, poll-period seam, idempotent completion) | 7 |
| §4 Duplicate execution safety | 7 (`Failure` branch in `tryStores`), 9 (re-dispatch test) |
| §5 Retargeting | 8 (retarget test), 7 (`MigrationSuperseded` re-drive) |
| §6 Rebalancer coordination — planning exclusion | 5 |
| §6 Rebalancer coordination — `pendingTransfers` strip | 8 |
| §6 Rebalancer coordination — `drive` backstop | 6 |
| §7 CLI | 10 |
| Error handling (warn + reschedule; terminal errors only at the entry point) | 7, 8 |
| Testing — unit | 1, 2, 3 |
| Testing — integration | 4, 5, 6, 8, 9 |
| Testing — `MainSuite` | 10 |

One deliberate deviation, flagged in File Structure: the spec lists `getStateForPoolMigration` as a unit test; its signature takes an `AspenClient`, so it is covered by integration tests in Task 4.

**Type consistency** — names used across task boundaries: `StoragePoolState.Migration`, `StoragePoolState.MigrationStatus.{InProgress, Complete}`, `StoragePoolState.migration` (Task 1 → 5, 7, 8, 10); `selectDeviceWithSpace` (2 → 3); `selectDeviceForStore`, `collectMemberDevices`, `collectDevices` (3 → 7); `State.getStateForPoolMigration` (4 → 7); `MigratePoolToSetDurableTask.{typeUUID, PoolIdKey, pollPeriod, prepareSystemTask, MigrationSuperseded}` and the four-argument constructor `(DurableTaskPointer, AspenClient, PoolId, Duration)` (7 → 8, 9); `AspenClient.migratePoolToSet` (8 → 9, 10). All consistent.
