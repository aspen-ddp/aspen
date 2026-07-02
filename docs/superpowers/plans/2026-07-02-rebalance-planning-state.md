# Rebalance Planning State Gathering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `common.rebalancing.State.getStateForRebalancePlanning`, which gathers a `PlanState` snapshot of all device and pool state for a level-0 storage device set, backed by a new generic concurrency-limited parallel execution helper.

**Architecture:** Add `runBoundedParallel` to `common.util` (a sliding-window bounded-concurrency version of `runSequentially`). Implement `getStateForRebalancePlanning` in terms of it: bounded-parallel read all member-device states, collect the pools they host, bounded-parallel read each pool's KV object, then assemble `PlanState` (filtering out `TransferringIn` stores, deriving per-store sizes and pool IDAs).

**Tech Stack:** Scala 3, `scala.concurrent.Future`, `java.util.concurrent.atomic`, ScalaTest (`AnyFunSuite` + `Matchers`, `Await` for async).

**Spec:** `docs/superpowers/specs/2026-07-02-rebalance-planning-state-design.md`

---

## File Structure

- `src/main/scala/org/aspen_ddp/aspen/common/util/package.scala` — add `runBoundedParallel` next to `runSequentially`. Requires new imports for `AtomicInteger` / `AtomicReferenceArray`.
- `src/test/scala/org/aspen_ddp/aspen/common/util/RunBoundedParallelSuite.scala` — new test suite for the helper (this is a pure, easily-tested utility, so it gets TDD coverage).
- `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala` — extend the `Device` case class and implement `getStateForRebalancePlanning`. Per the spec, this function gets **no** unit test (straightforward I/O orchestration; a mock `AspenClient` is not worth maintaining).

---

## Task 1: Add `runBoundedParallel` concurrency limiter

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/util/package.scala` (imports at top lines 1-9; add function after `runSequentially` at line 137-141)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/util/RunBoundedParallelSuite.scala` (create)

- [ ] **Step 1: Write the failing tests**

Create `src/test/scala/org/aspen_ddp/aspen/common/util/RunBoundedParallelSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.*

class RunBoundedParallelSuite extends AnyFunSuite with Matchers:

  given ExecutionContext = ExecutionContext.global
  val timeout: FiniteDuration = 5.seconds

  test("returns results in input order regardless of completion order"):
    // Later items complete sooner, so completion order != input order.
    val items = List(1, 2, 3, 4, 5)
    val f = runBoundedParallel(items, maxConcurrent = 5): i =>
      Future:
        Thread.sleep((6 - i) * 10L)
        i * 10
    Await.result(f, timeout) should be(List(10, 20, 30, 40, 50))

  test("never exceeds the concurrency limit"):
    val inFlight = new AtomicInteger(0)
    val maxObserved = new AtomicInteger(0)
    val items = (1 to 50).toList
    val f = runBoundedParallel(items, maxConcurrent = 4): _ =>
      Future:
        val cur = inFlight.incrementAndGet()
        maxObserved.updateAndGet(m => math.max(m, cur))
        Thread.sleep(5)
        inFlight.decrementAndGet()
        ()
    Await.result(f, timeout)
    maxObserved.get() should be <= 4

  test("empty input yields empty result without invoking f"):
    val called = new AtomicInteger(0)
    val f = runBoundedParallel(List.empty[Int], maxConcurrent = 4): _ =>
      called.incrementAndGet()
      Future.successful(())
    Await.result(f, timeout) should be(empty)
    called.get() should be(0)

  test("processes all items when there are fewer items than the limit"):
    val items = List("a", "b")
    val f = runBoundedParallel(items, maxConcurrent = 10)(s => Future.successful(s.toUpperCase))
    Await.result(f, timeout) should be(List("A", "B"))

  test("fails fast when an invocation fails"):
    val boom = new RuntimeException("boom")
    val items = List(1, 2, 3)
    val f = runBoundedParallel(items, maxConcurrent = 2): i =>
      if i == 2 then Future.failed(boom) else Future.successful(i)
    val thrown = intercept[RuntimeException](Await.result(f, timeout))
    thrown.getMessage should be("boom")

  test("requires a positive concurrency limit"):
    intercept[IllegalArgumentException]:
      runBoundedParallel(List(1, 2, 3), maxConcurrent = 0)(i => Future.successful(i))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly org.aspen_ddp.aspen.common.util.RunBoundedParallelSuite'`
Expected: compilation failure / FAIL — `runBoundedParallel` is not a member of `common.util`.

- [ ] **Step 3: Add the imports**

In `src/main/scala/org/aspen_ddp/aspen/common/util/package.scala`, add to the import block (currently lines 3-9, after `import java.nio.file.*`):

```scala
import java.util.concurrent.atomic.{AtomicInteger, AtomicReferenceArray}
```

(`scala.concurrent.{ExecutionContext, Future}` is already imported at line 9.)

- [ ] **Step 4: Implement `runBoundedParallel`**

In the same file, immediately after `runSequentially` (ends at line 141), add:

```scala
  /**
   * Executes the future-returning function `f` over `items` with at most `maxConcurrent`
   * invocations in flight at any instant. Results are returned in the same order as `items`
   * (matching Future.sequence semantics), regardless of completion order.
   *
   * Fails fast: if any invocation fails, the returned Future fails with that error. Invocations
   * already started are allowed to run to completion.
   *
   * A fixed pool of min(maxConcurrent, n) worker chains pull from a shared atomic index, giving a
   * true sliding window (a new invocation starts the instant one finishes) rather than a
   * chunk-barrier where the slowest item in a batch stalls the next batch.
   */
  def runBoundedParallel[A, B](items: Seq[A], maxConcurrent: Int)
                              (f: A => Future[B])
                              (implicit ec: ExecutionContext): Future[Seq[B]] =
    require(maxConcurrent > 0, s"maxConcurrent must be > 0, got $maxConcurrent")
    val arr = items.toIndexedSeq
    if arr.isEmpty then
      Future.successful(Vector.empty)
    else
      val results   = new AtomicReferenceArray[B](arr.length)
      val nextIndex = new AtomicInteger(0)

      def worker(): Future[Unit] =
        val i = nextIndex.getAndIncrement()
        if i >= arr.length then
          Future.unit
        else
          f(arr(i)).flatMap: b =>
            results.set(i, b)
            worker()

      val workers = (0 until math.min(maxConcurrent, arr.length)).map(_ => worker())
      Future.sequence(workers).map(_ => Vector.tabulate(arr.length)(results.get))
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly org.aspen_ddp.aspen.common.util.RunBoundedParallelSuite'`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/util/package.scala \
        src/test/scala/org/aspen_ddp/aspen/common/util/RunBoundedParallelSuite.scala
git commit -m "Add runBoundedParallel concurrency-limited parallel execution helper"
```

---

## Task 2: Extend `Device` and implement `getStateForRebalancePlanning`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala` (whole file — case class at line 14, function stub at lines 18-19)

No test task — per the spec, this I/O-orchestration function is not unit-tested.

- [ ] **Step 1: Rewrite `State.scala` with the extended `Device` and full implementation**

Replace the entire contents of `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala` with:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.{StorageDeviceId, StorageDeviceSetState, StorageDeviceState, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{byte2long, runBoundedParallel}

import scala.concurrent.{ExecutionContext, Future}

object State:
  /** Default cap on the number of concurrent metadata reads issued while gathering planning state.
   * Bounds load on large systems with many devices and pools. */
  val DefaultMaxConcurrentReads: Int = 20

  case class Store(storeId: StoreId, currentSize: Long, status: StorageDeviceState.StoreStatus)

  case class Pool(poolId: PoolId, ida: IDA, stores: Map[StoreId, Store])

  case class Device(deviceId: StorageDeviceId,
                    currentUsage: Long,
                    totalSize: Long,
                    stores: Map[StoreId, Store])

  case class PlanState(devices: Map[StorageDeviceId, Device], pools: Map[PoolId, Pool])

  /** Gather the device and pool state needed to compute a rebalancing plan for a level-0
   * (leaf) storage device set.
   *
   * Only level-0 sets are supported; a higher-level set fails with IllegalArgumentException.
   * An empty level-0 set yields an empty PlanState.
   *
   * Reads are issued with at most `maxConcurrentReads` in flight at a time (see runBoundedParallel)
   * to bound load on large systems.
   */
  def getStateForRebalancePlanning(client: AspenClient,
                                   storageDeviceSet: StorageDeviceSetState,
                                   maxConcurrentReads: Int = DefaultMaxConcurrentReads): Future[PlanningState] =
    given ExecutionContext = client.clientContext

    if storageDeviceSet.level != 0 then
      Future.failed(new IllegalArgumentException(
        s"getStateForRebalancePlanning only supports level-0 sets; set " +
          s"${storageDeviceSet.setId.uuid} is level ${storageDeviceSet.level}"))
    else
      // A store being transferred appears as TransferringOut on its source device and
      // TransferringIn on its destination. The source device owns the store until the transfer
      // completes (at which point the source's state drops the entry entirely), so we ignore
      // TransferringIn entries. This attributes each store to exactly one device and removes any
      // possibility of a duplicate StoreId key.
      def ownedStores(ds: StorageDeviceState): Map[StoreId, StorageDeviceState.StoreEntry] =
        ds.stores.filter((_, entry) => entry.status != StorageDeviceState.StoreStatus.TransferringIn)

      for
        deviceStates <- runBoundedParallel(storageDeviceSet.memberDevices, maxConcurrentReads)(
          client.getStorageDeviceState)

        poolIds = deviceStates.flatMap(ds => ownedStores(ds).keys).map(_.poolId).toSet

        poolPairs <- runBoundedParallel(poolIds.toSeq, maxConcurrentReads): poolId =>
          client.getStoragePoolPointer(poolId)
            .flatMap(client.read)
            .map(kvos => poolId -> kvos)
      yield
        val poolKvos = poolPairs.toMap

        def storeSize(storeId: StoreId): Long =
          poolKvos.get(storeId.poolId)
            .flatMap(_.contents.get(StoragePoolState.getStoreUsageKey(storeId.poolIndex)))
            .map(vs => byte2long(vs.value.bytes))
            .getOrElse(0L)

        // One Store per owned store id, shared between the device and pool views.
        val storesById: Map[StoreId, Store] =
          deviceStates.flatMap: ds =>
            ownedStores(ds).map((sid, entry) => sid -> Store(sid, storeSize(sid), entry.status))
          .toMap

        val devices: Map[StorageDeviceId, Device] =
          deviceStates.map: ds =>
            val itsStores = ownedStores(ds).keys.map(sid => sid -> storesById(sid)).toMap
            ds.storageDeviceId -> Device(ds.storageDeviceId, ds.currentUsage, ds.totalSize, itsStores)
          .toMap

        val pools: Map[PoolId, Pool] =
          poolIds.map: poolId =>
            val ida = StoragePoolState(poolKvos(poolId)).ida
            val poolStores = storesById.filter((sid, _) => sid.poolId == poolId)
            poolId -> Pool(poolId, ida, poolStores)
          .toMap

        PlanningState(devices, pools)
```

- [ ] **Step 2: Compile to verify it builds**

Run: `sbt compile`
Expected: SUCCESS, no errors. (This exercises the type-correctness of the new function; there is no unit test for it.)

- [ ] **Step 3: Run the util suite to confirm nothing regressed**

Run: `sbt 'testOnly org.aspen_ddp.aspen.common.util.RunBoundedParallelSuite'`
Expected: PASS (6 tests).

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala
git commit -m "Implement getStateForRebalancePlanning with bounded-concurrency reads"
```

---

## Task 3: Full build and test sweep

**Files:** none (verification only)

- [ ] **Step 1: Full compile**

Run: `sbt compile`
Expected: SUCCESS.

- [ ] **Step 2: Full test run**

Run: `sbt test`
Expected: PASS, including the new `RunBoundedParallelSuite`. No previously-passing tests regress.

- [ ] **Step 3: Commit (only if any incidental fixes were needed)**

If Steps 1-2 required changes, commit them:

```bash
git add -A
git commit -m "Fix build/test issues surfaced by rebalance planning state work"
```

If no changes were needed, skip this step.

---

## Self-Review Notes

- **Spec coverage:** Part 1 (`runBoundedParallel`: ordering, bounded concurrency, empty input, fewer-than-limit, fail-fast, positive-limit require) → Task 1 tests. Part 2 (level guard, bounded device reads, pool-id collection excluding `TransferringIn`, bounded pool reads, per-store size via `getStoreUsageKey`+`byte2long`, IDA via `StoragePoolState(kvos)`, `Device` with `currentUsage`/`totalSize`, `TransferringIn` exclusion in both maps, default 20) → Task 2 implementation.
- **Type consistency:** `runBoundedParallel[A, B](items, maxConcurrent)(f)(ec)` is used identically in Task 1 and Task 2. `Device(deviceId, currentUsage, totalSize, stores)` matches the spec's revised case class. `StorageDeviceState.StoreStatus.TransferringIn`, `StoragePoolState.getStoreUsageKey`, and `byte2long` match the verified APIs in the spec.
- **No placeholders:** every code and command step is concrete.
