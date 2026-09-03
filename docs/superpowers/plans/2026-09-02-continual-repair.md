# Continual Repair Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the never-invoked `Main.repair` with a `RepairService` that the `Host` runs
continually over the stores it owns, draining each pool's `errorTree` at a self-pacing rate.

**Architecture:** A new `org.aspen_ddp.aspen.server.repair` package. `RepairService` owns
scheduling (a fixed host-wide tick, a per-store deadline map, bounded parallelism, policy
caching); `StoreRepairer` owns one store's scan (walk the errorTree range, repair each entry,
delete the entry, return counts). Both talk to the `Host` through a three-method `RepairTarget`
trait so they can be tested against a recording fake. Timing policy lives in Aspen metadata —
pool-scoped on the pool object, host-scoped on the host object — with absent keys meaning
"defaults" and failed reads never gating repair.

**Tech Stack:** Scala 3 (quiet syntax), ScalaPB (`src/main/protobuf/codec.proto`), scribe logging,
ScalaTest (`AnyFunSuite`/`Matchers` for pure units, `IntegrationTestSuite`/`TestNetwork` for
anything needing a client or a `Host`).

**Spec:** `docs/superpowers/specs/2026-09-02-continual-repair-design.md`

## Global Constraints

- Scala 3 quiet-mode syntax. Braces only where they clearly aid readability. Indented
  `if/then/else`; avoid single-line `if a == 1 then value` unless it reads better.
- Defaults, exactly: `scanIntervalFloor` 30s, `scanIntervalCap` 15min,
  `minErrorEntryAgeForDeletion` 60s (preserves today's `Main.MinErrorEntryAgeForDeletion`),
  `maxConcurrentStoreScans` 4.
- Jitter is ±25% of the next interval.
- Sweep tick period is a constant 30s, equal to `RepairPolicy.Default.scanIntervalFloor`.
- An absent metadata key means the default. An unreadable metadata key never stops repair — fall
  back to last-known-good, then to the default, and log at debug.
- `runBoundedParallel` **fails fast**. Every per-store scan must be individually `recover`ed.
- The sweep runs on the `BackgroundTaskManager` execution context, never on the `Host` event
  loop — `Host.repair` enqueues to that loop, so calling it from the loop deadlocks.
- Both errorTree delete transactions must use a transaction with missed-update tracking
  disabled. This is the recursion fix, not a cosmetic detail.
- No CLI repair command. `RepairService` is the only driver.
- TDD: failing test, run it, minimal implementation, run it, commit.

## Deviations from the spec (accepted, both additive)

1. The spec says `disableMissedUpdateTracking()` is called on the delete transactions. It is
   declared `protected[client]` and is therefore unreachable from `aspen.server.repair` — which
   is precisely why today's `Main.repair` (in `aspen.cmdline`) omits it. Task 1 adds
   `AspenClient.newRepairTransaction()`, a `private[aspen]` factory that lives in package
   `client` and can make the call.
2. The spec calls `HostRepairLimits` a host-scoped record without naming an encoding. It gets the
   same protobuf treatment as `RepairPolicy` for consistency.

## File Structure

**Created:**

| Path | Responsibility |
|---|---|
| `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairPolicy.scala` | Pool-scoped timing record, codec, metadata read |
| `src/main/scala/org/aspen_ddp/aspen/server/repair/HostRepairLimits.scala` | Host-scoped load ceiling, codec, metadata read |
| `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairTarget.scala` | `RepairTarget` trait + `StoreNotHosted` |
| `src/main/scala/org/aspen_ddp/aspen/server/repair/ScanState.scala` | Pure pacing arithmetic |
| `src/main/scala/org/aspen_ddp/aspen/server/repair/StoreRepairer.scala` | One store, one scan; `ScanResult` |
| `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairService.scala` | Tick, deadline map, bounded parallelism, caches |
| `src/test/scala/org/aspen_ddp/aspen/client/NewRepairTransactionSuite.scala` | Task 1 |
| `src/test/scala/org/aspen_ddp/aspen/server/repair/RepairPolicySuite.scala` | Task 2 |
| `src/test/scala/org/aspen_ddp/aspen/server/repair/HostRepairTargetSuite.scala` | Task 3 |
| `src/test/scala/org/aspen_ddp/aspen/server/repair/StoreRepairerSuite.scala` | Task 4 |
| `src/test/scala/org/aspen_ddp/aspen/server/repair/ScanStateSuite.scala` | Task 5 |
| `src/test/scala/org/aspen_ddp/aspen/server/repair/RepairServiceSuite.scala` | Task 6 |

**Modified:** `client/Transaction.scala`, `client/AspenClient.scala`,
`client/internal/transaction/TransactionImpl.scala`,
`client/internal/transaction/TransactionBuilder.scala`, `common/metadata/StoragePoolState.scala`,
`common/metadata/HostState.scala`, `src/main/protobuf/codec.proto`, `server/Host.scala`,
`cmdline/Main.scala`, `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`.

**Deleted:** `TODO.txt`.

---

### Task 1: `AspenClient.newRepairTransaction()`

Everything downstream needs a transaction that will not write errorTree entries for its own
missed commits. `disableMissedUpdateTracking()` is `protected[client]`, so the factory has to
live in package `client`.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala:78-83`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:58`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala:39-43`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionBuilder.scala:153-155`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/NewRepairTransactionSuite.scala`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `AspenClient.newRepairTransaction(): Transaction` (`private[aspen]`)
  - `Transaction.missedUpdateTrackingEnabled: Boolean` (`private[aspen]`)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/client/NewRepairTransactionSuite.scala`:

```scala
package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite

import scala.concurrent.Future

class NewRepairTransactionSuite extends IntegrationTestSuite:

  test("an ordinary transaction tracks missed updates"):
    Future.successful:
      client.newTransaction().missedUpdateTrackingEnabled shouldBe true

  test("a repair transaction does not track missed updates"):
    Future.successful:
      client.newRepairTransaction().missedUpdateTrackingEnabled shouldBe false
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *NewRepairTransactionSuite'`
Expected: compile failure — `value missedUpdateTrackingEnabled is not a member of Transaction`
and `value newRepairTransaction is not a member of AspenClient`.

- [ ] **Step 3: Write minimal implementation**

In `Transaction.scala`, update the doc comment on `disableMissedUpdateTracking` and add the
accessor just below it:

```scala
  /** Used by MissedUpdateFinalizationActions and by AspenClient.newRepairTransaction to prevent
    *  circular loops when marking objects as having missed update transactions.
    *  This method should NOT be used for any other purposes.
    *
    */
  protected[client] def disableMissedUpdateTracking(): Unit

  /** True unless disableMissedUpdateTracking() has been called. Exists so the repair service's
    *  transaction factory is testable; the flag is otherwise write-only.
    */
  private[aspen] def missedUpdateTrackingEnabled: Boolean
```

In `TransactionBuilder.scala`, immediately above `disableMissedUpdateTracking()`:

```scala
  def missedUpdateTrackingEnabled: Boolean = synchronized {
    addMissedUpdateTrackingFA
  }
```

In `TransactionImpl.scala`, immediately above `disableMissedUpdateTracking()`:

```scala
  def missedUpdateTrackingEnabled: Boolean = synchronized { state } match {
    case Right(bldr) => bldr.missedUpdateTrackingEnabled
    case Left(_) => throw PostCommitTransactionModification()
  }
```

In `AspenClient.scala`, immediately after `def newTransaction(): Transaction`:

```scala
  /** A transaction for the repair service's errorTree deletes.
    *
    * Missed-update tracking must be off: if the delete's own commit misses a store, the
    * MissedUpdateFinalizationAction writes a fresh errorTree entry for the errorTree node
    * itself, which the next sweep repairs, which writes another. A one-shot CLI pass survived
    * that; a service that runs forever does not.
    */
  private[aspen] def newRepairTransaction(): Transaction =
    val tx = newTransaction()
    tx.disableMissedUpdateTracking()
    tx
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly *NewRepairTransactionSuite'`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client src/test/scala/org/aspen_ddp/aspen/client/NewRepairTransactionSuite.scala
git commit -m "Add AspenClient.newRepairTransaction for repair-service errorTree deletes"
```

---

### Task 2: `RepairPolicy` and `HostRepairLimits`

**Files:**
- Modify: `src/main/protobuf/codec.proto` (append at end of file)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StoragePoolState.scala:18`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/HostState.scala:29`
- Create: `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairPolicy.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/server/repair/HostRepairLimits.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/repair/RepairPolicySuite.scala`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `case class RepairPolicy(scanIntervalFloor: Duration, scanIntervalCap: Duration, minErrorEntryAgeForDeletion: Duration)` with `encode(): Array[Byte]`
  - `RepairPolicy.Default: RepairPolicy`, `RepairPolicy.decode(bytes: Array[Byte]): RepairPolicy`
  - `RepairPolicy.read(client: AspenClient, poolId: PoolId)(using ExecutionContext): Future[RepairPolicy]`
  - `case class HostRepairLimits(maxConcurrentStoreScans: Int)` with `encode(): Array[Byte]`
  - `HostRepairLimits.Default: HostRepairLimits`, `HostRepairLimits.decode(bytes: Array[Byte]): HostRepairLimits`
  - `HostRepairLimits.read(client: AspenClient, hostId: HostId)(using ExecutionContext): Future[HostRepairLimits]`
  - `StoragePoolState.RepairPolicyKey`, `HostState.RepairLimitsKey`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/server/repair/RepairPolicySuite.scala`:

```scala
package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.HostState
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MINUTES, SECONDS}

class RepairPolicySuite extends IntegrationTestSuite:

  test("the repair policy defaults preserve the CLI repair timings"):
    Future.successful:
      RepairPolicy.Default.scanIntervalFloor shouldBe Duration(30, SECONDS)
      RepairPolicy.Default.scanIntervalCap shouldBe Duration(15, MINUTES)
      RepairPolicy.Default.minErrorEntryAgeForDeletion shouldBe Duration(60, SECONDS)
      HostRepairLimits.Default.maxConcurrentStoreScans shouldBe 4

  test("a repair policy survives a codec round trip"):
    Future.successful:
      val p = RepairPolicy(Duration(5, SECONDS), Duration(90, SECONDS), Duration(120, SECONDS))
      RepairPolicy.decode(p.encode()) shouldBe p

  test("host repair limits survive a codec round trip"):
    Future.successful:
      val l = HostRepairLimits(11)
      HostRepairLimits.decode(l.encode()) shouldBe l

  // proto3 scalars read as zero when never written, which is also what a record written before
  // a field was added looks like. Zero must mean "use the default", not "scan every 0 seconds".
  test("unset fields in an encoded policy fall back to the defaults"):
    Future.successful:
      RepairPolicy.decode(Array[Byte]()) shouldBe RepairPolicy.Default
      HostRepairLimits.decode(Array[Byte]()) shouldBe HostRepairLimits.Default

  test("a pool with no repair policy key reads as the default policy"):
    RepairPolicy.read(client, PoolId.BootstrapPoolId).map: p =>
      p shouldBe RepairPolicy.Default

  test("a host with no repair limits key reads as the default limits"):
    HostRepairLimits.read(client, HostId.BootstrapHostId).map: l =>
      l shouldBe HostRepairLimits.Default
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *RepairPolicySuite'`
Expected: compile failure — `Not found: RepairPolicy`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/main/protobuf/codec.proto`:

```proto
//---------------------------------------------------------------------------
//- Repair
//---------------------------------------------------------------------------
message RepairPolicy {
  uint32 scanIntervalFloorSeconds = 1;
  uint32 scanIntervalCapSeconds = 2;
  uint32 minErrorEntryAgeForDeletionSeconds = 3;
}

message HostRepairLimits {
  uint32 maxConcurrentStoreScans = 1;
}
```

In `StoragePoolState.scala`, after `AllocationStrategyConfigKey`:

```scala
  private [aspen] val RepairPolicyKey = Key(Array[Byte](5))
```

In `HostState.scala`, after `TaskExecutorRootKey`:

```scala
  private [aspen] val RepairLimitsKey = Key(Array[Byte](2))
```

Create `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairPolicy.scala`:

```scala
package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MINUTES, SECONDS}

/** Pool-scoped repair timing.
  *
  * Timing belongs to the pool rather than the host because staleness risk is a property of the
  * pool's IDA: how many slices it can lose before data is unrecoverable. The host-scoped
  * companion to this record is HostRepairLimits, which bounds load instead.
  *
  * @param scanIntervalFloor            fastest a single store is scanned
  * @param scanIntervalCap              slowest a single store is scanned, reached by repeated
  *                                     doubling after clean scans
  * @param minErrorEntryAgeForDeletion  how long an errorTree entry must sit before its object's
  *                                     absence from the allocationTree is taken as proof of
  *                                     deletion
  */
case class RepairPolicy(scanIntervalFloor: Duration,
                        scanIntervalCap: Duration,
                        minErrorEntryAgeForDeletion: Duration):

  def encode(): Array[Byte] = codec.RepairPolicy(
    scanIntervalFloorSeconds = scanIntervalFloor.toSeconds.toInt,
    scanIntervalCapSeconds = scanIntervalCap.toSeconds.toInt,
    minErrorEntryAgeForDeletionSeconds = minErrorEntryAgeForDeletion.toSeconds.toInt
  ).toByteArray

object RepairPolicy:

  val Default: RepairPolicy = RepairPolicy(
    scanIntervalFloor = Duration(30, SECONDS),
    scanIntervalCap = Duration(15, MINUTES),
    minErrorEntryAgeForDeletion = Duration(60, SECONDS))

  /** proto3 scalars read as zero when the field was never written, which is also how a record
    * written by an older version looks after a field is added. Zero therefore means "unset" and
    * yields the default rather than an interval of zero, which would busy-loop the sweep.
    */
  def decode(bytes: Array[Byte]): RepairPolicy =
    val m = codec.RepairPolicy.parseFrom(bytes)

    def seconds(value: Int, dflt: Duration): Duration =
      if value <= 0 then dflt else Duration(value, SECONDS)

    RepairPolicy(
      seconds(m.scanIntervalFloorSeconds, Default.scanIntervalFloor),
      seconds(m.scanIntervalCapSeconds, Default.scanIntervalCap),
      seconds(m.minErrorEntryAgeForDeletionSeconds, Default.minErrorEntryAgeForDeletion))

  /** Reads the pool's policy. An absent key yields Default -- no pool has this key today and
    * nothing is required to write one, so repair works with zero administration. A failed read
    * fails this future; RepairService is what decides to fall back, since only it knows the
    * last-known-good value.
    */
  def read(client: AspenClient, poolId: PoolId)(using ec: ExecutionContext): Future[RepairPolicy] =
    for
      ptr <- client.getStoragePoolPointer(poolId)
      kvos <- client.read(ptr, "repair policy")
    yield
      kvos.contents.get(StoragePoolState.RepairPolicyKey) match
        case Some(vs) => decode(vs.value.bytes)
        case None => Default
```

Create `src/main/scala/org/aspen_ddp/aspen/server/repair/HostRepairLimits.scala`:

```scala
package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState}

import scala.concurrent.{ExecutionContext, Future}

/** Host-scoped repair load ceiling.
  *
  * The ceiling belongs to the host rather than the pool because it is a property of the machine:
  * a host may hold thousands of stores across many pools, and what bounds the damage a sweep can
  * do is how many of those it scans at once.
  */
case class HostRepairLimits(maxConcurrentStoreScans: Int):

  def encode(): Array[Byte] =
    codec.HostRepairLimits(maxConcurrentStoreScans = maxConcurrentStoreScans).toByteArray

object HostRepairLimits:

  val Default: HostRepairLimits = HostRepairLimits(maxConcurrentStoreScans = 4)

  /** A non-positive value is treated as unset. runBoundedParallel requires a positive bound, and
    * a zero read from a never-written proto3 field must not be able to stop repair entirely.
    */
  def decode(bytes: Array[Byte]): HostRepairLimits =
    val m = codec.HostRepairLimits.parseFrom(bytes)
    if m.maxConcurrentStoreScans <= 0 then Default
    else HostRepairLimits(m.maxConcurrentStoreScans)

  def read(client: AspenClient, hostId: HostId)(using ec: ExecutionContext): Future[HostRepairLimits] =
    for
      ptr <- client.getHostPointer(hostId)
      kvos <- client.read(ptr, "repair limits")
    yield
      kvos.contents.get(HostState.RepairLimitsKey) match
        case Some(vs) => decode(vs.value.bytes)
        case None => Default
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly *RepairPolicySuite'`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/protobuf/codec.proto src/main/scala/org/aspen_ddp/aspen/common/metadata src/main/scala/org/aspen_ddp/aspen/server/repair src/test/scala/org/aspen_ddp/aspen/server/repair
git commit -m "Add RepairPolicy and HostRepairLimits metadata records"
```

---

### Task 3: `RepairTarget`, `StoreNotHosted`, and the Host-side fixes

Two live defects are fixed here. `Repair` for an unhosted store never completes its promise,
which would wedge the sweep's single-flight slot for the life of the process. `RepairDelete`
succeeds it, which makes the caller delete an errorTree entry for a slice nobody deleted.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairTarget.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/Host.scala:130` (class declaration),
  `:1454-1456` (add `repairableStoreIds` after `getStoreIds`), `:1633-1643` (event handlers)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/repair/HostRepairTargetSuite.scala`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `trait RepairTarget` with `repairableStoreIds: List[StoreId]`,
    `repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit`,
    `repairDelete(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte], completion: Promise[Unit]): Unit`
  - `case class StoreNotHosted(storeId: StoreId) extends Exception`
  - `Host` implements `RepairTarget`; new `Host.repairableStoreIds`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/server/repair/HostRepairTargetSuite.scala`:

```scala
package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.UUID
import scala.concurrent.Promise
import scala.util.{Success, Try}

class HostRepairTargetSuite extends IntegrationTestSuite:

  // The bootstrap pool has three stores at indices 0, 1 and 2. Index 7 is never hosted.
  private val unhostedStore = StoreId(PoolId.BootstrapPoolId, 7.toByte)

  test("repairableStoreIds lists the stores the host currently owns"):
    client.read(radicle, "warm up").map: _ =>
      val ids = net.smgr.repairableStoreIds
      ids should contain (StoreId(PoolId.BootstrapPoolId, 0.toByte))
      ids should not contain unhostedStore

  test("a repair for an unhosted store fails rather than hanging"):
    for
      kvos <- client.read(radicle, "unhosted repair")
      p = Promise[Unit]()
      _ = net.smgr.repair(unhostedStore, kvos, p)
      _ = net.handleEvents()
      result <- p.future.transform(t => Success(t))
    yield
      result.isFailure shouldBe true
      result.failed.get shouldBe a [StoreNotHosted]

  test("a repair deletion for an unhosted store fails rather than succeeding"):
    val objectId = ObjectId(new UUID(1, 2))
    val p = Promise[Unit]()
    net.smgr.repairDelete(unhostedStore, objectId, Array[Byte](), p)
    net.handleEvents()
    p.future.transform(t => Success(t)).map: result =>
      result.isFailure shouldBe true
      result.failed.get shouldBe a [StoreNotHosted]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *HostRepairTargetSuite'`
Expected: compile failure — `Not found: StoreNotHosted`, `value repairableStoreIds is not a
member of Host`. After `RepairTarget.scala` exists but before the Host change, expect the
repair test to hang until the suite times out and the deletion test to fail on
`result.isFailure shouldBe true`.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairTarget.scala`:

```scala
package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId

import scala.concurrent.Promise

/** Completes a repair operation aimed at a store this host no longer holds.
  *
  * Failing rather than succeeding is deliberate: the store still exists, it just belongs to
  * another host now, so the errorTree entry must survive for that host's sweep to find.
  */
case class StoreNotHosted(storeId: StoreId)
  extends Exception(s"Store $storeId is not hosted here")

/** The three operations the repair service needs from a Host.
  *
  * Host is a large class with an event loop and a device-discovery constructor. Depending on
  * this trait instead lets StoreRepairer and RepairService be tested against a recording fake.
  */
trait RepairTarget:

  /** Stores this host currently owns and can write to. Excludes offline stores, stores being
    * rebuilt (already having everything read) and stores transferring out (about to stop being
    * ours).
    */
  def repairableStoreIds: List[StoreId]

  /** Brings the store's copy of the object up to date. Completes `completion` with
    * Failure(StoreNotHosted) if the store has left this host.
    */
  def repair(storeId: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit

  /** Deletes the store's copy of an object that is gone from the rest of the system. An empty
    * `storePointer` means "delete by ObjectId alone". Completes `completion` with
    * Failure(StoreNotHosted) if the store has left this host.
    */
  def repairDelete(storeId: StoreId, objectId: ObjectId,
                   storePointer: Array[Byte], completion: Promise[Unit]): Unit
```

In `Host.scala`, add the import alongside the existing ones:

```scala
import org.aspen_ddp.aspen.server.repair.{RepairTarget, StoreNotHosted}
```

Change the class declaration's `extends Logging {` (line 130) to:

```scala
                   val maxConcurrentRebuilds: Int = 2) extends Logging with RepairTarget {
```

After `getStoreIds` (line 1454-1456), add:

```scala
  /** Stores this host can currently repair. A store mid-rebuild is already having everything
    * read, and a store transferring out is about to stop being ours; scanning either wastes the
    * load budget on work that will be redone or discarded.
    */
  def repairableStoreIds: List[StoreId] = synchronized {
    (stores.keySet -- offlineStores -- rebuildingStores.keySet -- transferringOut.keySet).toList
  }
```

Replace the `Repair` and `RepairDelete` cases (lines 1633-1643) with:

```scala
      case Repair(storeId, os, completion) => stores.get(storeId) match
        case Some(store) => store.repair(os, completion)
        case None =>
          // The store left between the sweep selecting it and this event being handled. Leaving
          // the promise open would strand the scan that awaits it, and with it the sweep's
          // single-flight slot -- repair would stop host-wide until the process restarts.
          // Failing also leaves the errorTree entry in place for the store's new host.
          logger.debug(s"Ignoring repair of ${os.pointer.id}: store $storeId is not hosted here")
          completion.failure(StoreNotHosted(storeId))

      case RepairDelete(storeId, objectId, storePointer, completion) => stores.get(storeId) match
        case Some(store) => store.repairDelete(objectId, storePointer, completion)
        case None =>
          // Must fail, not succeed. Succeeding would tell the caller the slice was deleted, and
          // the caller would drop the errorTree entry -- leaving the object on the departed
          // store with no record that it needs removing.
          logger.debug(s"Ignoring repair deletion of $objectId: store $storeId is not hosted here")
          completion.failure(StoreNotHosted(storeId))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly *HostRepairTargetSuite'`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server src/test/scala/org/aspen_ddp/aspen/server/repair/HostRepairTargetSuite.scala
git commit -m "Fail repair operations aimed at stores the host no longer holds"
```

---

### Task 4: `StoreRepairer`

The per-entry logic from `Main.repairOne`, with `disableMissedUpdateTracking()` added, `println`
replaced by `logger`, counters, and mid-scan abandonment.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/repair/StoreRepairer.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/repair/StoreRepairerSuite.scala`

**Interfaces:**
- Consumes: `RepairTarget`, `StoreNotHosted` (Task 3); `RepairPolicy` (Task 2);
  `AspenClient.newRepairTransaction()` (Task 1).
- Produces:
  - `case class ScanResult(seen: Int, repaired: Int, deferred: Int)` with `foundEntries: Boolean`
  - `ScanResult.Empty`
  - `class StoreRepairer(client: AspenClient, target: RepairTarget)(using ExecutionContext)`
    with `scan(storeId: StoreId, policy: RepairPolicy): Future[ScanResult]`
  - `StoreRepairer.errorEntryMayBeDeleted(entryTimestamp: HLCTimestamp, now: HLCTimestamp, minAge: Duration): Boolean`
  - `StoreRepairer.decodeObjectId(key: Key): ObjectId` (`private[repair]`)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/server/repair/StoreRepairerSuite.scala`.

Note on `TestNetwork`: its comment says all transactions miss the third store, and
`MissedUpdateFinalizationAction.errorTimeout` is 50ms, so real errorTree entries appear for
poolIndex 2. These tests therefore use **poolIndex 0**, whose entries only exist because the test
seeded them.

```scala
package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{ObjectState as ClientObjectState, Transaction}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Future, Promise}

class StoreRepairerSuite extends IntegrationTestSuite:

  private val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)

  /** Duplicated rather than shared with MissedUpdateFinalizationAction on purpose: a bug in the
    * key layout must not cancel itself out between the writer and the reader.
    */
  private def buildErrorKey(poolIndex: Byte, objectId: ObjectId): Key =
    val bb = ByteBuffer.allocate(17)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(poolIndex)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)
    Key(bb.array())

  /** Records what the service asked the host to do, and lets the test make the store depart. */
  private class RecordingTarget(hosted: List[StoreId]) extends RepairTarget:
    val departed = new AtomicBoolean(false)
    @volatile var repairs: List[ObjectId] = Nil
    @volatile var deletions: List[(ObjectId, Seq[Byte])] = Nil

    def repairableStoreIds: List[StoreId] = if departed.get then Nil else hosted

    def repair(sid: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
      synchronized { repairs = repairs :+ os.pointer.id }
      completion.success(())

    def repairDelete(sid: StoreId, objectId: ObjectId,
                     storePointer: Array[Byte], completion: Promise[Unit]): Unit =
      synchronized { deletions = deletions :+ (objectId, storePointer.toSeq) }
      completion.success(())

  private def errorTree: Future[TieredKeyValueList] =
    client.getStoragePool(PoolId.BootstrapPoolId).map(_.errorTree)

  /** Seeds an errorTree entry directly. Provoking a real missed update is not deterministic in
    * TestNetwork, and the entry's shape is all StoreRepairer cares about.
    */
  private def seedErrorEntry(objectId: ObjectId, storePointer: Array[Byte]): Future[Unit] =
    errorTree.flatMap: tree =>
      val tx: Transaction = client.newRepairTransaction()
      given Transaction = tx
      tree.set(buildErrorKey(0.toByte, objectId), Value(storePointer)).flatMap: _ =>
        tx.commit().map(_ => ())

  private def errorEntryExists(objectId: ObjectId): Future[Boolean] =
    errorTree.flatMap(_.get(buildErrorKey(0.toByte, objectId))).map(_.isDefined)

  /** Allocates a real object. AllocationFinalizationAction writes the allocationTree entry, so
    * nothing here has to fake one -- waiting for the transaction to settle is enough.
    */
  private def allocateObject(): Future[ObjectId] =
    val f = client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        pool <- client.getStoragePool(PoolId.BootstrapPoolId)
        allocator = new PoolObjectAllocator(client, pool)
        ptr <- allocator.allocateKeyValueObject(Map(Key(Array[Byte](9)) -> Value(Array[Byte](9))))
      yield ptr
    for
      ptr <- f
      _ <- net.waitForTransactionsToComplete()
    yield ptr.id

  test("an entry whose object is in the allocation tree is repaired and the entry removed"):
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    for
      objectId <- allocateObject()
      _ <- seedErrorEntry(objectId, Array[Byte](1, 2, 3))
      result <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(objectId)
    yield
      target.repairs should contain (objectId)
      result.repaired shouldBe 1
      result.deferred shouldBe 0
      stillThere shouldBe false

  test("an aged entry with no allocation is deleted from the store and the tree"):
    val objectId = ObjectId(new UUID(7, 7))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    // minErrorEntryAgeForDeletion of zero makes a freshly seeded entry immediately eligible.
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](4, 5))
      result <- repairer.scan(storeId, policy)
      stillThere <- errorEntryExists(objectId)
    yield
      target.deletions shouldBe List((objectId, Seq[Byte](4, 5)))
      result.repaired shouldBe 1
      stillThere shouldBe false

  test("an empty store pointer reaches repairDelete as empty bytes"):
    val objectId = ObjectId(new UUID(8, 8))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte]())
      _ <- repairer.scan(storeId, policy)
    yield
      target.deletions shouldBe List((objectId, Seq.empty[Byte]))

  test("a fresh entry with no allocation is deferred, not deleted"):
    val objectId = ObjectId(new UUID(9, 9))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      result <- repairer.scan(storeId, RepairPolicy.Default)
      stillThere <- errorEntryExists(objectId)
    yield
      target.deletions shouldBe Nil
      result.seen shouldBe 1
      result.deferred shouldBe 1
      result.repaired shouldBe 0
      stillThere shouldBe true

  test("a store that departs mid-scan stops receiving repair calls"):
    val objectId = ObjectId(new UUID(10, 10))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      _ = target.departed.set(true)
      result <- repairer.scan(storeId, policy)
      stillThere <- errorEntryExists(objectId)
    yield
      target.deletions shouldBe Nil
      result.seen shouldBe 0
      stillThere shouldBe true

  test("a failed repair leaves the error tree entry in place"):
    val objectId = ObjectId(new UUID(11, 11))
    val failing = new RepairTarget:
      def repairableStoreIds: List[StoreId] = List(storeId)
      def repair(sid: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
        completion.failure(StoreNotHosted(sid))
      def repairDelete(sid: StoreId, oid: ObjectId,
                       sp: Array[Byte], completion: Promise[Unit]): Unit =
        completion.failure(StoreNotHosted(sid))
    val repairer = StoreRepairer(client, failing)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      _ <- repairer.scan(storeId, policy)
      stillThere <- errorEntryExists(objectId)
    yield
      stillThere shouldBe true

  // Idempotence only. This does NOT prove the recursion fix: in TestNetwork the store that
  // misses updates is index 2, so a re-tracking regression would write its entry outside the
  // [0,1) range this scan covers. Task 1's missedUpdateTrackingEnabled test is the direct proof
  // that repair transactions disable tracking; this one checks a drained range stays drained.
  test("a second scan after a drained tree finds nothing"):
    val objectId = ObjectId(new UUID(12, 12))
    val target = RecordingTarget(List(storeId))
    val repairer = StoreRepairer(client, target)
    val policy = RepairPolicy.Default.copy(minErrorEntryAgeForDeletion = Duration(0, SECONDS))
    for
      _ <- seedErrorEntry(objectId, Array[Byte](1))
      first <- repairer.scan(storeId, policy)
      _ <- net.waitForTransactionsToComplete()
      second <- repairer.scan(storeId, policy)
    yield
      first.seen shouldBe 1
      second.seen shouldBe 0

  test("a future dated entry is never eligible for deletion repair"):
    Future.successful:
      val now = HLCTimestamp(1_756_742_400_000L << 16)
      val future = HLCTimestamp((1_756_742_400_000L + 5_000) << 16)
      StoreRepairer.errorEntryMayBeDeleted(future, now, Duration(60, SECONDS)) shouldBe false

  test("a freshly written entry is not yet eligible for deletion repair"):
    Future.successful:
      val now = HLCTimestamp(1_756_742_400_000L << 16)
      val recent = HLCTimestamp((1_756_742_400_000L - 5_000) << 16)
      StoreRepairer.errorEntryMayBeDeleted(recent, now, Duration(60, SECONDS)) shouldBe false

  test("an aged entry is eligible for deletion repair"):
    Future.successful:
      val now = HLCTimestamp(1_756_742_400_000L << 16)
      val old = HLCTimestamp((1_756_742_400_000L - 120_000) << 16)
      StoreRepairer.errorEntryMayBeDeleted(old, now, Duration(60, SECONDS)) shouldBe true
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *StoreRepairerSuite'`
Expected: compile failure — `Not found: StoreRepairer`.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/org/aspen_ddp/aspen/server/repair/StoreRepairer.scala`:

```scala
package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.{AspenClient, StoragePool, Transaction}
import org.aspen_ddp.aspen.client.tkvl.KeyValueListNode
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer,
                                           KeyValueOperation, ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import scribe.Logging

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

/** What one store's scan found. `seen` drives pacing: any entry at all, repaired or deferred,
  * means the store is not quiet and should be scanned again at the floor.
  */
case class ScanResult(seen: Int, repaired: Int, deferred: Int):
  def foundEntries: Boolean = seen > 0

object ScanResult:
  val Empty: ScanResult = ScanResult(0, 0, 0)

/** Repairs one store, once. Walks the store's slice of its pool's errorTree, brings each
  * recorded object back into agreement with the rest of the system, and removes the entry.
  */
class StoreRepairer(client: AspenClient, target: RepairTarget)
                   (using ec: ExecutionContext) extends Logging:

  import StoreRepairer.*

  def scan(storeId: StoreId, policy: RepairPolicy): Future[ScanResult] =
    val seen = AtomicInteger(0)
    val repaired = AtomicInteger(0)
    val deferred = AtomicInteger(0)

    // errorTree keys are poolIndex :: objectId, so one store's entries are the half-open range
    // [poolIndex, poolIndex+1).
    val min = Key(Array[Byte](storeId.poolIndex))
    val max = Key(Array[Byte]((storeId.poolIndex + 1).toByte))

    for
      pool <- client.getStoragePool(storeId.poolId)
      _ <- pool.errorTree.foreachInRange(min, max,
             repairOne(pool, storeId, policy, seen, repaired, deferred))
    yield
      ScanResult(seen.get, repaired.get, deferred.get)

  private def repairOne(pool: StoragePool, storeId: StoreId, policy: RepairPolicy,
                        seen: AtomicInteger, repaired: AtomicInteger, deferred: AtomicInteger)
                       (node: KeyValueListNode, key: Key, value: ValueState): Future[Unit] =
    if !target.repairableStoreIds.contains(storeId) then
      // The store left this host mid-scan. foreachInRange has no early exit so the remaining
      // tree-node reads still happen, but those are cheap next to the per-object read and two
      // transactions each remaining entry would otherwise cost. Entries stay put for whichever
      // host owns the store now.
      logger.debug(s"Abandoning repair scan of $storeId: no longer repairable here")
      Future.unit
    else
      seen.incrementAndGet()
      val objectId = decodeObjectId(key)

      for
        oAllocation <- pool.allocationTree.get(Key(objectId.toBytes))
        _ <- oAllocation match
          case Some(allocation) =>
            repairUpdate(storeId, ObjectPointer(allocation.value.bytes), node, key).map: _ =>
              repaired.incrementAndGet()
              ()

          case None =>
            // Absent from the allocation tree means either the object was deleted or its
            // allocation has yet to be recorded. Only the first is safe to act on; the age of
            // the error entry is what tells them apart.
            if errorEntryMayBeDeleted(value.timestamp, HLCTimestamp.now,
                                      policy.minErrorEntryAgeForDeletion) then
              // The entry value holds the storePointer bytes captured when the update was
              // missed -- the only surviving copy, since deletion removed the allocation tree
              // entry. Empty means "delete by ObjectId alone".
              repairDeletion(storeId, objectId, value.value.bytes, node, key).map: _ =>
                repaired.incrementAndGet()
                ()
            else
              logger.debug(s"Deferring repair of $objectId on $storeId: absent from the " +
                           s"allocation tree but its error entry is too recent to treat as a " +
                           s"deletion")
              deferred.incrementAndGet()
              Future.unit
      yield ()

  private def repairUpdate(storeId: StoreId, ptr: ObjectPointer,
                           node: KeyValueListNode, key: Key): Future[Unit] =
    val fos = ptr match
      case kp: KeyValueObjectPointer => client.read(kp)
      case dp: DataObjectPointer => client.read(dp)

    val completion = Promise[Unit]()

    for
      os <- fos
      _ = target.repair(storeId, os, completion)
      _ <- completion.future
      _ <- deleteErrorEntryIfUnchanged(os.timestamp, node, key)
    yield
      logger.info(s"Repair complete for object ${ptr.id} on store $storeId")

  private def repairDeletion(storeId: StoreId, objectId: ObjectId, storePointer: Array[Byte],
                             node: KeyValueListNode, key: Key): Future[Unit] =
    val completion = Promise[Unit]()
    target.repairDelete(storeId, objectId, storePointer, completion)

    for
      _ <- completion.future
      _ <- deleteErrorEntry(node, key)
    yield
      logger.info(s"Repair complete (deletion) for object $objectId on store $storeId")

  private def deleteErrorEntry(node: KeyValueListNode, key: Key): Future[Unit] =
    val tx = client.newRepairTransaction()
    val fdelete = node.delete(key)(using tx)
    for
      _ <- fdelete
      _ <- tx.commit()
    yield ()

  /** Deletion of the entry is best effort. A transient error or a collision only costs another
    * pass. The timestamp guard is the point: if the entry's timestamp moved between reading the
    * object and writing it to the store, another update was missed in that window, and letting
    * the delete fail is how the next pass learns to try again.
    */
  private def deleteErrorEntryIfUnchanged(timestamp: HLCTimestamp,
                                          node: KeyValueListNode, key: Key): Future[Unit] =
    val tx = client.newRepairTransaction()
    val fdelete = node.delete(key,
      None,
      List(KeyValueUpdate.TimestampLessThan(key, timestamp)),
      (_, _) => Future.unit)(using tx)
    for
      _ <- fdelete
      _ <- tx.commit()
    yield ()

object StoreRepairer:

  /** Decodes the ObjectId from an errorTree key: one poolIndex byte then the UUID, big-endian. */
  private[repair] def decodeObjectId(key: Key): ObjectId =
    val bb = ByteBuffer.wrap(key.bytes)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.get() // poolIndex
    val msb = bb.getLong()
    val lsb = bb.getLong()
    ObjectId(new UUID(msb, lsb))

  /** Whether an errorTree entry has sat long enough that its object's absence from the
    * allocation tree can be read as proof of deletion.
    *
    * An absent allocationTree entry has two possible meanings: the object was deleted, or the
    * allocating transaction's AllocationFinalizationAction has not landed yet -- it runs
    * concurrently with the MissedUpdateFinalizationAction that wrote the errorTree entry, and
    * both may re-run after a crash. Acting on the second case would drop the errorTree entry for
    * a live object, leaving the store silently short a slice with no record of it.
    *
    * Repair is explicitly not latency sensitive, so we wait long enough to make that race
    * implausible. Waiting costs one more pass; guessing wrong costs a replica.
    *
    * A future dated entry yields a negative age and is therefore ineligible, which is what we
    * want -- clock skew is not evidence that anything was deleted.
    */
  def errorEntryMayBeDeleted(entryTimestamp: HLCTimestamp,
                             now: HLCTimestamp,
                             minAge: Duration): Boolean =
    (now - entryTimestamp) >= minAge
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly *StoreRepairerSuite'`
Expected: PASS (10 tests)

The first test depends on `AllocationFinalizationAction` having written the allocationTree entry
by the time the scan runs. If it proves flaky, extend the settle step in `allocateObject` — poll
`pool.allocationTree.get(Key(objectId.toBytes))` until it returns `Some` — rather than seeding
the allocationTree by hand, which would stop the test exercising the real path. Do not weaken the
assertions.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/repair/StoreRepairer.scala src/test/scala/org/aspen_ddp/aspen/server/repair/StoreRepairerSuite.scala
git commit -m "Add StoreRepairer, carrying the CLI repair logic with the recursion fix"
```

---

### Task 5: `ScanState` pacing

Pure arithmetic, no I/O, injected clock and `Random`.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/repair/ScanState.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/repair/ScanStateSuite.scala`

**Interfaces:**
- Consumes: `RepairPolicy` (Task 2).
- Produces:
  - `case class ScanState(nextDue: Long, currentInterval: Duration)`
  - `ScanState.admit(now: Long, policy: RepairPolicy, rand: Random): ScanState`
  - `ScanState.advance(prev: ScanState, now: Long, foundEntries: Boolean, policy: RepairPolicy, rand: Random): ScanState`
  - `ScanState.jitter(millis: Long, rand: Random): Long` (`private[repair]`)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/server/repair/ScanStateSuite.scala`:

```scala
package org.aspen_ddp.aspen.server.repair

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{Duration, MINUTES, SECONDS}
import scala.util.Random

class ScanStateSuite extends AnyFunSuite with Matchers:

  private def rand = new Random(12345L)
  private val policy = RepairPolicy.Default

  test("a newly admitted store is due within the first floor interval"):
    val r = rand
    (0 until 200).foreach: _ =>
      val s = ScanState.admit(1000L, policy, r)
      s.currentInterval shouldBe policy.scanIntervalFloor
      s.nextDue should be >= 1000L
      s.nextDue should be < 1000L + policy.scanIntervalFloor.toMillis

  test("jitter stays within plus or minus 25 percent"):
    val r = rand
    (0 until 500).foreach: _ =>
      val j = ScanState.jitter(1000L, r)
      j should be >= 750L
      j should be <= 1250L

  test("clean scans double the interval up to the cap and stop there"):
    val r = rand
    var s = ScanState(0L, policy.scanIntervalFloor)
    val intervals = (0 until 8).map: _ =>
      s = ScanState.advance(s, 0L, foundEntries = false, policy, r)
      s.currentInterval
    intervals.take(5) shouldBe Seq(
      Duration(60, SECONDS), Duration(120, SECONDS), Duration(240, SECONDS),
      Duration(480, SECONDS), Duration(15, MINUTES))
    intervals.drop(5).foreach(_ shouldBe Duration(15, MINUTES))

  test("a scan that finds entries snaps back to the floor"):
    val r = rand
    val backedOff = ScanState(0L, Duration(15, MINUTES))
    val s = ScanState.advance(backedOff, 0L, foundEntries = true, policy, r)
    s.currentInterval shouldBe policy.scanIntervalFloor

  test("the next due time is the new interval from now, jittered"):
    val r = rand
    val s = ScanState.advance(ScanState(0L, policy.scanIntervalFloor), 5000L,
                              foundEntries = true, policy, r)
    s.nextDue should be >= 5000L + 22_500L
    s.nextDue should be <= 5000L + 37_500L

  test("an interval below the pool floor is raised to it rather than doubled from"):
    val r = rand
    val stale = ScanState(0L, Duration(1, SECONDS))
    val s = ScanState.advance(stale, 0L, foundEntries = false, policy, r)
    s.currentInterval shouldBe policy.scanIntervalFloor
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *ScanStateSuite'`
Expected: compile failure — `Not found: ScanState`.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/org/aspen_ddp/aspen/server/repair/ScanState.scala`:

```scala
package org.aspen_ddp.aspen.server.repair

import scala.concurrent.duration.Duration
import scala.util.Random

/** Per-store pacing state.
  *
  * @param nextDue         monotonic-clock milliseconds; the store is scanned once the sweep's
  *                        clock reaches this
  * @param currentInterval the un-jittered interval that produced nextDue
  */
case class ScanState(nextDue: Long, currentInterval: Duration)

object ScanState:

  private val JitterFraction = 0.25

  /** Spreads a due time by up to plus or minus 25%, so a host that restarts with a thousand
    * stores does not make them all due on the same instant forever after.
    */
  private[repair] def jitter(millis: Long, rand: Random): Long =
    val span = (millis * JitterFraction).toLong
    if span <= 0 then millis
    else millis - span + rand.nextLong(2 * span + 1)

  /** A newly admitted store starts at the floor, due at a random offset inside the first floor
    * interval -- eager, because a store that just came online is the likeliest to have missed
    * updates, but spread, so admission of many stores at once does not stampede.
    */
  def admit(now: Long, policy: RepairPolicy, rand: Random): ScanState =
    val floorMs = math.max(1L, policy.scanIntervalFloor.toMillis)
    ScanState(now + rand.nextLong(floorMs), policy.scanIntervalFloor)

  /** Advances after a completed scan. A scan that found entries snaps back to the floor; a clean
    * scan doubles toward the cap. A failed scan passes foundEntries = false: a failed read says
    * nothing about whether the store is damaged, and retrying hard against a pool that cannot be
    * read is exactly the load this service exists to bound.
    */
  def advance(prev: ScanState, now: Long, foundEntries: Boolean,
              policy: RepairPolicy, rand: Random): ScanState =
    val next =
      if foundEntries then
        policy.scanIntervalFloor
      else
        val doubled = prev.currentInterval * 2
        if doubled < policy.scanIntervalFloor then policy.scanIntervalFloor
        else if doubled > policy.scanIntervalCap then policy.scanIntervalCap
        else doubled

    ScanState(now + jitter(next.toMillis, rand), next)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly *ScanStateSuite'`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/repair/ScanState.scala src/test/scala/org/aspen_ddp/aspen/server/repair/ScanStateSuite.scala
git commit -m "Add ScanState repair pacing"
```

---

### Task 6: `RepairService`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairService.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/repair/RepairServiceSuite.scala`

**Interfaces:**
- Consumes: `RepairTarget` (Task 3), `RepairPolicy`/`HostRepairLimits` (Task 2),
  `ScanState` (Task 5), `StoreRepairer`/`ScanResult` (Task 4).
- Produces:
  - `class RepairService(client: AspenClient, hostId: HostId, target: RepairTarget, backgroundTasks: BackgroundTaskManager, clock: () => Long = ...)(using ExecutionContext)`
  - `RepairService.cancel(): Unit`
  - `RepairService.sweep(): Future[Unit]` (`private[repair]`, one tick's worth of work)
  - `protected def scanStore(storeId: StoreId, policy: RepairPolicy): Future[ScanResult]` (overridable for tests)
  - `RepairService.TickPeriod: Duration`, `RepairService.StallAfter: FiniteDuration`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/server/repair/RepairServiceSuite.scala`:

```scala
package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.ObjectState as ClientObjectState
import org.aspen_ddp.aspen.common.metadata.HostState
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.MINUTES

class RepairServiceSuite extends IntegrationTestSuite:

  private val storeIds: List[StoreId] =
    (0 until 8).toList.map(i => StoreId(PoolId.BootstrapPoolId, i.toByte))

  /** Reports a fixed store list; never asked to repair anything, since scanStore is stubbed. */
  private class FixedTarget extends RepairTarget:
    def repairableStoreIds: List[StoreId] = storeIds
    def repair(sid: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
      completion.success(())
    def repairDelete(sid: StoreId, oid: ObjectId,
                     sp: Array[Byte], completion: Promise[Unit]): Unit =
      completion.success(())

  /** Counts concurrent scans and optionally fails one store's scan. */
  private class CountingService(clock: AtomicLong, failFor: Option[StoreId])
    extends RepairService(client, HostId.BootstrapHostId, FixedTarget(),
                          BackgroundTaskManager.NoBackgroundTaskManager,
                          () => clock.get):

    val inFlight = AtomicInteger(0)
    val maxInFlight = AtomicInteger(0)
    val scanned = AtomicInteger(0)

    override protected def scanStore(storeId: StoreId,
                                     policy: RepairPolicy): Future[ScanResult] =
      if failFor.contains(storeId) then
        Future.failed(RuntimeException(s"scan of $storeId failed"))
      else
        val n = inFlight.incrementAndGet()
        maxInFlight.updateAndGet(m => math.max(m, n))
        Future:
          Thread.sleep(25)
          inFlight.decrementAndGet()
          scanned.incrementAndGet()
          ScanResult.Empty

  test("the first sweep admits stores without scanning them"):
    val clock = AtomicLong(0L)
    val svc = CountingService(clock, None)
    svc.sweep().map: _ =>
      svc.scanned.get shouldBe 0

  test("concurrent scans never exceed the host limit"):
    val clock = AtomicLong(0L)
    val svc = CountingService(clock, None)
    for
      _ <- svc.sweep()
      _ = clock.set(60_000L)
      _ <- svc.sweep()
    yield
      svc.scanned.get shouldBe 8
      svc.maxInFlight.get should be <= HostRepairLimits.Default.maxConcurrentStoreScans
      svc.maxInFlight.get should be > 1

  test("one failing store does not abort the sweep for the rest"):
    val clock = AtomicLong(0L)
    val svc = CountingService(clock, Some(storeIds(3)))
    for
      _ <- svc.sweep()
      _ = clock.set(60_000L)
      _ <- svc.sweep()
    yield
      svc.scanned.get shouldBe 7

  test("a departed store is dropped from the pacing map and re-admitted if it returns"):
    val clock = AtomicLong(0L)
    var hosted = storeIds
    val target = new RepairTarget:
      def repairableStoreIds: List[StoreId] = hosted
      def repair(sid: StoreId, os: ClientObjectState, completion: Promise[Unit]): Unit =
        completion.success(())
      def repairDelete(sid: StoreId, oid: ObjectId,
                       sp: Array[Byte], completion: Promise[Unit]): Unit =
        completion.success(())

    val svc = new RepairService(client, HostId.BootstrapHostId, target,
                                BackgroundTaskManager.NoBackgroundTaskManager, () => clock.get)
    for
      _ <- svc.sweep()
      _ = svc.testingOnlyScanStates.keySet shouldBe storeIds.toSet
      _ = hosted = storeIds.drop(1)
      _ = clock.set(60_000L)
      _ <- svc.sweep()
      _ = svc.testingOnlyScanStates.keySet shouldBe storeIds.drop(1).toSet
      _ = hosted = storeIds
      _ = clock.set(120_000L)
      _ <- svc.sweep()
    yield
      // Re-admitted at the floor, due inside the first floor interval from 120s.
      val readmitted = svc.testingOnlyScanStates(storeIds.head)
      readmitted.currentInterval shouldBe RepairPolicy.Default.scanIntervalFloor
      readmitted.nextDue should be >= 120_000L
      readmitted.nextDue should be < 120_000L + RepairPolicy.Default.scanIntervalFloor.toMillis

  test("the sweep tick period matches the default scan floor"):
    Future.successful:
      RepairService.TickPeriod shouldBe RepairPolicy.Default.scanIntervalFloor
      RepairService.StallAfter should be > scala.concurrent.duration.Duration(30, MINUTES)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *RepairServiceSuite'`
Expected: compile failure — `Not found: RepairService`.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/org/aspen_ddp/aspen/server/repair/RepairService.scala`:

```scala
package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, runBoundedParallel}
import scribe.Logging

import scala.concurrent.duration.{Duration, FiniteDuration, HOURS, SECONDS}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/** Drives continual repair of every store a host owns.
  *
  * A single host-wide tick decides which stores are due, then scans the due set through a
  * bounded-parallelism gate. Each store paces itself: quiet stores back off toward the cap,
  * stores with entries snap back to the floor. That keeps the steady-state cost of a host with
  * thousands of quiet stores proportional to the cap rather than to the store count, while a
  * store that is actually behind is revisited promptly.
  *
  * @param clock monotonic milliseconds; injectable so pacing is testable without sleeping
  */
class RepairService(client: AspenClient,
                    hostId: HostId,
                    target: RepairTarget,
                    backgroundTasks: BackgroundTaskManager,
                    clock: () => Long = () => System.nanoTime() / 1000000L)
                   (using ec: ExecutionContext) extends Logging:

  import RepairService.*

  private val repairer = StoreRepairer(client, target)
  private val rand = Random()

  private var scanStates: Map[StoreId, ScanState] = Map()
  private var policyCache: Map[PoolId, RepairPolicy] = Map()
  private var limits: HostRepairLimits = HostRepairLimits.Default

  private val sweepTask = backgroundTasks.scheduleNonConcurrentPollingTask(
    "repair-sweep", TickPeriod, callNow = false, stallAfter = StallAfter)(sweep())

  def cancel(): Unit = sweepTask.cancel()

  /** Overridable so tests can count concurrency without doing I/O. */
  protected def scanStore(storeId: StoreId, policy: RepairPolicy): Future[ScanResult] =
    repairer.scan(storeId, policy)

  private[repair] def testingOnlyScanStates: Map[StoreId, ScanState] = synchronized { scanStates }

  private[repair] def sweep(): Future[Unit] =
    val now = clock()
    val repairable = target.repairableStoreIds
    val pools = repairable.map(_.poolId).distinct

    for
      _ <- refreshLimits()
      _ <- refreshPolicies(pools)
      due = selectDue(repairable, now)
      _ <- runBoundedParallel(due, limitOf.maxConcurrentStoreScans)(scanOne)
    yield ()

  private def limitOf: HostRepairLimits = synchronized { limits }

  private def policyOf(poolId: PoolId): RepairPolicy =
    synchronized { policyCache.getOrElse(poolId, RepairPolicy.Default) }

  /** Policy reads are advisory and must never gate repair -- this is the service that has to
    * keep working while the system is unhealthy. A pool whose metadata cannot be read keeps its
    * last-known-good policy, or the default if it never had one.
    */
  private def refreshPolicies(pools: List[PoolId]): Future[Unit] =
    val f = runBoundedParallel(pools, limitOf.maxConcurrentStoreScans): poolId =>
      val fp = RepairPolicy.read(client, poolId).map: policy =>
        synchronized:
          policyCache = policyCache + (poolId -> policy)
      fp.recover:
        case t: Throwable =>
          logger.debug(s"Repair policy read failed for pool $poolId; keeping " +
                       s"${policyOf(poolId)}: $t")
    f.map(_ => ())

  private def refreshLimits(): Future[Unit] =
    val fl = HostRepairLimits.read(client, hostId).map: l =>
      synchronized:
        limits = l
    fl.recover:
      case t: Throwable =>
        logger.debug(s"Repair limits read failed for host $hostId; keeping ${limitOf}: $t")

  /** Drops states for departed stores, admits new ones, and returns the due set.
    *
    * A newly admitted store is deliberately not due on the tick that admits it: admit() places
    * it at a random point inside the first floor interval so a host restart does not scan every
    * store at once.
    */
  private def selectDue(repairable: List[StoreId], now: Long): List[StoreId] = synchronized {
    val live = repairable.toSet
    scanStates = scanStates.filter((storeId, _) => live.contains(storeId))

    repairable.filter: storeId =>
      scanStates.get(storeId) match
        case Some(state) => state.nextDue <= now
        case None =>
          scanStates = scanStates + (storeId -> ScanState.admit(now, policyOf(storeId.poolId), rand))
          false
  }

  /** runBoundedParallel fails fast, so every store's scan is recovered here: one bad store must
    * not abort the sweep for the rest.
    */
  private def scanOne(storeId: StoreId): Future[Unit] =
    val policy = policyOf(storeId.poolId)

    val fFound = scanStore(storeId, policy).map: result =>
      if result.seen > 0 then
        logger.info(s"Repair scan of $storeId: ${result.seen} seen, ${result.repaired} " +
                    s"repaired, ${result.deferred} deferred")
      result.foundEntries

    val recovered = fFound.recover:
      case t: Throwable =>
        logger.warn(s"Repair scan of $storeId failed: $t")
        false

    recovered.map: foundEntries =>
      synchronized:
        scanStates.get(storeId).foreach: prev =>
          scanStates = scanStates +
            (storeId -> ScanState.advance(prev, clock(), foundEntries, policy, rand))

object RepairService:

  /** schedulePeriodic fixes its period at construction and pool policies differ, so the tick is
    * a constant rather than a per-pool value. A pool whose floor is set below this is rounded up
    * to it; floors above it work exactly as configured, which is the direction that matters,
    * since the knob exists to slow repair down.
    */
  val TickPeriod: Duration = Duration(30, SECONDS)

  /** A sweep of a thousand stores through a concurrency cap of four is legitimately slow. A
    * stall report is an operator's cue that something is wrong, so it must not fire for merely
    * slow work.
    */
  val StallAfter: FiniteDuration = FiniteDuration(6, HOURS)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly *RepairServiceSuite'`
Expected: PASS (5 tests)

If `RepairService` cannot be subclassed because the constructor's `using ec` clause confuses the
anonymous-class syntax in the test, give `CountingService` an explicit
`(using ExecutionContext)` clause and pass `executionContext` at the call site. Do not make
`scanStore` public to work around it.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/repair/RepairService.scala src/test/scala/org/aspen_ddp/aspen/server/repair/RepairServiceSuite.scala
git commit -m "Add RepairService sweep scheduling with bounded parallelism"
```

---

### Task 7: Wire into `Host`, remove `Main.repair`, remove `TODO.txt`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/Host.scala:188` (construct after
  `checkStorageDeviceTask`), `:1499-1508` (cancel in `shutdown()`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:1015-1132` (delete)
- Modify: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala:380-395` (delete the three
  relocated tests)
- Delete: `TODO.txt`

**Interfaces:**
- Consumes: `RepairService` (Task 6).
- Produces: nothing new. `Main.repair`, `Main.errorEntryMayBeDeleted` and
  `Main.MinErrorEntryAgeForDeletion` cease to exist.

- [ ] **Step 1: Write the failing test**

Add to `src/test/scala/org/aspen_ddp/aspen/server/repair/HostRepairTargetSuite.scala`:

```scala
  test("the host runs a repair service that can be cancelled"):
    client.read(radicle, "repair service").map: _ =>
      // Constructed alongside the other periodic tasks rather than waiting on task-executor
      // initialization: early sweeps simply find few stores, and the policy fallback covers
      // metadata reads that are not ready yet.
      noException should be thrownBy net.smgr.testingOnlyCancelRepairService()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly *HostRepairTargetSuite'`
Expected: compile failure — `value testingOnlyCancelRepairService is not a member of Host`.

- [ ] **Step 3: Write minimal implementation**

In `Host.scala`, immediately after `checkStorageDeviceTask` (line 188-190) and before
`checkForNewDevices()`:

```scala
  /** Continual repair of this host's stores. Constructed here rather than after task-executor
    * initialization: early sweeps simply find few stores, and RepairService falls back to
    * default policy for metadata that is not readable yet.
    */
  private val repairService = new RepairService(client, hostId, this, backgroundTasks)(using ec)

  private[aspen] def testingOnlyCancelRepairService(): Unit = repairService.cancel()
```

Extend the `Host.scala` import added in Task 3:

```scala
import org.aspen_ddp.aspen.server.repair.{RepairService, RepairTarget, StoreNotHosted}
```

In `shutdown()`, after `usageUpdateTask.foreach(_.cancel())`:

```scala
    repairService.cancel()
```

In `Main.scala`, delete lines 1015-1132 in full: the `MinErrorEntryAgeForDeletion` value with its
doc comment, `errorEntryMayBeDeleted`, and the whole `def repair(client: AspenClient, host: Host)`
including its nested `deleteErrorEntry`, `deleteErrorEntryByTimestamp`, `step2`, `step1` and
`repairOne`. Then remove the imports that become unused. Determine them by compiling; the
candidates are `KeyValueListNode`, `ValueState`, `KeyValueUpdate`, `HLCTimestamp`,
`ByteBuffer`/`ByteOrder`, `ObjectId`, `ObjectPointer`, `StoragePool`, `Promise`, `SECONDS` and
`Duration`. Delete only those the compiler reports as unused — several are used elsewhere in the
file.

In `MainSuite.scala`, delete lines 380-395: the `nowMillis` value, the `tsAtOffset` helper, and
the three `errorEntryMayBeDeleted` tests. Equivalent coverage now lives in `StoreRepairerSuite`.

In `client/internal/transaction/MissedUpdateFinalizationAction.scala`, the comment above
`markMissedUpdates` ends with "See the repair-deletion item in TODO.txt." That file is about to
be deleted, so repoint the sentence at the code that now owns the behaviour:

```scala
  // A repair pass must therefore treat an empty value as "delete by ObjectId alone" rather
  // than as an error. See StoreRepairer.repairDeletion in aspen.server.repair.
```

Change only that trailing sentence; leave the rest of the comment and all code untouched.

Delete `TODO.txt`.

- [ ] **Step 4: Run the full suite to verify nothing regressed**

Run: `sbt test`
Expected: PASS. In particular `MainSuite`, `HostRepairTargetSuite`, `StoreRepairerSuite`,
`RepairServiceSuite`, `ScanStateSuite`, `RepairPolicySuite` and `NewRepairTransactionSuite` all
green, and no compilation warnings about unused imports in `Main.scala`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "Run continual repair from the Host and remove the unreachable CLI repair"
```

---

## Self-Review

**Spec coverage.** Package layout → Tasks 2-6 (one unit per task, names as specified). Policy in
system metadata, split pool/host, absent-key and failed-read rules, all four default values →
Task 2 (records, keys, defaults) and Task 6 (`refreshPolicies`/`refreshLimits` caching and
fallback). Pacing — fixed tick, generous `stallAfter`, snapshot/drop/admit/select/group-by-pool,
`runBoundedParallel`, doubling to cap, snap to floor, ±25% jitter, eager-but-spread admission,
`repairableStoreIds` exclusions → Tasks 3, 5, 6. Both implementation traps → Task 6 (`scanOne`
recovers per store; `RepairService` runs on the `BackgroundTaskManager` EC, never the event
loop). Per-entry repair steps 1-4, empty-value pass-through, all three changes on the way across
from `Main` → Task 4. Scan-level failure backing off as if clean → Task 5 `advance` doc and Task 6
`scanOne`. Store departure — the hang, both events failing, cooperative abandonment, scan-state
drop → Tasks 3, 4, 6. Host integration, all four bullets → Tasks 3 and 7. Removals, all five →
Task 7. Every test named in the spec's Testing section maps to a test above; the one it lists that
is not a single test is "a failed read yields last-known-good, then Default", which Task 6 covers
structurally through `policyOf` and the `refreshPolicies` recover, and Task 2 covers for the
absent-key half.

**Placeholders.** None. Every code step carries the real code; the two fallback notes (Task 4
Step 4, Task 6 Step 4) name a specific alternative and forbid weakening the assertions rather
than deferring a decision.

**Type consistency.** `ScanResult` is produced in Task 4 and consumed in Task 6 with the same
`seen`/`repaired`/`deferred`/`foundEntries` shape. `RepairPolicy.scanIntervalFloor`/`Cap`/
`minErrorEntryAgeForDeletion` are named identically in Tasks 2, 4, 5, 6.
`HostRepairLimits.maxConcurrentStoreScans` likewise in Tasks 2 and 6. `RepairTarget`'s three
methods match `Host`'s existing `repair`/`repairDelete` signatures and the new
`repairableStoreIds`. `ScanState.advance` takes `(prev, now, foundEntries, policy, rand)` in both
Task 5 and Task 6. `newRepairTransaction()` is defined in Task 1 and used in Task 4 only.
