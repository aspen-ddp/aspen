# Automatic Rebalancing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `RebalancingDurableService` periodically sweep every level-0 storage device set and enroll a rebalance task wherever the plan is non-empty, with a persisted, administrator-settable sweep period exposed through a new `system-rebalance-period` cmdline subcommand.

**Architecture:** Two new keys in the service's existing KV state object hold the sweep period (in minutes, zero meaning disabled) and the `HLCTimestamp` of the last completed sweep. The sweep is folded into the service's existing `reconcile()` polling path rather than given its own timer, because `reconcile()` already reads that exact object every 2 minutes. A payload-free `AutoRebalancePeriodChanged` service message nudges the service to re-read after an administrator changes the period.

**Tech Stack:** Scala 3, ScalaPB (protobuf codegen from `src/main/protobuf/codec.proto`), scopt (cmdline parsing), ScalaTest (`AnyFunSuite` for unit tests, the project's `IntegrationTestSuite` for service tests), sbt.

**Spec:** `docs/superpowers/specs/2026-09-01-automatic-rebalancing-design.md`

## Global Constraints

- **Backwards compatibility is not required.** Both new state keys are always written by `initialServiceState`; decoders assume the keys are present. Do not write absent-key fallbacks or `DoesNotExist` requirements.
- **Scala 3 quiet syntax.** Prefer significant indentation over braces; braces only where they clearly aid readability. Prefer indented `if/then/else` over single-line forms.
- **Do not change `DefaultPollPeriod`.** The existing `RebalancingDurableService.pollPeriod` (2 minutes) is the `reconcile()` polling interval and an in-memory test seam. It is entirely distinct from the new sweep period and must not be conflated with it or renamed.
- **Zero means disabled.** The sweep period is stored, passed, and displayed as a `scala.concurrent.duration.Duration` in minutes; `Duration.Zero` means automatic rebalancing is off.
- **`os-lib` for filesystem work**, `Scaffeine` for caching. Neither is needed by this plan.
- **Commit after every task.** Each task ends with a green test run and a commit.

Run a single suite with: `sbt 'testOnly *SuiteName'`
Run a single test with: `sbt 'testOnly *SuiteName -- -z "test substring"'`

---

### Task 1: Persisted sweep-period and last-sweep state

Adds the two new keys, their encode/decode helpers, and the bootstrap defaults. Nothing reads them yet.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceState.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala:28-41`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceStateSuite.scala`

**Interfaces:**
- Consumes: `org.aspen_ddp.aspen.common.util.long2byte` / `byte2long` (package-level defs in `src/main/scala/org/aspen_ddp/aspen/common/util/package.scala`), `org.aspen_ddp.aspen.common.HLCTimestamp`.
- Produces:
  - `RebalancingServiceState.AutoRebalancePeriodKey: Key`
  - `RebalancingServiceState.LastAutoRebalanceKey: Key`
  - `RebalancingServiceState.encodeAutoRebalancePeriod(period: Duration): Array[Byte]`
  - `RebalancingServiceState.decodeAutoRebalancePeriod(bytes: Array[Byte]): Duration`
  - `RebalancingServiceState.encodeLastAutoRebalance(ts: HLCTimestamp): Array[Byte]`
  - `RebalancingServiceState.decodeLastAutoRebalance(bytes: Array[Byte]): HLCTimestamp`
  - `RebalancingDurableService.DefaultAutoRebalancePeriod: Duration` (8 hours)
  - `RebalancingDurableService.initialServiceState` now returns three entries.

- [ ] **Step 1: Write the failing tests**

Append to `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceStateSuite.scala`, and add `import org.aspen_ddp.aspen.common.HLCTimestamp` and `import scala.concurrent.duration.{Duration, HOURS, MINUTES}` to the existing import block:

```scala
  test("an auto-rebalance period round-trips through minutes"):
    RebalancingServiceState.decodeAutoRebalancePeriod(
      RebalancingServiceState.encodeAutoRebalancePeriod(Duration(4, HOURS))
    ) shouldBe Duration(240, MINUTES)

  test("a zero auto-rebalance period round-trips"):
    RebalancingServiceState.decodeAutoRebalancePeriod(
      RebalancingServiceState.encodeAutoRebalancePeriod(Duration.Zero)
    ) shouldBe Duration(0, MINUTES)

  test("a last-auto-rebalance timestamp round-trips"):
    val ts = HLCTimestamp.now
    RebalancingServiceState.decodeLastAutoRebalance(
      RebalancingServiceState.encodeLastAutoRebalance(ts)) shouldBe ts

  test("HLCTimestamp.Zero round-trips as the last-auto-rebalance timestamp"):
    RebalancingServiceState.decodeLastAutoRebalance(
      RebalancingServiceState.encodeLastAutoRebalance(HLCTimestamp.Zero)) shouldBe HLCTimestamp.Zero

  test("initialServiceState populates all three keys with their defaults"):
    val s = RebalancingDurableService.initialServiceState
    s.keySet shouldBe Set(RebalancingServiceState.ActiveTasksKey,
                          RebalancingServiceState.AutoRebalancePeriodKey,
                          RebalancingServiceState.LastAutoRebalanceKey)
    RebalancingServiceState.decodeActiveTasks(
      s(RebalancingServiceState.ActiveTasksKey)) shouldBe Nil
    RebalancingServiceState.decodeAutoRebalancePeriod(
      s(RebalancingServiceState.AutoRebalancePeriodKey)) shouldBe Duration(8, HOURS)
    RebalancingServiceState.decodeLastAutoRebalance(
      s(RebalancingServiceState.LastAutoRebalanceKey)) shouldBe HLCTimestamp.Zero
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *RebalancingServiceStateSuite'`
Expected: compile error — `value encodeAutoRebalancePeriod is not a member of object RebalancingServiceState`.

- [ ] **Step 3: Add the keys and codecs**

Replace the whole of `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceState.scala` with:

```scala
package org.aspen_ddp.aspen.common.rebalancing

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.util.{byte2long, long2byte}

import scala.concurrent.duration.{Duration, MINUTES}

/** Helpers for the RebalancingDurableService's KV state object.
 *
 *  ActiveTasksKey holds the encoded ActiveRebalancingTasks list: one
 *  (setId, taskStatePointer) entry per in-flight set rebalance.
 *
 *  AutoRebalancePeriodKey and LastAutoRebalanceKey drive the automatic sweep: how often to
 *  plan every level-0 set, and when the last sweep finished. Both are bare scalars rather
 *  than protobuf messages, matching SetRebalanceTaskState's plain-bytes UUID. */
object RebalancingServiceState:

  val ActiveTasksKey: Key = Key(Array[Byte](0))

  /** The interval between automatic rebalance sweeps, stored as whole minutes.
   *  Zero disables automatic rebalancing. */
  val AutoRebalancePeriodKey: Key = Key(Array[Byte](1))

  /** HLCTimestamp of the last *completed* automatic sweep. Written only by a sweep, never
   *  by a period change, so shortening the period can make a sweep immediately due. */
  val LastAutoRebalanceKey: Key = Key(Array[Byte](2))

  type ActiveTasks = List[(StorageDeviceSetId, KeyValueObjectPointer)]

  def encodeActiveTasks(tasks: ActiveTasks): Array[Byte] =
    codec.ActiveRebalancingTasks(tasks = tasks.map(Codec.encode)).toByteArray

  def decodeActiveTasks(bytes: Array[Byte]): ActiveTasks =
    codec.ActiveRebalancingTasks.parseFrom(bytes).tasks.map(Codec.decodeActiveRebalancingTask).toList

  def encodeAutoRebalancePeriod(period: Duration): Array[Byte] = long2byte(period.toMinutes)

  def decodeAutoRebalancePeriod(bytes: Array[Byte]): Duration = Duration(byte2long(bytes), MINUTES)

  def encodeLastAutoRebalance(ts: HLCTimestamp): Array[Byte] = long2byte(ts.asLong)

  def decodeLastAutoRebalance(bytes: Array[Byte]): HLCTimestamp = HLCTimestamp(byte2long(bytes))
```

- [ ] **Step 4: Add the default period and widen initialServiceState**

In `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala`, change the duration import on line 18 from:

```scala
import scala.concurrent.duration.{Duration, MINUTES}
```

to:

```scala
import scala.concurrent.duration.{Duration, HOURS, MINUTES}
```

and add `HLCTimestamp` to the `common` import on line 6:

```scala
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}
```

Immediately after the existing `pollPeriod` declaration (line 31), add:

```scala
  /** Default interval between automatic rebalance sweeps. Distinct from DefaultPollPeriod,
   *  which is how often reconcile() runs. */
  val DefaultAutoRebalancePeriod: Duration = Duration(8, HOURS)
```

Then replace `initialServiceState` (lines 38-41) with:

```scala
  /** The initial contents of the service's KV state object. Written into the services tree by
   *  the Bootstrap process (the service is a system-critical singleton that must always exist).
   *  All three keys are always written, so readers never have to handle an absent key. */
  def initialServiceState: Map[Key, Array[Byte]] =
    Map(
      RebalancingServiceState.ActiveTasksKey ->
        RebalancingServiceState.encodeActiveTasks(Nil),
      RebalancingServiceState.AutoRebalancePeriodKey ->
        RebalancingServiceState.encodeAutoRebalancePeriod(DefaultAutoRebalancePeriod),
      RebalancingServiceState.LastAutoRebalanceKey ->
        RebalancingServiceState.encodeLastAutoRebalance(HLCTimestamp.Zero)
    )
```

`Bootstrap.scala` needs no edit — it already calls `initialServiceState`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *RebalancingServiceStateSuite'`
Expected: PASS, 7 tests.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceState.scala \
        src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala \
        src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceStateSuite.scala
git commit -m "feat: persist the automatic rebalance period and last-sweep time"
```

---

### Task 2: AutoRebalancePeriodChanged service message

A payload-free nudge telling the service to re-read its state. It carries no value because `reconcile()` re-reads the authoritative one; sending it would create a second source of truth.

**Files:**
- Modify: `src/main/protobuf/codec.proto:545-561`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessage.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala:153-158`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessageSuite.scala`

**Interfaces:**
- Consumes: `RebalancingMessage.encode` / `decode` (existing).
- Produces: `case object AutoRebalancePeriodChanged extends RebalancingMessage` — used by Task 3's setter and handled in `receiveMessage`.

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessageSuite.scala`:

```scala
  test("AutoRebalancePeriodChanged round-trips"):
    RebalancingMessage.decode(
      RebalancingMessage.encode(AutoRebalancePeriodChanged)) shouldBe AutoRebalancePeriodChanged
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *RebalancingMessageSuite'`
Expected: compile error — `Not found: AutoRebalancePeriodChanged`.

- [ ] **Step 3: Add the protobuf message**

In `src/main/protobuf/codec.proto`, immediately before the existing `message RebalancingMessage` block (line 556), add:

```protobuf
message AutoRebalancePeriodChanged {
}
```

and add the third oneof entry so the block reads:

```protobuf
message RebalancingMessage {
  oneof msg {
    NewSetRebalanceInitiated newSetRebalanceInitiated = 1;
    TransferComplete transferComplete = 2;
    AutoRebalancePeriodChanged autoRebalancePeriodChanged = 3;
  }
}
```

ScalaPB regenerates on the next `sbt compile`; no manual codegen step is needed.

- [ ] **Step 4: Add the Scala variant and its codec**

In `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessage.scala`, add after the `TransferComplete` case class (line 17):

```scala
/** Sent after an administrator changes the automatic rebalance period. Carries no value: the
 *  service re-reads the authoritative one from its state object. */
case object AutoRebalancePeriodChanged extends RebalancingMessage
```

Add this case to `encode`'s match, after the `TransferComplete` case:

```scala
      case AutoRebalancePeriodChanged =>
        codec.RebalancingMessage(codec.RebalancingMessage.Msg.AutoRebalancePeriodChanged(
          codec.AutoRebalancePeriodChanged()))
```

Add this case to `decode`'s match, before the `Msg.Empty` case:

```scala
      case codec.RebalancingMessage.Msg.AutoRebalancePeriodChanged(_) =>
        AutoRebalancePeriodChanged
```

- [ ] **Step 5: Handle the message in the service**

In `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala`, extend `receiveMessage` (lines 153-158) to:

```scala
  override def receiveMessage(msg: ServiceMessage): Unit =
    RebalancingMessage.decode(msg.encodedContent) match
      case NewSetRebalanceInitiated(_) =>
        reconcile()
      case TransferComplete(setId, _, _, _) =>
        synchronized(tasks.get(setId)).foreach(_.wake())
      case AutoRebalancePeriodChanged =>
        reconcile()
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `sbt 'testOnly *RebalancingMessageSuite'`
Expected: PASS, 3 tests.

- [ ] **Step 7: Commit**

```bash
git add src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessage.scala \
        src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala \
        src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingMessageSuite.scala
git commit -m "feat: add the AutoRebalancePeriodChanged service message"
```

---

### Task 3: Read and write the sweep period

The API the cmdline calls, and the API Task 4's tests use to set up. Lives on the `RebalancingDurableService` companion beside `rebalanceStorageDeviceSet`, which the cmdline already calls directly.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala` (companion object, after `readServiceStatePointer` at line 44-49)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceSuite.scala`

**Interfaces:**
- Consumes: `RebalancingServiceState.{AutoRebalancePeriodKey, LastAutoRebalanceKey, encodeAutoRebalancePeriod, decodeAutoRebalancePeriod, decodeLastAutoRebalance}` (Task 1); `AutoRebalancePeriodChanged` (Task 2); the existing private `readServiceStatePointer(client)`.
- Produces:
  - `RebalancingDurableService.getAutoRebalanceStatus(client: AspenClient): Future[(Duration, HLCTimestamp)]` — `private[aspen]`, used by the cmdline display path
  - `RebalancingDurableService.getAutoRebalancePeriod(client: AspenClient): Future[Duration]`
  - `RebalancingDurableService.setAutoRebalancePeriod(client: AspenClient, period: Duration): Future[Unit]`

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceSuite.scala`, and add `HOURS` and `MINUTES` to the existing duration import so it reads `import scala.concurrent.duration.{Duration, HOURS, MILLISECONDS, MINUTES}`:

```scala
  atest("setAutoRebalancePeriod persists the period and getAutoRebalancePeriod reads it back"):
    given ExecutionContext = executionContext
    for
      initial  <- RebalancingDurableService.getAutoRebalancePeriod(client)
      _        <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(4, HOURS))
      updated  <- RebalancingDurableService.getAutoRebalancePeriod(client)
      _        <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration.Zero)
      disabled <- RebalancingDurableService.getAutoRebalancePeriod(client)
    yield
      initial shouldBe Duration(8, HOURS)
      updated shouldBe Duration(240, MINUTES)
      disabled shouldBe Duration(0, MINUTES)
```

No `SimpleDurableServiceExecutor` is needed: this exercises only client-side state. `sendServiceMessage` is a no-op when no host holds the service lease.

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *RebalancingServiceSuite -- -z "persists the period"'`
Expected: compile error — `value getAutoRebalancePeriod is not a member of object RebalancingDurableService`.

- [ ] **Step 3: Implement the accessors**

In `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala`, insert immediately after `readServiceStatePointer` (after line 49) and before `rebalanceStorageDeviceSet`:

```scala
  /** The automatic rebalance period together with the time of the last completed sweep. Read
   *  as a pair because the two live in the same object and the cmdline displays both, so a
   *  caller that wants both should not pay for two round trips. */
  private[aspen] def getAutoRebalanceStatus(client: AspenClient): Future[(Duration, HLCTimestamp)] =
    given ExecutionContext = client.clientContext
    for
      statePtr <- readServiceStatePointer(client)
      kvos <- client.read(statePtr)
    yield
      (RebalancingServiceState.decodeAutoRebalancePeriod(
         kvos.contents(RebalancingServiceState.AutoRebalancePeriodKey).value.bytes),
       RebalancingServiceState.decodeLastAutoRebalance(
         kvos.contents(RebalancingServiceState.LastAutoRebalanceKey).value.bytes))

  /** The interval between automatic rebalance sweeps. Zero means automatic rebalancing is off. */
  def getAutoRebalancePeriod(client: AspenClient): Future[Duration] =
    given ExecutionContext = client.clientContext
    getAutoRebalanceStatus(client).map(_._1)

  /** Set the interval between automatic rebalance sweeps; zero disables them. Deliberately does
   *  not touch LastAutoRebalanceKey, so shortening the period can make a sweep immediately due
   *  and lengthening it defers the next one relative to the last sweep that actually ran.
   *
   *  The nudge that follows is best-effort, as everywhere else in this service: the running
   *  service re-reads this value on every poll, so a dropped message costs latency, not
   *  correctness. */
  def setAutoRebalancePeriod(client: AspenClient, period: Duration): Future[Unit] =
    given ExecutionContext = client.clientContext

    val done: Future[Unit] = client.transactUntilSuccessful: tx =>
      given Transaction = tx
      for
        statePtr <- readServiceStatePointer(client)
        kvos <- client.read(statePtr)
      yield
        val reqs = KeyValueUpdate.KeyRevision(
                     RebalancingServiceState.AutoRebalancePeriodKey,
                     kvos.contents(RebalancingServiceState.AutoRebalancePeriodKey).revision) :: Nil
        val ops = Insert(RebalancingServiceState.AutoRebalancePeriodKey,
                    RebalancingServiceState.encodeAutoRebalancePeriod(period)) :: Nil
        tx.update(statePtr, None, None, reqs, ops)

    done.map: _ =>
      client.sendServiceMessage(ServiceUUID, RebalancingMessage.encode(AutoRebalancePeriodChanged))
      ()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *RebalancingServiceSuite -- -z "persists the period"'`
Expected: PASS.

- [ ] **Step 5: Run the whole rebalancing suite for regressions**

Run: `sbt 'testOnly *Rebalanc*'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala \
        src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceSuite.scala
git commit -m "feat: add get/set accessors for the automatic rebalance period"
```

---

### Task 4: The automatic sweep

The core of the feature. Splits the companion's single-set entry point so the sweep does not read each set's state twice, then folds a due-check and sweep into `reconcile()`.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala` (companion `rebalanceStorageDeviceSet` at lines 53-73; class body at lines 131-172)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceSuite.scala`

**Interfaces:**
- Consumes: `RebalancingServiceState.{AutoRebalancePeriodKey, LastAutoRebalanceKey, decodeAutoRebalancePeriod, decodeLastAutoRebalance, encodeLastAutoRebalance}` (Task 1); `setAutoRebalancePeriod` (Task 3, used by the tests); `client.listStorageDeviceSets(): Future[List[(String, StorageDeviceSetId)]]`; `client.getStorageDeviceSetState(setId): Future[StorageDeviceSetState]`; `StorageDeviceSetState.level: Int`.
- Produces: no new public API. Internally, `RebalancingDurableService.rebalanceReadSet(client, setId, setState): Future[Unit]` (private to the companion, callable from the companion class).

- [ ] **Step 1: Add the test-suite state helpers**

These are scaffolding for the tests in Step 2 — no production behavior yet. In `src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceSuite.scala`, replace the existing `readActiveTasks` helper (lines 45-55) with the following, which factors out the pointer lookup so the new readers can share it:

```scala
  /** Look up the service's KV state object via the services TKVL. */
  private def readServiceStatePointer(): Future[KeyValueObjectPointer] =
    given ExecutionContext = executionContext
    val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    tkvl.get(Key(RebalancingDurableService.ServiceUUID)).map:
      case Some(vs) => ServiceEntry.decode(vs.value.bytes).statePointer
      case None => throw new IllegalStateException("RebalancingDurableService is not registered")

  /** Read the service's ActiveRebalancingTasks. */
  private def readActiveTasks(): Future[RebalancingServiceState.ActiveTasks] =
    given ExecutionContext = executionContext
    for
      ptr <- readServiceStatePointer()
      kvos <- client.read(ptr)
    yield RebalancingServiceState.decodeActiveTasks(
            kvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)

  private def readLastAutoRebalance(): Future[HLCTimestamp] =
    given ExecutionContext = executionContext
    for
      ptr <- readServiceStatePointer()
      kvos <- client.read(ptr)
    yield RebalancingServiceState.decodeLastAutoRebalance(
            kvos.contents(RebalancingServiceState.LastAutoRebalanceKey).value.bytes)

  /** Overwrite the last-sweep timestamp so that a sweep is, or is not, due. There is no public
   *  API for this: only a completed sweep writes it in production. */
  private def writeLastAutoRebalance(ts: HLCTimestamp): Future[Unit] =
    given ExecutionContext = executionContext
    readServiceStatePointer().flatMap: ptr =>
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        client.read(ptr).map: kvos =>
          val reqs = KeyValueUpdate.KeyRevision(
                       RebalancingServiceState.LastAutoRebalanceKey,
                       kvos.contents(RebalancingServiceState.LastAutoRebalanceKey).revision) :: Nil
          val ops = Insert(RebalancingServiceState.LastAutoRebalanceKey,
                      RebalancingServiceState.encodeLastAutoRebalance(ts)) :: Nil
          tx.update(ptr, None, None, reqs, ops)

  /** Poll `cond` until it holds or `timeout` elapses. Fails the test on timeout. */
  private def awaitUntil(what: String, timeout: Duration = Duration(20000, MILLISECONDS))
                        (cond: => Future[Boolean]): Future[Unit] =
    given ExecutionContext = executionContext
    val deadline = System.nanoTime() + timeout.toNanos
    def loop(): Future[Unit] =
      cond.flatMap: ok =>
        if ok then Future.unit
        else if System.nanoTime() > deadline then
          Future.failed(new AssertionError(s"timed out waiting for: $what"))
        else
          val p = Promise[Unit]()
          client.backgroundTaskManager.schedule(Duration(100, MILLISECONDS)):
            p.success(())
          p.future.flatMap(_ => loop())
    loop()

  /** Let the service poll several times, then report whether anything was enrolled. Used for
   *  the negative cases, where the assertion is that no sweep happened. */
  private def nothingEnrolledAfterSeveralPolls(): Future[Boolean] =
    given ExecutionContext = executionContext
    val p = Promise[Unit]()
    client.backgroundTaskManager.schedule(Duration(2000, MILLISECONDS)):
      p.success(())
    p.future.flatMap(_ => readActiveTasks()).map(_.isEmpty)
```

Update the suite's imports to:

```scala
import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId,
                                            StorageDeviceSetState}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.impl.SimpleDurableServiceExecutor

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, HOURS, MILLISECONDS, MINUTES}
```

The existing `driveUntilActiveEmpty` helper is unchanged and continues to work against the new `readActiveTasks`.

- [ ] **Step 2: Write the failing tests**

Append to the same suite:

```scala
  atest("the sweep enrolls an unbalanced level-0 set once the period has elapsed"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(1, MINUTES))
      _ <- writeLastAutoRebalance(HLCTimestamp.Zero)
      _ <- awaitUntil("the sweep to enroll the set")(readActiveTasks().map(_.exists(_._1 == setId)))
      _ <- driveUntilActiveEmpty(setId, Duration(20000, MILLISECONDS))
      lastSweep <- readLastAutoRebalance()
      finalSet <- client.getStorageDeviceSetState(setId)
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      finalSet.pendingTransfers shouldBe empty
      // recordSweep stamped it; asserted as "not Zero" rather than an ordering comparison,
      // since HLCTimestamp is an AnyVal and ScalaTest's `be >` needs an Ordering in scope.
      lastSweep should not be HLCTimestamp.Zero

  atest("a zero period disables the sweep even when a set is unbalanced"):
    given ExecutionContext = executionContext
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration.Zero)
      _ <- writeLastAutoRebalance(HLCTimestamp.Zero)
      quiet <- nothingEnrolledAfterSeveralPolls()
      lastSweep <- readLastAutoRebalance()
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      quiet shouldBe true
      lastSweep shouldBe HLCTimestamp.Zero

  atest("a recent sweep defers the next one until the period elapses"):
    given ExecutionContext = executionContext
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(8, HOURS))
      _ <- writeLastAutoRebalance(HLCTimestamp.now)
      quiet <- nothingEnrolledAfterSeveralPolls()
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      quiet shouldBe true

  atest("the sweep skips a level-1 set instead of failing on it"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      // A level-1 set holds sets, not devices. getStateForRebalancePlanning throws on it, so
      // the sweep must filter it out before planning; if it does not, the level-0 set below
      // is never reached and the awaitUntil times out.
      upper <- client.createStorageDeviceSet("upper", 1, None)
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(1, MINUTES))
      _ <- writeLastAutoRebalance(HLCTimestamp.Zero)
      _ <- awaitUntil("the sweep to enroll the level-0 set")(
             readActiveTasks().map(_.exists(_._1 == setId)))
      active <- readActiveTasks()
      _ <- driveUntilActiveEmpty(setId, Duration(20000, MILLISECONDS))
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      active.map(_._1) should not contain upper

  atest("a set already being rebalanced is not enrolled twice by the sweep"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      // Enroll by hand first, so the set is in ActiveTasks (and shortly has non-empty
      // pendingTransfers) before the sweep ever looks at it.
      _ <- RebalancingDurableService.rebalanceStorageDeviceSet(client, setId)
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(1, MINUTES))
      _ <- writeLastAutoRebalance(HLCTimestamp.Zero)
      duringSweep <- readActiveTasks()
      _ <- driveUntilActiveEmpty(setId, Duration(20000, MILLISECONDS))
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      duringSweep.count(_._1 == setId) shouldBe 1

  atest("a set whose planning fails does not abort the sweep"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    RebalancingDurableService.pollPeriod = Duration(100, MILLISECONDS)
    val exec = makeExecutor()
    for
      _ <- net.createSecondDevice()
      broken <- client.createStorageDeviceSet("broken", 0, None)
      _ <- addBogusMemberDevice(broken)
      _ <- RebalancingDurableService.setAutoRebalancePeriod(client, Duration(1, MINUTES))
      _ <- writeLastAutoRebalance(HLCTimestamp.Zero)
      // listStorageDeviceSets() gives no ordering guarantee, so this asserts the property that
      // matters either way round: the healthy set is reached and the sweep is recorded.
      _ <- awaitUntil("the healthy set to be enrolled despite the broken one")(
             readActiveTasks().map(_.exists(_._1 == setId)))
      _ <- driveUntilActiveEmpty(setId, Duration(20000, MILLISECONDS))
      lastSweep <- readLastAutoRebalance()
    yield
      exec.shutdown()
      RebalancingDurableService.pollPeriod = RebalancingDurableService.DefaultPollPeriod
      lastSweep should not be HLCTimestamp.Zero
```

Add the corruption helper alongside the other private helpers in the suite. Planning reads
every member device, so a dangling device id is the cheapest deterministic failure — and it
fails inside `getStateForRebalancePlanning`, which is exactly the path `sweepOneSet` recovers:

```scala
  /** Append a member device id that has no StorageDeviceState object, so that
   *  getStateForRebalancePlanning fails for this set. */
  private def addBogusMemberDevice(setId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = executionContext
    val bogus = StorageDeviceId(UUID.randomUUID())
    client.getStorageDeviceSetPointer(setId).flatMap: setPtr =>
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        client.read(setPtr).map: dos =>
          val current = StorageDeviceSetState(dos)
          val updated = current.copy(memberDevices = bogus :: current.memberDevices)
          tx.overwrite(setPtr, dos.revision, DataBuffer(updated.toBytes))
```

This needs three more imports in the suite: `org.aspen_ddp.aspen.common.DataBuffer` (add it
to the existing `common.{...}` import), `org.aspen_ddp.aspen.common.metadata.{StorageDeviceId,
StorageDeviceSetState}` (add to the existing `metadata` import), and `java.util.UUID`.
`getStorageDeviceSetPointer` is `private[aspen]` and the suite is under `org.aspen_ddp.aspen`,
so it is in scope.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `sbt 'testOnly *RebalancingServiceSuite -- -z "the sweep enrolls an unbalanced"'`
Expected: FAIL with `timed out waiting for: the sweep to enroll the set` — the service has no sweep yet.

- [ ] **Step 4: Split the single-set entry point**

In `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala`, replace `rebalanceStorageDeviceSet` (lines 51-73) with:

```scala
  /** Build a plan for `setId` and enroll a SetRebalanceDurableTask, unless one is already in
   *  progress. Idempotent and safe against concurrent callers (revision-checked writes). */
  def rebalanceStorageDeviceSet(client: AspenClient, setId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = client.clientContext
    client.getStorageDeviceSetState(setId).flatMap(rebalanceReadSet(client, setId, _))

  /** As above, for a caller that has already read the set's state. The automatic sweep reads
   *  it to filter on level, and would otherwise pay for the same read twice.
   *
   *  Note that the level check is deliberately *not* here: the public entry point above must
   *  keep surfacing the planning failure when a user names a non-level-0 set on the command
   *  line, rather than silently doing nothing. */
  private def rebalanceReadSet(client: AspenClient,
                               setId: StorageDeviceSetId,
                               setState: StorageDeviceSetState): Future[Unit] =
    given ExecutionContext = client.clientContext

    if setState.pendingTransfers.nonEmpty then
      Future.unit
    else
      for
        statePtr <- readServiceStatePointer(client)
        stateKvos <- client.read(statePtr)
        active = RebalancingServiceState.decodeActiveTasks(
                   stateKvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
        r <-
          if active.exists(_._1 == setId) then
            Future.unit
          else
            planAndEnroll(client, setId, setState, statePtr)
      yield r
```

- [ ] **Step 5: Add the sweep to the service class**

In the same file, add the new mutable field beside the existing ones (after line 139, `private var pollTask: ScheduledTask = NoTask`):

```scala
  private var sweeping = false
```

Replace `reconcile()` (lines 160-172) with:

```scala
  private def reconcile(): Future[Unit] =
    if stopped then Future.unit
    else
      client.read(statePointer).flatMap: kvos =>
        val active = RebalancingServiceState.decodeActiveTasks(
                       kvos.contents(RebalancingServiceState.ActiveTasksKey).value.bytes)
        synchronized:
          if !stopped then
            active.foreach: (setId, taskStatePtr) =>
              if !tasks.contains(setId) && !completing.contains(setId) then
                startTask(setId, taskStatePtr)

        val period = RebalancingServiceState.decodeAutoRebalancePeriod(
                       kvos.contents(RebalancingServiceState.AutoRebalancePeriodKey).value.bytes)
        val lastSweep = RebalancingServiceState.decodeLastAutoRebalance(
                          kvos.contents(RebalancingServiceState.LastAutoRebalanceKey).value.bytes)

        if !sweepIsDue(period, lastSweep) || !beginSweep() then
          Future.unit
        else
          sweep()
            .andThen { case _ => endSweep() }
            .recover:
              case err => logger.warn(s"RebalancingDurableService sweep failed: $err")
      .recover:
        case err => logger.warn(s"RebalancingDurableService reconcile failed: $err")

  /** A period of zero disables sweeping. The value is still read on every poll, so re-enabling
   *  takes effect within one poll interval even if the nudge is lost.
   *
   *  A lastSweep in the future -- clock skew, or an HLC advanced by a peer -- yields a negative
   *  difference and simply defers the sweep until wall time catches up. */
  private def sweepIsDue(period: Duration, lastSweep: HLCTimestamp): Boolean =
    period > Duration.Zero && (HLCTimestamp.now - lastSweep) >= period

  /** Claim the sweep, returning false if one is already running. scheduleNonConcurrentPollingTask
   *  suppresses overlapping *timer* ticks, but receiveMessage calls reconcile() directly and can
   *  land on top of a timer-driven sweep. */
  private def beginSweep(): Boolean = synchronized:
    if sweeping || stopped then
      false
    else
      sweeping = true
      true

  private def endSweep(): Unit = synchronized { sweeping = false }

  /** Plan and enroll a rebalance for every level-0 storage device set, one set at a time, then
   *  record the sweep. Sequential rather than fanned out: each enrollment is a revision-checked
   *  write to the shared ActiveTasksKey, so concurrency here buys collisions rather than speed. */
  private def sweep(): Future[Unit] =
    for
      sets <- client.listStorageDeviceSets()
      _ <- sets.foldLeft(Future.unit): (prior, entry) =>
             prior.flatMap(_ => sweepOneSet(entry._2))
      _ <- recordSweep()
    yield ()

  /** One set's share of a sweep. A failure is logged and swallowed: one bad set must not stop
   *  the rest of the sweep. */
  private def sweepOneSet(setId: StorageDeviceSetId): Future[Unit] =
    if stopped then Future.unit
    else
      client.getStorageDeviceSetState(setId).flatMap: setState =>
        // Level 1+ sets hold sets rather than devices; getStateForRebalancePlanning throws on
        // them, so they are filtered out here rather than allowed to abort the sweep.
        if setState.level != 0 then Future.unit
        else RebalancingDurableService.rebalanceReadSet(client, setId, setState)
      .recover:
        case err =>
          logger.warn(s"Automatic rebalance of storage device set ${setId.uuid} failed: $err")

  /** Record the sweep even when individual sets failed. Holding the timestamp back on failure
   *  would turn the poll into a retry loop against the whole cluster for as long as one set
   *  stayed broken.
   *
   *  Revision-checked on LastAutoRebalanceKey alone, so this does not contend with the
   *  ActiveTasksKey writes that enrollment and completion perform. */
  private def recordSweep(): Future[Unit] =
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      client.read(statePointer).map: kvos =>
        val reqs = KeyValueUpdate.KeyRevision(
                     RebalancingServiceState.LastAutoRebalanceKey,
                     kvos.contents(RebalancingServiceState.LastAutoRebalanceKey).revision) :: Nil
        val ops = Insert(RebalancingServiceState.LastAutoRebalanceKey,
                    RebalancingServiceState.encodeLastAutoRebalance(HLCTimestamp.now)) :: Nil
        tx.update(statePointer, None, None, reqs, ops)
```

`shutdown()` needs no change: no new `ScheduledTask` is created, the existing `stopped` check at the top of `reconcile()` stops new sweeps, and `sweepOneSet` re-checks `stopped` between sets so an in-flight sweep unwinds promptly.

- [ ] **Step 6: Run the sweep tests to verify they pass**

Run: `sbt 'testOnly *RebalancingServiceSuite'`
Expected: PASS, 8 tests.

- [ ] **Step 7: Run the full rebalancing suite for regressions**

Run: `sbt 'testOnly *Rebalanc*'`
Expected: PASS. In particular the pre-existing `rebalanceStorageDeviceSet drains pendingTransfers` test must still pass — it exercises the refactored public entry point.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingDurableService.scala \
        src/test/scala/org/aspen_ddp/aspen/common/rebalancing/RebalancingServiceSuite.scala
git commit -m "feat: sweep every level-0 device set on the configured period"
```

---

### Task 5: Cmdline period parsing and formatting

Pure helpers, no I/O, so they carry the bulk of the cmdline test coverage. Nothing calls them yet.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (helpers beside `formatBytes` at line 1863; import on line 50)
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `Main.parseRebalancePeriod(period: Option[String], unit: Option[String]): Either[String, Option[Duration]]` — `Right(None)` is the display form, `Right(Some(d))` the set form, `Left` a user-facing error
  - `Main.formatRebalancePeriod(d: Duration): String`
  - `Main.formatRebalanceStatus(period: Duration, lastSweep: HLCTimestamp): String`
  - `Main.formatWallTime(millis: Long): String`

  All are `private[cmdline]`, matching `formatBytes`.

- [ ] **Step 1: Write the failing tests**

Append to `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`, adding `import org.aspen_ddp.aspen.common.HLCTimestamp` and `import scala.concurrent.duration.{Duration, DAYS, HOURS, MINUTES}` to the existing imports:

```scala
  test("parseRebalancePeriod returns the display form when no period is given"):
    Main.parseRebalancePeriod(None, None) shouldBe Right(None)

  test("parseRebalancePeriod accepts each unit in singular and plural form"):
    Main.parseRebalancePeriod(Some("20"), Some("minutes")) shouldBe Right(Some(Duration(20, MINUTES)))
    Main.parseRebalancePeriod(Some("1"), Some("minute")) shouldBe Right(Some(Duration(1, MINUTES)))
    Main.parseRebalancePeriod(Some("4"), Some("hours")) shouldBe Right(Some(Duration(4, HOURS)))
    Main.parseRebalancePeriod(Some("1"), Some("hour")) shouldBe Right(Some(Duration(1, HOURS)))
    Main.parseRebalancePeriod(Some("7"), Some("days")) shouldBe Right(Some(Duration(7, DAYS)))
    Main.parseRebalancePeriod(Some("1"), Some("day")) shouldBe Right(Some(Duration(1, DAYS)))

  test("parseRebalancePeriod is case-insensitive about units"):
    Main.parseRebalancePeriod(Some("4"), Some("HOURS")) shouldBe Right(Some(Duration(4, HOURS)))

  test("parseRebalancePeriod maps 'disabled' to a zero period"):
    Main.parseRebalancePeriod(Some("disabled"), None) shouldBe Right(Some(Duration.Zero))

  test("parseRebalancePeriod accepts an explicit zero"):
    Main.parseRebalancePeriod(Some("0"), Some("hours")) shouldBe Right(Some(Duration.Zero))

  test("parseRebalancePeriod rejects 'disabled' with a unit"):
    Main.parseRebalancePeriod(Some("disabled"), Some("hours")) shouldBe Left("'disabled' takes no unit")

  test("parseRebalancePeriod rejects a count with no unit"):
    Main.parseRebalancePeriod(Some("4"), None) shouldBe
      Left("a unit is required: minutes, hours, or days")

  test("parseRebalancePeriod rejects an unknown unit"):
    Main.parseRebalancePeriod(Some("4"), Some("weeks")) shouldBe
      Left("unknown unit 'weeks': expected minutes, hours, or days")

  test("parseRebalancePeriod rejects a non-numeric count"):
    Main.parseRebalancePeriod(Some("soon"), Some("hours")) shouldBe
      Left("period must be a whole number")

  test("parseRebalancePeriod rejects a negative count"):
    Main.parseRebalancePeriod(Some("-1"), Some("hours")) shouldBe Left("period must not be negative")

  test("formatRebalancePeriod renders zero as disabled"):
    Main.formatRebalancePeriod(Duration.Zero) shouldBe "disabled"

  test("formatRebalancePeriod selects the largest unit that divides evenly"):
    Main.formatRebalancePeriod(Duration(20, MINUTES)) shouldBe "20 minutes"
    Main.formatRebalancePeriod(Duration(240, MINUTES)) shouldBe "4 hours"
    Main.formatRebalancePeriod(Duration(7, DAYS)) shouldBe "7 days"
    Main.formatRebalancePeriod(Duration(90, MINUTES)) shouldBe "90 minutes"

  test("formatRebalancePeriod uses singular units for one"):
    Main.formatRebalancePeriod(Duration(1, MINUTES)) shouldBe "1 minute"
    Main.formatRebalancePeriod(Duration(1, HOURS)) shouldBe "1 hour"
    Main.formatRebalancePeriod(Duration(1, DAYS)) shouldBe "1 day"

  test("formatRebalanceStatus reports 'never' and the next poll before the first sweep"):
    val out = Main.formatRebalanceStatus(Duration(8, HOURS), HLCTimestamp.Zero)
    out should include("Automatic rebalancing period: 8 hours")
    out should include("Last sweep:                   never")
    out should include("Next sweep due:               next poll")

  test("formatRebalanceStatus omits the next sweep when disabled"):
    val out = Main.formatRebalanceStatus(Duration.Zero, HLCTimestamp.Zero)
    out should include("Automatic rebalancing period: disabled")
    out should not include "Next sweep due"

  test("formatRebalanceStatus reports both times once a sweep has run"):
    val out = Main.formatRebalanceStatus(Duration(8, HOURS), HLCTimestamp.now)
    out should include("Last sweep:")
    out should include("Next sweep due:")
    out should not include "never"

  test("formatWallTime renders a fixed epoch millisecond value"):
    // Asserted on shape rather than an exact string: the helper renders in the local zone.
    Main.formatWallTime(1_756_742_400_000L) should fullyMatch regex
      """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"""
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *MainSuite'`
Expected: compile error — `value parseRebalancePeriod is not a member of object Main`.

- [ ] **Step 3: Widen the duration import**

In `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, change line 50 from:

```scala
import scala.concurrent.duration.{Duration, HOURS, MILLISECONDS, SECONDS}
```

to:

```scala
import scala.concurrent.duration.{DAYS, Duration, HOURS, MILLISECONDS, MINUTES, SECONDS}
```

`HLCTimestamp` is already imported on line 24.

- [ ] **Step 4: Implement the helpers**

Insert immediately after `formatBytes` (after line 1873) in the same file:

```scala
  /** Parse the period arguments of `system-rebalance-period`.
   *
   *  `Right(None)` means no period was supplied: the display form. `Right(Some(d))` is the
   *  period to set, where zero disables automatic rebalancing. `Left` carries a message fit
   *  to show the user.
   *
   *  A missing period with a present unit cannot occur -- scopt fills positional arguments in
   *  order -- and is treated as the display form rather than given its own error. */
  private[cmdline] def parseRebalancePeriod(period: Option[String],
                                            unit: Option[String]): Either[String, Option[Duration]] =
    (period, unit) match
      case (None, _) =>
        Right(None)

      case (Some(p), _) if p.equalsIgnoreCase("disabled") =>
        if unit.isDefined then Left("'disabled' takes no unit")
        else Right(Some(Duration.Zero))

      case (Some(_), None) =>
        Left("a unit is required: minutes, hours, or days")

      case (Some(p), Some(u)) =>
        for
          count <- p.toLongOption.toRight("period must be a whole number")
          _ <- if count < 0 then Left("period must not be negative") else Right(())
          timeUnit <- u.toLowerCase match
                        case "minute" | "minutes" => Right(MINUTES)
                        case "hour" | "hours" => Right(HOURS)
                        case "day" | "days" => Right(DAYS)
                        case other => Left(s"unknown unit '$other': expected minutes, hours, or days")
        yield Some(Duration(count, timeUnit))

  /** Render an automatic rebalancing period using the largest unit that divides it evenly.
   *  A display helper in the mould of formatBytes. */
  private[cmdline] def formatRebalancePeriod(d: Duration): String =
    val minutes = d.toMinutes
    if minutes == 0 then "disabled"
    else if minutes % (60 * 24) == 0 then pluralUnits(minutes / (60 * 24), "day")
    else if minutes % 60 == 0 then pluralUnits(minutes / 60, "hour")
    else pluralUnits(minutes, "minute")

  private def pluralUnits(n: Long, unit: String): String =
    if n == 1 then s"$n $unit" else s"$n ${unit}s"

  /** Render a wall-clock millisecond value in the local time zone. */
  private[cmdline] def formatWallTime(millis: Long): String =
    java.time.Instant.ofEpochMilli(millis)
      .atZone(java.time.ZoneId.systemDefault())
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

  /** The display form of `system-rebalance-period`. The next-sweep line is omitted when
   *  automatic rebalancing is disabled, because there is no next sweep to report. */
  private[cmdline] def formatRebalanceStatus(period: Duration, lastSweep: HLCTimestamp): String =
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Automatic rebalancing period: ${formatRebalancePeriod(period)}"
    if lastSweep == HLCTimestamp.Zero then
      lines += "Last sweep:                   never"
      if period > Duration.Zero then
        lines += "Next sweep due:               next poll"
    else
      lines += s"Last sweep:                   ${formatWallTime(lastSweep.wallTime)}"
      if period > Duration.Zero then
        lines += s"Next sweep due:               ${formatWallTime(lastSweep.wallTime + period.toMillis)}"
    lines.mkString("\n")
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *MainSuite'`
Expected: PASS, including the pre-existing tests.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala \
        src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala
git commit -m "feat: add rebalance-period parsing and formatting helpers"
```

---

### Task 6: The system-rebalance-period subcommand

Wires the helpers and the API into a subcommand, and clears the TODO item this work completes.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:82-111` (`Args`), `:556` (new `cmd` block), `:677-684` (`checkConfig`), `:725` (dispatch), `:1789` (new handler after `rebalance`)
- Modify: `TODO.txt`

**Interfaces:**
- Consumes: `Main.parseRebalancePeriod`, `Main.formatRebalancePeriod`, `Main.formatRebalanceStatus` (Task 5); `RebalancingDurableService.getAutoRebalanceStatus`, `RebalancingDurableService.setAutoRebalancePeriod` (Task 3).
- Produces: the `system-rebalance-period` subcommand.

- [ ] **Step 1: Add the Args fields**

In `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, add two fields to the end of the `Args` case class (after `entityRef:String=""` on line 111):

```scala
                  rebalancePeriod:Option[String]=None,
                  rebalancePeriodUnit:Option[String]=None)
```

remembering to add a trailing comma to the `entityRef` line.

- [ ] **Step 2: Add the parser block**

Insert immediately after the existing `cmd("rebalance")` block (after line 556):

```scala
      cmd("system-rebalance-period").text("Displays or sets the automatic rebalancing period").
        action((_, c) => c.copy(mode = "system-rebalance-period")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<period>").optional().
            text("Interval between automatic rebalance sweeps, or \"disabled\". " +
                 "Omit to display the current period").
            action((x, c) => c.copy(rebalancePeriod = Some(x))),

          arg[String]("<unit>").optional().text("minutes, hours, or days").
            action((x, c) => c.copy(rebalancePeriodUnit = Some(x))),
        )
```

- [ ] **Step 3: Validate in checkConfig**

Replace the `checkConfig` block (lines 677-684) with:

```scala
      checkConfig { c =>
        if c.mode == "" then
          failure("Invalid command")
        else if Set(c.dataPort, c.cncPort, c.storeTransferPort).size != 3 then
          failure("data-port, cnc-port, and store-transfer-port must all be different")
        else if c.mode == "system-rebalance-period" then
          // Validated here rather than in the handler so a malformed period is a usage error,
          // reported before any client or network is built.
          parseRebalancePeriod(c.rebalancePeriod, c.rebalancePeriodUnit) match
            case Left(msg) => failure(msg)
            case Right(_) => success
        else
          success
      }
```

- [ ] **Step 4: Add the dispatch case**

Immediately after `case "rebalance" => rebalance(bootstrapConfigPath, cfg.setId)` (line 725), add:

```scala
            case "system-rebalance-period" =>
              systemRebalancePeriod(bootstrapConfigPath, cfg.rebalancePeriod, cfg.rebalancePeriodUnit)
```

- [ ] **Step 5: Add the handler**

Insert immediately after the `rebalance` handler (after line 1788, before the `resolveRef` doc comment):

```scala
  /** Display the automatic rebalancing period, or set it. checkConfig has already rejected a
   *  malformed period, so the Left branch here is belt and braces. */
  def systemRebalancePeriod(bootstrapConfigFile: os.Path,
                            period: Option[String],
                            unit: Option[String]): Int =
    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    parseRebalancePeriod(period, unit) match
      case Left(msg) =>
        println(s"Invalid period: $msg")
        1

      case Right(None) =>
        awaitAndReport(RebalancingDurableService.getAutoRebalanceStatus(client)):
          case Success((current, lastSweep)) =>
            println(formatRebalanceStatus(current, lastSweep))
          case Failure(err) =>
            println(s"Could not read the rebalance period: ${err.getMessage}")

      case Right(Some(d)) =>
        awaitAndReport(RebalancingDurableService.setAutoRebalancePeriod(client, d)):
          case Success(_) =>
            println(s"Automatic rebalancing period set to ${formatRebalancePeriod(d)}")
          case Failure(err) =>
            println(s"Could not set the rebalance period: ${err.getMessage}")
```

The nudge that `setAutoRebalancePeriod` sends is drained by the existing centralized `drainAndShutdown()`; the handler adds no drain logic of its own.

- [ ] **Step 6: Verify it compiles and the suites still pass**

Run: `sbt compile 'testOnly *MainSuite'`
Expected: compiles clean, MainSuite passes.

- [ ] **Step 7: Verify the command by hand**

Run: `sbt 'runMain org.aspen_ddp.aspen.cmdline.Main --help'`
Expected: the usage output lists `system-rebalance-period` with its `<period>` and `<unit>` arguments.

Run: `sbt 'runMain org.aspen_ddp.aspen.cmdline.Main system-rebalance-period /nonexistent 4 fortnights'`
Expected: a usage error reading `unknown unit 'fortnights': expected minutes, hours, or days` or the missing-config-file error, depending on which validator scopt reaches first. Either confirms the parser is wired in.

- [ ] **Step 8: Clear the TODO item**

Replace the entire contents of `TODO.txt` with an empty file — the whole file describes only this work.

```bash
: > TODO.txt
```

- [ ] **Step 9: Run the full test suite**

Run: `sbt test`
Expected: PASS. Investigate any failure before committing; do not commit a red suite.

- [ ] **Step 10: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala TODO.txt
git commit -m "feat: add the system-rebalance-period command"
```

---

## Notes for the Reviewer

Three behaviors are deliberate and easy to mistake for bugs:

1. **`lastSweep` advances even when sets failed.** Holding it back would turn the 2-minute poll into a cluster-wide retry loop for as long as one set stayed broken.
2. **The level-0 filter lives in the sweep, not in the shared enrollment path.** Moving it down would silently turn `aspen rebalance <level-1-set>` into a no-op instead of the error a user needs.
3. **`setAutoRebalancePeriod` never touches `lastSweep`.** That single rule is what makes shortening the period sweep promptly, lengthening it defer relative to the last real sweep, and re-enabling after a disable sweep straight away.

## Deviations From the Spec

Three places where this plan names things differently from the spec's sketches. Same behavior, same stored bytes; called out so a reviewer reading both does not think one is wrong.

| Spec | Plan | Why |
|---|---|---|
| `encodeAutoRebalancePeriod(minutes: Long)`, `DefaultAutoRebalancePeriodMinutes = 480` | `encodeAutoRebalancePeriod(period: Duration)`, `DefaultAutoRebalancePeriod = Duration(8, HOURS)` | The decoder already returns a `Duration` and the whole API speaks `Duration`; a `Long` on the encode side alone would be the only place callers had to convert. Encoded bytes are identical. |
| A private *overload* of `rebalanceStorageDeviceSet` taking a `StorageDeviceSetState` | A distinctly named private `rebalanceReadSet` | An overload differing only in arity reads ambiguously at the call site, where the whole point is which one already did the read. |
| Two cmdline helpers (`parseRebalancePeriod`, `formatRebalancePeriod`) | Those two plus `formatRebalanceStatus` and `formatWallTime` | The spec specified the display block's exact output but named no helper for it; keeping it pure is what makes that output testable in `MainSuite`. |
