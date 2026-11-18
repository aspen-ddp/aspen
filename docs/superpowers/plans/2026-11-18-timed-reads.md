# Timed Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `client.withReadTimeout(d)` returns an `ObjectReader` whose reads fail with `ReadTimeout` after `d` and
stop their drivers. Untimed reads are unchanged.

**Architecture:** `ReadManager.read` takes a `timeout` (default `Duration.Inf`). A finite timeout schedules one task
on the client's `backgroundTaskManager`, which calls a new `ReadDriver.timeOut()` and releases the read.
`BaseAspenClient`'s read bodies move into internal variants that take a timeout. The public methods pass
`Duration.Inf`, and a small `TimedObjectReader` view passes its own timeout.

**Tech Stack:** Scala 3, ScalaTest (AnyFunSuite, IntegrationTestSuite), sbt.

**Spec:** `docs/superpowers/specs/2026-11-18-timed-reads-design.md`

## Global Constraints

- Untimed reads behave exactly as today: no timer is scheduled and no existing signature changes.
- A timed-out read fails with the existing `org.aspen_ddp.aspen.client.ReadTimeout(pointer)`. It sends no further
  Reads and leaves `ReadManager.outstandingReads`.
- `withReadTimeout(Duration.Inf)` returns the client itself. Any other non-finite or non-positive duration throws
  `IllegalArgumentException`.
- The timeout bounds the object read, from the point the driver begins. IDA resolution is not bounded.
- Commits go directly on `main`, and each one ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Tests must be deterministic and fast (no flakes). Real-time waits stay at or under 500 ms.

## Review Focus

- Timer and completing response race: exactly one outcome, no exception. (Task 1: fire the timer after completion.)
- `Duration.Zero`, negative, `MinusInf`, `Undefined` passed to `withReadTimeout` throw IAE. (Task 2.)
- Composite reads (`readAppendPosition`, `readRange`) carry the timeout through `readByteRanges`. (Task 2.)
- `readOptional` on the timed view must not swallow `ReadTimeout` (it recovers only `InvalidObject`). (Task 2.)
- An untimed read must never time out, so no timer may be scheduled for it. (Task 1.)

---

### Task 1: ReadManager timeout and ReadDriver.timeOut

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadManager.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/read/ReadManagerSuite.scala`

**Interfaces:**
- Produces: `ReadDriver.timeOut(): Unit`, which fails an incomplete read with `ReadTimeout(objectPointer)` and runs
  `onComplete()` (SimpleReadDriver cancels its retransmit task there). It does nothing once the read is complete.
- Produces: `ReadManager.read(objectPointer: ObjectPointer, ida: IDA, comment: String, readType: ReadType = FullObject(), timeout: Duration = Duration.Inf): Future[ObjectState]`

- [ ] **Step 1: Write the failing tests** in `ReadManagerSuite`.
  - Add a local `CapturingTimers extends BackgroundTaskManager(ExecutionContext.parasitic)`. It overrides
    `schedule(delay)(fn)` to record `Scheduled(delay, fn, var cancelled)` and returns a task whose `cancel()` sets
    `cancelled`. It also has a `fire(s)` method that runs `fn` unless `cancelled`.
  - Add an `InlineClient` subclass that overrides `backgroundTaskManager` with the timers and records sent Reads.
  - Add a factory whose `BaseReadDriver` overrides `onComplete()` to count calls.

```scala
test("a read past its timeout fails with ReadTimeout, is released, and stops its driver") {
  val result = mgr.read(ptr, ida, "test", timeout = 5.seconds)
  timers.scheduled.map(_.delay) should be (List(5.seconds))
  timers.fire(timers.scheduled.head)
  val e = intercept[ReadTimeout](Await.result(result, 1.second))
  e.pointer should be (ptr)
  mgr.outstandingReadCount should be (0)
  completions should be (1)
  // A late reply is dropped: no read left to deliver it to, and the outcome stands
  mgr.receive(respond(ds0, HLCTimestamp.now).copy(readUUID = uuid))
  mgr.outstandingReadCount should be (0)
  result.value.get.isFailure should be (true)
}

test("a read that completes before its timeout cancels the timer, and a late firing changes nothing") {
  val result = mgr.read(ptr, ida, "test", timeout = 5.seconds)
  mgr.receive(respond(ds0, ts).copy(readUUID = uuid)); mgr.receive(respond(ds1, ts).copy(readUUID = uuid))
  Await.result(result, 1.second) shouldBe a [DataObjectState]
  timers.scheduled.head.cancelled should be (true)
  timers.scheduled.head.fnRunIgnoringCancel()   // the race: the timer was already running
  Await.result(result, 1.second) shouldBe a [DataObjectState]
  completions should be (1)
}

test("an untimed read schedules no timer") {
  mgr.read(ptr, ida, "test")
  timers.scheduled should be (empty)
}
```

  `fnRunIgnoringCancel` runs `fn` directly. It models a timer that had already started when `cancel` was called.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `sbt 'testOnly *ReadManagerSuite'`
Expected: compilation fails because `timeout` is not a parameter of `ReadManager.read`.

- [ ] **Step 3: Implement.**
  - `ReadDriver`: declare `timeOut()` with a scaladoc.
  - `BaseReadDriver.timeOut()`: `synchronized`. If the promise is incomplete, log at info, call
    `reportIfSlow("timed out")`, run `promise.failure(new ReadTimeout(objectPointer))`, then `onComplete()`.
  - `ReadManager.read`: once the driver has begun, and only when `timeout.isFinite`, schedule
    `client.backgroundTaskManager.schedule(timeout) { driver.timeOut(); synchronized { outstandingReads -= readUUID } }`.
    When `driver.readResult` completes, cancel the task (`onComplete(_ => task.cancel())` on `ExecutionContext.parasitic`).

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `sbt 'testOnly *ReadManagerSuite *BaseReadDriverSuite'`
Expected: every test passes.

- [ ] **Step 5: Commit**: `git commit -m "ReadManager reads can time out with ReadTimeout and stop their driver"`

---

### Task 2: The timed view, `AspenClient.withReadTimeout`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/internal/TimedObjectReader.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (abstract `withReadTimeout`, near `read`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala:101-201`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/TimedReadSuite.scala` (IntegrationTestSuite)

**Interfaces:**
- Consumes: `ReadManager.read(..., timeout)` from Task 1.
- Produces: `AspenClient.withReadTimeout(timeout: Duration): ObjectReader`
- Produces: `class TimedObjectReader(val client: BaseAspenClient, val timeout: FiniteDuration) extends ObjectReader`.
  It implements each abstract `ObjectReader` method by calling the `BaseAspenClient` variant of the same name that
  takes an extra trailing `timeout: Duration` parameter. Those variants are `private[client]` and declare no
  defaults, so the overloads compile.

- [ ] **Step 1: Write the failing tests** in `TimedReadSuite` (`atest`, network `net`, `client`, `radicle`).
  Silence reads with `net.withReadSilenced(net.stores.map(_.storeId).toSet)`. The data pointer is a synthetic
  `DataObjectPointer` in `Radicle.poolId`. No object needs to exist, because no store replies.

```scala
atest("a timed read of a reachable object succeeds")
  client.withReadTimeout(5.seconds).read(radicle).map(_.pointer should be (radicle))

atest("every timed read method fails with ReadTimeout while the stores are silent")
  val r = client.withReadTimeout(100.millis)
  // each of these fails with ReadTimeout (recoverWith on ReadTimeout => succeed, anything else => fail):
  //   r.read(radicle), r.readKeys(radicle, List(Key("x"))), r.readMetadata(radicle),
  //   r.readRange(dataPtr, 0, Some(4)), r.readAppendPosition(dataPtr), r.readOptional(radicle)

test("withReadTimeout(Duration.Inf) is the client itself; other non-positive or non-finite durations are refused")
  client.withReadTimeout(Duration.Inf) should be theSameInstanceAs client
  for d <- List(Duration.Zero, -1.second, Duration.MinusInf, Duration.Undefined) do
    an [IllegalArgumentException] should be thrownBy client.withReadTimeout(d)
```

  (The last test needs a live client. Use `atest` and return `succeed` if that is the suite's idiom.)

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `sbt 'testOnly *TimedReadSuite'`
Expected: compilation fails because `withReadTimeout` is not a member of `AspenClient`.

- [ ] **Step 3: Implement.**
  - In `BaseAspenClient`, move each read override's body (`read` ×2, `readKeys`, `readKeyRange`, `readMetadata`,
    `readRange`, `readRanges`, `readRangedWriteState`, `readAppendPosition`) and `readByteRanges` into the
    timeout-taking variant. Each `rmgr.read` call passes `timeout = timeout`. The public overrides call their variant
    with `Duration.Inf`.
  - `withReadTimeout`: `Duration.Inf` returns `this`. A finite positive duration returns
    `new TimedObjectReader(this, fd)`. Anything else throws an IAE whose message names the duration.
  - Scaladoc on `AspenClient.withReadTimeout`:
    - Reads are untimed by default.
    - The returned reader's reads fail with `ReadTimeout` after `timeout`, and stop retrying when they do.
    - `ReadTimeout` is not an `InvalidObject`, so the object may well exist.

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `sbt 'testOnly *TimedReadSuite *ReadManagerSuite'`
Expected: every test passes.

- [ ] **Step 5: Commit**: `git commit -m "AspenClient.withReadTimeout: an ObjectReader whose reads time out"`

---

### Task 3: UsageGuide section, backed by an example suite

**Files:**
- Modify: `UsageGuide.md`. Add `### Timed reads` after the `### AspenClient` section (before `### DataObject & KeyValueObject`).
- Create: `src/test/scala/org/aspen_ddp/aspen/examples/TimedReadsSuite.scala` (IntegrationTestSuite, modeled on `GuidedTourSuite`: its scaladoc says it is reproduced in UsageGuide.md so the guide cannot drift)

**Interfaces:**
- Consumes: `withReadTimeout` from Task 2.

- [ ] **Step 1: Write the example suite.** Write the example first so the guide copies working code. It defines
  `readIfAvailable(ptr: KeyValueObjectPointer): Future[Option[KeyValueObjectState]]`, which calls
  `client.withReadTimeout(500.millis).read(ptr)`, maps the result to `Some`, and recovers `ReadTimeout` to `None`.
  One `atest` checks two cases:
  - `readIfAvailable(radicle)` is defined while the stores are reachable.
  - `None` is returned inside `net.withReadSilenced(<all stores>)`.

- [ ] **Step 2: Run it**

Run: `sbt 'testOnly *examples.TimedReadsSuite'`
Expected: the test passes.

- [ ] **Step 3: Write the guide section.** It covers three points:
  - Reads retry until they succeed by default. That keeps error handling simple in the general case: a partition
    or downed hosts freeze the application until they are restored, then it carries on.
  - When giving up makes more sense, `client.withReadTimeout(d)` gives a reader whose reads fail with
    `ReadTimeout` after `d`. It offers every read method, and a timed-out read stops retrying.
  - `ReadTimeout` says nothing about whether the object exists.

  Then show the `readIfAvailable` example verbatim from the suite and name the suite as its source.

- [ ] **Step 4: Commit**: `git commit -m "UsageGuide: timed reads, with an example suite that keeps it compiling"`

---

### Task 4: Module docs, Todo and Backlog

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/CLAUDE.md`. Add a timed-reads bullet next to the read API.
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/CLAUDE.md`. Under read completion, describe the
  ReadManager timer, `ReadDriver.timeOut`, release from `outstandingReads`, and the fact that untimed reads
  schedule no timer.
- Modify: `Todo.txt` items 4.2, 4.3 and 7.5. Modify `Backlog.txt` lines 16-30.

- [ ] **Step 1: Edit `Todo.txt`.**
  - 4.2: append "Timed reads are available (`client.withReadTimeout`, a timed-out read fails `ReadTimeout`) for any
    read the sweep makes. The wait itself is the sweep's, not a read's."
  - 4.3: replace the Backlog cross-reference with the Backlog item's content, written as part of 4.3:
    - The repair scan's object reads (`StoreRepairer.readObject`) stall on an allocation held past expiry, or on
      any object below its read threshold or a key that stays NotRestorable.
    - Fix: switch them to timed reads and treat a `ReadTimeout` as the defer path (entry kept for a later pass),
      like `InvalidObject` short of a proof.
    - Rebuild's `restore` treats it as a failed object.
    - Note: "Timed reads are implemented (`client.withReadTimeout`, see client/CLAUDE.md); what remains is
      switching these consumers to them."
  - 7.5: delete the clause "the repair stall item's scope (see 4.3)".

- [ ] **Step 2: Delete** the Backlog entry "Repair scan stalls on an allocation held past expiry" (its whole
  paragraph, through "...client/internal/CLAUDE.md."). Confirm that nothing else cites it:
  `grep -rn "Repair scan stalls" --include=*.md --include=*.txt --include=*.scala .` should show only the plan and
  spec.

- [ ] **Step 3: Full-suite verification**

Run: `sbt test`
Expected: every suite passes, with the same test count as before plus the new tests. If something fails, rerun
that suite alone to tell a pre-existing flake from a regression, and report it either way.

- [ ] **Step 4: Commit**: `git commit -m "Docs: timed reads; Todo 4.3 absorbs the repair-scan stall Backlog item"`
