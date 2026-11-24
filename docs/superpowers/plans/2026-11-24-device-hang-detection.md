# Device Hang Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect a hung storage device with a dedicated per-device probe. Record its health (Healthy or Hung, since,
evidence) on the host and in the device's metadata record, log each transition, and show it in `show-device`.

**Architecture:**
- **Probe.** Each loaded device's `DeviceIo` owns a `HealthProbe`: one daemon thread that overwrites and forces a
  4 KiB file every 90 s.
- **Judge.** A judge on the control thread reads each probe's in-flight age every 30 s. It sets the device's
  `DeviceHealth`, logs the transition and calls the transition hooks.
- **Publisher.** A publisher writes the health into `StorageDeviceState` through the client, retrying on later passes
  until the record matches.

**Tech Stack:** Scala 3, os-lib, java.nio `FileChannel` (force), ScalaPB `codec.proto`, ScalaTest; tests run under
`LockstepThreading`, plus the threaded hung-device suite.

**Spec:** `docs/superpowers/specs/2026-11-24-device-hang-detection-design.md` (read it; it is the authority).

## Global Constraints

- Defaults: `probeInterval` = 90 s, `hangThreshold` = 120 s, judge period = 30 s (a constant).
- Hung means only this: the probe attempt in flight is older than `hangThreshold`.
  - A probe error alone never makes a device Hung.
  - No other observer operation trips Hung, however old it is (the backup trigger was dropped, spec §4).
- The probe file is `<device>/health-probe`: 4 KiB, created and filled once, then overwritten in place and forced.
- Each probe attempt waits until `probeInterval` after the previous one completed, so attempts never overlap.
- The probe's thread runs only while this host has the device loaded. It is armed when the load completes and
  disarmed on unload, and never runs for the host root. There is one probe per path: a reload inherits a stuck
  attempt and starts no second thread.
- Observer kind of probe attempts: `HealthProbe`. The name must not collide with `JobKind.DeviceProbe`.
- Logging: WARN on Healthy→Hung (with evidence), INFO on Hung→Healthy (with the hung duration), and nothing on a pass
  with no change. A probe error logs once while it persists.
- Evidence is capped at 1024 characters. It never names a store, because the observer has none.
- Publishing:
  - It runs on the client context, never on the device.
  - The transaction writes the health that is current when its body runs.
  - It never writes to a record whose status is Failed, or to one naming another host.
  - A failed publish is retried by the next judge pass.
- The judge never blocks and never submits device I/O.
- New CLI flags `--device-probe-interval` and `--device-hang-threshold`, in seconds, each at least 1.
- Test rules (CLAUDE.local.md):
  - No flaky tests.
  - Every test is as fast as possible: millisecond intervals or hand-driven clocks.
  - A full run must fit the 80 s hang-detection budget.
- Server threading rules in `server/CLAUDE.md`; module docs updated with the behavior (spec §7).

## Review Focus

1. The new `health-probe` file in every loaded device's root must not disturb device discovery, the copy layout
   listing, sweeps, or tests that inspect device directories. A scan must load such a device exactly as before.
   Test: Task 2.
2. A device unloaded while its probe is stuck, then reloaded, must show Hung at once without starting a second
   thread. Test: Task 2 (probe) and Task 3 (judge).
3. After a host restart, a record left Hung by the previous incarnation must end Healthy once the device works. A
   record already Healthy must get no write at all. Test: Task 4.
4. A health publish that races a usage write or a store-entry write: neither may lose the other's fields. Test:
   Task 4.
5. Host shutdown (`DeviceIo.close`) with a probe stuck must not hang or throw. A forgotten path's probe must be
   disarmed. Test: Task 2.

---

### Task 1: Health fields in StorageDeviceState, codec, show-device

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceState.scala`
- Modify: `src/main/protobuf/codec.proto` (enum + fields 8–10 of `StorageDeviceState`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala:1470-1545`
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (`formatDeviceState`, ~2889)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/CLAUDE.md`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala`,
  `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

**Interfaces:**
- Produces:
  - `StorageDeviceState.HealthStatus`, an enum with `Healthy` and `Hung`.
  - New trailing fields on `case class StorageDeviceState`:
    - `health: StorageDeviceState.HealthStatus = HealthStatus.Healthy`
    - `healthSince: HLCTimestamp = HLCTimestamp.Zero`
    - `healthEvidence: String = ""`
  - Proto:
    - `enum StorageDeviceHealth { STORAGE_DEVICE_HEALTH_HEALTHY = 0; STORAGE_DEVICE_HEALTH_HUNG = 1; }`
    - On `StorageDeviceState`: `StorageDeviceHealth health = 8; uint64 health_since = 9; string health_evidence = 10;`

- [ ] **Step 1: Write the failing tests**
  - `CodecRoundTripSuite`, test `"StorageDeviceState health round-trip"`:
    - A state with `health = Hung`, `healthSince = HLCTimestamp(123456L << 16)` and `healthEvidence = "probe in
      flight 3m12s"` decodes equal to itself.
    - A state built with defaults decodes with `health == Healthy`, `healthSince == HLCTimestamp.Zero` and
      `healthEvidence == ""`.
  - `MainSuite`, test `"formatDeviceState renders health"`:
    - A Hung state's output includes `"  Health:     Hung since "` and the evidence text.
    - A default state's output includes `"  Health:     Healthy"`.
- [ ] **Step 2: Run them, expect compile failure.** Run: `sbt 'testOnly *CodecRoundTripSuite *MainSuite'`
- [ ] **Step 3: Implement.**
  - Add the fields and the encode/decode helpers `encodeStorageDeviceHealth` / `decodeStorageDeviceHealth`, modelled
    on `encodeStorageDeviceStatus`.
  - In `formatDeviceState`, place the Health line right after the Status line, in the form
    `Health:     Hung since <since rendered as wall time> (<evidence>)` or `Health:     Healthy`.
  - Document the fields in `common/metadata/CLAUDE.md`. The hosting host writes them (Task 4), and every other writer
    carries them through `copy`.
- [ ] **Step 4: Run.** Run: `sbt 'testOnly *CodecRoundTripSuite *MainSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "StorageDeviceState health fields, codec and show-device"`

### Task 2: HealthProbe on each DeviceIo, armed by load, disarmed by unload

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/host/HealthProbe.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/DeviceIo.scala`
  - `DeviceIoObserver` gains a clock.
  - `DeviceIo.Config` gains `probe`.
  - `DeviceIo` gains `def probe: HealthProbe`.
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/ThreadedDeviceIo.scala`, `LockstepDeviceIo.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/DeviceDiscovery.scala` (~916, arm after
  `devices += ...`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/DeviceShutdown.scala` (`unloadDevice`, ~395, disarm)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/HealthProbeSuite.scala` (new),
  `src/test/scala/org/aspen_ddp/aspen/server/host/ThreadedDeviceIoSuite.scala`, and a discovery suite
  (`DiscoveryIoSuite`)

**Interfaces:**
- Produces:

```scala
final class DeviceIoObserver(clock: () => Long = System.nanoTime):  // stamps started() with clock()
  def now: Long                                                       // clock()

object HealthProbe:
  val FileName = "health-probe"; val Size = 4096; val Kind = "HealthProbe"
  final case class Config(interval: FiniteDuration = 90.seconds, hangThreshold: FiniteDuration = 120.seconds)
  /** A completed attempt: when it ended (probe clock), how long it took, and its error if it failed */
  final case class Result(endNanos: Long, duration: FiniteDuration, error: Option[Throwable])
  def writeAndForce(file: os.Path): Unit   // creates+fills Size bytes if absent/short, else overwrites in place; force(false)

final class HealthProbe(devicePath: os.Path, cfg: HealthProbe.Config, observer: DeviceIoObserver,
                        threadName: () => String, gate: () => Unit = () => (),
                        write: os.Path => Unit = HealthProbe.writeAndForce,
                        clock: () => Long = System.nanoTime):
  def config: HealthProbe.Config
  def arm(): Unit                // starts the thread if none is alive; first attempt at once
  def disarm(): Unit             // no new attempts; wakes a waiting thread, which exits; an attempt in flight stays
  def armed: Boolean
  def inFlightAge: Option[FiniteDuration]   // clock() - start of the attempt in flight
  def lastResult: Option[HealthProbe.Result]
  def hung: Boolean              // inFlightAge.exists(_ > cfg.hangThreshold)
  private[host] def threadAlive: Boolean

// DeviceIo.Config gains: probe: HealthProbe.Config = HealthProbe.Config()
// trait DeviceIo gains:  def probe: HealthProbe
```

- The probe's thread loop:
  - while armed: run one attempt, then wait `cfg.interval` on a monitor, which `disarm` notifies;
  - each attempt runs `gate()` then `write(devicePath / FileName)`, recorded in `observer` as kind
    `HealthProbe.Kind` with 4096 bytes;
  - `NonFatal` errors are caught into `Result.error`.
- `ThreadedDeviceIo` builds its probe with its own gate, its observer, and the thread name `dev-<label>-probe`.
  `close()` and `closeLane()` call `probe.disarm()`.
- `LockstepDeviceIo` delegates `probe` to its internal `ThreadedDeviceIo`, so holding a path stalls the probe too.

- [ ] **Step 1: Write the failing tests** in `HealthProbeSuite`. Use a manual clock (`var t = 0L`), a `write` that
  blocks on a latch or throws on demand, and `interval = 5.millis`. Each wait polls a condition with a deadline of a
  few seconds.
  - `"an attempt records ok and its duration"`: arm, then wait for `lastResult.exists(_.error.isEmpty)`.
  - `"a blocked attempt is in flight with its age"`:
    - Block the write, arm, and wait for `inFlightAge.isDefined`.
    - Set the clock 121 s later: `hung` is true, and the observer's `inFlight` has key `"HealthProbe"`.
    - Open the latch: `inFlightAge` becomes None and `hung` is false.
  - `"a failing attempt records its error and is not hung"`: the write throws `IOException`. `lastResult` holds the
    error, and `hung` is false.
  - `"attempts never overlap"`: a write that counts concurrent entries never sees 2 over 50 attempts.
  - `"disarm stops attempts and the thread exits"`: after `disarm`, `threadAlive` becomes false and the attempt count
    stops growing.
  - `"rearm while an attempt is stuck starts no second thread"`:
    - Block the write, arm, wait for in flight, then disarm and arm again.
    - Exactly one thread named `dev-x-probe` is alive, and `inFlightAge` is still defined.
    - Open the latch, and attempts resume on that thread.
  - `"writeAndForce creates a 4 KiB file and overwrites it in place"`: the size is 4096 after the first and the
    second call, on a temp dir.
- [ ] **Step 2: Add the remaining failing tests.**
  - `ThreadedDeviceIoSuite`, test `"close disarms the probe, even while an attempt is stuck"`: close returns without
    blocking, and `probe.armed` is false.
  - A discovery suite test, `"a device whose root holds health-probe loads and lists exactly as before"` (Review
    Focus 1): its copies and strays are unchanged, and its probe is armed after load.
  - A test that unload (`DeviceShutdown.unloadDevice`) disarms the probe.
- [ ] **Step 3: Run, expect failure.** Run: `sbt 'testOnly *HealthProbeSuite *ThreadedDeviceIoSuite *DiscoveryIoSuite'`
- [ ] **Step 4: Implement** the probe, the observer clock, the config field, `DeviceIo.probe`, and arm/disarm at the
  two `devices` writers:
  - arm in `DeviceDiscovery` after `ctx.devices.devices += ...`;
  - disarm in `DeviceShutdown.unloadDevice`, before `devices -= ...`.

  `ControlFixture.injectLoadedDevice` leaves the probe unarmed. Document that in its scaladoc.
- [ ] **Step 5: Run the touched suites, then the full suite.** Run: `sbt test`. Expected: all pass, within the usual
  time.
- [ ] **Step 6: Commit** `git commit -m "HealthProbe: per-device probe thread, armed while the device is loaded"`

### Task 3: DeviceHealth, the judge, transitions and the project 3 hook

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/host/DeviceHealthJudge.scala`
- Modify: `LocalDevices.scala` (`LocalStorageDeviceState` gains the health vars)
- Modify: `DeviceControl.scala` (`lazy val health: DeviceHealthJudge`)
- Modify: `HostServices.scala` (periodic trigger)
- Modify: `Host.scala` (the control event, if one is used)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/DeviceHealthJudgeSuite.scala` (new, `ControlFixture`-based)

**Interfaces:**
- Consumes: `DeviceIo.probe` (`hung`, `inFlightAge`, `lastResult`, `config`), `DeviceIoObserver.inFlight`/`now`,
  `DeviceIo.readsOverBound` (Task 2).
- Produces:

```scala
final case class DeviceHealth(status: StorageDeviceState.HealthStatus, since: HLCTimestamp, evidence: String)
object DeviceHealth:
  /** A device's health at load, before any transition: Healthy with no `since` */
  val Initial: DeviceHealth = DeviceHealth(StorageDeviceState.HealthStatus.Healthy, HLCTimestamp.Zero, "")

// LocalStorageDeviceState gains (control thread only):
//   var health: DeviceHealth = DeviceHealth.Initial
//   var publishedHealth: Option[DeviceHealth] = None   // Task 4
//   var healthPublishing: Boolean = false              // Task 4

final class DeviceHealthJudge(ctx: DeviceControl):
  def check(): Unit                                    // one pass over ctx.devices.devices; control thread only
  def onTransition(f: (LocalStorageDeviceState, DeviceHealth) => Unit): Unit   // project 3's hook
object DeviceHealthJudge:
  val Period: FiniteDuration = 30.seconds
  val MaxEvidence = 1024
  def evidence(probe: HealthProbe, observer: DeviceIoObserver, readsOverBound: Boolean): String
```

- A transition sets `sds.health = DeviceHealth(newStatus, HLCTimestamp.now, evidence(...))`, logs it as the Global
  Constraints say, then calls each registered hook.
- The evidence string holds:
  - the probe's in-flight age, or its last result;
  - each observer kind with its count and oldest age;
  - `dropping client reads` when `readsOverBound` is set;
  - truncated to `MaxEvidence`.
- The periodic trigger is scheduled in `HostServices` with `ctx.backgroundTasks.schedulePeriodic(DeviceHealthJudge.Period)`.
  It only hands `check()` to the control thread: a posted event, or `Future.delegate` on the control EC as the usage
  poll does. It must not wait for the TaskExecutor.

- [ ] **Step 1: Write the failing tests** in `DeviceHealthJudgeSuite`. Inject a loaded device whose probe has a manual
  clock and a latch-blocked write, and call `check()` on the control thread.
  - `"healthy below the threshold"`: the probe is in flight at 119 s, `check()` runs, and the health stays
    `Initial`. No hook is called and no WARN is logged.
  - `"hung above the threshold, once"`:
    - At 121 s, the first `check()` makes the status Hung, with `since > Zero`.
    - The evidence contains `"HealthProbe"`.
    - The hook is called once, with exactly one WARN.
    - A second `check()` changes nothing: no hook call, no log.
  - `"back to healthy when the probe returns"`:
    - After Hung, open the latch and wait for `inFlightAge.isEmpty`, then run `check()`.
    - The status is Healthy, with an INFO line that includes the hung duration.
    - The hook has been called twice in total.
  - `"a probe error alone stays healthy"`: the write throws. After `check()` the status is Healthy, and the error is
    logged once over two passes.
  - `"an old non-probe operation does not trip hung"`:
    - The observer has a `Delete` entry started 1 h earlier (manual observer clock), and the probe is idle.
    - After `check()` the status is Healthy.
  - `"a reload inherits a stuck probe and is hung at once"` (Review Focus 2):
    - The probe is stuck past the threshold, the device is unloaded and then injected again as a fresh
      `LocalStorageDeviceState` on the same `DeviceIo`.
    - The first `check()` gives Hung.
  - `"evidence is capped at 1024"`: with 200 synthetic observer kinds, the evidence length is at most 1024.
- [ ] **Step 2: Run, expect failure.** Run: `sbt 'testOnly *DeviceHealthJudgeSuite'`
- [ ] **Step 3: Implement** the judge, the health vars, the `DeviceControl.health` lazy val and the periodic trigger.
- [ ] **Step 4: Run.** Run: `sbt 'testOnly *DeviceHealthJudgeSuite *ControlContextSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "DeviceHealthJudge: hung while the probe is stuck past the threshold"`

### Task 4: Publishing health into StorageDeviceState

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/host/DeviceHealthPublisher.scala`
- Modify: `DeviceHealthJudge.scala` (publish loop at the end of each device's pass)
- Modify: `DeviceControl.scala` (publisher seam)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/DeviceHealthPublishSuite.scala` (new). Use the judge with a
  fake publisher, plus an integration test against a real client. Copy the setup of `StorageDeviceUsageManager`'s
  tests; if it has none, copy that of a `HostServices` test or the threaded host suite.

**Interfaces:**
- Consumes: `DeviceHealth`, `sds.publishedHealth`, `sds.healthPublishing` (Task 3), `StorageDeviceState` fields
  (Task 1).
- Produces:

```scala
trait DeviceHealthPublisher:
  /** Reads the device's record and writes `current()` (read inside the transaction body) into it, unless the
   *  record already holds it or must not be written. Never runs on the device. */
  def publish(id: StorageDeviceId, current: () => DeviceHealth): Future[DeviceHealthPublisher.Outcome]
object DeviceHealthPublisher:
  enum Outcome:
    case Written(h: DeviceHealth)    // committed
    case Unchanged(h: DeviceHealth)  // the record already held h: no transaction
    case Skipped                     // status Failed, or hostId is not this host: no transaction
final class ClientDeviceHealthPublisher(client: AspenClient, hostId: HostId) extends DeviceHealthPublisher
// DeviceControl: private[server] var healthPublisher: DeviceHealthPublisher = new ClientDeviceHealthPublisher(client, hostId)
```

- `ClientDeviceHealthPublisher` decides as follows, with `h = current()`:
  - The record is Failed, or its `hostId != hostId`: `Skipped`.
  - The record's `health == h.status`, and either `h.since == Zero` or `record.healthSince == h.since`:
    `Unchanged(h)`.
  - Otherwise it writes `health = h.status`, `healthSince = (if h.since == Zero then HLCTimestamp.now else h.since)`
    and `healthEvidence = h.evidence` with `state.copy`, under a `KeyRevision` on `StateKey`. It uses
    `transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision)` as `StorageDeviceUsageManager` does, and
    returns `Written(h)`.
  - No transaction is committed for `Unchanged` or `Skipped`.
- The judge's publish loop, per loaded device, after the transition step:
  - Start a publish when `!sds.healthPublishing && !sds.publishedHealth.contains(sds.health)`.
  - Set `healthPublishing = true` and pass `current = () => sds.health`. The body reads it on the client context;
    `health` must be `@volatile`, or snapshot it on the control thread before the call, which is acceptable because
    a newer value is published on the next pass.
  - Apply the outcome on the control thread with `ctx.onControlFuture`, only if `ctx.devices.isLoaded(sds)`:
    - `healthPublishing = false`;
    - `Written(h)` or `Unchanged(h)`: `publishedHealth = Some(h)`;
    - `Skipped`: `publishedHealth = Some(sds.health)`;
    - a failure: WARN once per failure streak, leave `publishedHealth`, and let the next pass retry.

- [ ] **Step 1: Write the failing tests.**
  - With a fake publisher, `"a transition is published and a failure is retried next pass"`:
    - The first pass fails the publish; the second pass publishes Hung.
    - `publishedHealth == Some(health)`.
  - With a fake publisher, `"a transition during a publish ends with the newer value"`:
    - Hung is published while the fake publisher is held.
    - The probe recovers, and the held publish completes with `Written(old)`.
    - The next pass publishes Healthy.
  - With a fake publisher, `"no publish while one is in flight"`: two passes with the publisher held start one
    publish.
  - Integration with the real client:
    - `"Hung then Healthy are written with since and evidence"`.
    - `"a healthy record at load is not written"`: the record's revision is unchanged after the first pass (Review
      Focus 3).
    - `"a stale Hung record is cleared after load"`: write Hung to the record, load the device with a working probe,
      run a pass, and the record is Healthy with `since > Zero`.
    - `"nothing is written to a Failed record"`.
    - `"usage and store-entry writes keep the health fields, and health keeps theirs"` (Review Focus 4): interleave
      `updateDeviceUsage` and `setStoreEntry` writes with a Hung publish. The final record has all three changes.
- [ ] **Step 2: Run, expect failure.** Run: `sbt 'testOnly *DeviceHealthPublishSuite'`
- [ ] **Step 3: Implement** the publisher, the seam and the publish loop.
- [ ] **Step 4: Run.** Run: `sbt 'testOnly *DeviceHealthPublishSuite *DeviceHealthJudgeSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Publish device health into StorageDeviceState until the record matches"`

### Task 5: CLI flags, the threaded hung-device test, docs

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
  - new `Args` fields beside `deviceIoThreads`/`deviceReadBound` (~147);
  - opts beside `--device-read-bound` (~477);
  - `DeviceIo.Config(..., probe = HealthProbe.Config(...))` at ~1068.
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/host/ThreadedHungDeviceSuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/CLAUDE.md`, `Todo.txt` (item 2)
- Test: `MainSuite` (flag parsing), `ThreadedHungDeviceSuite`

**Interfaces:**
- Consumes: everything above. `GatedThreading` in `ThreadedHungDeviceSuite` builds the hung device's
  `ThreadedDeviceIo` with its gate, which Task 2 also applies to the probe.

- [ ] **Step 1: Write the failing tests.**
  - `MainSuite`: `--device-probe-interval 45 --device-hang-threshold 300` parses to 45 s and 300 s, and `0` fails
    validation for each flag.
  - `ThreadedHungDeviceSuite`, new atest `"a hung device is judged Hung, the other stays Healthy, and it recovers"`:
    - Both devices use `HealthProbe.Config(interval = 50.millis, hangThreshold = 500.millis)`.
    - Drive the judge with `check()` passes instead of the 30 s timer.
    - Shut the gate: A's `health.status` becomes Hung within 5 s, and B's stays Healthy throughout.
    - A's published record shows Hung.
    - Open the gate: A's status becomes Healthy and its record shows Healthy.
    - Pool tasks are stamped before the gate, so A's evidence lists them. Assert that it contains `"HealthProbe"`.
- [ ] **Step 2: Run, expect failure.** Run: `sbt 'testOnly *MainSuite *ThreadedHungDeviceSuite'`
- [ ] **Step 3: Implement** the flags and the plumbing, then the docs:
  - `server/host/CLAUDE.md`: a "Device health" section covering the probe, its thread and lifecycle, the judge and
    its thresholds, publishing, the hook, and the limits:
    - no partial-hang detection;
    - a device that hangs before its load completes is never judged.
  - `Todo.txt` item 2: "Narrowed to hangs: spec docs/superpowers/specs/2026-11-24-device-hang-detection-design.md;
    slowness is Backlog 'Slow-device detection'". Mark it done once Task 5 passes.
- [ ] **Step 4: Run the full suite.** Run: `sbt test`, with a timeout of 80 s plus compile time. Expected: all pass.
  Then run `ThreadedHungDeviceSuite` and the new suites 5 times under load (`stress` or a parallel sbt) to check for
  flakiness.
- [ ] **Step 5: Commit** `git commit -m "Device hang detection: CLI flags, threaded hung-device test, docs"`
