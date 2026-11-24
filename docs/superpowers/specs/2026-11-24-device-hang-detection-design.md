# Device hang detection (Hung devices, project 2)

Status: design, 2026-11-24. Builds on project 1, device I/O isolation
(`docs/superpowers/specs/2026-11-23-device-io-isolation-design.md`, branch `device-threads`). It narrows Todo.txt
"Hung devices" item 2.

## 1. Intent

A host must notice when one of its storage devices has hung, meaning I/O to it no longer returns. It records a
device-level health state, with evidence, where an operator can see it (`show-device`) and where project 3 (handling
and notification) can act on it later.

Project 1 made a hung device stall only its own stores, so the host already survives a hang. What it lacks is any
knowledge of one: a hung device's stuck jobs are visible only through `DeviceIo.observer.inFlight`, and nothing reads
it.

## 2. Goals, non-goals, anti-goals

Goals:
- Detect a hung device, busy or idle, within a few minutes.
- Publish its health in the device's metadata record: Healthy or Hung, since when, and a one-line summary of the
  evidence. Log every transition, and show the health in `show-device`.
- Keep an in-memory health value per device on the host, which project 3 can subscribe to.

Non-goals (decided):
- Slow-device detection (latency, ailing hardware). This is deferred to Backlog.txt "Slow-device detection".
- Fast detection. A few minutes is fine (user, 2026-11-24).
- Partial hangs, where some files on a device stop returning but the probe's file still works. These are not
  detected. The user, 2026-11-24, dropped the backup trigger on old in-flight operations because legitimately long
  work would raise false Hungs. Examples are a large store delete, RocksDB recovery on open, and rebuild staging.
- Any response to a hang. Alerts, fail-stop, restart and migration belong to project 3.
- Per-operation tags on backend work, latency history, and discounting for compaction or journal work.

Anti-goals:
- A false Hung from a device that is merely busy. The thresholds start conservative.
- Detection that blocks or slows the control thread, a store thread, or a healthy device.
- Detection that takes any action. Todo.txt rules out automatic fail-stop on a hang, because a bug or a false
  positive must not cause a system-wide outage.

## 3. Constraints

- A thread blocked in a hung syscall cannot be interrupted or reclaimed. The probe's thread stays stuck while the
  device hangs, and this is accepted.
- The threading rules in `server/CLAUDE.md` apply:
  - nothing blocks the control thread;
  - every piece of host state has one owner thread;
  - callbacks never run on a shared pool.

  The judge therefore reads only thread-safe, non-blocking state.
- Tests are deterministic under `LockstepThreading` and must be neither flaky nor slow. The probe's scheduling and
  all the clocks the judge reads must be injectable.
- Backward compatibility with persisted data is not required.

## 4. Approaches considered

- **A. Watch existing traffic only.** The judge calls a device Hung when its oldest in-flight observer operation is
  older than a threshold. This costs nothing extra, but it misses an idle hung device:
  - The 20 s usage poll is a statfs, which the kernel can usually answer without touching the disk.
  - The device CRL's I/O runs on its own threads, outside the observer.

  It would also need a large threshold, because some legitimate jobs run long.
- **B. A dedicated probe per device (chosen).** Each loaded device gets its own probe thread, which does a small write
  and fsync every 90 s. A device is Hung when its probe has not returned within the threshold. This covers idle
  devices, and it does not depend on how long a store's real work takes. It costs one thread per device, which stays
  stuck while the device hangs, plus one 4 KiB write every 90 s.
- **C. Probe through the existing queues** (the management lane or the data pool). This saves a thread, but the probe
  then waits behind a long delete or a full data pool, which brings back the false positives B avoids.

The user chose B, with a 90 s probe interval instead of the proposed 30 s (fast detection is not needed). The user
also dropped B's proposed backup trigger, which fired on an observer operation older than 10 minutes.

## 5. Design

### 5.1 The health probe

- Name: `HealthProbe`, with the observer kind `HealthProbe`. The name must not collide with discovery's existing
  `JobKind.DeviceProbe`, which probes a path.
- There is at most one per device path. Its thread runs only while this host has a storage device loaded at that
  path:
  - It starts when the device's load completes: the `LocalStorageDeviceState` exists and the device is in service.
  - It stops when the device is unloaded, for any reason: shutdown for a move, disown, failure, or forget.
  - It never runs for the host root, for a path that holds no device, or for a rejected, foreign or Failed device.
- Where the probe lives is left to the builder. Keeping it on the path's `DeviceIo`, armed and disarmed by load and
  unload, fits the reuse rule: `DeviceIos` keeps a `DeviceIo` across a reload.
- It runs on a single daemon thread that does nothing else, named after the device label like the device's other
  threads. No other work can queue ahead of the probe.
- Every `probeInterval` (default 90 s), the probe overwrites a 4 KiB file, `<device>/health-probe`, in place and forces
  it (a channel write then `force(false)`, or a DSYNC channel). The probe's thread creates and fills the file once.
  Overwriting in place allocates no new blocks, so a full file system does not fail the probe. The next attempt starts
  `probeInterval` after the previous one completes, so attempts never overlap.
- The probe publishes two thread-safe values:
  - the start time of the attempt in flight, if any;
  - the last completed attempt: when it ran, its duration, and ok or the error.
- Its attempts are also recorded in the device's `DeviceIoObserver` as kind `HealthProbe`.
- If the probe's thread is stuck in an attempt when the device unloads, that attempt stays recorded as in flight.
  Because the probe is one per path, a reload inherits the stuck attempt and starts no second thread, and the
  reloaded device judges Hung at once. Disarming the probe only stops new attempts.

### 5.2 The judge

- It runs on the control thread on a periodic timer (`ctx.backgroundTasks`), every 30 s like the usage poll. It reads
  each loaded device's probe state and `observer.inFlight`. Both are thread-safe and non-blocking, and the judge never
  submits device I/O.
- A device is **Hung** while its probe attempt has been in flight longer than `hangThreshold`. The default is 120 s,
  the same as the Linux kernel's hung-task warning.
- Otherwise the device is **Healthy**. A Hung device returns to Healthy at the first pass after its stuck probe
  returns.
- A probe that fails with an error, such as EIO, does not make the device Hung: this project detects hangs only. The
  error is logged (once, while the error persists) and kept in the evidence.
- The evidence is a short string, built at each transition and capped at about 1 KiB. It holds:
  - the probe's in-flight age, or its last result;
  - the observer's in-flight summary per kind: count and oldest age. `inFlight` has no store, and pool tasks record
    none, so the evidence names none.
  - whether the device is dropping client reads (`readsOverBound`, a device-level flag).
- Worst-case detection time is `probeInterval + hangThreshold + judge period`, about 4 min with the defaults.
- `DeviceIoObserver` stamps entries with `System.nanoTime()` directly. It gains an injectable clock, as the probe has,
  so judge tests can control every age.

### 5.3 State, logging and publishing

- Each loaded device's control-thread state (`LocalStorageDeviceState`) holds its current `DeviceHealth`: the status,
  the time of the last transition (an HLC timestamp), and the evidence. A device starts Healthy when it loads.
- Each transition logs once: WARN for Healthy to Hung, with the evidence, and INFO for Hung to Healthy, with how long
  the device was hung. A pass with no change logs nothing.
- The device's `StorageDeviceState` record gains three fields, added to `codec.proto` and the codec:
  - `health`: an enum, Healthy or Hung, default Healthy;
  - `healthSince`: an HLC timestamp;
  - `healthEvidence`: a string.

  A record without them decodes as Healthy.
- Publishing:
  - The host keeps a last-published health per device, alongside the current one.
  - While the two differ and no publish is in flight, the judge starts one.
  - A publish is a transaction run on the client context, never on the device, so a host can publish a hang through
    its other devices and the network. It reads the record and writes back the health current when its transaction
    body runs, under a `KeyRevision`, as `StorageDeviceUsageManager.updateDeviceUsage` does. Because it writes the
    current health, a later transition never loses to an earlier publish.
  - On success, the last-published health becomes the value written. On any failure (collision, timeout, unknown
    outcome), the next judge pass retries, because the two still differ. A lost publish is therefore never permanent.
  - A publish writes nothing when the record's status is Failed: an operator has declared the device dead, and its
    record is a tombstone. It also writes nothing when the record names another host.
- On load, the last-published value is initialised from the device's record. A stale Hung left by an earlier
  incarnation is then republished as Healthy once the first judge pass finds the device working.
- Every other writer of `StorageDeviceState` uses `state.copy(...)`, so the health fields carry through unchanged
  (verified). The only writer that builds a record from scratch is device creation (`BaseAspenClient`), where the
  defaults apply.
- `show-device` prints a `Health:` line, for example `Health: Hung since <time> (probe in flight 3m12s; ...)`. The
  host writes this value itself, so a host that has died leaves its last value in place. Host liveness (heartbeats)
  is a separate signal, and writing a host-down health is not this project's job.

### 5.4 Configuration

- `probeInterval` (90 s) and `hangThreshold` (120 s) are configuration. They go in `DeviceIo.Config` or a sibling
  config, plumbed like `--device-io-threads`, with the flags `--device-probe-interval` and `--device-hang-threshold` in
  seconds.
- The judge's period (30 s) is a constant.

### 5.5 Hook for project 3

- The host exposes each device's current health, and a transition callback on the control thread. Project 3 can then
  attach alerting, or the suspected-bad-device response, without changing the detector.

## 6. Testing

- **Probe unit tests**, with an injectable clock and a blocking file-I/O seam:
  - a normal attempt records ok;
  - a blocked attempt shows as in flight, with its start time;
  - a failing attempt records its error;
  - attempts never overlap;
  - unload stops new attempts;
  - a reload inherits a stuck attempt and starts no second thread.
- **Judge tests**, deterministic, with injected time and fed probe state:
  - Healthy below the threshold, and Hung above it;
  - back to Healthy once the stuck probe completes;
  - exactly one log line per transition;
  - a probe error alone stays Healthy;
  - an old non-probe observer operation does not trip Hung.
- **Publish tests**:
  - a transition writes the three fields;
  - a failed publish is retried on the next pass;
  - a transition during a publish ends with the newer value published;
  - usage and store-entry writes keep the fields;
  - nothing is written to a Failed record;
  - a stale Hung is cleared after load.
- **Codec round trip** for the new fields, including a record without them.
- **Hung-device test.** Extend the project 1 threaded test, which blocks device A through the `DeviceIo` seam. The
  probe's seam must block too.
  - A goes Hung within the test's thresholds, while B stays Healthy.
  - A returns to Healthy after the latch opens.
  - `ThreadedDeviceIo` stamps pool tasks before the gate, so tasks waiting on the latch already show as in flight.
  - The thresholds are short, sized against the 80 s budget for a full run.
- **`show-device` formatting** of both states.
- Every test is fast: intervals and thresholds are milliseconds, or the clock is driven by hand.

## 7. Left to the builder

- Where `HealthProbe` lives, and where its configuration goes.
- How lockstep tests drive the probe and the judge: a manual tick, or the existing lockstep timer seam.
- The exact evidence wording, and the `show-device` layout around the existing lines.
- Whether the publish shares `StorageDeviceUsageManager`'s pointer cache or has a small manager of its own.
- The module docs:
  - `server/host/CLAUDE.md`: the probe, its thread and lifecycle, and the judge.
  - `common/metadata/CLAUDE.md`: the new fields and who writes them.
  - Todo.txt item 2, which is narrowed to hangs and points to this spec.
