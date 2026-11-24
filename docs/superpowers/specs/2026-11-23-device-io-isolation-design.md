# Device I/O isolation

**Status:** done, 2026-11-24 (commits `fd82a6f3..HEAD` on `device-threads`; measurements and defaults in
`docs/host-threading-measurement.md`, "Device I/O isolation"). Project 1 of three in `Todo.txt` "Hung devices: isolate,
detect, handle".

## Intent

A storage device can hang: every read, write, fsync, stat, open or close on it blocks indefinitely. Today one hung
device can freeze the whole host, because device I/O runs on threads that every device shares:

- the control thread (`aspen-host-control`) does a large share of the host's device I/O itself;
- FSIF's `fsif-io` / `fsif-serial` pools and `FsifOpenEc` are JVM-global;
- RocksDB reads, writes and closes run on the host `ec` (3 threads, `Main.scala`), which also runs heartbeats,
  `BackgroundTaskManager` timers, repair and the device-state transactions, so three stuck RocksDB writes stop the
  host's heartbeats and it looks dead to the cluster;
- rebuild staging I/O runs on the 3-thread `aspen-client` pool and `Await`s there;
- `CrlLock.held` (a JVM-global monitor) and `StoreTransferManager.directoryLock` are held across file I/O, and the
  control thread can wait on the latter.

The goal of this project: **a hung device stalls only its own stores.** The host keeps serving every other device's
stores, keeps heartbeating, and keeps managing its other devices (scans, loads, transfers, rebuilds, reconciles).

Detection (project 2) and handling/notification (project 3) come later. This design must make them possible without
reworking it: in particular there must be one place per device that sees every operation on that device begin and end.

## Goals

- G1. Every thread that performs or waits on device I/O belongs to a set dedicated to exactly one device.
- G2. The control thread, store threads, host `ec`, `aspen-client`, network threads and timers never perform or
  wait on device I/O, and hold no lock that a device-I/O thread holds across I/O.
- G3. One per-device choke point (`DeviceIo`, below) through which all of the device's I/O is issued, able to
  observe each operation's start and end (kind, store, size) for project 2.
- G4. A hung device's pending work cannot exhaust host memory over a long hang.
- G5. No throughput or latency regression on the existing single-device benchmarks beyond noise.

## Non-goals

- Detecting a hang, alerting, or acting on one (projects 2 and 3). Pass 1 has no timeouts on device I/O.
- Reclaiming threads stuck in a hung syscall. They cannot be interrupted; isolation confines them and leaves them.
- Finishing a hung device's shutdown, unload or transfer. Those wait as long as the device hangs; they must just not
  block anything else.
- Host-root disk hangs. The host root (`join-ceiling`, gossip state, bootstrap config, `storage-devices/` listing) is
  host-wide by nature; a hung host root is a host failure.
- Backward compatibility with persisted data (not required by the project).

## Anti-goals

- Any automatic fail-stop, store deletion or lost-store declaration triggered by slowness or a hang.
- Breaking the single-owner, lock-free model of `HostThreading` (control state owned by the control thread, store
  state by its executor).
- Shared state across devices guarded by new locks.

## Approaches considered

- **A. One control thread; device I/O as jobs on per-device threads (chosen).** The control thread keeps every
  decision and all cross-device logic; each step that touches a disk becomes a coarse job posted to the device's
  management lane, whose result comes back as a control event. Keeps the single-owner model and every cross-device
  rule intact (single store owner, `HostCopies`, duplicate retirement). Cost: synchronous control sequences become
  decide / run job / apply-result, and each must recheck its state when the result arrives.
- **B. A management thread per device** owning that device's lifecycle state, doing its I/O synchronously, with the
  host control thread keeping only cross-device work. Less async rewriting, but state that spans devices today
  becomes inter-thread protocols, the riskiest place to add races. Rejected.
- **C. Wrap every blocking call in place** (A with one job per syscall). Same isolation as A with many more
  intermediate states. Rejected.

## Design

### 1. `DeviceIo`: one per device path

A `DeviceIo` owns every thread that touches one device. It is created for a path under `<host-root>/storage-devices`
the first time discovery sees it, before anything about the device is read (the first stat of a device path can hang),
and lives while that path is known to the host. Threads are daemon threads named with the device id when known (path
basename before that), e.g. `dev-<id>-mgmt`, `dev-<id>-io-N`.

It has:

- **Management lane**: one thread, FIFO. Runs control-issued jobs: scans of the device directory, `config.yaml` and
  marker reads/writes, `CopyLayout.list`, copy moves/retire/release/delete (`os.remove.all`), staging sweeps,
  `crl_save.log` reads and writes, device CRL open and recovery (including the lost-CRL path), CRL creation, statfs,
  RocksDB opens and creates, FSIF opens and creates (replacing `FsifOpenEc` for this device).
- **Data pool**: N threads (default max(4, CPUs/2), configurable, see section 8). Runs backend data I/O: FSIF's `io` executor
  (reads, writes, journal, checkpoint, compaction moves, forces), RocksDB foreground ops (`BufferedConsistentRocksDB`
  puts/deletes/gets/iteration), rebuild staging writes and flushes.
- **FSIF serial pool**: 1 thread (default), the base for the device's FSIF stores' `SerialExecutor`s, replacing
  `sharedSerial`. Kept per device rather than shared because serial work still makes inline I/O calls (compaction's
  truncate) and FSIF `close()` closes channels there, and `FileChannel.close` waits for threads blocked in I/O on that
  channel. A shared serial pool would require proving no serial task ever touches the device; per device avoids the
  proof.
- The device's existing CRL threads stay as they are (already per device); its CRL open moves onto the management
  lane.

`DeviceIo` API (shape; names are the builder's):

```scala
trait DeviceIo:
  def deviceKey: DeviceKey                       // path, plus id once known
  /** Runs `job` on the management lane; the result is delivered as a control event (never a control callback that
   *  could run inline). `kind` labels it for observation. */
  def submit[T](kind: JobKind, storeId: Option[StoreId])(job: => T): Future[T]
  def dataEc: ExecutionContext                   // for backends; tasks wrapped for observation
  def serialEc: ExecutionContext                 // FSIF serial base
  def observer: DeviceIoObserver                 // project 2 hook; pass 1: counts and in-flight table only
  def close(): Unit                              // stops idle threads; a hung thread is abandoned
```

Every task the lane and pools run is wrapped so `DeviceIoObserver` sees `started(kind, storeId, bytes)` and
`finished(…, outcome)`. Pass 1 keeps an in-flight table (oldest start time per kind) and counters, exposed to logs and
tests only. Project 2 builds health judgment on it without callers changing.

### 2. Control thread: no device I/O, ever

`HostThreading` rule 2 changes from "slow local disk work is allowed on the control thread because it is rare" to
"the control thread performs no device I/O". Each control sequence that touches a device is restructured as:

1. **decide** on the control thread, from control state;
2. **mark** the affected copy/device busy in control state (so nothing else decides on it meanwhile);
3. **submit** one coarse job to that device's management lane (the job does all of the sequence's I/O, e.g. "retire
   copy: moves, save-file move, `saveStoreState`, fsyncs");
4. **apply** the result in a control event: clear the busy mark, recheck that the device and copy are still the ones
   the job was for (the device may have been unloaded, the copy superseded), then update state and continue.

The copy state machine already gives the place for this. The reconcile's inputs that come from disk (copy stats,
`CopyLayout.list`, phase-param reads) are gathered by one management-lane job per device and handed to
`CopyDecision.decide` on control. Actions that touch disk (`Retire`, `BringArrivedLive`, `BringRebuiltLive`,
`DropTransferIn`, `Create`, `CancelRebuild` cleanup, …) run as jobs, and the copy's `Activity` carries a new in-flight
state (e.g. `Activity.DiskJob(kind)`) that `decide` treats as busy (no new actions for that copy until it clears). This
keeps the "decide is a pure function" property and makes the in-flight state enumerable in `CopyDecisionSuite`.

Rules:

- A job touches exactly one device. Nothing on the control thread joins results across devices (no
  `Future.sequence` over devices for a scan, usage poll or reconcile): each device's result is applied on its own, so
  a hung device delays only its own steps.
- Periodic jobs (usage statfs every 20 s, device scans, reconcile input gathering) coalesce: at most one of each
  kind outstanding per device; a tick that finds one pending skips that device. This bounds a hung device's queue and
  is itself a signal for project 2.
- Discovery's host-root work (listing `<host-root>/storage-devices`) moves to a single host-root lane, so the
  "no I/O on control" rule is absolute and can be enforced mechanically. Per-path work after the listing (stat,
  `config.yaml`, markers) runs on that path's `DeviceIo`.
- The startup scan, which today runs on the Host constructor thread, posts the same jobs.

Known sequences to convert (from the 2026-11-23 inventory; the builder re-greps): `DeviceDiscovery` (scan, load,
adoption `bringIntoService`, `openDeviceCrl` and the lost-CRL path, sweeps), `StoreLoader` (`tryLoadStore` config
read, `handleLoad`'s `crl_save.log`/incarnation/acceptor-since/inherited-votes/arrived-at reads, existence checks,
`startOpen` now always async), `StoreCopies` (retire, release, delete), `CopyMoves`, `HostCopies` (listing and
existence), `StoreRebuilds` (staging sweeps, checkpoint drop, `RebuildingStore` construction's `RebuildState.load` and
cleanup), `StoreTransfers` / `StoreTransfersOut` (staging removal, bring-live, arrived-copy checks, extract), 
`DeviceShutdown` marker writes, `LocalDevices` usage.

### 3. Store threads

Store executors already post backend work; three sites do I/O inline and move to the device's management lane, with
results posted back to the store's executor: `StoreRouter.crlLoaded` (remove `crl_save.log` + syncDirectory),
transfer-out close's `saveStoreState`, and FSIF `estimateSize`'s file-size stats (or have FSIF track sizes in memory).

### 4. Backends

- `BackendOpener.open/create` take the device's `DeviceIo`. Both always run on its management lane and return an
  incomplete Future; `StoreLoader.startOpen`'s inline-completion path goes away (RocksDB opens no longer run on
  control).
- **FSIF**: `FsifBackend.open` gets `io = deviceIo.dataEc`, serial base `deviceIo.serialEc`. `sharedIo`,
  `sharedSerial` and `FsifOpenEc` are removed from production use (tests may keep a shared default).
- **RocksDB foreground**: `RocksDBBackend`/`BufferedConsistentRocksDB` run on `deviceIo.dataEc` instead of the host
  `ec`. Close runs there too.
- **RocksDB background (flush/compaction)**: RocksJava 8.11.3 offers no per-database or per-device thread pool:
  `Env.getDefault()` is the only real disk Env, `RocksEnv`'s constructor is not public, and `TimedEnv`/`RocksMemEnv`
  wrap a base Env and share its pools. Isolation is by sizing instead: every database sets `setMaxBackgroundJobs(k)`
  (default k = 2, at most 1 flush), and the process pools are sized to exactly cover every open database's maximum
  (LOW = sum of compaction slots, HIGH = sum of flush slots), uncapped by `maxBackgroundThreads`. A database never
  schedules more than k jobs, so a hung device's databases can occupy only their own slots and every other database
  always finds a free thread. `resizeThreadPools` already runs on every open/close.
- **RocksDB shared memory**: the shared `WriteBufferManager` and block cache stay host-wide. A hung database's
  unflushable memtables keep their share of write-buffer memory, so other databases flush earlier: degraded, not
  hung. Acceptable for pass 1; noted for project 2.
- **Rebuild staging** (`RebuildingStore`): staging writes, flushes, `RebuildState` checkpoints, `completeStaging` and
  `cleanupStaging` run on the destination device's `DeviceIo` (data pool for writes/flushes, management lane for
  the rest), chained asynchronously. No `Await` on `aspen-client` or anywhere else; FSIF `rebuildWrite`/`rebuildFlush`
  and RocksDB `rebuildWrite`/`rebuildFlush` become Future-returning.

### 5. Locks

- `CrlLock.held` becomes per CRL path (a concurrent map of path to lock object, or the `DeviceIo` owns its CRL lock
  state), and acquire/release run on the device's management lane.
- `StoreTransferManager.directoryLock` becomes per device. Control's `cancelTransferIn` / `beginTransferIn`
  supersede never take it: the removal they cause is a management-lane job on that device.
- The `RocksDBBackend` object monitor is held only across pool resizing (no I/O); unchanged.
- Audit rule for the builder: no lock reachable from a non-device thread may be held across device I/O.

### 6. Bounding a hung device's backlog (G4)

While a device hangs, its stores' executors keep running and keep posting backend work. Writes are already bounded by
the device's CRL admission (`CrlAdmission`). Reads are not: client retries over a long hang would grow the data pool's
queue without bound. The bound (my call, 2026-11-23) is applied where a client read enters the store, not inside the
pool, so a dropped read never leaves a half-made entry behind:

- `DeviceIo` counts its outstanding backend reads (submitted, not finished), across all its stores.
- When a network `Read` message (`Frontend`'s `NetworkRead`) arrives for a store whose device is over the bound
  (default a few thousand, configurable), the store drops the message before creating any state for it: no
  pending-read entry, no parked read, no answer. Callers already tolerate a silent store and read from the others.
- Internal reads (`TransactionRead`, `SnapshotRead`, `ReconcileRead`, rebuild source reads) are not dropped: each is
  already bounded by its own protocol (transactions by CRL admission, repair and reconcile scans and rebuilds by
  their own concurrency), and dropping them would need cleanup at each site.
- Writes are not counted against the read bound.

Management jobs are bounded by coalescing (section 2) and by the copy busy marks (one job per copy at a time).

### 7. Lifecycle and identity

- A `DeviceIo` follows the **path** under `<host-root>/storage-devices`, not the device id (my call): the id is known
  only after reading `config.yaml`, which can itself hang. It is created at first sight of the path and closed when
  the path is gone from the host's listing and its jobs have drained. A path that reappears while its `DeviceIo` is
  still alive reuses it, so a still-hung mount stays confined to its existing threads instead of gaining new ones.
  A hung `DeviceIo` is never drained: it stays, with its stuck threads, until the process exits. Thread names carry
  the path basename, plus the id once the device's config has been read.
- Identity conflicts (two paths with one device id, an id that changes under a path) are handled by discovery as
  today; this project does not change them.
- Jobs queued behind a hung job on the management lane stay queued. Control state for that device stays busy for as
  long as the device hangs, which is accepted: it affects only that device's stores. The periodic-job coalescing skips
  it each tick.
- `submit` returns a handle that can be cancelled before the job starts: a cancelled job never runs and its Future
  fails with a `Cancelled` result the apply step treats as "nothing happened". A superseding control step (e.g. a
  cancelled transfer-in) cancels its queued job instead of queuing another. A job already running cannot be cancelled.
- A device's unload/shutdown waits for its jobs as today's code waits for its I/O; it must only not block others.
- `Host.shutdown` closes every `DeviceIo` without waiting for hung ones.

### 7a. Stores with a copy on a hung device

A copy whose job never returns is busy forever. The rule (my call, following the anti-goals): **control never treats
a busy copy as absent, and never decides around it.** Any step that needs that copy's disk (its release, retirement,
its own load or close, a transfer out of it) waits. Steps for the same store that do not need it proceed as today.
For example, a rebuild or transfer of the store onto a healthy device can bring the new copy Active; the release of
the old copy on the hung device is then a queued job that waits. A wait affects only stores that have a copy on the
hung device, which is within the project's goal. Single-owner safety is unchanged: no rule infers anything from a
copy it cannot see. Moving a hung device's stores elsewhere (by hand or automatically) belongs to project 3; pass 1
only guarantees that such a move onto a healthy device is not blocked by the hung one.

### 8. Thread budget and performance

Per device: 1 management thread, N data threads (default 4), 1 FSIF serial thread, the existing CRL threads, plus 2
RocksDB background slots per RocksDB store. Today FSIF has max(4, CPUs) io threads shared by all devices, so one busy
NVMe device could use more than 4. The defaults must be measured against the benchmarks in
`docs/host-threading-measurement.md` (single and multiple devices, randrw and seqwrite) before and after; G5 is the
acceptance bar. N is a host option (e.g. `--device-io-threads`), and the builder may pick a different default (e.g.
scaled to CPUs) if measurement favors it. Process: measure the baseline on `main` before the backend changes land,
repeat after, with the same runs the measurement doc uses (several runs each, so noise is visible). Report both sets
of numbers. A regression outside run-to-run spread that a default change cannot fix stops the work and goes back to
the user; it does not get absorbed by changing the design silently.

### 9. Testing

- **Lockstep**: `LockstepThreading` gains device lanes. A job submitted to a `DeviceIo` is queued on the same FIFO
  (like `dispatch`), so its result arrives as a later event and the decide/apply split is exercised under the
  deterministic suites. A test hook holds a device's jobs (never runs them) to model a hang, and can release them.
  Existing integration suites will see more events per step; they must keep passing (ordering assertions may need
  updating, never weakening).
- **Hang test** (project done-criterion): a host with at least two devices, one hung via the hold hook (lockstep) and
  via a blocking fault-injection seam in `DeviceIo` (threaded suite, `ThreadedHostSuite`): the other device's stores
  keep serving reads and commits, the host keeps heartbeating, device management for the other device continues
  (a scan, a load, a transfer and a rebuild onto it complete), and the hung device's backlog stays bounded.
- **Enforcement** (mechanism, my call): job bodies live in dedicated files, one `*Disk` object per converted area
  (e.g. `StoreCopiesDisk`, `DeviceDiscoveryDisk`), whose methods take plain values (paths, ids), return plain
  results, and touch no control or store state. A usage suite in the style of `FileStatUsageSuite` scans the sources
  of `server/host`, `server/rebuild` and `server/transfer` (`StoreTransferManager`, `StoreTransfer`) and fails on any
  `os.*` filesystem call, `DurableFiles`, `FileStat`, `CrashRecoveryLog.saveStoreState/loadValidStoreState`,
  `DeviceMarkers` or `RebuildState` I/O outside an allowlist of those `*Disk` files and the `DeviceIo`
  implementation. The control side calls `deviceIo.submit(kind, store)(XDisk.op(args))`, so a grep can see the
  boundary; a closure that does I/O inline in a control file fails the suite.
- `CopyDecisionSuite` covers the new busy activity.

### 10. Documentation

Update `HostThreading`'s scaladoc (rule 2), `server/host/CLAUDE.md` (threading model, every sequence that changed),
`server/CLAUDE.md`, `server/store/CLAUDE.md` (backend executors), `server/crl/CLAUDE.md` (CRL open location,
`CrlLock`), `server/rebuild/CLAUDE.md`, and `Todo.txt` (project 1 done, notes for 2 and 3).

## Left to the builder

- Names, the exact `DeviceIo` API, and how jobs are grouped (which sequences become one job).
- Staging the work into a plan (a sensible order: `DeviceIo` + lockstep support and the hang test harness first;
  backends; store-thread sites; control sequences one class at a time behind the enforcement suite; locks; backlog
  bound; measurement).
- Default thread counts and the read-queue bound, within G5.
- Whether FSIF `estimateSize` moves to a job or to in-memory tracking.
