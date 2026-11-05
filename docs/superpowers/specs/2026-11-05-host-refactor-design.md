# Host Refactor: Separate Concerns and Move Stores onto Their Own Threads

Date: 2026-11-05. Branch: `host-refactor`.

## Intent

`server.Host` (about 4,000 lines) has two problems.

1. **One thread does everything.** Every store, network message, I/O completion and timer on a host goes through
   one `LinkedBlockingQueue[Event]`. A single thread drains it, and `handleEvent` runs inside a host-wide
   `synchronized`. In the vdisk fio runs (`Todo.txt`, "Store commit delay") that thread uses about 9 s of CPU per
   10 s. A store sees FSIF commits take about 67 ms when FSIF itself accounts for about 19 ms. The working theory is
   that most of the gap is completions waiting in that queue. This work goes ahead whatever further measurement
   shows: a single thread for every store is a ceiling the host must not have.
2. **Distinct concerns are mixed together.** Store I/O dispatch, device discovery and load decisions, the device
   reconcile, store transfers in and out, rebuilds, device shutdown and adoption, task-executor and service
   bootstrap, endpoint reconcile and gossip retransmission all live in one class. Its 72 `synchronized` blocks
   share one lock, and Future callbacks run on the general `ExecutionContext`.

**Success means** both:
- lower store commit latency in the vdisk fio runs (BackendTiming's "store commit (FsifBackend)" line); and
- a smaller `Host` whose concerns live in separate classes that can each be read and tested on their own.

## Goals

- Run stores on dedicated **store threads**, not the host's event thread. Each store stays single-threaded: at any
  moment at most one thread runs code for a given store, and every task for that store runs in the order it was
  posted.
- Make store-to-thread assignment **pluggable** through a strategy interface, so later setups can choose differently
  (for example, all stores of one physical device on one thread on large dedicated hardware, or a small shared pool on
  a light host). Only the interface and the default need to exist now.
- Keep the **hot path** (network message → store, I/O completion → store, heartbeat → store) entirely off the
  device-management side. No host-wide lock on it, and no hop through the control thread.
- Move device management onto a **single control thread** that owns all of its state, with no `synchronized`
  (details below).
- Split `Host` into classes along its real concerns (proposed split below).
- Provide a **lockstep** threading mode for the regression tests that keeps today's single-queue, test-thread-driven
  ordering, so the existing integration suites pass unchanged.
- Document the threading model so it is obvious to the next reader, especially where it departs from ordinary Scala
  Future usage.

## Non-goals

- Choosing the ideal threading strategy. Ship the interface and a reasonable default.
- Selecting the strategy from the host config file. For now it is chosen in code only (`Host`'s constructor and
  `cmdline/Main.scala`). A config option can come once there is a second real strategy.
- Performance of device management. Discovery, reconcile, transfers, rebuilds, shutdown and adoption are infrequent.
  Simplicity wins over speed there.
- Reducing contention on the shared `HostObjectCache` (a single `synchronized` cache shared by all stores). It will
  probably be the next contention point once stores run in parallel. Record it in `Todo.txt` as follow-up work; it is
  not part of this change.
- Changing Paxos, the CRL, the transaction protocol, or store behaviour. Store, Frontend and Tx change only as much
  as threading requires.
- Backward compatibility of persisted data. None is required, though none should be needed either.

## Anti-goals (a failure even if it works)

- Any flaky test. Correctness, then reliability, then speed (see `CLAUDE.local.md`).
- Any lock taken by both a store thread and the control thread, or by two store threads. Cross-thread interaction is
  by posting a task to the owning thread, and nothing else.
- Hot-path traffic that waits on the control thread, or on a host-wide lock.
- A regression-test mode whose message ordering differs from today's. The integration suites are very sensitive to
  ordering.
- Device-management code that silently runs a callback on the wrong thread.

## Constraints and facts the design relies on

These were checked in the code on 2026-11-05.
- **The CRL is thread-safe.** `SimpleCRL.save`, `deleteTransaction`, `dropTransactionObjectData` and `closeStore`
  just `put` on its own `ioQueue`. Save completions in `Tx` (`Tx.scala:329`, `:550`, `:570`) only send a network
  message and touch no store state, so they may keep running on the CRL's thread.
- **`TransactionStatusCache` is a Caffeine cache** and is safe to share between store threads. It stays host-wide,
  because a transaction status must be visible to every store on the host.
- **`HostObjectCache` is internally `synchronized`** and safe to share (see Non-goals).
- **`TransactionDriver` is internally `synchronized`** and runs its own timers on `BackgroundTaskManager` threads. Its
  callbacks send messages and do not touch store state. It may keep its own locking. `Store.transactionDrivers` (the
  map) is touched only on the store's thread.
- **Stores do not block on I/O.** Backends complete through a `CompletionHandler` callback. A store thread is
  therefore an event loop that sits mostly idle, not a thread parked in a blocking call.
- **There are no `Await` calls in `Host`.**
- **The regression harness is single-threaded.** `IntegrationTestSuite` is a ScalaTest `AsyncFunSuite`, whose
  `executionContext` runs everything serially on the test thread. `TestNetwork` delivers each message by calling
  `handleEvents()` → `Host.testingOnlyHandleEvents()` before and after it, draining every host's queue on the test
  thread. Host's Future callbacks run on that same serial context.

## Decided design

### 1. Threading model (two kinds of thread)

**Store threads.** Each loaded store is bound to one `StoreExecutor`: a single-threaded, FIFO `ExecutionContext`. All
code that touches a `Store`, its `Frontend` or its `Tx`s runs as a task on that executor. Several stores may share
one executor (that is what the strategy decides). A store never moves between executors while it is loaded; it may get
a different one the next time it is loaded.

**The control thread.** One single-threaded `ExecutionContext` per host owns all device-management state (everything
that today lives in Host's `var`s other than the store map). Every handler, and every Future callback in that code,
runs on it.

The two sides talk only by posting tasks:
- control → store: "install this store", "close this store", "repair this object", and so on, posted to the store's
  executor;
- store → control: results, posted back to the control context (usually by completing a Promise whose callbacks run
  on the control context).

### 2. The control thread replaces `synchronized` (documented deviation from usual Futures)

**Decided:** device-management code runs on a dedicated single-threaded `ExecutionContext`, the *control context*.
Every Future combinator in that code (`map`, `flatMap`, `foreach`, `onComplete`, `transform`, `recover` and so on)
names the control context, so its callback runs on the control thread. That code then needs no locks: each handler and
callback runs to completion before the next one starts, and all mutable device-management state is read and written
only on that thread. All 72 `synchronized` blocks in `Host` go.

This is deliberately unlike ordinary Scala Future usage, where callbacks run on a shared pool and shared state needs
locks. It is an actor-style "single owner" model built from plain Futures. It needs these rules, written where every
reader will see them:
1. **Device-management classes must not have a general-purpose `given ExecutionContext` in scope.** The only `given`
   is the control context, so a callback with no context named does not compile into "runs somewhere on a pool". Work
   that must run elsewhere (an FSIF open on `FsifOpenEc`, client transactions) names its context explicitly, and its
   result comes back through a control-context callback.
2. **Never block the control thread on a Future** (`Await`, `.get` on a Java future, a `LinkedBlockingQueue.take`
   waiting for another thread's answer). The thread would deadlock against its own callbacks. Slow local disk work
   (marker files, directory moves, fsyncs through `DurableFiles`) is allowed there, because those operations are rare.
3. **State owned by the control thread is never read from other threads.** Other threads that need an answer either
   post a request and receive a Future, or read an immutable snapshot that the control thread publishes through a
   `@volatile` field (see `HostStoreView` below).
4. **Store state is never touched from the control thread,** and the reverse also holds.

Where this is documented:
- a scaladoc block at the top of the control-context holder class and of each device-management class, stating
  rules 1–4 and pointing at the others;
- a short "Host threading model" section in `CLAUDE.md` (next to "Transaction Design"), covering store threads, the
  control thread, the lockstep test mode, and the rule that callbacks name the control context;
- one comment at each place that deliberately leaves the control context (for example, the FSIF open), saying where the
  work runs and how it gets back.

### 3. Pluggable store threading

Chosen in code (constructor parameter on `Host`, built in `cmdline/Main.scala` and `TestNetwork`). Roughly:

```scala
/** How a host runs its stores and its device-management control thread. */
trait HostThreading:
  /** The single-threaded context that owns device-management state. */
  def control: ExecutionContext
  /** Binds a store being loaded to an executor. Called on the control thread. */
  def assign(storeId: StoreId, deviceId: StorageDeviceId): StoreExecutor
  /** The store has been closed or unloaded; its executor slot may be reused. Called on the control thread. */
  def release(storeId: StoreId): Unit
  def shutdown(): Unit
```

`StoreExecutor` is a single-threaded FIFO `ExecutionContext`. Exact names and signatures are the builder's call.
Passing `deviceId` to `assign` is required, so that a future "one thread per physical device" strategy needs no
interface change.

**Default strategy (my call, per your invitation to suggest a better default):** a pool sized to
`Runtime.availableProcessors()`, as you asked, with two refinements:
- **Threads are created lazily**, so a host never runs more store threads than it has loaded stores.
- **A new store goes to the least-loaded thread** (fewest stores assigned), with ties broken by thread index. Placement
  by hash would cluster stores; least-loaded spreads the stores of one device across threads.

Store threads are named (for example `aspen-store-N`) so thread dumps show them, as the Todo investigation needed.
**They are daemon threads** (my call). With a crash-only design nothing needs them to finish, and test JVMs that
create many Hosts must not hang on them. The control thread keeps today's event-loop thread semantics: non-daemon,
stopped by `shutdown()`. `shutdown()` also stops the store executors, but adds no clean-shutdown work.

**Lockstep strategy (regression tests):** see section 6.

### 4. Hot path: the store router

A `StoreRouter` (name is the builder's call) holds a concurrent map `StoreId → (Store, StoreExecutor)` and a concurrent
set of offline store ids. Network threads call it directly from `receiveTransactionMessage` and `receiveClientRequest`.
Each call looks up the store and posts the handling to that store's executor, which runs the same code
`handleEvent`'s `TransactionMessage`, `ClientReq`, `Repair` and `RepairDelete` cases run today. An unknown, non-offline
store gets today's reply (`TxUnknownStore` / `ReadError.StoreNotFound`) straight from the calling thread in
production. In lockstep mode the whole lookup-and-dispatch is deferred to the queue instead (section 6).

The local `AspenClient` and the task executor inside a host reach local stores through the network messenger, like any
other client, so they arrive through `receive*` and the router. There is no direct path to preserve.

- **I/O completions:** each store's backend gets a completion handler that posts the completion to that store's
  executor. This replaces the single host-wide `IOHandler`.
- **Heartbeat:** the heartbeat timer posts `store.heartbeat()` to each loaded store's executor.
- **`SimpleDriverRecoveryMixin`:** its periodic check becomes a per-store task posted the same way. It must not walk
  `frontend.transactions` from another thread.
- **`TransactionCompletionQuery`** reads only the thread-safe `txStatusCache`, so it may be answered on the calling
  thread.

Only the control thread writes the router's map (when installing or removing a store). Network threads only read it.

### 5. Store lifecycle across threads

The control thread decides; the store's executor acts.

**Who owns the backend.** Before a store is installed (opening it, FSIF recovery on `FsifOpenEc`, out-of-space waits,
CRL save-file validation) and after its close has completed, the backend and the store directory belong to the
control side. Between install and close, the `Store` and its backend belong to the store's executor. At any time the
control side may read a backend's immutable fields (`storeId`, `path`, `crlSaveFile`) and its `@volatile` status flags
(`FsifBackend.hasFailed`, `isWriteFrozen`). These are the only exceptions to rule 4. All file-system work on a store
directory (transfer-out shipping, markers, discard, rebuild placement) happens only while the store is not installed,
which is already how Host sequences it (for example, `closeStoreForTransferOut` before shipping). FSIF's `onFatal`
posts `BackendFailed` to the control context, which then posts any store-side action to the store's executor.

- **Load:** the control thread decides to serve a store, reads and validates its CRL save file as today, then calls
  `assign`. It posts an install task to the chosen executor. That task constructs the `Store`, sets the backend's
  completion handler and, when there is CRL state, calls `frontend.awaitRecovery()`. Only then is the store
  registered in the router. Today's guarantee is unchanged: transaction messages that arrive before recovery finishes
  are held by `Frontend` (`heldMessages`) until `frontend.recover()` runs, and that step (after `crl.loadStore`
  completes) runs on the store's executor. Outcomes go back to the control thread through Promises.
- **Close:** the control thread first removes the store from the router and marks it offline, so new traffic gets
  today's offline handling. It then posts a close task to the store's executor. That task runs today's
  `ShutdownStore` sequence (object cache clear, `crl.closeStore`, `CrashRecoveryLog.saveStoreState`, `store.close()`,
  the fail-stop check, `StoreFailedOnClose`) and completes the caller's Promise. "Drain" means only FIFO order: tasks
  queued for that store ahead of the close run first, against a store that is still open. Waiting for in-flight
  backend I/O stays where it is today, inside `store.close()`. I/O completions and tasks that arrive after the close
  find the store gone and are dropped, as today's `IOCompletion` case does (`stores.get(...).foreach`). On a shared
  executor the close task concerns only its own store. The control thread calls `release` once the close completes.
- **Backend fail-stop (`BackendFailed`), `UnloadFailedStore`, FSIF open completion (`FsifOpened`)** are control-thread
  events. Any part of them that touches the `Store` is posted to its executor.

The exact ordering guarantees around close and fail-stop must hold what Host guarantees today (for example: a store is
never served without its CRL state; a fail-stop during close yields `StoreFailedOnClose`). The builder works out the
precise sequence from the current handlers.

### 6. Lockstep mode for the regression tests

`TestNetwork` constructs Host with a lockstep `HostThreading`. It keeps the behaviour the integration suites see today.
The rule is simple: **every place that does `events.put` today still enqueues exactly one item, at the same moment,
on one FIFO queue, and every Future callback that runs on `ec` today still runs on `ec`.**
- **One FIFO queue** serves every store executor and every control event (today's non-store `Event` cases: `LoadStore`,
  `ShutdownStore`, `BackendFailed`, `FsifOpened`, `ScanDevices`, `CheckAllDevices`, `HostMsg`, timers and so on).
- **Hot-path messages** each enqueue one item, as today, including messages for unknown stores. The router lookup, and
  the unknown or offline reply, happen when the item is drained, as `handleEvent` does today. In production the same
  lookup runs on the calling thread. The lockstep strategy supplies this "dispatch hop"; production supplies none.
- **The heartbeat** enqueues one item for the whole host, as today, and fans out to stores when that item is drained.
- **The queue is drained only by `TestNetwork.handleEvents()`** on the test thread, through a test hook equivalent to
  today's `testingOnlyHandleEvents()`.
- **The control context is the host's supplied `ec`** (the suite's serial `executionContext`), exactly where Future
  callbacks run today. This includes the callbacks of Promises that the store side completes. That `ec` is
  single-threaded and runs on the test thread, so the control code's single-owner rule still holds.
- **The `HostStoreView` snapshot** is republished synchronously wherever today's `stores` and `offlineStores` maps are
  updated, so a test that loads a store and then calls `containsStore` sees it exactly as now.

Tests that run a real `Host` inside `TestNetwork` (for example `FsifIntegrationSuite`'s `FsifTestHost`) also use
lockstep. In production, `HostStoreView` may lag the control thread slightly. That is acceptable: every operation
routed by it (`repair`, `repairDelete`) is rechecked against the router on the store's executor and fails with
`StoreNotHosted` if the store has left, as it does today when a store leaves between selection and handling.

**Acceptance bar:** every existing test outside the Host suites being rewritten passes unchanged with lockstep mode.
Any change to an integration test's expected ordering is a defect in lockstep mode, not a test to adjust.

### 7. Splitting `Host`

You decided: the Host suites (`HostFsifLoadSuite`, `HostDeviceDiscoverySuite`, `HostCrlSaveLoadSuite`,
`HostManagerSuite`, `HostEndpointSuite`, `HostTestHarness`, `HostRepairTargetSuite` and others that reach in through
`testingOnly*` hooks) are rewritten against the new classes rather than kept working through `Host`'s current test
surface. Hooks that only existed to work around the shared queue (for example, `testingOnlyCheckAllDevices` bypassing
the queue so that `InitializeTaskExecutor` is not drained) should become unnecessary once each concern is a class that
can be constructed and driven on its own.

**Coverage is preserved** (my call). Every behaviour a current Host suite asserts must still be asserted after the
rewrite. A test may be dropped only when it checks a test hook rather than host behaviour, and each one dropped is
named in its commit message.

Proposed split. The builder may adjust boundaries, but each concern below must end up out of `Host`:

| Class (proposed name) | Owns | Thread |
|---|---|---|
| `Host` | construction and wiring, public entry points (`receive*`, `repair*`, `loadStore`, `shutdown`), lifecycle | entry points run on the caller's thread and post work on |
| `HostThreading` + default + lockstep impls | store executors and the control context | – |
| `StoreRouter` | the loaded-store map, the offline set, hot-path dispatch, heartbeat fan-out | read on any thread, written by control |
| `StoreLoader` | opening backends (including FSIF on `FsifOpenEc`), CRL save-file validation, install/close/unload protocols, out-of-space waits (`storesAwaitingSpace`, `fsifOpening`) | control |
| `DeviceDiscovery` | the device-dir scan, `owning-host`/`shutdown.state` markers, load/adopt decisions, `loadedDeviceDirs`, `reportedDeviceDirs`, `pendingMarkerResolutions` | control |
| `DeviceReconciler` | `checkStorageDevice` and its deferral/recheck bookkeeping, store creation for Initializing entries | control |
| `StoreTransfers` | transfers in and out, pending starts and retransmits, retries, `recordTransferIn`, `updateStateForTransferredStore` | control |
| `StoreRebuilds` | rebuild slots, `rebuild-required`, retiring unserved stores, `markRebuiltStoreActive` | control |
| `DeviceShutdown` (with adoption) | `shutDownDevice`, `stopDeviceWork`, `completeDeviceShutdown`, `adoptDevice` | control |
| `HostServices` | task executor/service executor bootstrap, system task runner and its heartbeat, usage updates, gossip retransmit, endpoint reconcile | control |
| `HostStoreView` | immutable snapshot of served, repairable and offline stores and device paths, published by control via `@volatile`, backing `RepairTarget`, `containsStore`, `getStoreIds`, `getDevicePath` | written by control, read by anyone |

These classes share a lot of state today (for example, the reconcile consults transfers and rebuilds in flight, and
shutdown stops all of them). *Builder's call:* whether they share one control-thread-owned state holder or hold
references to one another, as long as all of it is touched only on the control thread.

The `Host`'s outcome enums (`TransferOutcome`, `RebuildOutcome`, `ShutdownOutcome`, `AdoptOutcome`,
`AdoptionClass`) move with the class that produces them.

### 8. Store, Frontend and Tx

Their existing `synchronized` blocks (in `Store`, and `Store.heartbeat`'s around `checkCollisionWait`) become
redundant once a store runs only on its own executor. Remove them when this is confirmed for each one; keep any that
guard state also touched by another thread (for example, by a `TransactionDriver` timer), and say why in a comment.
`Store` keeps its `executionContext` parameter for `TransactionDriver`s, which keep their own threading.

## Suggested order of work

My call. Each step leaves the full test suite green.

1. **Extract classes with no threading change.** Everything still runs on the existing event thread and lock. Rewrite
   the Host suites against the new classes as each one is extracted.
2. **Introduce the control context** and move device management onto it, removing its `synchronized`. Add the
   documentation from section 2.
3. **Introduce `HostThreading`, `StoreRouter` and store executors,** with the lockstep implementation first, so the
   regression tests stay green, then the default strategy.
4. **Remove now-redundant store-side locks** (section 8), update `CLAUDE.md`, and add the `HostObjectCache` follow-up
   to `Todo.txt`.
5. **Measure.** A before/after comparison of BackendTiming's "store commit (FsifBackend)" figure in the vdisk fio
   runs. Those need root and the TCMU setup, so the builder writes down the procedure and you run it. *My call.*
   There is no numeric target. The work ships either way, because the single-thread ceiling must go regardless. If the
   figure does not improve, record what the new thread dumps and timing show in `Todo.txt` as the next investigation.

## New tests the builder should add

- A store-threading test with real threads: many stores across several executors under concurrent traffic. It checks
  that each store's tasks never overlap and run in posting order, for example with an assertion inside the executor
  that detects concurrent entry. It must be deterministic in outcome (no timing-based assertions) and fast.
- Default-strategy tests: lazy thread creation, least-loaded placement, and slot reuse after `release`.
- Close-versus-traffic tests: messages racing a close get today's offline handling, never a closed store.

## Left to the builder

- Exact class names, package layout (suggested: `server.host` for the new classes) and signatures.
- Whether the device-management classes share a state holder (section 7).
- The precise install/close/fail-stop sequences, which must preserve the guarantees listed in section 5 and `CLAUDE.md`.
- The mechanics of stopping the executors in `shutdown()` (section 3 fixes what is expected).
- Whether `BootstrapHttpServer`, `HostManager` and the network bridge need anything beyond the existing public entry
  points.
