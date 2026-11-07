# Per-device CRLs, Ordered Commits and Backpressure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the host-level CRL with one CRL per storage device stored on it, persist each object's commits in
order, and drop new prepares on a device whose CRL is filling.

**Architecture:** Each `LocalStorageDeviceState` owns a `CrashRecoveryLog` (a `SimpleCRL` in `<device>/crl`) and a
`CrlAdmission` budget, opened when the host takes the device and shut down when it unloads it; stores get them from
their device. The `Frontend` tracks per-object in-flight transaction commits: a transaction touching such an object
withholds its vote (and a resolved one holds its commit) until the backend commit completes. Lock-only objects are no
longer committed.

**Tech Stack:** Scala 3, sbt, ScalaTest, os-lib.

**Spec:** `docs/superpowers/specs/2026-11-07-per-device-crl-and-commit-ordering-design.md`

## Global Constraints

- Commit directly on `main` (memory: commit-to-main). Commit messages end with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Scala 3 quiet syntax, indented if/then/else; os-lib for filesystem work.
- Host threading rules (`HostThreading.scala` scaladoc): never block the control thread on a Future; store state only
  on its executor; callbacks name the control context.
- Backward compatibility with persisted data is not required (CLAUDE.local.md).
- No flaky or timing-dependent tests; every test as fast as possible. Full runs: `timeout 70` is enough to detect hangs.
- CRL defaults stay 3 streams x 300 MB, now per device.
- No timeout on a wait for an in-flight commit (spec §3: a hung device's waiters wait as long as it hangs).
- No transaction class is exempt from backpressure (spec §4).
- Rare failure paths stay simple (memory: simplicity-over-corner-case-optimization).

## Review Focus

1. A transaction resolved as committed on a store that withheld its vote, while an earlier commit to the same object
   is still in flight: its commit must be held until the earlier one persists (Task 5 test "a resolved commit waits for
   the in-flight commit it follows").
2. Two concurrent `SharedRevisionLock` holders on a fence while neither changes it: both vote commit with no wait
   (Task 4 test "shared lockers of an unchanged fence never wait on each other").
3. The same device path unloaded and re-taken on one host before its old CRL finished shutting down: the new CRL must
   not open over files the old one still writes (Task 1 test "a device is not reloaded while its CRL is closing").
4. A prepare retransmission for a transaction the store admitted before the mark was passed: still answered (Task 6
   test "a known transaction's retransmitted prepare is answered past the mark").
5. A device whose `crl` directory is missing but which holds store directories: treated as a lost CRL, not as a new
   device (Task 2 test "a device with stores but no crl dir marks them rebuild-required").

---

### Task 1: One CRL per device

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/CrashRecoveryLogFactory.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/CrashRecoveryLog.scala` (trait: add `shutdown`, `admissionLimitBytes`; object: add `deviceCrlDir`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/SimpleCRL.scala` (`Factory` → `DeviceFactory`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StorageDeviceConfig.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/HostConfig.scala`, `server/HostManager.scala:23-24,131`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/Host.scala:142,168,194`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/DeviceControl.scala:47`, `host/LocalDevices.scala`,
  `host/DeviceDiscovery.scala` (`loadOwnedDevice`, `tryLoadDevice`), `host/DeviceShutdown.scala` (`unloadDevice`),
  `host/StoreLoader.scala:312,360,391,453`
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:1637,1663,1698-1711,1783,1855`
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala:42-61` and every suite building a Host or factory
  (`ControlFixture`, `HostDeviceDiscoverySuite`, `HostFsifLoadSuite`, `FsifIntegrationSuite`, `HostCrlSaveLoadSuite`,
  `StoreRouterSuite`, `HostManagerSuite:96`, `DeviceCheckNudgeSuite:57`, `HostConfigSuite`)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/DeviceCrlSuite.scala` (new)

**Interfaces:**
- Produces:
  - `trait CrashRecoveryLogFactory { def openCRL(devicePath: os.Path, config: StorageDeviceConfig): CrashRecoveryLog }`
    replacing `createCRL()`. Opening recovers the device's existing files; it throws if they cannot be recovered.
  - `CrashRecoveryLog.deviceCrlDir(devicePath: os.Path): os.Path = devicePath / "crl"`
  - On the trait: `def shutdown(): Unit = ()` (blocking; SimpleCRL already has it) and
    `def admissionLimitBytes: Long = Long.MaxValue` (SimpleCRL overrides it in Task 6).
  - `SimpleCRL.DeviceFactory(onThreadDeath: Throwable => Unit = _ => ()) extends CrashRecoveryLogFactory`: creates
    `deviceCrlDir` if absent and opens `SimpleCRL(deviceCrlDir(p), config.crlNumStreams, config.crlFileSizeMb * 1024L * 1024L, onThreadDeath).crl`.
  - `StorageDeviceConfig(storageDeviceId, aspenSystemId, crlNumStreams: Int = 3, crlFileSizeMb: Int = 300)`, yaml keys
    `crl-num-streams`, `crl-max-file-size-mb` (Optional, defaults above).
  - `LocalStorageDeviceState(storageDeviceId, devicePath, configFile, val crl: CrashRecoveryLog)` (Task 6 adds
    `admission`).
  - `TestNetwork.TestCRL` becomes a factory returning a fresh no-op CRL per device.

- [ ] **Step 1: Write the failing tests** in `DeviceCrlSuite` (a Host built like `HostDeviceDiscoverySuite.newRealHost`,
  but with `SimpleCRL.DeviceFactory()` and device configs at `crlFileSizeMb = 1`):
  - "each device gets its own CRL in its directory": two devices → `os.exists(deviceCrlDir(d) / "0.crl")` for both,
    and the two `LocalStorageDeviceState.crl` instances are distinct.
  - "a restarted host recovers a store's in-flight transaction from its device's CRL": prepare a transaction to a
    store (vote saved), stop the host without closing (crash-only; shut the CRLs down), start a new Host on the same
    dirs → `store.frontend.transactions.contains(txid)` and the recovered Tx holds its locks.
  - "unloading a device shuts its CRL down": unload via shutdown → the CRL's `shutdown()` was called (wrap the factory
    to record calls).
  - "a device is not reloaded while its CRL is closing": make the recorded `shutdown()` block on a latch; trigger a
    reload of the same path → `openCRL` is not called again until the latch is released; afterwards the device loads.
- [ ] **Step 2: Run** `sbt 'testOnly *DeviceCrlSuite'` — expect compile failure (`openCRL` undefined).
- [ ] **Step 3: Implement.**
  - Remove the host `crl` field from `HostConfig` (field, yaml, `CRLBackend`, `SimpleCRL` YObject) and its defaults
    from `HostManager` and `Main` bootstrap; `DeviceControl` takes `crlFactory: CrashRecoveryLogFactory` instead of `crl`.
  - `loadOwnedDevice` opens the device's CRL (Task 2 handles a failed open) before offering any child store, and
    registers `LocalStorageDeviceState` with it. The blocking SimpleCRL recovery on the control thread is accepted
    here exactly as `getFullRecoveryState` already is in `handleLoad`.
  - `StoreLoader.handleLoad` and `awaitCrlLoad`, `closeUnloaded`, `handleClose` use the store's device CRL
    (`ctx.devices.devices(storageDeviceId).crl`).
  - `unloadDevice` runs `crl.shutdown()` off the control thread (a dedicated daemon single-thread ec, like
    `FsifOpenEc`) and records the path in `closingCrls: Map[os.Path, Future[Unit]]` on `LocalDevices`; the completion
    removes it on the control context. `tryLoadDevice` returns `None` for a path in `closingCrls` (the 10 s scan
    retries).
  - `Main.startHost`: drop `onCrlThreadDeath` halting; pass `SimpleCRL.DeviceFactory(t => logger.error(...))` so a dead
    CRL thread stalls only its device. The recording wrapper keeps every CRL it opened; `stop()` shuts each down.
- [ ] **Step 4: Run** `sbt 'testOnly *DeviceCrlSuite *HostDeviceDiscoverySuite *HostCrlSaveLoadSuite *StoreRouterSuite *HostConfigSuite *HostManagerSuite'` — PASS.
- [ ] **Step 5: Full run** `timeout 70 sbt test` — PASS.
- [ ] **Step 6: Update docs**: CLAUDE.md "Crash Recovery Log" mentions (per device, `<device>/crl`, sizing in the device
  config, a dead CRL thread stalls one device), `server/crl/CLAUDE.md` (replace the halt sentence).
- [ ] **Step 7: Commit** `crl: one CRL per storage device, stored in <device>/crl`.

### Task 2: A lost device CRL marks its stores rebuild-required

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/DeviceDiscovery.scala` (`loadOwnedDevice`)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/host/DeviceCrlSuite.scala`

**Interfaces:**
- Consumes: `CrashRecoveryLogFactory.openCRL`, `CrashRecoveryLog.deviceCrlDir` (Task 1); `StoreRebuilds.markRebuildRequired(storePath)`.

- [ ] **Step 1: Write the failing tests:**
  - "a device whose CRL cannot be recovered marks its stores rebuild-required": a factory whose `openCRL` throws on the
    first call → every store dir on the device gets `rebuild-required`, none is served, the old `crl` dir is now
    `<device>/crl.lost-<uuid>` (contents kept), and a fresh CRL is opened.
  - "a device with stores but no crl dir marks them rebuild-required": delete `<device>/crl` from a device with a store,
    reload → the store is `rebuild-required`.
  - "a new device with no stores just creates its CRL": no marker written, `crl/` created.
- [ ] **Step 2: Run** `sbt 'testOnly *DeviceCrlSuite -- -z "rebuild-required"'` — FAIL.
- [ ] **Step 3: Implement** in `loadOwnedDevice`: the CRL is lost when `deviceCrlDir` is absent while the device holds
  any store dir, or when `openCRL` throws. Then mark every store dir `rebuild-required`, `os.move` the crl dir (if
  present) to `crl.lost-<random uuid>` and sync the device dir with `DurableFiles`, and open a fresh CRL. The existing
  reconcile pass (`DeviceReconciler.scala:120-130`) turns the stores into rebuilds.
- [ ] **Step 4: Run** the step-2 command — PASS; then `timeout 70 sbt test` — PASS.
- [ ] **Step 5: Docs**: CLAUDE.md (planned physical moves paragraph: a lost device CRL makes every store on it
  rebuild-required).
- [ ] **Step 6: Commit** `crl: a lost device CRL marks the device's stores rebuild-required`.

### Task 3: Physical moves carry the device CRL

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/host/StoreLoader.scala:432-472` (`handleClose`), `Host.scala`
  (`ShutdownStore` event), `host/DeviceShutdown.scala:207-235` (`closeStoresForShutdown`, `storesToClose`),
  `host/DeviceDiscovery.scala:264-278,331-345` (`adoptionClass`, `offlineStoresIn`, `uncleanStores`),
  `host/StoreTransfers.scala:553-610` (transfer-out close)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/DeviceAdoptionSuite.scala`, `server/DeviceShutdownSuite.scala`,
  `server/host/DeviceCrlSuite.scala`

**Interfaces:**
- Produces: `ShutdownStore(storeId, extractCrl: Boolean, ...)`. `extractCrl = true` (network transfer out only) does
  today's `crl.closeStore` + `saveStoreState`; `false` (shutdown, `Host.closeStore`) leaves the store's entries live in
  its device CRL and writes no `crl_save.log`.
- `AdoptionClass` keeps `Clean | CrlLost | Unclean`; new `adoptionClass` rule: no dir → Unclean; `rebuild-required`
  → CrlLost; `transferred-in` → Clean; `transferring-out` → Clean only with a valid `crl_save.log` (network transfer
  in flight), else CrlLost; any other dir → Clean (its state is in the device CRL).

- [ ] **Step 1: Write the failing tests:**
  - DeviceCrlSuite "a shut-down device adopted by another host recovers its in-flight transaction": real
    `SimpleCRL.DeviceFactory`, a store with a voted-but-unresolved transaction, `shutdownStorageDevice`, move the device
    dir to a second host's devices dir → adopted, the store's Frontend holds the recovered Tx with its locks, and the
    store dir has neither `transferring-out` nor `crl_save.log` at any point after the shutdown.
  - DeviceShutdownSuite: replace assertions that a shut-down store has `transferring-out` + `crl_save.log` with
    assertions that it has neither.
  - DeviceAdoptionSuite: `writeClosedStore` writes a plain store dir; the 210-240 cases become "a store marked
    rebuild-required is adopted as Rebuilding" and "a store whose network transfer left `transferring-out` with no
    valid save file is adopted as Rebuilding".
  - "a cancelled shutdown reloads its stores": no markers to remove, stores served again.
- [ ] **Step 2: Run** `sbt 'testOnly *DeviceAdoptionSuite *DeviceShutdownSuite *DeviceCrlSuite'` — FAIL.
- [ ] **Step 3: Implement.** `closeStoresForShutdown` passes `extractCrl = false` and writes no marker; `storesToClose`
  becomes loaded stores not offline (a crash mid-shutdown reloads the device and closes them again). `StoreTransfers`
  passes `extractCrl = true`. `bringIntoService` still removes a stray `transferring-out` from Active stores with no
  transfer. `keepRebuiltStoreOffline` is unchanged (its marker + empty save file still classify Clean).
- [ ] **Step 4: Run** the step-2 command, then `timeout 70 sbt test` — PASS.
- [ ] **Step 5: Docs**: CLAUDE.md "Planned physical moves" (no per-store marker or save file; adoption recovers from
  the device CRL; `crl_save.log` is for network transfers only) and the `crl_save.log` paragraph.
- [ ] **Step 6: Commit** `host: physical device moves carry the device CRL; no per-store save files`.

### Task 4: Unchanged objects get no backend commit

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala:383-430` (`doCommit`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/RequirementsApplyer.scala` (add helper)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`

**Interfaces:**
- Produces: `RequirementsApplyer.changedObjects(requirements: List[TransactionRequirement]): Set[ObjectId]` — ids named
  by a `DataUpdate`, `KeyValueUpdate`, `RefcountUpdate` or `VersionBump`. `RevisionLock`/`SharedRevisionLock` alone
  leave an object unchanged. Allocated objects always carry an update. Task 5 uses this set.

- [ ] **Step 1: Write the failing tests** (FrontendSuite, `DeferredCommitBackend`):
  - "a lock-only object gets no backend commit": txd with a `DataUpdate` on A and a `SharedRevisionLock` on fence F,
    committed → `backend.heldCommits == 1` and `TxCommitted` arrives after `deliverOne()`.
  - "a transaction that changes nothing here still reports committed": only a `RevisionLock` on F → `TxCommitted`
    with no backend commit.
- [ ] **Step 2: Run** `sbt 'testOnly *FrontendSuite -- -z "backend commit"'` — FAIL (2 held commits).
- [ ] **Step 3: Implement**: `toCommit` filters to `changedObjects(requirements)`; the existing
  `pendingObjectCommits == 0` path covers the rest. Locks on unchanged objects are released as today.
- [ ] **Step 4: Run** the full `sbt 'testOnly *FrontendSuite *SharedRevisionLockIntegrationSuite'` — PASS.
- [ ] **Step 5: Commit** `tx: objects a transaction leaves unchanged get no backend commit`.

### Task 5: Commits to an object reach the backend in order

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala` (commit tracking ~66-96, `commit` 198,
  `backendOperationComplete` 352-381)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala` (`determineDisposition` 339,
  `answerPrepare` 529, `resolvedAndAllObjectsLoaded` 443, `checkCollisionWait` 229)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/transaction/RequirementError.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`

**Interfaces:**
- Consumes: `RequirementsApplyer.changedObjects` (Task 4).
- Produces (Frontend, store executor only):
  - `def commitInFlight(oid: ObjectId): Boolean` — a `forTransaction` commit of `oid` is outstanding (deletions
    included). Tracked in `txCommitsInFlight: mutable.HashMap[ObjectId, Int]`.
  - `def awaitCommitPersisted(oids: Set[ObjectId], waiter: Tx): Unit`; on each `forTransaction` commit completion the
    Frontend calls `waiter.commitPersisted(oid)` for waiters of that object once its count reaches 0.
  - `def commitFailed(oid: ObjectId): Boolean` — set when a `forTransaction` commit of `oid` fails, cleared only by
    the store reloading (the Frontend is rebuilt).
- `case class BackendCommitFailed() extends RequirementError` — purely local, like `StaleObjectState`.
- Tx: `def commitPersisted(oid: ObjectId): Unit`; field `awaitingCommits: Set[ObjectId]`.

Behaviour (spec §3):
- Before `RequirementsChecker.check` in `determineDisposition`: if any of the Tx's objects `commitFailed`, vote abort
  with `BackendCommitFailed`; else if any `commitInFlight`, set `awaitingCommits`, register, and withhold the vote. No
  locks are taken while waiting (`lock()` requires a decided `VoteCommit`). `answerPrepare` keeps a retransmitted
  prepare in `pendingCollisionPrepare` while `awaitingCommits` is non-empty. `checkCollisionWait` ignores this wait.
  When `awaitingCommits` empties, run `endCollisionWait()`'s path (re-decide, lock, send delayed/pending responses).
- In `resolvedAndAllObjectsLoaded(committed = true)`: if any `changedObjects` id `commitInFlight`, hold the commit
  (reuse the `commitHeld` mechanism with a second condition) and run `doCommit()` once all are persisted. A held
  commit whose object then fails commits nothing for that object and reports it in `objectCommitErrors`.
- Locks still release at `doCommit` as today: the in-flight marker, not the lock, orders followers. This departs from
  the spec's "locks held until persisted" wording with the same effect, and avoids KV inserts dropping key locks on
  apply (`RequirementsLocker.scala:125-128`).

- [ ] **Step 1: Write the failing tests** (FrontendSuite, `DeferredCommitBackend`):
  - "a transaction built on an in-flight commit waits for it to persist": tx1 overwrites A, resolved committed, commit
    held; tx2 `overwriteAfter(tx1)` prepared → no `TxPrepareResponse`; `deliverOne()` → tx2 votes commit.
  - "a transaction not built on the in-flight commit waits rather than colliding": tx2 requires A's pre-tx1 revision →
    no response while held; after `deliverOne()` it votes abort with `RevisionMismatch` (not `TransactionCollision`).
  - "a wait on an in-flight commit has no timer": while held, call `tx2.checkCollisionWait(start + 10 * Tx.CollisionWait.toNanos)` → still no response.
  - "a resolved commit waits for the in-flight commit it follows": tx2 waiting, then `resolve(frontend, net, tx2,
    committed = true)` → `backend.heldCommits == 1` (only tx1's); after `deliverOne()` tx2's commit is issued and the
    backend receives tx1's state before tx2's (record commit order in `DeferredCommitBackend`).
  - "chained commits persist in commit order": three transactions on one object with ranged writes, commits delivered
    one at a time → backend's final state equals applying all three in order.
  - "waiters on a failed backend commit vote abort": tx1's commit `deliverOne(Right(CommitError.IOFailure))` → tx2
    votes abort; a later tx3 on A also votes abort.
  - Rewrite existing conflicting tests: 1563 "locks are released at finalization while a commit is outstanding" (tx2
    now waits for persistence) and 920 "a failed backend commit does not drop another transaction's shared lock" (a
    tx on the failed object now votes abort).
- [ ] **Step 2: Run** `sbt 'testOnly *FrontendSuite -- -z "in-flight commit"'` — FAIL.
- [ ] **Step 3: Implement** per the behaviour above.
- [ ] **Step 4: Run** `sbt 'testOnly *FrontendSuite *SharedRevisionLockIntegrationSuite *AbortVoteReportSuite'`, then
  `timeout 70 sbt test`; rerun the full suite 5 times under load to check for new flakes — PASS each time.
- [ ] **Step 5: Docs**: CLAUDE.md "Transaction Design" (one in-flight transaction commit per object; followers wait
  with no timer; a resolved follower's commit is held; failed commit → aborts until reload; lock-only objects are not
  committed). Remove the top Todo.txt item that this resolves ("Potential problem with rapid transactions").
- [ ] **Step 6: Commit** `store: one in-flight transaction commit per object; followers wait for persistence`.

### Task 6: Drop new prepares when a device's CRL is filling

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/crl/CrlAdmission.scala`
- Modify: `server/crl/simple/SimpleCRL.scala` (`admissionLimitBytes`), `server/host/LocalDevices.scala`,
  `host/DeviceDiscovery.scala` (create with the CRL), `host/StoreLoader.scala:312` (pass to Store),
  `server/store/Store.scala:20-36,96`, `server/store/Frontend.scala` (`receivePrepare` 317, CRL deletes 228/238,
  Tx forgetting), `server/transaction/Tx.scala:451`
- Test: `src/test/scala/org/aspen_ddp/aspen/server/crl/CrlAdmissionSuite.scala` (new), `server/store/FrontendSuite.scala`,
  `server/host/DeviceCrlSuite.scala`

**Interfaces:**
- Consumes: `CrashRecoveryLog.admissionLimitBytes` (Task 1).
- Produces:
  - `final class CrlAdmission(limitBytes: Long)` — thread-safe (`AtomicLong`), shared by a device's stores:
    `def tryAdmit(bytes: Long): Boolean` (admits while the current total is below `limitBytes`, so one transaction may
    cross it), `def release(bytes: Long): Unit`, `def admittedBytes: Long`. `CrlAdmission.Unlimited` for tests.
  - `SimpleCRL.admissionLimitBytes = maxSizeInBytes / 2`: when the ring recycles a stream, everything live is at most
    half a stream, so the copy-forward always fits. `Stream.writeEntry`'s `require` stays as the backstop.
  - `LocalStorageDeviceState(..., crl, val admission: CrlAdmission)`.
  - `Store(..., admission: CrlAdmission = CrlAdmission.Unlimited)` → `Frontend(..., admission)`.
  - `Frontend.admitPrepare(m: TxPrepare): Boolean` — true for a known transaction or one in `statusCache`; otherwise
    `tryAdmit(m.txd.serialize().length + m.objectUpdates.map(_.data.size).sum)` and, if admitted, records the bytes in
    `admitted: mutable.HashMap[TransactionId, Long]`.
  - `Frontend.releaseAdmission(txid: TransactionId): Unit` — idempotent; called wherever the Tx's CRL entry is deleted
    (`Frontend` 228/238, `Tx` 451 via the frontend) and wherever the Frontend forgets a Tx that never reached the CRL,
    and for all remaining entries when the store closes.

- [ ] **Step 1: Write the failing tests:**
  - CrlAdmissionSuite: "admits below the limit and refuses at it", "release makes room", "concurrent admits from many
    threads never exceed limit + one transaction" (deterministic: assert the bound, not timing).
  - FrontendSuite "a new prepare is dropped past the mark": `CrlAdmission(limit)` pre-filled to the limit → a new
    client prepare produces no Tx and no response.
  - FrontendSuite "a known transaction's retransmitted prepare is answered past the mark": admit tx1, fill the budget,
    retransmit tx1's prepare → response sent; `TxResolved`/`TxFinalized` for tx1 handled.
  - FrontendSuite "a finalized transaction releases its admission": `admittedBytes` returns to 0 after finalization
    (commit and abort cases).
  - FrontendSuite "a transaction waiting on an in-flight commit keeps its admission": admitted bytes stay counted while
    it waits.
  - DeviceCrlSuite "a full device slows only its own stores": two devices on one host, device A's admission filled →
    prepares to A's store dropped, prepares to B's store answered.
- [ ] **Step 2: Run** `sbt 'testOnly *CrlAdmissionSuite *FrontendSuite *DeviceCrlSuite -- -z "mark"'` — FAIL.
- [ ] **Step 3: Implement.** `Store.receiveTransactionMessage` calls `frontend.admitPrepare(m)` first for every
  `TxPrepare` and returns early (no driver, no status reply) when it is refused. Prepares held during recovery are
  admitted before being held.
- [ ] **Step 4: Run** the step-2 command, then `timeout 70 sbt test` — PASS.
- [ ] **Step 5: Docs**: CLAUDE.md "Transaction Design" (backpressure: a device past half a CRL stream of admitted
  bytes drops prepares for unseen transactions; nothing is exempt).
- [ ] **Step 6: Commit** `store: drop prepares for unseen transactions while the device CRL is filling`.
