# Per-device CRLs, ordered commits, and backpressure

Date: 2026-11-07. Addresses the top item of Todo.txt (rapid transactions on a slow backend), which grew into a
refactor of the Crash Recovery Log.

## Why

A store releases an object's requirement locks in `Tx.doCommit`, as soon as the committed update is applied in memory.
The backend commit follows asynchronously, and `Frontend` tracks any number of in-flight commits per object
(`inFlight`, `committing`). The transaction's CRL entry, object data included, stays live until its backend commits
succeed (`Frontend.TxCommits`). Two problems follow.

1. **Commit order is not durable.** Transactions A, B, C may all commit on one object while A's backend commit is still
   in flight. Their updates must reach the backend in commit order or the object is corrupted (overlapping byte-range
   updates are the clear case). Recovery cannot restore that order: if the store crashes after the status caches have
   forgotten A, B and C, each recovered transaction is re-resolved on its own and nothing records their sequence.
2. **A slow backend can halt the host.** The CRL is one ring of files per host (`HostConfig.SimpleCRL`, default
   3 x 300 MB) shared by every store. Unpersisted commits from a store whose backend is slower than its write load
   accumulate without bound; when live entries outgrow the ring, `require(canWriteEntry)` in `Stream.writeEntry` fails,
   the CRL thread dies and the host halts. One slow device takes down every store on the host.

Hosts are expected to mix media: NVMe and spinning disks. HDD-backed stores will always trail NVMe-backed ones. Any
remedy must slow only the stores whose own device is behind; writes to NVMe stores must stay fast while HDD stores slow
down. A faulty device running slower than normal should not bog down the rest of the host either.

## Goals

- Updates to an object reach its backend in commit order, so recovery never needs to reorder them.
- A device whose backend falls behind slows only its own stores' writers.
- The CRL can never overflow.
- A device's recovery state travels with it on a physical move.

## Non-goals

- Adopting a device without a clean shutdown (drive pulled from a running or crashed host). Made possible by this work,
  recorded at the top of Todo.txt, not built here.
- Placing a device's CRL somewhere other than on that device (for example HDD CRLs on NVMe). Also in that Todo note.
  This design must not preclude it: the CRL location should be a per-device path, defaulting to the device directory.
- The Todo.txt "potential hole in transaction recovery" item.
- Isolating each device's I/O threads, and detecting and handling hung devices (Backlog.txt). Shared I/O pools
  (FSIF, shared RocksDB engines) stay as they are here.
- Any change to network store transfers beyond what the CRL split forces: they keep their per-store save file.

## Anti-goals

- A fixed per-store allocation of CRL space. Store activity varies widely; a fixed share would throttle a busy store
  while idle stores sit on unused space.
- A host-wide trigger that throttles every store when any one device is behind.
- Errors returned to clients for backpressure. Writers slow down; they do not see failures.
- An in-flight commit on an object showing up as a requirement conflict for the next transaction on it.

## Design

### 1. One CRL per storage device, stored on that device

The host-level CRL goes away entirely. Each storage device has its own `SimpleCRL` (the existing three-file
implementation, unchanged in kind) in a directory on the device, by default `<device-dir>/crl`. Every store on the
device saves to, recovers from and deletes from its device's CRL. A device's CRL thread dying stops that device's
transactions, not the host's.

- **Opening.** The device's CRL is opened when the host takes the device (startup scan or adoption), before any of its
  stores load. Its recovered state replaces what `crl_save.log` supplied at store load for device moves and restarts.
- **Sizing.** `num-streams` / `max-file-size-mb` move from host config to a per-device setting, keeping the current
  defaults (3 x 300 MB). Builder's call where the setting lives (the device config yaml, with host config supplying
  defaults, is the obvious place).
- **Store close.** A store that is closed but stays on its device (device shutdown, offline) leaves its entries in the
  device CRL: they remain live and are copied forward like any other. Only a store leaving the device by network
  transfer extracts its state into `crl_save.log` for the staged copy, as today. A store discarded for rebuild has its
  entries deleted.
- **Lost CRL.** If a device's CRL is missing, unreadable or corrupt, every store on that device is marked
  `rebuild-required`, the same rule that applies today to a store whose `crl_save.log` fails to load.
- **Tests.** `TestNetwork.TestCRL` and the `CrashRecoveryLogFactory` seam become per-device; how is the builder's call.

### 2. Physical moves carry the CRL

Planned moves keep `shutdownStorageDevice`, `shutdown.state` (the HostId) and `owning-host`. What changes:

- A device shutdown no longer writes `transferring-out` or `crl_save.log` per store. Closing the stores and leaving the
  device CRL's state durable is enough: the CRL is already on the drive.
- Adoption no longer requires every Offline store dir to have `transferring-out` + `crl_save.log`. The adopting host
  opens the device's CRL and recovers each store from it. A store with no state in the CRL simply has no in-progress
  transactions.
- `rebuild-required` directories are still adopted as Rebuilding whatever their other markers.
- `crl_save.log`, `transferring-out` and `transferred-in` remain for network transfers only.

### 3. Commits to an object are persisted in order

A store allows at most one transaction per object in the commit phase: an object accepts its next transaction only
once the previous commit to it has been persisted by the backend.

- **Locks held until persisted.** A committing transaction keeps its requirement locks on an object until that object's
  backend commit completes, not merely until the update is applied in memory.
- **Waiters, not conflicts.** Any transaction whose prepare touches an object with a commit in flight waits on the store
  with its vote withheld, whether or not it builds on the in-flight transaction. When the commit completes, the waiting
  transactions have their requirements checked normally and vote. This generalises the existing collision wait
  (`Tx.CollisionWait`, `Frontend.awaitLocksReleased`), but a wait on an in-flight commit has no timer: a backend commit
  always completes or fails.
- **Every kind of transaction waits.** The rule applies to any transaction touching the object: writes, deletions,
  refcount changes, and requirement-only transactions such as a `SharedRevisionLock` on a vdisk fence. Reads are
  unaffected and keep seeing the committed-but-unpersisted state from memory, as today.
- **No partial locking while waiting.** A waiting transaction takes no locks on any of its objects until none of them
  has a commit in flight, so it never stalls writers on its other objects. (Builder-check call, not discussed.)
- **Failed backend commit.** Transactions waiting on an object whose backend commit failed get abort votes, and so does
  every later transaction on that object until the store reloads. The object stays pinned and served as today. No new
  recovery trigger: this is the simple path for a rare failure, not an optimised recovery. (Builder-check call.)
- **Hung backend.** A backend commit that neither completes nor fails leaves its waiters waiting for as long as the
  device hangs; there is no timeout. Once the device's CRL reaches the mark, new prepares are dropped. Isolating hung
  devices and detecting hangs is a separate, larger problem recorded in Backlog.txt.
- **Consequence.** At most one committed-but-unpersisted transaction per object exists in any CRL, so recovery never has
  to order them, and the hot single-writer chain (a vdisk block map, say) cannot grow beyond one in-flight commit.
  On NVMe this costs a single writer one backend-commit latency per update; dropping the waiter instead would cost a
  client retransmit interval, which is why waiters are held.

### 4. Backpressure: drop prepares when a device's CRL is filling

When a device's CRL passes a high-water mark of its capacity, that device's stores drop prepares for transactions they
have not yet seen. Nothing else is dropped:

- Prepares (including retransmits) for transactions the store already knows, accepts, commits, resolutions,
  finalizations, heartbeats and status requests are always handled. Dropping them would free nothing and could strand
  transactions.
- Transactions held in memory waiting on an in-flight commit (section 3) count toward the mark, so waiting cannot grow
  without bound.
- If enough other stores in the pool keep up, the transaction commits without the dropping store, which is then marked
  in its pool's ErrorTree and repaired later, as for any missed update. If the whole pool is behind, the client's
  retransmits stall, and that stall is the slowdown.
- No transaction is exempt. Missed-update markers, ErrorTree writes, DurableTask steps, FSIF allocation releases, and
  metadata- and bootstrap-pool stores slow down like everything else (decided).
- The mark is a simple fraction of the CRL's usable capacity, chosen with headroom for what is already admitted
  (in-progress votes, unpersisted commits, held waiters, copy-forward space). It is not derived from a worst-case
  bound, which would need per-transaction size caps or reservations. `Stream.writeEntry`'s `require` stays as the
  fail-stop backstop, now stopping one device rather than the host. (Builder-check call.)

Because each device has its own CRL, the mark is per device with no allocation among its stores: busy stores use what
idle ones do not, and an HDD device filling its CRL never affects an NVMe device beside it.

## Left to the builder

- The exact high-water mark and how "full" is measured (live bytes against usable ring capacity, accounting for
  copy-forward headroom), and how held waiters' bytes are counted.
- How the per-device CRL is wired through `DeviceControl`, `StoreLoader`, `DeviceShutdown`, adoption and
  `StoreTransfers`, and the threading of the CRL relative to `HostThreading`'s rules.
- How a withheld vote on an in-flight commit is represented in `Tx` and `Frontend`, and how it coexists with the
  existing collision wait.
- Test coverage. At minimum: ordered persistence of chained commits on a slow backend; a waiter that does not build on
  the in-flight transaction waits rather than aborts; abort votes after a failed backend commit; prepares dropped past
  the mark while already-known transactions proceed; an NVMe-like device unaffected by a slow device on the same host;
  device shutdown and adoption recovering in-progress transactions from the device's CRL; a lost device CRL marking
  its stores rebuild-required. Per CLAUDE.local.md, none may be timing-dependent or flaky.
- Updating CLAUDE.md (Crash Recovery Log, planned physical moves, transaction design) and `server/crl/CLAUDE.md`.
