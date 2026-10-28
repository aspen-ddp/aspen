# Design: Physically moving a storage device between hosts

**Date:** 2026-10-28
**Status:** Draft

## Intent

An operator needs to pull a disk out of one host and plug it into another, possibly days later, without
rebuilding or transferring any of its stores. Aspen has no notion of a device leaving service on purpose, so
today the only options are to transfer every store off the device (slow, and needs spare capacity) or to declare
it failed (and rebuild everything).

The procedure this design enables:

1. `aspen shutdown-device <device>`. The owning host closes every store on the device, preserving each store's
   CRL state, and marks the device `Shutdown`. `show-device` shows the progress.
2. Once the device reads `Shutdown`, the disk may be pulled. It may stay unplugged indefinitely.
3. The disk is plugged into another host. Within ~10 seconds that host notices it, claims ownership in a single
   transaction, loads its stores and announces them.

Alternatively, at step 2 the operator can run `aspen restart-device <device>` to bring the device back into
service on its original host.

The guiding principle: **a device coming up on its new host should look exactly like the original host crashing
and restarting.** Rebuilds and transfers interrupted by the shutdown resume through their existing crash-recovery
paths. No new resumption logic.

## Goals

- Planned, crash-safe removal of a device from its host, and its adoption by another host.
- Cancelling a shutdown (or bringing a `Shutdown` device back) on the original host.
- Hosts discover newly present device directories within ~10 seconds without restarting, cheaply enough to run
  that often.
- An explicit device status, so code and operators can read "failed" or "shut down" off a field rather than
  inferring it.

## Non-goals

- Moving a device while it is live. Stores are offline from shutdown until adoption.
- Any data movement over the network. The disk itself is the transport.
- Automatic retry of a lost nudge. If `shutdown-device` or `restart-device` appears stuck, the operator re-runs
  it; both are idempotent and resend the nudge. The hourly device check is the backstop.
- Backward compatibility with persisted data (not required in this project).

## Anti-goals

These would make it a failure even if it technically works:

- **A half-built store served as complete.** A `Rebuilding` store must never be flipped to `Active` or
  `Offline` by this feature.
- **A store loaded without its CRL state.** A device pulled before shutdown finished must not be adopted by
  another host.
- **Losing a transfer's identity.** `TransferringIn`/`TransferringOut` entries (and their `transferDevice` /
  `transferId`) must survive shutdown and adoption unchanged.
- **Two hosts believing they own the device.** Ownership changes only in the adoption transaction.

## Existing facts the design builds on

- `StorageDeviceState.StoreStatus` is `Initializing, Active, TransferringIn, TransferringOut, Rebuilding`.
- Device "failed" is currently derived: `isFailed` checks for the zeroed `FailedHostId` / `FailedStorageDeviceId`
  written by the tombstone in `FailedStorageDeviceDurableTask`.
- `Host.handleEvent(ShutdownStore)` (via `closeStore`) already removes a store from service, closes its CRL
  entry, saves its CRL state with `CrashRecoveryLog.saveStoreState(..., backend.crlSaveFile)`, and closes the
  backend. A transfer-out then writes the `transferring-out` marker (`StoreTransfer.TransferOutMarkerFile`) so
  the copy is not loaded on restart, and gossips `StoreOffline`.
- `Host.reinstateAbandonedTransferOut` already removes a `transferring-out` marker and loads the store when the
  tree says Active.
- `Host.checkForNewDevices` / `tryLoadDevice` already load devices at runtime. `CheckAllDevices`, which runs at
  construction and every `checkStorageDevicePeriod` (1 hour in `Main.scala`), calls it and then calls
  `checkStorageDevice` on every loaded device. That reads each device's `StorageDeviceState` over the network
  and reconciles.
- `HostState.storageDevices: Set[StorageDeviceId]` lists the devices a host owns. `Host.updateHostId` already
  claims a device for a host. It rewrites the device's `hostId` and each affected `StoragePoolState.stores(i).hostId`
  in one transaction, and is the natural base for the adoption transaction.
- **Existing bug to remove:** `reconcileDeviceState` (`Host.scala` ~1491) auto-claims any loaded device whose
  `hostId` names another host, and `tryLoadDevice` loads a device's stores before any ownership check. A device
  moved without a clean shutdown lacks each store's CRL state. Its stores cannot resume their transactions, and
  they may have missed object updates that the missed-transaction trees never recorded. Such a device must be
  refused (operator decision), not claimed. A dead host's disks are therefore recovered by declaring the device
  failed and rebuilding, not by moving them.
- `StoreMoved` gossip announces a store served by a (possibly new) host.

## Design

### Device status

Add a `status` field to `StorageDeviceState` (proto and codec):

| Status | Meaning |
|---|---|
| `Operational` | Normal service. The default for newly created devices. |
| `ShuttingDown` | Requested. The owning host is closing stores. Do not pull the disk. |
| `Shutdown` | All stores closed with CRL state saved. **Safe to physically remove.** The original host still owns it. |
| `Failed` | Declared dead (tombstoned). |

The tombstone transaction sets `Failed`, and `isFailed` reads the status. Whether the tombstone also keeps
zeroing the two ids is the builder's call (see "Left to the builder").

`show-device` displays the status.

### Store status

Add `StoreStatus.Offline`. **Only `Active` stores** change: `Active → Offline` at shutdown request,
`Offline → Active` at adoption or restart. `Initializing`, `Rebuilding`, `TransferringIn` and `TransferringOut`
entries are never touched by this feature.

### `AspenClient.shutdownStorageDevice(deviceId): Future[Unit]` and `shutdown-device`

- One transaction, guarded by the device's `StateKey` revision: every `Active` store → `Offline`, and status →
  `ShuttingDown`.
- After commit, send a best-effort `CheckStorageDevice` nudge to the owning host.
- Completes successfully once the status is `ShuttingDown`. If it is already `ShuttingDown` or `Shutdown`, it
  succeeds without writing, but **still resends the nudge**: that is how an operator recovers from a lost one.
- Fails if the device is `Failed`.
- **Pool write-threshold refusal.** On an `Operational` device the same transaction reads every pool with a store on
  the device and the state of every other device those pools use. If, for any such pool, fewer than the pool IDA's
  write threshold of its stores are on `Operational` devices other than this one, the request fails with
  `AspenClient.ShutdownWouldBreakPool(poolId)` (terminal; no `--force`) and writes nothing. A `ShuttingDown`,
  `Shutdown` or `Failed` device counts as unavailable. Without this, shutting down a device holding enough
  bootstrap-pool stores would make the metadata unreadable once its stores close, and the shutdown, a restart and any
  adoption could then never commit. The other `Operational` devices counted have their revisions locked by the
  transaction, so two concurrent shutdowns of devices sharing a pool cannot both pass. The check does not apply to a
  device already `ShuttingDown` or `Shutdown` (the idempotent no-write-but-nudge case).
- `shutdown-device` CLI command calls it, and prints a clear error for `ShutdownWouldBreakPool`.

### `AspenClient.restartStorageDevice(deviceId): Future[Unit]` and `restart-device`

- Valid for a `ShuttingDown` or `Shutdown` device on its owning host. One transaction: `Offline` stores →
  `Active`, status → `Operational`. Nudge the owning host after commit.
- Succeeds without writing (but still nudges) when the device is already `Operational`. Fails when `Failed`.
- `restart-device` CLI command calls it.

### Owning host: on seeing `ShuttingDown`

Driven from the existing device reconcile (`checkStorageDevice`), so a nudge or the hourly check triggers it:

1. Stop running work on the device. Cancel any active transfer-in/out sessions and stop any running
   `RebuildingStore`. Their device entries stay as they are. **Bounded wait:** before closing any store, the host
   waits for the rebuilds it cancelled to finish acting on their outcome (a cancelled walk stops reading at once and
   closes its staging backend), and for any transfer-in pool update already in flight for the device. The wait is
   bounded by the `Host` constructor parameter `shutdownWorkTimeout` (default 60 s). On timeout the shutdown proceeds
   and logs a warning naming the stores still running; they are left as a crash leaves them.
2. Close every loaded store using the transfer-out shutdown path: `closeStore` (saves CRL state), write the
   per-store "do not load" marker, and gossip `StoreOffline`. Stores that are not loaded (`Rebuilding`,
   `TransferringIn`, or `TransferringOut` already closed by its transfer) get no new handling. They are left
   exactly as a crash leaves them.
3. Write `shutdown.state` at the device root containing this host's `HostId`.
4. Only after 1–3 complete for every store, transact the device status to `Shutdown` (guarded on its revision).
   **Race guard (my call):** a rebuild or transfer-in that completes after the shutdown request can still flip an
   entry to `Active`. The `Shutdown` transaction therefore re-reads the device, and if any entry is `Active` it does
   not commit. Instead the host flips those entries to `Offline` (step-1 transaction semantics), closes them, and
   tries again. The device never reaches `Shutdown` with an `Active` entry.
5. Unload the device from this host: close everything, remove it from `storageDevices` and from the scan's
   skip set. It stays in `HostState.storageDevices`: this host still owns it.

The whole procedure must be safe to repeat. A crash at any point, followed by the next check, finishes the job.

Everything the "safe to remove" decision depends on is forced to disk before `Shutdown` is committed: each closed
store's marker and CRL save file, and `shutdown.state` (and `owning-host` whenever it is written). Each is written to a
temp file, forced, renamed into place, and its directory forced.

A rebuild or transfer in that finishes after its device has been adopted by another host writes nothing: only the
device's owner writes its entries and its pool entries, which always name the device's `hostId`.

While a device is `ShuttingDown` or `Shutdown`, its host ignores requests to start a transfer in or out of it,
and a host starting a transfer re-reads the owning `hostId` from `StorageDeviceState` on every attempt
(audit `startStoreTransferOut` and the transfer-in initiator). A pending transfer in re-reads its source device on
every retransmit, retry and device check, and re-addresses its `StartStoreTransfer` to the source's current host. Transfers whose peer is shut down simply keep
retrying until the device reappears.

### Owning host: `restart-device` before the disk moves

On seeing the device `Operational` with `shutdown.state` naming itself, the host deletes `shutdown.state`,
clears the per-store markers of stores now `Active`, and loads the device and stores as usual.

The device is unloaded at this point, so the restart nudge reaches it through the scan.
`CheckStorageDevice` for a device the host has not loaded already triggers `checkForNewDevices`. If the disk is
no longer physically present, nothing happens. `restart-device` only writes metadata, and the device loads
whenever it reappears on that host.

### Non-`Operational` devices elsewhere (my call)

A device that is `ShuttingDown` or `Shutdown` is never chosen as a destination: not for new pool stores,
rebuild targets, rebalancing, or pool-to-set migration. Otherwise that work would wait on a disk that may be on a
shelf for days. Declaring a `Shutdown` device failed stays allowed (the disk may turn out to be lost). Any
exhaustive `match` on `StoreStatus` or the new device status is audited for the new cases.

### Device scanning

- A new periodic scan every **10 seconds** runs only the local directory scan (`checkForNewDevices`). The hourly
  `CheckAllDevices` reconcile stays as is. It is the backstop for missed nudges, and running it every 10 seconds
  would mean every host reading every device's state over the network.
- `Host` remembers the directories it has **successfully loaded** and skips them without parsing their config.
  The common-case scan is then a single directory listing. An entry leaves the set when the device is unloaded
  (shutdown step 5) or its directory disappears, so a disk re-inserted on the same host is checked again.
- The scan runs on the host event loop (a new event or a reuse of the discovery call), under the same lock as
  today.
- **Decide before load.** The shutdown/ownership decision below is made before any store on a device is opened.
  Today's `tryLoadDevice` opens stores eagerly and must be split accordingly. A device owned by another host
  without a valid `shutdown.state` is never loaded and never claimed.
- Directories not in that set get the full check each scan: a valid `aspen-storage-device-config.yaml` for this
  `aspenSystemId`, then the shutdown/adoption logic below. Problems are logged **once per process per directory**
  (in memory only), then rechecked silently every scan.
- A device found by the scan gets an immediate `checkStorageDevice`, as `CheckAllDevices` does today.
- Update any docs that say a new device is not found until the host restarts.

### Loading a device with `shutdown.state`

| `shutdown.state` names | Device status | Action |
|---|---|---|
| this host | `Shutdown` | Ignore (do not load). |
| this host | `ShuttingDown` | Load, and the normal reconcile finishes the shutdown (a crash between writing `shutdown.state` and committing `Shutdown`). |
| this host | `Operational` | Restart path above. |
| another host | `Shutdown` | **Adopt** (below). |
| another host | anything else | Log once, ignore. |
| (no file), owner is another host | any | Pulled too early, or moved without shutdown: log once, ignore. Never claim. |
| (no file), owner is this host | `ShuttingDown` | Load, then the normal reconcile resumes the shutdown. |

### Adoption

1. Validate on disk that every store the device lists as `Offline` has its "do not load" marker and a CRL save
   file that parses and names that store (a store that transferred in during the shutdown has its transferred-in
   marker instead). If any fails, log once and ignore the device. The check is repeated inside the adoption
   transaction against the state it re-reads, and the post-commit steps act on the state committed. (It may go back into its original host, finish its shutdown,
   and return.) Entries in other states are not validated: they are whatever a crash would have left, and their
   existing recovery paths handle them.
2. One transaction, built from `updateHostId`, guarded on the device state revision, both `HostState` revisions
   and each affected pool's config revision, and retried on collision:
   - device: `Offline` stores → `Active`, status → `Operational`, `hostId` → this host
   - each affected `StoragePoolState`: the store entry's `hostId` → this host
   - old host's `HostState`: remove the device id. If that HostState cannot be found, skip this part and proceed,
     self-healing as the tombstone does (my call).
   - this host's `HostState`: add the device id
3. After commit: delete `shutdown.state`, remove the per-store markers of the now-`Active` stores, load the device
   and stores (CRL state restored as when a store finishes transferring in), and gossip `StoreMoved` for each
   loaded store.
4. The normal reconcile then resumes `Rebuilding` entries and the transfer machinery resumes
   `TransferringIn`/`TransferringOut` entries, exactly as after a crash.

A crash between steps 2 and 3 must be recoverable. On the next scan the device reads `Operational` and owned by
this host while `shutdown.state` names the old host, which must be treated as "adoption committed, finish step 3".

## Left to the builder

- **Per-store marker:** reuse `transferring-out` or add a dedicated one. The owner sees it either way. Reuse
  has the benefit that the remove-marker-and-load path already exists. A new marker is more explicit. Either way,
  make sure the existing transfer-out reconcile (e.g. `reinstateAbandonedTransferOut`, which fires for an Active
  entry with a marker) cannot misfire against an `Offline` store or an adopted one.
- Exact shape of `shutdown.state` (plain HostId UUID text is enough).
- How a `Rebuilding` store's partial data and checkpoint are left on disk. It must be whatever a crash leaves, so
  `RebuildingStore` resumes from it on the new host.
- Whether the tombstone keeps zeroing `hostId`/`storageDeviceId` in addition to setting `Failed`. The intent is
  that readers use the status (code clarity). Audit the ~6 uses of `FailedHostId` / `FailedStorageDeviceId`.
- Whether the 10-second period is configurable like `checkStorageDevicePeriod` (it should be, for tests).

## Testing

Follow the project priorities: correct, never flaky, fast. Prefer driving hosts through the existing testing
hooks (`testingOnlyCheckAllDevices`, `testingOnlyHandleHostMessage`) over waiting on timers. Cover at least:
shutdown with Active, Rebuilding and TransferringOut/In stores present; adoption on a second host, including
both `HostState` updates, pool `hostId` updates and resumed rebuild/transfer; restart on the original host,
including after unload; pulled-too-early and moved-without-shutdown rejection (and that the old auto-claim is
gone); a rebuild completing during shutdown;
crash between adoption commit and load; idempotent re-run of both commands; the scan skipping loaded
directories.
