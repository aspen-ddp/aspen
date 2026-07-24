# CLI `show` Commands — Design

**Date:** 2026-07-23
**Status:** Approved (pending spec review)

## Goal

Add five CLI commands to `org.aspen_ddp.aspen.cmdline.Main` that look up a single
system-metadata entity by name or UUID and print its full content in a
human-readable, multi-line form:

- `show-host`
- `show-device`
- `show-pool`
- `show-device-set`
- `show-allocation-group`

These complement the existing `list-*` commands, which show one summary line per
entity. The `show-*` commands display the complete state of one entity.

## Background / Existing Patterns

- Each CLI command is a `cmd(...)` block in the scopt `OptionParser` in `Main.scala`,
  sets a `mode` string on `Args`, and is dispatched in the `cfg.mode match` block to a
  `def` that: `configureLogging()` → `createAmoebaClient` → `network.startIoThread` →
  builds one `Future` → prints via `f.onComplete` → `Await.ready(f, 30.seconds)`.
- `list_devices` is the closest precedent: it resolves a name to an id, reads state,
  resolves related ids (device-set ids → names) with best-effort fallback to the raw
  UUID, and distinguishes "host not found" from a missing sub-reference via a private
  sentinel exception (`HostNotFound`).
- `formatBytes` is a `private[cmdline]` pure function unit-tested in `MainSuite`. This
  is the precedent for the formatter functions in this design.

### Client lookup methods available (`AspenClient`)

Name → id:
- `getHostId(name): Future[HostId]`
- `getStoragePoolId(name): Future[PoolId]`
- `getAllocationGroupId(name): Future[AllocationGroupId]`
- `getStorageDeviceSetId(name): Future[StorageDeviceSetId]`

Id → state:
- `getHostState(HostId): Future[HostState]`
- `getStorageDeviceState(StorageDeviceId): Future[StorageDeviceState]`
- `getStoragePoolState(PoolId): Future[StoragePoolState]`
- `getStorageDeviceSetState(StorageDeviceSetId): Future[StorageDeviceSetState]`
- `getAllocationGroupState(AllocationGroupId): Future[AllocationGroupState]`

**Gap:** there is no name → id lookup for storage devices, and `StorageDeviceState`
has no `name` field. Devices are UUID-only. Therefore `show-device` accepts a UUID only.

## Approach

Chosen: **pure formatter functions + thin CLI wrappers** (matches the
`formatBytes` / `MainSuite` precedent).

- Display logic lives in `private[cmdline]` pure functions (state → String), with no
  I/O and no futures, unit-tested directly in `MainSuite`.
- Each `show_*` wrapper handles client setup, argument resolution, the reads required
  for one-level name enrichment, error handling, and printing.

Rejected alternatives: inline formatting inside each wrapper (formatting not
unit-testable); `describe()` methods on the shared `common.metadata` state classes
(wrong layer — couples presentation into domain classes shared by client and server).

## Design

### 1. CLI surface & argument handling

Five new `cmd(...)` blocks in the scopt parser. Each takes the standard
`<bootstrap-config-file>` arg plus one entity arg.

| Command | Entity arg | Resolution |
|---|---|---|
| `show-host` | `<name-or-uuid>` | UUID → `HostId`; else `getHostId(name)` |
| `show-pool` | `<name-or-uuid>` | UUID → `PoolId`; else `getStoragePoolId(name)` |
| `show-device-set` | `<name-or-uuid>` | UUID → `StorageDeviceSetId`; else `getStorageDeviceSetId(name)` |
| `show-allocation-group` | `<name-or-uuid>` | UUID → `AllocationGroupId`; else `getAllocationGroupId(name)` |
| `show-device` | `<uuid>` | UUID only (devices have no name); validated as a UUID in the parser |

- Add one shared field to `Args`: `entityRef: String = ""`, reused by all five commands
  (they are mutually exclusive by `mode`).
- New `mode` values: `show-host`, `show-pool`, `show-device`, `show-device-set`,
  `show-allocation-group`, each dispatched in `cfg.mode match`.
- `show-device`'s parser arg validates the string as a UUID (fails fast with a clear
  message otherwise), consistent with existing UUID-arg validation (e.g. `transfer-store`).

**Auto-detect helper** (`private[cmdline]`):

```
def resolveRef[A](ref: String, byUuid: UUID => A, byName: String => Future[A]): Future[A]
```

Tries `UUID.fromString(ref)`; on success returns `Future.successful(byUuid(uuid))`,
otherwise returns `byName(ref)`. Used by all commands except `show-device`.

### 2. Formatter functions (testable core)

Five `private[cmdline]` pure functions in `Main.scala`. Each takes the already-fetched
state plus any pre-resolved related names (as `Option[String]`) and returns a `String`.
No I/O. Sizes are rendered with the existing `formatBytes`. A consistent indent /
label-alignment style is shared across all five.

```
formatHostState(s: HostState): String
formatDeviceState(s: StorageDeviceState, hostName: Option[String], setName: Option[String]): String
formatPoolState(s: StoragePoolState, setName: Option[String]): String
formatDeviceSetState(s: StorageDeviceSetState, parentName: Option[String]): String
formatAllocationGroupState(s: AllocationGroupState): String
```

When an `Option[String]` name is `None` (not resolved or resolution failed), the
formatter falls back to the raw UUID, matching `list_devices`'
`getOrElse(...uuid.toString)` behavior.

Content per entity:

- **Host** (`HostState`): name, UUID, address, dataPort, cncPort, storeTransferPort,
  and the `storageDevices` set (one UUID per line).
- **Device** (`StorageDeviceState`): UUID, host (name + id), device-set (name + id),
  currentUsage / totalSize via `formatBytes` plus usage percent (as in `list_devices`),
  and the `stores` map — one line per `StoreId` with its `StoreStatus` and, when present,
  its `transferDevice`.
- **Pool** (`StoragePoolState`): name, UUID, IDA (`toString`), maxObjectSize, device-set
  (name + id), currentUsage / maximumStoreSize via `formatBytes`, `allocationGroups`
  list, and the `stores` array indexed `[i] hostId  deviceId`.
- **Device-set** (`StorageDeviceSetState`): name, UUID, level, parent (name + id, or
  "none"), `memberDevices`, `memberSets`, `assignedPools`, and `pendingTransfers` count.
- **Allocation-group** (`AllocationGroupState`): name, UUID, level, current usage and
  maximum size via `formatBytes`, `members` (type, uuid, usage), and `parentGroups`.

Example (`show-pool`):

```
Pool: mypool
  UUID:        3f1ca5f7-74be-405d-aa48-739fc23651a4
  IDA:         Replication(width=3, write=2)
  Device Set:  fast-nvme (a1b2c3d4-...)
  Usage:       1.2 GiB
  Stores:
    [0] host_a  a1b2c3d4-...
    [1] host_b  c3d4e5f6-...
```

### 3. CLI wrappers, name resolution & error handling

Each `show_*` wrapper follows the `list_devices` shape. Inside its single `Future`:

1. Resolve the arg to an id (`resolveRef`, or a direct UUID for `show-device`).
2. Read the entity's state (`getXState`).
3. Read the related states needed for one-level name enrichment, producing
   `Option[String]` names:
   - `show-device`: read host and device-set to get their names.
   - `show-pool`: read the referenced device-set to get its name.
   - `show-device-set`: read the parent set (if any) to get its name.
   - `show-host`, `show-allocation-group`: no related-name reads (only raw member/device
     UUIDs are displayed).
4. Call the matching `formatX` function; `onComplete` prints the returned string.

**Error handling** (mirrors `list_devices`):

- **Primary entity not found:** wrap the primary lookup's `NoSuchElementException` into a
  distinct sentinel so the message is precise — e.g. `Error: pool 'foo' not found` —
  rather than mislabeling a missing sub-reference. Reuse the existing pattern
  (private `case class` sentinels like `HostNotFound`); add per-entity sentinels or a
  single generic `EntityNotFound(kind, ref)` as convenient during implementation.
- **Related-name resolution is best-effort:** if reading a related host/set to get its
  name fails, fall back to its raw UUID; do not fail the whole command.
- **Any other failure:** `Error showing <entity>: <message>`.

Each wrapper ends with `Await.ready(f, Duration(30, SECONDS))`.

## Testing

Unit tests in `MainSuite` (no running cluster required), consistent with the existing
`formatBytes` tests:

- `resolveRef`: a valid UUID string dispatches to `byUuid` (and does not call
  `byName`); a non-UUID string dispatches to `byName`.
- Each `formatX` function against hand-built state objects: assert the presence and
  correctness of key labels/values (name, UUID, sizes rendered via `formatBytes`,
  per-store lines, member lines), and the raw-UUID fallback when a name `Option` is
  `None`.

## Out of Scope

- Any name support for storage devices (they have no name in the metadata model).
- Machine-readable output formats (YAML/JSON).
- Deep/recursive enrichment (e.g. expanding every host device with its full state).
- Changes to `AspenClient` or the `common.metadata` state classes.
