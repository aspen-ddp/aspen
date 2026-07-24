# Design: `list-devices` CLI Command

**Date:** 2026-07-23
**Status:** Approved
**Scope:** Add a `list-devices` command to the Aspen cmdline utilities.

## Goal

Add a CLI command that lists the storage devices belonging to a given host. For
each device, display:

- Device UUID
- The name of the storage device set it belongs to
- Total capacity (human-readable)
- Percent utilization

This is the top item in `TODO.txt`:

```
Add CLI command "list-devices"
  - Requires a hostname argument
  - Lists each device UUID, the device set name it belongs to, total capacity, & percent utilization
```

## Context

The command follows the pattern established by the recently added listing
commands (`list-pools`, `list-hosts`, `list-allocation-groups`,
`list-device-sets`). Unlike those commands, `list-devices`:

- takes a `<hostname>` argument, and
- requires a multi-step async resolution and a four-column table,

so it does not fit the generic `list_entries` helper and gets its own function.

### Relevant existing code (verified)

- `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — the only file edited.
  Contains all CLI parsing, dispatch, and command implementations, including the
  `list_entries` helper and commands like `create_device_set` / `transfer_store`
  whose structure `list_devices` mirrors.
- `AspenClient` methods used (all confirmed present):
  - `getHostId(hostName: String): Future[HostId]` — resolves the hostname
    argument; throws `NoSuchElementException` via the registry when not found.
  - `getHostState(hostId: HostId): Future[HostState]`
  - `getStorageDeviceState(id: StorageDeviceId): Future[StorageDeviceState]`
  - `getStorageDeviceSetState(id: StorageDeviceSetId): Future[StorageDeviceSetState]`
- `HostState.storageDevices: Set[StorageDeviceId]`
- `StorageDeviceState`: `storageDeviceId`, `currentUsage: Long`, `totalSize: Long`,
  `storageDeviceSet: StorageDeviceSetId`.
- `StorageDeviceSetState.name: String`.

No wire-protocol changes are required (`codec.proto` / `Codec.scala` untouched).

## Approach

**Dedicated `list_devices` function** rather than generalizing `list_entries`.
The generic helper only handles flat `(name, id)` pairs from a single call in a
two-column layout. Generalizing it to N columns plus an argument would be
premature abstraction for one command of a different shape (YAGNI).

## Detailed Design

All changes are in `Main.scala`.

### 1. Argument parsing

Add a `cmd("list-devices")` block after the `list-device-sets` block:

- standard validated `<bootstrap-config-file>` argument (existence-checked, same
  as the other list commands), and
- a `<hostname>` string argument stored in the existing `Args.hostName` field
  (already present in the `Args` case class — reused, not added).

### 2. Dispatch

Add to the `cfg.mode match` block:

```scala
case "list-devices" => list_devices(bootstrapConfigPath, cfg.hostName)
```

### 3. Implementation function

```scala
def list_devices(bootstrapConfigFile: os.Path, hostname: String): Unit
```

- Standard setup: `configureLogging()`, `createAmoebaClient`,
  `network.startIoThread(client)`, `given ExecutionContext = client.clientContext`.
- For-comprehension:
  1. `hostId    <- client.getHostId(hostname)`
  2. `hostState <- client.getHostState(hostId)`
  3. `devStates <- Future.sequence(hostState.storageDevices.toList.map(client.getStorageDeviceState))`
  4. dedupe `devStates.map(_.storageDeviceSet).distinct`, then
     `Future.sequence(...getStorageDeviceSetState...)` and build a
     `setId -> name` map.
  5. yield rows `(uuid, setName, totalSize, pct)` sorted by uuid string.
- Percent utilization: `if totalSize > 0 then currentUsage.toDouble / totalSize * 100.0 else 0.0`
  (guards the `totalSize == 0` case seen on freshly initialized devices).
- Set-name fallback: if a set id is missing from the map, display the set's UUID
  string.

### 4. Byte formatter

A small private helper:

```scala
def formatBytes(n: Long): String
```

Produces binary units (`B`, `KiB`, `MiB`, `GiB`, `TiB`, `PiB`) with one decimal
place, e.g. `1.5 TiB`, `512.0 GiB`.

### 5. Output & error handling

Via `f.onComplete`:

- Success, no devices: `No devices found for host '<hostname>'`
- Success with devices: a header line, then one width-padded row per device
  (columns padded in the style of `list_entries`):

  ```
  Devices for host 'node_a'
    <uuid>  ssd-set     1.5 TiB   42.3%
    <uuid>  hdd-set    10.0 TiB    8.1%
  ```

- `Failure(_: NoSuchElementException)` → `Error: host '<hostname>' not found`
- Any other `Failure` → generic `Error listing devices: <message>`

Close with `Await.ready(f, Duration(30, SECONDS))`.

## Testing

The existing `list-*` commands ship without unit tests — they are thin CLI
wrappers over already-tested client methods. `list-devices` is the same shape,
so the stance is:

- `sbt compile` must succeed.
- Manual verification against the demo cluster (`./t list-devices ...`).
- One optional pure unit test of `formatBytes`, the only non-trivial pure logic.

## Out of Scope

- The `show-*` commands and other TODO items.
- Any wire-protocol / codec changes.
- A dedicated `listStorageDevices()` client method (not needed; existing
  per-device reads suffice).
