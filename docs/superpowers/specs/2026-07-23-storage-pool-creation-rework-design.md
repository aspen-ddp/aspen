# Storage Pool Creation Rework — Design

**Date:** 2026-07-23
**Status:** Approved (design)

## Summary

Rework storage pool creation so that the caller specifies a **storage device set**
instead of a pre-resolved list of device ids. Device selection is delegated to the
already-implemented `StorageDeviceSetState.selectDevicesForPool`, and the pool→set
relationship is made bidirectional by recording the new pool in the set's
`assignedPools` list. The CLI `new-pool` command (currently a stub) is replaced by a
working `create-pool` command that takes a device-set name.

`create-device-set` and other roadmap CLI/listing commands are explicitly **out of
scope** for this pass.

## Current State (verified)

- `AspenClient.createNewStoragePool` (`AspenClient.scala:139`) takes
  `storageDeviceIds: List[StorageDeviceId]`, validates `size >= ida.width`, resolves
  each id to its `hostId`, builds the `stores` array, and calls `createStoragePool`.
  It has **no current callers** (the only reference was the commented-out CLI stub).
- `StorageDeviceSetState.selectDevicesForPool(numStores, client, rng)`
  (`StorageDeviceSetState.scala:52`) already exists and works. It **cycles** its
  device list when the set has fewer distinct devices than `numStores`, and fails
  with `AllocationError` if the set is empty.
- `StoragePoolState.storageDeviceSet: StorageDeviceSetId` already exists — the
  pool→set reference (forward direction) is done.
- `StorageDeviceSetState.assignedPools: List[PoolId]` exists but is **never updated**
  during pool creation (reverse direction missing). A stale comment on
  `AspenClient.scala:136` calls this out as unimplemented.
- `createStoragePool` (`BaseAspenClient.scala:205`) runs a single retriable
  `runCreate` transaction: creates the pool KV object, puts it in `storagePoolsTree`,
  registers the name in the `"pool"` namespace, and updates each affected
  `StorageDeviceState` (adding `Initializing` store entries under a `KeyRevision`
  guard), then sends `CheckStorageDevice` host messages on commit.
- The CLI `new-pool` command (`Main.scala:173`, dispatch at `:277`, body `new_pool`
  at `:783`) resolves host names but the actual creation call is commented out
  (`:814`). It uses `Args.hosts`.
- `Args.hosts` (`Main.scala:68`) is used **only** by `new-pool` (`:277`). The other
  `.hosts` references (`:731`, `MetadataManager.scala`) are `BootstrapConfig.hosts`,
  unrelated to `Args`.
- `assignedPools` and `storageDeviceSet` are already in the wire format
  (`codec.proto` / `Codec.scala`) — no serialization changes needed.

## Design Decisions

- **Device uniqueness:** Keep the current cycling behavior. `create-pool` will
  succeed against a set with fewer distinct devices than `ida.width`, placing
  multiple stores on one device. This silently weakens IDA durability but keeps
  small/test setups working. No uniqueness enforcement is added.
- **Selection outside the transaction, mutation inside:** `createNewStoragePool`
  performs device selection (a plain read) before the create transaction.
  `createStoragePool` records `assignedPools` inside its existing transaction under a
  revision guard. The selection read being outside the tx is a harmless TOCTOU —
  worst case a retry re-selects.
- **Reuse existing serialization:** No `codec.proto` / `Codec.scala` changes.
- **Scope:** Only `createNewStoragePool` rework + `assignedPools` back-reference +
  `create-pool` CLI. `create-device-set` and listing/show commands are deferred.

## Detailed Design

### 1. `AspenClient.createNewStoragePool`

Drop `storageDeviceIds`; derive devices from the set.

```scala
def createNewStoragePool(name: String,
                         ida: IDA,
                         maxObjectSize: Option[Int],
                         backendConfig: BackendConfig,
                         storageDeviceSet: StorageDeviceSetId,
                         maximumStoreSize: Long): Future[PoolId]
```

Body:
1. Read the set: `getStorageDeviceSetState(storageDeviceSet)`.
2. `deviceIds <- set.selectDevicesForPool(ida.width, this)` — returns `ida.width`
   ids (cycling if the set is small; fails with `AllocationError` if empty).
3. Resolve each id → `StorageDeviceState` to get `hostId`; build the `stores` array
   (as today).
4. Build `StoragePoolState` and call `createStoragePool(config)`.

The old `storageDeviceIds.size < ida.width` guard is removed — `selectDevicesForPool`
now owns sizing.

Also delete the stale `// NOTE: ... not yet implemented` comment at
`AspenClient.scala:136`.

### 2. `createStoragePool` — `assignedPools` back-reference

Inside the existing `runCreate` transaction in `createStoragePool`
(`BaseAspenClient.scala:205`), add one atomic step alongside pool-object creation and
per-device updates:

1. Read the set object fresh: `getStorageDeviceSetPointer(config.storageDeviceSet)`
   → `read(ptr)` → `StorageDeviceSetState(dos)`.
2. If `config.poolId` is not already in `assignedPools` (idempotent for retries),
   append it: `updated = set.copy(assignedPools = config.poolId :: set.assignedPools)`.
3. Revision-guarded overwrite:
   `tx.overwrite(setPtr, dos.revision, DataBuffer(updated.toBytes))`.

This mirrors the existing `addToParent` pattern in `createStorageDeviceSet` and the
device-state update pattern already in this method. Concurrent pool creations against
one set serialize via the revision guard; the loser retries under `runCreate`.

### 3. CLI: `create-pool` (rename of `new-pool`)

```
create-pool <bootstrap-config-file> <pool-name> <ida-type> <width> \
            <read-threshold> <write-threshold> <device-set-name>
```

- Parser: rename `cmd("new-pool")` → `cmd("create-pool")`, set `mode = "create-pool"`.
  Replace the `<hosts>` arg with `<device-set-name>` → `Args.deviceSetName`.
- Dispatch (`:277`): `case "create-pool" => create_pool(...)`.
- Body (replacing `new_pool`):
  - Resolve `client.getStorageDeviceSetId(deviceSetName)` (already implemented via the
    `"device-set"` namespace).
  - Build the `IDA` from args (as the stub already does).
  - Call `createNewStoragePool(name, ida, None, RocksDBConfig(), setId, maximumStoreSize)`,
    `Await` the result, print the resulting `PoolId`.
  - Drop `require(hosts.length == width)`; keep the threshold sanity checks.
- `Args`: add `deviceSetName: String = ""` and `maximumStoreSize: Long = 0` (optional
  trailing CLI arg, defaulting to `0` — matching `StoragePoolState`'s existing
  `maximumStoreSize` default). Remove the now-unused `hosts` field.
- Hardcoded for now: `maxObjectSize = None`, `backendConfig = RocksDBConfig()`.

### 4. Call sites & tests

- `createNewStoragePool` has no production callers, so the signature change only
  affects the CLI (handled above) and any test/stub references.
- Update `TestNetwork.scala`, integration suites, and any client stubs
  (`SimpleAspenClient`, `BaseReadDriverSuite`) that reference the old signature so
  they compile — enumerate exact sites via `grep -rn createNewStoragePool src/`
  during implementation.
- No `codec.proto` / `Codec.scala` changes.

## Verification

- `sbt compile` clean.
- `sbt test` green (existing suites, including
  `StorageDeviceSetSelectionSuite` and any pool/device-set integration tests).
- Manual smoke (optional): `create-pool` against a bootstrap device set produces a
  pool whose `StoragePoolState.storageDeviceSet` and the set's `assignedPools` agree.

## Out of Scope

- `create-device-set` CLI command (and `createNewDeviceSet` alias — the existing
  `createStorageDeviceSet` already covers the client side).
- `list-*` / `show-*` CLI commands.
- Device-uniqueness enforcement.
- Pool deletion / `assignedPools` removal.
