# Design: `move-device-to-set` CLI command

**Date:** 2026-07-27
**Status:** Approved

## Goal

Add a CLI command, `move-device-to-set`, that moves a storage device from its
current level-0 (tier-0) device set into a different level-0 device set. The
change updates three objects in a single atomic transaction:

1. The `StorageDeviceState` is updated to point to the new device set.
2. The new `StorageDeviceSetState` is updated to include the device.
3. The old `StorageDeviceSetState` is updated to no longer include the device.

Moving a device between sets is a pure metadata change: no object data is
physically moved. Set membership only affects future device *selection* during
pool allocation and rebalancing. Devices that currently host active stores keep
hosting them, because stores are located via pool state, not set membership.

## Approach

Follow the established pattern used by `add-pool-to-group`, `add-group-to-group`,
and `transfer-store`:

- A static mutation helper on the metadata state object that builds the
  multi-object transaction.
- A thin `AspenClient` method that resolves the human-facing set name/UUID into
  an id and delegates to the helper.
- A CLI command in `Main.scala` that parses arguments and reports results.

This keeps the code consistent with the most recent commands and avoids
introducing a new server-side manager (a `StorageDeviceManager` is hinted at in
`TODO.txt` for other work, but is out of scope here).

## Relevant existing facts

- **`StorageDeviceState`** is a **KeyValue object**. Its state is stored under
  `StorageDeviceState.StateKey` and it already carries
  `storageDeviceSet: StorageDeviceSetId`. It is updated within a transaction via
  `tx.update(ptr, None, None, List(KeyRevision(StateKey, rev)), List(Insert(StateKey, bytes)))`
  (see `AspenClient.transferStore`).
- **`StorageDeviceSetState`** is a **DataObject** with
  `memberDevices: List[StorageDeviceId]`, `memberSets: List[StorageDeviceSetId]`,
  and `level: Int`. Level 0 sets hold devices (`memberDevices` may be non-empty,
  `memberSets` must be empty); level >= 1 sets hold sets. It is updated within a
  transaction via `tx.overwrite(ptr, dos.revision, DataBuffer(bytes))` (see
  `AllocationGroupState.addGroup`).
- The device already knows its **current** set via
  `deviceState.storageDeviceSet`, so the "old set" is derived, not passed as an
  argument.
- Name/UUID resolution helpers exist: `AspenClient.getStorageDeviceSetId(name)`
  resolves a set name to an id; UUIDs are parsed directly (as `rebalance` does
  with `StorageDeviceSetId(UUID.fromString(...))`).

## Components

### 1. `StorageDeviceSetState.moveDevice` (static helper)

```scala
def moveDevice(client: AspenClient,
               deviceId: StorageDeviceId,
               targetSetId: StorageDeviceSetId): Future[Unit]
```

Runs under `client.transactUntilSuccessfulWithRecovery(onFail)`. Inside
`prep(tx)`:

1. Read the device KV state (`getStorageDevicePointer` -> `read` ->
   `StorageDeviceState(kvos)`); derive `oldSetId = deviceState.storageDeviceSet`.
2. Read the target set DataObject. **Validate `target.level == 0`**; otherwise
   throw `StorageDeviceSetState.NotLevelZero(targetSetId)`.
3. **No-op** if `oldSetId == targetSetId`: return `Future.unit` without adding
   any operations to the transaction.
4. Read the old set DataObject.
5. Build all three updates in the one `tx`:
   - **Device (KV):**
     `tx.update(devPtr, None, None, List(KeyRevision(StateKey, stateKeyRev)),`
     `List(Insert(StateKey, deviceState.copy(storageDeviceSet = targetSetId).encode())))`
   - **Target set (DataObject):**
     `tx.overwrite(targetPtr, targetDos.revision,`
     `DataBuffer(target.copy(memberDevices = deviceId :: target.memberDevices.filter(_ != deviceId)).toBytes))`
     (filter first to guard against duplicate entries).
   - **Old set (DataObject):**
     `tx.overwrite(oldPtr, oldDos.revision,`
     `DataBuffer(old.copy(memberDevices = old.memberDevices.filter(_ != deviceId)).toBytes))`
     — proceeds even if the device was not present in the old set's
     `memberDevices` (self-healing toward the correct final state).

`onFail` maps non-retryable errors to `StopRetrying`:
`NoSuchElementException`, `ReadError`, and `NotLevelZero`.

**New error type:** `StorageDeviceSetState.NotLevelZero(setId: StorageDeviceSetId)`.

### 2. `AspenClient.moveDeviceToSet` (thin resolver)

```scala
def moveDeviceToSet(deviceId: StorageDeviceId, setNameOrId: String): Future[Unit] =
  given ExecutionContext = this.clientContext
  for
    targetSetId <- resolveSetId(setNameOrId)
    _           <- StorageDeviceSetState.moveDevice(this, deviceId, targetSetId)
  yield ()
```

`resolveSetId(setNameOrId)` first attempts to parse the string as a UUID
(`StorageDeviceSetId(UUID.fromString(...))`); if parsing fails, it falls back to
`getStorageDeviceSetId(setNameOrId)` for name resolution. An unresolvable name
yields `NoSuchElementException`.

### 3. CLI command in `Main.scala`

- Add `mode = "move-device-to-set"` with a `cmd(...)` block taking:
  - `<bootstrap-config-file>` (must exist),
  - `<device-uuid>` (the storage device UUID),
  - `<set-name-or-uuid>` (the target level-0 device set).
- Config fields: add `deviceId: String = ""` and reuse `newSetName` (or add a
  dedicated field) for the set argument. Exact field reuse is an implementation
  detail; the command must not collide with other modes' argument handling.
- Dispatch:
  `case "move-device-to-set" => move_device_to_set(bootstrapConfigPath, cfg.deviceId, cfg.<setArg>)`.
- `move_device_to_set(...)` mirrors `add_group_to_group`: `configureLogging()`,
  `createAmoebaClient`, `network.startIoThread(client)`, parse the device UUID
  via `StorageDeviceId(UUID.fromString(...))`, call `client.moveDeviceToSet`,
  report via `onComplete`, and `Await.ready(f, Duration(30, SECONDS))`.

### CLI error reporting (`reportError`)

- `IllegalArgumentException` (bad device UUID) ->
  `Error: invalid device UUID '<x>'`
- `NoSuchElementException` (device or set not found) ->
  `Error: device '<uuid>' or set '<x>' not found`
- `StorageDeviceSetState.NotLevelZero` ->
  `Error: target set '<x>' must be a level-0 (tier-0) set`
- fallback -> `Error moving device to set: <message>`

Success message:
`Device '<device-uuid>' moved to set '<set-name-or-uuid>'`.

## Data flow

```
CLI args
  -> move_device_to_set
    -> AspenClient.moveDeviceToSet
      -> resolveSetId (UUID or name)
      -> StorageDeviceSetState.moveDevice
        -> transactUntilSuccessfulWithRecovery
          -> single atomic tx:
               device KV state (Insert on StateKey)
             + target set DataObject (overwrite, add member)
             + old set DataObject   (overwrite, remove member)
```

## Testing

### State/transaction tests

Add to an integration suite (following `AllocationGroupStateSuite`, which extends
`IntegrationTestSuite`):

- Moves a device: the device's `storageDeviceSet` is updated, the target set's
  `memberDevices` contains the device, and the old set's `memberDevices` no
  longer contains it.
- No-op when the device is already in the target set (success, correct final
  state).
- Fails with `NotLevelZero` when the target set's `level != 0`.
- Self-heals when the old set does not list the device (still ends in the
  correct final state).
- Device not found -> `NoSuchElementException`.

### CLI tests

Add to `MainSuite`: parsing `move-device-to-set` arguments produces the correct
mode and populated config fields.

## Out of scope

- Server-side `StorageDeviceManager`.
- Any CnC / `CheckStorageDevice` notification (not needed; pure metadata).
- Any usage-cascade or durable task (device sets do not track aggregate usage the
  way allocation groups do).
- Moving a device into/out of level >= 1 sets (devices only ever live in level-0
  sets).
