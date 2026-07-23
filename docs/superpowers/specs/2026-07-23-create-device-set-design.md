# Design: `create-device-set` CLI command

Date: 2026-07-23

## Summary

Add a `create-device-set` command to the Aspen command-line utilities
(`org.aspen_ddp.aspen.cmdline.Main`). The command creates a new
`StorageDeviceSetState`, records it in system metadata, and fails if the name is
already in use.

The client-side creation logic already exists as
`AspenClient.createStorageDeviceSet(name, level, parent)` (implemented in
`BaseAspenClient`). It allocates the state `DataObject` on the bootstrap pool,
inserts it into the `storageDeviceSetsTree` (TKVL), registers
`"device-set".name -> UUID` in the `NamespacedUUIDRegistry`, and rejects
duplicate names via `Registry.DuplicateRegistration`. It also supports linking
the new set into a parent set's `memberSets` atomically.

Therefore this work does **not** add a new client method. It:

1. Fixes a validation bug in the existing `createStorageDeviceSet`.
2. Adds the `create-device-set` CLI command wired to that method.

No protobuf or `Codec` changes are required.

## Background

`StorageDeviceSetState` groups physical storage devices hierarchically:

- `level == 0`: a set of physical devices (`memberDevices` populated).
- `level >= 1`: a set of sets (`memberSets` populated).
- `parent`: optional reference to a higher-level set. Invariant:
  `parent.level > child.level`.

The invariant is currently documented but **not enforced** in
`createStorageDeviceSet` — a genuine bug that this work corrects.

## Part 1 — Fix parent-level validation in `createStorageDeviceSet`

File: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`
(and `AspenClient.scala` for the new exception type).

The fix lives in the client method so that every caller is protected, not just
the CLI.

- Add an exception to the `AspenClient` companion object, alongside
  `InvalidDestination`:

  ```scala
  class InvalidDeviceSetLevel(childLevel: Int, parentLevel: Int)
    extends Exception(s"Device set level $childLevel must be less than parent level $parentLevel")
  ```

- In `createStorageDeviceSet`'s `addToParent` helper, after reading the parent's
  state, fail the transaction when `sds.level >= parentState.level`. The failure
  must be non-retryable so an invalid request does not loop under `runCreate` —
  surfaced as `StopRetrying(InvalidDeviceSetLevel(...))`, consistent with the
  existing `DuplicateRegistration` / `NoSuchElementException` handling in the
  method's `onFail`.

  The exact retry-abort wiring will be confirmed against the `runCreate`
  implementation during coding; the design decision — validate here, fail
  creation — is fixed.

## Part 2 — `create-device-set` CLI command

File: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`.

Mirrors the existing `create-pool` command structure.

### `Args` additions

```scala
newSetName: String = "",
newSetLevel: Int = 0,
parentSetName: String = "",   // empty => no parent
```

### Command registration

`cmd("create-device-set")` with positional arguments:

- `<bootstrap-config-file>` — must exist.
- `<name>` — name of the new device set.
- `<level>` — `Int`, validated `>= 0`.
- `[parent-set-name]` — optional positional argument (matching how `create-pool`
  uses positional args). When present, resolved to a `StorageDeviceSetId` via
  `client.getStorageDeviceSetId`.

### Dispatch

Add `case "create-device-set" => create_device_set(...)` to the `cfg.mode`
match block.

### Handler

`def create_device_set(bootstrapConfigFile, name, level, parentSetName): Unit`
follows the `create_pool` shape:

1. `configureLogging()` -> `createAmoebaClient(bootstrapConfigFile)` ->
   `network.startIoThread(client)` -> `given ExecutionContext = client.clientContext`.
2. Resolve parent name to `Option[StorageDeviceSetId]` (only when provided).
3. Call `client.createStorageDeviceSet(name, level, parentOpt)`.
4. `Await` the result and print a success banner (as `create_pool` does).

## Error handling (CLI)

Recover the failed `Future` into friendly messages instead of raw stack traces:

- `DuplicateRegistration` (wrapped in `StopRetrying`) ->
  `"Error: a device set named '<name>' already exists"`.
- Parent lookup `NoSuchElementException` ->
  `"Error: parent device set '<parentName>' not found"`.
- `InvalidDeviceSetLevel` -> print the exception message.

## Testing

- Client test: creating a child set with `level >= parent.level` is rejected
  with `InvalidDeviceSetLevel`.
- Client test: creating a valid child links it into the parent's `memberSets`.
- Client test (existing behavior still holds): duplicate name is rejected.

CLI handler functions are thin and are not unit-tested in this codebase,
consistent with `create_pool` / `transfer_store` / `rebalance`.

## Files touched

- `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` — new
  `InvalidDeviceSetLevel` exception.
- `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` —
  parent-level validation in `createStorageDeviceSet`.
- `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — `Args` fields,
  command registration, dispatch case, and `create_device_set` handler.

## Out of scope

- No new `createNewDeviceSet` client method (existing
  `createStorageDeviceSet` is reused).
- No changes to `StorageDeviceSetState`, protobuf, or `Codec`.
- Other TODO CLI commands (`list-*`, `show-*`, `create-allocation-group`,
  etc.) are separate work items.
