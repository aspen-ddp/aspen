# create-allocation-group CLI Command — Design

**Date:** 2026-07-23
**Status:** Approved
**Scope:** Add a `create-allocation-group` command to the Aspen command-line utilities.

## Goal

Add a CLI command that creates a new allocation group, taking a name and a
level. The command must fail if the name is already in use.

Source: top item of `TODO.txt`:

> Add CLI command "create-allocation-group"
> - takes name and level arguments
> - fail if name already in use

## Background

The client-side API already exists and does the real work:

- `AspenClient.createAllocationGroup(groupName: String, level: Int): Future[AllocationGroupId]`
  (`src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:116`)
- Implemented in `BaseAspenClient` (`.../client/internal/BaseAspenClient.scala:162`).
  The implementation registers the group name in the `Namespaces.Group`
  namespace **inside the creation transaction**, so name-uniqueness is enforced
  atomically: a duplicate name fails with `Registry.DuplicateRegistration`.

Therefore this task is purely a CLI-layer addition in
`src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`. No changes are needed
to the client, protobuf/`Codec`, or `AllocationGroupState`.

The existing `create-device-set` command is a near-perfect template: same
argument shape (name + level) and the same `DuplicateRegistration` failure mode.

## Design

All changes are confined to `Main.scala`.

### 1. `Args` case class
Add two fields with defaults:
- `newGroupName: String = ""`
- `newGroupLevel: Int = 0`

### 2. Argument parser
Add a `cmd("create-allocation-group")` block modeled on `create-device-set`,
with arguments:
- `<bootstrap-config-file>` — must exist (validated).
- `<name>` — name of the new allocation group.
- `<level>` — hierarchy level; validated `>= 0` (same convention as
  `create-device-set`).

No parent argument. Attaching pools/groups is a separate concern covered by the
`add-pool-to-group` TODO item.

### 3. Dispatch
Add to the `cfg.mode match` block:

```scala
case "create-allocation-group" =>
  create_allocation_group(bootstrapConfigPath, cfg.newGroupName, cfg.newGroupLevel)
```

### 4. Implementation function
`create_allocation_group(bootstrapConfigFile, name, level)`, modeled on
`create_device_set`:

1. `configureLogging()`, then `createAmoebaClient(...)` + `network.startIoThread(client)`.
2. `given ExecutionContext = client.clientContext`.
3. `val f = client.createAllocationGroup(name, level)`.
4. `f.onComplete`:
   - Success → print a banner with the new `groupId.uuid`.
   - Failure → `reportError`:
     - `DuplicateRegistration` → `"Error: an allocation group named '<name>' already exists"`.
     - other → `"Error creating allocation group: <message>"`.
5. `Await.ready(f, Duration(30, SECONDS))`.

## Error handling

- **Duplicate name:** surfaces as `DuplicateRegistration` (the client retry
  layer unwraps `StopRetrying`), reported as a clear message. No pre-check /
  TOCTOU race — uniqueness is enforced atomically in the transaction.
- **Bad config file:** rejected by the parser's `validate`.
- **Negative level:** rejected by the parser's `validate`.
- **Timeout / network failure:** falls through to the generic error message.

## Non-goals / YAGNI

- No parent-group argument (separate `add-pool-to-group` item).
- No level validation beyond `>= 0`. The `AllocationGroupState.InvalidLevel`
  invariant (`child.level < parent.level`) is only meaningful when adding a
  group to a parent, which this command does not do.

## Testing

`Main.scala` CLI commands have no existing unit tests; they are exercised
manually against the demo cluster (see `TODO.txt` testing notes). Verification:

- `sbt compile` succeeds.
- The command parses and is dispatched (help output / arg validation).

No new test infrastructure is added, consistent with the current pattern for
sibling commands (`create-device-set`, `transfer-store`, etc.).
