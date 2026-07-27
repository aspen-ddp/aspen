# Design: `add-group-to-group` CLI command

**Date:** 2026-07-27
**Status:** Approved

## Summary

Add a CLI command that nests one allocation group inside another, mirroring the
existing `add-pool-to-group` command. The core logic already exists in
`AllocationGroupState.addGroup`, which atomically updates both group objects and
enforces the level invariant. This work is a thin `AspenClient` method plus CLI
wiring — no protobuf/codec changes are required (the schema already supports
`MemberType.Group` members).

## Background

Allocation groups form a hierarchy keyed by an integer `level`:

- **Level 0** groups contain **pools** (leaf groups).
- **Level 1+** groups contain **other groups** of strictly lower level
  (super-groups).

`AllocationGroupState.members` is a unified `List[Member]` holding both pools and
nested groups, distinguished by `Member.memberType` (`MemberType.Pool` /
`MemberType.Group`). A group also tracks the groups it belongs to via
`parentGroups: List[AllocationGroupId]`.

The nesting invariant is already enforced by `AllocationGroupState.addGroup`:

```scala
if child.level >= parent.level then throw new InvalidLevel()
```

So the **destination (parent) group must have a strictly greater level than the
source (child) group**.

## Requirements

- New CLI command `add-group-to-group`.
- Takes a bootstrap config file, a source group name, and a destination group name.
- Groups are identified by **name only** (resolved via `getAllocationGroupId`),
  consistent with `add-pool-to-group`. UUID-literal arguments are out of scope.
- Destination group level must be strictly greater than the source group level;
  a violation is reported with a clear message.
- Updates the group state objects accordingly (child's `parentGroups`, parent's
  `members`) in a single transaction.

## Design

### Component 1 — `AspenClient.addGroupToGroup`

A concrete `def` on the `AspenClient` trait with a default body (like
`addPoolToGroup`), so no test stubs (`TestNetwork`, `BaseReadDriverSuite`) need
updating:

```scala
def addGroupToGroup(sourceGroupName: String, destGroupName: String): Future[Unit] =
  given ExecutionContext = this.clientContext
  for
    childId  <- getAllocationGroupId(sourceGroupName)   // source = lower level
    parentId <- getAllocationGroupId(destGroupName)     // destination = higher level
    _        <- AllocationGroupState.addGroup(this, childId, parentId, None)
  yield ()
```

- `None` for `taskExecutor` → uses the system durable-task path for any usage
  cascade (no local executor), matching `addPoolToGroup`.
- Name resolution via `getAllocationGroupId` throws `NoSuchElementException` for
  unknown names.

### Component 2 — CLI command in `Main.scala`

Four edits, same shape as `add-pool-to-group`:

1. **`Args` field** — add a dedicated `srcGroupName: String = ""` (preferred over
   reusing `poolName`, which reads as semantically wrong for a group-to-group
   operation). Reuse the existing `newGroupName` field for the destination.
2. **Parser registration** — `cmd("add-group-to-group")` with arguments
   `<bootstrap-config-file> <source-group-name> <destination-group-name>` and help
   text noting *"destination group level must be strictly greater than the source
   group level."*
3. **Dispatch case** —
   `case "add-group-to-group" => add_group_to_group(bootstrapConfigPath, cfg.srcGroupName, cfg.newGroupName)`
4. **Handler function** `add_group_to_group(...)`, identical in structure to
   `add_pool_to_group` (`configureLogging()` → `createAmoebaClient` →
   `network.startIoThread(client)` → call → `f.onComplete` → `Await.ready(f, 30s)`),
   with a **three-case** `reportError`:
   - `NoSuchElementException` → `"Error: allocation group '<src>' or '<dest>' not found"`
   - `AllocationGroupState.InvalidLevel` → `"Error: destination group '<dest>' must have a higher level than source group '<src>'"`
   - fallback → `"Error adding group to group: <message>"`

This requires importing `AllocationGroupState` in `Main.scala`.

### Data flow

CLI args → `addGroupToGroup` → resolve both names to `AllocationGroupId` →
`AllocationGroupState.addGroup` runs a single transaction that:

1. Prepends `parentId` to the child's `parentGroups`.
2. Prepends a `Member(MemberType.Group, child.groupId.uuid, …)` to the parent's
   `members`.
3. Conditionally enrolls `UpdateAllocationGroupUsageTask` (only when the parent
   itself has grandparent groups to cascade to), via the system durable-task path.

The operation is idempotent: `addGroup` no-ops when the membership already exists.

### Error handling

- **Level violation**: enforced inside `addGroup` (`child.level >= parent.level`
  → `InvalidLevel`) and stopped from retrying in `modifyGroup`'s `onFail`, so it
  surfaces directly on the returned `Future`. The CLI reports it clearly. No
  redundant client-side level check is added.
- **Unknown names**: surfaced as `NoSuchElementException` from
  `getAllocationGroupId` and reported by the CLI.

## Testing

- **Client happy-path test** in `AllocationGroupStateSuite.scala`, mirroring the
  existing `"addPoolToGroup resolves names and adds the pool"` test: create two
  groups at different levels, call `addGroupToGroup`, assert the child's
  `parentGroups` contains the parent and the parent's `members` contains the child.
- **Level-violation test**: assert `addGroupToGroup` with source level ≥
  destination level fails with `InvalidLevel`.
- Before adding low-level tests, verify whether `AllocationGroupState.addGroup`
  already has direct coverage to avoid redundancy.

## Out of scope (YAGNI)

- UUID-string arguments (names only for now).
- `remove-group-from-group`.
- Any codec/protobuf changes — the schema already supports `MemberType.Group`.

## Affected files

| File | Change |
|---|---|
| `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` | Add concrete `addGroupToGroup` method |
| `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` | `Args` field, parser cmd, dispatch case, handler fn, import |
| `src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala` | Happy-path + level-violation tests |
