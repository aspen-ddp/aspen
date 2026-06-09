# StorageDeviceSet Device Selection Design

Date: 2026-06-09

## Summary

Add `StorageDeviceSetState.selectDevicesForPool` to choose a list of
`StorageDeviceId`s for a pool that needs `numStores` stores. Selection walks the
device-set hierarchy: a level-0 set draws from its member devices; a level-1+ set
distributes the request across its member sets and recurses down to level 0,
where the actual device ids live.

This pass adds the method and its unit tests only. Wiring the method into the
real pool-allocation path is out of scope.

## Goals

- Even distribution of selected stores across the available devices / sub-sets.
- Minimal, even reuse of device ids when `numStores` exceeds capacity.
- Best-effort avoidance of duplicate device ids, with even distribution taking
  priority at level 1+.
- Deterministic, fast unit tests for both the level-0 and level-1+ strategies.

## Public API

Added to `StorageDeviceSetState` (`common/metadata/StorageDeviceSetState.scala`):

```scala
def selectDevicesForPool(
    numStores: Int,
    client: AspenClient,
    rng: Random = new Random()
): Future[List[StorageDeviceId]]
```

- `numStores <= 0` returns `Future.successful(Nil)`.
- The method delegates to the recursive helper (below) with an empty exclusion
  set, `client.getStorageDeviceSetState` as the lookup function, and
  `client.clientContext` as the `ExecutionContext`.
- `client` is an explicit parameter. `rng` is an injectable, defaulted parameter
  so production callers omit it while tests pass a seeded `Random(seed)` for
  deterministic assertions.

## Core recursive helper (the testable seam)

The recursion does not need the full `AspenClient` — only the ability to resolve
a `StorageDeviceSetId` to its state. Factoring the algorithm to depend on a
narrow lookup function keeps the public API on `AspenClient` while making the
core logic pure and unit-testable without the integration harness.

```scala
private[metadata] def selectDevices(
    numStores: Int,
    exclude: Set[StorageDeviceId],
    lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
    rng: Random
)(using ec: ExecutionContext): Future[List[StorageDeviceId]]
```

`exclude` carries the device ids already chosen earlier in the overall
selection, so deeper recursive calls can avoid reusing them.

## Level 0 algorithm (`level == 0`, uses `memberDevices`)

Empty `memberDevices` → `Future.failed(AllocationError(...))`. Otherwise a single
unified rule:

```
shuffled  = rng.shuffle(memberDevices)
preferred = shuffled.filterNot(exclude.contains)          // non-excluded, in shuffled order
stream    = preferred.iterator ++ Iterator.continually(shuffled).flatten
result    = stream.take(numStores).toList
```

This one expression covers every case:

- **Direct call** (`exclude` empty): the first pass yields distinct devices, then
  round-robin cycling kicks in. No duplicates when `members >= numStores`;
  minimal and even reuse when `members < numStores` (each device used either
  `floor(numStores/members)` or `ceil(numStores/members)` times).
- **Recursive call** (`numStores == 1` with an accumulated `exclude`): returns the
  first non-excluded device in shuffled order, falling back to a duplicate only
  when every member device is already excluded.

Returned as a successful `Future`.

## Level 1+ algorithm (`level >= 1`, uses `memberSets`)

Empty `memberSets` → `Future.failed(AllocationError(...))`. Otherwise:

```
shuffled = rng.shuffle(memberSets)
visits   = Iterator.continually(shuffled).flatten.take(numStores).toList   // round-robin, e.g. [A,B,C,A,B]
```

Sequentially fold over `visits`, threading `(results, exclude)`:

- For each `setId` in `visits`: `lookup(setId)` to get the sub-set state, then
  recurse with `numStores = 1` and the current `exclude`. Take the single
  returned device, append it to `results`, and add it to `exclude`.

The fold is sequential because each step's `exclude` depends on the previous
step's result.

- **Even distribution** is guaranteed by the round-robin `visits` order: per-sub-set
  visit counts differ by at most 1.
- **Best-effort dedup** comes from the growing `exclude` set, with even
  distribution taking priority (we still visit in round-robin order; we only
  avoid already-chosen devices within each visit).
- A failure deep in the tree (e.g. an empty sub-set that cannot yield a device)
  propagates up as a failed `Future`.

## Error handling

- A set that cannot yield a required device (empty `memberDevices` at level 0, or
  empty `memberSets` at level 1+) fails the `Future` with `AllocationError`,
  consistent with `GroupObjectAllocator`'s empty-group behavior.
- The `level` field is trusted for member-type selection: `level == 0` reads
  `memberDevices`; `level >= 1` reads `memberSets`. (The class already documents
  these invariants.)

## Testing

A new ScalaTest suite, `StorageDeviceSetSelectionSuite`
(`src/test/scala/org/aspen_ddp/aspen/common/metadata/`), drives the
package-private `selectDevices` helper directly with a seeded `Random`. No
`AspenClient` instance is required.

### Level 0

Built from a hand-constructed level-0 `StorageDeviceSetState`; lookup is unused.

- `numStores < members`: result length equals `numStores`, all distinct, all
  drawn from members.
- `numStores == members`: result is a permutation of all members (distinct).
- `numStores > members`: length equals `numStores`; reuse is minimal and even —
  max per-device usage equals `ceil(numStores/members)`, min equals
  `floor(numStores/members)`.
- `exclude` honored: at `numStores == 1` with all-but-one excluded, returns the
  non-excluded device; with all excluded, returns some member (fallback).
- Determinism: identical seed produces identical results.

### Level 1+

Built from an in-memory `Map[StorageDeviceSetId, StorageDeviceSetState]` exposed
as `lookup = id => Future.successful(map(id))`.

- Even distribution: device counts per sub-set differ by at most 1 (the sub-set
  owning each returned device is resolved via the map).
- Best-effort dedup: when every sub-set has at least
  `ceil(numStores / numMemberSets)` distinct devices, the result has no
  duplicates.
- Dedup fallback: when a sub-set is revisited more times than it has distinct
  devices (because even distribution requires it), duplicates appear — and only
  from that exhausted sub-set — while the even per-sub-set distribution still
  holds.
- Nesting: a level-2 set over level-1 sets over level-0 sets chains correctly
  down to device ids.

Tests await results with `Await.result` using a test `ExecutionContext`,
following existing suite conventions.

## Out of scope

- Wiring `selectDevicesForPool` into the actual pool/store allocation path.
- Any change to `StorageDeviceSetState`'s serialized form (the method adds no
  fields).
