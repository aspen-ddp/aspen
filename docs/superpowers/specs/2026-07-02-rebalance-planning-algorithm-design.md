# Rebalance Planning Algorithm — Design

**Date:** 2026-07-02
**Status:** Approved (design)
**Scope:** Pure planning algorithm + a small `State.Device` extension. Persistence of
the plan to the SetStateObject and the Execution ServiceTask are **out of scope** and
will be covered by separate specs.

## Background

`getStateForRebalancePlanning` (see `2026-07-02-rebalance-planning-state-design.md` and
`common/rebalancing/State.scala`) already gathers a `PlanState` for a level-0 storage
device set: a `Map[StorageDeviceId, Device]` and a `Map[PoolId, Pool]`, where each
`Store` carries `(storeId, currentSize, status)`.

This spec defines the **pure function** that consumes a `PlanState` and produces a stable
transfer plan honoring, in strict priority order:

1. **Reliability** — minimize multiple stores of the same pool on the same *device*.
2. **Availability** — minimize multiple stores of the same pool on the same *host*.
3. **Balanced usage** — equalize device fill ratio (`currentUsage / totalSize`).

The plan must be **stable**: re-running the planner on the state that results from
applying the plan must return an empty plan (no flapping).

## Chosen approach

**Greedy, priority-phased, single pass with simulation.** Three sequential phases operate
on a mutable working copy of the state, in strict priority order. Each phase appends moves;
later phases observe the effects of earlier ones. A lower-priority phase may never sacrifice
a higher-priority goal. Rejected alternatives: a unified weighted cost function (blurs strict
priority, harder to prove stable) and an ILP/constraint solver (overkill, non-deterministic
output threatens the no-flapping requirement).

## Data model & interface

### `State.Device` extension

Add `hostId` so availability reasoning is possible. `getStateForRebalancePlanning` already
reads each `StorageDeviceState`, which carries `hostId` directly, so this is populated with
no additional reads.

```scala
case class Device(deviceId: StorageDeviceId,
                  hostId: HostId,
                  currentUsage: Long,
                  totalSize: Long,
                  stores: Map[StoreId, Store])
```

### Result type

```scala
case class Transfer(storeId: StoreId, fromDevice: StorageDeviceId, toDevice: StorageDeviceId)
```

Equivalent to the `List[(storeId, fromDeviceId, toDeviceId)]` in `TODO.txt`, just named for
clarity and test readability.

### Public entry point

A pure function — no `AspenClient`, no `Future`.

```scala
object Plan:
  case class Config(
    balanceSpreadThreshold: Double = 0.05,   // T: begin/stop balancing at 5% fill-ratio spread
    minBalanceMoveGain:     Double = 0.01     // a balance move must reduce spread by at least this
  )

  def computePlan(state: State.PlanState,
                  config: Config = Config()): List[Transfer]
```

### Working copy

Internally the algorithm operates on a lightweight mutable projection of `PlanState`
(device → set of `(poolId, storeId, size, status)`, plus a running `currentUsage` per
device). Each emitted `Transfer` is applied to this working copy so subsequent decisions —
within and across phases — see the updated placement and usage. `PlanState` itself is never
mutated.

### Eligibility

Only `StoreStatus.Active` stores are candidates to move. Non-`Active` stores
(`Initializing`, `TransferringIn`, `TransferringOut`, `Rebuilding`) are **immovable but still
present**: they count toward co-location and capacity, but are never selected as a source.

## The three phases

All phases run on the working copy, in strict priority order.

### Shared helper: `bestDestination(store, sourceDevice, phase)`

Returns the best eligible destination device, or `None`. A destination is *eligible* if:

- it is in the set and `≠ source`;
- **fit:** `dest.currentUsage + store.currentSize ≤ dest.totalSize`;
- **no higher-priority regression:** the move must not create or worsen a violation owned by
  an earlier phase (defined per-phase below);
- **no-overshoot** (balance phase only): after the move, `dest.fillRatio ≤ source.fillRatio`.

Among eligible destinations, pick by the deterministic ranking in "Determinism" below.

### Phase 1 — Reliability repair (same pool, same device)

For each pool, a device is *over-loaded* if it hosts ≥2 (`Active` + immovable) stores of that
pool. Goal: minimize the **maximum** same-pool-per-device count across the set.

- Iterate devices in deterministic order. For each over-loaded `(device, pool)`, move excess
  `Active` stores to devices hosting **0** stores of that pool.
- If no zero-count device exists (fewer devices than pool width → co-location unavoidable),
  move only if it *lowers the max co-location count* for that pool. Never move if it cannot
  reduce the max — this makes "reuse if not enough devices" both correct and stable.
- Destination preference favors a host with 0 stores of the pool, so reliability repair helps
  availability for free.

### Phase 2 — Availability repair (same pool, same host)

Identical logic at **host** granularity: minimize the maximum same-pool-per-host count.

- **Constraint:** a move is rejected if it would create or worsen a Phase-1 (device)
  violation. Reliability outranks availability.
- Same "only move if it reduces the max co-location" rule for the not-enough-hosts case.

### Phase 3 — Balance (fill ratio)

- Compute `spread = maxFillRatio − minFillRatio` over devices. If
  `spread ≤ balanceSpreadThreshold`, do nothing.
- While `spread > threshold`:
  - Source = most-full device; sink = least-full device.
  - Choose the `Active` store on the source whose move to the sink most reduces spread
    (largest store that still satisfies no-overshoot + fit).
  - **Constraints:** must not create or worsen any Phase-1 or Phase-2 violation; must satisfy
    fit + no-overshoot; must reduce spread by at least `minBalanceMoveGain`.
  - If no such move exists, stop.
  - Otherwise apply it, recompute spread, repeat.

### Once-per-plan pin

A store is moved **at most once per plan**. Once moved, it is pinned in the working copy.
This keeps the plan simple and the execution phase's one-at-a-time model clean.

## Determinism, stability & edge cases

### Deterministic ordering

Every iteration order and tie-break is total and content-based, never hash/insertion-order:

- Devices ordered by `StorageDeviceId`; pools by `PoolId`; stores by `StoreId`.
- **Destination ranking** (after eligibility filtering), in order:
  1. fewest same-pool stores on the destination *device* (0 preferred);
  2. fewest same-pool stores on the destination *host*;
  3. lowest destination fill ratio;
  4. `StorageDeviceId` as final tie-break.

### Stability — why a re-run yields an empty plan

Given the *post-plan* state as new input:

- Phases 1/2: each move strictly reduced a max co-location count, and we only move when such a
  reduction exists; at plan end no reducing move remains, so a re-run finds none.
- Phase 3: we stop when `spread ≤ threshold`; a re-run sees `spread ≤ threshold` and emits
  nothing. `minBalanceMoveGain` prevents churning on sub-threshold micro-improvements, and
  no-overshoot prevents A→B / B→A ping-pong.

The algorithm is pure: identical input always yields identical output.

### Edge cases

- **Empty set / single device:** no valid destinations → empty plan.
- **All devices equally full & no co-location:** empty plan.
- **Immovable stores block a repair:** best-effort with what is movable; if nothing can be
  moved, emit no move for that violation (documented, not an error).
- **No device can fit a store:** that store is skipped; other moves still proceed.
- **Fewer devices/hosts than pool width:** converge to the minimal achievable max co-location
  and stop.
- **Capacity vs. write-threshold:** the planner only checks physical fit + no-overshoot; the
  execution phase (separate spec) owns the write-threshold safety check before initiating each
  transfer.

## Testing strategy

Pure function + pure inputs means exhaustive, fast unit tests with hand-built `PlanState`
fixtures (no client, no I/O) plus small builder helpers.

**Priority-guarantee tests (core correctness contract):**

- Reliability never decreased: no output plan raises any pool's max-same-pool-per-device
  count; include a case where a balance-motivated move would help usage but is rejected
  because it would co-locate.
- Availability never gained at the cost of reliability: a host-repair move that would create a
  device co-location is rejected.
- Lexicographic ordering: balance moves never introduce a reliability/availability regression.

**Per-phase behavior:**

- Reliability: 2- and 3-same-pool-on-one-device cases resolve to distinct devices;
  "not enough devices" converges to minimal max co-location.
- Availability: same-pool-on-one-host resolves across hosts; multi-device-per-host topologies.
- Balance: heterogeneous device sizes converge to within threshold; the two named use cases —
  (a) one oversized store forcing migration, (b) a fresh empty device added — produce sensible
  fills.

**Stability tests (no-flapping contract):**

- For every scenario: apply the returned plan to the state, re-run `computePlan`, assert the
  second result is **empty**.
- Ping-pong guard: two near-equal devices just under/over threshold produce no oscillation.

**Determinism tests:**

- Same input → identical output list (order included).
- Shuffling input map/collection order does not change the output.

**Edge cases:** empty set, single device, all-immovable, no-fit-anywhere, fewer devices/hosts
than width — each asserts a well-defined (usually empty) plan.

## Affected files

- `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/State.scala` — extend `Device` with
  `hostId`; populate it in `getStateForRebalancePlanning`.
- New: the `Plan` object (`computePlan`, `Config`, `Transfer`) — likely
  `src/main/scala/org/aspen_ddp/aspen/common/rebalancing/Plan.scala`.
- New: test suite under `src/test/.../rebalancing/`.

### Note on IDA

The pure algorithm as designed does **not** reference any `IDA` field. Reliability and
availability are enforced by minimizing the maximum same-pool co-location count per
device/host — a criterion that is independent of `width`/`writeThreshold`/`failureTolerance`
and is strictly at-least-as-safe as any tolerance-based rule. `Pool.ida` remains available in
`PlanState` for a future refinement (e.g. allowing bounded co-location up to `failureTolerance`
when devices are scarce), but it is intentionally unused here to keep the algorithm simple and
provably stable.
