# Automatic Rebalancing Design

## Goal

Extend `RebalancingDurableService` to initiate rebalancing on its own, without an
administrator running `rebalance <setId>` for each storage device set. The service
periodically sweeps every level-0 storage device set, builds a plan for each, and enrolls
a `SetRebalanceDurableTask` wherever the plan is non-empty.

The sweep period is persisted, administrator-settable through a new
`system-rebalance-period` cmdline subcommand, and can be set to zero to disable automatic
rebalancing entirely while leaving on-demand `rebalance` untouched.

This completes the "passive background rebalancer" deferred as out of scope by
[2026-07-22-rebalancing-active-flow-design.md](2026-07-22-rebalancing-active-flow-design.md).

## Scope

- Two new keys in the `RebalancingDurableService` KV state object: the sweep period and
  the timestamp of the last completed sweep.
- Sweep logic folded into the service's existing `reconcile()` polling path.
- A third `RebalancingMessage` variant used as a nudge when the period changes.
- `getAutoRebalancePeriod` / `setAutoRebalancePeriod` on the `RebalancingDurableService`
  companion object.
- A `system-rebalance-period` cmdline subcommand that displays or sets the period.

Not in scope: per-set periods, a cap on the number of concurrently active automatic
rebalances, changes to the planning algorithm, changes to `SetRebalanceDurableTask`.

Backwards compatibility with clusters bootstrapped before this change is explicitly **not
required**. Both new keys are always written by `initialServiceState`; decoders assume
they are present.

## Data Model

### `RebalancingServiceState` — two new keys

The service's KV state object currently holds one key. Two are added:

```scala
object RebalancingServiceState:
  val ActiveTasksKey: Key           = Key(Array[Byte](0))   // existing
  val AutoRebalancePeriodKey: Key   = Key(Array[Byte](1))   // new
  val LastAutoRebalanceKey: Key     = Key(Array[Byte](2))   // new
```

Both new values are scalar `Long`s encoded with the existing `common.util.long2byte` /
`byte2long` helpers — the same plain-bytes approach `SetRebalanceTaskState` uses for its
bare UUID, rather than a protobuf message for a single number.

- `AutoRebalancePeriodKey` holds the sweep period **in minutes**. Zero means automatic
  rebalancing is disabled.
- `LastAutoRebalanceKey` holds `HLCTimestamp.asLong` for the last *completed* sweep.

`RebalancingServiceState` gains encode/decode helpers for each:

```scala
def encodeAutoRebalancePeriod(minutes: Long): Array[Byte]
def decodeAutoRebalancePeriod(bytes: Array[Byte]): Duration   // Duration(minutes, MINUTES)
def encodeLastAutoRebalance(ts: HLCTimestamp): Array[Byte]
def decodeLastAutoRebalance(bytes: Array[Byte]): HLCTimestamp
```

### Defaults

`RebalancingDurableService.initialServiceState` writes all three keys, so the bootstrap
process creates a fully-populated state object:

```scala
def initialServiceState: Map[Key, Array[Byte]] =
  Map(
    RebalancingServiceState.ActiveTasksKey ->
      RebalancingServiceState.encodeActiveTasks(Nil),
    RebalancingServiceState.AutoRebalancePeriodKey ->
      RebalancingServiceState.encodeAutoRebalancePeriod(DefaultAutoRebalancePeriodMinutes),
    RebalancingServiceState.LastAutoRebalanceKey ->
      RebalancingServiceState.encodeLastAutoRebalance(HLCTimestamp.Zero)
  )
```

with `DefaultAutoRebalancePeriodMinutes = 480` (8 hours) on the companion object.

A `LastAutoRebalanceKey` of `HLCTimestamp.Zero` means a freshly bootstrapped system
sweeps on its first `reconcile()`. This is intentional: the first sweep on an empty or
balanced system produces empty plans and costs a handful of reads.

Note that `DefaultAutoRebalancePeriodMinutes` is distinct from the existing
`DefaultPollPeriod` (2 minutes). The latter is the `reconcile()` polling interval and an
in-memory test seam; it is unchanged by this work. The two must not be conflated.

## Sweep Behavior

### Where it runs

The sweep is folded into the existing `reconcile()` rather than given its own timer.
`reconcile()` already reads the very state object that holds the period and the last-sweep
timestamp, every `pollPeriod` (2 minutes), via
`BackgroundTaskManager.scheduleNonConcurrentPollingTask`. Decoding two more keys from that
same read costs nothing, needs no second timer to manage and cancel, and gives config
freshness far better than the "poll every hour" the TODO asked for. The
`scheduleNonConcurrentPollingTask` wrapper already skips a tick whose predecessor has not
finished, which satisfies the TODO's "protect against pileups".

### Due check

After the existing active-task startup logic in `reconcile()`:

```
period = decode(AutoRebalancePeriodKey)
if period > 0:
    lastSweep = decode(LastAutoRebalanceKey)
    if (HLCTimestamp.now - lastSweep) >= period:
        sweep()
```

`HLCTimestamp.-` already yields a `Duration`, so the comparison is direct.

When the period is zero the service does nothing but keeps reading the value on every
poll, so re-enabling takes effect within one poll interval even if the nudge is lost.

A `lastSweep` in the future (clock skew, or an HLC advanced by a peer) yields a negative
difference and simply defers the sweep until wall time catches up. This is the behavior
HLC timestamps exist to provide and needs no special handling.

### The sweep itself

1. `client.listStorageDeviceSets()` to enumerate all sets.
2. For each set, read `client.getStorageDeviceSetState(setId)` and keep only `level == 0`.
   This filter is mandatory: `State.getStateForRebalancePlanning` throws
   `IllegalArgumentException` for non-level-0 sets.
3. **Sequentially** — folding over futures, not fanning out — call the rebalance entry
   point for each surviving set. Sequential enrollment bounds the burst of reads and
   revision-checked writes to the shared `ActiveTasksKey`, which is where concurrent
   enrollment would collide.
4. Write `LastAutoRebalanceKey = HLCTimestamp.now`.

All three "skip" conditions in the TODO are pre-existing behavior of
`rebalanceStorageDeviceSet` and require no new code: it no-ops when the set has non-empty
`pendingTransfers`, when the set already appears in `ActiveTasks`, and when
`Plan.computePlan` returns `Nil`. The sweep only drives the loop.

To avoid reading each set's state twice (once for the level filter, once inside the
entry point), the companion object is split:

```scala
// public, unchanged signature: reads the set state, then delegates
def rebalanceStorageDeviceSet(client: AspenClient, setId: StorageDeviceSetId): Future[Unit]

// private: takes an already-read set state
private def rebalanceStorageDeviceSet(client: AspenClient,
                                      setId: StorageDeviceSetId,
                                      setState: StorageDeviceSetState): Future[Unit]
```

The public entry point keeps its current behavior exactly, including surfacing the
`IllegalArgumentException` when a user passes a non-level-0 set to the `rebalance` command.
The level filter lives in the sweep, not in the shared path, so the CLI's error is not
silently downgraded to a no-op.

### Timestamp semantics

Three rules, stated explicitly because they determine how the feature behaves under
administrator action:

1. **`lastSweep` is written only by a completed sweep.** Changing the period never touches
   it. Shortening 8h to 1h when the last sweep was 3h ago sweeps on the next poll;
   lengthening 8h to 24h defers until 24h after the last sweep; re-enabling after a long
   disable sweeps promptly. One rule produces the expected behavior in all three cases.

2. **`lastSweep` advances even when individual sets fail.** Each set's failure is recovered
   and logged so that one bad set cannot abort the sweep, and the timestamp is written
   regardless of how many sets failed. Without this, a persistently failing set would turn
   the 2-minute poll into a retry hot loop against the whole cluster.

3. **Only one sweep runs at a time.** `scheduleNonConcurrentPollingTask` suppresses
   overlapping *timer* ticks, but `receiveMessage` calls `reconcile()` directly and can
   overlap with a timer-driven one. A `sweeping` flag, read and written under the service's
   existing `synchronized` discipline, guards the sweep body.

The `lastSweep` write is a `client.transactUntilSuccessful` touching only
`LastAutoRebalanceKey`, with a `KeyValueUpdate.KeyRevision` requirement on that key. It does
not touch `ActiveTasksKey`, so it does not contend with concurrent enrollment.

### Shutdown

No new `ScheduledTask` is created, so `shutdown()` needs no new `cancel()` call. The
existing `stopped` check at the top of `reconcile()` already prevents a sweep from starting
after shutdown; the sweep body re-checks `stopped` between sets so an in-flight sweep stops
promptly.

## Nudge Message

A third variant on the existing sealed trait:

```scala
case object AutoRebalancePeriodChanged extends RebalancingMessage
```

It carries no payload — the handler calls `reconcile()`, which re-reads the authoritative
value from the state object. Sending the new value in the message would create a second
source of truth for no benefit.

Required changes:

- `codec.proto`: `message AutoRebalancePeriodChanged {}` plus a third entry in the
  `RebalancingMessage` oneof (field number 3).
- `RebalancingMessage.encode` / `decode`: one case each.
- `RebalancingDurableService.receiveMessage`: one case calling `reconcile()`.

Delivery is best-effort, consistent with the rest of the service: the message may be
dropped if no host currently holds the service lease. The 2-minute poll remains the
correctness guarantee.

## API

Two methods on the `RebalancingDurableService` companion object, beside the existing
`rebalanceStorageDeviceSet`. Rebalancing operations already live there rather than on
`AspenClient`, and the cmdline already calls the companion directly.

```scala
def getAutoRebalancePeriod(client: AspenClient): Future[Duration]
def setAutoRebalancePeriod(client: AspenClient, period: Duration): Future[Unit]
```

`Duration` in minutes, zero meaning disabled — the same representation as the stored
value, so there is no conversion layer between the API and the state object.

`getAutoRebalancePeriod` reads the state object through the existing private
`readServiceStatePointer` and decodes `AutoRebalancePeriodKey`.

`setAutoRebalancePeriod` runs a `transactUntilSuccessful` with a `KeyRevision` requirement
on `AutoRebalancePeriodKey` and an `Insert` of the new value, then sends the
`AutoRebalancePeriodChanged` nudge. The nudge is best-effort: a failed send does not fail
the call.

The display path also needs the last-sweep timestamp (see below). Rather than a second
round trip, the CLI reads the state object once through a companion helper that returns
both values:

```scala
private[aspen] def getAutoRebalanceStatus(client: AspenClient): Future[(Duration, HLCTimestamp)]
```

`getAutoRebalancePeriod` is implemented in terms of it.

## Cmdline

### Usage

```
aspen system-rebalance-period <bootstrap-config-file>
aspen system-rebalance-period <bootstrap-config-file> 4 hours
aspen system-rebalance-period <bootstrap-config-file> 20 minutes
aspen system-rebalance-period <bootstrap-config-file> 7 days
aspen system-rebalance-period <bootstrap-config-file> disabled
```

The period is two tokens — a count and a unit — rather than one quoted string, so no
shell quoting is required and each argument validates independently. `disabled` is a
single-token alternative form, which is why the unit argument must be optional.

### Parser

- `Args` gains `rebalancePeriod: Option[String] = None` and
  `rebalancePeriodUnit: Option[String] = None`.
- A `cmd("system-rebalance-period")` block with the standard `<bootstrap-config-file>`
  first argument, followed by two `.optional()` string arguments.
- Dispatch: `case "system-rebalance-period" => systemRebalancePeriod(bootstrapConfigPath, cfg.rebalancePeriod, cfg.rebalancePeriodUnit)`.

### Parsing and formatting helpers

Both are pure and unit-testable, mirroring the existing `formatBytes`:

```scala
private[cmdline] def parseRebalancePeriod(period: Option[String],
                                          unit: Option[String]): Either[String, Option[Duration]]

private[cmdline] def formatRebalancePeriod(d: Duration): String
```

`parseRebalancePeriod` returns `Right(None)` for the display form (no period argument
given) and `Right(Some(d))` for the set form, so `checkConfig` can call it unconditionally
and only the `Left` cases are errors:

| Input | Result |
|---|---|
| `(None, None)` | `Right(None)` — display form |
| `(None, Some(_))` | unreachable: scopt fills positional arguments in order |
| `(Some("disabled"), None)` | `Right(Some(Duration.Zero))` |
| `(Some("4"), Some("hours"))` | `Right(Some(Duration(240, MINUTES)))` |
| units | `minute`, `minutes`, `hour`, `hours`, `day`, `days` |
| `(Some("4"), None)` | `Left("a unit is required: minutes, hours, or days")` |
| `(Some("4"), Some("weeks"))` | `Left("unknown unit 'weeks': expected minutes, hours, or days")` |
| `(Some("-1"), Some("hours"))` | `Left("period must not be negative")` |
| `(Some("x"), Some("hours"))` | `Left("period must be a whole number")` |
| `(Some("disabled"), Some(_))` | `Left("'disabled' takes no unit")` |

A count of `0` with a unit is accepted and is equivalent to `disabled`.

Validation is wired into the existing `checkConfig` block (Main.scala:677), keyed on
`c.mode == "system-rebalance-period"`, so malformed input produces a scopt usage error
before any client or network is constructed.

`formatRebalancePeriod` selects the largest unit that divides the value evenly, pluralizes
correctly, and renders zero as `disabled`:

| Duration | Rendered |
|---|---|
| `0` | `disabled` |
| `1 minute` | `1 minute` |
| `20 minutes` | `20 minutes` |
| `240 minutes` | `4 hours` |
| `60 minutes` | `1 hour` |
| `10080 minutes` | `7 days` |
| `90 minutes` | `90 minutes` (does not divide evenly into hours) |

### Handler

Standard shape for the file: `configureLogging()` → `createAmoebaClient` →
`network.startIoThread(client)` → build the future → `awaitAndReport`. The centralized
`drainAndShutdown()` already gives the nudge time to leave the process, so the handler adds
no drain logic of its own.

Display form output:

```
Automatic rebalancing period: 8 hours
Last sweep:                   2026-09-01 14:03:22
Next sweep due:               2026-09-01 22:03:22
```

When disabled:

```
Automatic rebalancing period: disabled
Last sweep:                   2026-09-01 14:03:22
```

When no sweep has run yet (`HLCTimestamp.Zero`), `Last sweep` renders as `never` and
`Next sweep due` as `next poll`.

Set form output:

```
Automatic rebalancing period set to 4 hours
```

## Testing

Test-driven throughout: pure helpers first, then encoding, then the service behavior.

### `MainSuite` — pure helpers

- `parseRebalancePeriod` across every row of the table above, including each unit in both
  singular and plural form.
- `formatRebalancePeriod` across every row of its table: unit selection, pluralization,
  `disabled`, and a value that does not divide evenly.

### `RebalancingServiceStateSuite`

- Round-trip `encodeAutoRebalancePeriod` / `decodeAutoRebalancePeriod`, including zero.
- Round-trip `encodeLastAutoRebalance` / `decodeLastAutoRebalance`, including
  `HLCTimestamp.Zero`.
- `initialServiceState` contains all three keys, with the period defaulting to 480 minutes
  and the last-sweep timestamp to zero.

### `RebalancingMessageSuite`

- Round-trip `AutoRebalancePeriodChanged` through `encode` / `decode`.

### `RebalancingServiceSuite` — integration

- A period of zero performs no sweep even when a set has a non-empty plan.
- A stale `lastSweep` with a non-zero period sweeps, enrolls the unbalanced set, and
  advances `lastSweep`.
- A fresh `lastSweep` does not sweep.
- A level-1 set is skipped rather than raising `IllegalArgumentException`.
- A set already listed in `ActiveTasks`, and a set with non-empty `pendingTransfers`, are
  both skipped.
- A set whose planning fails does not abort the sweep: later sets are still processed and
  `lastSweep` still advances.
- `setAutoRebalancePeriod` persists the value and the running service picks it up.

The existing `@volatile var pollPeriod` seam already lets tests drive `reconcile()` on a
short interval, and tests can write an old `LastAutoRebalanceKey` directly to make a sweep
due. No new test seam is required.

## Files Touched

| File | Change |
|---|---|
| `common/rebalancing/RebalancingServiceState.scala` | Two new keys plus encode/decode helpers |
| `common/rebalancing/RebalancingDurableService.scala` | Defaults in `initialServiceState`, sweep in `reconcile()`, `sweeping` guard, new message case, `get`/`set` API, split entry point |
| `common/rebalancing/RebalancingMessage.scala` | `AutoRebalancePeriodChanged` variant, encode/decode |
| `protobuf/codec.proto` | `AutoRebalancePeriodChanged` message, third oneof entry |
| `cmdline/Main.scala` | `Args` fields, `cmd` block, `checkConfig` case, dispatch, handler, two pure helpers |
| `test/.../cmdline/MainSuite.scala` | Parse and format helper tests |
| `test/.../rebalancing/RebalancingServiceStateSuite.scala` | Encoding and default tests |
| `test/.../rebalancing/RebalancingMessageSuite.scala` | Message round-trip test |
| `test/.../rebalancing/RebalancingServiceSuite.scala` | Sweep integration tests |

`server/store/Bootstrap.scala` needs no edit: it already calls
`RebalancingDurableService.initialServiceState`.
