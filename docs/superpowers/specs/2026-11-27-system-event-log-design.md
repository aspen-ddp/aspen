# System Event Log — Design

Date: 2026-11-27. Source: the EventLogging item in `Todo.txt`, refined in conversation.

## Intent

Aspen needs a system-wide, durable, time-ordered log of events that any client or host can write to in a
transaction, that is stored compactly in Aspen objects, that live code can react to (observers), and that
administrators can read back (a public reader and a `cmdline` command; the command was next on the agenda and is
folded into this project).

## Goals

- `Event`/`EventLogEntry` types, factories through the existing `RegisteredTypeFactory`/`TypeRegistry` system.
- An `EventLogDurableService` (a `DurableService`, modelled on `RebalancingDurableService`) that moves entries from an
  `incomingEvents` KV TKVL into compressed log DataObjects indexed by a `loggedEvents` DataObject TKVL, and passes each
  logged event to registered observers.
- A default system event log registered by `Bootstrap`, allocating from the bootstrap pool.
- `AspenClient.prepareSystemEventLogEntry`, `logSystemEvent`, `setSystemLogAllocator`, `readSystemEvents`.
- `cmdline` `event-log` command, plus commands to list, add and remove observers.
- Observer setters on `AspenClient`, with a reload message so changes apply without a service restart.

## Non-goals

- Exactly-once logging. **Duplicate log entries are allowed** (see Pin rule below).
- Reliable observer delivery. **Observers are best-effort**: an event whose move commits but whose host crashes, or
  whose service moves to another host, before observers are called is never observed. The scaladoc of
  `EventObserver`, `EventObserverFactory` and the service must say so plainly.
- Retention, truncation or deletion of old log objects.
- Optimizing late (out-of-order) events into old log objects beyond correctness.

## Anti-goals

- Writing every event twice into large objects on every poll (full-object rewrite per batch). This is what the tail
  scheme exists to avoid.
- A reader that silently omits committed events because it raced compaction or a split. Duplicates in a read are
  acceptable; omissions are not.
- Two code paths for append-capable vs. non-append IDAs beyond the single tail-write primitive.

## Data types

- `EventLogEntry(timestamp: HLCTimestamp, typeUUID: UUID, data: Array[Byte])`. **Change from the Todo**: the
  timestamp is an `HLCTimestamp` (8 bytes, NTP-compatible) rather than raw nanoseconds since the epoch, because the
  root CLAUDE.md requires HLC for cross-host time comparison and the log is ordered by time across hosts. Encoded with
  protobuf (`codec.proto`) for the `incomingEvents` value.
- `trait Event`: `timestamp: HLCTimestamp`, `typeUUID: UUID`, and `def describe: String` (default: the data's byte
  count). `describe` is an addition for the `cmdline` output.
- `trait EventFactory extends RegisteredTypeFactory`: `def createEvent(e: EventLogEntry): Event`.
- `trait EventObserver`: `observedTypes: Option[Set[UUID]]` (None = all), `def observe(e: Event): Unit`.
- `trait EventObserverFactory extends RegisteredTypeFactory`:
  `def createEventObserver(typeUUID: UUID, configData: Array[Byte]): EventObserver`.
- `DroppedEvent`: a built-in event type with a fixed type UUID and a built-in factory. Data (protobuf): the dropped
  event's type UUID and a reason (enum; initially `ExceededMaximumLogEntrySize`). Its timestamp is the dropped event's.

## Service state object (KV)

| Key | Value |
|---|---|
| incomingEvents | encoded `Root` of a KV TKVL; key = random UUID, value = encoded `EventLogEntry` |
| loggedEvents | encoded `Root` of a DataObject TKVL; key = 8-byte HLC of the smallest entry in a log object, value = that log object's pointer |
| typeIds | mapping type UUID -> 2-byte internal id (protobuf) |
| observers | list of (observer factory type UUID, config bytes) (protobuf) |
| logAllocator | `ObjectAllocatorId` (17 bytes) |
| tails | the ordered list of tail object pointers and which one is current (protobuf) |

**Change from the Todo**: `logObjectAllocatorUUID` becomes an `ObjectAllocatorId`, the codebase's typed allocator
identity (pool or group); a bare UUID cannot say which. `setSystemLogAllocator` takes an `ObjectAllocatorId` too.

## Storage layout: approaches

Considered:

1. **Tail objects for every IDA, appending within a tail where the IDA allows it** — *chosen*. One code path; the
   permanent full objects are sorted and compressed as one zstd frame (best ratio). Cost: each event is written twice
   (tail, then full object), bounded and into small objects.
2. Separate paths by IDA: append-capable IDAs append zstd frames straight onto the open full object, others use tails.
   Fewer bytes written on append IDAs, but two paths to build and test, and permanent objects of many small frames
   compress worse unless recompacted (which reintroduces the double write).
3. No tails; rewrite the open object per batch on non-append IDAs. Rejected: that per-batch large rewrite is the cost
   the tails remove.

### Chosen scheme

- **Full log objects**: the `loggedEvents` tree's values. Content = one zstd frame (zstd-jni, already a dependency)
  over a list of logged records sorted by timestamp. Logged record = timestamp, 2-byte type id, data (exact framing:
  builder's call; protobuf length-delimited is fine).
- **Size limits**: target full-object size = `min(2 MiB, allocator.maxObjectSize)`. 2 MiB is soft; the allocator's
  `maxObjectSize`, when defined, is a **hard cap** the written (compressed) object never exceeds.
- **Tails**: 10 tail DataObjects (count is a constant), each targeting about a tenth of the full size, pointers in the
  state's `tails` key, allocated from the log allocator. Each batch goes into the current tail:
  - if the tail object's pool IDA is Replication or RandomAccessReedSolomon: append the batch as a new zstd frame
    (concatenated frames decode to the concatenated content) via `Transaction.append`;
  - otherwise: overwrite the tail with its old content plus the batch (whole tail rewritten; it is small).
  - Append support is decided by the pool the tail object actually lives in (read from its pointer), since a group
    allocator may span pools with differing IDAs.
  - When the current tail reaches its target, the next tail becomes current.
- **Compaction**: when the tails together reach the full-object target (or no tail can take the next batch), one
  transaction decodes and sorts all tail records, allocates a new full object (from the log allocator) holding them,
  inserts it into `loggedEvents` (this also satisfies "every allocation stores its pointer"), and empties every tail.
  If the records would exceed the hard cap after compression, compaction writes several full objects.
- **Late events**: an event whose timestamp is at or after the start of the oldest record held in the tails, or after
  the last full object's range, simply goes into a tail; compaction sorts it. An event older than that belongs to the
  full object whose key range covers it: that object is rewritten with the event merged in, and split into two
  objects (new one allocated and inserted into `loggedEvents` in the same transaction) if it would exceed the
  target. Not optimized; correctness only.
- **Oversized events**: an entry whose logged record cannot fit in a single log object under the hard cap (checked
  against zstd's compress bound, so the check is conservative and the write never overshoots) is not logged; a
  `DroppedEvent` (same timestamp, the dropped type UUID, reason `ExceededMaximumLogEntrySize`) is logged in its place,
  in the same transaction that deletes the incoming entry. Note: an entry larger than the `incomingEvents` tree's KV
  node limit cannot be inserted at all; `prepareSystemEventLogEntry` fails for it (normal TKVL size error). The
  `DroppedEvent` path covers events that fit `incomingEvents` but not a log object.

## Service behaviour

- **Startup**: read the state object; load `typeIds`; create observers via the `TypeRegistry` from `observers`. An
  observer whose factory is not registered on this host is logged and skipped.
- **Observer reload**: a `ReloadObservers` service message makes the running instance re-read `observers` and rebuild
  its observer set (decided by the user). Since the message is best-effort, each poll also compares the `observers`
  key's revision with the one last loaded, and reloads when it differs, so a lost message only delays the change.
- **Poll**: a non-concurrent polling task (as `RebalancingDurableService`) plus a nudge `ServiceMessage` that
  triggers an immediate scan. Each scan walks `incomingEvents` and moves entries in bounded batches (bounded by bytes
  and count so a transaction stays reasonably sized; bounds are the builder's call). One transaction per batch:
  - write the batch to the current tail (or compact first / rewrite an old full object for late events);
  - delete the moved incoming keys at their read `KeyRevision` (this is the transaction's pin);
  - if the batch introduced new type UUIDs, assign next-highest ids and update `typeIds` (revision-checked) in the same
    transaction, so no log object ever names an id the map lacks. Exhausting 65535 ids fails loudly (log + stop
    moving), it is not expected to happen.
  - update `tails` if the current tail changed.
- **After each batch commits**: for each moved entry, create an `Event` via its factory (an entry with no registered
  factory is skipped for observation, still logged) and call every observer whose `observedTypes` is None or contains
  the type. Observer exceptions are caught and logged. Best-effort, per Non-goals.
- **Allocator changes**: each poll re-reads `logAllocator` (and the `loggedEvents` root's node allocator comes from the
  root itself). New full objects use the current allocator. Tails allocated from an old allocator are replaced at the
  next compaction (new tails allocated, old ones refcount-decremented, in the compaction transaction).
- **Lease handoff**: two instances may briefly overlap after a lease moves. Every move transaction is revision-checked
  on the incoming keys, the tail and the state, so at most one commits per entry; the loser retries against fresh
  state.

## Bootstrap

`Bootstrap` registers the service as an unclaimed `ServiceEntry` in the services tree, as it does the rebalancing and
system-task services, with a fixed `ServiceTypeUUID` and `ServiceUUID`. It allocates the state object, an empty
`incomingEvents` KV tree, an empty `loggedEvents` DataObject tree (Bootstrap currently only builds KV-object roots;
a DataObject-tree variant is needed), the 10 empty tail objects, empty `typeIds`/`observers`, and
`logAllocator = PoolObjectAllocatorId(Radicle.poolId)`.

## Client API (`AspenClient`)

- `prepareSystemEventLogEntry(e: EventLogEntry)(using Transaction): Future[Unit]` — inserts `e` under a fresh random
  UUID key (`requireDoesNotExist`) into the system log's `incomingEvents`. Completes when the transaction is ready to
  commit. Does not nudge; such entries are picked up by the next poll (documented).
- `logSystemEvent(e: EventLogEntry): Future[Unit]` — one transaction around the above, commit, then a best-effort
  nudge. **No automatic retry on failure**; the scaladoc says so.
- **Pin rule**: a `DoesNotExist` insert is not a pin, so `logSystemEvent`'s transaction is marked
  `Transaction.replaySafe("event log entries may be duplicated")`. A replay that lands after the service has moved
  and deleted the entry re-inserts it and produces a duplicate log entry; duplicates are explicitly allowed.
  Transactions built with `prepareSystemEventLogEntry` normally carry the caller's own pin; when they do not, the
  caller marks `replaySafe` the same way (documented).
- `setSystemLogAllocator(id: ObjectAllocatorId): Future[Unit]` — verifies the allocator exists (`getAllocator`), then in
  one transaction updates the `loggedEvents` root's node allocator and the state's `logAllocator`.
- `addSystemEventObserver(factoryType: UUID, config: Array[Byte]): Future[Unit]` and
  `removeSystemEventObserver(factoryType: UUID, config: Array[Byte]): Future[Unit]` — revision-checked
  update of the `observers` key (adding an existing pair or removing an absent one is a no-op), then a best-effort
  `ReloadObservers` nudge. Adding verifies that the factory type is registered on the calling client, so typos fail
  early.
- `readSystemEvents(from: HLCTimestamp, to: Option[HLCTimestamp] = None): Future[...]` — returns raw `EventLogEntry`s
  in timestamp order (duplicates possible), type ids expanded to UUIDs. Plus a helper turning an entry into an `Event`
  when a factory is registered. Return shape (a `Seq`, or a streaming/iterator form for large ranges) is the
  builder's call; large ranges must not require holding the whole log in memory if a streaming form is cheap.
  - **No-omission requirement**: a read must never omit an event committed before the read began. The reader reads the
    state object (tails + `typeIds`) and the tails' content **before** the `loggedEvents` tree, so a compaction that
    commits between them produces a duplicate, never a gap. A late-event split (records moving from one full object
    to a newly allocated one) must likewise not cause a gap; the mechanism (e.g. re-reading the tree when an object's
    records stop short of the next key, or storing each object's covered range in its header) is the builder's call.

## `cmdline`

`event-log [--since <time>] [--until <time>] [--type <uuid>] [--limit N]`, following the `rebalance` /
`system-rebalance-period` pattern in `cmdline/Main.scala`. One line per entry: timestamp, type name or UUID,
`describe`. `DroppedEvent` prints the dropped type and the reason; a type without a registered factory prints its UUID
and byte count:

```
2026-11-27T20:14:03.112Z  a1b2…  StoreTransferComplete store=… from=… to=…
2026-11-27T20:14:05.870Z  DroppedEvent  type=c3d4… reason=exceeded maximum log entry size
2026-11-27T20:14:06.001Z  e5f6… (no factory, 312 bytes)
```

Also `event-log-observers` (list), `event-log-add-observer <factory-uuid> [--config-file <path>]` and
`event-log-remove-observer <factory-uuid> [--config-file <path>]`. Exact command names are the builder's call.

## Constraints

- Root CLAUDE.md invariants: pin rule, allocations stored in another object, crash-only, HLC for time, collisions are
  expensive (the service is the single writer of `loggedEvents`, tails and `typeIds`; clients only touch
  `incomingEvents` with random keys).
- Tests: deterministic, fast, no flakes (`CLAUDE.local.md`). Poll period is a per-client test seam as in
  `RebalancingDurableService`.
- Update module CLAUDE.md guides for what changes (client API in `client/CLAUDE.md`; Bootstrap/services where
  documented), and add one for the new package.

## Calls made by Claude (correct if wrong)

- Package: `common/eventlog` alongside `common/rebalancing`.
- HLC timestamps and `ObjectAllocatorId` instead of raw nanos / bare UUIDs (above).
- Default poll period 5 s plus nudge; `logSystemEvent` nudges, `prepareSystemEventLogEntry` does not.
- Tail count 10, tail target = full target / 10.
- Tails migrate to a new allocator at the next compaction.
- `Event.describe` added for display.

## Clarifications from the builder check (Claude's calls)

- **Ranges and keys**: full object *i* covers `[key_i, key_{i+1})`; the first object also covers everything before
  its key. The state holds `tailFloor`, the largest timestamp held by any full object (set at compaction). An event
  with timestamp `>= tailFloor` goes to a tail; an older one is merged into its covering full object. A late event
  that sorts before the first object's key rekeys that object: delete the old key and insert the new one in one
  `prepareUpdates` batch. A split never divides records with equal timestamps (keys stay unique); if no such split
  point exists, the object stays oversized up to the hard cap, and past it the event is dropped as oversized.
- **Sizes**: the tail target is soft; the hard cap (`allocator.maxObjectSize`) applies to each written object,
  evaluated against the pool the object is actually in. A batch that would push the current tail past the cap moves on
  to the next tail; if no tail can take it, compaction runs first. The oversized-event check is against the
  full-object hard cap, not the tail target.
- **Reader retries**: if a tail or log object the reader holds a pointer to has been deleted or replaced mid-read
  (allocator-change compaction, split), the read restarts from the state read, with a bounded number of restarts,
  and fails after that.
- **Client collisions**: `incomingEvents` is a KV tree; writes carry per-key requirements (no node-level
  serialization), so writers with random keys do not collide with each other except around node splits and the
  slice-size limit. `logSystemEvent` still does not retry (per the Todo); a failure surfaces to the caller.
- **Several entries per transaction**: provide `prepareSystemEventLogEntries(es: Seq[EventLogEntry])` (one
  `prepareUpdates` batch). `prepareSystemEventLogEntry` is the single-entry form, and the scaladoc says to use the
  batched form rather than calling it twice in one transaction.

## Left to the builder

Exact logged-record framing, batch bounds, reader return shape, split no-gap mechanism, protobuf message names,
test structure.
