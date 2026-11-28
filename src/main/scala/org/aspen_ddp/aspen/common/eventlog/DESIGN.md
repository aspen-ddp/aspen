# System event log

The system event log is an append-mostly, time-ordered log of typed events kept in Aspen objects. Any client can
log an event (optionally inside its own transaction); a durable service, `EventLogDurableService`, moves logged
events into compact, sorted log objects; readers read a timestamp range; observers are called as events are moved.
This document describes the package `org.aspen_ddp.aspen.common.eventlog` as built, and calls out the behaviors
that surprise: duplicates, dropped events, best-effort observers and the storage optimizations.

Related: `docs/superpowers/specs/2026-11-27-system-event-log-design.md` (the design spec; where it and the code
differ, this document describes the code and says so).

## 1. Data types

- `EventLogEntry(timestamp: HLCTimestamp, typeUUID, data: Array[Byte])`: what clients log. `data` is opaque to the
  log. `HLCTimestamp` is wall-time milliseconds in the high 48 bits and a 16-bit logical counter in the low 16
  (`common/HLCTimestamp.scala`); it orders as a signed Long.
- `Event` / `EventFactory`: the decoded form. A factory is a `RegisteredTypeFactory` (found through the client's
  type registry by the entry's type UUID); `createEvent(entry)` yields an `Event` with `timestamp`, `typeUUID` and
  `describe` (empty means "display falls back to the byte count").
- `EventObserver` / `EventObserverFactory`: `observedTypes: Option[Set[UUID]]` (None observes everything) and
  `observe(event)`. The factory builds an observer from its persisted `(factory type UUID, config bytes)`.
- `DroppedEvent(timestamp, droppedType, reason)`: the placeholder logged in place of an entry that cannot be stored
  (section 8). `DropReason` has one case, `ExceededMaximumLogEntrySize`.
- `LoggedRecord(timestamp, typeId: Short, data)`: an entry as held in a log object. The type UUID is replaced by a
  16-bit id (read unsigned) from the state's type table, so log objects do not repeat 16-byte UUIDs.
- `LogKey(ts)`: the key of a full object in `loggedEvents`: the 8 big-endian bytes of `ts.asLong ^ Long.MinValue`.
  `HLCTimestamp` orders as a signed Long but `ByteArrayKeyOrdering` compares unsigned bytes; flipping the sign bit
  makes the two orders agree. Never build these keys by hand.
- `FullObject` / `FullObjectHeader(filedKey, upperBound: Option[HLCTimestamp])`: the layout of a full log object:
  a fixed 17-byte header (1 presence byte for the bound, 8 bytes `filedKey`, 8 bytes bound) followed by one zstd
  frame of length-delimited protobuf `LoggedRecord`s. `FullObject.overhead` is the header size and is added to
  every size check.
- `EventLogMessage` (`Nudge`, `ReloadObservers`): the payloads of service messages sent to the service.

## 2. Service state

The service's state is one KeyValue object (a `ServiceEntry` in the Radicle services tree, key
`EventLogDurableService.ServiceUUID`, names it; Bootstrap creates it with every key present, so nobody handles an
absent one). Keys (`EventLogState`):

| Key | Value |
|---|---|
| `IncomingKey` | Root of `incomingEvents`, a KV-object TKVL tree of entries logged but not yet moved |
| `LoggedKey` | Root of `loggedEvents`, a DataObject TKVL tree: `LogKey(filedKey)` -> pointer of a full object |
| `TypeIdsKey` | event type UUID -> 16-bit type id (ids are assigned, never reused; at most 65536 types) |
| `ObserversKey` | the observer list, `(factory type, config bytes)` each |
| `AllocatorKey` | the `ObjectAllocatorId` that new log objects are allocated from |
| `TailsKey` | `Tails(pointers, current, allocator)`: `TailCount` (10) tail objects, the index being written, and the allocator they came from |
| `TailFloorKey` | `tailFloor`: the largest timestamp held by any full object |

Type ids are assigned in increasing order and never reused. Once all 65536 are taken, a batch that needs a new one
(an event type never logged before) cannot be planned: the service logs an error and moves none of it. The entry
stays in `incomingEvents`, and batches are read in key order, so every batch that includes it fails the same way:
once fewer than `MaxBatchEntries` entries precede its key, moving stops for every type. `incomingEvents` then grows
without bound while clients keep logging (they are not told), and readers still return the waiting entries from it.

`loggedEvents` always has a root node: Bootstrap allocates an empty one. A rootless tree would make the first
insert write the root into the state object in the same transaction as the move's own state update, and a
transaction may update an object once.

## 3. Incoming, tails, full objects

Entries only ever move forward along `incoming -> tails -> full objects`.

1. A client inserts the entry under a random UUID key in `incomingEvents` (`requireDoesNotExist`), either in its own
   transaction (`prepareSystemEventLogEntry`) or in a transaction of its own (`logSystemEvent`). After the commit
   the client sends the service a best-effort `Nudge`.
2. The service reads up to one batch (at most `MaxBatchEntries` = 1000 entries; `MaxBatchBytes` = 1 MiB is a soft
   bound: reading stops once the batch reaches it, so the entry that crosses it is included) and
   moves it in one transaction: the batch's records go into the tails (and any full-object work below), and the
   moved incoming keys are deleted at their read `KeyRevision` (that delete is the move's pin). The transaction also
   requires the revisions of `TailsKey` and `TailFloorKey`, so two service instances overlapping after a lease
   handoff never both move the same entry: the loser aborts and its next batch re-reads. Scans run one at a time; a
   nudge arriving during a scan makes it scan again. A scan gives up after `MaxConsecutiveFailures` failed
   transactions in a row, until the next poll (`DefaultPollPeriod`, 15 s) or nudge. A failure that repeats on
   every attempt (a planner exception on the batch at the front of `incomingEvents`, say) therefore stalls all moving:
   each poll retries the same batch and fails again, and `incomingEvents` grows until the cause is fixed.
3. Tails are `TailCount` data objects holding zstd frames of records. One tail is "current"; a batch is appended to
   it as a single new frame, moving to the next tail when the current one is full. A tail's target size is the full
   object target divided by `TailCount`. Tails hold only records strictly after `tailFloor`.
4. Full objects (target `DefaultTargetObjectSize`, 2 MiB, capped by the allocator's `maxObjectSize`) hold sorted
   records for a timestamp range and are listed in `loggedEvents` under their `filedKey`.

Undecodable incoming values are logged at error and deleted with the batch.

## 4. Compaction

When no tail has room for a record of the batch, the same transaction compacts. The planner (`LogPlanner`, pure:
inputs `PlanInput`, output `LogPlan` of `LogStep`s) takes every tail's records plus the batch, sorts them, and packs
them into new full objects split only at timestamp boundaries (a run of equal timestamps is never split, except by
the hard cap). Each new object gets a header with its `filedKey` (its first timestamp) and the next object's key as
`upperBound` (None for the last). `LogWriter` allocates the objects, inserts them into `loggedEvents` in one
`prepareUpdates` batch (each tree node updated once), empties every tail (overwriting it with the frame of any
records the plan appends after the compaction), and `TailFloorKey` advances to the largest timestamp compacted.
Every size check uses zstd's compress bound, so nothing written exceeds its cap.

Each tail is overwritten at the revision read, so a concurrent append aborts the compaction.

Late means timestamp `<= tailFloor` (not `<`). Tails only ever hold records strictly after `tailFloor`, so a
compaction never files an object under a key that already exists: every new key is above every existing key, even
when the last full object holds only records at `tailFloor`.

## 5. Late events, splits and rekeys

Events can arrive with old timestamps (a slow client, a replayed transaction). Once full objects exist, a record
at or below `tailFloor` cannot go to a tail. The batch's oldest late record picks the covering full object (the one
filed under the largest key at or below it; the first object when it precedes every key). The service finds it
starting from the tier-0 node containing the timestamp (`DataObjectTKVL.getContainingNode`, then `foreachFrom`),
reads it and rewrites it in the same transaction with every late record of the batch in its range merged in
(`LogStep.RewriteFull`). Late records outside that range wait for a later batch. The rewrite:

- overwrites the object at the revision read;
- **splits** it when it outgrows the target: the first part overwrites the object, later parts are allocated and
  inserted into `loggedEvents`, and the first part's `upperBound` becomes the first new part's key;
- **rekeys** it when a record precedes the first object: its `filedKey` moves to the earlier timestamp, the new key
  is inserted and the old key deleted (when a new part is filed under the old key, its put replaces the old entry);
- is re-written whole (new frame) only here; a full object is otherwise written exactly once.

## 6. The reader

`EventLogReader.read(from, to, limit)` (via `AspenClient.readSystemEvents`) returns entries in `[from, to]` sorted
by timestamp, at most `limit`. It never omits an event logged before the read began, and may return duplicates
(section 7). It exploits the forward-only movement of section 3:

1. read `incomingEvents`, then the state object (tails, type ids), then every tail, then walk `loggedEvents`,
   reading each full object as the walk reaches it. An entry that moves between two reads is seen twice, not
   zero times. Every one of these reads is settled (`AspenClient.settledReader`, `client/internal/CLAUDE.md`): it
   reflects every transaction that committed before it began. That is what makes the order an argument. A read that
   shows a move M applied (an entry gone from `incomingEvents`, tails emptied, a header or tree changed) proves M
   committed before the read completed, so before the next read began, and the next read therefore shows M's
   records where M put them. Without it, a later read of an object in another pool could return the state M
   replaced, from stores that voted commit for M but have not applied it: the reader's client knows M committed only
   if its status cache or another store of that same object says so. Objects of one pool would be safe without it
   (a store applies all its objects of a transaction at once, and read majorities overlap), but once
   `setSystemLogAllocator` points the log at another pool (or a group spanning pools), tails, tree nodes and full
   objects live apart from `incomingEvents` and from each other. The walk of `incomingEvents` resumes after the last
   key it visited when a join removes the next node (the service's deletes cause them), rather than restarting the
   read.
2. the walk starts at the largest key at or below `from` in the tier-0 node containing it (only when that node
   holds none, from the start of the tree). The first key the walk visits must be the key it started from: a rekey
   that deleted it in between would otherwise skip the object covering `from`.
3. each full object's header is checked against the walk: `filedKey` must equal the key it was found under, and
   `upperBound` must be the next key the walk found (the last object has none). A split changes the upper bound and a
   rekey changes the filed key, so either committed between the walk and the read fails the check. So does an object
   read as `InvalidObject` or a record whose type id the state read does not know.
4. a failed check abandons the attempt and starts again from `incomingEvents`, at most `MaxRestarts` (5) times, then
   fails with `IllegalStateException`.

The walk stops past `to`, or once `limit` records timestamped before the next key are in hand.

A settled read waits for the transactions holding write locks in each store's first reply to resolve, so a read of a
busy current tail or of `incomingEvents` under heavy logging takes up to a transaction's resolution longer. The cost
is the reader's alone; the service and loggers are unaffected.

The reader holds test hooks per instance (`testAfterIncomingRead`, `testDuringIncomingWalk`, `testAfterTailsRead`,
`testAfterContainingNode`, `testBeforeFirstObjectRead`, and the `restarts` counter), so instances are not shared
between reads in tests.

## 7. Duplicates

Entries can appear twice in a read result, and the log itself can hold an entry twice. Callers that need
exactly-once must carry their own event identity in the payload. The paths found:

1. **Replayed insert.** `logSystemEvent` and an unpinned `prepareSystemEventLogEntry` transaction are
   `replaySafe` (an incoming insert is no pin). A replay of the insert transaction landing after the service moved
   and deleted its entry inserts it again (its `requireDoesNotExist` now holds, the key being gone), so the event is
   logged a second time.
2. **Reader racing a move.** The reader reads incoming and then the tails: an entry moved from incoming into a tail
   between the two reads is returned from both.
3. **Reader racing a compaction.** The reader reads the tails and later reaches the new full object: records that
   compacted in between are returned from both.
Only (1) writes a duplicate into the log; (2) and (3) are read-time artifacts that vanish on the next read.
A reader racing a split or rekey does not duplicate: its header checks restart the read (section 6).

## 8. Dropped events

An entry that cannot be stored is replaced by a `DroppedEvent` carrying its timestamp, its type UUID and a reason.
The cap is a property of where the log is stored, not of the event:

- A log object is bounded by the log allocator's `maxObjectSize` (the pool's or group's slice limit). The
  `incomingEvents` tree lives in the bootstrap pool, so what it can hold is bounded by that pool's limit. An event
  that fits `incomingEvents` can still exceed the log allocator's cap when that cap is smaller: the same event is
  logged on one allocator and dropped on another.
- An event larger than `incomingEvents` can hold fails at insert (the client's transaction fails), before the
  service sees it.
- `setSystemLogAllocator` to a smaller-cap allocator can turn already-accepted tail records, larger than the new
  cap, into `DroppedEvent`s at the compaction that migrates the tails.
- The `DroppedEvent` replacement is a record like any other and is placed by timestamp whenever it fits. When it
  cannot be placed (its run of equal timestamps already fills an object, or the cap is below one replacement
  record) the record vanishes without trace and `LogWriter` logs a warning. That is the only traceless loss.
- A late record merges into an existing full object (section 5) only while that object still packs under the caps.
  An object that no longer does (its content was written under a larger cap than its pool's or the allocator's
  current one) takes nothing: every late record in its range becomes a `DroppedEvent` (`LogPlanner.planLate`), and
  the replacement cannot merge either, so it too vanishes without trace, with `LogWriter`'s warning.

## 9. Observers and messages

Observers are best-effort. The service delivers each moved event, in timestamp order, after the move transaction
commits and only to the observers loaded when the transaction was read. Therefore:

- an event committed just before a crash or a move of the service to another host is never observed;
- `observe` must be fast and must not block; an exception from one observer is logged and affects neither the
  others nor the log;
- a `Nudge` or `ReloadObservers` message may be lost: polling (every `pollPeriod`) covers both.

The observer set is installed only from a strictly newer write of `ObserversKey` (compared by the value's write
timestamp), so a read that started earlier but finishes later (a poll racing a batch) never replaces a newer set. A
factory that is not registered or throws is logged and skipped. Observers are loaded before the first scan, so the
events the first scan moves are observed.

## 10. The log allocator

`AspenClient.setSystemLogAllocator(id)` writes `AllocatorKey` and the `NodeAllocator` of the `loggedEvents` root in
one transaction (both revision checked; the pin). New full objects, and new tree nodes, come from the new allocator
from the service's next batch. Existing objects stay where they are. The tails move at the next compaction: new
tails are allocated from the new allocator and take the compaction's refill, and the old tails are exclusively
revision-locked (so an append committed since aborts the compaction) and have their refcount decremented, in the
same transaction that stores the new `Tails` in `TailsKey`. Until then, while tails await migration, each tail's
cap is the smaller of its own pool's and the new allocator's.

## 11. Client API

On `AspenClient`:

- `logSystemEvent(entry)`: logs in its own transaction.
- `prepareSystemEventLogEntry(entry)(using tx)` / `prepareSystemEventLogEntries(entries)`: add entries to the
  caller's transaction, committed or not with it. The caller's transaction must be pinned or `replaySafe`.
- `readSystemEvents(from, to, limit)`: the reader. `decodeSystemEvent(entry)`: the entry's `Event`, when its factory
  is registered.
- `addSystemEventObserver` / `removeSystemEventObserver` / `listSystemEventObservers`: edit `ObserversKey` with a
  revision-checked write that retries on conflict, then send `ReloadObservers`. An edit that changes nothing (adding
  an observer already listed, removing one that is not) commits nothing.
- `setSystemLogAllocator(id)`: section 10.

The command line has `event-log` (display a range), `event-log-observers`, `event-log-add-observer` and
`event-log-remove-observer`.

## 12. Storage optimizations

Whether a tail can be appended to is decided by the IDA of the pool it lives in (`LogWriter.load`):

- **Replication and RandomAccessReedSolomon**: a batch is appended to the current tail as one zstd frame
  (`Transaction.append` at the `AppendPosition` read), so the write is proportional to the batch. The frames are
  concatenated; the decoder reads every concatenated frame.
- **Other IDAs (classic ReedSolomon)**: the whole tail is rewritten (existing content plus the new frame).
- **Compaction** reads every tail whole in either case, because its records are needed.
- **Full objects** are written once, as one frame, and re-written only for a late event or a split.
