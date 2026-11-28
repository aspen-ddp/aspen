# System Event Log Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A system-wide durable event log: clients insert events transactionally, a durable service moves them into
compressed time-ordered log objects and notifies best-effort observers, with a public reader and `cmdline` commands.

**Architecture:** New package `org.aspen_ddp.aspen.common.eventlog`. An `EventLogDurableService` (modelled on
`RebalancingDurableService`, registered by `Bootstrap`) drains an `incomingEvents` KV TKVL into 10 small tail
DataObjects (append on Replication/RandomAccessReedSolomon, whole overwrite otherwise), compacts full tails into one
sorted zstd frame per full log object indexed by a `loggedEvents` DataObject TKVL. Placement decisions are pure
functions (`LogPlanner`), executed against a Transaction by `LogWriter`, mirroring the TKVL `plan*` split.

**Tech Stack:** Scala 3, ScalaTest `AsyncFunSuite` (`IntegrationTestSuite`), ScalaPB (`src/main/protobuf/codec.proto`),
zstd-jni (`com.github.luben.zstd`), scopt (`cmdline/Main.scala`).

**Spec:** `docs/superpowers/specs/2026-11-27-system-event-log-design.md` — read it before any task.

## Global Constraints

- Read first: root `CLAUDE.md`, `client/CLAUDE.md`, `client/tkvl/CLAUDE.md`, `client/internal/transaction/CLAUDE.md`,
  `server/CLAUDE.md`. Package root `src/main/scala/org/aspen_ddp/aspen/`; tests mirror it under `src/test/scala/...`.
- Timestamps are `common.HLCTimestamp`; allocators are `client.ObjectAllocatorId` (never bare UUIDs).
- Pin rule: every transaction has a pin or `tx.replaySafe(reason)`. Client insert transactions with no other pin use
  `replaySafe("event log entries may be duplicated")`. Service move transactions are pinned by deleting incoming keys
  at their `KeyRevision` (normal TKVL delete).
- Every allocation's pointer is stored in another object in the same transaction.
- Default poll period **15 s**; tail count **10**; full-object target **`min(2 MiB, maxObjectSize)`**; tail target =
  full target / 10 (soft). The hard cap (`maxObjectSize` of the pool an object lives in, when defined) is never exceeded
  by a written object.
- Duplicates are allowed; omissions by the reader are not. Observers and service messages are best-effort.
- `logSystemEvent` never retries automatically.
- Scala 3 quiet syntax, indented if/then/else. os-lib for files. Tests deterministic and fast; per-client test seams
  (WeakHashMap keyed by `AspenClient`, as `RebalancingDurableService.setTestPollPeriod`) for poll period and target
  object size; never a shared static.
- Run tests: `sbt 'testOnly *SuiteName'` (full runs: 80 s is enough to detect hangs).
- Commit on `main` after each task; message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **HLC key ordering**: `HLCTimestamp` compares as a *signed* Long and current NTP-era values have the top bit set;
   `loggedEvents` keys under `ByteArrayKeyOrdering` (unsigned) must sort identically. Test in Task 2.
2. **Concatenated zstd frames**: `Zstd.decompress(bytes, size)` / `getFrameContentSize` read only the first frame; tails
   hold many. Decoding must handle N frames and an empty tail. Test in Task 2.
3. **Service handoff overlap**: two instances moving the same batch; at most one commits per entry and nothing is lost.
   Test in Task 5.
4. **Reader racing a split/rekey of a full object** (tree read before, object read after): must restart, not omit.
   Test in Task 8.
5. **Small-slice pools** (e.g. `ReedSolomon(5,3,4)` with a small `testPoolMaxSliceSize`): cap below the 2 MiB target and
   below one event; events dropped as `DroppedEvent`, nothing written above the cap. Test in Task 6.

---

## File Structure

`src/main/scala/org/aspen_ddp/aspen/common/eventlog/`:

| File | Responsibility |
|---|---|
| `EventLogEntry.scala` | `EventLogEntry` + protobuf encode/decode |
| `Event.scala` | `Event`, `EventFactory`, `EventObserver`, `EventObserverFactory` |
| `DroppedEvent.scala` | `DroppedEvent`, `DropReason`, its factory |
| `LogKey.scala` | HLC <-> order-preserving 8-byte `Key` |
| `LogRecords.scala` | logged-record framing, zstd frame encode, multi-frame decode, size bound |
| `FullObjectHeader.scala` | header of a full log object (filed key, optional upper bound) |
| `EventLogState.scala` | state-object keys and value codecs (typeIds, observers, tails, tailFloor, allocator) |
| `EventLogMessage.scala` | `Nudge`, `ReloadObservers` service messages |
| `LogPlanner.scala` | pure placement/compaction/late-event/drop planning |
| `LogWriter.scala` | applies a `LogPlan` to a Transaction |
| `EventLogDurableService.scala` | factory object + service class: polling, moving, observers |
| `SystemEventLog.scala` | client-side operations behind the `AspenClient` methods; Bootstrap initial state |
| `EventLogReader.scala` | range read with no-omission restarts |
| `DESIGN.md`, `CLAUDE.md` | package documentation |

Modified: `src/main/protobuf/codec.proto`, `client/AspenClient.scala`, `server/store/Bootstrap.scala`,
`server/TypeFactories.scala`, `common/TypeFactories.scala`, `cmdline/Main.scala`, `client/CLAUDE.md`,
`common/metadata/CLAUDE.md` (if it lists Bootstrap-registered services; else `server/CLAUDE.md`), root `CLAUDE.md`
module table (one row for `common/eventlog/CLAUDE.md`).

Tests under `src/test/scala/org/aspen_ddp/aspen/common/eventlog/`.

---

### Task 1: Event types and DroppedEvent

**Files:**
- Create: `common/eventlog/EventLogEntry.scala`, `Event.scala`, `DroppedEvent.scala`
- Modify: `src/main/protobuf/codec.proto` (append messages), `common/TypeFactories.scala` (register `DroppedEvent`)
- Test: `common/eventlog/EventTypesSuite.scala` (plain `AnyFunSuite`)

**Interfaces:**
- Produces:
  - `final case class EventLogEntry(timestamp: HLCTimestamp, typeUUID: UUID, data: Array[Byte])` with
    `def encode(): Array[Byte]`, `object EventLogEntry { def decode(b: Array[Byte]): EventLogEntry }` (proto
    `EventLogEntry { fixed64 timestamp; UUID typeUUID; bytes data }`, reuse the existing `UUID` proto message).
    Override `equals`/`hashCode` to compare `data` by content.
  - `trait Event { def timestamp: HLCTimestamp; def typeUUID: UUID; def describe: String = "" }` — an empty
    `describe` makes display fall back to the entry's byte count (Task 11).
  - `trait EventFactory extends RegisteredTypeFactory { def createEvent(e: EventLogEntry): Event }`
  - `trait EventObserver { def observedTypes: Option[Set[UUID]]; def observe(e: Event): Unit }`
  - `trait EventObserverFactory extends RegisteredTypeFactory { def createEventObserver(typeUUID: UUID, configData:
    Array[Byte]): EventObserver }`
  - `enum DropReason { case ExceededMaximumLogEntrySize }`;
    `final case class DroppedEvent(timestamp: HLCTimestamp, droppedType: UUID, reason: DropReason) extends Event`
    (`typeUUID = DroppedEvent.TypeUUID`, `describe = s"type=$droppedType reason=exceeded maximum log entry size"`),
    `object DroppedEvent extends EventFactory { val TypeUUID: UUID = UUID.fromString("<generate one>"); def
    entryFor(dropped: EventLogEntry, reason: DropReason): EventLogEntry }`.
- Scaladoc on `EventObserver`/`EventObserverFactory`: observers are best-effort; events committed just before a crash
  or service move are never observed; `observe` must be fast and must not block.

- [ ] **Step 1: Write failing tests** — `"EventLogEntry round-trips through encode/decode"` (timestamp, UUID, 0-byte
  and 1000-byte data); `"DroppedEvent.entryFor carries the dropped type and reason and the dropped timestamp"` (decode
  via `DroppedEvent.createEvent(entry)`, assert fields, `describe` contains the dropped UUID).
- [ ] **Step 2: Run** `sbt 'testOnly *EventTypesSuite'` — expect compile failure.
- [ ] **Step 3: Implement** the files and proto messages above (`DroppedEventData { UUID droppedType; DropReason
  reason }`, proto enum `DropReason`).
- [ ] **Step 4: Run** `sbt 'testOnly *EventTypesSuite'` — PASS.
- [ ] **Step 5: Commit** `"Event log: entry, event and observer types, DroppedEvent"`.

### Task 2: Keys, record framing and compression

**Files:**
- Create: `LogKey.scala`, `LogRecords.scala`, `FullObjectHeader.scala`
- Test: `common/eventlog/LogRecordsSuite.scala`

**Interfaces:**
- Produces:
  - `object LogKey { def apply(ts: HLCTimestamp): Key; def decode(k: Key): HLCTimestamp }` — 8 bytes big-endian of
    `ts.asLong ^ Long.MinValue`, so `ByteArrayKeyOrdering` order equals `HLCTimestamp` order.
  - `final case class LoggedRecord(timestamp: HLCTimestamp, typeId: Short, data: Array[Byte])` (content equality).
  - `object LogRecords`:
    - `def encodeFrame(records: Seq[LoggedRecord]): Array[Byte]` — one zstd frame over length-delimited proto
      `LoggedRecord { fixed64 timestamp; uint32 typeId; bytes data }`.
    - `def decodeFrames(bytes: Array[Byte]): Vector[LoggedRecord]` — all concatenated frames (use
      `ZstdInputStream`, which reads concatenated frames), empty input -> empty.
    - `def maxFrameSize(rawSize: Int): Int` — `Zstd.compressBound(rawSize)`.
    - `def recordSize(r: LoggedRecord): Int` — its length-delimited encoded size.
  - `final case class FullObjectHeader(filedKey: HLCTimestamp, upperBound: Option[HLCTimestamp])` and
    `object FullObject { def encode(h: FullObjectHeader, records: Seq[LoggedRecord]): Array[Byte]; def decode(b:
    Array[Byte]): (FullObjectHeader, Vector[LoggedRecord]) }` — fixed-size header (1 + 8 + 8 bytes) then one frame.
    `FullObject.overhead: Int` is the header size, which planners add to every size check.

- [ ] **Step 1: Write failing tests**
  - `"LogKey order matches HLCTimestamp order"`: 1000 random Longs including `Long.MinValue`, `-1`, `0`, `Long.MaxValue`,
    and `HLCTimestamp.now`; sorting keys with `ByteArrayKeyOrdering` equals sorting timestamps. Round-trip decode.
  - `"decodeFrames reads every concatenated frame"`: `encodeFrame(a) ++ encodeFrame(b) ++ encodeFrame(c)` decodes to
    `a ++ b ++ c`; `decodeFrames(Array.empty)` is empty.
  - `"encodeFrame never exceeds maxFrameSize"`: random and incompressible (random-byte) data, 1–5000 records.
  - `"FullObject round-trips with and without an upper bound"`.
- [ ] **Step 2: Run** `sbt 'testOnly *LogRecordsSuite'` — compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `"Event log: order-preserving keys, record frames, full-object format"`.

### Task 3: State object and messages

**Files:**
- Create: `EventLogState.scala`, `EventLogMessage.scala`; proto messages in `codec.proto`
- Test: `common/eventlog/EventLogStateSuite.scala`

**Interfaces:**
- Produces `object EventLogState`:
  - Keys (single-byte, as `RebalancingServiceState`): `IncomingKey = Key(0)` (encoded KV `Root`), `LoggedKey = Key(1)`
    (encoded DataObject `Root`), `TypeIdsKey = Key(2)`, `ObserversKey = Key(3)`, `AllocatorKey = Key(4)`,
    `TailsKey = Key(5)`, `TailFloorKey = Key(6)`.
  - `type TypeIds = Map[UUID, Short]`; `encodeTypeIds/decodeTypeIds`.
  - `final case class ObserverConfig(factoryType: UUID, config: Array[Byte])` (content equality);
    `encodeObservers(List[ObserverConfig]) / decodeObservers`.
  - `encodeAllocator(ObjectAllocatorId) / decodeAllocator` (the 17-byte `toArray`).
  - `final case class Tails(pointers: Vector[DataObjectPointer], current: Int, allocator: ObjectAllocatorId)` (the
    allocator the tails came from; Task 10 compares it with `AllocatorKey`); `encodeTails/decodeTails`.
  - `encodeTailFloor(HLCTimestamp) / decodeTailFloor`.
  - `val TailCount = 10`.
- Produces `sealed trait EventLogMessage` with `case object Nudge`, `case object ReloadObservers`,
  `EventLogMessage.encode/decode` (proto `EventLogMessage { oneof msg { ... } }`, `Empty` throws, as
  `RebalancingMessage`).

- [ ] **Step 1: Failing tests** — round-trip each codec (`TypeIds` with 0 and 300 entries; observers with empty and
  non-empty config; allocator for a pool id and a group id; tails with 10 pointers and `current = 7`); messages
  round-trip.
- [ ] **Step 2: Run** `sbt 'testOnly *EventLogStateSuite'` — compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `"Event log: service state codecs and messages"`.

### Task 4: Pure placement planner

**Files:**
- Create: `LogPlanner.scala`
- Test: `common/eventlog/LogPlannerSuite.scala` (randomized, no Aspen system)

**Interfaces:**
- Consumes: `LoggedRecord`, `LogRecords.recordSize/maxFrameSize`, `FullObject.overhead`, `EventLogState.TailCount`.
- Produces:
  ```scala
  final case class TailView(index: Int, length: Int, cap: Option[Int])          // current byte length, pool cap
  final case class FullView(key: HLCTimestamp, upperBound: Option[HLCTimestamp],
                            records: Vector[LoggedRecord], cap: Option[Int])     // only for the object a late event hits
  final case class PlanInput(batch: Vector[LoggedRecord],   // sorted by timestamp, type ids assigned
                             tails: Vector[TailView], current: Int,
                             tailRecords: Option[Vector[LoggedRecord]],         // Some only when compaction is possible
                             tailFloor: HLCTimestamp, firstFullKey: Option[HLCTimestamp],
                             lateTarget: Option[FullView],                      // covering object of the oldest late record
                             target: Int, newObjectCap: Option[Int])
  enum LogStep:
    case AppendToTail(index: Int, records: Vector[LoggedRecord])
    case Compact(objects: Vector[(FullObjectHeader, Vector[LoggedRecord])], newTailFloor: HLCTimestamp)
    case RewriteFull(oldKey: HLCTimestamp, objects: Vector[(FullObjectHeader, Vector[LoggedRecord])])  // 1st reuses the old object
    case Drop(record: LoggedRecord)                                              // becomes a DroppedEvent record
  final case class LogPlan(steps: Vector[LogStep], consumed: Vector[LoggedRecord], newCurrent: Int)
  object LogPlanner { def plan(in: PlanInput): LogPlan }
  ```
- Rules (from the spec, "Chosen scheme" and "Clarifications"):
  - A record with `maxFrameSize(recordSize) + FullObject.overhead > cap` (cap = `newObjectCap`) is `Drop`ped. The
    executor logs a `DroppedEvent` record for it in its place (in-order, into a tail).
  - Records `>= tailFloor` go to tails: fill `current` up to its soft target (`target / TailCount`), never letting
    `length + maxFrameSize(raw)` pass the tail's `cap`; then advance `current`. When no tail can take the next record,
    emit `Compact` (needs `tailRecords`; if `None`, stop and return `consumed` so far — the executor re-plans with tail
    content loaded). `Compact` sorts tail records, packs them into objects each `<= target` and `<= cap` (by
    `maxFrameSize` bound), sets each header's `filedKey` = its first record, `upperBound = None`, and
    `newTailFloor` = max timestamp compacted; all tails become empty and `current = 0`.
  - Records `< tailFloor` are late. At most one `RewriteFull` per plan (one full object per transaction): merge the late
    records that fall in `lateTarget`'s range, sort; if the result exceeds `target`/cap, split at a timestamp boundary
    (never between equal timestamps) — the first part keeps the object, later parts are new objects; each part's
    `upperBound` = next part's key, the last part keeps the old `upperBound`. A late record earlier than
    `firstFullKey` rekeys the first object (`filedKey` changes). If no split point exists and the merge exceeds the
    cap, the late record is `Drop`ped instead. Late records outside `lateTarget` stay unconsumed (next transaction).
  - `consumed` lists exactly the batch records the plan placed or dropped; unconsumed ones stay in `incomingEvents`.

- [ ] **Step 1: Failing tests**
  - `"in-order records fill tails in order and advance current at the soft target"`.
  - `"no written tail or object exceeds its cap"` — randomized: 2000 cases, random caps (None, 200 B–64 KiB), random
    record sizes (0–2× cap), random tail lengths; assert every `AppendToTail` keeps `length + bound <= cap`, every
    `Compact`/`RewriteFull` object bound `<= cap`.
  - `"every batch record is consumed exactly once or left for later"` — randomized; consumed ∪ unconsumed = batch,
    disjoint; every consumed record appears in exactly one step.
  - `"compaction sorts tail records and sets the floor to the largest compacted timestamp"`.
  - `"a record too large for one object is dropped"` (cap 1000, record 2000 incompressible bytes).
  - `"a late record is merged into its covering object; an oversized merge splits at a timestamp boundary and links
    upper bounds"`; `"equal timestamps are never split apart"`; `"a record older than the first key rekeys the first
    object"`.
- [ ] **Step 2: Run** `sbt 'testOnly *LogPlannerSuite'` — compile failure.
- [ ] **Step 3: Implement** `LogPlanner.plan` per the rules.
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `"Event log: pure placement and compaction planner"`.

### Task 5: Bootstrap, service, client insert, moving into tails

**Files:**
- Create: `EventLogDurableService.scala`, `LogWriter.scala`, `SystemEventLog.scala`
- Modify: `server/store/Bootstrap.scala` (register next to the rebalancing entry), `server/TypeFactories.scala`,
  `client/AspenClient.scala`
- Test: `common/eventlog/EventLogServiceSuite.scala` (`IntegrationTestSuite`, executors as in
  `RebalancingServiceSuite`), `common/eventlog/EventLogOverwriteSuite.scala` (same tests, `override def testIda =
  ReedSolomon(5, 3, 4)`, the classic-RS overwrite path)

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces:
  - `object EventLogDurableService extends DurableServiceFactory`: `ServiceTypeUUID`, `ServiceUUID` (generate),
    `DefaultPollPeriod = 15.seconds`, `DefaultTargetObjectSize = 2 * 1024 * 1024`,
    `private[aspen] def setTestPollPeriod(client, Duration)`, `private[aspen] def setTestTargetObjectSize(client, Int)`.
  - `object SystemEventLog`:
    `private[aspen] def initialState(incoming: Root, logged: Root, tails: Vector[DataObjectPointer], allocator:
    ObjectAllocatorId): Map[Key, Array[Byte]]` (empty typeIds/observers, `tailFloor = HLCTimestamp.Zero`,
    `Tails(tails, 0, allocator)`); `private[aspen] def statePointer(client): Future[KeyValueObjectPointer]` (services TKVL lookup,
    as `RebalancingDurableService.readServiceStatePointer`).
  - `AspenClient` (concrete methods delegating to `SystemEventLog`):
    - `def prepareSystemEventLogEntry(e: EventLogEntry)(using Transaction): Future[Unit]`
    - `def prepareSystemEventLogEntries(es: Seq[EventLogEntry])(using Transaction): Future[Unit]` — one
      `prepareUpdates` of `TKVLInsert(Key(UUID.randomUUID()), Value(e.encode()), requireDoesNotExist = true)`.
      Both register `tx.result.foreach(_ => sendServiceMessage(ServiceUUID, encode(Nudge)))`, failures swallowed.
    - `def logSystemEvent(e: EventLogEntry): Future[Unit]` — `transact` once (no retry), `replaySafe("event log
      entries may be duplicated")`. Scaladoc: no automatic retry; may be duplicated.
- Bootstrap: `incoming = allocateTree(ByteArrayKeyOrdering)`; `logged = Root(0, ByteArrayKeyOrdering, None,
  bootstrapNodeAllocator, TreeKind.DataObjects)`; 10 × `allocateData(Array.empty)`; allocator
  `PoolObjectAllocatorId(Radicle.poolId)`; unclaimed `ServiceEntry` in the services tree.
- Service: on create, read state, load typeIds; poll via `scheduleNonConcurrentPollingTask("event-log-move",
  pollPeriod)`; `receiveMessage(Nudge)` runs a scan now (one scan at a time: a scan already running sets a "rescan"
  flag, as the rebalancing `beginSweep` claim). A scan loops: read up to 1000 entries / 1 MiB raw from `incomingEvents`
  (`foreach`, stop early), assign new type ids (next = max + 1, ids 0–65535 held in a `Short` read unsigned; past 65535 → log
  an error and stop moving), build `PlanInput`, `LogWriter.apply(plan)` + delete consumed keys + revision-checked state updates in one
  `transact` (collision → re-read and retry next loop), until the tree is empty.
- `LogWriter`: `AppendToTail` → `tx.append(readAppendPosition(tail), frame)` when the tail's pool IDA is `Replication`
  or `RandomAccessReedSolomon`, else `tx.overwrite(tail, rev, old ++ frame)`. (Compaction/RewriteFull/Drop executors
  arrive in Task 6/7; here they throw `NotImplementedError` and tests keep below compaction size.)

- [ ] **Step 1: Failing tests** (both suites; target object size 64 KiB, poll 100 ms)
  - `"Bootstrap registers the event log with empty state"` — statePointer resolves; 10 tails; empty typeIds; floor Zero.
  - `"logged events reach the tails and leave incomingEvents"` — `logSystemEvent` 50 entries of 3 types; drive until
    `incomingEvents` is empty; decoding all tails yields exactly those 50 (multiset), 3 type ids assigned.
  - `"prepareSystemEventLogEntries inserts several entries in one transaction"`.
  - `"a nudge moves entries without waiting for the poll"` — poll period 1 hour; entry moved after `logSystemEvent`.
  - `"two overlapping service instances never lose or double-move an entry"` — construct two
    `EventLogDurableService` instances on the same state directly; 200 entries; tails hold each exactly once.
  - Overwrite suite only: `"tails on classic ReedSolomon are rewritten whole"` — content equals expectation.
- [ ] **Step 2: Run** `sbt 'testOnly *EventLogServiceSuite *EventLogOverwriteSuite'` — FAIL.
- [ ] **Step 3: Implement** the files above.
- [ ] **Step 4: Run** — PASS. Then `sbt 'testOnly *RebalancingServiceSuite *BasicIntegrationSuite'` — PASS (Bootstrap
  change).
- [ ] **Step 5: Commit** `"Event log: bootstrap service, client insert, move into tails"`.

### Task 6: Compaction and dropped events

**Files:**
- Modify: `LogWriter.scala`, `EventLogDurableService.scala`
- Test: `EventLogServiceSuite.scala`, new `common/eventlog/EventLogSmallSliceSuite.scala` (`testIda = ReedSolomon(5,
  3, 4)`, `testPoolMaxSliceSize = Some(4096)`)

**Interfaces:**
- `Compact` executor in one transaction: allocate each object from `getAllocator(logAllocator)` with
  `allocateDataObject(DataBuffer(FullObject.encode(...)))`, insert `LogKey(filedKey) -> ptr.toArray` into
  `loggedEvents` (one `prepareUpdates`), overwrite every tail with empty content, write `Tails(_, 0, _)` and the new
  `tailFloor` with `KeyRevision` requirements.
- `Drop` executor: put `DroppedEvent.entryFor(entry, ExceededMaximumLogEntrySize)` (as a `LoggedRecord`, its type id
  assigned like any other) into the in-order path; the original entry's incoming key is deleted.
- The service passes `tailRecords` (decoded tails) when the planner returned with unconsumed records due to no tail
  room, and re-plans.

- [ ] **Step 1: Failing tests**
  - `"filling the tails compacts them into a sorted full object"` — target 16 KiB; enough events for 2 compactions;
    `loggedEvents` has 2 keys; each object decodes sorted; tails + objects hold every event; floor = max compacted ts.
  - Small-slice suite: `"an event larger than the object cap is replaced by a DroppedEvent"` — 10 KiB incompressible
    event; the log holds a `DroppedEvent` with the dropped type, same timestamp, reason `ExceededMaximumLogEntrySize`;
    `"no log object exceeds the pool's maxObjectSize"` — many random-size events; read every tail and full object,
    size `<= pool.maxObjectSize`.
- [ ] **Step 2: Run** `sbt 'testOnly *EventLogServiceSuite *EventLogSmallSliceSuite'` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `"Event log: compaction into full objects, dropped events"`.

### Task 7: Late events

**Files:**
- Modify: `LogWriter.scala`, `EventLogDurableService.scala`
- Test: `common/eventlog/EventLogLateEventSuite.scala`

**Interfaces:**
- Service: for the oldest late record in a batch, find the covering object (largest `loggedEvents` key `<=` it, else
  the first), read and decode it into `lateTarget`.
- `RewriteFull` executor: overwrite the existing object (revision-checked) with the first part; allocate the rest;
  one `loggedEvents.prepareUpdates` for new keys plus, on rekey, delete the old key and insert the new one.

- [ ] **Step 1: Failing tests** — after one compaction:
  - `"a late event is merged into its covering full object"`.
  - `"a late event that overflows an object splits it at a timestamp boundary"` — upper bounds link the parts.
  - `"an event older than every full object rekeys the first object"` — tree key equals the new min timestamp.
- [ ] **Step 2: Run** `sbt 'testOnly *EventLogLateEventSuite'` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `"Event log: late events into full objects"`.

### Task 8: Reader

**Files:**
- Create: `EventLogReader.scala`
- Modify: `client/AspenClient.scala`
- Test: `common/eventlog/EventLogReaderSuite.scala`

**Interfaces:**
- Produces on `AspenClient`:
  - `def readSystemEvents(from: HLCTimestamp = HLCTimestamp.Zero, to: Option[HLCTimestamp] = None,
    limit: Option[Int] = None): Future[Vector[EventLogEntry]]` — sorted by timestamp, `[from, to]` inclusive,
    duplicates possible, type ids mapped back to UUIDs.
  - `def decodeSystemEvent(e: EventLogEntry): Option[Event]` — via `typeRegistry.getType[EventFactory]`.
- Algorithm (pinned by the spec's no-omission rule): (1) read state (tails, typeIds); (2) read and decode tails;
  (3) walk `loggedEvents` (`foreachInRange` from the key covering `from`), reading each object; (4) for each object
  check its header: `filedKey` must equal its tree key, and a `Some(upperBound)` must equal the next tree key in this
  walk's snapshot (or the walk's end); any mismatch, or an `InvalidObject`, restarts from (1); at most 5 restarts,
  then fail with `IllegalStateException`. Merge-sort tails and objects; apply range and limit.

- [ ] **Step 1: Failing tests**
  - `"readSystemEvents returns logged events in timestamp order across full objects and tails"` — after compaction +
    pending tail events; filters by `from`/`to`; `limit`.
  - `"a reader that read the tree before a split restarts instead of omitting"` — use a test seam
    (`private[eventlog] var testAfterTreeRead: () => Future[Unit]`, per-reader instance, not static) to run a
    late-event split between steps (3) and the object reads; result contains every event.
  - `"a reader racing compaction may duplicate but never omits"` — seam between (2) and (3) runs a compaction.
  - `"decodeSystemEvent returns None for an unregistered type and a DroppedEvent for its type"`.
- [ ] **Step 2: Run** `sbt 'testOnly *EventLogReaderSuite'` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `"Event log: public reader"`.

### Task 9: Observers

**Files:**
- Modify: `EventLogDurableService.scala`, `SystemEventLog.scala`, `client/AspenClient.scala`
- Test: `common/eventlog/EventLogObserverSuite.scala`

**Interfaces:**
- Produces on `AspenClient`:
  - `def addSystemEventObserver(factoryType: UUID, config: Array[Byte]): Future[Unit]` — fails with
    `IllegalArgumentException` if `typeRegistry.getType[EventObserverFactory](factoryType)` is empty; adding an
    existing pair is a no-op; revision-checked on `ObserversKey`, `transactUntilSuccessful`; then best-effort
    `ReloadObservers`.
  - `def removeSystemEventObserver(factoryType: UUID, config: Array[Byte]): Future[Unit]` — absent pair is a no-op.
  - `def listSystemEventObservers(): Future[List[ObserverConfig]]`.
- Service: builds observers at start and on reload (`ReloadObservers`, or a poll that sees `ObserversKey`'s revision
  differ from the last loaded); unknown factory → warn and skip. After each committed move transaction, for each
  consumed record in timestamp order, `decodeSystemEvent`, then call matching observers; exceptions logged.

- [ ] **Step 1: Failing tests** (a test `EventObserverFactory` in `userTypeFactories` recording events in a
  per-instance buffer looked up by config bytes)
  - `"an added observer sees events of its observed types only"`; `"an observer with observedTypes None sees all"`.
  - `"removing an observer stops delivery"`.
  - `"a lost reload message is covered by the poll"` — write `ObserversKey` directly (no message); observer active after
    a poll.
  - `"adding an unregistered factory type fails"`.
  - `"an observer that throws does not stop the log or other observers"`.
- [ ] **Step 2: Run** `sbt 'testOnly *EventLogObserverSuite'` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `"Event log: observers and reload"`.

### Task 10: Changing the log allocator

**Files:**
- Modify: `SystemEventLog.scala`, `client/AspenClient.scala`, `LogWriter.scala`
- Test: `common/eventlog/EventLogAllocatorSuite.scala`

**Interfaces:**
- Produces `AspenClient.setSystemLogAllocator(id: ObjectAllocatorId): Future[Unit]` — `getAllocator(id)` first
  (fails if missing); one `transactUntilSuccessful`: `KVObjectRootManager.setNewRoot`-style rewrite of `LoggedKey`
  with the root's `NodeAllocator` replaced by `NodeAllocator(client, List((id, size)))` (size = existing tier-0 size,
  checked by `validate(TreeKind.DataObjects)`), and `AllocatorKey`, both revision-checked; then `Nudge`.
- Compaction: when `Tails.allocator` differs from `AllocatorKey`, allocate 10 new empty tails from the new allocator,
  `setRefcount(old, rc, rc.decrement())` each old tail, store the new `Tails`.

- [ ] **Step 1: Failing tests** — create a second pool (as existing pool-creation tests do, e.g. `client.createPool`
  used by `MaxSliceSizeIntegrationSuite`); `"setSystemLogAllocator directs new full objects and tails to the new
  pool"`; `"setSystemLogAllocator fails for an unknown allocator"`; old tails' refcount reaches 0.
- [ ] **Step 2: Run** `sbt 'testOnly *EventLogAllocatorSuite'` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `"Event log: changing the log allocator"`.

### Task 11: cmdline commands

**Files:**
- Modify: `cmdline/Main.scala` (Args fields, `Command` entries, dispatch, handler functions)
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala` (parser tests, following existing ones)

**Interfaces:**
- Commands (`Commands` list, beside `system-rebalance-period`):
  - `event-log <client-config> [--since <time>] [--until <time>] [--type <uuid>] [--limit N]` — times ISO-8601
    instants (`java.time.Instant.parse`) converted to `HLCTimestamp`; prints one line per entry:
    `<ISO instant>  <typeName or UUID>  <describe, or "N bytes" when empty>`; type name is the factory's class simple name; unregistered:
    `<uuid> (no factory, N bytes)`. `--type` filters client-side.
  - `event-log-observers <client-config>` — lists `factoryType` and config length.
  - `event-log-add-observer <client-config> <factory-uuid> [--config-file <path>]`,
    `event-log-remove-observer <client-config> <factory-uuid> [--config-file <path>]` (config read with `os.read.bytes`;
    absent = empty).
- Pure formatting in `def formatEventLine(e: EventLogEntry, decoded: Option[Event], name: Option[String]): String` so
  it is testable without a system.

- [ ] **Step 1: Failing tests** — parser accepts each command and option, rejects a bad UUID and a bad time;
  `formatEventLine` for a decoded event, a `DroppedEvent`, and an unregistered type matches the spec's sample format.
- [ ] **Step 2: Run** `sbt 'testOnly *MainSuite'` — FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** `"cmdline: event-log and observer commands"`.

### Task 12: Documentation and full run

**Files:**
- Create: `common/eventlog/DESIGN.md`, `common/eventlog/CLAUDE.md`
- Modify: root `CLAUDE.md` (module table row), `client/CLAUDE.md` (event log API bullet), the guide that lists
  Bootstrap-registered services (`grep -rn RebalancingDurableService --include=CLAUDE.md src`)

- [ ] **Step 1: Write `DESIGN.md`** covering the spec's "Package documentation" section exactly: data types, state,
  incoming → tails → full objects, compaction, late events/splits/rekey, the reader's no-omission restarts, client API,
  observers; and the call-outs: **duplicates** (replayed insert after the service deleted the entry; reader racing
  compaction; any other path found while implementing), **dropped events** depending on the allocator's fixed
  `maxObjectSize` (same event logged on one allocator, dropped on another; oversize for `incomingEvents` fails at
  insert), **best-effort observers and messages**, **Replication/RandomAccessReedSolomon append optimization** vs. whole
  rewrites elsewhere. Describe what was built, not the plan.
- [ ] **Step 2: Write `CLAUDE.md`** — points to DESIGN.md; invariants: key encoding, one full object per transaction,
  reader header checks, pin rule usage, no static test seams.
- [ ] **Step 3: Run the full suite** `timeout 80 sbt test` under normal load — all PASS. Re-run the event log suites 5
  times (`sbt 'testOnly *EventLog*'`) to check for flakes.
- [ ] **Step 4: Commit** `"Event log: DESIGN.md and module guides"`.
