# Partial Key-Value Object Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let clients read an explicit set of keys, or an inclusive key range, from a KeyValue object, with stores returning only the requested entries.

**Architecture:** A new `KeySet` read type joins the existing `KeyRange` in `ReadType`, the protobuf message, and the codec. The store filters the encoded `KVObjectState` for these read types before replying, and never touches its cached copy. The client read driver carries the `ReadType`. For partial reads it skips the client object cache and opportunistic rebuild, and it marks the resulting `KeyValueObjectState` as `partial`. `AspenClient` exposes `readKeys` and `readKeyRange`.

**Tech Stack:** Scala 3, ScalaPB/protobuf (`src/main/protobuf/codec.proto`), ScalaTest, sbt.

**Spec:** `docs/superpowers/specs/2026-10-23-kv-partial-reads-design.md`

## Global Constraints

- Scala 3 quiet syntax. Use indented if/then/else. Match the surrounding code (per `CLAUDE.md`).
- `KeyRange` is inclusive at **both** ends.
- Partial results are **never** put into `client.objectCache` and **never** cause `sendOpportunisticRebuild` or `opportunisticRebuildManager.markRepairNeeded`.
- A store's cached `ObjectState` is never modified by a partial read.
- Partial results always carry the object's min, max, left and right, along with revision, refcount, timestamp and locked transactions.
- A requested key that doesn't exist is absent from `contents`. It is not an error.
- In a partial `ReadResponse`, `sizeOnStore` stays the full object's encoded size.
- Partial-ness is a flag on `KeyValueObjectState` (`partial: Boolean`), not a subtype.
- `SingleKey`, `LargestKeyLessThan*`, `ByteRange` and `MetadataOnly` stay unimplemented. The store treats them like `FullObject`, as it does today.
- Run single suites with `sbt 'testOnly *SuiteName'`, or `sbt 'testOnly *SuiteName -- -z "substring"'`.

## Review Focus

1. **Empty key list:** `readKeys(ptr, Nil)` succeeds with empty `contents`, `partial == true`, and min/max/left/right present. (Task 2 store test, Task 4 integration test.)
2. **Range with `startKey > endKey`:** succeeds with empty `contents`. It is not an error. (Task 2.)
3. **Non-lexical ordering at the bounds:** with `IntegerKeyOrdering`, keys equal to `startKey` and `endKey` are included and keys just outside are excluded. (Task 2.)
4. **Store cache after a partial read:** a full read immediately afterwards still returns every key. This covers both the cached and the backend-loaded paths. (Task 2.)
5. **Stale store during a partial read:** a store at an older revision makes a full read trigger an opportunistic rebuild. The same responses for a partial read must not trigger one. (Task 3.)

---

### Task 1: `KeySet` read type and wire encoding

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/objects/ReadType.scala`
- Modify: `src/main/protobuf/codec.proto` (`enum ReadType` ~line 296, `message Read` ~line 306)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (Read encode ~689–720, decode ~731–741)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala` (`test("Read round-trip")` ~line 371)

**Interfaces:**
- Produces: `case class KeySet(keys: List[Key]) extends ReadType`. The existing `KeyRange(minimum: Key, maximum: Key, ordering: KeyOrdering)` keeps its shape. Add a doc comment saying it is inclusive at both ends.

- [ ] **Step 1: Write the failing test.** Extend `test("Read round-trip")` with:

```scala
    val keySet = Read(storeId(10), ClientId(uuid(20)), uuid(30), kvPtr(1),
      KeySet(List(Key(Array[Byte](1)), Key(Array[Byte](2, 3)))))
    val decodedKS = Codec.decode(Codec.encode(keySet)).readType.asInstanceOf[KeySet]
    decodedKS.keys.map(_.bytes.toList) shouldBe List(List[Byte](1), List[Byte](2, 3))

    val emptyKeySet = Read(storeId(10), ClientId(uuid(20)), uuid(30), kvPtr(1), KeySet(Nil))
    Codec.decode(Codec.encode(emptyKeySet)).readType.asInstanceOf[KeySet].keys shouldBe Nil
```

- [ ] **Step 2: Run it and confirm it fails.** `sbt 'testOnly *CodecRoundTripSuite -- -z "Read round-trip"'`. Expected: a compile error, because `KeySet` is not defined.

- [ ] **Step 3: Implement.** Add `KeySet` to `ReadType.scala`. In `codec.proto`, add `READ_TYPE_KEY_SET = 7;` to `enum ReadType` and `repeated bytes keys = 12;` to `message Read`. In `Codec.scala`, add a `case rt: KeySet` that encodes each key's bytes into `keys`, and decode `READ_TYPE_KEY_SET` to `KeySet(m.keys.map(b => Key(b.toByteArray)).toList)`. Every other read type sends an empty `keys`.

- [ ] **Step 4: Run it and confirm it passes.** Run the same command. Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -m "Add KeySet read type and its wire encoding"`

---

### Task 2: Store-side filtering of partial KV reads

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/server/store/PartialRead.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala` (`NetworkRead` ~line 24, `readObjectForNetwork` ~line 101, `backendReadComplete` NetworkRead branch ~line 250)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/Host.scala:1994` (pass `r.readType`)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/PartialReadSuite.scala` (create)
- Test: `src/test/scala/org/aspen_ddp/aspen/server/store/FrontendSuite.scala`

**Interfaces:**
- Consumes: `KeySet` and `KeyRange` from Task 1.
- Produces:
  - `object PartialRead { def filter(objectType: ObjectType.Value, data: DataBuffer, readType: ReadType): DataBuffer }`. For a `KeyValue` object with a `KeySet` or `KeyRange` read type, it decodes with `KVObjectState.decode`, keeps the matching `content` entries, and re-encodes with `KVObjectState.encode`, preserving min, max, left and right. A `KeySet` match is key-byte equality. A `KeyRange` entry is kept when `ordering.compare(k, minimum) >= 0 && ordering.compare(k, maximum) <= 0`. For every other combination it returns `data` unchanged.
  - `Frontend.readObjectForNetwork(clientId: ClientId, readUUID: UUID, pointer: ObjectPointer, readType: ReadType): Unit`
  - `NetworkRead(clientId: ClientId, requestUUID: UUID, readType: ReadType)`

- [ ] **Step 1: Write failing `PartialReadSuite` tests.** Build an object with `KVObjectState.encode(Some(min), Some(max), Some(left), Some(right), contents)`. Use integer-encoded keys 1, 2, 3, 4, 5 (use whichever encoding `IntegerKeyOrdering` expects; see `KeyOrdering.scala:51`) with server `ValueState`s. Decode each result with `KVObjectState.decode` and assert:
  - `"KeySet keeps only requested keys"`: `KeySet(List(k2, k4, k9))` gives content keys `{k2, k4}`, and min, max, left and right equal the originals.
  - `"empty KeySet keeps no keys"`: `KeySet(Nil)` gives empty content and min, max, left and right present.
  - `"KeyRange is inclusive at both ends"`: `KeyRange(k2, k4, IntegerKeyOrdering)` gives `{k2, k3, k4}`.
  - `"KeyRange with min > max is empty"`: `KeyRange(k4, k2, IntegerKeyOrdering)` gives empty content.
  - `"non-partial read types and Data objects are unchanged"`: `filter(ObjectType.KeyValue, data, FullObject())` and `filter(ObjectType.Data, data, KeySet(Nil))` both return a buffer whose bytes equal `data`.

- [ ] **Step 2: Run and confirm failure.** `sbt 'testOnly *PartialReadSuite'`. Expected: a compile error, because `PartialRead` is not defined.

- [ ] **Step 3: Implement `PartialRead.filter`** as described in the Interfaces block.

- [ ] **Step 4: Run and confirm it passes.** `sbt 'testOnly *PartialReadSuite'`. Expected: PASS.

- [ ] **Step 5: Write failing `FrontendSuite` tests.** Add a `mkFrontend` variant that also returns its `TestNet`. Allocate a KV object with keys k1–k3 in the `MapBackend` with `backend.bootstrapAllocate(oid2, ObjectType.KeyValue, metadata, KVObjectState.encode(...))`. Then:
  - `"partial network read returns only requested keys and leaves the cache intact"`: call `readObjectForNetwork(clientId, uuid, kvPtr, KeySet(List(k1)))`. Decode the `ReadResponse`'s `objectData` and assert the content keys are `{k1}`. Assert `sizeOnStore` equals the full encoded size. Next, call `readObjectForNetwork(..., FullObject())`, which is now served from the store cache, and assert the content keys are `{k1, k2, k3}`. Finally assert the `ObjectState` in `cache` still decodes to `{k1, k2, k3}`.
  - Run the same test a second time with the object already in `cache` before the first read, so the cached path is covered too.

- [ ] **Step 6: Run and confirm failure.** `sbt 'testOnly *FrontendSuite -- -z "partial network read"'`. Expected: a compile error, because `readObjectForNetwork` takes 3 arguments.

- [ ] **Step 7: Thread `readType` through `Frontend` and `Host`.** Apply `PartialRead.filter(os.objectType, os.data, readType)` in both response sites, the cached path and the `backendReadComplete` `NetworkRead` branch. Keep `os.data.size` (the unfiltered size) as `sizeOnStore`.

- [ ] **Step 8: Run and confirm it passes.** `sbt 'testOnly *FrontendSuite'` and `sbt 'testOnly *PartialReadSuite'`. Expected: PASS.

- [ ] **Step 9: Commit.** `git commit -m "Filter KeySet and KeyRange reads on the store"`

---

### Task 3: Read-type-aware client read path

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/ObjectState.scala` (`KeyValueObjectState` ~line 104, its `equals`/`hashCode`/`toString`, and the companion `apply` ~line 283)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadDriver.scala` (`Factory`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/SimpleReadDriver.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/KeyValueObjectReader.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadManager.scala:109`
- Modify: any other `ReadDriver.Factory` implementations or callers the compiler reports (for example `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala:73`)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala`

**Interfaces:**
- Consumes: `KeySet` and `KeyRange` from Task 1.
- Produces:
  - `KeyValueObjectState(..., contents: Map[Key, ValueState], partial: Boolean = false)` as the last constructor parameter, also on the companion `apply`. Include it in `equals` and `toString`.
  - `ReadDriver.Factory = (ReadDriverClient, ObjectPointer, IDA, UUID, String, Boolean, ReadType) => ReadDriver`. The new last parameter is the read type.
  - `BaseReadDriver(client, objectPointer, ida, readUUID, comment, disableOpportunisticRebuild: Boolean = false, readType: ReadType = FullObject())`
  - `KeyValueObjectReader(metadataOnly: Boolean, pointer: KeyValueObjectPointer, ida: IDA, readUUID: UUID, partial: Boolean = false)`
  - `ReadManager.read(objectPointer: ObjectPointer, ida: IDA, comment: String, disableOpportunisticRebuild: Boolean = false, readType: ReadType = FullObject()): Future[ObjectState]`

- [ ] **Step 1: Write failing `BaseReadDriverSuite` tests.** Extend `mkReader` with a `readType: ReadType = FullObject()` parameter. Build a recording client: a `ReadDriverClient.NoOp` subclass that overrides `objectCache` with a cache recording `put` calls and `messenger` with a `Messenger` recording `sendClientRequest` messages. Use `kvptr`, and give each store KV data from `KVObjectState.encode(...)` with one key.
  - `"full KV read caches and rebuilds stale stores"` (the control case): ds0 and ds1 reply at revision `nrev2`, and ds2 replies at the older `rev`. Assert that `readResult` completes, `put` was called once, and an `OpportunisticRebuild` was sent to ds2.
  - `"partial KV read neither caches nor rebuilds"`: send the same responses with `readType = KeySet(List(key))`. Assert that `readResult` completes with `partial == true`, `put` was never called, and no `OpportunisticRebuild` was sent.
  - `"partial KV read sends its read type"`: after `begin()`, every recorded `Read` message has `readType == KeySet(List(key))`.

- [ ] **Step 2: Run and confirm failure.** `sbt 'testOnly *BaseReadDriverSuite'`. Expected: a compile error, because `BaseReadDriver` has no `readType` parameter.

- [ ] **Step 3: Implement.** Add the `partial` flag to `KeyValueObjectState`. Replace the hardcoded `readType` in `BaseReadDriver` with the constructor parameter. Define `isPartial = readType.isInstanceOf[KeySet] || readType.isInstanceOf[KeyRange]`. When `isPartial`, skip `objectCache.put`, `markRepairNeeded` and `sendOpportunisticRebuild`. Pass `isPartial` to `KeyValueObjectReader`, which passes it to the `KeyValueObjectState` it builds. Update the `Factory` type, `SimpleReadDriver` and its `Factory`, `noErrorRecoveryReadDriver`, and `ReadManager.read`. Fix every remaining compile error the factory change causes.

- [ ] **Step 4: Run and confirm it passes.** `sbt 'testOnly *BaseReadDriverSuite'` and `sbt 'testOnly *KeyValueObjectReaderSuite'`. Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -m "Carry ReadType through client reads; skip cache and rebuild for partial reads"`

---

### Task 4: `readKeys` / `readKeyRange` client API

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/ObjectReader.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (~line 57–59)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` (~line 110)
- Modify: any other concrete `AspenClient` or `ObjectReader` implementations the compiler reports
- Test: `src/test/scala/org/aspen_ddp/aspen/BasicIntegrationSuite.scala`

**Interfaces:**
- Consumes: `ReadManager.read(..., readType)` from Task 3, and `KeySet` and `KeyRange` from Task 1.
- Produces, on `ObjectReader` and `AspenClient` next to `read`:
  - `def readKeys(pointer: KeyValueObjectPointer, keys: List[Key], comment: String): Future[KeyValueObjectState]` (abstract, implemented in `BaseAspenClient`)
  - `def readKeys(pointer: KeyValueObjectPointer, keys: List[Key]): Future[KeyValueObjectState] = readKeys(pointer, keys, "")`
  - `def readKeys(pointer: KeyValueObjectPointer, key: Key, moreKeys: Key*): Future[KeyValueObjectState] = readKeys(pointer, key :: moreKeys.toList, "")`
  - `def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering, comment: String): Future[KeyValueObjectState]` (abstract, implemented in `BaseAspenClient`)
  - `def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering): Future[KeyValueObjectState] = readKeyRange(pointer, startKey, endKey, ordering, "")`

- [ ] **Step 1: Write failing integration tests.** Follow `atest("Allocate KeyValue object")`. Allocate a KV object with keys 1–5 (`Key(Array[Byte](n))`) and commit. Then:
  - `atest("Read subset of keys")`: `client.readKeys(kp, k2, k4, Key(Array[Byte](9)))` returns content keys `{k2, k4}` with the correct values and `partial == true`. `client.readKeys(kp, Nil)` returns empty content. `client.read(kp)` afterwards returns all 5 keys with `partial == false`.
  - `atest("Read key range")`: `client.readKeyRange(kp, k2, k4, ByteArrayKeyOrdering)` returns `{k2, k3, k4}` with `partial == true`.

- [ ] **Step 2: Run and confirm failure.** `sbt 'testOnly *BasicIntegrationSuite -- -z "Read"'`. Expected: a compile error, because `readKeys` is not a member.

- [ ] **Step 3: Implement.** In `BaseAspenClient`, mirror `read(pointer: KeyValueObjectPointer, comment)`: `resolveIda(pointer).flatMap(ida => rmgr.read(pointer, ida, comment, readType = KeySet(keys)).map(_.asInstanceOf[KeyValueObjectState]))`. Do the same for `KeyRange(startKey, endKey, ordering)`.

- [ ] **Step 4: Run and confirm it passes.** `sbt 'testOnly *BasicIntegrationSuite'`. Expected: PASS.

- [ ] **Step 5: Full regression.** `sbt test`. Expected: all suites pass.

- [ ] **Step 6: Update `Todo.txt`.** Remove the "Add ability to read subsets of keys" item. Commit with `git commit -m "Add readKeys and readKeyRange to AspenClient"`
