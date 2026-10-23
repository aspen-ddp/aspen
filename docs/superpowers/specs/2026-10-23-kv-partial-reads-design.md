# Partial Key-Value Object Reads — Design

Date: 2026-10-23
Source: top item of `Todo.txt` ("Add ability to read subsets of keys")

## Intent

Let clients read a subset of a KeyValue object's entries — either an explicit set of keys or an
inclusive key range — with the stores returning only the requested entries rather than the whole
object. This is a general-purpose capability, not driven by one specific data structure; the
value is in not shipping (and IDA-restoring) every entry of a large KV object when the caller
only needs a few.

## Goals

- `AspenClient` gains:
  - `readKeys(pointer: KeyValueObjectPointer, keys: List[Key]): Future[KeyValueObjectState]`
  - A varargs convenience so callers can write `readKeys(ptr, key1, key2, key3)`
  - `readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering): Future[KeyValueObjectState]`
    - Inclusive on both ends: if `startKey` or `endKey` exists it is included in the result
- Stores filter entries server-side, so only requested entries cross the network.
- The returned `KeyValueObjectState` is flagged as partial so code handed one cannot mistake it
  for the object's full contents.
- Tested end to end (codec, store filtering, client restore, client API).

## Non-goals

- `SingleKey`, `LargestKeyLessThan`, `LargestKeyLessThanOrEqualTo`, `ByteRange`, `MetadataOnly`
  read types remain unimplemented. They already exist in `ReadType` and the codec; they stay as
  they are. (Our call — see "Calls made".)
- No change to how full-object reads behave.
- No client-side serving of partial reads from the client object cache (reads do not consult
  that cache today either; it is write-only from the read path).

## Anti-goals (would make this a failure even if it "works")

- **A partial result must never be treated as the whole object.** Concretely:
  - Partial results are **never** inserted into `client.objectCache`. (User decision.)
  - Partial reads **never** trigger opportunistic rebuild — neither `sendOpportunisticRebuild`
    to individual stores nor `opportunisticRebuildManager.markRepairNeeded`. A key missing from
    a partial response must never be interpreted as a deletion and pushed to a store.
    (User decision.)
- The store-side object cache must not be altered by a partial read: filtering is applied to a
  copy of the data being sent, never to the cached `ObjectState`.

## Current state (what exists)

- `common/objects/ReadType.scala`: sealed `ReadType` with `MetadataOnly`, `FullObject`,
  `ByteRange`, `SingleKey`, `LargestKeyLessThan`, `LargestKeyLessThanOrEqualTo`,
  `KeyRange(minimum, maximum, ordering)`.
- `common/network/Codec.scala` (≈ lines 689–741) and `src/main/protobuf/codec.proto`
  (`enum ReadType`, `message Read` fields `key`, `min`, `max`, `comparison`, ...) round-trip all
  of the above. There is no multi-key variant.
- `client/internal/read/BaseReadDriver.scala:26` hardcodes `readType = FullObject()` with a
  `TODO: Support multiple read types`. It unconditionally puts results in `client.objectCache`
  and drives opportunistic rebuild.
- `server/store/Frontend.scala` `readObjectForNetwork(clientId, readUUID, pointer)` ignores the
  read type entirely and always returns the full encoded object data (from the store's
  `objectCache` or via `backend.read` + `pendingReads` → `backendReadComplete`).
- Per-store KV data is the encoded `server/store/KVObjectState` (min/max/left/right plus
  per-key revision/timestamp/IDA-encoded value). The client's `KeyValueObjectReader` restores
  each key independently from the store slices, so filtering by key on the store side is
  compatible with the existing restore logic.

## Design

### Read types

- Add `case class KeySet(keys: List[Key]) extends ReadType` (name is the builder's choice) for
  explicit key lists. No ordering is needed — membership is by key bytes.
- Use the existing `KeyRange(minimum, maximum, ordering)` for ranges. Its semantics are defined
  as inclusive on both ends.
- The caller supplies the `KeyOrdering` for range reads, same as `KeyValueObjectState.keyInRange`
  does today; KV objects do not record their own ordering.

### Wire format

- Add a new `ReadType` enum value for the key set and a repeated `bytes` field (e.g. `keys`) to
  `message Read`. Extend the encode/decode in `Codec.scala` and add round-trip cases to
  `CodecRoundTripSuite`.

### Client read path

- `BaseReadDriver` takes the `ReadType` as a parameter (default `FullObject()`), threaded through
  `ReadManager.read` and the driver factory, and sends it in each `Read` message.
- When the read type is a partial KV read:
  - skip `client.objectCache.put`
  - skip `sendOpportunisticRebuild` and `markRepairNeeded`
  - everything else (retry, threshold restore, locked-transaction reread, `HLCTimestamp.update`)
    is unchanged.
- `KeyValueObjectReader` restore logic is unchanged: it restores whatever keys appear in the
  store responses, applying the existing per-key threshold / deleted / not-restorable logic.
- `KeyValueObjectState` gains a flag (e.g. `val partial: Boolean = false`, or a field carrying
  the `ReadType` that produced it — builder's choice, but it must be a flag on
  `KeyValueObjectState`, not a subtype). The reader sets it for partial reads.
- `BaseAspenClient` implements `readKeys` / `readKeyRange` by resolving the IDA and calling
  `rmgr.read` with the appropriate read type, matching how `read(KeyValueObjectPointer, ...)`
  is done. Add the methods to the `AspenClient` / `ObjectReader` traits alongside `read`, with
  comment-less overloads following the existing `read(pointer)` / `read(pointer, comment)` pattern.

### Store

- `Frontend.readObjectForNetwork` takes the `ReadType`, and `NetworkRead` carries it so the
  deferred `backendReadComplete` path applies it too.
- For KV partial reads the store decodes the object's `KVObjectState`, keeps only the matching
  `content` entries (key set membership, or `ordering.compare` within `[minimum, maximum]`),
  and re-encodes. min/max/left/right are always included. The cached `ObjectState` is not
  modified.
- Revision, refcount, timestamp and `lockedWriteTransactions` are returned as for full reads.
- Partial read types sent for a Data object are an error the builder should reject cleanly
  (client side, before sending, is sufficient).

## Constraints

- Scala 3, quiet syntax per `CLAUDE.md`; match surrounding code.
- Wire compatibility with older peers is not a concern beyond what the codec already handles.

## Calls made (not explicitly decided by the user)

- Missing requested keys are simply absent from the result — not an error.
- Partial results include the object's min/max/left/right; they are small, and tiered lists need
  them to know whether a range continues into a sibling node.
- The caller passes the ordering for range reads.
- Other existing read types stay unimplemented.
- The `sizeOnStore` reported in a partial `ReadResponse` remains the full object's size.

## Left to the builder

- Exact names of the new read type, flag, and proto field.
- Whether to filter by decoding/re-encoding `KVObjectState` or by a streaming scan of the
  encoded buffer.
- Test structure, following existing suites.
