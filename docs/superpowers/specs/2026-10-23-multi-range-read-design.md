# Multi-Range Data Object Reads — Design

Date: 2026-10-23
Source: top item of `Todo.txt` ("readRangedWriteState is implemented in terms of calls to readRange")

## Intent

`ObjectReader.readRanges` and `readRangedWriteState` are currently built from one independent
`readRange` per range plus one `readAppendPosition`. Each is a separate read request, so a concurrent
writer can leave the pieces at different revisions. `RangedWriteFetch` retries up to three rounds and
then fails with `UnstableRangedWriteState`. Combining bytes from different revisions would silently
corrupt parity, so the retry is necessary, but the failure is avoidable: add a read type that fetches
several byte ranges of a data object **and its length trailer** in one request, the same way
`KeySet` reads several keys of a key-value object in one request. Every piece then comes from one
revision per store by construction.

## Goals

- A new `ReadType`, `ByteRanges`, handled end to end: model, protobuf codec, store-side clipping,
  client-side restore.
- `readRanges` issues exactly one read request per call. All returned `DataObjectRangeState`s share one
  revision.
- `readRangedWriteState` issues exactly one read request: the ranges for its writes plus the length
  come back together. No retry loop, no `UnstableRangedWriteState` from the revision-mismatch path.
- `ByteRanges` replaces `ByteRange`, `ByteRangeToEnd` and `SliceTail`, which are deleted. `readRange` and
  `readAppendPosition` are re-expressed on it, leaving one data-object partial-read type and one
  reader code path.
- Public signatures of `readRanges` / `readRangedWriteState` / `readRange` / `readAppendPosition` stay as
  they are.
- Tested end to end (codec, store clipping, restore for Replication and AppendingReedSolomon, client
  API, concurrent-writer case).

## Non-goals

- No change to the behaviour or signatures of `readRange` and `readAppendPosition`.
- No wire compatibility with the removed read types (user decision: not required).
- Classic ReedSolomon still fails with `RangeReadNotSupported`.
- No client-object-cache interaction (as with every other partial read).

## Anti-goals

- A multi-range result must never be treated as the whole object: never cached in `objectCache`, never
  triggers opportunistic rebuild. Add `ByteRanges` to the partial-read list in `BaseReadDriver`
  in place of `ByteRange` / `ByteRangeToEnd` / `SliceTail`.
- Must not return ranges from mixed revisions. If the reader cannot get a threshold of stores
  agreeing on one revision it fails/retries through the existing `BaseObjectReader` machinery, not by
  stitching.

## Design

### Read type

```scala
/** Several slice-coordinate ranges, plus optionally the last `tail` bytes of the slice */
case class ByteRanges(ranges: List[(Int, Int)], tail: Int) extends ReadType   // (offset, length)
```

Ranges are in slice coordinates. The client produces them with
`ida.sliceRangeFor` per requested range. `tail` is `ida.lengthProbeBytes`, so the response always
carries the length trailer (0 for Replication, `cellSize` for AppendingReedSolomon).

### Collapse of the single-range read types

`ByteRange`, `ByteRangeToEnd` and `SliceTail` are removed from `ReadType`, `PartialRead`, `Codec`,
`BaseReadDriver` and `DataObjectRangeReader`. Their replacements:

- `ByteRange(o, l)` → `ByteRanges(List((o, l)), 0)`
- `ByteRangeToEnd(o)` → `ByteRanges(List((o, Int.MaxValue)), 0)` (the store clamps to the slice size)
- `SliceTail(n)` → `ByteRanges(Nil, n)`

`readRange` becomes a call to the same internal one-request path as `readRanges` with a single range and
`tail = 0` (so it does not pay for a trailer it does not need), and `readAppendPosition` sends an empty
range list with `tail = lengthProbeBytes`. Existing tests that construct the removed types are updated to
`ByteRanges`; their expectations do not change. In `codec.proto` the removed enum values
(`BYTE_RANGE = 2`, `BYTE_RANGE_TO_END = 8`, `SLICE_TAIL = 9`) are deleted outright, as no compatibility is
needed.

### Store side (`PartialRead`)

For `(ObjectType.Data, ByteRanges)` the store clips each range (both ends clamped to `[0, size]`), then
the last `tail` bytes of the slice (or the whole slice when shorter), and returns the **concatenation in
request order**, ranges first and tail last. Ranges are not deduplicated or merged by the store;
overlap simply repeats bytes. This keeps the store trivial and the layout a pure function of
`(ranges, tail, sliceSize)`.

### Wire format

Add `READ_TYPE_BYTE_RANGES = 10` and, to `Read`, `repeated uint32 range_offsets = 13` and
`repeated uint32 range_lengths = 14` (parallel lists). The existing `length` field carries `tail`.
Additive only.

### Client side

- `DataObjectRangeReader` has a single path for `ByteRanges`. It already receives `sliceSize` (from `sizeOnStore`) with each response. Since
  every store clips identically, it recomputes the clipped length of each range and the tail, splits
  every store's concatenated buffer at those boundaries, and calls `restoreRange` once per piece
  (segments are equal-length across stores, as `restoreRange` requires). The tail piece yields the
  object length; piece restores that reach the end of the slice may also yield it.
- New `ObjectState` subtype (name left to the builder, e.g. `DataObjectRangesState`) carrying the
  common revision/refcount/timestamps, the object length, and one `DataObjectRangeState` per requested
  range in request order. `BaseReadDriver` selects the reader for `ByteRanges`.
- `readRanges`: normalise and validate (negative offset/length fail with `IllegalArgumentException`
  as `readRange` does; empty list still resolves the IDA first so unsupported IDAs fail with
  `RangeReadNotSupported`), issue one `ByteRanges` read, trim each piece with the existing `trimRange`.
- `readRangedWriteState`: one `ByteRanges` read for `writes.map(w => (w.offset, w.data.length))`;
  the position comes from the same result, then `RangedWriteFetch.combine` builds the state.
- `RangedWriteFetch.fetch`'s retry loop, `MaxAttempts` and the revision-mismatch branch of `combine`
  are no longer reachable and are removed. Whether `UnstableRangedWriteState` has other uses is for the
  builder to check; delete it only if it has none.

## Constraints

- Scala 3 quiet syntax, indented if/then/else, os-lib — per `CLAUDE.md`.
- Update `CLAUDE.md`'s IDA paragraph if it describes `readRanges` behaviour, and remove the item from
  `Todo.txt`.

## Left to the builder

- Name and placement of the new state class; internal helper structure.
- Whether `readRanges` merges adjacent/overlapping requested ranges before sending (optimisation only).
- A sane upper bound on range count per request, if one is wanted.

## Calls made (mine, not discussed with the user)

- `ByteRanges` always carries a tail field rather than making it optional. `readRange` and `readRanges`
  send `tail = 0`; `readAppendPosition` and `readRangedWriteState` send `tail = lengthProbeBytes` because
  they need the object length.
- Store-side concatenation with no merge, rather than a structured multi-part response (avoids any
  `ReadResponse` change).
- Removing the retry loop outright rather than keeping it as a fallback.
- Deleting the old enum values rather than reserving them, following the user's "cleanest changes,
  no compatibility" direction.
