# Ranged Writes — Design

Date: 2026-10-23
Source: top item of `Todo.txt` ("RangedWrites - AppendingReedSolomon should be able to efficiently
update sections of a large DataObject")

## Intent

Binary workflows sometimes need to change a small section of a large DataObject. Today the only
options are to rewrite the whole object or to append. Sending only the changed bytes saves network
bandwidth. Stores will still read and write their entire local slices; that cost is accepted. The
win is on the network.

## Goals

- A transaction operation that overwrites bytes at an offset in a DataObject, sending only those
  bytes (scaled by the IDA's `width / restoreThreshold` for erasure coding).
- Works in **Replication** and **AppendingReedSolomon** pools. Classic `ReedSolomon` is not
  supported: the operation fails with `RangeWriteNotSupported` (new, modelled on
  `AppendNotSupported`), raised at build time.
- Several ranged writes to one object in a single transaction.
- A write extending past the current end of the object grows the object.
- A write starting past the current end fails, unless the call sets `zeroFill = true`, in which case
  the gap is filled with zeros.
- If the pool has a `maxObjectSize`, the resulting object may not exceed it.
- A typed way to obtain the object state a ranged write needs, so the caller cannot forget to wait
  for a read. Fetching must not require the caller to compute alignment themselves.

## Non-goals / anti-goals

- No change to classic `ReedSolomon`.
- No async work inside `Transaction`. All `Transaction` methods stay synchronous. Committing before
  a read finished must be impossible by construction, not merely documented.
- No new backend operations. Stores read and write whole slices (as append does today).
- Not a failure if the caller needs a read round trip before writing; that read covers only the
  needed byte ranges, not the whole object.
- The caller must never have to compute row/cell alignment.

## How it works (background from the append implementation)

Append sends each store an XOR delta (`common/ida/AppendDelta`). Reed-Solomon over GF(2^8) is
linear, so XORing the encoded delta into every slice (parity included) yields the slices of the new
object. For Replication the delta is simply the new bytes.

For an overwrite the delta is `old XOR new`, so the client must know the old bytes in the region
being changed, plus the old length trailer if the length changes. For AppendingReedSolomon, writes
that touch part of a row need the old bytes of that row's affected cells. That is the source of the
alignment requirement. The revision requirement on the object protects against the old bytes
changing between the read and the commit (same mechanism as `AppendPosition.revision`).

AppendingReedSolomon layout: rows of `k * cellSize` bytes, stream byte `p` goes to data shard
`(p % rowBytes) / cellSize` at slice offset `(p / rowBytes) * cellSize + p % cellSize`; an 8-byte
big-endian logical length trailer occupies the last 8 bytes of the final row (the end of data shard
k-1's slice). `appendDeltas` in `common/ida/IDA.scala` already builds a partial-row delta, using
`xorLong` to clear the old trailer and set the new one; ranged writes should reuse that machinery.

## API (shape decided by the user; exact types are the builder's call)

Main method on `Transaction`:

    rangedWrites(pointer, writes: List[RangedWrite], state, zeroFill: Boolean = false): Unit

Convenience: `rangedWrite(pointer, write, state, zeroFill = false)` calls `rangedWrites` with a
one-element list. `RangedWrite` is an offset plus an array of bytes. The user suggested the
signature `rangedWrites(ptr, List[RangedWrite], ObjectRevision)`; it is a suggestion. The final
parameter must carry whatever object state the write needs (see below); the builder chooses the
type and name.

Rules enforced by `rangedWrites` itself, immediately (at the call, not at commit):

- Overlapping writes within one call throw immediately. This is a programming error. (Exception
  type is the builder's call.)
- Writes must be non-empty, offsets non-negative.
- The standard transaction rules apply: the object may be modified only once per transaction, so a
  ranged write cannot be combined with an append, overwrite or another ranged write call on the same
  object (multiple writes are expressed through the one list).

Enforced when the transaction is built/committed (like `AppendNotSupported`):

- Unsupported IDA -> `RangeWriteNotSupported`.
- Resulting length exceeds the pool's `maxObjectSize` -> fail. `maxObjectSize` exists on
  `StoragePoolState` and `StoragePool` but is **not enforced anywhere today**. Enforcement for
  ranged writes is client-side (matches the existing trust model, see the doc comment on
  `DataUpdateOperation`); it requires plumbing the limit into the builder. Whether append should
  also begin honouring it is out of scope.
- Start offset beyond the current length and `zeroFill == false` -> fail.
- For AppendingReedSolomon also keep the existing bound `length <= Int.MaxValue - rowBytes`.

### Obtaining the needed object state (decided: caller supplies it up front)

The caller supplies the state needed for the write as an argument, so the type system forces the
read to complete before the transaction call. Requirements:

- If the caller already holds the full object state from a normal read, that state alone must be
  sufficient. No extra read.
- Otherwise a helper fetches the needed state. It takes the intended writes (or their ranges),
  computes the alignment-expanded ranges itself (including data before and after the target range
  where the IDA requires it, and the trailer when the length may change), and reads them in one
  round. The user must never compute those ranges.
- AspenClient gains a **multi-range read** (several byte ranges in one call), alongside the existing
  single-range `readRange`. It is useful on its own, and the helper builds on it.
- Where the helper lives (AspenClient vs Transaction vs a companion object) and the shape of the
  state type are the builder's choice, provided the above holds and one type works for both sources
  of state (full object, or fetched ranges).
- The state's revision becomes the revision requirement of the update. A stale state aborts the
  transaction the same way a stale `AppendPosition` does.

## Store side

- Add a `DataUpdateOperation` value (for example `WriteRange`), plus the matching proto enum value
  in `src/main/protobuf/codec.proto` and both mapping lines in `common/network/Codec.scala`.
- Each store receives a per-store delta like `AppendDelta` does today. `AppendDelta` holds one
  XOR region ending at the new slice length, which a mid-object edit violates. The delta must
  become **multi-region (sparse)**: a list of (offset, bytes) regions, so distant edits never send
  the bytes between them (decided: sending the span between distant writes defeats the feature).
  Use a versioned encoding that still decodes the existing version-1 format.
- **No alignment-expanded reads are needed.** Parity at a slice offset is a combination of the data
  bytes at the same offset, and by linearity the parity delta depends only on the data deltas
  (`old XOR new` at the edited bytes, zero elsewhere), never on the old contents of neighbouring
  cells. So the client needs only the old bytes under each write plus the object length. For
  AppendingReedSolomon a store's regions are the affected rows' cells (at most `cellSize` bytes per
  affected row); for Replication they are exactly the edited bytes.
- `RequirementsChecker` verifies the revision, that an update payload is present, and that the
  store's slice length (and trailer on the store holding it) matches what the client assumed.
  `RequirementsApplyer` applies it: zero-extend if growing, then XOR/overwrite. Both matches are
  exhaustive over the enum, so the compiler will point to every site.
- Mismatch yields a requirement error like `AppendMismatch`.
- Replication: stores can either apply the XOR delta or overwrite the range; either is acceptable.

## Testing

Model on the append suites: `AppendIntegrationSuite` (end to end, stale revision aborts, repaired
lagging store, classic RS rejection), `AppendDeltaSuite`, `AppendingReedSolomonSuite` (delta versus
full-encode equivalence: applying the ranged delta must equal re-encoding the whole new object),
`RequirementsApplyerSuite`/`RequirementsCheckerSuite`, `CodecRoundTripSuite`, `TransactionBuilderSuite`.
Cover: writes within a row, spanning rows, at row/cell boundaries, extension past the end, zero-fill
gap, gap without zero-fill failing, overlap throwing, max-size, multiple writes in one call,
fetched-ranges state versus full-object state, Replication and ARS.

## Left to the builder

- Names and exact types (`RangedWrite`, the state type, the helper, exceptions).
- Exact delta encoding details (multi-region is required).
- Where the state-fetching helper lives.
- Whether Replication applies XOR or direct overwrite.
