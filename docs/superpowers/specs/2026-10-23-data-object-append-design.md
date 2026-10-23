# Data Object Append and Byte-Range Reads — Design

Date: 2026-10-23
Source: top item of `Todo.txt` ("Implement DataUpdateOperation.Append correctly for slicing IDAs")

## Intent

Support log-style DataObjects: large objects (multiple megabytes) that grow by small appends
(around a kilobyte) and are read in full, by tailing, or from arbitrary offsets. Today an append
can only be done as a full overwrite, so every 1 KiB append resends the whole multi-megabyte
object. That write amplification is the problem being solved.

Append is currently rejected at every layer (`TransactionBuilder.append` throws
`AppendNotYetSupported`, `RequirementsChecker` fails it with `UnsupportedOperation`,
`RequirementsApplyer` throws). The old store-local append was silently wrong under ReedSolomon.

### Why the existing ReedSolomon can't do it

`ReedSolomon` cuts the padded object into k contiguous slices of `n/k + 1` bytes. The slice size
depends on the total length, so appending even one byte moves every data-shard boundary and
changes every parity byte. A tail-local append is impossible with that layout. The existing
`ReedSolomon` IDA is solid and stays unchanged (user decision); a new IDA with a different
layout is added beside it.

## Goals

- A new IDA, `AppendingReedSolomon`, using the same Reed-Solomon math (Backblaze codec) with an
  append-friendly layout (below).
- `append` works on DataObjects in **Replication** and **AppendingReedSolomon** pools, sending
  roughly `appendedBytes × width / restoreThreshold` over the network rather than the whole object.
- Byte-range reads work on DataObjects in Replication and AppendingReedSolomon pools: full reads,
  reads from an arbitrary offset (with or without a length), and tail reads.
- Both appender styles are first-class (user decision):
  - A single writer that carries the current length forward from its previous append.
  - Independent appenders that discover the current length with a cheap tail read.
- Developed first as a standalone, exhaustively tested layout/encoding component, then wired into
  the transaction and store paths (per the Todo note).

## Non-goals

- No change to the existing `ReedSolomon` IDA or its on-disk format.
- No append or byte-range support for classic `ReedSolomon` pools. Both are rejected there
  (user decision), with a clear client-side error before anything is sent.
- No append or range reads for KeyValue objects.
- No store-local disk optimisation. A store may rewrite its whole slice value on each append
  (RocksDB likely does); that is local work, not network traffic, and can be improved later.
- No special handling for heavy contention between many concurrent appenders to one object.
  The revision requirement serialises them; colliding appends abort and retry as usual.

## Anti-goals

- **An append must never silently corrupt an object** (this is how the old implementation failed).
  Every append is computed against an exact, verified prior state; a store whose state doesn't
  match votes abort.
- **A partial (range or tail) read result must never be mistaken for the whole object.** As with
  KV partial reads: never inserted into the client object cache, never triggers opportunistic
  rebuild, and the returned state is clearly flagged as partial.
- Must not depend on any single store being available. In particular, a tail read must not
  rely on reading from data store k-1 (user decision).

## Constraints

- An IDA is fixed per StoragePool, so choosing `AppendingReedSolomon` is a pool-level decision.
  Every DataObject *and* KeyValue object in such a pool uses it.
- Stores already know the size of the slice they hold (e.g. the RocksDB value is
  1-byte header + fixed metadata + slice). They report `data.size` with every read response,
  including MetadataOnly reads.
- Store updates are atomic with the revision change and happen exactly once (confirmed by user).
- IDAs travel over the network via the protobuf `Codec` (`common/network/Codec.scala`); the
  legacy 4-byte `serializeIDAType` form has no other callers.

## Layout: AppendingReedSolomon

Parameters: `width`, `restoreThreshold` (k), `writeThreshold`, and a cell size `w`.

- The logical encoded stream is: object bytes, then zeros, then an 8-byte big-endian logical
  length trailer occupying the last 8 bytes of the final row.
- The stream is divided into **rows** of `k × w` bytes. Row `r`, cell `i` is stream bytes
  `[r·k·w + i·w, r·k·w + (i+1)·w)`.
- Data store `i` (encoding index `i < k`) holds cell `i` of every row, concatenated. Parity store
  `p` holds, for each row, the Reed-Solomon parity of that row's k cells. Because the Backblaze
  codec computes parity position-by-position across shards, encoding is "scatter into the k data
  shards, then one `encodeParity` call over the full shard length."
- Every fragment has the same length, `rows × w`. The number of rows is the smallest count such
  that `objectBytes + 8 ≤ rows × k × w`. If the data fills the final row too far to leave room for
  the trailer, a new row is added. An empty object is one row containing only zeros and the
  trailer.
- Logical byte `o` lives in row `o / (k·w)`, cell `(o mod k·w) / w`, at fragment offset
  `row·w + (o mod w)`. Any logical range maps to a whole-row range, which is the **same slice
  byte range on every store**.
- The trailer is erasure-coded like everything else, so any k fragments recover the length.
- Restore: rebuild missing data shards (as `ReedSolomon` does), gather the stream, read the
  trailer, truncate.

### Validation (on construction)
Same bounds as `ReedSolomon` (`1 ≤ k ≤ width ≤ 127`, `k ≤ writeThreshold ≤ width`), plus
`w ≥ 8`, so the 8-byte trailer always lies entirely within data store k-1's final cell.

### Cell size (our call)
`w` is a pool-creation parameter carried in the IDA's protobuf encoding, defaulting to **64
bytes**. It trades per-append overhead against per-cell scatter/gather cost. Each append resends
`w` bytes per store per touched row, and an empty object costs one row (`w` bytes per store). At
`w = 64` and k = 4, a row is 256 bytes, so a 1 KiB append sends about 5 rows, or 320 bytes per
store. Tests cover a range of `w` (8 through a few KiB), with 64 as the main case.

## Append

The client knows, for the target object: its pointer, pool IDA, current revision, and current
logical length `L`.

Reed-Solomon over GF(2^8) is linear and addition is XOR, so the change to every fragment can be
computed from a sparse **delta** stream that is non-zero only where bytes change:

- the new data bytes at logical positions `[L, L + m)` (previously zero),
- the old trailer value XORed out at its old position (the client knows it: it's `L`),
- the new trailer value `L + m` XORed in at its new position (which may be in a new row).

All of these fall in rows from `L / (k·w)` (the old final row) onwards. The client encodes just
those rows of the delta through the IDA, giving each store a buffer covering the same slice range
`[startRow·w, newRows·w)`.

Each store applies one uniform operation: **extend the slice with zeros to the new slice length,
then XOR the supplied buffer into the slice at the given offset.** For data cells this is
effectively a write (XOR onto zeros). For parity cells it updates parity in place.

The same operation covers Replication: the "delta" is just the new bytes, the offset is the
current slice length `L`, and there is no trailer. XOR onto the zero extension is a plain append.

### Safety
- The append carries the required revision (as `overwrite` does). A store at a different revision
  votes abort. Because updates are atomic and exactly-once, each delta is applied once, against
  precisely the state it was computed for.
- Each store also checks that its current slice length equals the length the append was computed
  against, and votes abort on mismatch. This catches a wrong `L` supplied by the caller that
  happens to pair with the right revision (for AppendingReedSolomon, `L` determines the old row
  count; for Replication it is the slice length itself).
- A store that missed an append can't catch up from a later delta. It falls behind on revision
  and is repaired by the existing mechanisms, with no new trigger (our call): the missed-update
  finalization action and `StoreRepairer`, plus opportunistic rebuild from *full* reads.
  `getRebuildDataForStore` re-encodes the full object through the IDA. Partial reads never
  trigger repair. The end-to-end test drives repair through these existing paths.
- A wrong `L` that still yields the right row count would corrupt the trailer region, and the
  slice-length check can't catch it. This is closed by the **append position** (user decision):
  `append` never takes a raw length, only an `AppendPosition` (revision + logical length) that
  callers can't construct themselves. Only a tail read or a successful append produces one. A
  position from the right revision always carries the right length, and a stale position fails
  the revision check.
- Backstop: since `w ≥ 8`, the old trailer always sits in plain bytes at the end of data store
  k-1's slice. When that store participates, it checks the trailer equals `L` and votes abort on
  mismatch.

## Overwrite, allocation, rebuild and KV objects (our call)

Everything that works today on a DataObject keeps working in an AppendingReedSolomon pool,
using the new layout: allocation and `overwrite` encode the whole object with the trailer, full
reads decode it, and rebuild re-encodes the full object. KeyValue objects in these pools keep
all existing behaviour, including KV partial reads, which filter on unencoded keys at the store.
They simply pay the per-value row-padding overhead. A pool intended mostly for small KV values
should keep using classic `ReedSolomon` or Replication.

## Byte-range reads

Store side: implement the existing `ReadType.ByteRange` in **slice coordinates**: return slice
bytes `[offset, offset + length)`, with an open-ended "to the end of the slice" form, **and** a
"last N bytes of the slice" form (our call). Every fragment ends on a row boundary, so
"last `w` bytes" from any k stores rebuilds the final row, and with it the current length, in
one round trip without knowing the slice size in advance. Out-of-range requests are clipped to
the slice. The response still carries the full slice size.

Client side: the IDA translates logical ranges into slice ranges.
- **Replication:** slice coordinates equal logical coordinates; the logical length is the slice
  size reported by the store.
- **AppendingReedSolomon:** the logical range is widened to whole rows, the same slice range is
  requested from the stores, any k matching responses are decoded, and the extra bytes are trimmed.
  When the returned rows include the final row (known from the reported slice size), the trailer
  gives the current length and is stripped.
- **Classic ReedSolomon:** rejected client-side.

Read patterns built on this:
- **Full read:** unchanged.
- **Read from offset (optional length):** as above.
- **Tail read:** "from the row containing offset X to the end." A writer that needs only the
  current length asks for the final row. A follower asks from its last-seen offset and gets new
  data plus the current length in one round trip.

Consistency follows the existing read rules (our call): the result is assembled only from k
responses that agree on revision, and the slice size reported by *those* responses is
authoritative. Same-revision stores hold identical slice lengths, so "last N bytes" and
"from offset X" select the same rows on each of them. Mismatched revisions are handled exactly
as for full reads (wait, retry, or fail per the existing read driver). Making the read driver
handle range requests is expected work, not a surprise.

## Client API

The builder chooses the exact names and signatures, within these requirements:
- `Transaction.append` takes the pointer, an `AppendPosition`, and the data. It throws a clear
  error for classic ReedSolomon pools. `AppendPosition` (user decision) holds the revision and
  logical length and can't be built by callers (private constructor). It's obtained from a tail
  read, or from the result of a committed append. That lets a single writer carry it forward,
  for example via a future or a callback on the transaction that yields the next position.
- `AspenClient` / `ObjectReader` gain range reads (offset plus optional length) and a tail read.
  The tail read returns an `AppendPosition` and, optionally, the bytes from a given offset.
- Range/tail results are a distinct, clearly partial result type or are flagged as partial
  (mirroring the KV partial reads design), carrying revision, refcount, timestamps, logical
  length, the starting offset, and the bytes.
- `MetadataObjectState` is not extended with a logical size: for AppendingReedSolomon the stores
  can't report it without a decode. The tail read is the way to learn it.

## Build order

1. **Standalone layout component.** `AppendingReedSolomon` as an `IDA` with encode/restore, plus
   pure functions for: logical→slice range mapping, computing the per-store append delta for a
   given `(L, newData)`, applying a delta to a slice, and decoding a row range with trailer
   handling. Exhaustively property-tested with no Aspen stack involved: for random k, width, w,
   initial contents and sequences of appends, applying deltas to encoded fragments must equal
   encoding the concatenated content from scratch. Also restore from every k-subset of
   fragments, range reads at every boundary, empty objects, appends that exactly fill a row or
   split the trailer across a row boundary, and many tiny appends.
2. **Codec and IDA plumbing.** Protobuf encoding of the new IDA (including `w`), pool creation
   able to select it, and any `ida match` sites (e.g. `KeyValueOperation.encode`) handling it.
3. **Store side.** `ByteRange` reads in `PartialRead`; Append in `RequirementsChecker` (revision
   and slice-length checks, allowed for Replication and AppendingReedSolomon) and in
   `RequirementsApplyer` (extend-and-XOR).
4. **Client side.** `TransactionBuilder.append` computes per-store deltas through the IDA;
   range/tail reads in the read path; remove `AppendNotYetSupported` and update the
   `DataUpdateOperation.Append` docs.
5. **End-to-end tests** on both Replication and AppendingReedSolomon pools, including a
   store missing an append and then being rebuilt, and concurrent appenders colliding.

## Left to the builder

- Exact API names and signatures, and the wire format of the per-store append payload
  (offset and new slice length alongside the XOR buffer).
- Whether ByteRange's open-ended form is a sentinel length or a separate read type.
- How `KeyValueOperation.encode` and other generic-IDA sites adapt, so long as KV objects in an
  AppendingReedSolomon pool remain correct.
