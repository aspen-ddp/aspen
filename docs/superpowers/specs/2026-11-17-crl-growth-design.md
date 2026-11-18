# CRL growth and oversized-transaction drops (Todo 4.1, CRL-W2)

## Intent

One transaction whose CRL bytes on a store exceed what a stream can take kills the CRL of every device it touches
(`Stream.writeEntry`'s `require` throws on the CRL thread, `crlFailed` unloads the device), and the client's
retransmit repeats it after every restart (docs/safety/crash-recovery-log.md, CRL-W2). The fix suggested in Todo 4.1
(a client-visible abort with a named `RequirementError`) is **not** used. Instead:

1. **Oversized transactions are dropped silently.** A prepare that would make a transaction's CRL bytes on the
   receiving store exceed the CRL's per-transaction bound is dropped without a reply, exactly as admission drops a
   prepare today when the device's CRL is full. Nothing is voted, nothing is written.
2. **The CRL grows when disk space allows**, raising the device's admission limit, so bursts of ordinary
   transactions find room instead of being held back. Growth is crash-safe at every step and hidden behind the
   `CrashRecoveryLog` trait; SimpleCRL grows by adding one stream file of its existing size.

Silent drops are backpressure: the client's retransmit recovers a dropped transaction if room frees up before the
transaction horizon is crossed. A transaction too large for one entry can never fit and is never recovered that way;
that is an accepted, documented limitation.

## Goals

- No client request can make a device's CRL fail.
- A device's in-flight capacity grows with its CRL, within a configured cap and a free-space floor.
- A crash at any point of a growth leaves a CRL that opens and recovers everything it held.
- CRL sizing defaults scale with the device's capacity.

## Non-goals

- A transaction larger than one stream's per-transaction bound is never stored (option B, entries spanning streams,
  was rejected).
- The CRL never shrinks. A grown CRL keeps its streams until the device is recreated.
- No early failure in `TransactionBuilder` and no client-visible error for oversized transactions.
- Streams are never resized; growth only adds streams of the recorded size.

## Anti-goals

- Growth that can leave the CRL unopenable, or that loses or resurrects an entry, after a crash.
- Growth that can fill the device: stores must keep room for object data.
- Any per-transaction cost on the hot path beyond one comparison in `admitPrepare` (memory:
  no hot-path cost for rare cases). Growth, evacuation stalls and drops are the rare path.

## Approaches considered

- **Insert a stream only when a rotation would not fit (rejected).** Smallest change, but a rotation still copies
  forward the next stream's whole live set in one entry, so the safe admission limit stays about half a stream
  however many streams exist. Growth would only make overflow rarer, never raise the provable limit.
- **Ring with gradual copy-forward (chosen).** Ring order recorded in `crl/sizing`; growth inserts a stream after
  the active one; copy-forward is spread over the active stream's entries and moves only the live pieces lying in the
  stream being evacuated; the writer enters a stream only once it holds nothing needed. The provable limit grows
  with the ring. Recovery keeps its structure.
- **Pool of streams cleaned in any order (rejected).** Best space use (a cleaner picks the least-live stream), but
  recovery's torn-write check relies on a fixed next stream, so every entry would have to name its successor: a large
  rewrite of `Recovery` for little gain under a 20-stream cap.

## Design

### 1. Sizing defaults (device config)

`StorageDeviceConfig` keys become optional; defaults are computed when a CRL is **created**, from the total size of
the file system holding the device directory (`capacity`, `FileStore.getTotalSpace` of the device path, via
os-lib/`toNIO` at the boundary):

| Key | Default | Recorded in `crl/sizing`? |
|---|---|---|
| `crl-num-streams` | 4 (initial streams; also the minimum) | yes (as the ring) |
| `crl-max-file-size-mb` | `capacity / 2048`, rounded down to a 4 KiB multiple, at least 64 MiB | yes |
| `crl-max-streams` | 20 (≈ 1% of capacity at the default file size) | no: read from config at each open |
| `crl-min-free-percent` | 5 | no: read from config at each open |

Examples: 1 TiB device → 512 MiB streams, cap 20 streams = 10 GiB. 10 GiB device → 64 MiB floor.
An explicit `crl-max-file-size-mb` is still honored (tests use 1-2 MiB). An existing CRL opens with its recorded
ring and size whatever the config says, as today. `crl-max-file-size-mb` remains in MiB; the capacity-derived default
is computed in bytes and needs no MiB rounding (4 KiB alignment only). Minimum stream count for a new CRL rises from
2 to 4 (decided: it keeps the per-transaction bound at half a stream while the in-flight limit is at least today's,
see 4). A new CRL asked for fewer than 4 streams is refused with a clear error, like today's `require` at 2;
tests that create 2- or 3-stream CRLs move to 4. An existing CRL recorded
with fewer than 4 streams is not supported (no persisted-data compatibility is required). *(Builder's call where the yaml stays readable: the config written by
`yamlConfig` may omit the defaulted keys.)*

### 2. `crl/sizing` records the ring

The sizing file gains the ring: the stream numbers in ring order, e.g. `ring: 0,3,1,2`. Stream files stay named
`<n>.crl` with `n` in `0 until ringSize`; a grown stream takes the next unused number. `StreamLocation.streamId`
(a short) is unchanged. Everything that today computes the next stream as `(i + 1) % n` (`SimpleCRL`
rotation, `Recovery`'s newer-intact-entry check of the next stream) uses the recorded ring's successor instead.

`crl/sizing` is the commit point of a growth: a stream exists exactly when the ring lists it.

### 3. Crash-safe growth (SimpleCRL)

Steps, each durable before the next:

1. `<n>.crl.tmp` is created and preallocated at the stream size (`FilePreallocation`), and forced.
2. Renamed to `<n>.crl`; the directory forced.
3. On the CRL thread, at a point where the active stream is A: the ring with `n` inserted right after A is written to
   `crl/sizing` (`DurableFiles`, atomic replace + directory force). Only then does the writer know of the stream.
4. The admission limit is raised (4).

Open rules (replacing `checkSizing`'s exact-set check):

- A stream file numbered `ringSize` that the ring does not list is a growth that never committed (crashed between
  2 and 3): it is removed, with a warning. A `<n>.crl.tmp` left by a growth is removed likewise. (The existing
  `.crl.tmp` rule for a CRL *creation*, which has no stream files, is unchanged.)
- Any other unlisted stream file, a listed one missing, or a listed one of another size is `CrlSizingMismatch`, as
  today.
- A listed stream that no entry was ever written to is all zeros: recovery already treats all-zero regions as no
  entry.

Crash after 3 and before any write into `n`: the ring has an empty stream after A; the next rotation enters it.
That is the state the invariant requires, so recovery and the next rotation proceed normally.

Steps 1-2 run off the CRL thread (preallocation may zero-fill on file systems without `fallocate`): on a growth
thread the CRL owns (*builder's call:* the writer thread is not used, so DSYNC writes are not delayed). Step 3 is a
small file write on the CRL thread, done on the rare path only.

**When to grow** *(my call)*: when a store's `admitPrepare` refuses a new transaction because its tier's ceiling is
reached, the Frontend calls a new trait method `crl.requestGrowth()` (default: no-op). The CRL grows by one stream if
none is in progress, the ring is below `crl-max-streams`, and the file system's free space after preallocating one
more stream would stay at or above `crl-min-free-percent` of capacity. One growth at a time; a refused or failed
growth is logged (rate-limited) and retried on a later request no sooner than a fixed back-off. A failed
preallocation removes its `.tmp` and leaves the CRL as it was; it never fails the CRL.

### 4. Gradual copy-forward and the admission limit

Today a rotation copies forward, in the new stream's first entry, every record with data in the stream after it,
rewriting each record whole (`Record.closeStream`, `clearDynamicData`). That is replaced by:

- **Piece relocation.** A Tx's pieces (its txd and each object update) have individual locations; evacuating a stream
  rewrites only the pieces lying in it, plus the record's static entry (which every entry rewrites anyway).
  `Tx.objectUpdateLocations` becomes per-update optional locations rather than all-or-nothing. Stubs keep their
  anchor move. The on-disk static record already names one location per piece, so a partly evacuated record (some
  pieces moved, some not) is representable as is; if the builder finds the encoding must change anyway, that is
  accepted (no compatibility with persisted CRLs is required, CLAUDE.local.md). `Recovery`'s "oldest verified entry
  holding any of its pieces" rule already covers records whose pieces lie in different entries.
- **Evacuation is gradual and first.** While the writer is in stream X, every entry it writes first carries pending
  evacuation of the stream after X (`succ(X)`), up to the space the entry can take, then pending saves. When X is full,
  the writer rotates into `succ(X)` only if `succ(X)` holds nothing needed. Saves that do not fit beside the
  evacuation wait in the pending entry, unvoted. X has no room left to evacuate into once it is full, so the limit
  (below) must guarantee that evacuation of `succ(X)` always completes within X. A growth committed meanwhile
  inserts an empty stream after X, which the writer may enter at once.
- **Invariant (replacing "the stream after the active one holds no needed entry")**: the writer only ever enters a
  stream that holds no needed entry, so a torn write overwrites nothing recovery needs. `oldestEntryNeeded` may point
  into `succ(active)` while it is being evacuated; recovery's chain walk already handles needed entries anywhere.
- **Admission limit.** With per-transaction bound `T = S/2` (5) and ring size `n`, the target limit is
  `(n − 2) · S / 2`: `S` for the minimum 4 streams (twice today's `S/2`), `9 · S` for 20. `CrlAdmission.limitBytes`
  becomes mutable (raised only; lanes recompute from it). **The builder must prove the bound** before coding it, and may lower the formula if needed, but never below `S/2`
  at 4 streams and never by lowering `T` (decided: the minimum of 4 streams exists to keep both): that with live bytes ≤ limit + T + the unchecked charges (late data, stubs, recovered
  state), evacuation of `succ(X)` plus the entry and padding overhead of repacking always fits in X, and that stalled
  saves are written within a bounded number of rotations. The proof goes into crash-recovery-log.md; a randomized
  model test of worst-case placements (all live data in one stream, consecutive dense streams, a maximal T arriving at
  the worst moment) backs it.
- `Stream.writeEntry`'s `require` stays as the fail-stop backstop.

### 5. Oversized transactions are dropped

- New trait method `CrashRecoveryLog.maxTransactionBytes` (default `Long.MaxValue`). SimpleCRL: `S / 2`
  *(my call: the user's rule is "drop what will not fit"; half a stream is what fits with the admission proof's
  margin, and at the 64 MiB floor it still allows 32 MiB per store per transaction, above the 10 MiB recommendation)*.
- `Frontend.admitPrepare` drops a prepare when the transaction's bytes as `CrlAdmission` counts them
  (`TxOverheadBytes` + description + object updates), including data already counted for an admitted transaction,
  would exceed `maxTransactionBytes`. This covers a data-less prepare admitted first and a later prepare bringing the
  data: the later one is dropped, and the store stays as one that never received the data. That needs no new
  handling: if the transaction commits on other stores, this store's missed update goes through the existing
  missed-update marking and repair path, and the data-less admission's charge is released by the existing paths
  (deletion or forgetting of the transaction). The dropped prepare charges nothing.
- Dropped exactly like an admission drop in `Store.receiveTransactionMessage`: no reply, no vote, no CRL write. Logged
  at warn, rate-limited, naming the transaction and its size, so an operator can find the client.
- No client change. The builder confirms and documents what the client sees when every store keeps dropping (the
  driver's existing behavior once retransmits stop at the horizon) in `client/internal/CLAUDE.md`.

### 6. Documentation

- **Architecture.md, CRL section**: the CRL grows (to a cap, within a free-space floor) and never shrinks; a
  transaction whose CRL bytes on one store exceed half a stream is silently dropped and can never commit;
  applications should not write objects whose slice on a store exceeds **10 MiB**.
- `docs/safety/crash-recovery-log.md`: remove CRL-W2 from Open weaknesses; fold the ring, growth steps, open rules,
  invariant, limit proof and the drop rule into the design text (present tense).
- `server/crl/CLAUDE.md`, `server/store/CLAUDE.md` (admission), `StorageDeviceConfig` scaladoc/sample yaml.
- `Todo.txt`: remove 4.1 (numbers of other items unchanged).

## Testing (in order; LockstepThreading where possible)

1. Reproduce CRL-W2 first: an oversized save kills a SimpleCRL today.
2. Oversized prepare dropped without a reply, the CRL alive; same for data arriving after a data-less prepare.
3. Growth crash at each step: `.tmp` left; renamed but not in the ring; ring committed, stream unwritten; first entry
   in the new stream torn. Each opens and recovers every live record.
4. Ring order: recovery and rotation follow the recorded ring after inserts at different positions.
5. Gradual evacuation: worst-case placements from 4's model; stalled saves complete; limit rises after a growth.
6. Growth respects `crl-max-streams` and `crl-min-free-percent`; failed preallocation leaves the CRL unchanged.
7. Capacity-derived defaults (pure function test) and an explicit config still honored; an existing CRL keeps its
   recorded sizing.

## Left to the builder

- The admission-limit proof and final formula (4), and the stall bound.
- The exact `crl/sizing` encoding of the ring.
- The growth thread's ownership and shutdown (it must not outlive the CRL; a growth in flight at shutdown is
  abandoned and its files are cleaned at the next open).
- Back-off value for repeated growth requests.

Effort: L.
