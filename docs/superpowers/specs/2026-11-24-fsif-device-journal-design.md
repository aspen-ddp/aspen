# FSIF device journal: one preallocated journal per storage device

Status: built, 2026-11-24 (rulings and differences: section 10). Replaces each FSIF store's own
`journal.a`/`journal.b` with one journal per storage device, shared by every FSIF store on it and preallocated so that
a force changes no file metadata. Paths are relative to `src/main/scala/org/aspen_ddp/aspen/server/` unless they start
with `src/`, `docs/` or `scripts/`.

## 1. Why

`Todo.txt`, "FSIF throughput after device I/O isolation", has the measurements. In short:

- Each FSIF store journals its commits in its own file. A batch is one blocking write plus `force` on a device data
  thread (`JournalWriter.send`). The vdisk workload commits every transaction on all 16 stores of its pool, so each
  transaction needs 16 fsyncs on the device. Each one appends, so each fdatasync is an ext4 jbd2 commit (~10 ms
  here). ext4 merges only the fsyncs in flight together, so a device needs about as many blocking data threads as
  it has stores. With fewer, transactions wait whole fsync rounds, and reads queue behind the forces.
- Microbenchmark (16 real FsifBackends on one `ThreadedDeviceIo`, randrw-like): 49 ops/s on 1 data thread, 131 on 4,
  374 on 12 (today's default on 24 CPUs), 767 on 24. One store on one thread: 962 ops/s.
- One shared journal file written by one thread: 490 tx/s appending, 1355 tx/s preallocated (pure overwrite, no
  metadata change). Sixteen separate journals peaked at 381 tx/s and needed 16 threads to get there.

So batching across a device's stores, in one file whose forces change no metadata, gives more throughput on 1-2
threads than per-store journals give on any thread count. The device CRL already works this way (a ring of
preallocated streams, DSYNC writes, one writer per device).

## 2. Goals, non-goals, anti-goals

**Goals**
- One journal per storage device, shared by all its FSIF stores, written by one thread with one batch in flight.
  Records appended meanwhile form the next batch.
- The journal is a fixed ring of zero-filled files. A journal write never allocates or extends a file, so its force
  is a data-only flush, and a full device never fails a journal write.
- Throughput: the 16-store microbenchmark on 1-2 data threads per device at least matches today's best (767 ops/s,
  24 threads). Then a fio run (`docs/host-threading-measurement.md`) shows no regression against baseline.
- Every invariant of `store/backend/fsif/DESIGN.md` section 3 still holds, re-argued where it changes (section 6).
- A store directory at rest, whenever it leaves the device journal's reach (transfer, move to another device), holds
  its whole state (the "detach" rule, 4.6).

**Non-goals**
- No change to the CRL, its format, its retention or its proofs (approach B was rejected, section 3).
- No evacuation of a lagging store's records (decided: 4.5).
- No change to RocksDB.
- No backward compatibility with persisted FSIF stores (CLAUDE.local.md).
- No io_uring or other asynchronous file I/O.

**Anti-goals** (failure even if it works)
- A store declared lost by a systemic bug in the new code on every device at once. Every new "lost" trigger is listed
  in section 5 with what a false trigger would look like, as DESIGN.md section 4 does.
- A journal record applied to the wrong directory (another copy, an earlier tenancy, a transferred-in copy).
- A copy reloaded after a crash without the acknowledged writes the journal held for it.
- Making the device need many threads again by another route, e.g. a per-store force that every commit waits for.

## 3. Approaches considered

- **A. A new FSIF device journal, built the way the CRL is built (chosen).** It is separate from the CRL. Two logs
  and two forces per transaction per device stay (the CRL's vote save and FSIF's commit), but the FSIF one becomes a
  single batched force instead of one per store. The CRL and its safety proofs are untouched, and FSIF keeps its own
  retention, tied to in-place writes and index checkpoints, which does not fit the CRL's per-transaction retention.
- **B. FSIF records written into the device CRL.** One log and one writer per device, with vote saves and commit
  records sharing batches. Rejected: it reworks the CRL's format, retention and proofs, and changes the backend
  commit contract.
- **Per-store journals, preallocated.** Each force becomes data-only (1.4 ms against 5 ms alone), but each still
  issues its own device flush, and 16 at once got worse past 8 threads. It does not remove the thread-per-store need.
- **More data threads, or a separate force pool sized to the stores.** These work around the problem rather than fix
  it, and they are the thread cost the Todo describes.

## 4. Design

### 4.1 Files and format

`<device>/fsif-journal/` holds:
- `sizing`: the file count, the file size and the journal's random id (UUID), written durably before any journal
  file. Recorded once, at creation; an existing journal always opens with its recorded sizing (as the CRL's
  `crl/sizing` does).
- `<n>.fj` for `n` in `0 until count`: each exactly the file size, **zero-filled with real writes and forced at
  creation**, never `fallocate`d (a first write into an unwritten extent converts it, which is a metadata change and
  costs the jbd2 commit this design removes). Creation goes through a scratch directory renamed into place once
  every file is written and forced, as the CRL's replacement does, so a device never holds a partial journal.

Defaults: 4 files of 256 MiB. Device config keys `fsif-journal-files` and `fsif-journal-file-size-mb` override them
for a new journal only. Minimum 3 files. The file size must be a multiple of 4096.

**Creation is lazy** (builder-check call): a device gets its journal just before the first FSIF store is created on
it (a lane job ahead of `BackendOpener.create`), so RocksDB-only devices never pay for one. "No journal and no FSIF
directory naming one" is a normal state, not loss (J2 needs a directory whose index names a journal). Tests give
their devices a small sizing (e.g. 3 files of 1 MiB) through the device config.

**File header** (first 4 KiB of each file, rewritten whenever the writer enters the file, with a DSYNC write before
any batch goes into it): magic, journal id, the file's pass salt (a fresh `SecureRandom` long per entry), the serial
of the first batch the file holds this pass (`first`), the **oldest needed serial** at the moment of entry (the
minimum, over live attachments, of the oldest serial each still needs; `first` if none), and a CRC over the header.

**What a file holds** (builder-check call): a file whose header is valid holds this pass's batches `first`,
`first + 1`, ... contiguously from just after its header. Its batches end where the next file in serial order
begins: every serial from `first` up to the next file's `first - 1` must be present and valid in it, otherwise the
journal is corrupt. Only the newest file (the greatest valid `first`) may end in a torn tail. A file whose header
is invalid holds nothing: it is a file being entered when a crash tore its header rewrite, or one never entered.
Whether it hid needed batches is decided by the needed-range rule in 4.7 step 1, never by the header alone.

**Batch**: length, batch serial (a device-wide `Long`, rising by one per batch across files and passes), records, and
a CRC over the journal id, the file's pass salt (neither stored in the batch), the length, the serial and the records.
A stale batch from an earlier pass of the file, or another journal's bytes, can therefore never pass as a batch.

**Record**: today's Mapping and Delta records (`JournalFormat`), each prefixed with its **attachment id** (4.2). A
batch holds records of many stores, in append order.

Writes go through a DSYNC `FileChannel` (one write per batch, as `StreamWriter` does), padded to 4096 bytes so no
batch shares a sector with the next. A batch never spans files: when it does not fit what is left of the current
file, the writer moves to the next file (4.5).

### 4.2 Attachments and the index

An **attachment** is one open of one FsifBackend against one device journal. Its id is a fresh UUID chosen at open.

Each store's index file (`IndexFile`) gains: the journal id, the attachment id, the **covered serial** (the highest
batch serial whose records for this attachment are reflected in this index and in forced data; records of the
attachment above it are the ones replay applies), and a **detached** flag (set by the index a detach writes, 4.6,
and by the index device-load replay writes, 4.7). A directory whose newest valid index is detached, or names
another journal, needs nothing from this journal.

At open, before its first append, a backend writes and forces a fresh index (seq + 1) naming the device's journal id,
its new attachment id and the journal's current tail serial. This is an ordinary index switch (index s+1 over index
s), so recovery's rules for two index files are unchanged. A directory whose newest valid index names another
journal id (transferred in, moved from another device, created elsewhere) has no records here by definition.

Records are matched to a directory only by attachment id, never by store id, copy id or path. So renames within the
device, several copies of one store on a device, rebuild staging directories and earlier tenancies cannot receive
another directory's records.

### 4.3 The device journal at runtime

One `FsifDeviceJournal` per loaded storage device, owned by its own thread (`dev-<label>-fjournal`), created by the
device's `DeviceIo` beside the data and serial pools. It is the only writer of the journal files.

- `append(attachment, records): Future[Unit]`, callable from any store's serial executor: enqueues on a lock-free
  queue. The journal thread takes everything queued, encodes one batch, writes it (DSYNC), and completes each
  appender's Future by posting to that store's serial executor (the Future's callbacks run there, as
  `JournalWriter`'s do today). One batch in flight.
- The order of one attachment's records in the journal is its append order, so per-object journal order (DESIGN 3.3)
  holds as today: everything still goes through the backend's `appendRecords`.
- Each completion carries the batch serial, which the backend records as the newest durable serial of its records.
- `JournalWriter`'s retry and `failAll` move into the device journal (4.8).

`FsifBackend` no longer opens `journal.a`/`journal.b`. Its `JournalWriter` is replaced by its attachment handle.

### 4.4 Per-store checkpoints

A store's checkpoint keeps its purpose (write an index that covers its applied records) but loses the journal
switch:
1. **Snapshot and Draining**: snapshot the index as of the store's last appended record (the last append the backend
   issued; every issued append was admitted, 4.5, so it will be written). Until that append is durable, its batch
   serial taken as `S`, the store's new appends wait in order on its serial executor (a drain buffer), and are then
   issued in order. The journal merges an attachment's queued appends into one batch, so an append issued right after
   the snapshot could share `S`'s batch; holding it until `S` is durable gives every post-snapshot record a serial
   above `S` (corrected by ruling R8; the original text said post-snapshot appends always have serials above `S`).
2. **Applying**: wait until every Delta appended up to the snapshot is applied (forced in place) or cancelled, as
   today.
3. **Barrier**: wait until everything appended so far is durable: a cancelled Delta may still be mapped in the
   snapshot, and the record that cancelled it came after the snapshot.
4. **Finishing**: write index s+1 with covered serial `S` and force it, then the old index file is emptied (as
   today).

There is no header step. Appends wait only while Draining, at most one batch per checkpoint. The trigger changes
(4.5).

### 4.5 Space, reclaim and holding

The ring is used in order. The writer can enter file `k+1` (rewriting its header) only if no **live attachment**
needs any batch in it: for every attachment with records in `k+1`'s previous pass, its covered serial (from its last
finished checkpoint) is at least the highest such serial, or the attachment is **released**.

- A **live attachment** is one whose backend is open, or one closed without a detach (a fail-stopped close, 4.6) and
  not yet released. The journal tracks, per attachment, its oldest needed serial.
- An attachment is **released** when it detaches (4.6), or, for one that did not detach, when the host reports that
  its copy's move out of replay's scope (to `lost/` or `pending-delete/`, or its delete) is durable
  (`StoreRebuilds.moveToLostOnLane`'s result, Durable, and the other `CopyMoves` results). Until then a crash could
  reload that copy from where it was, and replay must still find its records (DESIGN.md "A crash between a FailStop
  close and its move ..."). A copy is never moved while its backend is open (host P5), so every move follows a close.
- An idle attachment (no records above its covered serial) needs nothing and pins no file, however old its covered
  serial: the oldest serial it needs is "none".
- **Checkpoint pressure**: when the writer is in the last free file, the journal asks every live attachment whose
  oldest needed serial lies in the oldest file to checkpoint now. Today's size trigger (`maybeCheckpoint`) becomes:
  checkpoint when the store's unapplied records span more than half the ring, or on that request.
- **Admission, before any mutation** (builder-check call; it keeps DESIGN 2.9's no-deadlock argument). Today's
  `admitted` step stays where it is (every commit, repair, repairDelete and `rebuildFlush` chunk, with the same
  `appendBound`s), but it reserves its bound against the **device** journal's free space instead of the store's
  journal: the room left in the current file plus every free file, less what is already reserved. An operation that
  does not fit waits in the store's `capHeld`, in arrival order, having mutated nothing and appended nothing; the
  journal tells each store when room returns, and asks for checkpoints (pressure, above). An admitted operation's
  appends therefore always fit, so an issued append is never held and every checkpoint's Barrier completes. A
  reservation is returned once the operation's appends are written (unused bytes back at once). `journalHardLimit`
  goes; `MappingAppendSize` headroom for a compaction move's Mapping stays (`journalHasRoomForMove`), now reserved
  from the device journal. A single operation larger than one file minus its header can never fit: the file size
  must exceed `appendBound(MaxSegmentPayloadSize)` plus `MappingAppendSize`, or the sizing is refused.
- **Close and abandon**: operations held in `capHeld` are abandoned or failed exactly as today (`abandonHeld`,
  fail-stop). Appends already issued are written even after their store closes; their Futures are dropped if the
  store abandoned them. Such records are replayed at the next device load if the attachment is still live (they were
  never acknowledged, which is harmless, as unacknowledged records in today's journal are), and ignored if it was
  released.
- **Headroom**: index writes are not journal appends, so a checkpoint never needs journal space.
- **Decided (2026-11-24): no evacuation.** A store that cannot checkpoint (frozen by ENOSPC on its data file) holds
  the ring, and then every FSIF store on the device holds its commits until it can; reads go on. Out-of-space is
  per file system, so its neighbours are almost always frozen too. Accepted cost: a store whose own writes would
  still fit (EDQUOT only) waits as well, and so does the device while a fail-stopped copy's move to `lost/` is
  retried (`retryPendingLost`, each device check).

### 4.6 Detach on close

Every close of an FsifBackend except a fail-stopped one **detaches**: it quiesces, finishes a checkpoint covering
every record it appended (forcing its in-place writes), and then tells the journal it is released. `close()`'s
Future completes only after that. A close of a frozen store cannot finish its checkpoint: it abandons its held work
as today (`abandonHeld`) and does not detach, so it stays live and its records stay in the ring for the next device
load to replay.

Every route that takes a directory out of the journal's reach passes through a close first: transfer out
(`ClosePurpose.TransferOut`, before `closedForTransfer`'s extraction and move to `outbound/`), shutdown, relinquish,
reload, and a rebuild staging store's close before its rename into `rebuilt/`. So a shipped or renamed directory
holds its whole state. A closed store that was never reloaded after a crash has no records left either: the device
load replayed them (4.7) before anything could ship it.

A fail-stopped close (`ClosePurpose.FailStop`, `Unserved` after a failure) does not detach; its attachment is
released when its copy's move to `lost/` is durable (4.5).

### 4.7 Device load: open, recover, replay

A new `FsifJournalOpen` job on the device's management lane runs in `DeviceDiscovery.loadOwnedDevice` after the CRL
open and before P2 (`retireDuplicates`), with the copy listing as input. It touches no control state; its result
(`FsifJournalOpenResult`: Opened, Absent, Lost, Failed, mirroring `CrlOpenResult`) is applied by the control thread.
A device with no journal and no directory whose index names one is Absent: nothing is created until its first FSIF
store create (4.1).

1. **Recover the ring** (builder-check call; never keyed on directories' covered serials, which an idle store leaves
   arbitrarily old). Read every file header; order the valid ones by `first`. The **needed range** starts at the
   newest valid header's oldest needed serial and ends at the tail. Every serial in it must be found, in order, in
   the files as 4.1 defines them; a missing one (a gap, a rotted header hiding batches, a batch failing its CRC
   before the newest file) is corrupt. In the newest file, the first batch that fails its CRC or has the wrong
   serial ends the journal: a torn tail if no CRC-valid batch with the next serial exists later in that file
   (`JournalFormat.findLater`'s rule), otherwise corrupt. A CRC-valid batch whose records do not parse is corrupt.
   Batches below the needed range are not read.
2. **Replay into every attached directory.** The directories are the device's copies in every phase except `lost/`
   and `pending-delete/`, plus the FSIF rebuild staging directories under `rebuilding/`. This is every directory
   that can later be loaded, resumed or shipped (the device load offers nothing else to `tryLoadStore`, and a
   staging resume opens only `rebuilding/`). For each whose newest valid index names this journal and is not
   detached, run today's `FsifRecovery.recover` steps 2-5 with that attachment's records above its covered serial as
   the journal: replay selection, every post-replay check (L7-L12, including the D0 slot bound and "mapped slot past
   EOF"), absolute Deltas written and forced, then an index s+1 written with the journal's tail serial as covered
   serial and the detached flag set. After this, no directory needs any batch; the ring's contents are dead.
3. **Open the writer** positioned after the tail, entering the next file (header rewrite, oldest needed serial =
   its own `first`) before the first batch. When the tail file holds no batch of its pass (the tail is at its
   header: a crash or a failed write came between its entry header and its first batch), the writer re-enters the
   tail file itself instead, with a fresh salt: its header already claims the next serial as its `first`, and
   entering the next file would give a second valid header that `first` (R22). Every valid header's `first` is
   thereby unique; should two ever be equal, step 1 orders a file holding no batch at its `first` before the one
   that does, and finds two that both hold one corrupt.

Records whose attachment matches no listed directory are ignored: their directory was released (moved to `lost/`,
deleted, or detached before shipping).

Store opens then run `FsifRecovery` without a journal (builder-check call, the refactor boundary): load the newest
valid index (L1), require it detached and naming this journal or another journal (a transferred-in or newly created
directory; an index naming this journal but not detached at store open is a bug, thrown as an `IOException` so the
load retries rather than declaring a loss), run the checks that need no journal (L11, L12), and attach (4.2). The D0
bound (L10) and the replay checks run only at device-load replay. A load retry later in the device's life opens the
same way: replay has already run for every directory at this load, and the directory's own earlier attachment, if
any, detached or was released.

A detach-or-not check guards shipping (builder-check call): `closedForTransfer`'s extraction job also reads the
copy's newest index and refuses the transfer (logged at error, the retransmit retries) unless it is detached or
names another journal.

**One directory's replay fails** (any of L7-L12, or an I/O error writing its data file): that copy alone is moved to
`lost/` inside the job, through `CopyDisk.relocate` and CopyMoves' seams, exactly as the lost-CRL path moves copies
(below); the others go on. A staging directory is removed instead (as `RebuildingStore.openStaging` treats a
`BackendStateLost` staging store). An I/O error that is transient (`DeviceDiscovery.isTransient`) fails the job,
leaving the device unloaded for the next scan, with nothing moved.

**The journal is lost** (corrupt per step 1, its `sizing` unreadable or mismatched, its directory or files missing on
a device holding FSIF copies whose index names it): decided 2026-11-24, **every FSIF copy on the device is moved to
`lost/`** and rebuilt; RocksDB copies are untouched. The procedure is the lost-CRL path's
(`DeviceCrlDisk.open`/`replaceLost`), step for step:
- decide every copy to move before any is moved; a read that fails leaves all where they are and fails the job;
- move each with `CopyDisk.relocate` to `lost/` (T10); a move that fails or lands unforced fails the job, the device
  left unloaded and the journal untouched for the next scan, the unforced moves returned to be held for the resync
  as `CrlOpenResult.Failed`'s are;
- force every phase directory a copy may have left or entered, then each `lost/` copy with the directories an
  earlier unforced move of it left (`CopyMoves.unforcedUnder`);
- remove every FSIF staging directory under `rebuilding/` (its rebuild starts over);
- keep the old directory aside as `fsif-journal.lost-<uuid>`, create a fresh journal in `fsif-journal.creating`, and
  rename it into place durably;
- log at error, naming the cause, as `DeviceCrlDisk.open` does for each cause.
The reconcile pass then turns each `lost/` copy into a rebuild, as for a lost CRL. A refusal-only case mirrors the
CRL's `Refused` (the journal locked by another writer, or files that do not match a readable sizing: damage or a hand
edit, the device refused until repaired, nothing moved).

The journal's lock (`<device>/fsif-journal/lock`) works as `CrlLock` does.

### 4.8 Failures at runtime

- **A journal write fails** (any error; a DSYNC write into preallocated space cannot be ENOSPC, but if one ever
  reports it, it is treated as any error): the journal fails. Every live attachment's backend fail-stops through its
  own `failStop` (today's L13), so each takes the existing fail-stop route: `onFatal` posts `BackendFailed`, the store
  is closed with `ClosePurpose.FailStop`, its copy moved to `lost/`, rebuilt. Later FSIF opens on the device fail
  with an `IOException` (a load retry) until the device is reloaded; the next device load recovers the journal as
  after a crash.
- Reads, in-place writes, data forces, copy-on-write, compaction and their ENOSPC freeze stay per store, as today.
- A data force failure (fsyncgate, DESIGN 3.8) is unchanged.

### 4.9 Threads

- Per device: the journal thread (new), the data pool and the serial pool, as today.
- The journal thread is a `DeviceIo` thread like the pools' (builder-check call): every write runs through the
  `gate` and is recorded in the `DeviceIoObserver` (kind `fsif-journal`), so device hang detection
  (`DeviceHealthJudge`) sees a journal write stuck in DSYNC as it sees a stuck data task. `LockstepDeviceIo` runs the
  journal's writes as deterministic tasks, as it runs the pools'. The health probe is unchanged.
- The data pool no longer carries journal forces, only reads, in-place writes, data forces, copy-on-write, moves and
  index writes. Once measured (section 7), `DeviceIo.defaultDataThreads` is lowered to what the measurement
  supports, expected 2-4.
- `FsifBackend.sharedIo`/`sharedSerial` stay for tests; tests that build a backend without a device give it its own
  `FsifDeviceJournal` over `FsifFiles` (so `SimulatedFsifFiles` crash sweeps work), and `LockstepDeviceIo` runs the
  journal's work deterministically as it runs the pools'.

## 5. New and changed "lost" triggers (DESIGN.md section 4)

| # | Trigger | What a false trigger from a systemic bug looks like |
|---|---|---|
| J1 | Device journal corrupt (a valid batch after an invalid one, records that do not parse, a needed serial missing) | A change to the record or batch encoding, the CRC's inputs or the serial arithmetic: every device's FSIF copies after an upgrade, at once |
| J2 | Journal missing on a device holding a directory whose index names it | A journal directory renamed by new code, or an index's journal id read wrongly |
| J3 | One directory's replay fails (L7-L12 moved from store open to device load) | As for L7-L12 today, but now found at device load |
| J4 | A journal write fails at runtime (L13 at device scope) | An error classifier or an I/O path bug that fails the device's journal: every FSIF store on the device fail-stops |

L4-L6 (journal corruption, seq rules of the two per-store journals) disappear. L2's journal half disappears.

## 6. Invariants to re-argue (DESIGN.md section 3)

- **3.1 Commit completion means durable**: an append's Future completes only after its batch's DSYNC write returns.
- **3.2 Durable-before-map**: unchanged; data forces stay per store, before the Mapping's append.
- **3.3 Per-object journal order**: one attachment's records are in append order in the device journal.
- **3.4 Replay selection**: unchanged, applied per attachment.
- **3.5 Slot-freeing**: "unmapping record durable" now means its batch is durable.
- **3.6 Recovery bound**: computed per directory from its index and its own replayed Mappings.
- **3.7 Checkpoint switch and Barrier**: the journal switch goes; what remains is that index s+1's covered serial S
  never exceeds a record not yet applied or not yet durable, and that no record left out of the snapshot is at or
  below S: S is the batch serial of the snapshot's last append, and later appends wait until that batch is durable
  (Draining, ruling R8), so they land in later batches; Applying and Barrier cover the rest (4.4). The ring never
  reuses a file holding a record above a live attachment's covered serial (4.5). DESIGN 2.9's no-deadlock argument
  carries over because admission still happens before any mutation, now against device journal space (4.5): no
  admitted operation waits for a held one, every issued append fits, and the appends Draining holds belong to admitted
  operations and are issued once S is durable, so every Barrier completes.
- **3.9 Torn tail versus corruption**: re-proved for the ring: per-pass salt and journal id in every batch CRC, a
  header rewritten before a file's first batch of a pass, one batch in flight.
- **3.11 Idempotent recovery**: device-load replay is per directory today's steps 2-5, whose writes are idempotent,
  and the writer does not start until every directory has its new index; a crash inside replay replays again.
- New: **records reach only their directory** (attachment id, 4.2); **released means unreloadable** (4.5: an
  attachment that did not detach is released only once its copy is durably in `lost/` or deleted).

## 7. Measurement and success

1. The microbenchmark from the investigation (kept outside the repo at
   `/tmp/claude-1000/-home-rakis-devel-aspen/6dc4a5b3-53e4-4944-bd8a-6530da645acc/scratchpad/bench/FsifDeviceBench.scala`;
   to be added under `src/test/scala/org/aspen_ddp/aspen/bench/` as a runnable `main`, not a suite): 16 stores on one device at 1, 2, 4 and 8 data threads.
   Success: at 1-2 threads at least today's 767 ops/s.
2. `docs/host-threading-measurement.md`'s fio procedure, both trees back to back, alternating, three runs each.
   Success: FSIF randrw and seqwrite throughput and fsync latency no worse than the `fd82a6f3` baseline at the new
   default thread count.
3. Then set `DeviceIo.defaultDataThreads` from the results.

## 8. Documents to update

`store/backend/fsif/DESIGN.md` (2.3, 2.4, 2.7, 2.9, 2.13, 3, 4), `server/store/CLAUDE.md` (Backends),
`server/host/CLAUDE.md` (device load, the lost path, Host threading model), `server/CLAUDE.md` (BackendTiming stages),
`docs/host-threading-measurement.md`, `Todo.txt`.

## 9. Left to the builder

- Class and file names, the job kind name, and how the attachment handle is passed through `BackendOpener`.
- Encoding details of headers, batches and the index's new fields, within 4.1-4.2.
- How the journal thread is woken and how it batches (a queue drained per write is enough).
- Test layout, as long as the crash sweeps (every op of a checkpoint, of a device-load replay, of the lost-journal
  path) and the model suite run against several stores sharing one simulated journal.

## 10. As built

Rulings (ledger `.superpowers/sdd/2026-11-24-fsif-device-journal/progress.md`) that changed the design above; the
others (R5, R7, R9, R10, R14) only ordered the work. `store/backend/fsif/DESIGN.md` describes the code.

- **R3**: a journal file is at most 1 GiB (`fsif-journal-file-size-mb` 1-1024), and recovery reads the headers first,
  then one file at a time.
- **R4**: the writer starts on a fresh ring in file 0 (first serial 0); after a replay it enters the file after the
  tail before its first batch, never appending after a possibly torn tail, or re-enters the tail file itself when it
  holds no batch of its pass (R22: no two valid headers share a `first`).
- **R6**: device-load replay reports `ForeignUndetached` (newest index undetached but naming another journal), which
  the device load treats as that copy's loss.
- **R8**: a checkpoint holds the store's new appends in a drain buffer from its snapshot until the snapshot's last
  append is durable (4.4, 6 3.7 corrected): one attachment's queued appends can share a batch.
- **R11**: `rebuildFlush` appends at most as many Mappings at once as one reservation fits in a journal file.
- **R12**: a store open accepts an undetached index naming this journal when the journal says its attachment needs no
  replay (`FsifDeviceJournal.needsReplay` false: a failed open or a frozen close of this journal's lifetime); any
  other is an `IOException`, a load retry.
- **R13**: such an open releases that old attachment first, returning the reservations a frozen close abandoned; and
  admission reserves `appendBound(min(size, segmentPayloadSize))`, so an oversized slice replies `SliceTooLarge`
  instead of waiting forever.
- **R15**: production sizing is 4 x 256 MiB; tests size their journals small through the device config keys.
- **R16**: a journal write failure (J4), after each store's own fail-stop, takes the device out of service the way a
  failed CRL does (`DeviceShutdown.fsifJournalFailed`): unloaded until the host restarts, not marked Failed.
- **R17**: a store open on a device with no journal directory (loaded Absent, its FSIF copies transferred in or
  otherwise detached) creates the journal (`FsifJournalDisk.forOpen`), as a create does.
- **R18**: the device-load open classifies failures as `DeviceCrlDisk.open` does: Refused (locked, sizing mismatch, a
  config that cannot size a replacement), Transient (counted by `FsifJournalOpenAttempts`), Undecided (before anything
  was decided), and anything else the lost-journal path. (R22 changes the no-FSIF-directory case.)
- **R19**: a copy whose store config cannot be read but which holds FSIF index files leaves the device unloaded for
  the next scan (Undecided); one with neither is not an FSIF directory.
- **R20**: at store open, an undetached index naming another journal is that copy's loss (`StoreLost`), as at device
  load (R6).
- **R22** (final review): the writer re-enters a tail file holding no batch of its pass (4.7 step 3), and the scan
  orders or rejects equal firsts safely. On a device with no FSIF directory, a journal that cannot be opened (any
  failure after the pre-decision steps but a lock) is replaced through the lost-journal path with nothing to move,
  logged at warn, instead of Undecided forever. A locked journal at device load stays Refused (as R18), logged as
  possibly transient (usually this host's own journal closed with a write in flight): counting it as transient would
  let a lock held by another writer add up with other transient failures to a lost journal under that writer. A copy lost alone by
  its replay is forced with its earlier unforced move's directories, as the lost-journal path's copies are.

Also differing from sections 4-7:
- Checkpoints start on the journal's request (pressure) or when an operation waits for journal room while the store
  has uncovered records; there is no "half the ring" size trigger. Pressure is: no free file after the current one
  and the current one 3/4 full, a refused reservation, or a batch waiting for a file.
- A reservation of `c` padded bytes is charged `2c - 4096`, covering the unusable tail its batch may leave in a file.
- Batch and header CRCs are CRC32C. The journal thread is `dev-<label>-fjournal-0`.
- A shipped copy's detach check is in the outbound move (`TransferDisk.moveOutbound`), not in `closedForTransfer`'s
  extraction; an FSIF rebuild staging store that closes attached is discarded and its rebuild starts over.
- Measured (7): `docs/host-threading-measurement.md`, "FSIF device journal".
