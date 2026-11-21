### Crash Recovery Log
- Similar to a write-ahead-log in traditional databases
- Stores the state necessary for Paxos as well as the TransactionDescription, commit/abort vote, and object data
- A record's saved `TransactionStatus` is persisted (bits 3-4 of the static record's Paxos mask byte), so disk
  recovery returns the status `readStoreState` and `crl_save.log` do (crash-recovery-log C15). A recovered Tx restores a
  saved resolution from it (`Tx.oresolution`, transaction-recovery 7.2)
- Only read at application startup to recover in-progress transactions
- Takes advantage of the fact that Transactions are typically very short-lived so their state usually does not need to
  be retained for long
- Implemented as a ring of fixed-size, write-only files (streams) that are continually recycled, which grows by whole
  streams under load and never shrinks (Growth, below)
- Functions similar to a circular buffer. Invariant: the stream after the active one holds no needed entry. The
  rules below are proven in safety `crash-recovery-log.md` 6.1 (W1-W6, S1); keep them when changing SimpleCRL:
  - Bounded entries (W1): a request that would push the newest unwritten entry past `maxEntryBytes` (S/2) seals it;
    sealed entries queue (`sealedEntries`) and are written in order, one write in flight. Deletions of one request may
    span entries, handler on the last; a stub and the deletion it makes share one. Handlers and barriers go to the
    newest unwritten entry (`whenWritten`). Serial, previous link and offset are given at the build
  - Frozen entries (W1a): an entry holds each save's `TxForm` (immutable state + keep-data flag) taken when the save
    was added, with whether it writes the pieces (`TxSave.writesPieces`) or names those an earlier entry wrote. A later
    save never changes an earlier entry. Never build an entry from a record's current state
  - The durable image (W3, `simple.Image`): each record's last-built form (`BuiltTx`, `BuiltStub`) and its pieces
    (`simple.Piece`: location + serial of the entry holding it), changed only by an entry's build
    (`LogEntry.createEntryBuffers`). It is kept apart from the live records (`transactions` of `LiveTx`, `stubs`),
    which `readStoreState` reads and a deletion leaves at once: a deleted record stays in the image, pinned and
    evacuated, until its deletion's entry is built
  - One home per record (W4): an update-count change writes the txd again too, so a record's pieces lie in one entry
  - Evacuation first (W2, W5): when the head does not fit, the writer enters the successor at offset 0 and first
    writes an evacuation entry (`TxEvacuation`) carrying, in their last-built forms, the image's records with pieces in
    the stream after it; then the head, beside it or after another rotation (`SimpleCRL.writeHead`). An evacuation
    writes no pending state, piece or deletion, and always fits (Lemma A)
  - Fail-stop (W6): a head not written after `n − 1` rotations in a row throws on the CRL thread (the CRL fails);
    a head larger than a stream fails it at once
  - `SimpleCRL.apply` creates no CRL of fewer than 3 streams or a stream size not a multiple of 4096 (S1; a recorded
    sizing like that is unreadable); device configs ask for at least 4.
  "After" is the ring order recorded in `crl/sizing` (`Sizing.ring`, `ring: 0,2,1`; `Sizing.successor`), not numeric
  order: rotation and `Recovery`'s newer-intact-entry check of the next stream both follow it. Stream files are
  `<n>.crl` for `n` in `0 until ringSize`; a growth inserts the next number after the active stream, and its
  rewrite of `crl/sizing` is the commit point (Growth, below). A sizing file whose ring is missing or not a
  permutation of `0 until num-streams` is unreadable. Tests reach other rings with `SimpleCRL.apply`'s `ringForTest`
  (creation only)
- Growth (`requestGrowth`, `GrowthPolicy`, `SimpleCRL.apply`'s `growth`; default `Disabled`; safety 5.11): one
  stream at a time, never shrinking. `requestGrowth` (any thread, cheap, idempotent) wakes the CRL's own growth
  thread (daemon, none when `maxStreams` is at or below the ring size), which checks the policy (`ringSize <
  maxStreams`, `freeSpace() - S >= capacity() * minFreePercent / 100`), preallocates `<n>.crl.tmp` (n = ring size),
  renames it to `<n>.crl`, forces the directory and queues `GrowthReady(n)`. The CRL thread commits it only at a
  quiescent point (`commitGrowthWhenQuiescent`: no rotation between its evacuation and its head, so W6's count runs
  over one ring): `crl/sizing` rewritten with `n` right after the active stream, then `StreamWriter.addStream`, then
  the `onAdmissionLimitRaised` listeners with the new `admissionLimitBytes`. The sizing rewrite must stay the last
  step that touches the files: `removeUncommittedGrowth` relies on it
  - Wiring: `DeviceFactory.openCRL` builds the `GrowthPolicy` from the `CrlSizing` it resolved (capacity and free
    space from `getTotalSpace` / `getUsableSpace` of the device path); `DeviceDiscovery` registers
    `sds.admission.raiseLimit` as the listener right after constructing the device state; `Frontend.admitPrepare`
    calls `requestGrowth` on every `Full`
  - At the cap `requestGrowth` refuses at once and logs it once. A refused or failed attempt logs, removes what it
    created and ignores requests for `retryAfter` (10 s); it never fails the CRL, with two fail-stop exceptions
    after the rename of the new sizing: its directory force failing twice (whether the new ring is durable is then
    unknown), and `addStream` failing to open the committed file. A restart opens whichever ring is on disk
  - Shutdown and death set `growthStopped`, interrupt the growth thread and then take `growthFileLock`, before the
    CRL lock is released: the step in progress aborts (a zero-fill or force with ClosedByInterruptException; a
    fallocate is one call), no later step runs, and the leftovers are removed at the next open. `StreamWriter` dies
    of a write to a stream it does not know rather than completing it unwritten
- One CRL per storage device, in `<device>/crl` (`CrashRecoveryLog.deviceCrlDir`), opened by
  `CrashRecoveryLogFactory.openCRL` when the device loads and shut down when it unloads
- Sizing keys in the device config are all optional (`CrlSizingDefaults.resolve`, from the capacity of the file
  system holding the device): `crl-num-streams` 4 (also the minimum; fewer is refused), `crl-max-file-size-mb`
  capacity / 2048 rounded down to 4 KiB, at least 64 MiB, `crl-max-streams` 20 (at most 1024: a `StreamLocation`
  holds the stream number in 2 bytes), `crl-min-free-percent` 5 (the last two are growth bounds, read at each open).
  The stream size is fixed when the device's CRL is created; the stream count only grows. Both are recorded durably
  in `crl/sizing` (`SimpleCRL.SizingFile`, with the ring) before any stream file, with the CRL's random id
  (`Sizing.crlId`). An existing CRL always opens with its recorded sizing, whatever the config asks (a warning is
  logged for another stream size or a smaller ring than configured; a ring grown past the configured count is
  normal), so config edits and default changes are harmless. `DeviceFactory.openCRL` holds the config to `resolve`
  only when `crl/` holds neither a sizing file nor stream files (a new CRL): a config `resolve` refuses is then
  `CrlConfigInvalid`, with nothing created in `crl/`. With either file present it takes `resolveExisting`, which never
  throws (invalid growth bounds disable growth), so a bad config never hides the `CrlSizingMismatch` below.
  `SimpleCRL.apply` throws `CrlSizingMismatch` for stream files without a readable sizing file or of another count or
  size than recorded. That is damage or a hand edit, never declared a lost CRL: the device is refused until repaired
  (`DeviceDiscovery.openDeviceCrl`)
- `openCRL`'s `holdsStores` (`SimpleCRL.apply`'s `mustExist`): the device holds stores, so its CRL must exist. With no
  stream files (only `sizing` or `.crl.tmp` files at most) the open throws `CrlMissing`, writing nothing, and the
  caller declares the CRL lost instead of creating an empty one under live stores. So anything that creates
  stores on a device not yet loaded (`Main.bootstrap`) creates its CRL first
- Each entry header carries CRC32Cs of the header and of the body (`LogEntry`). Recovery drops a torn newest entry
  (only the write in flight at a crash can be torn) and throws `CrlCorrupt` for any other failed checksum (the body
  of the entry before a dropped one is checked too, `StreamReader.lastIntactEntry`), or when an
  intact entry newer than the last one found exists in that stream or the next. Data is read only from entries
  the walk verified. All-zero regions are not entries. Every header bears the CRL id from `crl/sizing`, and
  `StreamReader.readHeader` takes no other header for an entry: the old-cycle object data that the newer-entry scan
  probes at 4 KiB boundaries can never pass for an entry, whether crafted or an image of another CRL
- `oldestEntryNeeded` is the oldest serial in `simple.LivePieces`, a counted index of the entries holding the pieces
  of the image (recovery reads pieces only from entries its walk verified). At an entry's build its new pieces are
  added, the pieces its forms supersede (relocated, old count, dropped) and its deleted records' pieces are removed,
  and then it stamps `oldestEntryNeeded`. Removing them only at that build is what keeps a torn entry safe: the entry
  before it was stamped while they were pinned. A deleted record's save in the same unwritten entry is taken out of it
  (`LogEntry.deleteTx`), so it is never pinned
- A failed open shuts its writer down before throwing, so a retry never runs two writers over the same files
- An open writes nothing (crash-recovery-log 5.7): recovery returns each live record with the pieces its newest static
  record names, each with the serial of the verified entry holding it (`Recovery.entryHolding`; a stub: its anchor in
  the newest entry holding it, `Recovery.RecoveredTx`/`RecoveredStub`), and the constructor pins them in `LivePieces` as
  if it had written them (they are the image's last-built forms). The next rotations evacuate them from there. Do not
  reintroduce an open-time re-save
- An open CRL holds an exclusive lock on `crl/lock` (`simple/CrlLock`), from before its files are read until its writer
  has closed them; a second opener, in this process or another, gets `CrlLocked` and touches nothing. Never open
  the lock file elsewhere: closing any descriptor of it drops the process's POSIX lock, which is why in-process
  openers are refused by a registry first. It does not fence shared-storage double mounts (crash-recovery-log AS3)
- `readStoreState` answers from the CRL thread's in-memory live records and stubs (`onGetRecoveryState`), never from
  the stream files, and leaves them live. Store loads (`StoreRouter`), transfers out (`StoreTransfers`,
  `StoreLoader` extraction) and the save that precedes a drop (`StoreCopies`) all read this way
- A transfer out reads a store's state (`readStoreState`) and removes it with
  `deleteTransactions` only once its `crl_save.log` is durable: entries leave the log only through a written deletion
- `dropStore` is one CRL-thread request (`DropStore`): it deletes what the store holds when processed, so every
  earlier save is dropped and no later one (crash-recovery-log C14). Its transaction deletions, like a stub save's, are
  cancellable (`LogEntry.deleteTxUnlessResaved`): a same-id save later in the same unwritten entry is written. Every
  other deletion is final within its entry
- Stream files are preallocated at creation (`SimpleCRL.createStreamsIfNew`, `server.FilePreallocation`): `fallocate`
  through FFM on Linux, zero-fill elsewhere or on `EOPNOTSUPP`. A CRL write never allocates, so a full device cannot
  fail one (crash-recovery-log 5.1). Each file is written as `<n>.crl.tmp`, renamed, and the directory forced before the
  writer starts. A `.crl.tmp` at open is a creation that wrote nothing, and it starts over, only when `holdsStores`
  (`mustExist`) is unset and every one is a listed stream's `<n>.crl.tmp` with no `<n>.crl` beside it. Any other
  (a stray beside its stream, an unlisted name, any on a device with stores) is damage: `CrlSizingMismatch`, nothing
  removed, never a fresh CRL under live state. The replacement of a lost CRL on a device holding stores is never
  created in place: `createCRL` (`SimpleCRL.create`) builds it in an empty scratch directory, which the caller renames
  to `crl/` once finished. A `<ringSize>.crl` or
  `<ringSize>.crl.tmp` the ring does not list is a growth that never committed: removed at open with a warning
  (`removeUncommittedGrowth`, run before that creation rule), but only when every listed stream exists and no other file
  is unlisted. Otherwise, as for any other unlisted stream file, the open is `CrlSizingMismatch` and removes nothing:
  removing it beside missing streams could leave none, and a new empty CRL would be created under live stores.
  `StreamWriter` opens existing files only. Copy-on-write file systems (btrfs, ZFS) allocate on every overwrite whatever
  the preallocation, so a full device can stop the CRL there: the creation warns once
- A failed CRL (its thread or its writer died; one path: the writer's death kills the CRL thread) answers nothing:
  every pending and later Future fails with `CrlFailed` (`SimpleCRL.tracked`), `failed` completes and
  `onThreadDeath` runs once. Save completions never run, so no vote follows. The host unloads the device until it
  restarts (`DeviceShutdown.crlFailed`, `server/host/CLAUDE.md`), and the restart recovers the files as after a crash
- A shut down CRL (`shutdown()`) answers nothing more either: on its `Shutdown` the CRL thread waits for the
  completion thread to run every handler already handed over, sets `closed`, then fails every pending tracked Future
  with `CrlClosed`, and every later one fails with it too. Handlers of unwritten (and in-flight) entries never run.
  Callers treat it as a retryable failure (a drop or deletion waiting on an unloaded device frees its slot)
- Completion handlers (save, stub, deletion, `whenDurable`) run on the CRL's **completion thread**
  (`SimpleCRL.completions`), never on the writer or CRL thread (safety `crash-recovery-log.md` C2). At each
  `WriteComplete` the CRL thread issues the next entry first, then hands over the written entry's handlers and its
  `afterInFlight` barriers, so the handlers' network sends overlap the next write. A barrier or empty deletion that
  completes at once is handed over too, so entries' handlers and `afterInFlight` barriers run in request order;
  within one entry the handlers run newest first (`LogEntry` prepends them). Handlers must not block: one slow
  handler delays every later one on the device. One that throws fails the CRL (`HandlerFailed`). `BackendTiming`
  logs the `crl write` and `crl handlers` stages. Tests hold a written entry unacknowledged with
  `entryWrittenForTest` (`SimpleCRLSuite.holdEntryOf`), never with a blocking handler
- `admissionLimitBytes` (SimpleCRL `admissionLimitFor`: `(n − 2) · S/2 − S/16 − H · (n − 1)` for `n` streams of `S`
  bytes, `H = EntryFixedBytes` (118) per evacuation entry a head may wait behind, so it holds at every ring size: with it
  every evacuation fits and every head is written within `n − 1` rotations, safety 6.1) sizes the device's
  `CrlAdmission`, which drops prepares for unseen transactions past it. It rises with each committed growth
  (`onAdmissionLimitRaised`, `CrlAdmission.raiseLimit`). `maxAdmissionLimitBytes` is the limit at
  `CrlSizingDefaults.MaxMaxStreams`, never at this open's `crl-max-streams` (a lowered or invalid cap must not declare
  states lost fleet-wide): `StoreLoader.neverFits` judges a `crl_save.log` state against it. Lanes (`AdmissionTier`, the store's pool's, persisted in its
  StoreConfig): data stores admit below 3/4 of it, metadata stores below 7/8, bootstrap stores up to it, so the marker
  batches each tier's finalization needs keep headroom (cross-document-review 3, CRL admission lanes).
  `Stream.writeEntry`'s `require` stays the fail-stop backstop. The admission counts everything the CRL holds from the
  device's open (`recoveredBytesByStore`, `CrlAdmission.recoveredBytes`), retained per store until the store loads and
  has counted its own, or its entries leave the CRL: records of stores that do not load are evacuated too
  (crash-recovery-log 6, admission). The Frontend and store loader keep every save charged (safety 6.1, A1-A4: late data
  checked, reload swaps, loaded state checked by `tryChargeLoaded`, no save after a release)
- `maxTransactionBytes` (SimpleCRL: `T = S/2 − EntryOverheadReserve`, 8 KiB; trait default unlimited): the most bytes,
  as `CrlAdmission` counts them, one store's transaction may hold. `Frontend.admitPrepare` drops a prepare past it
  unsaved, with no reply (`PrepareAdmission.Oversized`, `server/store/CLAUDE.md`), so every entry stays within
  `maxEntryBytes` (W1) and none trips `writeHead`'s `require`, which would kill the CRL (safety 6, Oversized, and
  10.20). Such a transaction can never commit on the store; the client sees `TransactionOutcomeUnknown`
- Deletions are final across every unwritten entry, the one in flight included (W1b): a save of an id any of them
  deletes is skipped, and its handler runs once the deletion is durable. It creates no live record
  (`LogEntry.deletesFinally`, `SimpleCRL.skippedAfterFinalDeletion`): `readStoreState` would report it live
  (crash-recovery-log C9), and it would save a state whose admission was released. When only the entry in flight deletes
  the id, the handler waits in `afterInFlight`. The exceptions are deletions a stub's save or a `dropStore` made, which
  a later save in the same entry cancels
- `dropTransactionObjectData` writes nothing: it only flags the live record, so saves made after it leave the data
  out. Saves already queued keep it (their forms are frozen), and so does the last-built form: an evacuation carries
  the dropped data, pinned, until a later save's entry is built. Until then a restart, and `readStoreState`, still
  return the data. A store that needs the data durably gone saves the state without it (`Tx.persistWithoutUpdateOf`);
  a save's update count change writes every piece again, the txd included (W4; transaction-recovery 7.4)
- `whenDurable(storeId, handler)` (`Barrier`): runs the handler once every request queued before it is durable: on
  the newest unwritten entry holding anything, sealed ones included (`newestUnwritten`); after the write in flight
  if no unwritten entry holds anything (`afterInFlight`); else at once. It orders a write outside the CRL (a backend
  deletion) after a deletion, which has no completion of its own. The trait's default runs it at once, for CRLs that
  keep nothing on disk; a CRL that dies never runs it
- Applied stubs (`StoreRecoveryState`, `saveAppliedStub`/`deleteAppliedStub`): a (store, txid) entry meaning the store
  holds that transaction's update, with the transaction's start timestamp (`Stub.StaticSize`: TxId + 8 B), by which the
  store expires it (`Frontend.expireStubs`). A namespace apart from transaction state (own header counts and deletion
  list in each `LogEntry`), evacuated like a transaction (`BuiltStub.anchor`); a stub's save replaces the transaction's
  state in the same entry. `readStoreState`, `crl_save.log`, `loadStore`, `deleteTransactions` and `dropStore` carry
  both. Charged to CrlAdmission without a check (`AppliedStubBytes`, 41), by the Frontend: never refused, but counted,
  and released when the store expires the stub, after its deletion is queued (as every charge is released only after
  its record's deletion is queued: crash-recovery-log Lemma C)
- The device's CRL is shared by all its stores; sizing comes from the device config. A loading store reads its state
  with the non-blocking `readStoreState` from its executor (`server/host/CLAUDE.md`)
