### Crash Recovery Log
- Similar to a write-ahead-log in traditional databases
- Stores the state necessary for Paxos as well as the TransactionDescription, commit/abort vote, and object data
- A record's saved `TransactionStatus` is persisted (bits 3-4 of the static record's Paxos mask byte), so disk
  recovery returns the status `readStoreState` and `crl_save.log` do (safety F14). A recovered Tx restores a saved
  resolution from it (`Tx.oresolution`, transaction-recovery R-F6)
- Only read at application startup to recover in-progress transactions
- Takes advantage of the fact that Transactions are typically very short-lived so their state usually does not need to
  be retained for long
- Implemented as a series of fixed-size, write-only files that are continually recycled.
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
- Growth (`requestGrowth`, `GrowthPolicy`, `SimpleCRL.apply`'s `growth`; default `Disabled`): one stream at a time,
  never shrinking. `requestGrowth` (any thread, cheap, idempotent) wakes the CRL's own growth thread (daemon, none
  when `maxStreams` is at or below the ring size), which checks the policy (`ringSize < maxStreams`, `freeSpace() - S
  >= capacity() * minFreePercent / 100`), preallocates `<n>.crl.tmp` (n = ring size), renames it to `<n>.crl`, forces
  the directory and queues `GrowthReady(n)`. The CRL thread commits it only at a quiescent point
  (`commitGrowthWhenQuiescent`: no rotation between its evacuation and its head, so W6's count runs over one ring):
  `crl/sizing` rewritten with `n` right after the active stream, then `StreamWriter.addStream`, then the
  `onAdmissionLimitRaised` listeners with the new `admissionLimitBytes`. The sizing rewrite must stay the last step
  that touches the files: `removeUncommittedGrowth` relies on it. At the cap `requestGrowth` refuses at once and logs
  it once. A refused or failed attempt logs, removes what it created and ignores requests for `retryAfter` (10 s); it
  never fails the CRL, with two fail-stop exceptions after the rename of the new sizing: its directory force failing
  twice (whether the new ring is durable is then unknown), and `addStream` failing to open the committed file. A
  restart opens whichever ring is on disk. Shutdown and death set `growthStopped`, interrupt the growth thread and
  then take `growthFileLock`, before the CRL lock is released: the step in progress aborts (a zero-fill or force
  with ClosedByInterruptException; a fallocate is one call), no later step runs, and the leftovers are removed at the
  next open. `StreamWriter` dies of a write to a stream it does not know rather than completing it unwritten
- One CRL per storage device, in `<device>/crl` (`CrashRecoveryLog.deviceCrlDir`), opened by
  `CrashRecoveryLogFactory.openCRL` when the device loads and shut down when it unloads
- Sizing keys in the device config are all optional (`CrlSizingDefaults.resolve`, from the capacity of the file
  system holding the device): `crl-num-streams` 4 (also the minimum; fewer is refused), `crl-max-file-size-mb`
  capacity / 2048 rounded down to 4 KiB, at least 64 MiB, `crl-max-streams` 20, `crl-min-free-percent` 5 (the last two
  are growth bounds, read at each open). Stream count and size are fixed when the device's
  CRL is created and recorded durably in `crl/sizing` (`SimpleCRL.SizingFile`) before any stream file, with the
  CRL's random id (`Sizing.crlId`). An existing
  CRL always opens with its recorded sizing, whatever the config asks (a warning is logged), so config edits and
  default changes are harmless. `SimpleCRL.apply` throws `CrlSizingMismatch` for stream files without a readable
  sizing file or of another count or size than recorded. That is damage or a hand edit, never declared a lost CRL:
  the device is refused until repaired (`DeviceDiscovery.openDeviceCrl`)
- `openCRL`'s `holdsStores` (`SimpleCRL.apply`'s `mustExist`): the device holds stores, so its CRL must exist. With no
  stream files (only `sizing` or `.crl.tmp` files at most) the open throws `CrlMissing`, writing nothing, and the
  caller declares the CRL lost instead of creating an empty one under live stores. So anything that creates
  stores on a device not yet loaded (`Main.bootstrap`) creates its CRL first
- Each entry header carries CRC32Cs of the header and of the body (`LogEntry`). Recovery drops a torn newest entry
  (only the write in flight at a crash can be torn) and throws `CrlCorrupt` for any other failed checksum, or when an
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
- An open writes nothing (safety F16): recovery returns each live record with the pieces its newest static record
  names, each with the serial of the verified entry holding it (`Recovery.entryHolding`; a stub: its anchor in the
  newest entry holding it, `Recovery.RecoveredTx`/`RecoveredStub`), and the constructor pins them in `LivePieces` as
  if it had written them (they are the image's last-built forms). The next rotations evacuate them from there. Do not
  reintroduce an open-time re-save
- An open CRL holds an exclusive lock on `crl/lock` (`simple/CrlLock`), from before its files are read until its writer
  has closed them; a second opener, in this process or another, gets `CrlLocked` and touches nothing. Never open
  the lock file elsewhere: closing any descriptor of it drops the process's POSIX lock, which is why in-process
  openers are refused by a registry first. It does not fence shared-storage double mounts (safety SL M9)
- A transfer out reads a store's state (`readStoreState`, which leaves it live) and removes it with
  `deleteTransactions` only once its `crl_save.log` is durable: entries leave the log only through a written deletion
- `dropStore` is one CRL-thread request (`DropStore`): it deletes what the store holds when processed, so every
  earlier save is dropped and no later one (safety F12). Its transaction deletions, like a stub save's, are
  cancellable (`LogEntry.deleteTxUnlessResaved`): a same-id save later in the same unwritten entry is written. Every
  other deletion is final within its entry
- Stream files are preallocated at creation (`SimpleCRL.createStreamsIfNew`, `server.FilePreallocation`): `fallocate`
  through FFM on Linux, zero-fill elsewhere or on `EOPNOTSUPP`. A CRL write never allocates, so a full device cannot
  fail one (safety F7). Each file is written as `<n>.crl.tmp`, renamed, and the directory forced before the writer
  starts: a `.tmp` at open is a creation that wrote nothing, and it starts over. A `<ringSize>.crl` or
  `<ringSize>.crl.tmp` the ring does not list is a growth that never committed: removed at open with a warning
  (`removeUncommittedGrowth`, run before that creation rule), but only when every listed stream exists and no other
  file is unlisted. Otherwise, as for any other unlisted stream file, the open is `CrlSizingMismatch` and removes
  nothing: removing it beside missing streams could leave none, and a new empty CRL would be created under live
  stores. `StreamWriter` opens existing files only. Copy-on-write file systems (btrfs, ZFS) allocate on overwrite
  anyway: the open warns once
- A failed CRL (its thread or its writer died; one path: the writer's death kills the CRL thread) answers nothing:
  every pending and later Future fails with `CrlFailed` (`SimpleCRL.tracked`), `failed` completes and
  `onThreadDeath` runs once. Save completions never run, so no vote follows. The host unloads the device until it
  restarts (`DeviceShutdown.crlFailed`, `server/host/CLAUDE.md`), and the restart recovers the files as after a crash
- `admissionLimitBytes` (SimpleCRL: `(n − 2) · S/2 − S/16` for `n` streams of `S` bytes, 15S/16 at 4 streams: with
  it every evacuation fits and every head is written within `n − 1` rotations, safety 6.1) sizes the
  device's `CrlAdmission`, which drops prepares for unseen transactions past it. Lanes (`AdmissionTier`, the store's
  pool's, persisted in its StoreConfig): data stores admit below 3/4 of it, metadata stores below 7/8, bootstrap
  stores up to it, so the marker batches each tier's finalization needs keep headroom (safety X5, G10). `Stream.writeEntry`'s `require` stays
  the fail-stop backstop. The admission counts everything the CRL holds from the device's open
  (`recoveredBytesByStore`, `CrlAdmission.recoveredBytes`), retained per store until the store loads and has counted
  its own, or its entries leave the CRL: records of stores that do not load are evacuated too (safety F8). The
  Frontend and store loader keep every save charged (safety 6.1, A1-A4: late data checked, reload swaps, loaded
  state checked by `tryChargeLoaded`, no save after a release)
- `maxTransactionBytes` (SimpleCRL: half a stream less `EntryOverheadReserve`, 8 KiB; trait default unlimited): the
  most bytes, as `CrlAdmission` counts them, one store's transaction may hold. `Frontend.admitPrepare` drops a prepare
  past it unsaved (`PrepareAdmission.Oversized`, `server/store/CLAUDE.md`), so no entry outgrows a stream and trips
  `Stream.writeEntry`'s `require`, which would kill the CRL (CRL-W2)
- Deletions are final across every unwritten entry, the one in flight included (W1b): a save of an id any of them
  deletes is skipped, and its handler runs once the deletion is durable. It creates no live record
  (`LogEntry.deletesFinally`, `SimpleCRL.skippedAfterFinalDeletion`): an unwritten one would pin `oldestEntryNeeded`
  until recovery broke on the wrap, and save a state whose admission was released. The exceptions are deletions a
  stub's save or a `dropStore` made, which a later save in the same entry cancels
- `dropTransactionObjectData` writes nothing: it only flags the live record, so saves made after it leave the data
  out. Saves already queued keep it (their forms are frozen), and so does the last-built form: an evacuation carries
  the dropped data, pinned, until a later save's entry is built. Until then a restart, and `readStoreState`, still
  return the data. A store that needs the data durably gone saves the state without it (`Tx.persistWithoutUpdateOf`);
  a save's update count change writes every piece again, the txd included (W4; transaction-recovery R-N13)
- `whenDurable(storeId, handler)` (`Barrier`): runs the handler once every request queued before it is durable: on
  the current entry if it holds anything, after the write in flight if the current entry is empty
  (`afterInFlight`), else at once. It orders a write outside the CRL (a backend deletion) after a deletion, which has
  no completion of its own. The trait's default runs it at once, for CRLs that keep nothing on disk; a CRL that
  dies never runs it
- Applied stubs (`StoreRecoveryState`, `saveAppliedStub`/`deleteAppliedStub`): a (store, txid) entry meaning the store
  holds that transaction's update, with the transaction's start timestamp (`Stub.StaticSize`: TxId + 8 B), by which the
  store expires it (`Frontend.expireStubs`). A namespace apart from transaction state (own header counts and deletion
  list in each `LogEntry`), evacuated like a transaction (`BuiltStub.anchor`); a stub's save replaces the transaction's
  state in the same entry. `readStoreState`, `crl_save.log`, `loadStore`, `deleteTransactions` and `dropStore` carry
  both. Charged to CrlAdmission without a check (`AppliedStubBytes`, 41), by the Frontend: never refused, but counted,
  and released when the store expires the stub
- The device's CRL is shared by all its stores; sizing comes from the device config. A loading store reads its state
  with the non-blocking `readStoreState` from its executor (`server/host/CLAUDE.md`)
