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
- Functions similar to a circular buffer. Invariant: the stream after the active one holds no needed entry. A rotation
  recycles that stream and copies forward, into its first entry, the live data of the stream after it (the next to be
  recycled), so a torn rotation write only ever overwrites entries no recovery needs. Needs at least 2 streams
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
- A failed open shuts its writer down before throwing, so a retry never runs two writers over the same files
- An open writes nothing (safety F16): recovery returns each live record with the locations its newest static record
  names and the serial of the oldest verified entry holding any of them (a stub: its anchor in the newest entry
  holding it, `Recovery.RecoveredTx`/`RecoveredStub`), and the constructor queues them by that serial as if it had
  written them. The next rotation copies them forward from there. Do not reintroduce an open-time re-save
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
  starts: a `.tmp` at open is a creation that wrote nothing, and it starts over. `StreamWriter` opens existing files
  only. Copy-on-write file systems (btrfs, ZFS) allocate on overwrite anyway: the open warns once
- A failed CRL (its thread or its writer died; one path: the writer's death kills the CRL thread) answers nothing:
  every pending and later Future fails with `CrlFailed` (`SimpleCRL.tracked`), `failed` completes and
  `onThreadDeath` runs once. Save completions never run, so no vote follows. The host unloads the device until it
  restarts (`DeviceShutdown.crlFailed`, `server/host/CLAUDE.md`), and the restart recovers the files as after a crash
- `admissionLimitBytes` (SimpleCRL: half a stream, so a recycled stream's live copy-forward always fits) sizes the
  device's `CrlAdmission`, which drops prepares for unseen transactions past it. Lanes (`AdmissionTier`, the store's
  pool's, persisted in its StoreConfig): data stores admit below 3/4 of it, metadata stores below 7/8, bootstrap
  stores up to it, so the marker batches each tier's finalization needs keep headroom (safety X5, G10). `Stream.writeEntry`'s `require` stays
  the fail-stop backstop. The admission counts everything the CRL holds from the device's open
  (`recoveredBytesByStore`, `CrlAdmission.recoveredBytes`), retained per store until the store loads and counts its
  own, or its entries leave the CRL: records of stores that do not load are copied forward too (safety F8)
- `maxTransactionBytes` (SimpleCRL: half a stream less `EntryOverheadReserve`, 8 KiB; trait default unlimited): the
  most bytes, as `CrlAdmission` counts them, one store's transaction may hold. `Frontend.admitPrepare` drops a prepare
  past it unsaved (`PrepareAdmission.Oversized`, `server/store/CLAUDE.md`), so no entry outgrows a stream and trips
  `Stream.writeEntry`'s `require`, which would kill the CRL (CRL-W2)
- Deletions are final within an entry: a save of an id the unwritten entry already deletes is skipped, and its
  handler runs once the deletion is durable. It creates no live record (`LogEntry.deletesFinally`): an unwritten one
  would pin `oldestEntryNeeded` until recovery broke on the wrap. The exceptions are deletions a stub's save or a
  `dropStore` made, which a later save cancels
- `dropTransactionObjectData` writes nothing: it only flags the in-memory record, so the next rewrite of the record
  (a save, a copy-forward) leaves the data out. Until then a restart, and `readStoreState`, still return the data. A
  store that needs the data durably gone saves the state without it (`Tx.persistWithoutUpdateOf`); a save's update
  count change rewrites the record's update locations, keeping its txd location (transaction-recovery R-N13)
- `whenDurable(storeId, handler)` (`Barrier`): runs the handler once every request queued before it is durable: on
  the current entry if it holds anything, after the write in flight if the current entry is empty
  (`afterInFlight`), else at once. It orders a write outside the CRL (a backend deletion) after a deletion, which has
  no completion of its own. The trait's default runs it at once, for CRLs that keep nothing on disk; a CRL that
  dies never runs it
- Applied stubs (`StoreRecoveryState`, `saveAppliedStub`/`deleteAppliedStub`): a (store, txid) entry meaning the store
  holds that transaction's update, with the transaction's start timestamp (`Stub.StaticSize`: TxId + 8 B), by which the
  store expires it (`Frontend.expireStubs`). A namespace apart from transaction state (own header counts and deletion
  list in each `LogEntry`), copied forward like a transaction (`Stub.anchor`); a stub's save replaces the transaction's
  state in the same entry. `readStoreState`, `crl_save.log`, `loadStore`, `deleteTransactions` and `dropStore` carry
  both. Charged to CrlAdmission without a check (`AppliedStubBytes`, 41), by the Frontend: never refused, but counted,
  and released when the store expires the stub
- The device's CRL is shared by all its stores; sizing comes from the device config. A loading store reads its state
  with the non-blocking `readStoreState` from its executor (`server/host/CLAUDE.md`)
