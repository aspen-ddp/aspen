### Crash Recovery Log
- Similar to a write-ahead-log in traditional databases
- Stores the state necessary for Paxos as well as the TransactionDescription, commit/abort vote, and object data
- Only read at application startup to recover in-progress transactions
- Takes advantage of the fact that Transactions are typically very short-lived so their state usually does not need to
  be retained for long
- Implemented as a series of fixed-size, write-only files that are continually recycled.
- Functions similar to a circular buffer. Invariant: the stream after the active one holds no needed entry. A rotation
  recycles that stream and copies forward, into its first entry, the live data of the stream after it (the next to be
  recycled), so a torn rotation write only ever overwrites entries no recovery needs. Needs at least 2 streams
- One CRL per storage device, in `<device>/crl` (`CrashRecoveryLog.deviceCrlDir`), opened by
  `CrashRecoveryLogFactory.openCRL` when the device loads and shut down when it unloads
- Sizing (`crl-num-streams`, `crl-max-file-size-mb`, both required in the device config) is fixed when the device's
  CRL is created and recorded durably in `crl/sizing` (`SimpleCRL.SizingFile`) before any stream file. An existing
  CRL always opens with its recorded sizing, whatever the config asks (a warning is logged), so config edits and
  default changes are harmless. `SimpleCRL.apply` throws `CrlSizingMismatch` for stream files without a readable
  sizing file or of another count or size than recorded. That is damage or a hand edit, never declared a lost CRL:
  the device is refused until repaired (`DeviceDiscovery.openDeviceCrl`)
- Each entry header carries CRC32Cs of the header and of the body (`LogEntry`). Recovery drops a torn newest entry
  (only the write in flight at a crash can be torn) and throws `CrlCorrupt` for any other failed checksum, or when an
  intact entry newer than the last one found exists in that stream or the next. Data is read only from entries
  the walk verified. All-zero regions are not entries
- A failed open shuts its writer down before throwing, so a retry never runs two writers over the same files
- An open CRL holds an exclusive lock on `crl/lock` (`simple/CrlLock`), from before its files are read until its writer
  has closed them; a second opener, in this process or another, gets `CrlLocked` and touches nothing. Never open
  the lock file elsewhere: closing any descriptor of it drops the process's POSIX lock, which is why in-process
  openers are refused by a registry first. It does not fence shared-storage double mounts (safety SL M9)
- A transfer out reads a store's state (`readStoreState`, which leaves it live) and removes it with
  `deleteTransactions` only once its `crl_save.log` is durable: entries leave the log only through a written deletion
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
  device's `CrlAdmission`, which drops prepares for unseen transactions past it. `Stream.writeEntry`'s `require` stays
  the fail-stop backstop. The admission counts everything the CRL holds from the device's open
  (`recoveredBytesByStore`, `CrlAdmission.recoveredBytes`), retained per store until the store loads and counts its
  own, or its entries leave the CRL: records of stores that do not load are copied forward too (safety F8)
- Deletions are final within an entry: a save of an id the unwritten entry already deletes is skipped, and its
  handler runs once the deletion is durable. It creates no live record (`LogEntry.deletesFinally`): an unwritten one
  would pin `oldestEntryNeeded` until recovery broke on the wrap. The exception is a deletion a stub's save made,
  which a later save cancels
- Applied stubs (`StoreRecoveryState`, `saveAppliedStub`/`deleteAppliedStub`): a (store, txid) entry meaning the store
  holds that transaction's update. A namespace apart from transaction state (own header counts and deletion list in
  each `LogEntry`), copied forward like a transaction (`Stub.anchor`); a stub's save replaces the transaction's state
  in the same entry. `readStoreState`, `crl_save.log`, `loadStore`, `deleteTransactions` and `dropStore` carry both.
  Charged to CrlAdmission without a check (`AppliedStubBytes`), by the Frontend: never dropped, but counted
- The device's CRL is shared by all its stores; sizing comes from the device config. A loading store reads its state
  with the non-blocking `readStoreState` from its executor (`server/host/CLAUDE.md`)
