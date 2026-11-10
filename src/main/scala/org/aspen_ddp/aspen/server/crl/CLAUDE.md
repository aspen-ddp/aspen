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
- Sizing (`crl-num-streams`, `crl-max-file-size-mb`) is fixed when the device's CRL is created: `SimpleCRL.apply`
  throws `SizingMismatch` for stream files of another count or size, and the device's CRL is then treated as lost
- A failed open shuts its writer down before throwing, so a retry never runs two writers over the same files
- A transfer out reads a store's state (`readStoreState`, which leaves it live) and removes it with
  `deleteTransactions` only once its `crl_save.log` is durable: entries leave the log only through a written deletion
- Stores vote only after a save completes, so a dead CRL thread stops every transaction on its device while reads
  keep working. `onThreadDeath` reports it; the `host` command logs it and keeps serving its other devices
- `admissionLimitBytes` (SimpleCRL: half a stream, so a recycled stream's live copy-forward always fits) sizes the
  device's `CrlAdmission`, which drops prepares for unseen transactions past it. `Stream.writeEntry`'s `require` stays
  the fail-stop backstop
- Applied stubs (`StoreRecoveryState`, `saveAppliedStub`/`deleteAppliedStub`): a (store, txid) entry meaning the store
  holds that transaction's update. A namespace apart from transaction state (own header counts and deletion list in
  each `LogEntry`), copied forward like a transaction (`Stub.anchor`); a stub's save replaces the transaction's state
  in the same entry. `readStoreState`, `crl_save.log`, `loadStore`, `deleteTransactions` and `dropStore` carry both.
  Charged to CrlAdmission without a check (`AppliedStubBytes`), by the Frontend: never dropped, but counted
