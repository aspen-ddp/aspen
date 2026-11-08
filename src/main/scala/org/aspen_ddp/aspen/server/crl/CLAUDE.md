### Crash Recovery Log
- Similar to a write-ahead-log in traditional databases
- Stores the state necessary for Paxos as well as the TransactionDescription, commit/abort vote, and object data
- Only read at application startup to recover in-progress transactions
- Takes advantage of the fact that Transactions are typically very short-lived so their state usually does not need to
  be retained for long
- Implemented as a series of fixed-size, write-only files that are continually recycled.
- Functions similar to a circular buffer when the oldest "live" data is copied to the head of the buffer whenever it
  is about to overwrite that live data.
- One CRL per storage device, in `<device>/crl` (`CrashRecoveryLog.deviceCrlDir`), opened by
  `CrashRecoveryLogFactory.openCRL` when the device loads and shut down when it unloads
- Stores vote only after a save completes, so a dead CRL thread stops every transaction on its device while reads
  keep working. `onThreadDeath` reports it; the `host` command logs it and keeps serving its other devices
- `admissionLimitBytes` (SimpleCRL: half a stream, so a recycled stream's live copy-forward always fits) sizes the
  device's `CrlAdmission`, which drops prepares for unseen transactions past it. `Stream.writeEntry`'s `require` stays
  the fail-stop backstop
