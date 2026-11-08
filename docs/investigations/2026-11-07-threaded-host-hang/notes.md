# Threaded-host test hang (2026-11-07)

Captured during Task 5 of docs/superpowers/plans/2026-11-07-per-device-crl-and-commit-ordering.md (one in-flight
commit per object), after the lost-wakeup fix. Files are xz-compressed: `xz -dk <file>.xz`.
Files in this directory:
`fixjstack5.txt` and `fixjstack5-b.txt` (taken 20 s apart), `fix5.log`, `hungreads.txt`.
- **Every relevant thread is idle in both dumps:**
  - The ScalaTest thread for the suite is parked in `SerialExecutionContext.recRunNow`, in `LinkedBlockingQueue.take`:
    it waits for a callback that never arrives.
  - `aspen-store-0`, `-1` and `-2` are parked in `SerialLoop.loop`, also in `LinkedBlockingQueue.take`, with an empty
    queue.
  - `aspen-host-control` and `aspen-store-balancer` are idle too.
- **The stores did almost no work.** Over the stores' 271 s life, `aspen-store-0` used 20.7 ms of CPU and
  `aspen-store-1` and `-2` used 0.1 ms each. A test of 200+ transactions does far more, so it stalled near its start,
  most likely in `allocate()`, before the 200 overwrites.
- **Reads were never answered.** The client logged 11 `*** HUNG READ in test concurrent transactions commit on store
  threads ... Num Responses: 0` reads. Reads never consult the in-flight state (`readObjectForNetwork` serves memory or
  the backend), so a withheld vote or held commit cannot leave a read with zero responses. Either the stores never
  received those reads, or their responses were lost.
- **No status output exists.** Scribe warns are suppressed in tests, so `Stuck Tx` lines don't appear, and this test
  never calls `waitForTransactionsToComplete`. Nothing printed `printTransactionStatus` for it, and there is no Tx
  state to show.

Hypothesis, unconfirmed: this is a message-delivery or routing stall in the threaded TestNetwork host rather than a
commit-ordering wait. Candidates:
- `StoreRouter` treating the stores as unknown, offline or loading, which makes them silent;
- the adaptive pool's mailbox moves;
- `TestNetwork.handleEvents` in non-lockstep mode.

It may predate Task 5, which only changes timing; the 0-in-8 baseline cannot rule that out. The earlier hangs before
the fix also showed zero-response reads, so some of them may have been this hang rather than the reviewer's lost
wakeup.

Suggested next step: run the hunt with a dump of `StoreRouter` state (installed, offline, loading, awaitingDevice) and
the mailbox queue lengths, written to a file at the hang, on both this tree and d7ab3839.

