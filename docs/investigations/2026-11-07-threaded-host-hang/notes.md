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


## Resolution (2026-11-08)

Every hang reproduced on 2026-11-08 (4 in 5 idle full runs at df58a20b) was in AdaptiveThreadedHostSuite, and so were
those in the logs above. The test body completes. The hang is in `waitForTransactionsToComplete`: the allocation
transaction's `AllocationFinalizationAction` never finishes, because its read of the pool's AllocationTree node never
completes.

Per-store event logs and Frontend state, dumped while the test stalled, showed:
- No held commits, waiting voters or in-flight commits on any store. The commit ordering was idle and correct.
- Every store had committed all 20 AllocationTree inserts. The same insert reached the three stores up to 66 ms apart,
  queued behind earlier commits of the node (one in-flight commit per object), with the adaptive pool packing the
  stores onto fewer threads.

How the read got stuck:
- An insert (TKVL `set`) leaves the node's revision unchanged. A read inside that window gets three replies with the
  same revision but different keys: the key on one store, and another store still holding the insert's lock.
- `KeyValueObjectReader` then can neither restore the key nor take it as deleted, so it waits for more replies.
- The test client's `noErrorRecoveryReadDriver` never re-sent a read, so no more replies ever came. Production's
  `SimpleReadDriver` re-sends with backoff.

Fix: `noErrorRecoveryReadDriver` re-sends every 100 ms until the read completes. A store that stays behind still hangs
the read, so a missed update is never hidden. Result: 10 clean idle full runs after it, against 4 hangs in 5 before.

The zero-response hung reads seen on 2026-11-08 were issued by finalizers after the suite's teardown had shut down its
host's threads, so nothing answers them. They are not the hang. The zero-response reads in the loaded capture above
(fixjstack5) are probably the same, but its near-idle store threads were not re-examined: if a loaded hang still shows
that signature, it is a different problem.

The production side (a 2 s read stall in this race) is in Todo.txt.
