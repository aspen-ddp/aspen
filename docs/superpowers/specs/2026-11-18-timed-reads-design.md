# Timed reads

## Intent

Client reads retry until they restore the object or fail fatally. Most of the system depends on that. A few
consumers must not wait forever, though. Rebuild's walk and pending-allocation sweep (Todo 4.2, 4.3) and the repair
scan (the Backlog item "Repair scan stalls on an allocation held past expiry", now folded into Todo 4.3) each stall
behind a read that never completes. This change adds an opt-in read timeout. Existing callers see no difference.

## Goals, non-goals

- Goal: any `ObjectReader` read can be bounded by a timeout. A timed-out read fails with `ReadTimeout` and stops
  its driver: no further retransmits, and the read leaves `ReadManager.outstandingReads`.
- Goal: untimed reads behave exactly as today, with no timer and no new per-read cost.
- Non-goal: switching rebuild or repair to timed reads. That is Todo 4.2 and 4.3 work, which this change unblocks.
- Anti-goal: a Future-level timeout wrapper that leaves the driver retrying in the background.

## Approaches

1. Extra overloads `read(p, comment, timeout)` on every read method. Explicit, but it roughly doubles
   `ObjectReader`'s surface. A default argument is not possible here: Scala allows defaults on only one overload
   of a name, and `read` has two.
2. **Timed view (chosen).** `client.withReadTimeout(d): ObjectReader` returns a reader bound to `d`. It adds one
   method, changes no existing signature, gives every read method (including readRange, readRangedWriteState,
   readAppendPosition, readOptional) the bound, and lets a consumer build one timed reader and use it throughout.

## Design

- `AspenClient.withReadTimeout(timeout: Duration): ObjectReader`. `Duration.Inf` returns the client itself, and
  a non-positive timeout is an `IllegalArgumentException`. The view delegates to `BaseAspenClient`'s read methods,
  which pass a `timeout` through to `ReadManager.read`. Untimed calls pass `Duration.Inf`.
- `ReadManager.read(..., timeout)`: for a finite timeout, it schedules one task on `client.backgroundTaskManager`.
  When the task fires on a read that is still incomplete, the driver fails the read with `ReadTimeout(pointer)`,
  cancels its retransmits (`onComplete` / `shutdown`), and is removed from `outstandingReads`. A read that
  finishes first cancels the task. A late response for a timed-out read takes the existing "completed or unknown
  read" path.
- `ReadDriver` gains a method for failing the read from outside (run under the driver's lock, a no-op once
  complete). `noErrorRecoveryReadDriver`'s checker thread already exits once the promise completes.
- `ReadTimeout` is a `TransientReadError`, not an `InvalidObject`, so `readOptional` does not swallow it.

## Verification

- Deterministic tests (TestNetwork `ManualTimers`):
  - A read whose stores never reply fails with `ReadTimeout` when the timer fires, leaves
    `outstandingReadCount == 0`, and sends no further Reads.
  - A read that completes before the timeout succeeds, and the cancelled timer has no effect.
  - An untimed read and `withReadTimeout(Duration.Inf)` schedule no timer.
  - The view's partial reads (readKeys, readRange, readMetadata) carry the timeout too.
- The full test suite passes unchanged.

## Docs

- `UsageGuide.md`: a short "Timed reads" section. Reads are infinite by default because that keeps error handling
  simple in the general case: everything freezes until the partition heals or the hosts come back. Timed reads
  are there for cases where giving up makes more sense. It includes a user-friendly code example, and the example
  is compiled and executed by a permanent test so it cannot drift (my call).
- `client/CLAUDE.md` and `client/internal/CLAUDE.md`: timed reads and their failure.
- Todo 4.2 and 4.3: note that timed reads are implemented and available. 4.3 absorbs the Backlog item's repair
  mapping (a timed-out read defers the entry, like an InvalidObject short of a proof), and the Backlog entry is
  removed. Todo 7.5's sub-item about that entry's scope is dropped.

## Left to the builder (my calls)

- The timeout is one deadline over IDA resolution and the object read (added after review). Only the timed
  caller's wait on an uncached pool's load is bounded: that load is shared with untimed callers of getStoragePool,
  so it is never failed by a timeout, and it fills the cache once the stores answer.
- The name of the new `ReadDriver` method.
