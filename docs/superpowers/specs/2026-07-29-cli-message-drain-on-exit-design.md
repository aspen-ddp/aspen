# Design: Draining pending host messages on CLI exit

**Date:** 2026-07-29
**Status:** Approved

## Goal

`CheckStorageDevice` nudges sent by short-lived CLI commands can die with the
process. When that happens the affected stores wait up to
`Main.CheckStorageDevicesPeriod` — one hour — for the host's next periodic
check.

`create-storage-device` avoids this: it sends the nudge itself on the main
thread, then calls `ZMQNet.awaitHostMessagesSent(hostId, timeout)` followed by
`ZMQNet.shutdown(linger)` before returning. `create-pool` and `transfer-store`
do not. Both return an exit code to `main`, which calls `System.exit`
(`Main.scala:633`) with no drain of any kind.

Two things have to be true for a drain to work, and today neither is:

1. The nudge must actually be enqueued in `ZMQNet` by the time the drain runs.
2. The drain must be able to see every place a message can be waiting, for
   hosts the caller cannot name.

This design establishes both, and moves the drain to the single process exit
point so that no future command has to remember it.

## The enqueue race

`BaseAspenClient.createStoragePool` (lines 349-355) and
`AspenClient.transferStore` (lines 271-277) both send their nudge from
`tx.result.foreach`.

`TransactionImpl.commit()` returns the same future as `TransactionImpl.result`
— one promise, documented at `TransactionImpl.scala:131-136`. So on commit that
promise fans out to two independent callbacks, both dispatched onto
`clientContext`, which in the CLI is a three-thread pool
(`Main.createAmoebaClient`, line 664):

- **(a)** the `tx.result.foreach` above, which calls `sendHostMessage`
- **(b)** `transact`'s `for { _ <- tx.commit() }` continuation
  (`AspenClient.scala:169-172`), which resolves the future the CLI awaits

Nothing orders (a) before (b). `Await.ready` returns as soon as (b) runs, and
(a) may still be queued. A drain at that moment observes an empty `sendQueue`
and reports success having drained nothing.

This is why the fix cannot be "add a drain to the two commands". The drain has
nothing to wait on.

## Design

### 1. Send the nudges inside the awaited chain

Both client methods stop registering a callback on `tx.result`. Each builds its
`CheckStorageDevice` messages while staging, returns them out of `prepare`, and
sends them from a `.map` on the future the transaction helper returns:

```scala
override protected def createStoragePool(config: StoragePoolState): Future[PoolId] =
  given ExecutionContext = clientContext

  val f = runCreate(_ => Future.unit): tx =>
    ...
    def stageDeviceUpdate(du: DeviceUpdate): CheckStorageDevice =
      ...
      tx.update(du.pointer, None, None, reqs, ops)
      CheckStorageDevice(du.state.hostId, clientId, du.storageDeviceId)  // built, not sent

    for
      ...
    yield
      val nudges = devUpdates.map(stageDeviceUpdate)
      ...
      (config.poolId, nudges)

  f.map: (poolId, nudges) =>
    nudges.foreach(sendHostMessage)
    poolId
```

The `.map` body runs on a client thread, but the future the CLI awaits *is* the
map's result, so it cannot complete until the body has finished. That is the
happens-before the drain needs.

`transferStore` gets the same treatment: its `prepare` yields the
`CheckStorageDevice` instead of sending it, and a `.map` sends it.

Properties preserved:

- **Commit-only.** `map` is skipped on a failed future exactly as `foreach` is.
- **No cross-attempt duplication.** Each retry attempt produces its own list,
  and the outer future carries only the successful attempt's value.
- **Signatures unchanged.** `Future[PoolId]` and `Future[Unit]` as before.

The `sendHostMessage` calls are wrapped so a throw cannot fail the command.
The nudge is best-effort by design; pulling it into the chain is the one thing
that could newly couple it to the command's result.

### 2. Generalize the drain

`ZMQNet.awaitHostMessagesSent(hostId, timeout)` is replaced by:

```scala
def awaitPendingMessagesSent(timeout: Duration): Boolean
```

It has no callers other than `create_storage_device`, which loses its bespoke
drain (section 3), so the old method is deleted rather than kept alongside.

Same 25ms polling loop. The predicate widens from two holding places to all
three a message can occupy:

- `sendQueue.isEmpty` — the global outbound queue. Already checked today; the
  existing method's Scaladoc notes it is "stricter than the name implies".
- Nothing parked in `MetadataManager` behind a pending host or pool lookup —
  `PendingHostLookup.messageQueue` and `PendingPoolLookup.storeQueues`.
- Every *resolved* host entry has `odealer.isDefined` and empty
  `pendingMessages`.

The `MetadataManager` case is not hypothetical padding: it is the common path
for `create-pool`. The nudged hosts are typically ones the CLI has never talked
to, so `getHostEntryOrQueueMessage` returns `None` and parks the message while
an Aspen read resolves the host. A drain ignoring that would report success on
exactly the case this design exists to fix.

This needs two small read-only additions to `MetadataManager`, both under its
existing `synchronized`: one exposing the resolved host entries, one reporting
whether any pending-lookup queue is non-empty.

The change also *loosens* one condition. Today's predicate requires the named
host to be resolved, so it returns `false` for a host that never resolves. The
new one asks only that nothing is parked, which is the right question when the
caller cannot name the hosts involved.

Two limits belong in the Scaladoc:

- The existing race stands. The IO thread dequeues from `pendingMessages` just
  before calling `dealer.send`, so a `true` return can beat the final send by a
  few instructions.
- A *failed* host lookup drops the entry and its parked messages — see the
  `peekHostEntry` Scaladoc (`MetadataManager.scala:100-105`). The predicate then
  reports drained when the message is simply gone.

Neither is fixable at this layer. The receiving host's periodic check remains
the correctness guarantee either way; this is a latency optimization.

### 3. Drain at the single exit point

`Main.createNetwork` (line 644) is the only place a `ZMQNet` is constructed —
its callers are `createAmoebaClient` (line 656) and `host` (line 932). It
records the instance in a private field in `Main`.

The exit path at `Main.scala:631` becomes:

```scala
drainAndShutdown()
System.exit(exitCode)
```

`drainAndShutdown` is a no-op when nothing was registered — `bootstrap` never
builds a network. Otherwise it calls `awaitPendingMessagesSent(DrainTimeout)`
and then `shutdown(SendLinger)`. `host` and `amoeba` block in `joinIoThread`
and never reach it.

The two constants currently local to `create_storage_device` — a 5 second drain
timeout and a 1 second send linger — move up beside `CheckStorageDevicesPeriod`.

The drain never changes the exit code. A timeout is a latency report, not a
command failure. This is already the established position in the existing
Scaladoc and it does not change.

### 4. Operator-visible output

`create_storage_device` loses its `awaitHostMessagesSent`/`shutdown` pair and
its `flushed` conditional. It keeps the guidance text, which is worth keeping
and does not depend on the drain outcome: the host should load the device
shortly, within `CheckStorageDevicesPeriod` if the notification is lost, or on
next start if it is down.

On timeout the exit path prints one generic line — notifications could not be
confirmed to have left the process, and affected hosts will act on their next
periodic check. Commands that sent no nudges normally never see it.

## Testing

**The ordering property cannot be tested here, and the tests do not claim to.**
`TestNetwork`'s client messenger records every `sendHostMessage` into
`capturedHostMessages` (`TestNetwork.scala:162`), so an integration test can
assert the nudges are present as soon as the `createNewStoragePool` and
`transferStore` futures complete. But that assertion holds whether or not the
fix is in place, and this was confirmed empirically: reverting both client
methods to the detached `tx.result.foreach` and re-running the suite leaves
both tests passing.

`IntegrationTestSuite` extends `AsyncFunSuite`, whose `executionContext` is
serial. The detached callback is submitted to that queue when the commit
promise completes, and the test's own assertion continuation can only be
enqueued afterwards — it is reached by resolving the outer future, which is
itself a continuation behind the same queue. So the callback has always run by
the time the assertions execute, whichever order the promise dispatches its two
listeners in. The queue erases the very distinction the test would need to see.

The bug needs a genuinely concurrent `clientContext` to observe, which is what
the CLI has (`Main.createAmoebaClient`, a three-thread pool) and what
`TestNetwork` deliberately is not — it asserts single-threaded use in
`handleEvents`. Building a multi-threaded harness to turn this red is out of
proportion to the change.

So the ordering property is guarded by the comment at `stageDeviceUpdate` and by
review, not by a test. That is a real gap and is recorded here rather than
papered over.

What the tests are still worth keeping for is coverage that did not previously
exist: that a nudge is sent at all, exactly one per distinct device, addressed
to the right host with the right device id. They are named for that.

**The `MetadataManager` additions** are unit-testable in the style of the
existing `peekHostEntry` coverage: park a message behind a pending lookup and
assert not-drained; resolve the lookup and assert drained.

**The exit wiring gets no direct coverage.** `MainSuite` is a pure unit suite
that does not drive commands end to end, and standing up a harness that does is
out of proportion to a five-line change at a single call site. Stated here
rather than papered over.

## Files affected

| File | Change |
|------|--------|
| `client/internal/BaseAspenClient.scala` | `createStoragePool`: build nudges during staging, send from `.map` |
| `client/AspenClient.scala` | `transferStore`: same |
| `common/network/implementations/zmqnet/ZMQNet.scala` | Replace `awaitHostMessagesSent` with `awaitPendingMessagesSent` |
| `common/network/MetadataManager.scala` | Two read-only accessors for the drain predicate |
| `cmdline/Main.scala` | Register network on creation; drain at exit; hoist constants; trim `create_storage_device` |
| Integration suite (`StorageDeviceSetIntegrationSuite` or a sibling) | Ordering assertions on the two client methods. `TestNetwork` already records what is needed and should not need changing |
| `TODO.txt` | Remove the completed item |

## Out of scope

- **A flush sentinel in `sendQueue`.** Considered: a marker the IO thread
  acknowledges, giving a real happens-before instead of a polled read. Rejected
  as machinery a terminating process does not need — and on its own it would be
  *worse*, since it would complete while a nudge sat parked behind an unresolved
  host lookup.
- **Returning nudged host ids from the client API.** Considered so the CLI could
  send the nudges itself, mirroring `create-storage-device`. Rejected: it
  changes public signatures and pushes a library duty onto every caller.
- **The `rebalance` command.** It gains the exit drain for free like everything
  else, but nothing about its nudge behaviour is examined here.
