# Design: Exception safety in MetadataManager's lookup paths

Date: 2026-07-30

## Goal

A throw from any one call in `MetadataManager`'s host and pool lookup paths currently costs far
more than the operation that threw: the tail of a pool's store handoffs, a permanently wedged
host or pool entry, or ZMQNet's entire IO thread. Contain each throw to the thing that threw.

## The bugs

Four sites, three distinct failure shapes.

### 1. The pool handoff loop drops the tail of the pool

`startPoolLookup`'s `Success` continuation iterates the pool's stores, recording each store's host
and handing that store's parked messages to a host lookup or a resolved host entry
(`MetadataManager.scala:270-295`). Any throw from the per-store body escapes `foreach`, so every
*remaining* store loses both its `stores` mapping and its rescued queue. `pendingPoolLookups` was
already cleared at the top of the block, so those queues are unreachable and their messages are
gone.

The loop was already exposed this way through `storeResolved`, which in ZMQNet ends in
`wakeIoThread()` — a send on a socket that may be dead during CLI teardown
(`ZMQNet.scala:156-164`). The pool-lookup rescue added `getHostState` as a second throwing call
site on the same body.

The lost `stores` mappings are self-healing: a later send for those stores finds no mapping and
starts a fresh pool lookup. The parked messages are not.

### 2. A synchronous throw from either lookup call wedges its entry

Both lookup starters install their pending entry *before* making the call that resolves it:

- `startHostLookup` does `hosts += hostId -> Left(phl)` at `:238`, then `client.getHostState` at
  `:239`.
- `startPoolLookup` does `pendingPoolLookups += storeId.poolId -> ppl` at `:264`, then
  `client.getStoragePoolState` at `:265`.

If the call throws rather than returning a future, no continuation will ever run, and the entry
stays pending for the life of the process. For a host that means `getHostEntry` returns `None`
forever and every later message for that host parks in a queue nobody will drain. For a pool it
means the same for every store in it. In both cases `hasParkedMessages` stays true forever, so
`ZMQNet.awaitPendingMessagesSent` can only ever time out.

This is the mirror image of bug 1: rather than dropping messages quietly, it pins them
permanently.

The throw also propagates. `getHostEntryOrQueueMessage` is called from ZMQNet's send loop at
`ZMQNet.scala:401` and `:412`, which has no per-item guard — see bug 4.

### 3. A throwing `createHostEntry` wedges the same way

In `startHostLookup`'s `Success` continuation, `networkImplInterface.createHostEntry` at `:246`
is the only call between the pending `Left` and the resolved `Right`. A throw there leaves the
host at `Left` with exactly the consequences of bug 2.

### 4. ZMQNet's send loop dies on any throw

`ioThread`'s send-queue loop (`ZMQNet.scala:397-455`) handles each item with no guard. The only
enclosing handler is `case t: Throwable if shuttingDown` at `:456`, so a throw while the process
is running normally ends the IO thread: the rest of the queue, every host's traffic, and all
inbound polling stop. The router-message loop immediately above already guards each message
(`:389-392`); the send loop does not.

## Design

One rule covers bugs 2 and 3: **a lookup that fails — by returning a failed `Future` or by
throwing — returns its subject to the never-looked-up state, so a later call retries.** Neither
lookup starter propagates to its caller.

Bugs 1 and 4 are containment rather than retryability: a throw costs the one store, or the one
send-queue item, that threw, and nothing beyond it.

### 1. `startHostLookup(hostId, phl)`

```scala
      case Some(client) =>
        given ExecutionContext = client.clientContext
        hosts += hostId -> Left(phl)
        try
          client.getHostState(hostId).onComplete:
            case Failure(err) =>
              logger.error(s"HostState lookup failed for hostId $hostId. Error: $err")
              synchronized:
                hosts -= hostId
            case Success(hostState) =>
              synchronized:
                try
                  hosts += hostId -> Right(networkImplInterface.createHostEntry(...))
                catch
                  case NonFatal(t) =>
                    logger.error(s"Failed to create the host entry for hostId $hostId. Error: $t", t)
                    hosts -= hostId
        catch
          case NonFatal(t) =>
            logger.error(s"HostState lookup call threw for hostId $hostId. Error: $t", t)
            hosts -= hostId
```

Both catches take the action the existing `Failure` branch takes, which is what makes the rule a
rule rather than three separate decisions.

Two choices worth recording:

- **Install first, clean up in the catch**, rather than reordering so the throwing call precedes
  the map mutation. Reordering would make the cleanup unnecessary and would structurally remove
  the forgotten-cleanup bug class, but it would also give up an ordering guarantee: today, a
  re-entrant call arriving while the lookup call is in flight cannot start a duplicate lookup,
  because the pending entry is already installed. That re-entrancy is not reachable now — the
  client's reads leave through `ZMQNet.queueForSend`, which hands off to the IO thread rather than
  running inline — but "not reachable today" is a weaker guarantee than one the ordering enforces.
- **The outer `try` spans the `onComplete` registration**, not just the call. It could be narrowed
  by capturing the future in an `Option`, but nothing in the continuation throws past the inner
  guard except a broken logger, and in that case `hosts -= hostId` is what the `Failure` branch
  was about to do anyway.

The method now never throws. That is what keeps a synchronous lookup failure from reaching
ZMQNet's send loop through `getHostEntry` or `getHostEntryOrQueueMessage`.

### 2. `startPoolLookup`

The same outer guard on `client.getStoragePoolState`, undoing
`pendingPoolLookups += storeId.poolId -> ppl`. Plus the per-store guard inside the handoff loop:

```scala
              poolState.stores.zipWithIndex.foreach: (se, index) =>
                val sid = StoreId(storeId.poolId, index.toByte)
                stores += sid -> se.hostId
                try
                  ppl.storeQueues.get(sid).foreach: storeQueue =>
                    ...unchanged...
                catch
                  case NonFatal(t) =>
                    logger.error(s"Handoff failed for messages parked on store $sid, host ${se.hostId}. " +
                                 s"Those messages are lost; the rest of the pool is unaffected. Error: $t", t)
```

`stores += sid -> se.hostId` stays outside the `try`. Nothing in it can throw, so this is
legibility rather than mechanism: the mapping is the part that must land for every store
regardless, and its position says so.

No guard is needed around the loop itself. Everything in the continuation outside the per-store
`try` — the `pendingPoolLookups` removal, `StoreId` construction, `zipWithIndex` — is incapable of
throwing.

What the per-store guard is worth is narrower than it looks. Once section 1 makes
`startHostLookup` non-throwing, the `None` and `Left` branches cannot throw and the `Right`
branch's `storeResolved` is the loop's only live throw site — the case that prompted this work. It
stays as the loop's general guard so that the next call added to that body does not have to
re-derive the argument.

### 3. ZMQNet's send loop

Wrap the `qmsg match`, leaving `qmsg = sendQueue.poll()` outside it so the loop always advances:

```scala
          catch
            case t: Throwable if !shuttingDown =>
              logger.error(s"Error processing send queue item: $t", t)
```

This mirrors the poll guard at `:361` exactly, `Throwable`-plus-`!shuttingDown` included: a throw
from a context that `shutdown()` closed still unwinds to the handler at `:456` and lets the thread
run off its normal end.

The change deliberately uses `NonFatal` in `MetadataManager` and `Throwable` here.
MetadataManager's guards run on the client's callback thread, where swallowing an
`InterruptedException` would break cancellation. ZMQNet's guard sits two screens below an
identical `Throwable if !shuttingDown`, and matching the adjacent idiom is worth more than
matching across files.

This introduces one accepted limit. A throw partway through `NewHostAvailable` — from
`dealer.connect`, say — leaves a created socket that was never registered in `connectedDealers`
and never assigned to `entry.odealer`. It leaks until the context closes, and that host is
permanently unsendable. The IO thread surviving is worth more; recorded in `TODO.txt` rather than
fixed here.

### 4. Documentation

- `startHostLookup`'s scaladoc gains a line: a synchronous throw from the lookup call is treated
  as a lookup failure, so the method never throws.
- `hasParkedMessages`' scaladoc already notes that a failed host or pool lookup drops the entry
  and everything parked on it. Extend "failed" to cover failing by throwing, not only by returning
  a failed `Future`.
- `TODO.txt` gains the `NewHostAvailable` partial-failure socket leak.

## Testing

### Fixture seams

Both doubles exist; each needs a way to be told to throw.

`LookupRecordingClient` records the call before throwing, so "a lookup was attempted" stays
observable:

```scala
def failLookupWith(hostId: HostId, err: Throwable): Unit
def clearLookupFailure(hostId: HostId): Unit
def failPoolLookupWith(poolId: PoolId, err: Throwable): Unit
def clearPoolLookupFailure(poolId: PoolId): Unit
```

The `clear*` methods are what let a test show the subject is *retryable* rather than merely
un-wedged: fail once, clear, call again, assert a second lookup started.

`RecordingNetworkImpl`:

```scala
def throwOnStoreResolved(hostId: HostId, err: Throwable): Unit
def throwOnCreateHostEntry(hostId: HostId, err: Throwable): Unit
```

Both throw *after* draining and recording, because that is where the real failure lives: ZMQNet's
`storeResolved` empties the queue and adds to `sendQueue` before calling `wakeIoThread()`. A
double that threw before draining would model a failure that cannot happen, and would make test 1
weaker than it appears.

### New suite: `MetadataManagerExceptionSafetySuite`

1. **A throwing `storeResolved` does not cost the rest of the pool.** Park messages for stores 0
   and 1 of `unknownPoolId`; set `storeResolved` to throw for `bootstrapHostId`; resolve the pool
   with store 0 on `bootstrapHostId` and store 1 on `remoteHostId`. Assert `client.lookups`
   contains `remoteHostId`; resolve it; assert store 1's message reached
   `deliveredTo(remoteHostId)`. Then send to store 1 again and assert `poolLookups` still has
   length 1 — the surviving `stores` mapping is observable that way without reaching into private
   state.

   Red today: the throw escapes `foreach`, so no lookup is started and nothing is delivered.

2. **A synchronous `getHostState` throw leaves the host retryable.** `failLookupWith(remoteHostId,
   ...)`, then `getHostEntryOrQueueMessage(remoteHostId, msg)` returns `None` without throwing and
   `hasParkedMessages` is false. Clear the failure, call again, assert a second entry in
   `client.lookups`.

   Red today: the call throws.

3. **A throwing `createHostEntry` leaves the host retryable.** The same shape, failing in the
   `Success` continuation instead.

   Red today: `hosts` stays `Left` forever, so `hasParkedMessages` never goes false and the second
   call parks a message instead of starting a lookup. This pins the wedge.

4. **A synchronous `getStoragePoolState` throw leaves the pool retryable.** The same shape against
   `startPoolLookup`.

   Red today: the call throws, and `pendingPoolLookups` is wedged.

In tests 1 and 3 the continuation runs inline on the test thread under
`ExecutionContext.parasitic`, so today's exact route for the escaping throw — rethrown at
`promise.success`, or handed to parasitic's `reportFailure` — must be confirmed when the red step
is written. The assertions are red before the change and green after it either way; only the
description of the pre-change failure depends on which.

### No test for the ZMQNet guard

There is no ZMQNet suite — `ProtobufMessageCodecSuite` is the only file under `zmqnet/` — and
ZMQNet's constructor binds real sockets, so covering five lines would mean standing up a harness
or extracting a seam. The guard is a verbatim copy of the reviewed pattern at `:361`. The gap is
accepted and stated rather than papered over.

## Files affected

| File | Change |
| --- | --- |
| `src/main/.../common/network/MetadataManager.scala` | `NonFatal` import; guard both lookup calls, the `createHostEntry` continuation, and the per-store handoff; scaladoc |
| `src/main/.../zmqnet/ZMQNet.scala` | Per-item guard in the send-queue loop |
| `src/test/.../common/network/MetadataManagerFixture.scala` | Throw seams on `LookupRecordingClient` and `RecordingNetworkImpl` |
| `src/test/.../common/network/MetadataManagerExceptionSafetySuite.scala` | New suite, four tests |
| `TODO.txt` | Add the `NewHostAvailable` partial-failure socket leak |

## Out of scope

- **The router-message guard's missing shutdown condition.** `ZMQNet.scala:389-392` catches
  `Throwable` unconditionally, so a decode error during shutdown logs at error rather than trace.
  Noise, not a fault.
- **`MetadataManager`'s constructor loop over bootstrap hosts.** It calls `createHostEntry` with
  no guard, and should: a throw there means the bootstrap config cannot be honoured, and failing
  construction is the correct outcome.
- **`refreshBootstrapConfig`.** Already has `try`/`catch`/`finally` covering its write and its
  reload.
- **Recovering the messages a guard drops.** They are gone. That is the limit `hasParkedMessages`
  already documents, unchanged by this work.
- **Distinguishing "lookup started" from "lookup failed instantly" at the call site.** Both return
  `None`, as a pending lookup already does. Giving callers a third answer would change every call
  site to gain nothing they can act on.
