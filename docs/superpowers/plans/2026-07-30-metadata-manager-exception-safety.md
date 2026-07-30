# MetadataManager Exception Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Contain a throw from any single call in `MetadataManager`'s host and pool lookup paths to the operation that threw, instead of losing the tail of a pool's handoffs, wedging a lookup entry forever, or killing ZMQNet's IO thread.

**Architecture:** Six independent guards, each committed separately. Bugs of the "wedge" kind (a synchronous throw from a lookup call, or a throwing `createHostEntry`) are fixed by one rule — a lookup that fails, however it fails, returns its subject to the never-looked-up state so a later call retries. Bugs of the "collateral damage" kind (the per-store handoff loop, ZMQNet's send-queue loop) are fixed by containment — a throw costs the one store, or the one queue item, that threw.

**Tech Stack:** Scala 3, ScalaTest (`AnyFunSuite` + `Matchers`), sbt, jeromq, scribe logging.

**Spec:** `docs/superpowers/specs/2026-07-30-metadata-manager-exception-safety-design.md`

---

## Background an implementer needs

**`MetadataManager` resolves logical addresses to network endpoints.** Aspen addresses messages to `StoreId`s and `HostId`s, not hosts and ports. When a message is sent to a store the manager has never resolved, it *parks* the message in a queue and starts an asynchronous lookup against the Aspen client. When the lookup resolves, the parked messages are handed to the network layer. There are two lookup kinds and they chain: a `StoreId` resolves via a *pool* lookup to a `HostId`, which resolves via a *host* lookup to an endpoint.

**Three maps hold that state**, all guarded by the manager's own monitor:

- `stores: Map[StoreId, HostId]` — resolved store→host mappings.
- `hosts: Map[HostId, Either[PendingHostLookup, T]]` — `Left` means a lookup is in flight with messages parked on it; `Right` means resolved.
- `pendingPoolLookups: Map[PoolId, PendingPoolLookup]` — pool lookups in flight, each holding one message queue per store.

**Why a stuck `Left` is serious.** `hasParkedMessages` reports true while any queue is non-empty, and `ZMQNet.awaitPendingMessagesSent` polls it so a short-lived CLI process can drain before exiting. An entry stuck at `Left` with no continuation to resolve it makes that predicate true forever: every CLI command touching that host times out on exit, and `getHostEntry` returns `None` for the rest of the process's life.

**Tests use doubles, not a network.** `MetadataManagerFixture` supplies `LookupRecordingClient` (records every lookup, hands back a `Promise` the test completes when it chooses) and `RecordingNetworkImpl` (drains what it is handed, the way ZMQNet does, and records where each message went). `newManager()` returns `(mgr, client, impl)`.

**The execution context is `ExecutionContext.parasitic`,** which runs a future's continuation *inline* — on the thread completing the promise, or on the registering thread if the promise is already complete. This was measured during planning: **parasitic silently swallows a throw from an `onComplete` callback**, so `promise.success(...)` returns normally even when the continuation throws. That is exactly why the pre-fix wedges are invisible, and it means the red steps below key off state assertions rather than expected exceptions. Do not write a red step that expects a thrown exception from a promise completion.

**Scala 3 style for this repo:** quiet/indented syntax, indented `if/then/else` and `match`. Braces only where they clearly help.

---

## File Structure

| File | Responsibility | Change |
| --- | --- | --- |
| `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala` | Logical-address resolution and message parking | Four guards + scaladoc (Tasks 1–4, 6) |
| `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala` | ZeroMQ transport and its IO thread | Per-item guard in the send-queue loop (Task 5) |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala` | Shared doubles and fixture | Throw seams on both doubles (Tasks 1–4) |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerExceptionSafetySuite.scala` | Coverage for what a throw costs | New file (Tasks 1–4) |
| `TODO.txt` | Known-issue log | One entry (Task 6) |

Existing suites (`MetadataManagerDrainSuite`, `MetadataManagerPeekHostEntrySuite`, `MetadataManagerPoolLookupSuite`) are not modified. The new seams default to off, so their behaviour is unchanged — they serve as the regression check that these guards changed nothing on the success paths.

---

## Task 1: Guard `startHostLookup`'s lookup call

A synchronous throw from `client.getHostState` leaves `hosts += hostId -> Left(phl)` installed with no continuation to ever resolve it, and propagates into ZMQNet's unguarded send loop.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerExceptionSafetySuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`, the two-argument `startHostLookup`

> Anchor every edit below on the named symbol, not on a line number. Tasks in this plan add lines
> to these files as they go, so any line number quoted here is stale by the next task.

- [ ] **Step 1: Add the throw seam to `LookupRecordingClient`**

In `MetadataManagerFixture.scala`, add this member to `LookupRecordingClient`, immediately after the `poolPromises` declaration:

```scala
  private var lookupFailures: Map[HostId, Throwable] = Map()
```

Then add these two methods immediately after `poolLookupPromise`:

```scala
  /** Makes getHostState throw `err` synchronously for `hostId` rather than returning a future.
   *
   *  The call is still recorded in `lookups` before the throw, which is what lets a test tell a
   *  retried lookup from a wedged one: both leave the host unresolved, and only the call count
   *  distinguishes them. */
  def failLookupWith(hostId: HostId, err: Throwable): Unit = synchronized:
    lookupFailures += hostId -> err

  def clearLookupFailure(hostId: HostId): Unit = synchronized:
    lookupFailures -= hostId
```

Then replace the `getHostState` override in full:

```scala
  override def getHostState(hostId: HostId): Future[HostState] = synchronized:
    lookups += hostId
    lookupFailures.get(hostId) match
      case Some(err) => throw err
      case None => lookupPromise(hostId).future
```

- [ ] **Step 2: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerExceptionSafetySuite.scala`:

```scala
package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** What a throw costs in MetadataManager's lookup paths.
 *
 *  Every test here fails one call and asserts on what survived. Two failure shapes recur:
 *
 *    - A lookup that throws must leave its subject *retryable*, not merely unresolved. The
 *      distinction is invisible in the resolved/unresolved state alone -- a wedged entry and a
 *      cleared one both report "not resolved" -- so each test proves it by making a second call
 *      and asserting a second lookup was started.
 *    - A throw in the pool handoff loop must cost only its own store.
 *
 *  Note that ExecutionContext.parasitic swallows a throw from an onComplete callback: a promise
 *  completion returns normally even when the continuation it ran threw. Nothing here can assert
 *  on an exception escaping a completion, and the pre-fix bugs were invisible for that reason.
 *
 *  The message type is incidental -- MetadataManager never inspects a message, only the address
 *  it was sent to -- so these reuse the nudge the other suites use.
 */
class MetadataManagerExceptionSafetySuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  test("a host lookup call that throws leaves the host retryable"):
    val (mgr, client, impl) = newManager()

    client.failLookupWith(remoteHostId, new RuntimeException("getHostState exploded"))

    // Pre-fix this throw escapes getHostEntryOrQueueMessage, which ZMQNet's send loop calls with
    // no per-item guard -- so the IO thread dies. Post-fix a synchronous throw is just another
    // way for a lookup to fail: logged, entry removed, None returned.
    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge()) should be(None)
    client.lookups.toList should be(List(remoteHostId))

    // The pending entry was removed rather than left behind. Left at Left(phl) it would never
    // resolve -- no continuation exists -- so this would stay true for the life of the process
    // and awaitPendingMessagesSent could only ever time out.
    mgr.hasParkedMessages should be(false)

    client.clearLookupFailure(remoteHostId)

    // Retryable, not merely un-wedged: the second send starts a second lookup instead of parking
    // behind the dead one.
    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge()) should be(None)
    client.lookups.toList should be(List(remoteHostId, remoteHostId))
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `sbt 'testOnly *MetadataManagerExceptionSafetySuite -- -z "a host lookup call that throws"'`

Expected: FAIL. The suite errors with `java.lang.RuntimeException: getHostState exploded` thrown out of the first `mgr.getHostEntryOrQueueMessage` call — the throw propagates to the caller instead of being handled.

- [ ] **Step 4: Add the `NonFatal` import**

In `MetadataManager.scala`, replace the import on line 11:

```scala
import scala.util.{Failure, Success}
```

with:

```scala
import scala.util.control.NonFatal
import scala.util.{Failure, Success}
```

- [ ] **Step 5: Guard the lookup call**

In `MetadataManager.scala`, replace the two-argument `startHostLookup(hostId: HostId, phl: PendingHostLookup)` in full — leaving its scaladoc above it untouched — with:

```scala
  private def startHostLookup(hostId: HostId, phl: PendingHostLookup): Unit =
    oClient match
      case None => logger.error(s"Host lookup preformed before AspenClient initialized. HostId: $hostId")
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
                hosts += hostId -> Right(networkImplInterface.createHostEntry(
                  hostId,
                  hostState.name,
                  hostState.address,
                  hostState.dataPort,
                  hostState.cncPort,
                  hostState.storeTransferPort,
                  phl.messageQueue
                ))
        catch
          case NonFatal(t) =>
            // A lookup that fails by throwing is still a failed lookup, so undo the pending entry
            // installed above and let a later call retry -- exactly what the Failure branch does.
            // Left in place it would never resolve, because no continuation exists to resolve it:
            // the host would be unreachable and hasParkedMessages stuck true for the life of the
            // process. Swallowing rather than rethrowing is what keeps this out of ZMQNet's send
            // loop, whose two calls into getHostEntryOrQueueMessage have no guard of their own.
            // The monitor is held by the caller, per this method's contract, so the removal below
            // needs no synchronized of its own.
            logger.error(s"HostState lookup call threw for hostId $hostId. Error: $t", t)
            hosts -= hostId
```

The `try` deliberately spans the `onComplete` registration rather than the call alone. Narrowing it would mean capturing the future in an `Option`; nothing in the continuation throws past Task 2's guard except a broken logger, and in that case `hosts -= hostId` is what the `Failure` branch was about to do anyway.

- [ ] **Step 6: Run the test to verify it passes**

Run: `sbt 'testOnly *MetadataManagerExceptionSafetySuite -- -z "a host lookup call that throws"'`

Expected: PASS — `Tests: succeeded 1, failed 0`.

- [ ] **Step 7: Run the existing MetadataManager suites for regressions**

Run: `sbt 'testOnly *MetadataManager*'`

Expected: PASS, all suites. The seam defaults to off, so nothing else changes behaviour.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerExceptionSafetySuite.scala
git commit -m "Treat a throwing host lookup call as a failed lookup"
```

---

## Task 2: Guard `createHostEntry` in the success continuation

`networkImplInterface.createHostEntry` is the only call between the pending `Left` and the resolved `Right`. A throw there leaves the host at `Left` with the same consequences as Task 1's bug.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerExceptionSafetySuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala` (the `Success` branch written in Task 1)

- [ ] **Step 1: Add the throw seam to `RecordingNetworkImpl`**

In `MetadataManagerFixture.scala`, add this field to `RecordingNetworkImpl`, immediately after the `delivered` declaration:

```scala
  private var createFailures: Map[HostId, Throwable] = Map()
```

Add this method immediately after `deliveredTo`:

```scala
  /** Makes createHostEntry throw for `hostId` -- after draining and recording, because that is
   *  where the real failure lives: ZMQNet's createHostEntry empties the queue and enqueues
   *  NewHostAvailable before reaching the wakeIoThread() call that can actually fail. A double
   *  that threw before draining would model a failure that cannot happen. */
  def throwOnCreateHostEntry(hostId: HostId, err: Throwable): Unit = synchronized:
    createFailures += hostId -> err
```

Replace the body of `createHostEntry` — the `synchronized: drain(...)` line pair and the `new MetadataManager.HostEntry(...)` line that follows it — with:

```scala
    synchronized:
      drain(hostId, queuedMessages)
      createFailures.get(hostId) match
        case Some(err) => throw err
        case None => ()
    new MetadataManager.HostEntry(hostId, name, address, dataPort, cncPort, storeTransferPort)
```

- [ ] **Step 2: Write the failing test**

Append to `MetadataManagerExceptionSafetySuite`:

```scala
  test("a host entry that fails to build leaves the host retryable"):
    val (mgr, client, impl) = newManager()

    impl.throwOnCreateHostEntry(remoteHostId, new RuntimeException("createHostEntry exploded"))

    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge()) should be(None)
    client.lookups.toList should be(List(remoteHostId))

    // parasitic runs the continuation inline on this thread and swallows its throw, so this line
    // returns normally either way. That silence is the whole problem: pre-fix nothing fails, the
    // entry simply never advances from Left to Right.
    client.lookupPromise(remoteHostId).success(remoteHostState)

    // The entry did not reach Right -- createHostEntry never returned one to install.
    mgr.peekHostEntry(remoteHostId) should be(None)

    // ...and it did not stay at Left either. Pre-fix it did, so this send parks behind a pending
    // lookup nothing will ever resolve. Post-fix the host is back to never-looked-up and the send
    // starts a fresh lookup. This assertion is the one that separates the two states.
    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge()) should be(None)
    client.lookups.toList should be(List(remoteHostId, remoteHostId))
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `sbt 'testOnly *MetadataManagerExceptionSafetySuite -- -z "a host entry that fails to build"'`

Expected: FAIL on the final assertion — `List(remoteHostId) was not equal to List(remoteHostId, remoteHostId)`. The host is wedged at `Left`, so the second send parks instead of starting a lookup.

- [ ] **Step 4: Guard the entry creation**

In `MetadataManager.scala`, replace the `Success` branch of `startHostLookup` with:

```scala
            case Success(hostState) =>
              synchronized:
                try
                  hosts += hostId -> Right(networkImplInterface.createHostEntry(
                    hostId,
                    hostState.name,
                    hostState.address,
                    hostState.dataPort,
                    hostState.cncPort,
                    hostState.storeTransferPort,
                    phl.messageQueue
                  ))
                catch
                  case NonFatal(t) =>
                    // Same rule as a failed lookup: drop back to never-looked-up so a later call
                    // retries. Without this the entry stays at Left forever -- the lookup did
                    // resolve, so nothing will ever run again to advance or remove it. Repair
                    // first and log second, so a logger that throws cannot leave the wedge behind.
                    hosts -= hostId
                    logger.error(s"Failed to create the host entry for hostId $hostId. Error: $t", t)
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *MetadataManagerExceptionSafetySuite -- -z "a host entry that fails to build"'`

Expected: PASS.

- [ ] **Step 6: Run the full MetadataManager suites**

Run: `sbt 'testOnly *MetadataManager*'`

Expected: PASS, all suites.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerExceptionSafetySuite.scala
git commit -m "Drop the host entry when createHostEntry throws"
```

---

## Task 3: Guard `startPoolLookup`'s lookup call

Structurally identical to Task 1, one map down. A synchronous throw from `client.getStoragePoolState` leaves `pendingPoolLookups` holding an entry no continuation will remove, so every later message for every store in that pool parks forever.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerExceptionSafetySuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`, `startPoolLookup`

- [ ] **Step 1: Add the pool throw seam to `LookupRecordingClient`**

In `MetadataManagerFixture.scala`, add this field next to `lookupFailures`:

```scala
  private var poolLookupFailures: Map[PoolId, Throwable] = Map()
```

Add these two methods next to `failLookupWith` / `clearLookupFailure`:

```scala
  /** Makes getStoragePoolState throw `err` synchronously for `poolId`. Recorded in `poolLookups`
   *  before the throw, for the same reason failLookupWith records first. */
  def failPoolLookupWith(poolId: PoolId, err: Throwable): Unit = synchronized:
    poolLookupFailures += poolId -> err

  def clearPoolLookupFailure(poolId: PoolId): Unit = synchronized:
    poolLookupFailures -= poolId
```

Replace the `getStoragePoolState` override in full:

```scala
  override def getStoragePoolState(poolId: PoolId): Future[StoragePoolState] = synchronized:
    poolLookups += poolId
    poolLookupFailures.get(poolId) match
      case Some(err) => throw err
      case None => poolLookupPromise(poolId).future
```

- [ ] **Step 2: Write the failing test**

Append to `MetadataManagerExceptionSafetySuite`:

```scala
  test("a pool lookup call that throws leaves the pool retryable"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    client.failPoolLookupWith(unknownPoolId, new RuntimeException("getStoragePoolState exploded"))

    mgr.getHostEntryOrQueueMessage(store0, nudge()) should be(None)
    client.poolLookups.toList should be(List(unknownPoolId))

    // A pendingPoolLookups entry no continuation will remove parks every later message for every
    // store in the pool, forever -- one wedge covering a whole pool rather than one host.
    mgr.hasParkedMessages should be(false)

    client.clearPoolLookupFailure(unknownPoolId)

    mgr.getHostEntryOrQueueMessage(store0, nudge()) should be(None)
    client.poolLookups.toList should be(List(unknownPoolId, unknownPoolId))
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `sbt 'testOnly *MetadataManagerExceptionSafetySuite -- -z "a pool lookup call that throws"'`

Expected: FAIL. The suite errors with `java.lang.RuntimeException: getStoragePoolState exploded` out of the first `mgr.getHostEntryOrQueueMessage` call.

- [ ] **Step 4: Guard the lookup call**

In `MetadataManager.scala`, in `startPoolLookup`'s `Some(client)` branch, wrap the `client.getStoragePoolState(...).onComplete:` expression in a `try` and add the `catch` after it. The whole `onComplete` block — both the `Failure` and `Success` cases, including the store loop — is indented two further spaces to sit inside the `try`. The result, replacing everything from `case Some(client) =>` to the end of `startPoolLookup`:

```scala
      case Some(client) =>
        given ExecutionContext = client.clientContext

        val ppl = new PendingPoolLookup(pendingStoreLookupQueueSize)
        ppl.enqueueMessage(storeId, msg)
        pendingPoolLookups += storeId.poolId -> ppl
        try
          client.getStoragePoolState(storeId.poolId).onComplete:
            case Failure(err) =>
              logger.error(s"StoragePool lookup failed for poolId ${storeId.poolId}. Error: $err")
              synchronized:
                pendingPoolLookups -= storeId.poolId
            case Success(poolState) =>
              synchronized:
                pendingPoolLookups -= storeId.poolId
                poolState.stores.zipWithIndex.foreach: (se, index) =>
                  val sid = StoreId(storeId.poolId, index.toByte)
                  stores += sid -> se.hostId
                  ppl.storeQueues.get(sid).foreach: storeQueue =>
                    hosts.get(se.hostId) match
                      case None =>
                        // The pool named a host this process has never looked up. Move the queue
                        // onto a host lookup instead of dropping it: pendingPoolLookups was cleared
                        // above, so nothing else will ever come back for these messages. Doing it
                        // here, inside the same synchronized block, is also what keeps
                        // hasParkedMessages from dipping false while they are between queues.
                        // The move is guaranteed loss-free only while pendingHostLookupQueueSize is
                        // at least pendingStoreLookupQueueSize; otherwise EvictingQueue silently
                        // drops the oldest of what is being rescued.
                        // A fresh queue per store: one shared across the loop would end up
                        // referenced by every rescued host entry, so whichever host resolved first
                        // would drain all of them.
                        val phl = new PendingHostLookup(pendingHostLookupQueueSize)
                        phl.drainIntoQueue(storeQueue)
                        startHostLookup(se.hostId, phl)
                      case Some(e) => e match
                        case Left(phl) => phl.drainIntoQueue(storeQueue)
                        case Right(hostEntry) => networkImplInterface.storeResolved(hostEntry, sid, storeQueue)
        catch
          case NonFatal(t) =>
            // Same rule as startHostLookup: a lookup that fails by throwing is a failed lookup, so
            // undo the pending entry and let a later call retry. Left in place it wedges the whole
            // pool -- every store in it, not just the one addressed here. Repair first and log
            // second, so a logger that throws cannot leave the wedge behind.
            pendingPoolLookups -= storeId.poolId
            logger.error(s"StoragePool lookup call threw for poolId ${storeId.poolId}. Error: $t", t)
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *MetadataManagerExceptionSafetySuite -- -z "a pool lookup call that throws"'`

Expected: PASS.

- [ ] **Step 6: Run the full MetadataManager suites**

Run: `sbt 'testOnly *MetadataManager*'`

Expected: PASS, all suites. `MetadataManagerPoolLookupSuite`'s six tests are the check that the re-indented loop body still behaves identically.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerExceptionSafetySuite.scala
git commit -m "Treat a throwing pool lookup call as a failed lookup"
```

---

## Task 4: Guard the per-store handoff body

The original finding. A throw from the per-store body escapes `foreach`, so every remaining store loses both its `stores` mapping and its rescued queue — and `pendingPoolLookups` was already cleared, so those queues are unreachable.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerExceptionSafetySuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala` (the store loop from Task 3)

- [ ] **Step 1: Add the `storeResolved` throw seam**

In `MetadataManagerFixture.scala`, add this field to `RecordingNetworkImpl` next to `createFailures`:

```scala
  private var storeResolvedFailures: Map[HostId, Throwable] = Map()
```

Add this method next to `throwOnCreateHostEntry`:

```scala
  /** Makes storeResolved throw for `hostId`, after draining and recording. ZMQNet's storeResolved
   *  empties the queue and enqueues ProcessPendingMessages before its wakeIoThread() call, which
   *  is the one that can fail -- on a socket closed by CLI teardown. */
  def throwOnStoreResolved(hostId: HostId, err: Throwable): Unit = synchronized:
    storeResolvedFailures += hostId -> err
```

Replace the body of `storeResolved` — its single `synchronized:` block — with:

```scala
    synchronized:
      storeResolutionsBuffer += hostEntry.hostId -> storeId
      drain(hostEntry.hostId, queuedMessages)
      storeResolvedFailures.get(hostEntry.hostId) match
        case Some(err) => throw err
        case None => ()
```

- [ ] **Step 2: Write the failing test**

Append to `MetadataManagerExceptionSafetySuite`:

```scala
  test("a throwing storeResolved costs only its own store, not the rest of the pool"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val store1 = StoreId(unknownPoolId, 1.toByte)
    val msg0 = nudge()
    val msg1 = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg0) should be(None)
    mgr.getHostEntryOrQueueMessage(store1, msg1) should be(None)

    // bootstrapHostId is already Right(...) from the constructor, so store 0 takes the
    // storeResolved branch -- the loop's one remaining throw site now that startHostLookup no
    // longer throws. In ZMQNet the throw comes from wakeIoThread() sending on a socket that CLI
    // teardown already closed.
    impl.throwOnStoreResolved(bootstrapHostId, new RuntimeException("wakeIoThread exploded"))

    // Store 0 is listed first so there is a tail to lose. Pre-fix the throw escapes foreach and
    // store 1 gets neither its stores mapping nor its rescued queue.
    client.poolLookupPromise(unknownPoolId).success(
      poolStateWith(unknownPoolId, bootstrapHostId, remoteHostId))

    // Store 0's own messages were consumed before the throw, as they are in ZMQNet: the queue is
    // drained and ProcessPendingMessages enqueued before the wake fails.
    impl.deliveredTo(bootstrapHostId) should be(List(msg0))

    // Store 1's queue was still rescued onto a host lookup.
    client.lookups.toList should be(List(remoteHostId))
    client.lookupPromise(remoteHostId).success(remoteHostState)
    impl.deliveredTo(remoteHostId) should be(List(msg1))
    mgr.hasParkedMessages should be(false)

    // Store 1's stores mapping survived too -- a later send resolves straight through the host
    // path rather than starting a second pool lookup.
    mgr.getHostEntryOrQueueMessage(store1, nudge()).map(_.hostId) should be(Some(remoteHostId))
    client.poolLookups.toList should be(List(unknownPoolId))
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `sbt 'testOnly *MetadataManagerExceptionSafetySuite -- -z "a throwing storeResolved costs only its own store"'`

Expected: FAIL on `client.lookups.toList should be(List(remoteHostId))` — `List() was not equal to List(remoteHostId)`. The throw escaped `foreach` before store 1's iteration, and parasitic swallowed it, so the promise completion returned normally with nothing done.

- [ ] **Step 4: Guard the per-store body**

In `MetadataManager.scala`, replace the store loop inside the `Success` branch with:

```scala
                poolState.stores.zipWithIndex.foreach: (se, index) =>
                  val sid = StoreId(storeId.poolId, index.toByte)
                  // Outside the try on purpose. Nothing here can throw, so this is legibility
                  // rather than mechanism: the mapping is the part that must land for every store
                  // regardless of what the handoff does, and its position says so.
                  stores += sid -> se.hostId
                  try
                    ppl.storeQueues.get(sid).foreach: storeQueue =>
                      hosts.get(se.hostId) match
                        case None =>
                          // The pool named a host this process has never looked up. Move the queue
                          // onto a host lookup instead of dropping it: pendingPoolLookups was cleared
                          // above, so nothing else will ever come back for these messages. Doing it
                          // here, inside the same synchronized block, is also what keeps
                          // hasParkedMessages from dipping false while they are between queues.
                          // The move is guaranteed loss-free only while pendingHostLookupQueueSize is
                          // at least pendingStoreLookupQueueSize; otherwise EvictingQueue silently
                          // drops the oldest of what is being rescued.
                          // A fresh queue per store: one shared across the loop would end up
                          // referenced by every rescued host entry, so whichever host resolved first
                          // would drain all of them.
                          val phl = new PendingHostLookup(pendingHostLookupQueueSize)
                          phl.drainIntoQueue(storeQueue)
                          startHostLookup(se.hostId, phl)
                        case Some(e) => e match
                          case Left(phl) => phl.drainIntoQueue(storeQueue)
                          case Right(hostEntry) => networkImplInterface.storeResolved(hostEntry, sid, storeQueue)
                  catch
                    case NonFatal(t) =>
                      // Contain the throw to this store. Uncaught it escapes foreach, and since
                      // pendingPoolLookups was cleared above, every later store's queue becomes
                      // unreachable -- the tail of the pool lost to one store's failure.
                      //
                      // Now that startHostLookup swallows, the None and Left branches cannot throw
                      // and storeResolved is the only live throw site here. It stays a guard over
                      // the whole body so the next call added to it does not have to re-derive
                      // this argument.
                      logger.error(s"Handoff failed for messages parked on store $sid, host ${se.hostId}. " +
                                   s"Those messages are lost; the rest of the pool is unaffected. Error: $t", t)
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *MetadataManagerExceptionSafetySuite -- -z "a throwing storeResolved costs only its own store"'`

Expected: PASS.

- [ ] **Step 6: Run the full MetadataManager suites**

Run: `sbt 'testOnly *MetadataManager*'`

Expected: PASS — four tests in this suite, plus all of `MetadataManagerDrainSuite`, `MetadataManagerPeekHostEntrySuite` and `MetadataManagerPoolLookupSuite`.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerExceptionSafetySuite.scala
git commit -m "Contain a failed store handoff to the store that failed"
```

---

## Task 5: Guard ZMQNet's send-queue loop

`ioThread`'s send loop handles each item with no guard, and the only enclosing handler matches `if shuttingDown`. A throw during normal running ends the IO thread: the rest of the queue, every host's traffic, and all inbound polling stop.

**This task ships without a test.** There is no ZMQNet suite — `ProtobufMessageCodecSuite` is the only file under `zmqnet/` — and ZMQNet's constructor binds real sockets, so covering five lines would mean standing up a harness or extracting a seam. The guard is a verbatim copy of the reviewed pattern at `ZMQNet.scala:361`. Do not invent a partial harness for it; the gap is accepted deliberately.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala:396-455`

- [ ] **Step 1: Wrap each send-queue item**

Replace lines 396-455 with the following. The change is a `try` after `while qmsg != null do`, the case block indented two further spaces, and a `catch` before the `qmsg = sendQueue.poll()` line — which must stay outside the `try`, or a throw would skip it and the loop would spin forever on the same item.

```scala
        // Process send queue items
        var qmsg = sendQueue.poll()
        while qmsg != null do
          try
            qmsg match
              case SendToStore(storeId, msg) =>
                metadataManager.getHostEntryOrQueueMessage(storeId, msg) match
                  case Some(hostEntry) =>
                    hostEntry.odealer match
                      case Some(dealer) =>
                        dealer.send(ProtobufMessageCodec.encodeMessage(msg))
                      case None =>
                        hostEntry.pendingMessages.add(msg)
                  case None =>
                    // MetadataManager queued the message for later delivery

              case SendToHost(hostId, msg) =>
                metadataManager.getHostEntryOrQueueMessage(hostId, msg) match
                  case Some(hostEntry) =>
                    hostEntry.odealer match
                      case Some(dealer) =>
                        dealer.send(ProtobufMessageCodec.encodeMessage(msg))
                      case None =>
                        hostEntry.pendingMessages.add(msg)
                  case None =>
                    // MetadataManager queued the message for later delivery

              case SendToClient(msg) =>
                clients.get(msg.toClient).foreach: zmqIdentity =>
                  orouterSocket.foreach: router =>
                    router.send(zmqIdentity, ZMQ.SNDMORE)
                    router.send(ProtobufMessageCodec.encodeMessage(msg))

              case NewHostAvailable(entry) =>
                val dealer = context.createSocket(SocketType.DEALER)
                dealer.setIdentity(clientId.toBytes)
                dealer.connect(s"tcp://${entry.address}:${entry.dataPort}")
                entry.odealer = Some(dealer)
                entry.opollItem = Some(new PollItem(dealer, ZMQ.Poller.POLLIN))
                connectedDealers.add(dealer)

                // Send initial heartbeat if we are a server node
                oheartbeatMessage.foreach(dealer.send(_))

                // Drain any pending messages
                var pending = entry.pendingMessages.poll()
                while pending != null do
                  dealer.send(ProtobufMessageCodec.encodeMessage(pending))
                  pending = entry.pendingMessages.poll()

                connectedHosts += entry
                rebuildPoller()

              case ProcessPendingMessages(entry) =>
                entry.odealer.foreach: dealer =>
                  var pending = entry.pendingMessages.poll()
                  while pending != null do
                    dealer.send(ProtobufMessageCodec.encodeMessage(pending))
                    pending = entry.pendingMessages.poll()
          catch
            // One bad item must not take the thread down with it. Everything else depends on this
            // loop surviving: the rest of the queue, every other host's traffic, and all inbound
            // polling. Guarded on !shuttingDown for the same reason the poll above is -- a throw
            // from a context that shutdown() closed should unwind to the handler below and let the
            // thread run off its normal end, not be logged as a fault and retried.
            //
            // A NewHostAvailable that fails partway leaves a created socket that was never
            // registered in connectedDealers nor assigned to entry.odealer. It leaks until the
            // context closes and that host stays unsendable; the thread surviving is worth more.
            // Recorded in TODO.txt.
            case t: Throwable if !shuttingDown =>
              logger.error(s"Error processing send queue item: $t", t)

          qmsg = sendQueue.poll()
```

- [ ] **Step 2: Verify it compiles**

Run: `sbt compile`

Expected: success, no errors. Scala 3's significant indentation makes a mis-indented case a compile error, which is the check that the re-indent landed correctly.

- [ ] **Step 3: Run the full test suite**

Run: `sbt test`

Expected: PASS. Nothing exercises this path directly; this confirms nothing else regressed.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala
git commit -m "Keep one bad send-queue item from killing the ZMQ IO thread"
```

---

## Task 6: Documentation

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`, the scaladoc above `hasParkedMessages` and above the two-argument `startHostLookup`
- Modify: `TODO.txt`

- [ ] **Step 1: Extend `startHostLookup`'s scaladoc**

In `MetadataManager.scala`, in the scaladoc above `startHostLookup(hostId: HostId, phl: PendingHostLookup)`, insert this paragraph immediately before the closing `Caller must hold this object's monitor.` line:

```scala
 *  A synchronous throw from the lookup call is treated as a lookup failure rather than
 *  propagated: the pending entry is removed and phl's messages are dropped, exactly as the
 *  Failure branch does. The method therefore never throws -- which matters because
 *  getHostEntryOrQueueMessage is called from ZMQNet's send loop, where an escaping throw would
 *  end the IO thread.
 *
```

- [ ] **Step 2: Extend `hasParkedMessages`' scaladoc**

In the scaladoc above `hasParkedMessages`, replace this paragraph:

```scala
 *  Note that a failed host or pool lookup drops the entry and everything parked on it (see
 *  peekHostEntry's scaladoc for the host case), so this can go false because the message was
 *  discarded rather than sent. Nothing at this layer can tell the two apart.
```

with:

```scala
 *  Note that a failed host or pool lookup drops the entry and everything parked on it (see
 *  peekHostEntry's scaladoc for the host case), so this can go false because the message was
 *  discarded rather than sent. Nothing at this layer can tell the two apart. A lookup call that
 *  fails by throwing is handled the same way as one that returns a failed Future, so it has the
 *  same effect here.
```

- [ ] **Step 3: Label each guard with the rule it implements**

By this point `MetadataManager.scala` holds four superficially similar `catch NonFatal` blocks implementing *two* different rules, which is the real comprehension risk — a labelling problem, not a factoring one. Tasks 1, 2 and 3 all implement the **retryability rule**: a lookup that fails, however it fails, returns its subject to the never-looked-up state so a later call retries. Task 4 implements the **containment rule**: a throw costs only the store that threw.

Add a lead-in line as the first line of each catch block's comment, above the existing text. Do not reword what is already there.

In `startHostLookup`'s outer catch (the one guarding the `getHostState` call):

```scala
            // Retryability rule.
```

In `startHostLookup`'s inner catch (the one guarding `createHostEntry`):

```scala
                    // Retryability rule.
```

In `startPoolLookup`'s catch:

```scala
            // Retryability rule.
```

In the per-store handoff loop's catch:

```scala
                      // Containment rule: this one costs a store, it does not restore one.
```

Match each to the indentation of the comment line already beneath it. As built, those are 12, 20, 12 and 22 spaces respectively.

- [ ] **Step 4: Record the two ZMQNet socket leaks in TODO.txt**

In `TODO.txt`, insert these two entries immediately after the `StoreManager.checkStorageDevice leaks its activeDeviceChecks guard` block (which currently ends at line 42), each separated by a blank line:

```
ZMQNet.ioThread leaks a socket when NewHostAvailable fails partway through
  - The send-queue loop now guards each item, so a throw from dealer.connect no longer takes the
    IO thread down with it. What survives is a created socket that was never registered in
    connectedDealers and never assigned to entry.odealer: it leaks until the context closes, and
    that host stays permanently unsendable
  - Fix wants the socket closed on the failure path, which in turn wants a seam that lets a test
    make connect() fail -- the same reason the checkStorageDevice leak above is unguarded

A createHostEntry that throws late orphans the entry the IO thread already accepted
  - createHostEntry enqueues NewHostAvailable before the wakeIoThread() that can throw, so a
    throw from that wake still leaves a fully live entry behind: the IO loop drains sendQueue on
    every iteration regardless of the wake, so the dealer is created, registered in
    connectedDealers and connectedHosts, flushed, and heartbeated thereafter
  - Meanwhile MetadataManager's guard has removed the host from its map, so the next send builds
    a second entry and a second dealer for the same host. The first is unreachable from the
    manager but still live in the IO thread
  - This is the accepted cost of the guard -- an orphaned entry beats a permanent wedge -- and
    reachability is low, since wakeIoThread fails on a closed socket, i.e. during teardown of a
    process that is exiting anyway. Recorded rather than fixed
  - Distinct from the leak above: there NewHostAvailable fails, here it succeeds for an entry the
    manager has already forgotten
```

- [ ] **Step 5: Verify it compiles**

Run: `sbt compile`

Expected: success.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala TODO.txt
git commit -m "Document how a throwing lookup call is handled"
```

---

## Final verification

- [ ] **Run the whole suite**

Run: `sbt test`

Expected: PASS, no failures. `MetadataManagerExceptionSafetySuite` contributes four tests.

- [ ] **Confirm the diff touches only the five intended files**

Run: `git diff --stat 96fdbf4..HEAD -- src TODO.txt`

Expected: exactly `MetadataManager.scala`, `ZMQNet.scala`, `MetadataManagerFixture.scala`, `MetadataManagerExceptionSafetySuite.scala`, `TODO.txt`.

- [ ] **Re-read the spec's "Out of scope" list and confirm nothing there was touched**

Specifically: the router-message guard at `ZMQNet.scala:389-392` still catches `Throwable` unconditionally, `MetadataManager`'s constructor loop over bootstrap hosts is still unguarded, and `refreshBootstrapConfig` is unchanged.
