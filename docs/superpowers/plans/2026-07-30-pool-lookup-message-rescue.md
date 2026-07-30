# Pool Lookup Message Rescue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `MetadataManager.startPoolLookup` from silently discarding messages parked behind a pool lookup when the resolved pool names a host the process has never looked up.

**Architecture:** `startHostLookup` is split so a caller can supply an already-seeded `PendingHostLookup`. The `hosts.get(...) == None` branch of `startPoolLookup`'s `Success` handler builds one, drains the store's parked queue into it, and starts the lookup — all inside the `synchronized` block that already cleared the pool entry, so the messages are never untracked. Test observability comes from replacing the fixture's no-op `NetworkImplInterface` with one that records what it is handed.

**Tech Stack:** Scala 3, ScalaTest (`AnyFunSuite` + `Matchers`), sbt.

**Spec:** `docs/superpowers/specs/2026-07-30-pool-lookup-message-rescue-design.md`

---

## Background for the implementer

`MetadataManager` (`src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`) sits
between the ZeroMQ network layer and the Aspen cluster. Messages are addressed to logical entities
— a `HostId`, or a `StoreId` (a pool UUID plus a store index) — and MetadataManager's job is to
turn those into a resolved host entry the network layer can open a socket to. Resolution is
asynchronous, so it parks messages while it waits:

- `hosts: Map[HostId, Either[PendingHostLookup, T]]` — `Left` while the host state is being
  fetched, `Right` once resolved. A `PendingHostLookup` owns one `EvictingQueue[Message]`.
- `pendingPoolLookups: Map[PoolId, PendingPoolLookup]` — a `PendingPoolLookup` owns one
  `EvictingQueue[Message]` *per store* in the pool, because the pool state is what reveals which
  host each store lives on.
- `stores: Map[StoreId, HostId]` — the resolved mapping.

All of that state is guarded by `synchronized` on the manager itself. `startHostLookup` and
`startPoolLookup` are private and do **not** take the lock themselves — every caller already holds
it. Keep that convention.

When a pool lookup resolves, the messages parked per-store have to be handed to the right host.
That handoff is the code being fixed.

`EvictingQueue` (`src/main/scala/org/aspen_ddp/aspen/common/util/EvictingQueue.scala`) is a
fixed-capacity queue that drops its oldest element on overflow. Its API is `enqueue(a)`,
`dequeue(): Option[A]`, `size`, `isEmpty`.

Tests use `ExecutionContext.parasitic`, which runs a future's continuation inline on whichever
thread completes the promise. That is what lets a test write `promise.success(...)` and assert
MetadataManager's state on the very next line with no waiting — and it is also why seeding a
`PendingHostLookup` *before* starting its lookup is load-bearing rather than stylistic.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala` | The fix: split `startHostLookup`, repair the `None` branch, extend one scaladoc |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala` | Shared test fixture: gains a recording `NetworkImplInterface` and a pool-state builder |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPoolLookupSuite.scala` | **New.** All coverage of what a resolving pool lookup does with parked messages |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerDrainSuite.scala` | Mechanical: destructure the fixture's new triple |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPeekHostEntrySuite.scala` | Mechanical: destructure the fixture's new triple |
| `TODO.txt` | Remove the entry this plan closes |

`ZMQNet.scala`, `Main.scala`, and the codec are untouched.

---

## Task 1: Make the fixture record what the network layer is handed

The fixture's current `NetworkImplInterface` is the thinnest thing that satisfies the type:
`createHostEntry` ignores its `queuedMessages` argument and `storeResolved` is `() => ()`. Nothing
can observe a message arriving, so a test could only assert that a lookup was *started* — which
would pass against a broken fix that started the lookup and still dropped the queue.

This task is a pure refactor: no production code changes, and the existing suites must still pass
unchanged in meaning.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerDrainSuite.scala:19,32,46,54`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPeekHostEntrySuite.scala:11,17,29,39,51`

- [ ] **Step 1: Extend the fixture's imports**

In `MetadataManagerFixture.scala`, replace the metadata import on line 7:

```scala
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, HostState, StoragePoolState}
```

with:

```scala
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, HostState, StorageDeviceId, StorageDeviceSetId, StoragePoolState}
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
```

`scala.collection.mutable` and `java.util.UUID` are already imported (lines 16 and 15).

- [ ] **Step 2: Add the recording network implementation**

In `MetadataManagerFixture.scala`, insert this class between `LookupRecordingClient` and the
`MetadataManagerFixture` trait — that is, after line 81 and before the `/** A MetadataManager over
a temp bootstrap config ... */` comment:

```scala
/** A NetworkImplInterface that drains what MetadataManager hands it, the way ZMQNet does, and
 *  records where each message ended up.
 *
 *  Draining rather than peeking is deliberate: ZMQNet's createHostEntry and storeResolved both
 *  empty the EvictingQueue they are given before returning, so a double that left messages in
 *  place would not model the handoff it is here to observe.
 *
 *  Lock-ordering note: MetadataManager calls both methods while holding its own monitor, so the
 *  order is always manager then this object. Tests read the recordings without holding the
 *  manager's lock, which keeps that order intact.
 */
class RecordingNetworkImpl extends MetadataManager.NetworkImplInterface[MetadataManager.HostEntry]:

  /** Every (hostId, storeId) pair storeResolved was called with, in call order. */
  val storeResolutions: mutable.ListBuffer[(HostId, StoreId)] = mutable.ListBuffer()

  private var delivered: Map[HostId, List[Message]] = Map()

  /** Messages drained on `hostId`'s behalf, in arrival order. Empty if it received none. */
  def deliveredTo(hostId: HostId): List[Message] =
    synchronized:
      delivered.getOrElse(hostId, Nil)

  /** Caller holds this object's monitor. */
  private def drain(hostId: HostId, queuedMessages: EvictingQueue[Message]): Unit =
    val buf = mutable.ListBuffer[Message]()
    var omsg = queuedMessages.dequeue()
    while omsg.isDefined do
      omsg.foreach(buf.append)
      omsg = queuedMessages.dequeue()
    delivered += hostId -> (delivered.getOrElse(hostId, Nil) ++ buf.toList)

  def createHostEntry(hostId: HostId,
                      name: String,
                      address: String,
                      dataPort: Int,
                      cncPort: Int,
                      storeTransferPort: Int,
                      queuedMessages: EvictingQueue[Message]): MetadataManager.HostEntry =
    synchronized:
      drain(hostId, queuedMessages)
    new MetadataManager.HostEntry(hostId, name, address, dataPort, cncPort, storeTransferPort)

  def storeResolved(hostEntry: MetadataManager.HostEntry,
                    storeId: StoreId,
                    queuedMessages: EvictingQueue[Message]): Unit =
    synchronized:
      storeResolutions += hostEntry.hostId -> storeId
      drain(hostEntry.hostId, queuedMessages)
```

- [ ] **Step 3: Add the pool-state builder to the fixture trait**

In `MetadataManagerFixture.scala`, add this method to the `MetadataManagerFixture` trait,
immediately after the `remoteHostState` val (currently lines 102-103):

```scala
  /** A StoragePoolState placing one store on each of `hostIds`, store index matching position.
   *
   *  Everything but the pool id and the store entries is filler. MetadataManager reads only
   *  poolState.stores, and each StoreEntry only for its hostId.
   */
  protected def poolStateWith(poolId: PoolId, hostIds: HostId*): StoragePoolState =
    val stores = hostIds.map: hostId =>
      StoragePoolState.StoreEntry(hostId, StorageDeviceId(UUID.randomUUID()))
    StoragePoolState(
      poolId,
      "test_pool",
      Replication(hostIds.size, hostIds.size),
      None,
      stores.toArray,
      RocksDBConfig(),
      StorageDeviceSetId(UUID.randomUUID()))
```

- [ ] **Step 4: Have `newManager()` return the recorder**

In `MetadataManagerFixture.scala`, replace the whole `newManager()` method and its scaladoc
(currently lines 125-146) with:

```scala
  /** A MetadataManager over the fixture's bootstrap config, wired to a fresh recording client and
   *  a fresh RecordingNetworkImpl. Returning the impl is what lets a suite assert that a message
   *  actually reached a host rather than merely that a lookup was started. */
  protected def newManager(): (MetadataManager[MetadataManager.HostEntry], LookupRecordingClient, RecordingNetworkImpl) =
    val impl = new RecordingNetworkImpl
    val client = new LookupRecordingClient
    val mgr = new MetadataManager[MetadataManager.HostEntry](bootstrapConfigFile, impl)
    mgr.setAspenClient(client)
    (mgr, client, impl)
```

- [ ] **Step 5: Update the nine existing call sites**

Neither existing suite uses the recorder, so each site takes a wildcard. In
`MetadataManagerDrainSuite.scala` (lines 19, 32, 46, 54) and
`MetadataManagerPeekHostEntrySuite.scala` (lines 11, 17, 29, 39, 51), change every occurrence of:

```scala
    val (mgr, client) = newManager()
```

to:

```scala
    val (mgr, client, _) = newManager()
```

- [ ] **Step 6: Run both existing suites to verify nothing changed in meaning**

Run: `sbt 'testOnly *MetadataManagerDrainSuite *MetadataManagerPeekHostEntrySuite'`

Expected: PASS, 9 tests total (4 + 5). Draining at `createHostEntry` time is invisible to both
suites — `hasParkedMessages` inspects only `Left` entries, and `peekHostEntry` inspects only the
entry, never its queue.

- [ ] **Step 7: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerDrainSuite.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPeekHostEntrySuite.scala
git commit -m "Record message handoffs in the MetadataManager test fixture"
```

---

## Task 2: Rescue the parked queue instead of dropping it

The core fix, driven by the test that is red against today's code.

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPoolLookupSuite.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala:209-232` (split `startHostLookup`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala:255-256` (the `None` branch)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPoolLookupSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** Coverage for what startPoolLookup does with the messages parked on it once the pool resolves.
 *
 *  The message type is incidental -- MetadataManager never inspects a message, only the address
 *  it was sent to -- so these use the same nudge the drain suite does. Each call to nudge()
 *  carries a fresh device id, which makes individual messages distinguishable in assertions.
 */
class MetadataManagerPoolLookupSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  test("a resolving pool lookup starts a host lookup for a store on an unknown host"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val msg = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg) should be(None)
    client.poolLookups.toList should be(List(unknownPoolId))

    // remoteHostId is absent from the bootstrap config and has never been looked up, so resolving
    // the pool onto it takes the hosts.get(...) == None branch -- the one that used to drop the
    // queue on the floor.
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, remoteHostId))

    client.lookups.toList should be(List(remoteHostId))
    // The message moved from the pool's store queue onto the host lookup rather than vanishing,
    // so the exit drain still knows it is holding something.
    mgr.hasParkedMessages should be(true)
    impl.deliveredTo(remoteHostId) should be(empty)

    client.lookupPromise(remoteHostId).success(remoteHostState)

    impl.deliveredTo(remoteHostId) should be(List(msg))
    mgr.hasParkedMessages should be(false)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *MetadataManagerPoolLookupSuite'`

Expected: FAIL at `client.lookups.toList should be(List(remoteHostId))` with `List() was not equal
to List(HostId(88888888-8888-8888-8888-888888888888))`. Today's `None` branch starts no lookup, so
nothing was ever recorded. (The two assertions after it would fail too: `hasParkedMessages` is
already false because `pendingPoolLookups -= poolId` ran, and nothing is ever delivered.)

- [ ] **Step 3: Split `startHostLookup` so the caller owns the queue**

In `MetadataManager.scala`, replace `startHostLookup` (lines 209-232) with:

```scala
  private def startHostLookup(hostId: HostId, oMsg: Option[Message]): Unit =
    val phl = new PendingHostLookup(pendingHostLookupQueueSize)
    oMsg.foreach(phl.enqueueMessage)
    startHostLookup(hostId, phl)

  /** Starts `hostId`'s lookup, parking `phl`'s messages until it resolves.
   *
   *  The caller supplies the PendingHostLookup already seeded rather than adding to it
   *  afterwards, because getHostState's continuation can run inline on this thread whenever the
   *  future is already complete, and it builds the host entry from phl.messageQueue. Anything
   *  enqueued after this returns would miss that handoff.
   *
   *  Caller must hold this object's monitor. */
  private def startHostLookup(hostId: HostId, phl: PendingHostLookup): Unit =
    oClient match
      case None => logger.error(s"Host lookup preformed before AspenClient initialized. HostId: $hostId")
      case Some(client) =>
        given ExecutionContext = client.clientContext
        hosts += hostId -> Left(phl)
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
```

The `oClient == None` case behaves exactly as before: it logs and installs nothing, and the
freshly built `phl` is discarded along with whatever the caller seeded into it.

- [ ] **Step 4: Rescue the queue in the `None` branch**

In `MetadataManager.scala`, in `startPoolLookup`'s `Success` handler, replace:

```scala
                  hosts.get(se.hostId) match
                    case None =>
```

with:

```scala
                  hosts.get(se.hostId) match
                    case None =>
                      // The pool named a host this process has never looked up. Move the queue
                      // onto a host lookup instead of dropping it: pendingPoolLookups was cleared
                      // above, so nothing else will ever come back for these messages. Doing it
                      // here, inside the same synchronized block, is also what keeps
                      // hasParkedMessages from dipping false while they are between queues.
                      val phl = new PendingHostLookup(pendingHostLookupQueueSize)
                      phl.drainIntoQueue(storeQueue)
                      startHostLookup(se.hostId, phl)
```

Leave the `Some(e)` branch below it exactly as it is.

This branch is reachable at most once per host even when a pool puts several stores on the same
one: `startHostLookup` installs `hosts += hostId -> Left(phl)` synchronously before returning — or
`Right(entry)`, if the continuation ran inline — so a later iteration matches `Some(...)` instead.
Task 3's shared-host test pins that. The one exception is an inline *failure*, whose continuation
does `hosts -= hostId`; each remaining store on that host then retries the lookup. The messages
are dropped either way, so the cost is a duplicate lookup and a duplicate error log.

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *MetadataManagerPoolLookupSuite'`

Expected: PASS, 1 test.

- [ ] **Step 6: Run the neighbouring suites for regressions**

Run: `sbt 'testOnly *MetadataManager*'`

Expected: PASS, 10 tests (4 drain + 5 peek + 1 new).

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPoolLookupSuite.scala
git commit -m "Rescue messages parked on a pool lookup that names an unknown host"
```

---

## Task 3: Pin the surrounding behaviour

Four more tests. Unlike Task 2's, these pass against the code as it stands after Task 2 — they
are regression guards, and two of them cover branches that have never had any coverage at all.

The first of the four is the important one. Task 2's test does not actually pin the seeding order
the design calls load-bearing: `lookupPromise(remoteHostId)` is still unresolved when
`startHostLookup` registers its continuation, so that continuation always runs later, and swapping
`phl.drainIntoQueue(storeQueue)` to *after* the `startHostLookup` call still passes. Completing the
host promise before the pool promise is what forces `getHostState` to hand back an already-complete
future, so `parasitic` runs the continuation inline — inside the pool loop, before a
drain-after-start would have run.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPoolLookupSuite.scala`

- [ ] **Step 1: Add the inline-resolution, shared-host, already-resolved-host, and failed-lookup tests**

Append these four tests to `MetadataManagerPoolLookupSuite.scala`:

```scala
  test("a host that resolves inline still receives the rescued messages"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val msg = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg) should be(None)

    // Completing the host promise up front makes getHostState hand back an already-completed
    // future, so parasitic runs startHostLookup's continuation inline -- inside the pool loop,
    // and inside the same synchronized block. That is the case the seed-before-start ordering
    // exists for: the continuation builds the host entry out of phl.messageQueue, so anything
    // drained in after the call would miss the handoff entirely.
    client.lookupPromise(remoteHostId).success(remoteHostState)

    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, remoteHostId))

    client.lookups.toList should be(List(remoteHostId))
    impl.deliveredTo(remoteHostId) should be(List(msg))
    mgr.hasParkedMessages should be(false)
```

Then these three:

```scala
  test("two stores on the same unknown host share a single lookup"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val store1 = StoreId(unknownPoolId, 1.toByte)
    val msg0 = nudge()
    val msg1 = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg0) should be(None)
    // The second send finds the pool lookup already pending and parks behind it in its own
    // per-store queue, so there is still only one pool lookup in flight.
    mgr.getHostEntryOrQueueMessage(store1, msg1) should be(None)
    client.poolLookups.toList should be(List(unknownPoolId))

    client.poolLookupPromise(unknownPoolId).success(
      poolStateWith(unknownPoolId, remoteHostId, remoteHostId))

    // Store 0's iteration installs the pending host entry, so store 1's iteration finds it and
    // drains into the same queue rather than starting a second lookup and orphaning the first.
    client.lookups.toList should be(List(remoteHostId))
    mgr.hasParkedMessages should be(true)

    client.lookupPromise(remoteHostId).success(remoteHostState)

    impl.deliveredTo(remoteHostId) should be(List(msg0, msg1))
    mgr.hasParkedMessages should be(false)

  test("a store on an already-resolved host is handed over by storeResolved"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val msg = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg) should be(None)

    // bootstrapHostId is Right(...) from the constructor, so this takes the branch the fix leaves
    // alone -- and which nothing covered before.
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, bootstrapHostId))

    client.lookups.toList should be(empty)
    impl.storeResolutions.toList should be(List(bootstrapHostId -> store0))
    impl.deliveredTo(bootstrapHostId) should be(List(msg))
    mgr.hasParkedMessages should be(false)

  test("a message rescued onto a host lookup is still dropped if that lookup fails"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)

    mgr.getHostEntryOrQueueMessage(store0, nudge()) should be(None)
    client.poolLookupPromise(unknownPoolId).success(poolStateWith(unknownPoolId, remoteHostId))
    mgr.hasParkedMessages should be(true)

    // The limit the fix does not remove: a failed lookup drops the entry and everything parked on
    // it. What changed is only where that can happen -- a message parked on a pool lookup can now
    // be dropped one stage later, by the host lookup it was moved to. hasParkedMessages' scaladoc
    // says so, and this is what it is describing.
    client.lookupPromise(remoteHostId).failure(new NoSuchElementException("no such host"))

    mgr.hasParkedMessages should be(false)
    impl.deliveredTo(remoteHostId) should be(empty)
```

- [ ] **Step 2: Run the suite**

Run: `sbt 'testOnly *MetadataManagerPoolLookupSuite'`

Expected: PASS, 5 tests.

- [ ] **Step 3: Verify the inline test actually pins the ordering**

The other four tests pass whether or not the seeding order is right, so confirm this one does not.
Temporarily reorder the `None` branch in `MetadataManager.scala` to drain *after* starting:

```scala
                      val phl = new PendingHostLookup(pendingHostLookupQueueSize)
                      startHostLookup(se.hostId, phl)
                      phl.drainIntoQueue(storeQueue)
```

Run: `sbt 'testOnly *MetadataManagerPoolLookupSuite'`

Expected: FAIL, exactly one test — "a host that resolves inline still receives the rescued
messages" — at `impl.deliveredTo(remoteHostId) should be(List(msg))` with `List() was not equal to
List(CheckStorageDevice(...))`. The other four still pass, which is the point.

Then revert the reorder (restore drain-then-start) and re-run to confirm 5 passing. Do **not**
commit the mutant.

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPoolLookupSuite.scala
git commit -m "Cover shared hosts, resolved hosts, and the surviving drop in pool lookup"
```

- [ ] **Step 5: Pin that each rescued store queue gets its own host lookup**

Added after Task 3's code review, which found that the per-iteration freshness of the
`PendingHostLookup` is unpinned by the five tests above. The same-host and different-host cases
turn out to pin *different* invariants: the shared-host test covers "one lookup per host", but
nothing covers "one queue per store". Hoisting the allocation above the
`poolState.stores.zipWithIndex.foreach` loop — a plausible "don't allocate in a loop" refactor —
passes all five, and under it two unknown hosts share one queue, so whichever resolves first
receives the other's messages. Messages on the wrong socket is a worse failure than the drop this
plan set out to fix.

Add a second unknown host to `MetadataManagerFixture.scala`, immediately after `remoteHostState`
and before `poolStateWith`:

```scala
  /** A second host absent from the bootstrap config. Two distinct unknown hosts are what make it
   *  observable that each rescued store queue gets its own PendingHostLookup rather than sharing
   *  one across the pool's stores. */
  protected val otherHostId: HostId = HostId(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))

  protected val otherHostState: HostState =
    HostState(otherHostId, "other_host", "10.0.0.10", 7000, 7001, 7002, Set())
```

Append to `MetadataManagerPoolLookupSuite.scala`:

```scala
  test("two stores on two different unknown hosts get separate lookups"):
    val (mgr, client, impl) = newManager()

    val store0 = StoreId(unknownPoolId, 0.toByte)
    val store1 = StoreId(unknownPoolId, 1.toByte)
    val msg0 = nudge()
    val msg1 = nudge()

    mgr.getHostEntryOrQueueMessage(store0, msg0) should be(None)
    mgr.getHostEntryOrQueueMessage(store1, msg1) should be(None)

    client.poolLookupPromise(unknownPoolId).success(
      poolStateWith(unknownPoolId, remoteHostId, otherHostId))

    // Each store's iteration builds its own PendingHostLookup. Sharing one across the loop would
    // pass every other test in this suite and still deliver both messages to whichever host
    // resolved first -- messages on the wrong socket, which is worse than the drop this fixed.
    client.lookups.toList should be(List(remoteHostId, otherHostId))

    client.lookupPromise(remoteHostId).success(remoteHostState)
    client.lookupPromise(otherHostId).success(otherHostState)

    impl.deliveredTo(remoteHostId) should be(List(msg0))
    impl.deliveredTo(otherHostId) should be(List(msg1))
    mgr.hasParkedMessages should be(false)
```

Also make the failed-lookup test name the lookup it fails, by adding this after its
`mgr.hasParkedMessages should be(true)` line:

```scala
    client.lookups.toList should be(List(remoteHostId))
```

Verify by mutation as in Step 3: hoist the allocation above the loop, confirm the new test is the
one that fails (both messages delivered to `remoteHostId`, none to `otherHostId`), revert, and
re-run for 6 passing. Then `sbt 'testOnly *MetadataManager*'` for 15, and commit the two test files
as `Pin that each rescued store queue gets its own host lookup`.

---

## Task 4: Say where a parked message can now be dropped

Three scaladoc corrections and the TODO removal. Two of the three came out of Task 2's code
review, which found claims in the new comments that are true of the tests but overstated about
production.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala` (`hasParkedMessages` scaladoc, `startHostLookup`'s scaladoc, the `None` branch comment)
- Modify: `TODO.txt:1-14`

- [ ] **Step 1: Extend `hasParkedMessages`' scaladoc**

In `MetadataManager.scala`, replace the closing paragraph of `hasParkedMessages`' scaladoc:

```scala
   *  Note that a failed host or pool lookup drops the entry and everything parked on it (see
   *  peekHostEntry's scaladoc for the host case), so this can go false because the message was
   *  discarded rather than sent. Nothing at this layer can tell the two apart. */
```

with:

```scala
   *  Note that a failed host or pool lookup drops the entry and everything parked on it (see
   *  peekHostEntry's scaladoc for the host case), so this can go false because the message was
   *  discarded rather than sent. Nothing at this layer can tell the two apart.
   *
   *  A message parked on a pool lookup moves to a host lookup when the pool resolves onto a host
   *  this process has not looked up yet, so that drop can happen at a later stage than the one
   *  the message was originally parked on. */
```

`awaitPendingMessagesSent`'s scaladoc in `ZMQNet.scala` already states the limit in terms that
cover both stages ("a failed host or pool lookup drops the entry and the messages parked on it")
and needs no change.

- [ ] **Step 2: Correct the two over-claims in `startHostLookup`'s scaladoc**

Task 2's scaladoc on the `PendingHostLookup` overload says the continuation can run inline
"whenever the future is already complete". That is not true of production: `clientContext` is an
`ExecutionContext.fromExecutorService(...)` (`Main.scala:707`, and the same at 821, 941, 1040),
which is a `BatchingExecutor` — a callback registered on an already-complete future from inside a
task already running on that EC is appended to the current batch and runs after the current task,
not inline. Seeding first is still the right posture for a pluggable EC, but the sentence asserts
something about production that does not hold. The scaladoc also documents the seeding contract
without saying what the `oClient == None` branch does with the caller's messages.

In `MetadataManager.scala`, replace the scaladoc on the `PendingHostLookup` overload:

```scala
  /** Starts `hostId`'s lookup, parking `phl`'s messages until it resolves.
   *
   *  The caller supplies the PendingHostLookup already seeded rather than adding to it
   *  afterwards, because getHostState's continuation can run inline on this thread whenever the
   *  future is already complete, and it builds the host entry from phl.messageQueue. Anything
   *  enqueued after this returns would miss that handoff.
   *
   *  Caller must hold this object's monitor. */
```

with:

```scala
  /** Starts `hostId`'s lookup, parking `phl`'s messages until it resolves.
   *
   *  The caller supplies the PendingHostLookup already seeded rather than adding to it
   *  afterwards, because getHostState's continuation can run inline on this thread -- it does in
   *  tests under ExecutionContext.parasitic, and would under any EC that dispatches an
   *  already-complete future's callback directly -- and it builds the host entry out of
   *  phl.messageQueue. Anything enqueued after this returns would miss that handoff.
   *
   *  If no client is set the lookup is not started and phl's messages are discarded along with
   *  it. That is unreachable from a caller already running inside a client callback, which is
   *  where the rescued-queue call site lives.
   *
   *  Caller must hold this object's monitor. */
```

- [ ] **Step 3: Note the queue-size assumption the rescue rests on**

`EvictingQueue` drops the *oldest* entry on overflow, silently. The rescue moves up to
`pendingStoreLookupQueueSize` messages into a queue sized `pendingHostLookupQueueSize`, so it is
loss-free only while the latter is at least the former. The defaults (20 and 100,
`MetadataManager.scala:57-58`) satisfy that, and `ZMQNet` is the only production construction and
takes both defaults — but both are public constructor parameters with no enforced relationship.

In `MetadataManager.scala`, extend the comment on the `None` branch by appending one sentence to
it, so the block reads:

```scala
                    case None =>
                      // The pool named a host this process has never looked up. Move the queue
                      // onto a host lookup instead of dropping it: pendingPoolLookups was cleared
                      // above, so nothing else will ever come back for these messages. Doing it
                      // here, inside the same synchronized block, is also what keeps
                      // hasParkedMessages from dipping false while they are between queues.
                      // The move is loss-free only while pendingHostLookupQueueSize is at least
                      // pendingStoreLookupQueueSize; otherwise EvictingQueue silently drops the
                      // oldest of what is being rescued.
                      val phl = new PendingHostLookup(pendingHostLookupQueueSize)
                      phl.drainIntoQueue(storeQueue)
                      startHostLookup(se.hostId, phl)
```

- [ ] **Step 4: Remove the closed entry from `TODO.txt`**

Delete lines 1-14 of `TODO.txt` — the entry beginning `MetadataManager silently drops parked
messages when a resolving pool lookup` through its final bullet (`it the storeQueue instead of
dropping it`) and the blank line that follows. The file must now begin with:

```
StoreManager.activeDeviceChecks is keyed by device id only, and load state can
```

- [ ] **Step 5: Verify the whole file still compiles and the suites pass**

Run: `sbt 'testOnly *MetadataManager*'`

Expected: PASS, 15 tests (4 drain + 5 peek + 6 pool lookup).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala TODO.txt
git commit -m "Note where a parked message can now be dropped"
```

- [ ] **Step 7: Three corrections from Task 4's code review**

Added after Task 4's code review, which accepted the three edits above but found two of them narrower
than the thing they describe, and one omission at the point of use.

**(a) The `None` branch says nothing about why the queue is allocated inside the loop.** Steps 1-3
explain why the queue is moved and why the move happens inside the `synchronized` block, but not why
`new PendingHostLookup(...)` sits inside `poolState.stores.zipWithIndex.foreach`. Hoisting it is a
plausible "don't allocate in a loop" refactor, and under it every rescued store queue shares one
object referenced by several `Left` entries — whichever host resolves first drains all of them and
the messages go out on the wrong socket. Task 3's Step 5 test pins this; the code site should say it
too. Append to the `None`-branch comment:

```scala
                      // A fresh queue per store: one shared across the loop would end up
                      // referenced by every rescued host entry, so whichever host resolved first
                      // would drain all of them.
```

**(b) The `hasParkedMessages` note names only half the stage-crossing.** The `case Left(phl) =>
phl.drainIntoQueue(storeQueue)` branch immediately below the `None` branch — which pre-dates this
work — moves a pool-parked message onto an *in-flight* host lookup, with the same exposure to a later
host-lookup failure. As written a reader could conclude that case is safe. Widen the condition, which
costs no length:

```scala
   *  A message parked on a pool lookup moves to a host lookup when the pool resolves onto a host
   *  that is not already resolved, so that drop can happen at a later stage than the one the
   *  message was originally parked on. */
```

**(c) Two small fixes to the queue-size sentence.** "loss-free only while A is at least B" literally
asserts that loss occurs whenever A < B, which is false — loss depends on actual occupancy, not the
configured sizes. Change `The move is loss-free only while` to `The move is guaranteed loss-free only
while`. Separately, whoever can *break* the invariant is whoever overrides the two constructor
parameters at `MetadataManager.scala:57-58`, and nothing there points at the constraint; the spec
records the decision not to enforce it with a `require`, so a pointer is the cheap substitute. Add,
above `pendingHostLookupQueueSize`:

```scala
                                                      // Must be >= pendingStoreLookupQueueSize; see
                                                      // startPoolLookup's rescue of a parked store queue
```

Wrapped to two lines deliberately: as a single line it ran to 176 characters, a 54-character outlier
over anything else in the file.

Run `sbt 'testOnly *MetadataManager*'` (still 15 tests, still passing — nothing here is behavioural)
and commit.

---

## Task 5: Full verification

**Files:** none modified.

- [ ] **Step 1: Compile everything**

Run: `sbt compile`

Expected: success, no errors. This catches any other caller of the fixture or of
`startHostLookup` that the targeted test runs would miss.

- [ ] **Step 2: Run the full test suite**

Run: `sbt test`

Expected: PASS. If anything unrelated to `common.network` fails, check whether it also fails on
`git stash` before treating it as a regression from this work — report either outcome honestly
rather than assuming.

Outcome: `sbt compile` clean; `sbt test` passed with 562 tests across 71 suites, 0 failures.

- [ ] **Step 3: Close the gaps the final review found**

The whole-implementation review passed the production code — it traced the concurrency paths and
found no defect — but named two test gaps and confirmed both by mutation. Test-only work:

**(a) Nothing parks two messages on one store queue.** Every test enqueued exactly one message per
`StoreId`, so `drainIntoQueue`, the function the rescue is built on, had no multi-element coverage
in either branch. Replacing its `while` loop with a single
`storeQueue.dequeue().foreach(messageQueue.enqueue)` left all 15 tests green — a regression
discarding all but the first parked message per store, the exact silent loss this work exists to
eliminate, would have shipped. Fixed by parking two messages on store 0 in the first pool-lookup
test and asserting `impl.deliveredTo(remoteHostId) should be(List(msg0, msg1))`, which pins
completeness and order together.

**(b) The inline test never asserted the host reached `Right`.** Delivery plus
`hasParkedMessages == false` both still hold if the entry is left `Left(phl)` with an emptied queue.
Moving `hosts += hostId -> Left(phl)` below the `onComplete` registration left all 15 green, and
under inline resolution that wedges the host pending forever: every later message to it parks
permanently and `awaitPendingMessagesSent` never drains. The peek suite cannot catch it because its
promises are incomplete at registration time, so statement order is invisible there. Fixed with one
line: `mgr.peekHostEntry(remoteHostId).map(_.name) should be(Some("remote_host"))`.

Two minor items taken alongside: a first test for `startPoolLookup`'s `Failure` branch, which had no
coverage at all (the pool-stage sibling of the drain suite's host-failure test), and making the
fixture's `storeResolutions` private behind a synchronized `resolutions` accessor, matching
`deliveredTo`.

Run: `sbt 'testOnly *MetadataManager*'` — 16 tests (4 drain + 5 peek + 7 pool lookup). Then `sbt
test` — 563 tests, 0 failures.

---

## Out of scope

Per the spec, deliberately not addressed:

- **Store queues the resolved pool never claims.** The loop iterates `poolState.stores`, so a
  queue keyed by a store index the pool does not contain is never visited and is discarded. It
  needs an out-of-range `StoreId`, and there is nowhere to send a message addressed to a store the
  pool says does not exist. No fix, no log line.
- **The failed-lookup drop itself.** Both `startHostLookup` and `startPoolLookup` discard their
  queues on failure. Task 3's fourth test pins that behaviour rather than changing it.
- **Pre-warming host lookups for stores with no parked messages.** The loop touches a host only
  when a queue exists for its store; resolving every host in a pool eagerly would be speculative
  work.
- **Exception safety inside the handoff loop.** Raised by the final review, deliberately left for a
  separate decision. Any throw from the per-store body escapes `foreach`, so the remaining stores
  never get their `stores += sid -> se.hostId` mapping installed and their rescued queues are
  dropped; the exception surfaces only via the EC's `reportFailure`. The loop was already exposed
  this way through `networkImplInterface.storeResolved` — `ZMQNet`'s version calls
  `wakeIoThread()`, which sends on a socket that can be dead during CLI teardown — and this work
  adds `client.getHostState` as a second throwing call site. Wrapping the body in a `try`/`catch`
  that logs and continues would cost one store rather than the tail of the pool, but it changes
  behaviour beyond the drop this plan scoped, so it belongs to its own change.
