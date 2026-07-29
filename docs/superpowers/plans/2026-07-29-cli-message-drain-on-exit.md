# CLI Message Drain On Exit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop short-lived CLI commands from abandoning the `CheckStorageDevice` nudges they send, so a host acts on a new pool or store transfer immediately instead of waiting up to an hour for its next periodic device check.

**Architecture:** Two halves. First, `BaseAspenClient.createStoragePool` and `AspenClient.transferStore` stop sending their nudges from a detached `tx.result.foreach` callback and instead send them from a `.map` on the future the caller awaits — so there is something to drain by the time the command returns. Second, `ZMQNet.awaitHostMessagesSent(hostId, timeout)` is generalized to `awaitPendingMessagesSent(timeout)`, which polls all three places a message can be held, and `Main` calls it once at the single `System.exit` site rather than each command remembering to.

**Tech Stack:** Scala 3, sbt, ScalaTest (`AnyFunSuite` for unit suites, `AsyncFunSuite` via `IntegrationTestSuite`), ZeroMQ via jeromq, scribe for logging.

**Spec:** `docs/superpowers/specs/2026-07-29-cli-message-drain-on-exit-design.md`

---

## File Structure

| File | Change | Responsibility after the change |
|------|--------|----------------------------------|
| `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` | Modify | Gains `sendBestEffortHostMessage`, the one place a latency-only message is sent without being allowed to fail its caller. `transferStore` yields its nudge instead of sending it from a callback. |
| `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` | Modify | `createStoragePool` builds one nudge per device while staging and sends them all from a `.map` on the transaction future. |
| `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala` | Modify | Gains two read-only accessors, `resolvedHostEntries` and `hasParkedMessages`, so a drain can see messages parked behind lookups for hosts it cannot name. |
| `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala` | Modify | `awaitHostMessagesSent` is replaced by `awaitPendingMessagesSent`, whose predicate covers the send queue, parked messages, and every resolved host entry. |
| `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` | Modify | Registers the network on creation, drains once before `System.exit`, hoists the drain/linger constants, and drops `create_storage_device`'s bespoke drain. |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerTestFixture.scala` | Create | Shared test fixture: the recording client and the temp bootstrap config that two `MetadataManager` suites both need. |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPeekHostEntrySuite.scala` | Modify | Loses its private copy of the fixture; keeps its tests unchanged. |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerDrainSuite.scala` | Create | Unit coverage for the two new accessors, including the failure paths that drop parked messages. |
| `src/test/scala/org/aspen_ddp/aspen/client/DeviceCheckNudgeSuite.scala` | Create | Integration coverage that each client method sends exactly one correctly-addressed nudge per device. Does not, and cannot, test send ordering — see Task 1. |
| `TODO.txt` | Modify | The completed item is removed. |

**Ordering:** Tasks 1 and 2 (the client restructure) come first because nothing downstream is meaningful without them — a drain added to a process that has not yet enqueued its nudge observes an empty queue and reports success. Task 3 is a pure test refactor that Task 4 depends on. Task 5 changes `ZMQNet` and `Main` together because deleting `awaitHostMessagesSent` breaks `Main`'s compile until its call site is updated.

**Note on `CheckStorageDevice`:** its fields are `(toHost: HostId, fromClient: ClientId, deviceId: StorageDeviceId)`. The third field is `deviceId`, not `storageDeviceId`. Assertions below use `.deviceId`.

---

### Task 1: `createStoragePool` sends its nudges inside the awaited chain

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:29` (trait declaration), `:303` (next to `sendHostMessage`)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala:285-375` (`createStoragePool`)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/DeviceCheckNudgeSuite.scala` (create)

**Background:** `TransactionImpl.commit()` returns the *same* promise as `TransactionImpl.result` (documented at `TransactionImpl.scala:131-136`). So today's `tx.result.foreach { ... sendHostMessage(msg) }` is a second, independent listener on that promise. On the CLI's three-thread `clientContext` there is no ordering between it and the continuation that resolves the future the CLI awaits. This task removes the second listener.

**These tests do not test the ordering property, and must not claim to.** `IntegrationTestSuite` extends `AsyncFunSuite`, whose `executionContext` is serial, so the detached callback is always queued ahead of the test's own assertion continuation. The tests pass before the fix as well as after — confirmed empirically by reverting both client methods and re-running — and they cannot detect a return to the detached callback either. The spec records why (`Testing` section). Do not expect a red run in Step 2 below; the step verifies the *suite compiles and runs*.

What the tests are worth keeping for is coverage that did not previously exist: a nudge is sent at all, exactly one per distinct device, addressed to the right host with the right device id. They are named for that. The ordering property is guarded by the comment at `stageDeviceUpdate` and by review, not by a test.

- [ ] **Step 1: Write the regression test**

Create `src/test/scala/org/aspen_ddp/aspen/client/DeviceCheckNudgeSuite.scala`:

```scala
package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId}
import org.aspen_ddp.aspen.common.network.CheckStorageDevice
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
import org.aspen_ddp.aspen.server.{HostConfig, StorageDeviceManager}

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** These tests do NOT verify the send-ordering property, and cannot.
 *
 *  The bug they were written alongside is a race: a nudge sent from a detached tx.result
 *  callback may not have been sent when the future the caller awaits completes, so a CLI
 *  command that exits at that moment loses it. Observing the race needs a genuinely
 *  concurrent clientContext, which the CLI has and TestNetwork deliberately does not --
 *  AsyncFunSuite's ExecutionContext is serial, so a detached callback always runs before the
 *  assertions below regardless of where the send lives. Verified by reverting the fix and
 *  re-running: both tests still pass. They cannot detect a return to the detached callback
 *  either. The ordering property is guarded by the comment at stageDeviceUpdate
 *  and by review, not by these tests.
 *
 *  What they do verify, which nothing did before: a nudge is sent at all, exactly one per
 *  distinct device, addressed to the right host with the right device id.
 */
class DeviceCheckNudgeSuite extends IntegrationTestSuite:

  private val systemId = UUID.fromString("33333333-3333-3333-3333-333333333333")
  private val bootstrapSet = StorageDeviceSetId.BootstrapStorageDeviceSetId

  /** Temp trees created by the current test, removed by teardown. */
  private val tempRoots = mutable.ListBuffer[os.Path]()

  override def subFixtureTeardown(): Unit =
    tempRoots.foreach: p =>
      try os.remove.all(p)
      catch case _: Throwable => ()
    tempRoots.clear()

  /** A HostConfig for the TestNetwork's bootstrap host. Only hostId and aspenSystemId are
   *  consulted by createStorageDevice; the rest is filler. */
  private def hostConfig() = HostConfig(
    HostId.BootstrapHostId,
    systemId,
    "testhost",
    "localhost",
    1234,
    1235,
    1236,
    HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300))

  /** Creates `<tmp>/host/storage-devices` and returns the host directory. */
  private def newHostDir(): Path =
    val base = Files.createTempDirectory("aspen-nudge-test")
    tempRoots += os.Path(base)
    val hostDir = base.resolve("host")
    Files.createDirectories(hostDir.resolve(StorageDeviceManager.StorageDevicesDirName))
    hostDir

  private def takeNudges(): List[CheckStorageDevice] =
    net.takeCapturedHostMessages().collect:
      case m: CheckStorageDevice => m

  atest("createNewStoragePool sends one device-check nudge per device"):
    given ExecutionContext = executionContext
    // Discard anything bootstrap left behind so the assertions see only this pool's nudges.
    takeNudges()
    for
      _ <- client.createNewStoragePool(
                  "nudge-pool",
                  Replication(1, 1),
                  None,
                  RocksDBConfig(),
                  bootstrapSet,
                  0L)
      // Bound with no intervening await, so the capture reflects what has been sent at the
      // moment the future completes.
      nudges =  takeNudges()
      devState <- client.getStorageDeviceState(StorageDeviceId.BootstrapStorageDeviceId)
    yield
      nudges.map(_.deviceId) should be(List(StorageDeviceId.BootstrapStorageDeviceId))
      nudges.map(_.toHost) should be(List(devState.hostId))
      nudges.map(_.fromClient) should be(List(client.clientId))

  atest("transferStore sends a device-check nudge to the destination device's host"):
    given ExecutionContext = executionContext
    // TestNetwork ships with a single storage device, so the destination has to be made.
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)
    for
      destId <- StorageDeviceManager.createStorageDevice(
                  client, hostConfig(), hostDir, dir, bootstrapSet, systemId)
      _ <- net.waitForTransactionsToComplete()
      // Discard anything device creation sent, so the assertions see only the transfer's nudge.
      _ =  takeNudges()
      _ <- client.transferStore(storeId, destId)
      nudges =  takeNudges()
      destState <- client.getStorageDeviceState(destId)
    yield
      nudges.map(_.deviceId) should be(List(destId))
      nudges.map(_.toHost) should be(List(destState.hostId))
      nudges.map(_.fromClient) should be(List(client.clientId))
```

- [ ] **Step 2: Run the new suite to confirm it compiles and runs**

Run: `sbt 'testOnly org.aspen_ddp.aspen.client.DeviceCheckNudgeSuite'`

Expected: both tests PASS. As explained above they pass before the fix too; this step establishes the suite is wired up correctly, not that the bug is present. If either test FAILS here, stop — the harness assumptions are wrong and the rest of the plan rests on them.

- [ ] **Step 3: Commit the tests**

```bash
git add src/test/scala/org/aspen_ddp/aspen/client/DeviceCheckNudgeSuite.scala
git commit -F - <<'MSGEOF'
Add a regression guard for CheckStorageDevice nudge ordering

Asserts both createNewStoragePool and transferStore have sent their
nudges by the time their futures complete. The nudges currently go out
from a detached tx.result callback, which is unordered against the
future the caller awaits, so a CLI command exiting at that moment can
lose them.

The suite cannot fail on that race: AsyncFunSuite's ExecutionContext is
serial, so the detached callback is always queued ahead of the
assertions. It guards against the sends drifting back out of the
awaited chain once they are moved into it.
MSGEOF
```

- [ ] **Step 4: Add the best-effort send helper to `AspenClient`**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, add the scribe import to the import block:

```scala
import org.aspen_ddp.aspen.server.store.backend.BackendConfig
import scribe.Logging
```

Change the trait declaration (line 29) from:

```scala
trait AspenClient extends ObjectReader:
```

to:

```scala
trait AspenClient extends ObjectReader with Logging:
```

Then, directly below `private[aspen] def sendHostMessage(msg: HostMessage): Unit` (line 303), add:

```scala
  /** Sends a message that is purely a latency optimization: the receiving host reaches the
   *  same state on its own through its periodic polling, just later. A failure to send one
   *  must therefore never fail the operation that produced it.
   */
  private[aspen] def sendBestEffortHostMessage(msg: HostMessage): Unit =
    try sendHostMessage(msg)
    catch
      case t: Throwable => logger.warn(s"Failed to send best-effort host message $msg: $t")
```

- [ ] **Step 5: Restructure `createStoragePool`**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`, replace the whole of `createStoragePool` (from `override protected def createStoragePool` at line 285 through the closing `config.poolId` at line 375) with:

```scala
  override protected def createStoragePool(config: StoragePoolState): Future[PoolId] =
    // Pool creation has no special recovery handling, so onFail is a no-op. For the production
    // client this is equivalent to the previous transactUntilSuccessful (retry, no recovery);
    // for the test client runCreate performs a single attempt.
    val fStaged = runCreate(_ => Future.unit): tx =>
      given Transaction = tx

      def createPoolObj(alloc: ObjectAllocator): Future[KeyValueObjectPointer] =
        for
          errTreeRoot <- alloc.allocateKeyValueObject(Map())
          allocTreeRoot <- alloc.allocateKeyValueObject(Map())

          nodeAllocator = SinglePoolNodeAllocator(this, radicle.poolId)

          poolConfig = config.encode()
          errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), nodeAllocator).encode()
          allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), nodeAllocator).encode()

          poolPtr <- alloc.allocateKeyValueObject(Map(
            StoragePoolState.ConfigKey -> Value(poolConfig),
            StoragePoolState.ErrorTreeKey -> Value(errorTree),
            StoragePoolState.AllocationTreeKey -> Value(allocTree)
          ))
        yield
          poolPtr

      case class DeviceUpdate(storageDeviceId: StorageDeviceId,
                              pointer: KeyValueObjectPointer,
                              kvos: KeyValueObjectState,
                              stores: List[StoreId],
                              state: StorageDeviceState)

      def collectDevices(stores: Array[StoragePoolState.StoreEntry]): List[Future[DeviceUpdate]] =
        val devMap = stores.zipWithIndex.foldLeft(Map[StorageDeviceId, List[StoreId]]()): (m, tpl) =>
          val (entry, poolIndex) = tpl
          val storeId = StoreId(config.poolId, poolIndex.toByte)
          val l = m.get(entry.storageDeviceId) match
            case None => storeId :: Nil
            case Some(lst) => storeId :: lst
          m + (entry.storageDeviceId -> l)

        devMap.map { (storageDeviceId, stores) =>
          for
            devPtr <- client.getStorageDevicePointer(storageDeviceId)
            devKvos <- client.read(devPtr)
          yield
            DeviceUpdate(storageDeviceId, devPtr, devKvos, stores, StorageDeviceState(devKvos))
        }.toList

      def stageDeviceUpdate(du: DeviceUpdate): CheckStorageDevice =
        val updates = du.stores.map { storeId =>
          storeId -> StorageDeviceState.StoreEntry(
            StorageDeviceState.StoreStatus.Initializing,
            None
          )
        }.toMap

        val newState = du.state.copy(stores = du.state.stores ++ updates)

        val reqs = List(KeyRevision(StorageDeviceState.StateKey, du.kvos.contents(StorageDeviceState.StateKey).revision))
        val ops = List(Insert(StorageDeviceState.StateKey, newState.encode()))

        tx.update(du.pointer, None, None, reqs, ops)

        // Built here, sent by the map below once the transaction has committed. Registering
        // this on tx.result instead would send it from a task the returned future does not
        // wait on: tx.commit() hands back the same promise as tx.result, so a caller that
        // exits the moment this future completes could lose the message.
        CheckStorageDevice(
          du.state.hostId,
          clientId,
          du.storageDeviceId
        )

      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        poolPtr <- createPoolObj(bsPool.allocator)
        _ <- storagePoolsTree.preparePut(config.poolId.uuid, poolPtr)
        _ <- namespacedRegistry.prepareRegisterObject(Namespaces.Pool, config.name, config.poolId.uuid)
        devUpdates <- Future.sequence(collectDevices(config.stores))
        setPtr <- getStorageDeviceSetPointer(config.storageDeviceSet)
        setDos <- read(setPtr)
      yield
        val nudges = devUpdates.map(stageDeviceUpdate)

        // Record the pool in the device set's assignedPools (reverse of the pool's
        // storageDeviceSet reference). Idempotent so transaction retries are safe.
        val setState = StorageDeviceSetState(setDos)
        if !setState.assignedPools.contains(config.poolId) then
          val updatedSet = setState.copy(assignedPools = config.poolId :: setState.assignedPools)
          tx.overwrite(setPtr, setDos.revision, DataBuffer(updatedSet.toBytes))

        // Each retry attempt builds its own list and only the successful attempt's value
        // reaches the map below, so a retried transaction cannot double-send.
        nudges

    fStaged.map: nudges =>
      nudges.foreach(sendBestEffortHostMessage)
      config.poolId
```

- [ ] **Step 6: Run the suite**

Run: `sbt 'testOnly org.aspen_ddp.aspen.client.DeviceCheckNudgeSuite'`

Expected: the `createNewStoragePool` test PASSES. The `transferStore` test also passes — it is unaffected until Task 2.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala
git commit -F - <<'MSGEOF'
Send createStoragePool's nudges from inside the awaited chain

tx.commit() returns the same promise as tx.result, so a nudge sent
from tx.result.foreach goes out on a task the returned future does not
wait on. A caller that exits as soon as that future completes -- the
create-pool CLI command does -- can lose the message, costing the
affected host up to a full check period.

updateDevice now returns the CheckStorageDevice it used to send, and a
map on the transaction future sends the collected list. The map's
result is the future the caller awaits, so it cannot complete before
the sends have run.

sendBestEffortHostMessage keeps the send from newly coupling a
latency-only message to the command's result.
MSGEOF
```

---

### Task 2: `transferStore` sends its nudge inside the awaited chain

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:214-277` (`transferStore`)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/DeviceCheckNudgeSuite.scala` (already written in Task 1)

- [ ] **Step 1: Restructure `transferStore`**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, replace the whole of `transferStore` (from `def transferStore` at line 214 through the `sendHostMessage(msg)` at line 277) with:

```scala
  def transferStore(storeId: StoreId, destinationId: StorageDeviceId): Future[Unit] =
    given ExecutionContext = this.clientContext

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: InvalidDestination => throw StopRetrying(e)
      case e: StoreNotActive => throw StopRetrying(e)

    val fStaged = transactUntilSuccessfulWithRecovery(onFail): tx =>
      given Transaction = tx

      for
        pool <- getStoragePool(storeId.poolId)
        poolState <- pool.getState()
        sourceId = poolState.stores(storeId.poolIndex).storageDeviceId
        srcPtr <- getStorageDevicePointer(sourceId)
        srcKvos <- read(srcPtr)
        srcState = StorageDeviceState(srcKvos)
        dstPtr <- getStorageDevicePointer(destinationId)
        dstKvos <- read(dstPtr)
        dstState = StorageDeviceState(dstKvos)
      yield
        if sourceId == destinationId then
          throw InvalidDestination()

        srcState.stores.get(storeId) match
          case None => throw StoreNotActive(storeId)
          case Some(entry) =>
            if entry.status != StorageDeviceState.StoreStatus.Active then
              throw StoreNotActive(storeId)

            // Update Source Device
            val newSrcEntry = StorageDeviceState.StoreEntry(
              StorageDeviceState.StoreStatus.TransferringOut,
              Some(destinationId)
            )
            val newSrcStores = srcState.stores + (storeId -> newSrcEntry)
            val newSrcState = srcState.copy(stores = newSrcStores)

            val srcReqs = List(KeyRevision(StorageDeviceState.StateKey, srcKvos.contents(StorageDeviceState.StateKey).revision))
            val srcOps = List(Insert(StorageDeviceState.StateKey, newSrcState.encode()))

            tx.update(srcPtr, None, None, srcReqs, srcOps)

            // Update Destination Device
            val newDstEntry = StorageDeviceState.StoreEntry(
              StorageDeviceState.StoreStatus.TransferringIn,
              Some(sourceId)
            )
            val newDstStores = dstState.stores + (storeId -> newDstEntry)
            val newDstState = dstState.copy(stores = newDstStores)

            val dstReqs = List(KeyRevision(StorageDeviceState.StateKey, dstKvos.contents(StorageDeviceState.StateKey).revision))
            val dstOps = List(Insert(StorageDeviceState.StateKey, newDstState.encode()))

            tx.update(dstPtr, None, None, dstReqs, dstOps)

            // Built here, sent by the map below once the transaction has committed.
            // Registering this on tx.result instead would send it from a task the returned
            // future does not wait on: tx.commit() hands back the same promise as tx.result,
            // so a caller that exits the moment this future completes could lose the message.
            CheckStorageDevice(
              dstState.hostId,
              clientId,
              destinationId
            )

    fStaged.map(sendBestEffortHostMessage)
```

Note the yield block's type: the `None` branch throws, so the match — and therefore the block — is a `CheckStorageDevice`, and `fStaged` is a `Future[CheckStorageDevice]`. `fStaged.map(sendBestEffortHostMessage)` gives back the `Future[Unit]` the signature promises.

- [ ] **Step 2: Run the suite**

Run: `sbt 'testOnly org.aspen_ddp.aspen.client.DeviceCheckNudgeSuite'`

Expected: both tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala
git commit -F - <<'MSGEOF'
Send transferStore's nudge from inside the awaited chain

Same change as createStoragePool: the prepare block yields the
CheckStorageDevice rather than registering a tx.result callback, and a
map on the transaction future sends it. The transfer-store CLI command
awaits that future and then exits, so a send that is not ordered
before its completion can be lost.
MSGEOF
```

---

### Task 3: Extract the shared `MetadataManager` test fixture

**Files:**
- Create: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerTestFixture.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPeekHostEntrySuite.scala:1-117`

This is a pure refactor: green before, green after. Task 4 needs the same recording client and temp bootstrap config that `MetadataManagerPeekHostEntrySuite` currently keeps privately, plus the ability to hold a *pool* lookup open, so the fixture moves out into a file both suites share.

- [ ] **Step 1: Create the shared fixture**

Create `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerTestFixture.scala`:

```scala
package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.TestNetwork
import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, HostState, StoragePoolState}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.EvictingQueue
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/** An AspenClient whose lookups never resolve on their own: it records what it was asked
 *  about and hands back a Promise the test completes when it chooses.
 *
 *  The recording is what makes "this call started no lookup" observable without reaching into
 *  MetadataManager's private state -- getHostState and getStoragePoolState are the only things
 *  startHostLookup and startPoolLookup do that a caller can see.
 *
 *  ExecutionContext.parasitic runs the continuation inline rather than on a pool thread: on the
 *  thread that completes the Promise, or on the registering thread if the Promise is already
 *  complete. Either way the MetadataManager callback that installs (or, on failure, drops) the
 *  entry has finished by the time the test's next line runs, which removes all waiting.
 *
 *  Everything else is inherited from TestNetwork.TClient purely so this file does not have to
 *  stub the whole AspenClient surface. No read, transaction or message ever leaves it: the
 *  messenger discards sends and the radicle pointer is never dereferenced.
 */
class LookupRecordingClient extends TestNetwork.TClient(
    ExecutionContext.parasitic,
    ClientMessenger.None,
    Radicle.pointer,
    Replication(1, 1)):

  /** Every hostId getHostState was called with, in call order. */
  val lookups: mutable.ListBuffer[HostId] = mutable.ListBuffer[HostId]()

  /** Every poolId getStoragePoolState was called with, in call order. */
  val poolLookups: mutable.ListBuffer[PoolId] = mutable.ListBuffer[PoolId]()

  private var promises: Map[HostId, Promise[HostState]] = Map()
  private var poolPromises: Map[PoolId, Promise[StoragePoolState]] = Map()

  /** The Promise backing `hostId`'s lookup, created on first use. Callable before or after the
   *  lookup itself so a test can complete it either way round. */
  def lookupPromise(hostId: HostId): Promise[HostState] = synchronized:
    promises.get(hostId) match
      case Some(p) => p
      case None =>
        val p = Promise[HostState]()
        promises += hostId -> p
        p

  /** The Promise backing `poolId`'s lookup, created on first use. */
  def poolLookupPromise(poolId: PoolId): Promise[StoragePoolState] = synchronized:
    poolPromises.get(poolId) match
      case Some(p) => p
      case None =>
        val p = Promise[StoragePoolState]()
        poolPromises += poolId -> p
        p

  override def getHostState(hostId: HostId): Future[HostState] = synchronized:
    lookups += hostId
    lookupPromise(hostId).future

  override def getStoragePoolState(poolId: PoolId): Future[StoragePoolState] = synchronized:
    poolLookups += poolId
    poolLookupPromise(poolId).future


/** A MetadataManager over a temp bootstrap config naming exactly one host, plus the ids of a
 *  host and a pool that config does not name -- reaching either requires a lookup. */
trait MetadataManagerFixture extends BeforeAndAfterAll:
  this: Suite =>

  protected val systemId: UUID = UUID.fromString("55555555-5555-5555-5555-555555555555")
  protected val poolId: PoolId = PoolId(UUID.fromString("66666666-6666-6666-6666-666666666666"))

  /** Present in the bootstrap config, so MetadataManager's constructor resolves it with no
   *  lookup at all. */
  protected val bootstrapHostId: HostId = HostId(UUID.fromString("77777777-7777-7777-7777-777777777777"))

  /** Absent from the bootstrap config: reaching it requires a lookup. */
  protected val remoteHostId: HostId = HostId(UUID.fromString("88888888-8888-8888-8888-888888888888"))

  /** Absent from the bootstrap config: reaching a store in it requires a pool lookup. */
  protected val unknownPoolId: PoolId = PoolId(UUID.fromString("99999999-9999-9999-9999-999999999999"))

  protected val remoteHostState: HostState =
    HostState(remoteHostId, "remote_host", "10.0.0.9", 6000, 6001, 6002, Set())

  private var tempDir: Path = scala.compiletime.uninitialized
  private var bootstrapConfigFile: os.Path = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    tempDir = Files.createTempDirectory("aspen-metadata-manager")
    val f = tempDir.resolve("aspen-bootstrap-config.yaml")
    val yaml = BootstrapConfig.generateBootstrapConfig(
      systemId,
      Replication(1, 1),
      List(HostState(bootstrapHostId, "bootstrap_host", "127.0.0.1", 5000, 5001, 5002, Set())),
      List(StoreId(poolId, 0.toByte) -> bootstrapHostId))
    Files.write(f, yaml.getBytes(StandardCharsets.UTF_8))
    bootstrapConfigFile = os.Path(f)

  override def afterAll(): Unit =
    try os.remove.all(os.Path(tempDir))
    catch case _: Throwable => ()

  /** A MetadataManager over the fixture's bootstrap config, wired to a fresh recording client.
   *  The NetworkImplInterface is the smallest thing that satisfies the type: it builds a plain
   *  HostEntry and ignores store resolution, neither of which the suites here touch. */
  protected def newManager(): (MetadataManager[MetadataManager.HostEntry], LookupRecordingClient) =
    val impl = new MetadataManager.NetworkImplInterface[MetadataManager.HostEntry]:
      def createHostEntry(hostId: HostId,
                          name: String,
                          address: String,
                          dataPort: Int,
                          cncPort: Int,
                          storeTransferPort: Int,
                          queuedMessages: EvictingQueue[Message]): MetadataManager.HostEntry =
        new MetadataManager.HostEntry(hostId, name, address, dataPort, cncPort, storeTransferPort)

      def storeResolved(hostEntry: MetadataManager.HostEntry,
                        storeId: StoreId,
                        queuedMessages: EvictingQueue[Message]): Unit = ()

    val client = new LookupRecordingClient
    val mgr = new MetadataManager[MetadataManager.HostEntry](bootstrapConfigFile, impl)
    mgr.setAspenClient(client)
    (mgr, client)
```

- [ ] **Step 2: Trim the existing suite down to its tests**

Replace lines 1 through 117 of `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPeekHostEntrySuite.scala` — everything from `package` through the closing `(mgr, client)` of the old private `newManager` — with:

```scala
package org.aspen_ddp.aspen.common.network

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MetadataManagerPeekHostEntrySuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:
```

Leave everything from line 119 (`  test("peekHostEntry returns a bootstrap host without starting a lookup"):`) to the end of the file untouched.

- [ ] **Step 3: Run the suite to confirm the refactor changed nothing**

Run: `sbt 'testOnly org.aspen_ddp.aspen.common.network.MetadataManagerPeekHostEntrySuite'`

Expected: all 5 tests PASS, same as before.

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerTestFixture.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerPeekHostEntrySuite.scala
git commit -F - <<'MSGEOF'
Extract the shared MetadataManager test fixture

The drain-accessor suite needs the same recording client and temp
bootstrap config, so move both out of MetadataManagerPeekHostEntrySuite
into a fixture trait. LookupRecordingClient also gains pool-lookup
recording, which the new suite needs to hold a pool lookup open.

No behaviour change; the existing tests are untouched.
MSGEOF
```

---

### Task 4: `MetadataManager` drain accessors

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala:110` (below `peekHostEntry`)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerDrainSuite.scala` (create)

**Background:** `create-pool` nudges hosts the CLI has typically never contacted, so `getHostEntryOrQueueMessage` parks the message behind a lookup and returns `None`. A drain that only inspects resolved host entries cannot see that message at all — it would report success on exactly the case this work exists to fix. These two accessors give the drain the missing view.

- [ ] **Step 1: Write the failing tests**

Create `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerDrainSuite.scala`:

```scala
package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.store.StoreId
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/** Coverage for the two read-only accessors ZMQNet.awaitPendingMessagesSent polls. */
class MetadataManagerDrainSuite extends AnyFunSuite
    with Matchers
    with MetadataManagerFixture:

  private def nudge(): Message =
    CheckStorageDevice(remoteHostId, ClientId.Null, StorageDeviceId(UUID.randomUUID()))

  test("resolvedHostEntries omits a host whose lookup is still pending"):
    val (mgr, client) = newManager()

    mgr.resolvedHostEntries.map(_.hostId) should be(List(bootstrapHostId))

    mgr.getHostEntry(remoteHostId) should be(None)
    // A lookup with no message parked on it must not hold the drain open.
    mgr.hasParkedMessages should be(false)
    mgr.resolvedHostEntries.map(_.hostId) should be(List(bootstrapHostId))

    client.lookupPromise(remoteHostId).success(remoteHostState)
    mgr.resolvedHostEntries.map(_.hostId).toSet should be(Set(bootstrapHostId, remoteHostId))

  test("hasParkedMessages sees a message held behind a pending host lookup"):
    val (mgr, client) = newManager()

    mgr.hasParkedMessages should be(false)

    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge()) should be(None)
    mgr.hasParkedMessages should be(true)

    // Resolving hands the queue to the newly created host entry, which is where the network
    // implementation picks it up. Nothing is parked in MetadataManager any more, and the
    // drain's remaining check -- the resolved entry's own pending queue -- takes over.
    client.lookupPromise(remoteHostId).success(remoteHostState)
    mgr.hasParkedMessages should be(false)

  test("hasParkedMessages sees a message held behind a pending pool lookup"):
    val (mgr, client) = newManager()

    val unknownStore = StoreId(unknownPoolId, 0.toByte)
    mgr.getHostEntryOrQueueMessage(unknownStore, nudge()) should be(None)
    client.poolLookups.toList should be(List(unknownPoolId))
    mgr.hasParkedMessages should be(true)

  test("a failed lookup drops the parked message rather than reporting it"):
    val (mgr, client) = newManager()

    mgr.getHostEntryOrQueueMessage(remoteHostId, nudge()) should be(None)
    mgr.hasParkedMessages should be(true)

    // The limit the drain's scaladoc has to state: a failure drops the entry and the messages
    // parked on it, so the predicate goes quiet because the message is gone, not because it
    // was sent. Nothing at this layer can distinguish the two.
    client.lookupPromise(remoteHostId).failure(new NoSuchElementException("no such host"))
    mgr.hasParkedMessages should be(false)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly org.aspen_ddp.aspen.common.network.MetadataManagerDrainSuite'`

Expected: compilation FAILS with `value resolvedHostEntries is not a member of ...MetadataManager` and `value hasParkedMessages is not a member of ...MetadataManager`.

- [ ] **Step 3: Add the accessors**

In `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`, insert immediately after `peekHostEntry`'s closing `case _ => None` (line 110) and before `def getHostEntry`:

```scala
  /** Every host entry that has resolved, in no particular order. Excludes hosts whose lookup
   *  is still pending and hosts never looked up at all.
   *
   *  Exists for ZMQNet.awaitPendingMessagesSent, which has to inspect every host rather than
   *  one named host: its callers cannot name the hosts a message was addressed to. */
  def resolvedHostEntries: List[T] =
    synchronized:
      hosts.values.collect { case Right(hostEntry) => hostEntry }.toList

  /** True if any message is parked behind an unresolved host or pool lookup.
   *
   *  A parked message is invisible to a caller inspecting host entries: the host it is
   *  addressed to has no entry yet, so there is nothing to look at. A drain that ignored this
   *  would report success while still holding the message -- which is the common case for a
   *  nudge sent to a host the process has not talked to before.
   *
   *  Note that a failed host or pool lookup drops the entry and everything parked on it (see
   *  peekHostEntry's scaladoc for the host case), so this can go false because the message was
   *  discarded rather than sent. Nothing at this layer can tell the two apart. */
  def hasParkedMessages: Boolean =
    synchronized:
      val parkedOnHost = hosts.values.exists:
        case Left(pendingHostLookup) => !pendingHostLookup.messageQueue.isEmpty
        case Right(_) => false

      val parkedOnPool = pendingPoolLookups.values.exists: ppl =>
        ppl.storeQueues.values.exists(q => !q.isEmpty)

      parkedOnHost || parkedOnPool
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `sbt 'testOnly org.aspen_ddp.aspen.common.network.MetadataManagerDrainSuite'`

Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerDrainSuite.scala
git commit -F - <<'MSGEOF'
Add MetadataManager accessors for the pending-message drain

A drain that inspects only resolved host entries cannot see a message
parked behind an unresolved host or pool lookup, which is the common
case for a nudge to a host the process has never contacted.

resolvedHostEntries exposes the resolved entries; hasParkedMessages
reports whether any lookup queue still holds something. Both are
read-only and take the existing lock.
MSGEOF
```

---

### Task 5: `awaitPendingMessagesSent` and the single exit-point drain

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala:186-234` (`awaitHostMessagesSent`, and one line of `shutdown`'s scaladoc)
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:69` (constants), `:633` (exit), `:644` (`createNetwork`), `:1450-1479` (`create_storage_device`)

**Background:** the exit wiring gets no automated coverage. `MainSuite` is a pure unit suite that does not drive commands end to end, and standing up a harness that does is out of proportion to a change of this size. The spec states this rather than papering over it. Verification here is `sbt compile` plus the full suite.

- [ ] **Step 1: Replace the drain in `ZMQNet`**

In `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala`, replace the scaladoc block and method ending at line 221 (the whole comment starting `/** ... Returns true if it drained.` through the final `drained` of `awaitHostMessagesSent`) with:

```scala
  /** Blocks until ZMQNet is holding no outbound messages, or `timeout` elapses, polling every
   *  25ms. Returns true if it drained.
   *
   *  Takes no host id because its callers cannot name the hosts involved: a command may nudge
   *  hosts it never looked up itself. The predicate therefore covers all three places a
   *  message can be waiting:
   *
   *    - the shared send queue, which holds messages for every host
   *    - MetadataManager's lookup queues, where a message sits while the host or pool it is
   *      addressed to is resolved -- the common case for a nudge to a host this process has
   *      not talked to before
   *    - every resolved host entry's own pending queue, each of which needs a dealer socket
   *      before anything can leave it
   *
   *  Known limits, none fixable at this layer:
   *
   *    - The IO thread removes an item from the send queue just before handing it to a socket
   *      or to MetadataManager, so a return of true can beat the final send, or briefly miss a
   *      message on its way into a lookup queue.
   *    - A failed host or pool lookup drops the entry and the messages parked on it (see
   *      MetadataManager.peekHostEntry). The predicate then goes quiet because the message is
   *      gone rather than sent.
   *
   *  This is not a delivery guarantee in any case. ZMQ buffers internally and the peer may be
   *  down. It establishes only that ZMQNet is no longer holding the message, which is exactly
   *  what a short-lived process would otherwise abandon on exit.
   *
   *  A false return is not a command failure. It means only that ZMQNet may still be holding
   *  something; the caller should report degraded latency rather than an error, since the
   *  receiving host's periodic polling remains the correctness guarantee.
   *
   *  A short-lived process should follow this with shutdown() before exiting: draining
   *  ZMQNet's own queues only hands the message to ZMQ, and shutdown() is what gives ZMQ a
   *  window to put it on the wire.
   */
  def awaitPendingMessagesSent(timeout: Duration): Boolean =
    val deadline = System.nanoTime() + timeout.toNanos
    val pollIntervalMillis = 25L

    def drained: Boolean =
      sendQueue.isEmpty &&
        !metadataManager.hasParkedMessages &&
        metadataManager.resolvedHostEntries.forall: entry =>
          entry.odealer.isDefined && entry.pendingMessages.isEmpty

    while !drained && System.nanoTime() - deadline < 0 do
      Thread.sleep(pollIntervalMillis)

    drained
```

Then in `shutdown`'s scaladoc just below, change:

```scala
   *  up for the life of the process. Call awaitHostMessagesSent() first: this only bounds how
```

to:

```scala
   *  up for the life of the process. Call awaitPendingMessagesSent() first: this only bounds how
```

Also update the stale reference in the comment at line 32:

```scala
    // Written by the zmq-io thread outside of any lock. Volatile because awaitHostMessagesSent
```

becomes:

```scala
    // Written by the zmq-io thread outside of any lock. Volatile because awaitPendingMessagesSent
```

- [ ] **Step 2: Hoist the drain constants in `Main`**

In `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`, directly below `val CheckStorageDevicesPeriod: Duration = Duration(1, HOURS)` (line 69), add:

```scala

  // How long a terminating command waits for outbound messages to leave ZMQNet, and how long
  // ZMQ is then given to put them on the wire before the process exits.
  val NotificationDrainTimeout: Duration = Duration(5, SECONDS)
  val NotificationSendLinger: Duration = Duration(1, SECONDS)

  // Set by createNetwork so main can drain outbound messages before System.exit. Plain rather
  // than volatile because it is thread-confined: every command builds its network by a direct
  // call on the main thread, and main reads it back on that same thread. A command that built
  // one from a callback would need this made volatile, or the drain would see None and skip.
  private var onetwork: Option[ZMQNet] = None
```

- [ ] **Step 3: Register the network on creation**

Replace `createNetwork` (line 644) with:

```scala
  def createNetwork(bootstrapConfigFile: os.Path,
                    ohost: Option[(HostId, Int)],
                    oclientId: Option[ClientId]): (NetworkBridge, ZMQNet) = {
    val b = new NetworkBridge

    val heartbeatPeriod = Duration(10, SECONDS)
    val net = new ZMQNet(bootstrapConfigFile, oclientId, ohost, heartbeatPeriod, b)
    onetwork = Some(net)
    (b, net)
  }
```

- [ ] **Step 4: Add `drainAndShutdown` and call it at the exit point**

Add the following method directly above `def main(args: Array[String]): Unit = {` (line 145):

```scala
  /** Gives outbound messages a chance to leave the process before it exits.
   *
   *  CheckStorageDevice nudges are why this exists: a command that sends one and then exits
   *  would otherwise abandon it, costing the receiving host up to CheckStorageDevicesPeriod.
   *  Commands that sent nothing pass through without waiting, and bootstrap -- which
   *  builds no network at all -- skips it entirely.
   *
   *  Never affects the exit code. A timeout means messages may still be held, which is a
   *  latency report rather than a command failure: the receiving host's periodic check remains
   *  the correctness guarantee.
   *
   *  host() blocks in joinIoThread and amoeba_server() in Thread.currentThread.join(); neither
   *  reaches here.
   */
  private def drainAndShutdown(): Unit =
    onetwork.foreach: net =>
      // Nothing here may change the exit code, and shutdown() deliberately races the IO
      // thread's use of the sockets it closes, so swallow whatever comes back: the command's
      // result is already decided and the process is on its way out.
      try
        if !net.awaitPendingMessagesSent(NotificationDrainTimeout) then
          println("Could not confirm all notifications left this process. Affected hosts will " +
                  s"act on their next periodic check, within $CheckStorageDevicesPeriod.")
        net.shutdown(NotificationSendLinger)
      catch
        case _: Throwable => ()
```

Then replace the exit at line 631-633:

```scala
    // All of Aspen's threads are daemon threads, so the process would exit here anyway.
    // The explicit exit is what carries the status code out to the shell.
    System.exit(exitCode)
```

with:

```scala
    // All of Aspen's threads are daemon threads, so the process would exit here anyway.
    // The explicit exit is what carries the status code out to the shell. The drain first is
    // what keeps a nudge sent moments ago from dying with the process.
    drainAndShutdown()
    System.exit(exitCode)
```

- [ ] **Step 5: Trim `create_storage_device`'s bespoke drain**

Replace lines 1450-1479 of `Main.scala` — from the comment `// How long the user waits for the notification below` through `network.shutdown(notificationSendLinger)` and the `case Failure(err) => reportError(err)` that follows — with:

```scala
      awaitAndReport(f):
        case Success(deviceId) =>
          println(s"Created storage device ${deviceId.uuid} at $deviceDirectory")
          // Best-effort nudge so the host loads the device now rather than on its next periodic
          // storage-device check. Losing it costs at most one check period and is never a
          // requirement, so nothing here may fail the command. main's drainAndShutdown is what
          // gives the message its chance to leave the process.
          client.sendHostMessage(CheckStorageDevice(hostCfg.hostId, client.clientId, deviceId))
          // A dealer socket accepts a send whether or not the peer is up, so a registered but
          // unreachable host reaches here too. Promise nothing beyond the handoff and name
          // both fallbacks.
          println(s"A running host should load the device shortly, or within " +
                  s"$CheckStorageDevicesPeriod if the notification is lost. A host that is " +
                  "down loads the device when it next starts.")
        case Failure(err) => reportError(err)
```

- [ ] **Step 6: Compile**

Run: `sbt compile`

Expected: success, with no remaining reference to `awaitHostMessagesSent`. Confirm with:

Run: `grep -rn "awaitHostMessagesSent" src/`
Expected: no output. (Matches under `docs/` are historical plan and spec documents for the previous piece of work and are left alone.)

- [ ] **Step 7: Run the full suite**

Run: `sbt test`

Expected: all suites PASS.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala \
        src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -F - <<'MSGEOF'
Drain pending messages once, at the CLI's single exit point

awaitHostMessagesSent took a hostId, which create-pool cannot supply:
it nudges one host per device and those ids never reach the CLI layer.
Replace it with awaitPendingMessagesSent, whose predicate covers the
send queue, MetadataManager's lookup queues, and every resolved host
entry.

main now drains and shuts the network down before System.exit, so no
command has to remember to. create-storage-device loses its own drain
and keeps only the guidance text, which never depended on the outcome.
The drain never changes the exit code: a timeout is a latency report,
not a command failure.
MSGEOF
```

---

### Task 6: Remove the completed TODO item

**Files:**
- Modify: `TODO.txt:1-24`

- [ ] **Step 1: Delete the item**

Remove lines 1 through 25 of `TODO.txt` — the entire block from `Two other CheckStorageDevice nudges can be lost to process exit` through the `and that it's safe to exit?` line and the blank line after it — so the file now begins with:

```
StoreManager.activeDeviceChecks is keyed by device id only, and load state can
now change while a check is in flight
```

- [ ] **Step 2: Verify the file still parses as expected**

Run: `head -5 TODO.txt`

Expected:

```
StoreManager.activeDeviceChecks is keyed by device id only, and load state can
now change while a check is in flight
  - A CheckStorageDevice for a device not yet on disk takes checkStorageDevice's
    None branch and leaves a lookup in flight. If the config then appears and a
    later event loads the device before that lookup completes, the later event's
```

- [ ] **Step 3: Commit**

```bash
git add TODO.txt
git commit -F - <<'MSGEOF'
Remove the completed CLI message-drain TODO item

Both nudges now go out inside the future the CLI awaits, and main
drains the network before System.exit. The item's flush-sentinel idea
was considered and rejected in the design doc.
MSGEOF
```

---

## Verification

After Task 6, the whole change is in. Confirm:

- [ ] `sbt test` — all suites pass
- [ ] `grep -rn "tx.result.foreach" src/main/` — no CheckStorageDevice send remains behind a detached callback

Manual verification of the CLI paths is possible with the commands in `TODO.txt`'s testing-notes section (`./t bootstrap`, `./t host`, `./t new-pool`, `./t transfer-store`), watching the host log for a `CheckStorageDevice` arriving promptly rather than an hour later. Not required by this plan.
