# AllocationGroupCache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `AllocationGroupCache` class that caches `AllocationGroupState` instances and refreshes stale entries in the background.

**Architecture:** A single class in `aspen.client.internal` with three mutable maps protected by `synchronized(this)`. One public method `fetchState` returns cached state immediately when available or fetches it on cache miss. Stale entries are refreshed via fire-and-forget background reads.

**Tech Stack:** Scala 3, ScalaTest (AsyncFunSuite), scala.concurrent.Future

---

### Task 1: Create AllocationGroupCache with fetchState for cache miss (no cached pointer)

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCache.scala`
- Create: `src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala`

- [ ] **Step 1: Write the test file with a mock client and the first test**

The test suite needs a minimal `AspenClient` mock that lets us control what `getAllocationGroupPointer` and `getAllocationGroupState` return. We test that calling `fetchState` on an empty cache fetches the state and caches it.

```scala
package org.aspen_ddp.aspen.client.internal

import java.util.UUID
import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, KeyValueObjectState, ObjectCache, RetryStrategy, Transaction, TransactionStatusCache, TypeRegistry}
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.{AllocationGroupState, HostId, StorageDeviceId, StoragePoolState}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientResponse, HostMessage}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.cnc.{CnCFrontend, NewStore}
import org.aspen_ddp.aspen.server.store.backend.BackendConfig
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*

object AllocationGroupCacheSuite:

  val groupId1 = AllocationGroupId(new UUID(0, 1))
  val groupId2 = AllocationGroupId(new UUID(0, 2))
  val poolId = PoolId(new UUID(0, 10))
  val ptr1 = DataObjectPointer(ObjectId(new UUID(0, 100)), poolId)
  val ptr2 = DataObjectPointer(ObjectId(new UUID(0, 101)), poolId)

  val state1 = AllocationGroupState(groupId1, level = 1, name = "group1", members = Nil, parentGroups = Nil)
  val state2 = AllocationGroupState(groupId2, level = 1, name = "group2", members = Nil, parentGroups = Nil)

  class MockClient extends AspenClient:
    val clientId: ClientId = ClientId(new UUID(0, 99))
    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache
    val typeRegistry: TypeRegistry = null
    val retryStrategy: RetryStrategy = null
    val radicle: KeyValueObjectPointer = null

    def clientContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
    def backgroundTaskManager: BackgroundTaskManager = BackgroundTaskManager.NoBackgroundTaskManager
    private[client] def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None
    private[client] val messenger: Messenger = Messenger.None
    private[client] val objectCache: ObjectCache = ObjectCache.NoCache

    def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] = Future.failed(new Exception("not implemented"))
    def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] = Future.failed(new Exception("not implemented"))
    def newTransaction(): Transaction = null
    protected def createStoragePool(config: StoragePoolState): Future[PoolId] = ???
    def getStoragePoolId(poolName: String): Future[PoolId] = ???
    def getHostId(hostName: String): Future[HostId] = ???
    def getAllocationGroupId(groupName: String): Future[AllocationGroupId] = ???
    def getStoragePoolPointer(poolId: PoolId): Future[KeyValueObjectPointer] = ???
    def getHostPointer(hostId: HostId): Future[KeyValueObjectPointer] = ???
    def getStorageDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer] = ???
    def createAllocationGroup(groupName: String, level: Int): Future[AllocationGroupId] = ???
    private[aspen] def receiveClientResponse(msg: ClientResponse): Unit = ()
    override def sendHostMessage(msg: HostMessage): Unit = ()
    private[aspen] def getSystemAttribute(key: String): Option[String] = None
    private[aspen] def setSystemAttribute(key: String, value: String): Unit = ()

    var getAllocationGroupPointerFn: AllocationGroupId => Future[DataObjectPointer] = _ => Future.failed(new Exception("not set"))
    var getAllocationGroupStateFn: AllocationGroupId => Future[AllocationGroupState] = _ => Future.failed(new Exception("not set"))

    override def getAllocationGroupPointer(allocationGroupId: AllocationGroupId): Future[DataObjectPointer] =
      getAllocationGroupPointerFn(allocationGroupId)

    override def getAllocationGroupState(allocationGroupId: AllocationGroupId): Future[AllocationGroupState] =
      getAllocationGroupStateFn(allocationGroupId)

class AllocationGroupCacheSuite extends AsyncFunSuite with Matchers:
  import AllocationGroupCacheSuite.*

  test("fetchState with empty cache fetches state and caches it"):
    val mock = new MockClient
    mock.getAllocationGroupPointerFn = _ => Future.successful(ptr1)
    mock.getAllocationGroupStateFn = _ => Future.successful(state1)

    val cache = AllocationGroupCache(mock)

    for
      result <- cache.fetchState(groupId1)
    yield
      result shouldBe state1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *AllocationGroupCacheSuite'`
Expected: Compilation failure — `AllocationGroupCache` does not exist yet.

- [ ] **Step 3: Write the AllocationGroupCache class with case 3 (no pointer, no state)**

```scala
package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.AllocationGroupState
import org.aspen_ddp.aspen.common.objects.DataObjectPointer

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}

class AllocationGroupCache(client: AspenClient, refreshDuration: Duration = Duration(20, SECONDS)):

  given ExecutionContext = client.clientContext

  private var pointers: Map[AllocationGroupId, DataObjectPointer] = Map.empty
  private var refreshing: Set[AllocationGroupId] = Set.empty
  private var states: Map[AllocationGroupId, (Long, AllocationGroupState)] = Map.empty

  def fetchState(groupId: AllocationGroupId): Future[AllocationGroupState] =
    synchronized:
      states.get(groupId)
    match
      case Some((_, state)) =>
        Future.successful(state)

      case None =>
        synchronized:
          pointers.get(groupId)
        match
          case Some(pointer) =>
            client.read(pointer).map: dos =>
              val state = AllocationGroupState(dos)
              synchronized:
                states = states + (groupId -> (System.currentTimeMillis(), state))
              state

          case None =>
            client.getAllocationGroupPointer(groupId).foreach: pointer =>
              synchronized:
                pointers = pointers + (groupId -> pointer)
            client.getAllocationGroupState(groupId).map: state =>
              synchronized:
                states = states + (groupId -> (System.currentTimeMillis(), state))
              state
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *AllocationGroupCacheSuite'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCache.scala src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala
git commit -m "Add AllocationGroupCache with initial fetch support"
```

---

### Task 2: Test cached state is returned on second call

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala`

- [ ] **Step 1: Add test verifying cached state is returned and pointer is only fetched once**

```scala
  test("fetchState returns cached state on second call"):
    val mock = new MockClient
    var ptrCallCount = 0
    mock.getAllocationGroupPointerFn = id =>
      ptrCallCount += 1
      Future.successful(ptr1)
    mock.getAllocationGroupStateFn = _ => Future.successful(state1)

    val cache = AllocationGroupCache(mock)

    for
      first <- cache.fetchState(groupId1)
      second <- cache.fetchState(groupId1)
    yield
      first shouldBe state1
      second shouldBe state1
      ptrCallCount shouldBe 1
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `sbt 'testOnly *AllocationGroupCacheSuite'`
Expected: PASS — the cached state path returns the state immediately, and the pointer was only fetched once.

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala
git commit -m "Add test verifying cached state is returned on repeat fetch"
```

---

### Task 3: Test and implement stale state refresh (case 1 with background refresh)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCache.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala`

The refresh uses `client.getAllocationGroupState(groupId)` rather than `client.read(pointer)` directly. This avoids needing to construct a valid `DataObjectState` in tests and is negligible overhead since `getAllocationGroupState` just does a pointer lookup (which we've cached) plus the same read+decode.

- [ ] **Step 1: Add test for stale state triggering a background refresh**

```scala
  test("fetchState triggers background refresh for stale state"):
    val mock = new MockClient
    val updatedState = state1.copy(name = "updated-group1")
    val refreshPromise = Promise[AllocationGroupState]()
    var fetchCount = 0

    mock.getAllocationGroupPointerFn = _ => Future.successful(ptr1)
    mock.getAllocationGroupStateFn = _ =>
      fetchCount += 1
      if fetchCount == 1 then Future.successful(state1)
      else refreshPromise.future

    val cache = AllocationGroupCache(mock, Duration(1, MILLISECONDS))

    for
      first <- cache.fetchState(groupId1)
      _ = Thread.sleep(10)
      stale <- cache.fetchState(groupId1)
      _ = refreshPromise.success(updatedState)
      _ = Thread.sleep(50)
      refreshed <- cache.fetchState(groupId1)
    yield
      first shouldBe state1
      stale shouldBe state1
      refreshed shouldBe updatedState
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *AllocationGroupCacheSuite'`
Expected: FAIL — the current implementation doesn't have refresh logic yet (case 1 just returns the cached state without checking staleness).

- [ ] **Step 3: Update AllocationGroupCache with the complete implementation including refresh**

Replace the full contents of `AllocationGroupCache.scala`:

```scala
package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.AllocationGroupState
import org.aspen_ddp.aspen.common.objects.DataObjectPointer

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}

class AllocationGroupCache(client: AspenClient, refreshDuration: Duration = Duration(20, SECONDS)):

  given ExecutionContext = client.clientContext

  private var pointers: Map[AllocationGroupId, DataObjectPointer] = Map.empty
  private var refreshing: Set[AllocationGroupId] = Set.empty
  private var states: Map[AllocationGroupId, (Long, AllocationGroupState)] = Map.empty

  def fetchState(groupId: AllocationGroupId): Future[AllocationGroupState] =
    synchronized:
      states.get(groupId)
    match
      case Some((timestamp, state)) =>
        val now = System.currentTimeMillis()
        if now - timestamp > refreshDuration.toMillis then
          val shouldRefresh = synchronized:
            if refreshing.contains(groupId) then
              false
            else
              refreshing = refreshing + groupId
              true
          if shouldRefresh then
            client.getAllocationGroupState(groupId).map: newState =>
              synchronized:
                states = states + (groupId -> (System.currentTimeMillis(), newState))
                refreshing = refreshing - groupId
            .recover:
              case _ =>
                synchronized:
                  refreshing = refreshing - groupId
        Future.successful(state)

      case None =>
        synchronized:
          pointers.get(groupId)
        match
          case Some(pointer) =>
            client.read(pointer).map: dos =>
              val state = AllocationGroupState(dos)
              synchronized:
                states = states + (groupId -> (System.currentTimeMillis(), state))
              state

          case None =>
            client.getAllocationGroupPointer(groupId).foreach: pointer =>
              synchronized:
                pointers = pointers + (groupId -> pointer)
            client.getAllocationGroupState(groupId).map: state =>
              synchronized:
                states = states + (groupId -> (System.currentTimeMillis(), state))
              state
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *AllocationGroupCacheSuite'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCache.scala src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala
git commit -m "Add background refresh for stale AllocationGroupState entries"
```

---

### Task 4: Test that duplicate refreshes are prevented

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala`

- [ ] **Step 1: Add test for no duplicate concurrent refreshes**

```scala
  test("fetchState does not trigger duplicate concurrent refreshes"):
    val mock = new MockClient
    var stateCallCount = 0
    val refreshPromise = Promise[AllocationGroupState]()

    mock.getAllocationGroupPointerFn = _ => Future.successful(ptr1)
    mock.getAllocationGroupStateFn = _ =>
      stateCallCount += 1
      if stateCallCount == 1 then Future.successful(state1)
      else refreshPromise.future

    val cache = AllocationGroupCache(mock, Duration(1, MILLISECONDS))

    for
      _ <- cache.fetchState(groupId1)
      _ = Thread.sleep(10)
      stale1 <- cache.fetchState(groupId1)
      stale2 <- cache.fetchState(groupId1)
      _ = refreshPromise.success(state1.copy(name = "refreshed"))
      _ = Thread.sleep(50)
    yield
      stale1 shouldBe state1
      stale2 shouldBe state1
      stateCallCount shouldBe 2
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `sbt 'testOnly *AllocationGroupCacheSuite'`
Expected: PASS — the `refreshing` set prevents the second `fetchState` from triggering another refresh.

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala
git commit -m "Add test verifying no duplicate concurrent refreshes"
```

---

### Task 5: Test refresh failure preserves stale state

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala`

- [ ] **Step 1: Add test for refresh failure keeping stale state**

```scala
  test("failed refresh preserves stale state and allows future refreshes"):
    val mock = new MockClient
    var stateCallCount = 0

    mock.getAllocationGroupPointerFn = _ => Future.successful(ptr1)
    mock.getAllocationGroupStateFn = _ =>
      stateCallCount += 1
      if stateCallCount == 1 then Future.successful(state1)
      else if stateCallCount == 2 then Future.failed(new Exception("network error"))
      else Future.successful(state1.copy(name = "recovered"))

    val cache = AllocationGroupCache(mock, Duration(1, MILLISECONDS))

    for
      first <- cache.fetchState(groupId1)
      _ = Thread.sleep(10)
      _ <- cache.fetchState(groupId1)
      _ = Thread.sleep(50)
      stillCached <- cache.fetchState(groupId1)
      _ = Thread.sleep(50)
      recovered <- cache.fetchState(groupId1)
    yield
      first shouldBe state1
      stillCached shouldBe state1
      recovered shouldBe state1.copy(name = "recovered")
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `sbt 'testOnly *AllocationGroupCacheSuite'`
Expected: PASS — the `.recover` block removes the groupId from `refreshing`, allowing a subsequent refresh attempt, and the stale state is still returned.

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala
git commit -m "Add test for refresh failure preserving stale state"
```

---

### Task 6: Test initial fetch failure propagates to caller

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala`

- [ ] **Step 1: Add test for initial fetch failure propagation**

```scala
  test("fetchState propagates failure when no cached state exists"):
    val mock = new MockClient
    mock.getAllocationGroupPointerFn = _ => Future.successful(ptr1)
    mock.getAllocationGroupStateFn = _ => Future.failed(new Exception("unavailable"))

    val cache = AllocationGroupCache(mock)

    recoverToSucceededIf[Exception]:
      cache.fetchState(groupId1)
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `sbt 'testOnly *AllocationGroupCacheSuite'`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/client/internal/AllocationGroupCacheSuite.scala
git commit -m "Add test for initial fetch failure propagation"
```

---

### Task 7: Run full test suite

**Files:** None (verification only)

- [ ] **Step 1: Run the full AllocationGroupCache test suite**

Run: `sbt 'testOnly *AllocationGroupCacheSuite'`
Expected: All tests pass.

- [ ] **Step 2: Run the full project test suite to check for regressions**

Run: `sbt test`
Expected: All existing tests still pass.

- [ ] **Step 3: Commit (if any final adjustments were needed)**

```bash
git add -A
git commit -m "Final adjustments for AllocationGroupCache"
```
