# BaseAspenClient De-duplication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the duplication between `SimpleAspenClient` and `TestNetwork.TClient` by extracting their shared implementation into a common `BaseAspenClient` abstract class, with the recovery-disabling differences expressed as a few extension points.

**Architecture:** A new abstract class `BaseAspenClient` (in `client.internal`, main scope) implements the ~80% of `AspenClient` that both clients share. Recovery dials that can be computed without `this` (caches, driver factories) are **constructor parameters**; those that need `this` or are genuine logic (`opportunisticRebuildManager`, the read-IDA strategy `resolveIda`, and the create-transaction wrapper `runCreate`) are **overridable members**. `SimpleAspenClient` fills them with production values; `TClient` fills them with disabled values.

**Tech Stack:** Scala 3, sbt, ScalaTest. Prefer Scala 3 quiet syntax and indented control flow per `CLAUDE.md`.

> **Note on TDD for this plan:** This is a pure structural refactor — no new runtime behavior. `TestNetwork`/`TClient` is the backbone of essentially the whole existing test suite, so **the existing suite is the regression harness**. Each task's verification is `sbt compile` (and, for Tasks 2–3, `sbt test`). No new tests are written.

---

## File Structure

- **Create** `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`
  — abstract base holding all shared plumbing + the three extension points.
- **Modify** `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala`
  — reduce to a `super(...)` call plus production overrides.
- **Modify** `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`
  — reduce `TClient` (currently lines 64–227) to a `super(...)` call plus disabled overrides.
- **Unchanged** `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`
  — the trait is not modified.

---

## Task 1: Create BaseAspenClient

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala`

This abstract class compiles on its own (nothing uses it yet). It implements every abstract
member of `AspenClient` except three: `opportunisticRebuildManager` (inherited abstract) plus
two new `protected` seams, `resolveIda` and `runCreate`.

- [ ] **Step 1: Create the file with full contents below**

```scala
package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.read.{ReadDriver, ReadManager}
import org.aspen_ddp.aspen.client.internal.transaction.{ClientTransactionDriver, TransactionImpl, TransactionManager}
import org.aspen_ddp.aspen.client.registries.Registry.DuplicateRegistration
import org.aspen_ddp.aspen.client.registries.{NamespacedUUIDRegistry, UUIDObjectRegistry}
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, Root, SinglePoolNodeAllocator, TieredKeyValueList}
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.metadata.*
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.compute.ServiceEntry

import java.util.UUID
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future}

/** Shared implementation for all AspenClient implementations.
  *
  * Everything that SimpleAspenClient and TestNetwork.TClient have in common lives here.
  * The differences between a full production client and a minimal test client are confined
  * to a small set of "dials":
  *   - constructor parameters that can be built without `this` (caches, driver factories)
  *   - the abstract members below (`opportunisticRebuildManager`, `resolveIda`, `runCreate`)
  */
abstract class BaseAspenClient(
    val executionContext: ExecutionContext,
    private[aspen] val radicle: KeyValueObjectPointer,
    override val clientId: ClientId,
    private[client] val messenger: ClientMessenger,
    override val txStatusCache: TransactionStatusCache,
    private[client] val objectCache: ObjectCache,
    readDriverFactory: ReadDriver.Factory,
    txDriverFactory: ClientTransactionDriver.Factory,
    userTypeFactories: List[RegisteredTypeFactory]) extends AspenClient:

  given ec: ExecutionContext = executionContext

  var attributes: Map[String, String] = Map()

  val typeRegistry: TypeRegistry = TypeRegistry(
    org.aspen_ddp.aspen.common.TypeFactories.factories,
    org.aspen_ddp.aspen.client.TypeFactories.factories,
    org.aspen_ddp.aspen.server.TypeFactories.factories,
    userTypeFactories
  )

  private val rmgr = new ReadManager(this, readDriverFactory)

  private val objectRegistry = new UUIDObjectRegistry(this, radicle, Radicle.ObjectRegistryKey)
  private val namespacedRegistry = new NamespacedUUIDRegistry(this, radicle, Radicle.NamespacedRegistryKey)

  private val storagePoolsTree = new MetadataTree(this, radicle, Radicle.StoragePoolsTreeKey)
  private val allocationGroupsTree = new MetadataTree(this, radicle, Radicle.AllocationGroupsTreeKey)
  private val hostsTree = new MetadataTree(this, radicle, Radicle.HostsTreeKey)
  private val storageDevicesTree = new MetadataTree(this, radicle, Radicle.StorageDevicesTreeKey)
  private val storageDeviceSetsTree = new MetadataTree(this, radicle, Radicle.StorageDeviceSetsTreeKey)

  // protected so subclass allocator-cache overrides can delegate to it
  protected val allocatorManager = new ObjectAllocatorManager(this)

  private val txManager = new TransactionManager(this, txDriverFactory)

  val retryStrategy: RetryStrategy = new ExponentialBackoffRetryStrategy(this)

  val backgroundTaskManager: BackgroundTaskManager = new BackgroundTaskManager(executionContext)

  def clientContext: ExecutionContext = executionContext

  // ---- Extension points (recovery dials) ----

  /** Resolves the IDA used to read the given object. Production resolves it from the pool;
    * the test client returns a fixed IDA to break the bootstrap-pool read cycle. */
  protected def resolveIda(pointer: ObjectPointer): Future[IDA]

  /** Runs a create-style transaction. Production retries with recovery; the test client runs
    * a single attempt so failures surface instead of being retried away. */
  protected def runCreate[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T]

  // `opportunisticRebuildManager` remains abstract (declared on the AspenClient trait) and is
  // overridden by each subclass.

  // ---- Reads ----

  override def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
    resolveIda(pointer).flatMap { ida =>
      rmgr.read(pointer, ida, comment).map(_.asInstanceOf[DataObjectState])
    }

  override def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
    resolveIda(pointer).flatMap { ida =>
      rmgr.read(pointer, ida, comment).map(_.asInstanceOf[KeyValueObjectState])
    }

  // ---- Transactions ----

  override def newTransaction(): Transaction =
    TransactionImpl(this, txManager, _ => 0, None)

  override def getAllocator(allocatorId: ObjectAllocatorId): Future[ObjectAllocator] =
    allocatorManager.getAllocator(allocatorId)

  // ---- Id lookups ----

  override def getStoragePoolId(poolName: String): Future[PoolId] =
    namespacedRegistry.getRegisteredObject("pool", poolName).map(PoolId(_))

  override def getHostId(hostName: String): Future[HostId] =
    namespacedRegistry.getRegisteredObject("host", hostName).map(HostId(_))

  override def getAllocationGroupId(groupName: String): Future[AllocationGroupId] =
    namespacedRegistry.getRegisteredObject("group", groupName).map(AllocationGroupId(_))

  override def getStorageDeviceSetId(setName: String): Future[StorageDeviceSetId] =
    namespacedRegistry.getRegisteredObject("device-set", setName).map(StorageDeviceSetId(_))

  // ---- Pointer lookups ----

  override def getStoragePoolPointer(poolId: PoolId): Future[KeyValueObjectPointer] =
    storagePoolsTree.get(poolId.uuid).map(_.asInstanceOf[KeyValueObjectPointer])

  override def getHostPointer(hostId: HostId): Future[KeyValueObjectPointer] =
    hostsTree.get(hostId.uuid).map(_.asInstanceOf[KeyValueObjectPointer])

  override def getStorageDevicePointer(storageDeviceId: StorageDeviceId): Future[KeyValueObjectPointer] =
    storageDevicesTree.get(storageDeviceId.uuid).map(_.asInstanceOf[KeyValueObjectPointer])

  override def getAllocationGroupPointer(allocationGroupId: AllocationGroupId): Future[DataObjectPointer] =
    allocationGroupsTree.get(allocationGroupId.uuid).map(_.asInstanceOf[DataObjectPointer])

  override def getStorageDeviceSetPointer(storageDeviceSetId: StorageDeviceSetId): Future[DataObjectPointer] =
    storageDeviceSetsTree.get(storageDeviceSetId.uuid).map(_.asInstanceOf[DataObjectPointer])

  // ---- Creates (all routed through the runCreate seam) ----

  override def createAllocationGroup(groupName: String, level: Int): Future[AllocationGroupId] =
    val ags = AllocationGroupState(
      AllocationGroupId(UUID.randomUUID()),
      level,
      groupName,
      Nil,
      Nil
    )

    def onFail(err: Throwable): Future[Unit] = err match
      case e: DuplicateRegistration => throw StopRetrying(e)

    runCreate(onFail): tx =>
      given Transaction = tx
      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        ptr <- bsPool.allocator.allocateDataObject(DataBuffer(ags.toBytes))
        _ <- allocationGroupsTree.preparePut(ags.groupId.uuid, ptr)
        _ <- namespacedRegistry.prepareRegisterObject("group", ags.name, ags.groupId.uuid)
      yield
        ags.groupId

  override def createStorageDeviceSet(name: String, level: Int, parent: Option[StorageDeviceSetId]): Future[StorageDeviceSetId] =
    val sds = StorageDeviceSetState(
      StorageDeviceSetId(UUID.randomUUID()),
      name,
      level,
      parent,
      Nil,
      Nil,
      Nil
    )

    // When a parent is supplied, add the new set to the parent's memberSets as part of
    // the same transaction so the parent/child link is established atomically.
    def addToParent(parentId: StorageDeviceSetId)(using tx: Transaction): Future[Unit] =
      for
        parentPtr <- getStorageDeviceSetPointer(parentId)
        parentDos <- read(parentPtr)
      yield
        val parentState = StorageDeviceSetState(parentDos)
        val updated = parentState.copy(memberSets = sds.setId :: parentState.memberSets)
        tx.overwrite(parentPtr, parentDos.revision, DataBuffer(updated.toBytes))

    def onFail(err: Throwable): Future[Unit] = err match
      case e: DuplicateRegistration => throw StopRetrying(e)
      case e: NoSuchElementException => throw StopRetrying(e)

    runCreate(onFail): tx =>
      given Transaction = tx
      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        ptr <- bsPool.allocator.allocateDataObject(DataBuffer(sds.toBytes))
        _ <- storageDeviceSetsTree.preparePut(sds.setId.uuid, ptr)
        _ <- namespacedRegistry.prepareRegisterObject("device-set", sds.name, sds.setId.uuid)
        _ <- parent match
               case None => Future.unit
               case Some(parentId) => addToParent(parentId)
      yield
        sds.setId

  override protected def createStoragePool(config: StoragePoolState): Future[PoolId] =
    // Pool creation has no special recovery handling, so onFail is a no-op. For the production
    // client this is equivalent to the previous transactUntilSuccessful (retry, no recovery);
    // for the test client runCreate performs a single attempt.
    runCreate(_ => Future.unit): tx =>
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

      def updateDevice(du: DeviceUpdate): Unit =
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

        tx.result.foreach: _ =>
          val msg = CheckStorageDevice(
            du.state.hostId,
            clientId,
            du.storageDeviceId
          )
          sendHostMessage(msg)

      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        poolPtr <- createPoolObj(bsPool.allocator)
        _ <- storagePoolsTree.preparePut(config.poolId.uuid, poolPtr)
        _ <- namespacedRegistry.prepareRegisterObject("pool", config.name, config.poolId.uuid)
        devUpdates <- Future.sequence(collectDevices(config.stores))
      yield
        devUpdates.foreach(updateDevice)

        config.poolId

  // ---- Misc plumbing ----

  override def shutdown(): Unit = backgroundTaskManager.shutdown(Duration(50, MILLISECONDS))

  def receiveClientResponse(msg: ClientResponse): Unit = msg match
    case m: ReadResponse => rmgr.receive(m)
    case m: TransactionCompletionResponse => rmgr.receive(m)
    case m: TransactionResolved => txManager.receive(m)
    case m: TransactionFinalized => txManager.receive(m)

  private[aspen] def sendHostMessage(msg: HostMessage): Unit =
    messenger.sendHostMessage(msg)

  private lazy val servicesTkvl =
    TieredKeyValueList(this, KVObjectRootManager(this, Radicle.ServicesTreeKey, radicle))

  override def getServiceHost(serviceUUID: UUID): Future[Option[HostId]] =
    servicesTkvl.get(Key(serviceUUID)).map:
      case None => None
      case Some(vs) =>
        val entry = ServiceEntry.decode(vs.value.bytes)
        if entry.isClaimed then Some(HostId(entry.hostId)) else None

  def getSystemAttribute(key: String): Option[String] = attributes.get(key)
  def setSystemAttribute(key: String, value: String): Unit = attributes += key -> value
```

- [ ] **Step 2: Compile**

Run: `sbt compile`
Expected: BUILD SUCCESS. `BaseAspenClient.scala` compiles. `SimpleAspenClient` and `TClient`
are untouched and still compile (they still independently implement `AspenClient`).

If compilation fails on a missing import or a member still being abstract, fix the import /
add the missing member; do not change any other file.

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala
git commit -m "Add BaseAspenClient with shared AspenClient implementation

Holds the plumbing common to SimpleAspenClient and TestNetwork.TClient, with
recovery differences expressed as constructor parameters plus the abstract
seams resolveIda and runCreate. Not yet wired into either subclass.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Refactor SimpleAspenClient to extend BaseAspenClient

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala`

Replace the entire file. The public constructor signature is preserved (production callers
unaffected); the body collapses to a `super(...)` call plus the production overrides.

- [ ] **Step 1: Replace the entire file with the contents below**

```scala
package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.internal.network.Messenger as ClientMessenger
import org.aspen_ddp.aspen.client.internal.read.SimpleReadDriver
import org.aspen_ddp.aspen.client.internal.transaction.SimpleClientTransactionDriver
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{KeyValueObjectPointer, ObjectPointer}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

class SimpleAspenClient(val msngr: ClientMessenger,
                        clientId: ClientId,
                        executionContext: ExecutionContext,
                        radicle: KeyValueObjectPointer,
                        txStatusCacheDuration: FiniteDuration,
                        initialReadDelay: Duration,
                        maxReadDelay: Duration,
                        txRetransmitDelay: Duration,
                        allocationRetransmitDelay: Duration,
                        userTypeFactories: List[RegisteredTypeFactory] = Nil)
  extends BaseAspenClient(
    executionContext,
    radicle,
    clientId,
    msngr,
    new TransactionStatusCache(txStatusCacheDuration),
    new SimpleObjectCache,
    new SimpleReadDriver.Factory(initialReadDelay, maxReadDelay).apply,
    SimpleClientTransactionDriver.factory(txRetransmitDelay),
    userTypeFactories):

  // Created exactly once, with thread-safe (lazy val) initialization.
  override lazy val opportunisticRebuildManager: OpportunisticRebuildManager =
    new SimpleOpportunisticRebuildManager(this)

  override protected def resolveIda(pointer: ObjectPointer): Future[IDA] =
    getStoragePool(pointer.poolId).map(_.ida)

  override protected def runCreate[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T] =
    transactUntilSuccessfulWithRecovery(onCommitFailure)(prepare)

  override def getCachedAllocator(allocatorId: ObjectAllocatorId): Option[ObjectAllocator] =
    allocatorManager.get(allocatorId)

  override def cacheAllocator(allocator: ObjectAllocator): Unit =
    allocatorManager.put(allocator)
```

> Note: `allocationRetransmitDelay` was already an unused constructor parameter before this change;
> it is intentionally left as-is (out of scope).

- [ ] **Step 2: Compile**

Run: `sbt compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Run the full test suite**

Run: `sbt test`
Expected: all tests pass (green). The production client's behavior is unchanged, and the test
suite still runs on `TClient` (not yet refactored), so this catches any regression in the shared
base or in `SimpleAspenClient`'s wiring.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala
git commit -m "Refactor SimpleAspenClient to extend BaseAspenClient

Collapses SimpleAspenClient to a super(...) call plus production overrides.
opportunisticRebuildManager is now a lazy val (created once, thread-safe),
fixing the prior def that returned a new instance on every call.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Refactor TestNetwork.TClient to extend BaseAspenClient

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` (the `TClient` class, currently lines 64–227)

Only the `TClient` class body changes. The surrounding `TestNetwork` object/class and the imports
stay, except that several now-unused imports can be dropped (optional; leaving them is harmless
aside from unused-import warnings).

- [ ] **Step 1: Replace the entire `TClient` class**

Find the class declaration that currently begins:

```scala
  class TClient(executionContext: ExecutionContext,
                msngr: ClientMessenger,
                val radicle: KeyValueObjectPointer,
                ida: IDA,
                userTypeFactories: List[RegisteredTypeFactory] = Nil) extends AspenClient {
```

and replace that entire class (through its closing brace, currently at line 227) with:

```scala
  class TClient(executionContext: ExecutionContext,
                msngr: ClientMessenger,
                radicle: KeyValueObjectPointer,
                ida: IDA,
                userTypeFactories: List[RegisteredTypeFactory] = Nil)
    extends BaseAspenClient(
      executionContext,
      radicle,
      ClientId(new UUID(0, 1)),
      msngr,
      TransactionStatusCache.NoCache,
      ObjectCache.NoCache,
      BaseReadDriver.noErrorRecoveryReadDriver,
      ClientTransactionDriver.noErrorRecoveryFactory,
      userTypeFactories):

    override def opportunisticRebuildManager: OpportunisticRebuildManager =
      OpportunisticRebuildManager.None

    // Fixed IDA: the test network lives entirely within the bootstrap pool, so resolving the
    // pool per-read would recurse. Returning the known IDA breaks that cycle.
    override protected def resolveIda(pointer: ObjectPointer): Future[IDA] =
      Future.successful(ida)

    // Single attempt, no retry: test failures must surface rather than being retried away.
    override protected def runCreate[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T] =
      transact(prepare)
```

- [ ] **Step 2: Fix imports in TestNetwork.scala**

Ensure the following symbols are importable in `TestNetwork.scala` (most already are):
- `BaseAspenClient` — add `org.aspen_ddp.aspen.client.internal.BaseAspenClient` to the existing
  `client.internal` import (which currently imports `{MetadataTree, ObjectAllocatorManager, OpportunisticRebuildManager}`).
  After the refactor `MetadataTree` and `ObjectAllocatorManager` are no longer referenced by
  `TClient`; the import becomes `{BaseAspenClient, OpportunisticRebuildManager}`.
- `ObjectPointer` — add `org.aspen_ddp.aspen.common.objects.ObjectPointer` to the existing
  `common.objects` import.
- `Transaction`, `TransactionStatusCache`, `ObjectCache`, `ClientId`, `IDA`, `BaseReadDriver`,
  `ClientTransactionDriver`, `Future`, `ExecutionContext`, `KeyValueObjectPointer`, `UUID`,
  `RegisteredTypeFactory` — already imported in the file.

Concretely, update the two affected import lines to:

```scala
import org.aspen_ddp.aspen.client.internal.{BaseAspenClient, OpportunisticRebuildManager}
```

and add `ObjectPointer` to the objects import:

```scala
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectId, ObjectPointer}
```

- [ ] **Step 3: Compile**

Run: `sbt compile`
Expected: BUILD SUCCESS. If the compiler reports unused imports as errors (fatal warnings),
remove only the now-unused imports it names (e.g. `ReadManager`, `TransactionImpl`,
`NamespacedUUIDRegistry`, `UUIDObjectRegistry`, `KVObjectRootManager`, `TieredKeyValueList`,
`ExponentialBackoffRetryStrategy`, `BackgroundTaskManager`, `ServiceEntry`, `MetadataTree`,
`ObjectAllocatorManager`). Otherwise leave imports as they are.

- [ ] **Step 4: Run the full test suite**

Run: `sbt test`
Expected: all tests pass (green). This is the real regression gate — `TClient` underpins the
entire suite, so a green run confirms the disabled-recovery behavior is preserved.

- [ ] **Step 5: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala
git commit -m "Refactor TestNetwork.TClient to extend BaseAspenClient

Collapses TClient to a super(...) call plus disabled-recovery overrides,
removing the duplicated plumbing. The previously-unimplemented (???) methods
are now inherited working implementations from the base.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Extract shared plumbing into base → Task 1.
- Recovery dials as constructor params (caches, factories) → Task 1 constructor + Tasks 2/3 `super(...)`.
- Overridable seams (`opportunisticRebuildManager`, `resolveIda`, `runCreate`) → Task 1 (declared) + Tasks 2/3 (overridden).
- `create*` unified through `runCreate` → Task 1 (all three create methods) + Tasks 2/3 overrides.
- `???` gaps filled by inheritance → Task 1 implements `getStoragePoolId`/`getHostId`/`getAllocationGroupId`/`createStoragePool`; Task 3 removes TClient's `???`.
- `opportunisticRebuildManager` once-only + thread-safe → Task 2 `override lazy val`.
- TClient fixed-IDA read preserved → Task 3 `resolveIda` override.
- TClient no-retry create preserved → Task 3 `runCreate` = `transact`.
- SimpleAspenClient public constructor unchanged → Task 2 signature identical.
- `AspenClient` trait unchanged → no task modifies it.
- Regression via existing suite → Tasks 2 & 3 run `sbt test`.

**Placeholder scan:** none — every code step contains full, compilable source.

**Type consistency:** `resolveIda(pointer: ObjectPointer): Future[IDA]` and
`runCreate[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T]`
are declared identically in the base and overridden with identical signatures in both subclasses.
The base constructor parameter order/types match the `super(...)` argument order in both subclasses:
`(executionContext, radicle, clientId, messenger, txStatusCache, objectCache, readDriverFactory,
txDriverFactory, userTypeFactories)`.
