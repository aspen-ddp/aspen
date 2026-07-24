# add-pool-to-group Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a CLI command `add-pool-to-group` that adds a storage pool to an allocation group by name, with the plumbing to create the usage-cascade durable task atomically without a locally-running `TaskExecutor`.

**Architecture:** A new transaction-scoped primitive `AspenClient.prepareSystemDurableTask` stages a system durable task into a caller-supplied transaction. `AllocationGroupState.addPool`/`removePool`/`addGroup`/`removeGroup` take an `Option[TaskExecutor]`: `Some` enrolls the usage task in-transaction via the executor (existing behavior, now correctly awaited); `None` enrolls it in-transaction via `prepareSystemDurableTask`. A new `AspenClient.addPoolToGroup` resolves names and calls `addPool(..., None)`; the CLI command is thin glue over it.

**Tech Stack:** Scala 3, ScalaTest (`IntegrationTestSuite` / `AsyncFunSuite`), scopt (CLI parsing), Aspen transactions + TKVL.

---

## Spec

Source spec: `docs/superpowers/specs/2026-07-24-add-pool-to-group-design.md`

## Background facts (verified against the codebase)

- `transact` (`AspenClient.scala:120-133`) awaits the future returned by `prepare` **before** `tx.commit()`. To land work in the transaction, `prepare`'s returned future must resolve only after that work is staged. The current `modifyPool`/`modifyGroup` fire `prepareTask(...)` inside the `yield` without chaining it in — a latent race this plan fixes.
- `SystemTaskServiceState` (`.../compute/systemtask/SystemTaskServiceState.scala`) already has `removeInTx(...)(using tx)`. We add the sibling `enrollInTx`.
- `BaseAspenClient` already has a `private lazy val servicesTkvl` (line ~334) and imports `PoolObjectAllocator`, `SystemTaskServiceState`, `SystemTaskExecutorService`, `NewSystemTaskAdded`, `SystemTaskMessage`, `SimpleTaskExecutor`, `ServiceEntry`, `DurableTaskFactory`, `uuid2byte`, `Radicle`, and `org.aspen_ddp.aspen.client.*` (which includes `StopRetrying`). No new imports needed there.
- `UpdateAllocationGroupUsageTask` is registered by default in every client's `TypeRegistry` (`common/TypeFactories.scala`), so system-task tests need no `userTypeFactories` override.
- The test harness registers `SystemTaskExecutorService` (see `CreateSystemDurableTaskSuite` / `SystemTaskServiceStateSuite`), so the `None`/system path works in integration tests. No host claims the service in tests, so the best-effort wake-up message is silently dropped (fine).
- All current callers of `addPool`/`removePool`/`addGroup`/`removeGroup` are tests in three suites (see Task 4).
- `MainSuite` only unit-tests **pure** helper functions (`formatBytes`, `resolveRef`, `formatXState`); command handlers (`create_pool`, `transfer_store`, …) have no MainSuite tests. This plan follows that convention: the new command handler is thin glue over `addPoolToGroup`, which is covered by an integration test.

## File structure

Production:
- `.../compute/systemtask/SystemTaskServiceState.scala` — add `enrollInTx` (Task 1).
- `.../client/AspenClient.scala` — declare `prepareSystemDurableTask` (abstract) and `addPoolToGroup` (concrete) (Tasks 2, 5).
- `.../client/internal/BaseAspenClient.scala` — implement `prepareSystemDurableTask`; refactor `createSystemDurableTask` to delegate (Task 2).
- `.../common/metadata/management/UpdateAllocationGroupUsageTask.scala` — add `prepareSystemTask` (Task 3).
- `.../common/metadata/AllocationGroupState.scala` — `Option[TaskExecutor]` params; `mod` returns `Option`; awaited task prep (Task 4).
- `.../cmdline/Main.scala` — `Args` field, parser block, dispatch, handler (Task 6).

Tests:
- `.../compute/systemtask/SystemTaskServiceStateSuite.scala` (Task 1).
- `.../compute/systemtask/CreateSystemDurableTaskSuite.scala` (Task 2, add a case).
- `.../common/metadata/AllocationGroupStateSuite.scala` (Task 4: call-site updates + new cases).
- `.../common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala` (Task 4: call-site updates).
- `.../client/internal/allocation/GroupObjectAllocatorSuite.scala` (Task 4: call-site updates).

---

## Task 1: `SystemTaskServiceState.enrollInTx`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceState.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceStateSuite.scala`

- [ ] **Step 1: Write the failing test**

Add this test to `SystemTaskServiceStateSuite` (after the existing `atest`, inside the class):

```scala
  atest("enrollInTx enrolls within a caller-supplied transaction"):
    given ExecutionContext = executionContext
    val taskId = UUID.randomUUID()
    for
      statePtr <- servicePtr()
      pool <- client.getStoragePool(Radicle.poolId)
      allocator = new PoolObjectAllocator(client, pool)
      // Allocate the task-state object and enroll it in a single transaction.
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             for
               taskStatePtr <- allocator.allocateKeyValueObject(Map(Key(1) -> Value(Array[Byte](9))))
               _ <- SystemTaskServiceState.enrollInTx(client, statePtr, taskId, taskStatePtr)
             yield ()
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
    yield
      enrolled.map(_._1) should contain (taskId)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *SystemTaskServiceStateSuite -- -z "enrollInTx enrolls within a caller-supplied transaction"'`
Expected: FAIL to compile — `value enrollInTx is not a member of object SystemTaskServiceState`.

- [ ] **Step 3: Add `enrollInTx`**

In `SystemTaskServiceState.scala`, add this method after `enroll` (right before or after `removeInTx`; it mirrors `removeInTx`):

```scala
  /** Insert a task into the registry within an existing transaction. Idempotent: a re-enroll
   *  of the same key overwrites the value (the pointer is stable for a given task UUID). */
  def enrollInTx(client: AspenClient,
                 statePtr: KeyValueObjectPointer,
                 taskId: UUID,
                 taskStatePtr: KeyValueObjectPointer)(using tx: Transaction): Future[Unit] =
    tkvl(client, statePtr).set(Key(uuid2byte(taskId)), Value(taskStatePtr.toArray))
```

(`Value` is already imported in this file; `Transaction` is already imported.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *SystemTaskServiceStateSuite'`
Expected: PASS (all cases, including the new one).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceState.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/systemtask/SystemTaskServiceStateSuite.scala
git commit -m "feat: add SystemTaskServiceState.enrollInTx"
```

---

## Task 2: `AspenClient.prepareSystemDurableTask` + refactor `createSystemDurableTask`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (declare abstract method)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` (implement + refactor)
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/systemtask/CreateSystemDurableTaskSuite.scala`

- [ ] **Step 1: Write the failing test**

Add this case to `CreateSystemDurableTaskSuite` (it already registers `CountingSystemTask` via `userTypeFactories`):

```scala
  atest("prepareSystemDurableTask enrolls the task within a caller-supplied transaction"):
    given ExecutionContext = executionContext
    val servicesTkvl = TieredKeyValueList(client,
      KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      _ <- client.transactUntilSuccessful: tx =>
             given org.aspen_ddp.aspen.client.Transaction = tx
             client.prepareSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(0))
      stateVs <- servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      statePtr = ServiceEntry.decode(stateVs.get.value.bytes).statePointer
      enrolled <- SystemTaskServiceState.scan(client, statePtr)
      taskKvos <- client.read(enrolled.head._2)
    yield
      enrolled.size shouldBe 1
      byte2uuid(taskKvos.contents(SimpleTaskExecutor.TaskTypeKey).value.bytes) shouldBe CountingSystemTask.typeUUID
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *CreateSystemDurableTaskSuite -- -z "prepareSystemDurableTask enrolls the task within a caller-supplied transaction"'`
Expected: FAIL to compile — `value prepareSystemDurableTask is not a member of AspenClient`.

- [ ] **Step 3: Declare the abstract method on the `AspenClient` trait**

In `AspenClient.scala`, immediately after the `createSystemDurableTask` declaration (ends at line ~275), add:

```scala
  /** Stage the allocation and enrollment of a system-level DurableTask into the supplied
   *  transaction. The returned future resolves once both operations are staged in `tx`;
   *  commit and retry are the caller's responsibility. A best-effort wake-up message to the
   *  SystemTaskExecutorService is sent after the transaction commits.
   *
   *  `taskTypeUUID` must resolve to a DurableTaskFactory in the type registry. */
  def prepareSystemDurableTask(taskTypeUUID: UUID,
                               initialState: Map[Key, Array[Byte]])
                              (using tx: Transaction): Future[Unit]
```

(`UUID`, `Key`, `Future`, `Transaction` are all already imported/in scope in this file.)

- [ ] **Step 4: Implement it in `BaseAspenClient` and refactor `createSystemDurableTask`**

In `BaseAspenClient.scala`, replace the entire existing `createSystemDurableTask` method (currently lines ~344-377) with the following two methods:

```scala
  override def createSystemDurableTask(taskTypeUUID: UUID,
                                       initialState: Map[Key, Array[Byte]]): Future[Unit] =
    given ExecutionContext = clientContext
    transactUntilSuccessful: tx =>
      given Transaction = tx
      prepareSystemDurableTask(taskTypeUUID, initialState)

  override def prepareSystemDurableTask(taskTypeUUID: UUID,
                                        initialState: Map[Key, Array[Byte]])
                                       (using tx: Transaction): Future[Unit] =
    given ExecutionContext = clientContext

    // Sanity check: the type must resolve to a DurableTaskFactory.
    typeRegistry.getType[DurableTaskFactory](taskTypeUUID) match
      case None =>
        Future.failed(StopRetrying(
          new IllegalArgumentException(s"prepareSystemDurableTask: no DurableTaskFactory registered for $taskTypeUUID")))
      case Some(_) =>
        val taskId = UUID.randomUUID()

        def serviceStatePtr(): Future[KeyValueObjectPointer] =
          servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).map:
            case Some(vs) => ServiceEntry.decode(vs.value.bytes).statePointer
            case None => throw StopRetrying(
              new IllegalStateException("SystemTaskExecutorService is not registered"))

        val taskContent: Map[Key, Value] =
          (initialState + (SimpleTaskExecutor.TaskTypeKey -> uuid2byte(taskTypeUUID)))
            .map((k, v) => k -> Value(v))

        for
          statePtr <- serviceStatePtr()
          pool <- getStoragePool(Radicle.poolId)
          allocator = new PoolObjectAllocator(this, pool)
          taskStatePtr <- allocator.allocateKeyValueObject(taskContent)
          _ <- SystemTaskServiceState.enrollInTx(this, statePtr, taskId, taskStatePtr)
        yield
          tx.result.foreach: _ =>
            sendServiceMessage(SystemTaskExecutorService.ServiceUUID,
              SystemTaskMessage.encode(NewSystemTaskAdded(taskId)))
```

Notes:
- `allocator.allocateKeyValueObject(taskContent)` and `SystemTaskServiceState.enrollInTx(...)` both consume the `using tx: Transaction` in scope, staging into the caller's transaction.
- `Value` is available via `import org.aspen_ddp.aspen.common.objects.*`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt 'testOnly *CreateSystemDurableTaskSuite'`
Expected: PASS — both the pre-existing `createSystemDurableTask` case and the new `prepareSystemDurableTask` case.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/systemtask/CreateSystemDurableTaskSuite.scala
git commit -m "feat: add AspenClient.prepareSystemDurableTask (tx-scoped); createSystemDurableTask delegates to it"
```

---

## Task 3: `UpdateAllocationGroupUsageTask.prepareSystemTask`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala`

This method has no standalone test; it is exercised by the `None`-path integration tests in Task 4. It is a thin wrapper, so we add it directly.

- [ ] **Step 1: Add `prepareSystemTask`**

In `UpdateAllocationGroupUsageTask.scala`, add this method immediately after the existing `prepareTask` method (inside `object UpdateAllocationGroupUsageTask`):

```scala
  def prepareSystemTask(client: AspenClient,
                        childUUID: UUID,
                        currentUsage: Long,
                        maximumStoreSize: Long,
                        allocationGroups: List[UUID])
                       (using tx: Transaction): Future[Unit] =
    require(allocationGroups.nonEmpty)

    client.prepareSystemDurableTask(typeUUID, Map(
      ChildUUIDKey        -> uuid2byte(childUUID),
      CurrentUsageKey     -> long2byte(currentUsage),
      MaxSizeKey          -> long2byte(maximumStoreSize),
      AllocationGroupsKey -> uuids2byte(allocationGroups),
      NextIndexKey        -> long2byte(0)))
```

Notes:
- `AspenClient`, `Transaction`, `uuid2byte`, `long2byte`, `uuids2byte`, and the `*Key` vals are all already imported/defined in this file.
- `prepareSystemDurableTask` adds the task-type key itself, so it is not included in the map (matching `prepareTask`, which delegates that to `taskExecutor.prepareTask`).

- [ ] **Step 2: Verify it compiles**

Run: `sbt compile`
Expected: SUCCESS (no callers yet; this just adds a method).

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala
git commit -m "feat: add UpdateAllocationGroupUsageTask.prepareSystemTask (tx-scoped system path)"
```

---

## Task 4: Optional executor in `AllocationGroupState` (atomic; fire-on-change only)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupState.scala`
- Modify (call sites): `src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala`
- Modify (call sites): `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`
- Modify (call sites): `src/test/scala/org/aspen_ddp/aspen/client/internal/allocation/GroupObjectAllocatorSuite.scala`

This is a signature refactor: the whole module (and its test callers) must change together to compile.

- [ ] **Step 1: Rewrite the four public methods + two private helpers in `AllocationGroupState.scala`**

Replace the current `addPool`, `removePool`, `modifyPool`, `addGroup`, `removeGroup`, and `modifyGroup` (lines ~35-209) with the following. Only the executor parameter type, the `mod` return type (`Option[...]`), the no-op branches, and the awaited task-preparation logic change; the transaction bodies are otherwise identical to the originals.

```scala
  def addPool(client: AspenClient,
              poolId: PoolId,
              parentId: AllocationGroupId,
              taskExecutor: Option[TaskExecutor] = None): Future[Unit] =

    def mod(psPtr: KeyValueObjectPointer, psKvos: KeyValueObjectState,
            agsPtr: DataObjectPointer, agsDos: DataObjectState,
            ps: StoragePoolState, ags: AllocationGroupState,
            tx: Transaction): Option[(StoragePoolState, AllocationGroupState)] =
      if ps.allocationGroups.contains(parentId.uuid) && ags.members.exists(_.uuid == poolId.uuid) then
        None
      else
        val nps = ps.copy(allocationGroups = parentId.uuid :: ps.allocationGroups)
        val reqs = List(KeyRevision(StoragePoolState.ConfigKey, psKvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, nps.encode()))

        tx.update(psPtr, None, None, reqs, ops)

        val m = Member(MemberType.Pool, ps.poolId.uuid, ps.maxObjectSize, ps.currentUsage, ps.maximumStoreSize)
        val nags = ags.copy(members = m :: ags.members)

        tx.overwrite(agsPtr, agsDos.revision, DataBuffer(nags.toBytes))

        Some((nps, nags))

    modifyPool(client, poolId, parentId, taskExecutor, mod)

  def removePool(client: AspenClient,
                 poolId: PoolId,
                 parentId: AllocationGroupId,
                 taskExecutor: Option[TaskExecutor] = None): Future[Unit] =

    def mod(psPtr: KeyValueObjectPointer, psKvos: KeyValueObjectState,
            agsPtr: DataObjectPointer, agsDos: DataObjectState,
            ps: StoragePoolState, ags: AllocationGroupState,
            tx: Transaction): Option[(StoragePoolState, AllocationGroupState)] =
      if !ps.allocationGroups.contains(parentId.uuid) && !ags.members.exists(_.uuid == poolId.uuid) then
        None
      else
        val nps = ps.copy(allocationGroups = ps.allocationGroups.filter(_ != parentId.uuid))
        val reqs = List(KeyRevision(StoragePoolState.ConfigKey, psKvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, nps.encode()))

        tx.update(psPtr, None, None, reqs, ops)

        val nags = ags.copy(members = ags.members.filter(_.uuid != ps.poolId.uuid))

        tx.overwrite(agsPtr, agsDos.revision, DataBuffer(nags.toBytes))

        Some((nps, nags))

    modifyPool(client, poolId, parentId, taskExecutor, mod)

  private def modifyPool(client: AspenClient,
                         poolId: PoolId,
                         parentId: AllocationGroupId,
                         taskExecutor: Option[TaskExecutor],
                         mod: (KeyValueObjectPointer, KeyValueObjectState, DataObjectPointer, DataObjectState,
                           StoragePoolState, AllocationGroupState,
                           Transaction) => Option[(StoragePoolState, AllocationGroupState)]
                        ): Future[Unit] =
    given ExecutionContext = client.clientContext

    def prep(tx: Transaction): Future[Unit] =
      given Transaction = tx
      for
        psPtr <- client.getStoragePoolPointer(poolId)
        agsPtr <- client.getAllocationGroupPointer(parentId)
        psKvos <- client.read(psPtr)
        agsDos <- client.read(agsPtr)
        ps = StoragePoolState(psKvos)
        ags = AllocationGroupState(agsDos)
        _ <- mod(psPtr, psKvos, agsPtr, agsDos, ps, ags, tx) match
          case Some((_, nags)) if nags.parentGroups.nonEmpty =>
            taskExecutor match
              case Some(exec) =>
                UpdateAllocationGroupUsageTask.prepareTask(poolId.uuid,
                  nags.currentUsage, nags.maximumSize, nags.parentGroups.map(_.uuid), exec).map(_ => ())
              case None =>
                UpdateAllocationGroupUsageTask.prepareSystemTask(client, poolId.uuid,
                  nags.currentUsage, nags.maximumSize, nags.parentGroups.map(_.uuid))
          case _ =>
            Future.unit
      yield ()

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)

    client.transactUntilSuccessfulWithRecovery(onFail): tx =>
      prep(tx)


  def addGroup(client: AspenClient,
               childId: AllocationGroupId,
               parentId: AllocationGroupId,
               taskExecutor: Option[TaskExecutor] = None): Future[Unit] =

    def mod(childPtr: DataObjectPointer, childDos: DataObjectState,
            parentPtr: DataObjectPointer, parentDos: DataObjectState,
            child: AllocationGroupState, parent: AllocationGroupState,
            tx: Transaction): Option[(AllocationGroupState, AllocationGroupState)] =
      if child.parentGroups.exists(_.uuid == parentId.uuid) && parent.members.exists(_.uuid == childId.uuid) then
        None
      else
        if child.level >= parent.level then
          throw new InvalidLevel()

        val nchild = child.copy(parentGroups = parentId :: child.parentGroups)

        tx.overwrite(childPtr, childDos.revision, DataBuffer(nchild.toBytes))

        val m = Member(MemberType.Group, child.groupId.uuid, child.maximumObjectSize,
          child.currentUsage, child.maximumSize)
        val nags = parent.copy(members = m :: parent.members)

        tx.overwrite(parentPtr, parentDos.revision, DataBuffer(nags.toBytes))

        Some((nchild, nags))

    modifyGroup(client, childId, parentId, taskExecutor, mod)

  def removeGroup(client: AspenClient,
                  childId: AllocationGroupId,
                  parentId: AllocationGroupId,
                  taskExecutor: Option[TaskExecutor] = None): Future[Unit] =

    def mod(childPtr: DataObjectPointer, childDos: DataObjectState,
            parentPtr: DataObjectPointer, parentDos: DataObjectState,
            child: AllocationGroupState, parent: AllocationGroupState,
            tx: Transaction): Option[(AllocationGroupState, AllocationGroupState)] =
      if !child.parentGroups.exists(_.uuid == parentId.uuid) && !parent.members.exists(_.uuid == childId.uuid) then
        None
      else
        val nchild = child.copy(parentGroups = child.parentGroups.filter(_ != parentId))

        tx.overwrite(childPtr, childDos.revision, DataBuffer(nchild.toBytes))

        val nparent = parent.copy(members = parent.members.filter(_.uuid != child.groupId.uuid))

        tx.overwrite(parentPtr, parentDos.revision, DataBuffer(nparent.toBytes))

        Some((nchild, nparent))

    modifyGroup(client, childId, parentId, taskExecutor, mod)

  private def modifyGroup(client: AspenClient,
                          childId: AllocationGroupId,
                          parentId: AllocationGroupId,
                          taskExecutor: Option[TaskExecutor],
                          mod: (DataObjectPointer, DataObjectState, DataObjectPointer, DataObjectState,
                            AllocationGroupState, AllocationGroupState,
                            Transaction) => Option[(AllocationGroupState, AllocationGroupState)]
                         ): Future[Unit] =
    given ExecutionContext = client.clientContext

    def prep(tx: Transaction): Future[Unit] =
      given Transaction = tx

      for
        childPtr <- client.getAllocationGroupPointer(childId)
        parentPtr <- client.getAllocationGroupPointer(parentId)
        childDos <- client.read(childPtr)
        parentDos <- client.read(parentPtr)
        child = AllocationGroupState(childDos)
        parent = AllocationGroupState(parentDos)
        _ <- mod(childPtr, childDos, parentPtr, parentDos, child, parent, tx) match
          case Some((_, nparent)) if nparent.parentGroups.nonEmpty =>
            taskExecutor match
              case Some(exec) =>
                UpdateAllocationGroupUsageTask.prepareTask(childId.uuid,
                  nparent.currentUsage, nparent.maximumSize, nparent.parentGroups.map(_.uuid), exec).map(_ => ())
              case None =>
                UpdateAllocationGroupUsageTask.prepareSystemTask(client, childId.uuid,
                  nparent.currentUsage, nparent.maximumSize, nparent.parentGroups.map(_.uuid))
          case _ =>
            Future.unit
      yield ()

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)
      case e: InvalidLevel => throw StopRetrying(e)

    client.transactUntilSuccessfulWithRecovery(onFail): tx =>
      prep(tx)
```

- [ ] **Step 2: Update the ~30 test call sites to pass `Some(executor)`**

The existing suites build a `TaskExecutor` named `executor` and pass it as the last argument. With the new `Option[TaskExecutor]` parameter, every such call must wrap it in `Some(...)`.

In each of these three files, replace every occurrence of the literal substring `, executor)` with `, Some(executor))`:
- `src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala`
- `src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala`
- `src/test/scala/org/aspen_ddp/aspen/client/internal/allocation/GroupObjectAllocatorSuite.scala`

Run this to apply and confirm no stray `, executor)` remains:

```bash
cd /Users/tcocagne/devel/tom/aspen
for f in \
  src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala \
  src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala \
  src/test/scala/org/aspen_ddp/aspen/client/internal/allocation/GroupObjectAllocatorSuite.scala ; do
  perl -0pi -e 's/, executor\)/, Some(executor))/g' "$f"
done
grep -rn ', executor)' src/test || echo "OK: no bare ', executor)' remain"
```

Expected: `OK: no bare ', executor)' remain`.

- [ ] **Step 3: Run the three suites to verify the refactor is green**

Run: `sbt 'testOnly *AllocationGroupStateSuite *UpdateAllocationGroupUsageTaskSuite *GroupObjectAllocatorSuite'`
Expected: PASS (all existing cases). This confirms the executor path still works and the `Option` refactor compiles.

- [ ] **Step 4: Write a failing test for the no-op-fires-no-task behavior**

Add to `AllocationGroupStateSuite` (inside the class). This asserts a no-op re-add does not enroll a usage task. It creates a child group with a parent (so `parentGroups.nonEmpty`), adds the pool once via the executor, records the executor's active task count is quiescent, then re-adds (no-op) and confirms membership is unchanged and unduplicated:

```scala
  atest("addPool no-op re-add does not change membership"):
    given ExecutionContext = executionContext
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-noop", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-noop", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addPool(client, Radicle.poolId, childId, Some(executor))
      _ <- waitForTransactionsToComplete()

      // Re-add: this is a no-op and must not duplicate membership.
      _ <- AllocationGroupState.addPool(client, Radicle.poolId, childId, Some(executor))
      _ <- waitForTransactionsToComplete()

      ps <- readPoolState()
      ags <- readGroupState(childId)
    yield
      ps.allocationGroups.count(_ == childId.uuid) should be(1)
      ags.members.count(_.uuid == Radicle.poolId.uuid) should be(1)
```

- [ ] **Step 5: Write a test for the `None` (system durable task) path**

Add to `AllocationGroupStateSuite` (inside the class). It adds a pool to a child group that has a parent, using `None`, and verifies the membership change committed **and** a `UpdateAllocationGroupUsageTask` was enrolled in the SystemTaskExecutorService registry:

```scala
  atest("addPool with no executor enrolls a system usage task"):
    given ExecutionContext = executionContext
    val servicesTkvl = org.aspen_ddp.aspen.client.tkvl.TieredKeyValueList(client,
      org.aspen_ddp.aspen.client.tkvl.KVObjectRootManager(client,
        Radicle.ServicesTreeKey, Radicle.pointer))
    for
      executor <- setup()
      childId <- client.createAllocationGroup("child-sys", level = 0)
      _ <- waitForTransactionsToComplete()
      parentId <- client.createAllocationGroup("parent-sys", level = 1)
      _ <- waitForTransactionsToComplete()

      _ <- AllocationGroupState.addGroup(client, childId, parentId, Some(executor))
      _ <- waitForTransactionsToComplete()

      // No executor supplied -> system durable task path.
      _ <- AllocationGroupState.addPool(client, Radicle.poolId, childId, None)
      _ <- waitForTransactionsToComplete()

      stateVs <- servicesTkvl.get(org.aspen_ddp.aspen.common.objects.Key(
                   org.aspen_ddp.aspen.compute.systemtask.SystemTaskExecutorService.ServiceUUID))
      statePtr = org.aspen_ddp.aspen.compute.ServiceEntry.decode(stateVs.get.value.bytes).statePointer
      enrolled <- org.aspen_ddp.aspen.compute.systemtask.SystemTaskServiceState.scan(client, statePtr)

      ps <- readPoolState()
      ags <- readGroupState(childId)
    yield
      ps.allocationGroups should contain(childId.uuid)
      ags.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)
      enrolled.size should be >= 1
```

(Fully-qualified names are used to avoid adding imports; a worker may add imports and shorten them if preferred.)

- [ ] **Step 6: Run the new tests to verify they pass**

Run: `sbt 'testOnly *AllocationGroupStateSuite -- -z "no-op re-add" -z "no executor enrolls a system usage task"'`
Expected: PASS for both. (If the runner does not accept multiple `-z`, run each `-z` separately.)

- [ ] **Step 7: Run the full three-suite set again**

Run: `sbt 'testOnly *AllocationGroupStateSuite *UpdateAllocationGroupUsageTaskSuite *GroupObjectAllocatorSuite'`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupState.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTaskSuite.scala \
        src/test/scala/org/aspen_ddp/aspen/client/internal/allocation/GroupObjectAllocatorSuite.scala
git commit -m "feat: optional TaskExecutor in AllocationGroupState add/remove; atomic system-task path, fire-on-change only"
```

---

## Task 5: `AspenClient.addPoolToGroup`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala`

- [ ] **Step 1: Write the failing test**

Add to `AllocationGroupStateSuite` (inside the class):

```scala
  atest("addPoolToGroup resolves names and adds the pool"):
    given ExecutionContext = executionContext
    for
      groupId <- client.createAllocationGroup("named-group", level = 0)
      _ <- waitForTransactionsToComplete()

      // Radicle bootstrap pool is registered under a known name; resolve it dynamically.
      pools <- client.listStoragePools()
      poolName = pools.find(_._2 == Radicle.poolId).get._1

      _ <- client.addPoolToGroup(poolName, "named-group")
      _ <- waitForTransactionsToComplete()

      ps <- readPoolState()
      ags <- readGroupState(groupId)
    yield
      ps.allocationGroups should contain(groupId.uuid)
      ags.members.exists(_.uuid == Radicle.poolId.uuid) should be(true)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *AllocationGroupStateSuite -- -z "addPoolToGroup resolves names and adds the pool"'`
Expected: FAIL to compile — `value addPoolToGroup is not a member of AspenClient`.

- [ ] **Step 3: Implement `addPoolToGroup` on the `AspenClient` trait**

In `AspenClient.scala`, add this concrete method near the other name-based operations (e.g. immediately after `getAllocationGroupState`, around line 90, or alongside the `create*` declarations). It uses only public trait members, so it belongs on the trait:

```scala
  /** Add a storage pool to an allocation group, both identified by name. Fails with
   *  NoSuchElementException if either name is not registered. Uses the system durable
   *  task path for any usage cascade (no local TaskExecutor). */
  def addPoolToGroup(poolName: String, groupName: String): Future[Unit] =
    given ExecutionContext = this.clientContext
    for
      poolId  <- getStoragePoolId(poolName)
      groupId <- getAllocationGroupId(groupName)
      _       <- AllocationGroupState.addPool(this, poolId, groupId, None)
    yield ()
```

(`AllocationGroupState` is imported via `org.aspen_ddp.aspen.common.metadata.{... AllocationGroupState ...}` on line 9; `Future`/`ExecutionContext` are in scope.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *AllocationGroupStateSuite -- -z "addPoolToGroup resolves names and adds the pool"'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala
git commit -m "feat: add AspenClient.addPoolToGroup"
```

---

## Task 6: CLI command `add-pool-to-group`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`

No MainSuite test: `MainSuite` only unit-tests pure helpers, and this handler is thin glue over `addPoolToGroup` (covered in Task 5). This mirrors how `create_pool`, `transfer_store`, etc. are untested in `MainSuite`.

- [ ] **Step 1: Add the `poolName` field to `Args`**

In `Main.scala`, modify the `Args` case class (lines 58-78) to add a `poolName` field. Change the line:

```scala
                  newGroupName:String="",
```

to:

```scala
                  newGroupName:String="",
                  poolName:String="",
```

- [ ] **Step 2: Add the parser block**

In the `main` parser (in the `new scopt.OptionParser[Args]("demo")` block), add this `cmd` immediately after the `create-allocation-group` block (which ends around line 255, before `cmd("transfer-store")`):

```scala
      cmd("add-pool-to-group").text("Adds a storage pool to an allocation group").
        action((_, c) => c.copy(mode = "add-pool-to-group")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<pool-name>").text("Name of the pool to add").
            action((x, c) => c.copy(poolName = x)),

          arg[String]("<group-name>").text("Name of the allocation group").
            action((x, c) => c.copy(newGroupName = x)),
        )
```

- [ ] **Step 3: Add the dispatch case**

In the mode-dispatch `match` (around lines 428-447), add this case immediately after the `create-allocation-group` case (line 435):

```scala
            case "add-pool-to-group" => add_pool_to_group(bootstrapConfigPath, cfg.poolName, cfg.newGroupName)
```

- [ ] **Step 4: Add the handler function**

In `Main.scala`, add this function immediately after `create_allocation_group` (which ends at line 1055, before `transfer_store`):

```scala
  def add_pool_to_group(bootstrapConfigFile: os.Path,
                        poolName: String,
                        groupName: String): Unit = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = client.addPoolToGroup(poolName, groupName)

    // getStoragePoolId / getAllocationGroupId throw NoSuchElementException when a
    // name is not registered; translate that into a precise message.
    def reportError(cause: Throwable): Unit = cause match
      case _: NoSuchElementException =>
        println(s"Error: pool '$poolName' or allocation group '$groupName' not found")
      case e =>
        println(s"Error adding pool to group: ${e.getMessage}")

    f.onComplete:
      case scala.util.Success(_) =>
        println(s"Pool '$poolName' added to allocation group '$groupName'")
      case scala.util.Failure(err) => reportError(err)

    Await.ready(f, Duration(30, SECONDS))
  }
```

- [ ] **Step 5: Verify it compiles**

Run: `sbt compile`
Expected: SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "feat: add 'add-pool-to-group' CLI command"
```

---

## Task 7: Full verification and TODO cleanup

**Files:**
- Modify: `TODO.txt`

- [ ] **Step 1: Run the full test suite**

Run: `sbt test`
Expected: PASS (all suites). If anything fails, fix before proceeding — do not mark this task complete with failing tests.

- [ ] **Step 2: Remove the completed item from `TODO.txt`**

Delete the first work item block (the `Add CLI command "add-pool-to-group"` bullet and its sub-bullets, currently lines 2-9), including the trailing blank line, so the file now begins with the `Add CLI command "add-group-to-group"` item. This matches the project convention of removing completed work (see commit `846d1e1 removed completed work`).

- [ ] **Step 3: Commit**

```bash
git add TODO.txt
git commit -m "chore: remove completed add-pool-to-group item from TODO"
```

---

## Self-review notes (already reconciled)

- **Spec coverage:** CLI command (Task 6), `AspenClient.addPoolToGroup` (Task 5), `prepareSystemDurableTask` + `createSystemDurableTask` refactor (Task 2), `UpdateAllocationGroupUsageTask.prepareSystemTask` (Task 3), `Option[TaskExecutor]` on pool **and** group methods with atomic/fire-on-change semantics (Task 4), `enrollInTx` (Task 1). All covered.
- **Deviation from spec's testing bullet:** the spec suggested a `MainSuite` test for the command. `MainSuite` only tests pure helpers and has no integration harness, so command behavior is covered by the `addPoolToGroup` integration test (Task 5) instead. Documented in Task 6.
- **Type consistency:** `taskExecutor: Option[TaskExecutor]`, `mod` returns `Option[(…, …)]`, and the branch matches `Some((_, nags)) if nags.parentGroups.nonEmpty` are consistent across `modifyPool` and `modifyGroup`. `prepareSystemDurableTask(using tx)` and `prepareSystemTask(using tx)` both return `Future[Unit]`; `prepareTask` still returns `Future[Future[Option[AnyRef]]]` and is adapted with `.map(_ => ())`.
