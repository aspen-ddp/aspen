# Storage Pool Creation Rework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make storage pool creation take a storage device set (instead of a pre-resolved device-id list), select devices via `selectDevicesForPool`, record the new pool in the set's `assignedPools`, and replace the stubbed `new-pool` CLI command with a working `create-pool`.

**Architecture:** `AspenClient.createNewStoragePool` reads the target `StorageDeviceSetState` and calls the existing `selectDevicesForPool(ida.width, …)` (a plain read, before the transaction). `createStoragePool`'s existing single `runCreate` transaction gains one atomic step: read the set object fresh and append the new `PoolId` to `assignedPools` under a revision guard. The CLI `create-pool` command resolves a device-set name and calls the reworked client method.

**Tech Stack:** Scala 3, sbt, ScalaTest (`IntegrationTestSuite` / `atest`), scopt (CLI), Aspen transactions + metadata trees.

**Reference spec:** `docs/superpowers/specs/2026-07-23-storage-pool-creation-rework-design.md`

---

## File Structure

- **Modify** `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` — rework `createNewStoragePool` signature + body (~lines 136–167).
- **Modify** `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` — add `assignedPools` back-reference inside `createStoragePool` (~lines 277–286).
- **Modify** `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — replace `new-pool` command/`new_pool` body with `create-pool`/`create_pool`; update `Args`.
- **Modify (test)** `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala` — add integration test covering both client-side changes.

No `codec.proto` / `Codec.scala` changes — `assignedPools` and `storageDeviceSet` are already in the wire format.

**Notes for the implementer (facts verified in the codebase):**
- `createNewStoragePool` is a concrete method on the `AspenClient` **trait**; it has **no production callers** (the old CLI call was commented out). So changing its signature does not break `SimpleAspenClient` / `BaseReadDriverSuite` stubs — those only override abstract members.
- `StorageDeviceSetState.selectDevicesForPool(numStores, client, rng=Random())` already exists (`StorageDeviceSetState.scala:52`), returns `Future[List[StorageDeviceId]]`, cycles when the set is small, and fails with `AllocationError` if empty.
- The bootstrap storage device set (`StorageDeviceSetId.BootstrapStorageDeviceSetId`) already contains one member device (`StorageDeviceId.BootstrapStorageDeviceId`) and `assignedPools == List(PoolId.BootstrapPoolId)` — this is what the integration test relies on.
- `Main.scala` already imports `IDA, ReedSolomon, Replication` and has a `createIDA(args: Args): IDA` helper (line 286) that validates thresholds. Reuse it. `RocksDBConfig` is **not yet imported** in `Main.scala`.

---

## Task 1: Failing integration test for pool creation from a device set

**Files:**
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala`

- [ ] **Step 1: Add the two new imports** at the top of the suite (after the existing `import org.aspen_ddp.aspen.common.pool.PoolId` at line 7):

```scala
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig
```

- [ ] **Step 2: Add the failing test** at the end of the class body (after the existing `createStorageDeviceSet links the new set into its parent` test, ~line 79). `StorageDeviceSetId`, `StorageDeviceId`, `StorageDeviceSetState`, and `StoragePoolState` are in the same package (`common.metadata`), so no imports are needed for them:

```scala
  atest("createNewStoragePool selects devices from the set and records the pool in assignedPools"):
    given ExecutionContext = executionContext
    val setId = StorageDeviceSetId.BootstrapStorageDeviceSetId
    for
      poolId <- client.createNewStoragePool(
                  "rework-pool",
                  Replication(1, 1),
                  None,
                  RocksDBConfig(),
                  setId,
                  0L)
      _ <- waitForTransactionsToComplete()
      poolState <- client.getStoragePoolState(poolId)
      setState <- client.getStorageDeviceSetState(setId)
    yield
      poolState.storageDeviceSet should be(setId)
      poolState.stores.length should be(1)
      poolState.stores(0).storageDeviceId should be(StorageDeviceId.BootstrapStorageDeviceId)
      setState.assignedPools should contain(poolId)
      setState.assignedPools should contain(PoolId.BootstrapPoolId)
```

- [ ] **Step 3: Run the test to verify it fails (compile error on the new signature)**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite -- -z "records the pool in assignedPools"'`
Expected: **Compilation failure** — `createNewStoragePool` does not accept this argument list (the current signature still has `storageDeviceIds: List[StorageDeviceId]` and no direct `BackendConfig` before the set). This confirms the test targets the new API.

- [ ] **Step 4: Do NOT commit yet** — the suite will not compile until Task 2. Proceed to Task 2.

---

## Task 2: Rework `createNewStoragePool` and add the `assignedPools` back-reference

This task makes two coordinated edits (client trait + base client) so the Task 1 test compiles and passes.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:136-167`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala:277-286`

- [ ] **Step 1: Replace `createNewStoragePool`** in `AspenClient.scala`. Delete the stale `// NOTE:` comment (lines 136–138) and the whole method (139–167), replacing with:

```scala
  def createNewStoragePool(name: String,
                           ida: IDA,
                           maxObjectSize: Option[Int],
                           backendConfig: BackendConfig,
                           storageDeviceSet: StorageDeviceSetId,
                           maximumStoreSize: Long): Future[PoolId] =
    given ExecutionContext = this.clientContext
    val poolId = PoolId(UUID.randomUUID())
    for
      set <- getStorageDeviceSetState(storageDeviceSet)
      deviceIds <- set.selectDevicesForPool(ida.width, this)
      devices <- Future.sequence(deviceIds.map(sid => getStorageDeviceState(sid)))
      stores = devices.map(dev => StoragePoolState.StoreEntry(dev.hostId, dev.storageDeviceId)).toArray
      config = StoragePoolState(
        poolId,
        name,
        ida,
        maxObjectSize,
        stores,
        backendConfig,
        storageDeviceSet,
        0L,
        maximumStoreSize
      )
      _ <- createStoragePool(config)
    yield
      poolId
```

Notes: `getStorageDeviceSetState`, `getStorageDeviceState`, `StoragePoolState`, `StorageDeviceSetId`, `BackendConfig`, `IDA`, `PoolId`, and `UUID` are all already imported/available in `AspenClient.scala` (the old method used most of them; `getStorageDeviceSetState` is defined in this same trait at line 90). No new imports needed.

- [ ] **Step 2: Add the `assignedPools` back-reference** in `BaseAspenClient.scala`. Replace the final `for`/`yield` block of `createStoragePool` (currently lines 277–286):

```scala
      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        poolPtr <- createPoolObj(bsPool.allocator)
        _ <- storagePoolsTree.preparePut(config.poolId.uuid, poolPtr)
        _ <- namespacedRegistry.prepareRegisterObject("pool", config.name, config.poolId.uuid)
        devUpdates <- Future.sequence(collectDevices(config.stores))
      yield
        devUpdates.foreach(updateDevice)

        config.poolId
```

with:

```scala
      for
        bsPool <- getStoragePool(PoolId.BootstrapPoolId)
        poolPtr <- createPoolObj(bsPool.allocator)
        _ <- storagePoolsTree.preparePut(config.poolId.uuid, poolPtr)
        _ <- namespacedRegistry.prepareRegisterObject("pool", config.name, config.poolId.uuid)
        devUpdates <- Future.sequence(collectDevices(config.stores))
        setPtr <- getStorageDeviceSetPointer(config.storageDeviceSet)
        setDos <- read(setPtr)
      yield
        devUpdates.foreach(updateDevice)

        // Record the pool in the device set's assignedPools (reverse of the pool's
        // storageDeviceSet reference). Idempotent so transaction retries are safe.
        val setState = StorageDeviceSetState(setDos)
        if !setState.assignedPools.contains(config.poolId) then
          val updatedSet = setState.copy(assignedPools = config.poolId :: setState.assignedPools)
          tx.overwrite(setPtr, setDos.revision, DataBuffer(updatedSet.toBytes))

        config.poolId
```

Notes: `getStorageDeviceSetPointer`, `read`, `tx.overwrite`, `DataBuffer`, and `StorageDeviceSetState` are all already used in this file (see `createStorageDeviceSet`'s `addToParent` at lines 179–186, which uses the identical fresh-read + revision-guarded-overwrite pattern). No new imports needed.

- [ ] **Step 3: Run the Task 1 integration test — expect PASS**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite -- -z "records the pool in assignedPools"'`
Expected: PASS. If it fails to compile, re-check the `for`-comprehension indentation (Scala 3 quiet syntax) and that the `yield` block is a plain expression sequence.

- [ ] **Step 4: Run the full suite to confirm no regressions**

Run: `sbt 'testOnly *StorageDeviceSetIntegrationSuite'`
Expected: all tests PASS (the pre-existing 6 tests + the new one).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/common/metadata/StorageDeviceSetIntegrationSuite.scala
git commit -m "Rework createNewStoragePool to select from device set and record assignedPools"
```

---

## Task 3: Replace the `new-pool` CLI command with `create-pool`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — `Args` (line 56), backend import (line 26), `new-pool` parser block (173–204), dispatch (277), `new_pool` body (783–819).

- [ ] **Step 1: Add the `RocksDBConfig` import.** Change the backend import on line 26 from:

```scala
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBBackend}
```

to:

```scala
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBBackend, RocksDBConfig}
```

- [ ] **Step 2: Update `Args`.** In the `case class Args(...)` (lines 56–69), remove the `hosts:List[String]=Nil,` field and add `deviceSetName` and `maximumStoreSize`. The field becomes:

```scala
  case class Args(mode:String="",
                  hostDirectory:File=null,
                  bootstrapConfigFile:File=null,
                  hostName:String="",
                  storeName:String="",
                  host:String="",
                  port:Int=0,
                  newPoolName: String="",
                  idaType: String="",
                  width:Int=0,
                  readThreshold:Int=0,
                  writeThreshold:Int=0,
                  deviceSetName:String="",
                  maximumStoreSize:Long=0L,
                  setId:String="")
```

(`hosts` is used only by the `new-pool` command being replaced — the other `.hosts` references in `Main.scala:731` and `MetadataManager.scala` are `BootstrapConfig.hosts`, unrelated to `Args`.)

- [ ] **Step 3: Replace the `new-pool` parser block** (lines 173–204) with the `create-pool` command:

```scala
      cmd("create-pool").text("Creates a new storage pool").
        action((_, c) => c.copy(mode = "create-pool")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<pool-name>").text("Name of the new Pool").
            action((x, c) => c.copy(newPoolName = x)),

          arg[String]("<ida-type>").text("IDA type. Must be Replication or Reed-Solomon").
            action((x, c) => c.copy(idaType = x.toLowerCase())).
            validate { x =>
              val xl = x.toLowerCase
              if xl == "replication" || xl == "reed-solomon" then
                success
              else
                failure("IDA type must be Replication or Reed-Solomon")
            },

          arg[Int]("<width>").text("Total number of slices/replicas").
            action((x, c) => c.copy(width = x)),

          arg[Int]("<read-threshold>").text("Minimum number of slices/replicas that must be read to reconstruct an object").
            action((x, c) => c.copy(readThreshold = x)),

          arg[Int]("<write-threshold>").text("Minimum number of slices/replicas that must be written to successfully write an object").
            action((x, c) => c.copy(writeThreshold = x)),

          arg[String]("<device-set-name>").text("Name of the storage device set that will host the pool's stores").
            action((x, c) => c.copy(deviceSetName = x)),

          arg[Long]("<maximum-store-size>").optional().text("Maximum per-store size in bytes (0 = default/unbounded)").
            action((x, c) => c.copy(maximumStoreSize = x)),
        )
```

- [ ] **Step 4: Update the dispatch.** Change line 277 from:

```scala
            case "new-pool" => new_pool(bootstrapConfigPath, cfg.newPoolName, cfg.idaType, cfg.width, cfg.readThreshold, cfg.writeThreshold, cfg.hosts)
```

to (reusing the existing `createIDA` helper, which validates thresholds):

```scala
            case "create-pool" => create_pool(bootstrapConfigPath, cfg.newPoolName, createIDA(cfg), cfg.deviceSetName, cfg.maximumStoreSize)
```

- [ ] **Step 5: Replace the `new_pool` body** (lines 783–819) with `create_pool`:

```scala
  def create_pool(bootstrapConfigFile: os.Path,
                  poolName: String,
                  ida: IDA,
                  deviceSetName: String,
                  maximumStoreSize: Long): Unit = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = for
      setId <- client.getStorageDeviceSetId(deviceSetName)
      poolId <- client.createNewStoragePool(poolName, ida, None, RocksDBConfig(), setId, maximumStoreSize)
    yield poolId

    val poolId = Await.result(f, Duration(30, SECONDS))

    println("******************************************")
    println(s"* New Pool Created: ${poolId.uuid}")
    println("******************************************")
  }
```

Notes: `Await`, `Duration`, `SECONDS`, `IDA`, and `ExecutionContext` are already imported in `Main.scala` (lines 47–48, 12). `client.getStorageDeviceSetId(name)` resolves the set via the `"device-set"` namespace and returns `Future[StorageDeviceSetId]`.

- [ ] **Step 6: Compile to verify the CLI change**

Run: `sbt compile`
Expected: SUCCESS with no reference to `new_pool`, `cfg.hosts`, or the old `new-pool` command remaining. If the compiler flags an unused/missing `hosts` reference elsewhere, re-verify Step 2 (only the `new-pool` path used it).

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "Replace stubbed new-pool CLI with working create-pool command"
```

---

## Task 4: Full build + test verification

**Files:** none (verification only).

- [ ] **Step 1: Full compile**

Run: `sbt compile`
Expected: SUCCESS.

- [ ] **Step 2: Full test suite**

Run: `sbt test`
Expected: all tests PASS, including `StorageDeviceSetIntegrationSuite`, `StorageDeviceSetSelectionSuite`, and any pool-related integration suites.

- [ ] **Step 3: If everything is green, no further commit is needed** (work was committed in Tasks 2 and 3). If `sbt test` surfaced an unexpected caller of the old signature, fix it minimally, re-run, and commit with message `Fix call site for reworked createNewStoragePool`.

---

## Self-Review Notes

- **Spec coverage:** §1 (createNewStoragePool rework) → Task 2 Step 1. §2 (assignedPools back-reference) → Task 2 Step 2. §3 (create-pool CLI) → Task 3. §4 (call sites & tests / no codec changes) → Tasks 1, 2, 4. Cycling behavior preserved (no uniqueness check added). ✓
- **Type consistency:** `createNewStoragePool(name, ida, maxObjectSize, backendConfig, storageDeviceSet, maximumStoreSize)` is used identically in the test (Task 1 Step 2), the trait definition (Task 2 Step 1), and the CLI caller (Task 3 Step 5). `StoragePoolState(...)` argument order matches the current constructor. ✓
- **No placeholders:** every code step contains complete code and exact run commands. ✓
