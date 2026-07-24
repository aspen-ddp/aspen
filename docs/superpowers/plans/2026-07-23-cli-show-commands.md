# CLI `show` Commands Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add five CLI commands (`show-host`, `show-device`, `show-pool`, `show-device-set`, `show-allocation-group`) that look up one system-metadata entity by name or UUID and print its full content.

**Architecture:** Pure `private[cmdline]` formatter functions (state → String) rendered as multi-line `label: value` text, unit-tested in `MainSuite`; thin `show_*` wrappers in `Main.scala` handle client setup, argument resolution (UUID-or-name), one-level related-name enrichment (best-effort), error handling, and printing. Mirrors the existing `list_devices` / `formatBytes` patterns.

**Tech Stack:** Scala 3, scopt (CLI parsing), ScalaTest (`AnyFunSuite` + `Matchers`), sbt.

**Spec:** `docs/superpowers/specs/2026-07-23-cli-show-commands-design.md`

---

## File Structure

- **Modify** `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
  - New import: `AllocationGroupId`.
  - `Args`: add `entityRef: String = ""`.
  - New private helpers: `resolveRef`, `optName`, `EntityNotFound`.
  - Five pure formatters: `formatHostState`, `formatDeviceState`, `formatPoolState`, `formatDeviceSetState`, `formatAllocationGroupState`.
  - Five wrappers: `show_host`, `show_device`, `show_pool`, `show_device_set`, `show_allocation_group`.
  - Five `cmd(...)` parser blocks + five `case` dispatch lines.
- **Modify** `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`
  - Unit tests for `resolveRef` and all five formatters.

All new code lives in `Main`/`MainSuite`, following the established precedent that pure CLI helpers are `private[cmdline]` and tested directly (as `formatBytes` already is).

### Reference: exact constructors used in tests (verified from source)

```
HostState(hostId: HostId, name: String, address: String, dataPort: Int,
          cncPort: Int, storeTransferPort: Int, storageDevices: Set[StorageDeviceId])

StorageDeviceState(storageDeviceId: StorageDeviceId, hostId: HostId,
                   currentUsage: Long, totalSize: Long,
                   stores: Map[StoreId, StorageDeviceState.StoreEntry],
                   storageDeviceSet: StorageDeviceSetId)
StorageDeviceState.StoreEntry(status: StorageDeviceState.StoreStatus,
                              transferDevice: Option[StorageDeviceId])
// StoreStatus enum: Initializing, Active, TransferringIn, TransferringOut, Rebuilding

StoragePoolState(poolId: PoolId, name: String, ida: IDA, maxObjectSize: Option[Int],
                 stores: Array[StoragePoolState.StoreEntry], backendConfig: BackendConfig,
                 storageDeviceSet: StorageDeviceSetId, currentUsage: Long = 0,
                 maximumStoreSize: Long = 0, allocationGroups: List[UUID] = Nil)
StoragePoolState.StoreEntry(hostId: HostId, storageDeviceId: StorageDeviceId)

StorageDeviceSetState(setId: StorageDeviceSetId, name: String, level: Int,
                      parent: Option[StorageDeviceSetId],
                      memberDevices: List[StorageDeviceId], memberSets: List[StorageDeviceSetId],
                      assignedPools: List[PoolId],
                      pendingTransfers: List[(StoreId, StorageDeviceId, StorageDeviceId)] = Nil)

AllocationGroupState(groupId: AllocationGroupId, level: Int, name: String,
                     members: List[AllocationGroupState.Member],
                     parentGroups: List[AllocationGroupId])
AllocationGroupState.Member(memberType: AllocationGroupState.MemberType, uuid: UUID,
                            maxObjectSize: Option[Int], currentUsage: Long, maximumSize: Long)
// MemberType enum: Pool, Group
// AllocationGroupState.currentUsage / .maximumSize are computed (sum over members)
```

Client methods used by wrappers: `getHostId`, `getStoragePoolId`, `getAllocationGroupId`,
`getStorageDeviceSetId` (name→id) and `getHostState`, `getStorageDeviceState`,
`getStoragePoolState`, `getStorageDeviceSetState`, `getAllocationGroupState` (id→state).

---

## Task 1: `resolveRef` helper, `Args.entityRef`, and import

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

- [ ] **Step 1: Write the failing tests**

Add to `MainSuite.scala`. First ensure these imports are present at the top of the file (add any that are missing):

```scala
import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, SECONDS}
```

Then add:

```scala
  test("resolveRef parses a UUID string and dispatches to byUuid"):
    val u = UUID.fromString("3f1ca5f7-74be-405d-aa48-739fc23651a4")
    var nameCalled = false
    val f = Main.resolveRef[String](
      u.toString,
      uuid => s"uuid:$uuid",
      _ => { nameCalled = true; Future.successful("name") })
    Await.result(f, Duration(1, SECONDS)) shouldBe s"uuid:$u"
    nameCalled shouldBe false

  test("resolveRef falls back to byName for a non-UUID string"):
    val f = Main.resolveRef[String](
      "mypool",
      uuid => s"uuid:$uuid",
      n => Future.successful(s"name:$n"))
    Await.result(f, Duration(1, SECONDS)) shouldBe "name:mypool"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: FAIL — compilation error, `resolveRef` is not a member of `Main`.

- [ ] **Step 3: Add the import, the `Args` field, and `resolveRef`**

In `Main.scala`, add the import near the other `common` imports (after line 19, `import org.aspen_ddp.aspen.common.pool.PoolId`):

```scala
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
```

Add the new field to the `Args` case class (append after `parentSetName:String=""`, keeping the closing paren):

```scala
                  parentSetName:String="",
                  entityRef:String="")
```

Add `resolveRef` inside `object Main` (place it just above `formatBytes`, around line 1017):

```scala
  /** Resolve a user-supplied entity reference that may be either a UUID or a name.
   *  If `ref` parses as a UUID it is wrapped via `byUuid`; otherwise it is looked up
   *  by name via `byName`. */
  private[cmdline] def resolveRef[A](ref: String,
                                     byUuid: UUID => A,
                                     byName: String => Future[A]): Future[A] =
    try
      val uuid = UUID.fromString(ref)
      Future.successful(byUuid(uuid))
    catch
      case _: IllegalArgumentException => byName(ref)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: PASS (including the existing `formatBytes` tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala \
        src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala
git commit -m "Add resolveRef helper and entityRef arg for CLI show commands"
```

---

## Task 2: `formatHostState`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

- [ ] **Step 1: Write the failing test**

Add to `MainSuite.scala` (add `import org.aspen_ddp.aspen.common.metadata.*` at the top if not already present):

```scala
  test("formatHostState renders identity, ports, and device list"):
    val hostId = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
    val devId  = StorageDeviceId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val s = HostState(hostId, "node_a", "127.0.0.1", 4750, 4751, 4752, Set(devId))
    val out = Main.formatHostState(s)
    out should include ("Host: node_a")
    out should include ("11111111-1111-1111-1111-111111111111")
    out should include ("127.0.0.1")
    out should include ("4750")
    out should include ("22222222-2222-2222-2222-222222222222")

  test("formatHostState shows 'none' when there are no devices"):
    val hostId = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
    val s = HostState(hostId, "node_a", "127.0.0.1", 4750, 4751, 4752, Set.empty)
    Main.formatHostState(s) should include ("Storage Devices:     none")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: FAIL — `formatHostState` is not a member of `Main`.

- [ ] **Step 3: Implement `formatHostState`**

Add inside `object Main` (below `resolveRef`):

```scala
  private[cmdline] def formatHostState(s: HostState): String =
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Host: ${s.name}"
    lines += s"  UUID:                ${s.hostId.uuid}"
    lines += s"  Address:             ${s.address}"
    lines += s"  Data Port:           ${s.dataPort}"
    lines += s"  CnC Port:            ${s.cncPort}"
    lines += s"  Store Transfer Port: ${s.storeTransferPort}"
    if s.storageDevices.isEmpty then
      lines += "  Storage Devices:     none"
    else
      lines += "  Storage Devices:"
      s.storageDevices.toList.map(_.uuid.toString).sorted.foreach: d =>
        lines += s"    $d"
    lines.mkString("\n")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala \
        src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala
git commit -m "Add formatHostState formatter"
```

---

## Task 3: `formatDeviceState`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

- [ ] **Step 1: Write the failing test**

Add to `MainSuite.scala` (add `import org.aspen_ddp.aspen.common.store.StoreId` and `import org.aspen_ddp.aspen.common.pool.PoolId` at the top if not already present):

```scala
  test("formatDeviceState renders identity, resolved names, usage, and stores"):
    val devId  = StorageDeviceId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val hostId = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val storeId = StoreId(PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444")), 0.toByte)
    val stores = Map(storeId ->
      StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))
    val s = StorageDeviceState(devId, hostId, 512L, 2048L, stores, setId)
    val out = Main.formatDeviceState(s, Some("node_a"), Some("fast-nvme"))
    out should include ("Storage Device: 22222222-2222-2222-2222-222222222222")
    out should include ("node_a")
    out should include ("fast-nvme")
    out should include ("Active")
    out should include (storeId.toString)

  test("formatDeviceState falls back to raw UUIDs when names are None"):
    val devId  = StorageDeviceId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val hostId = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val s = StorageDeviceState(devId, hostId, 0L, 0L, Map.empty, setId)
    val out = Main.formatDeviceState(s, None, None)
    out should include ("11111111-1111-1111-1111-111111111111")
    out should include ("33333333-3333-3333-3333-333333333333")
    out should include ("Stores:     none")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: FAIL — `formatDeviceState` is not a member of `Main`.

- [ ] **Step 3: Implement `formatDeviceState`**

Add inside `object Main`:

```scala
  private[cmdline] def formatDeviceState(s: StorageDeviceState,
                                         hostName: Option[String],
                                         setName: Option[String]): String =
    val host = hostName.getOrElse(s.hostId.uuid.toString)
    val set  = setName.getOrElse(s.storageDeviceSet.uuid.toString)
    val pct  = if s.totalSize > 0 then s.currentUsage.toDouble / s.totalSize * 100.0 else 0.0
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Storage Device: ${s.storageDeviceId.uuid}"
    lines += s"  Host:       $host (${s.hostId.uuid})"
    lines += s"  Device Set: $set (${s.storageDeviceSet.uuid})"
    lines += f"  Usage:      ${formatBytes(s.currentUsage)} / ${formatBytes(s.totalSize)} ($pct%.1f%%)"
    if s.stores.isEmpty then
      lines += "  Stores:     none"
    else
      lines += "  Stores:"
      s.stores.toList.sortBy(_._1.toString).foreach: (storeId, entry) =>
        val xfer = entry.transferDevice.map(d => s" -> ${d.uuid}").getOrElse("")
        lines += s"    $storeId  ${entry.status}$xfer"
    lines.mkString("\n")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala \
        src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala
git commit -m "Add formatDeviceState formatter"
```

---

## Task 4: `formatPoolState`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

- [ ] **Step 1: Write the failing test**

Add to `MainSuite.scala` (add `import org.aspen_ddp.aspen.common.ida.Replication` and `import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig` at the top):

```scala
  test("formatPoolState renders identity, IDA, device set, and stores"):
    val poolId = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
    val hostId = HostId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
    val devId  = StorageDeviceId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val stores = Array(StoragePoolState.StoreEntry(hostId, devId))
    val s = StoragePoolState(poolId, "mypool", Replication(3, 2), None, stores,
      RocksDBConfig(), setId, currentUsage = 1024L, maximumStoreSize = 0L, allocationGroups = Nil)
    val out = Main.formatPoolState(s, Some("fast-nvme"))
    out should include ("Pool: mypool")
    out should include ("44444444-4444-4444-4444-444444444444")
    out should include ("Replication")
    out should include ("fast-nvme")
    out should include ("[0]")
    out should include ("22222222-2222-2222-2222-222222222222")

  test("formatPoolState falls back to set UUID and shows 'none' lists"):
    val poolId = PoolId(UUID.fromString("44444444-4444-4444-4444-444444444444"))
    val setId  = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val s = StoragePoolState(poolId, "mypool", Replication(3, 2), None, Array.empty,
      RocksDBConfig(), setId)
    val out = Main.formatPoolState(s, None)
    out should include ("33333333-3333-3333-3333-333333333333")
    out should include ("Alloc Groups: none")
    out should include ("Stores:       none")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: FAIL — `formatPoolState` is not a member of `Main`.

- [ ] **Step 3: Implement `formatPoolState`**

Add inside `object Main`:

```scala
  private[cmdline] def formatPoolState(s: StoragePoolState, setName: Option[String]): String =
    val set = setName.getOrElse(s.storageDeviceSet.uuid.toString)
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Pool: ${s.name}"
    lines += s"  UUID:         ${s.poolId.uuid}"
    lines += s"  IDA:          ${s.ida}"
    lines += s"  Max Obj Size: ${s.maxObjectSize.map(_.toString).getOrElse("unbounded")}"
    lines += s"  Device Set:   $set (${s.storageDeviceSet.uuid})"
    lines += s"  Usage:        ${formatBytes(s.currentUsage)}"
    lines += s"  Max Store Sz: ${if s.maximumStoreSize == 0 then "unbounded" else formatBytes(s.maximumStoreSize)}"
    if s.allocationGroups.isEmpty then
      lines += "  Alloc Groups: none"
    else
      lines += "  Alloc Groups:"
      s.allocationGroups.foreach: g =>
        lines += s"    $g"
    if s.stores.isEmpty then
      lines += "  Stores:       none"
    else
      lines += "  Stores:"
      s.stores.zipWithIndex.foreach: (entry, i) =>
        lines += s"    [$i] host ${entry.hostId.uuid}  device ${entry.storageDeviceId.uuid}"
    lines.mkString("\n")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala \
        src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala
git commit -m "Add formatPoolState formatter"
```

---

## Task 5: `formatDeviceSetState`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

- [ ] **Step 1: Write the failing test**

Add to `MainSuite.scala`:

```scala
  test("formatDeviceSetState renders identity, parent name, and members"):
    val setId    = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val parentId = StorageDeviceSetId(UUID.fromString("55555555-5555-5555-5555-555555555555"))
    val devId    = StorageDeviceId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val s = StorageDeviceSetState(setId, "fast-nvme", 0, Some(parentId),
      List(devId), Nil, Nil)
    val out = Main.formatDeviceSetState(s, Some("root-set"))
    out should include ("Device Set: fast-nvme")
    out should include ("33333333-3333-3333-3333-333333333333")
    out should include ("Level:    0")
    out should include ("root-set")
    out should include ("22222222-2222-2222-2222-222222222222")

  test("formatDeviceSetState shows 'none' parent and empty member lists"):
    val setId = StorageDeviceSetId(UUID.fromString("33333333-3333-3333-3333-333333333333"))
    val s = StorageDeviceSetState(setId, "root-set", 1, None, Nil, Nil, Nil)
    val out = Main.formatDeviceSetState(s, None)
    out should include ("Parent:   none")
    out should include ("Member Devices: none")
    out should include ("Member Sets:    none")
    out should include ("Assigned Pools: none")
    out should include ("Pending Transfers: 0")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: FAIL — `formatDeviceSetState` is not a member of `Main`.

- [ ] **Step 3: Implement `formatDeviceSetState`**

Add inside `object Main`:

```scala
  private[cmdline] def formatDeviceSetState(s: StorageDeviceSetState,
                                            parentName: Option[String]): String =
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Device Set: ${s.name}"
    lines += s"  UUID:     ${s.setId.uuid}"
    lines += s"  Level:    ${s.level}"
    val parent = s.parent match
      case None    => "none"
      case Some(p) => s"${parentName.getOrElse(p.uuid.toString)} (${p.uuid})"
    lines += s"  Parent:   $parent"
    if s.memberDevices.isEmpty then
      lines += "  Member Devices: none"
    else
      lines += "  Member Devices:"
      s.memberDevices.foreach(d => lines += s"    ${d.uuid}")
    if s.memberSets.isEmpty then
      lines += "  Member Sets:    none"
    else
      lines += "  Member Sets:"
      s.memberSets.foreach(m => lines += s"    ${m.uuid}")
    if s.assignedPools.isEmpty then
      lines += "  Assigned Pools: none"
    else
      lines += "  Assigned Pools:"
      s.assignedPools.foreach(p => lines += s"    ${p.uuid}")
    lines += s"  Pending Transfers: ${s.pendingTransfers.length}"
    lines.mkString("\n")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala \
        src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala
git commit -m "Add formatDeviceSetState formatter"
```

---

## Task 6: `formatAllocationGroupState`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

- [ ] **Step 1: Write the failing test**

Add to `MainSuite.scala` (add `import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId`):

```scala
  test("formatAllocationGroupState renders identity, usage, and members"):
    val groupId = AllocationGroupId(UUID.fromString("66666666-6666-6666-6666-666666666666"))
    val member = AllocationGroupState.Member(
      AllocationGroupState.MemberType.Pool,
      UUID.fromString("44444444-4444-4444-4444-444444444444"),
      None, 1024L, 4096L)
    val s = AllocationGroupState(groupId, 0, "gold", List(member), Nil)
    val out = Main.formatAllocationGroupState(s)
    out should include ("Allocation Group: gold")
    out should include ("66666666-6666-6666-6666-666666666666")
    out should include ("Level: 0")
    out should include ("Pool")
    out should include ("44444444-4444-4444-4444-444444444444")

  test("formatAllocationGroupState shows 'none' for empty members and parents"):
    val groupId = AllocationGroupId(UUID.fromString("66666666-6666-6666-6666-666666666666"))
    val s = AllocationGroupState(groupId, 1, "platinum", Nil, Nil)
    val out = Main.formatAllocationGroupState(s)
    out should include ("Members: none")
    out should include ("Parent Groups: none")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: FAIL — `formatAllocationGroupState` is not a member of `Main`.

- [ ] **Step 3: Implement `formatAllocationGroupState`**

Add inside `object Main`:

```scala
  private[cmdline] def formatAllocationGroupState(s: AllocationGroupState): String =
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Allocation Group: ${s.name}"
    lines += s"  UUID:  ${s.groupId.uuid}"
    lines += s"  Level: ${s.level}"
    lines += s"  Usage: ${formatBytes(s.currentUsage)} / ${formatBytes(s.maximumSize)}"
    if s.members.isEmpty then
      lines += "  Members: none"
    else
      lines += "  Members:"
      s.members.foreach: m =>
        lines += s"    ${m.memberType} ${m.uuid}  ${formatBytes(m.currentUsage)} / ${formatBytes(m.maximumSize)}"
    if s.parentGroups.isEmpty then
      lines += "  Parent Groups: none"
    else
      lines += "  Parent Groups:"
      s.parentGroups.foreach(p => lines += s"    ${p.uuid}")
    lines.mkString("\n")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala \
        src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala
git commit -m "Add formatAllocationGroupState formatter"
```

---

## Task 7: `show_*` wrappers, `optName`, and `EntityNotFound`

These wrappers perform live client I/O and cannot be unit-tested without a running
cluster; verification for this task is a successful `sbt compile`. Logic mirrors the
existing `list_devices` wrapper.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`

- [ ] **Step 1: Add the `EntityNotFound` sentinel**

Add next to the existing `HostNotFound` case class (around line 78):

```scala
  private case class EntityNotFound(kind: String, ref: String)
    extends Exception(s"$kind '$ref' not found")
```

- [ ] **Step 2: Add the `optName` helper**

Add inside `object Main` (near `resolveRef`):

```scala
  /** Resolve a related entity's display name, best-effort. Any failure (missing
   *  reference, read error) yields None so a `show` command still succeeds using the
   *  raw UUID as a fallback. */
  private def optName[A](f: Future[A])(name: A => String)
                        (using ExecutionContext): Future[Option[String]] =
    f.map(a => Some(name(a))).recover { case _ => None }
```

- [ ] **Step 3: Add the five wrapper defs**

Add inside `object Main` (place after `list_devices`, before `list_entries`):

```scala
  def show_host(bootstrapConfigFile: os.Path, ref: String): Unit =
    configureLogging()
    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)
    given ExecutionContext = client.clientContext

    val f =
      (for
        hostId    <- resolveRef(ref, HostId(_), client.getHostId)
        hostState <- client.getHostState(hostId)
      yield formatHostState(hostState)).recoverWith:
        case _: NoSuchElementException => Future.failed(EntityNotFound("host", ref))

    f.onComplete:
      case scala.util.Success(text)                => println(text)
      case scala.util.Failure(e: EntityNotFound)   => println(s"Error: ${e.getMessage}")
      case scala.util.Failure(err)                 => println(s"Error showing host: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

  def show_device(bootstrapConfigFile: os.Path, uuidStr: String): Unit =
    configureLogging()
    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)
    given ExecutionContext = client.clientContext

    val deviceId = StorageDeviceId(UUID.fromString(uuidStr))

    val f =
      (for
        dev      <- client.getStorageDeviceState(deviceId)
        hostName <- optName(client.getHostState(dev.hostId))(_.name)
        setName  <- optName(client.getStorageDeviceSetState(dev.storageDeviceSet))(_.name)
      yield formatDeviceState(dev, hostName, setName)).recoverWith:
        case _: NoSuchElementException => Future.failed(EntityNotFound("storage device", uuidStr))

    f.onComplete:
      case scala.util.Success(text)              => println(text)
      case scala.util.Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case scala.util.Failure(err)               => println(s"Error showing storage device: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

  def show_pool(bootstrapConfigFile: os.Path, ref: String): Unit =
    configureLogging()
    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)
    given ExecutionContext = client.clientContext

    val f =
      (for
        poolId  <- resolveRef(ref, PoolId(_), client.getStoragePoolId)
        pool    <- client.getStoragePoolState(poolId)
        setName <- optName(client.getStorageDeviceSetState(pool.storageDeviceSet))(_.name)
      yield formatPoolState(pool, setName)).recoverWith:
        case _: NoSuchElementException => Future.failed(EntityNotFound("pool", ref))

    f.onComplete:
      case scala.util.Success(text)              => println(text)
      case scala.util.Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case scala.util.Failure(err)               => println(s"Error showing pool: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

  def show_device_set(bootstrapConfigFile: os.Path, ref: String): Unit =
    configureLogging()
    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)
    given ExecutionContext = client.clientContext

    val f =
      (for
        setId <- resolveRef(ref, StorageDeviceSetId(_), client.getStorageDeviceSetId)
        set   <- client.getStorageDeviceSetState(setId)
        parentName <- set.parent match
          case None    => Future.successful(None)
          case Some(p) => optName(client.getStorageDeviceSetState(p))(_.name)
      yield formatDeviceSetState(set, parentName)).recoverWith:
        case _: NoSuchElementException => Future.failed(EntityNotFound("device set", ref))

    f.onComplete:
      case scala.util.Success(text)              => println(text)
      case scala.util.Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case scala.util.Failure(err)               => println(s"Error showing device set: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

  def show_allocation_group(bootstrapConfigFile: os.Path, ref: String): Unit =
    configureLogging()
    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)
    given ExecutionContext = client.clientContext

    val f =
      (for
        groupId <- resolveRef(ref, AllocationGroupId(_), client.getAllocationGroupId)
        group   <- client.getAllocationGroupState(groupId)
      yield formatAllocationGroupState(group)).recoverWith:
        case _: NoSuchElementException => Future.failed(EntityNotFound("allocation group", ref))

    f.onComplete:
      case scala.util.Success(text)              => println(text)
      case scala.util.Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case scala.util.Failure(err)               => println(s"Error showing allocation group: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))
```

- [ ] **Step 4: Verify it compiles**

Run: `sbt compile`
Expected: SUCCESS, no errors. (The new wrappers are not yet referenced by the parser — that is Task 8. Unused private methods do not fail compilation.)

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "Add show_* CLI wrappers with best-effort name resolution"
```

---

## Task 8: Parser commands and dispatch

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`

- [ ] **Step 1: Add the five `cmd(...)` blocks**

In the scopt parser, add these after the existing `list-devices` block (after the block
ending around line 327, before the `checkConfig(...)` line):

```scala
      cmd("show-host").text("Displays the full state of a host").
        action((_, c) => c.copy(mode = "show-host")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name-or-uuid>").text("Host name or UUID").
            action((x, c) => c.copy(entityRef = x)),
        )

      cmd("show-device").text("Displays the full state of a storage device").
        action((_, c) => c.copy(mode = "show-device")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<uuid>").text("Storage device UUID").
            action((x, c) => c.copy(entityRef = x)).
            validate { x =>
              try
                UUID.fromString(x)
                success
              catch
                case _: Throwable => failure("Storage device id must be a valid UUID")
            },
        )

      cmd("show-pool").text("Displays the full state of a storage pool").
        action((_, c) => c.copy(mode = "show-pool")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name-or-uuid>").text("Pool name or UUID").
            action((x, c) => c.copy(entityRef = x)),
        )

      cmd("show-device-set").text("Displays the full state of a storage device set").
        action((_, c) => c.copy(mode = "show-device-set")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name-or-uuid>").text("Device set name or UUID").
            action((x, c) => c.copy(entityRef = x)),
        )

      cmd("show-allocation-group").text("Displays the full state of an allocation group").
        action((_, c) => c.copy(mode = "show-allocation-group")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name-or-uuid>").text("Allocation group name or UUID").
            action((x, c) => c.copy(entityRef = x)),
        )
```

- [ ] **Step 2: Add the dispatch cases**

In the `cfg.mode match` block, add these lines after the existing
`case "list-devices" => ...` line (around line 357):

```scala
            case "show-host"              => show_host(bootstrapConfigPath, cfg.entityRef)
            case "show-device"            => show_device(bootstrapConfigPath, cfg.entityRef)
            case "show-pool"              => show_pool(bootstrapConfigPath, cfg.entityRef)
            case "show-device-set"        => show_device_set(bootstrapConfigPath, cfg.entityRef)
            case "show-allocation-group"  => show_allocation_group(bootstrapConfigPath, cfg.entityRef)
```

- [ ] **Step 3: Verify it compiles and existing tests pass**

Run: `sbt compile && sbt 'testOnly org.aspen_ddp.aspen.cmdline.MainSuite'`
Expected: compile SUCCESS; all `MainSuite` tests PASS.

- [ ] **Step 4: Manual smoke test (optional, requires a running system)**

With a bootstrapped/running demo cluster (see `TODO.txt` "Testing commands"), from the
project root:

```bash
./t show-host demo/bootstrap_config.yaml bootstrap-host
./t show-pool demo/bootstrap_config.yaml <pool-name-or-uuid>
./t show-device-set demo/bootstrap_config.yaml <set-name-or-uuid>
```

Expected: a multi-line block for the entity. An unknown name/uuid prints
`Error: <kind> '<ref>' not found`.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala
git commit -m "Wire show-* CLI commands into the argument parser"
```

---

## Self-Review Notes

- **Spec coverage:** all five commands (Tasks 2–8), UUID-or-name auto-detect (`resolveRef`, Task 1), UUID-only `show-device` (Task 8 validation + Task 7 wrapper), multi-line `label: value` output with `formatBytes` (Tasks 2–6), one-level best-effort name enrichment (`optName`, Task 7), distinct not-found messaging (`EntityNotFound`, Task 7), and testing via `MainSuite` (Tasks 1–6) are all covered.
- **Type consistency:** formatter signatures used in Task 7 wrappers exactly match those defined in Tasks 2–6; `entityRef` (Task 1) is consumed in Task 8; `EntityNotFound` and `optName` (Task 7) are used only within Task 7.
- **Out of scope (per spec):** no device naming, no YAML/JSON output, no deep enrichment, no changes to `AspenClient` or `common.metadata`.
