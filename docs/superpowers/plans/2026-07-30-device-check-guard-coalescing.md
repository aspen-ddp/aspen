# Device Check Guard Coalescing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `StoreManager` decide a device's loaded/unloaded branch when its state lookup completes rather than when it is dispatched, and make the `activeDeviceChecks` guard defer colliding check requests instead of discarding them.

**Architecture:** `checkStorageDevice` is split into a policy half (collision rule) and a mechanism half (`startDeviceCheck`: one lookup, one callback). The callback re-reads `storageDevices` under the instance lock it already holds, so the branch reflects the device's state at completion. A new `deferredDeviceChecks` set records requests that collided with an in-flight lookup, and the completing check re-dispatches once. A `protected def lookupStorageDeviceState` seam lets tests hold a lookup in flight across a device load.

**Tech Stack:** Scala 3, sbt, ScalaTest (`AsyncFunSuite` via `IntegrationTestSuite`), scribe logging.

**Spec:** `docs/superpowers/specs/2026-07-30-device-check-guard-coalescing-design.md`

---

## Background you need before starting

Read these before Task 1. They are short and the plan assumes them.

**`StoreManager` is a single-threaded event-loop actor.** `handleEvent` is wrapped in
`synchronized`, and every public reader of shared state takes the same instance lock. All the
mutable fields (`storageDevices`, `stores`, `offlineStores`, `activeDeviceChecks`) are guarded
by that one monitor.

**Future callbacks do NOT run on the event-loop thread.** They run on `ec`. The `.onComplete`
bodies in `checkStorageDevice` already open with `synchronized`, which is what makes a re-read
of `storageDevices` inside one safe — while the callback holds the lock, no `handleEvent` call
can be loading a device.

**`scala.concurrent.Future.foreach`/`onComplete` always dispatch through the
`ExecutionContext`.** They never run inline, even on an already-completed Future, because `ec`
is a real thread pool rather than `scala.concurrent.ExecutionContext.parasitic`. Several
correctness claims below depend on this.

**The bug.** `checkStorageDevice` branches on `storageDevices.get(storageDeviceId)` *before*
starting the lookup and builds one of two callbacks from that decision. Runtime storage device
discovery (2026-07-28) made a device able to go unloaded -> loaded while a lookup is
outstanding. When it does: (a) the event that loaded it calls `checkStorageDevice`, which the
guard discards outright, and (b) the in-flight callback runs the `None` body it was built with
and adds the now-loaded device's stores to `offlineStores`, re-adding ids that `tryLoadStore`
and the `LoadStore` handler just removed. Nothing removes them again.

---

## File Structure

Only two files change, plus `TODO.txt`.

| File | Responsibility | Change |
|---|---|---|
| `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` | The device check itself: seam, policy/mechanism split, re-read, deferral | Modify |
| `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala` | `RecordingStoreManager` arming seam and the four new tests | Modify |
| `TODO.txt` | Retire the fixed entry, amend the sync-throw entry, record two residuals | Modify |

`StoreManager.scala` is 1000 lines and does far more than device checking. Splitting it is out
of scope for this change and is not attempted.

## Test commands

```bash
sbt compile
sbt 'testOnly *StoreManagerDeviceDiscoverySuite'
sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "substring of test name"'
```

---

### Task 1: Add the storage-device-state lookup seam

Pure refactor. No behaviour change. This exists so later tasks can hold a lookup in flight.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` — add `lookupStorageDeviceState`, route `checkStorageDevice`'s two lookup calls through it (currently lines 756 and 769)

- [ ] **Step 1: Add the seam method**

Insert this immediately **before** `private def checkStorageDevice(storageDeviceId: StorageDeviceId): Unit =` (currently line 683):

```scala
  /** The storage-device-state read issued by a device check.
   *
   *  A seam rather than a direct client call so a test can hold a lookup in flight while the
   *  device it names is loaded underneath it. That interleaving is the only way to exercise
   *  the branch this check takes when it completes, and it cannot be produced through the real
   *  client, whose reads resolve on their own schedule.
   */
  protected def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    client.getStorageDeviceState(storageDeviceId)
```

No new imports are needed. `Future`, `Promise`, `StorageDeviceState` and `StorageDeviceId` are
all already imported at the top of the file.

- [ ] **Step 2: Route the two check lookups through the seam**

Inside `checkStorageDevice` there are exactly two calls to replace. Change:

```scala
            client.getStorageDeviceState(storageDeviceId).onComplete: result =>
```

to:

```scala
            lookupStorageDeviceState(storageDeviceId).onComplete: result =>
```

in **both** the `case Some(local) =>` branch (currently line 756) and the `case None =>` branch
(currently line 769).

**Do not touch the third call.** Inside the nested `check` function, the `TransferringIn`
branch calls `client.getStorageDeviceState(fromDeviceId)` (currently line 724). That reads a
*different* device — the transfer source — and must keep calling the client directly. A
blanket find-and-replace across the file is wrong.

- [ ] **Step 3: Verify nothing else changed behaviour**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite'`
Expected: PASS, all 13 existing tests. This is a refactor; a failure here means a call site was
mis-edited.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala
git commit -m "Add a seam for the storage device state lookup

checkStorageDevice's two lookups now go through a protected method. No
behaviour change; this is what lets a test hold a lookup in flight across
a device load."
```

---

### Task 2: Test infrastructure — offline-stores hook and lookup arming

Adds the observability and control a test needs. The test written here passes on current
behaviour; it characterises the seam so later red tests can be trusted.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` — add `testingOnlyOfflineStores` after `testingOnlyActiveDeviceChecks` (currently line 872)
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala` — extend `RecordingStoreManager`, add one test

- [ ] **Step 1: Add the offline-stores testing hook**

Insert immediately after the existing `testingOnlyActiveDeviceChecks` method:

```scala
  /** Testing hook: the stores currently marked offline. */
  private[aspen] def testingOnlyOfflineStores: Set[StoreId] =
    synchronized(offlineStores)
```

- [ ] **Step 2: Add the imports the test file will need**

In `StoreManagerDeviceDiscoverySuite.scala`, change:

```scala
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
```

to:

```scala
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState}
```

and change:

```scala
import scala.concurrent.{ExecutionContext, Future}
```

to:

```scala
import scala.concurrent.{ExecutionContext, Future, Promise}
```

- [ ] **Step 3: Add the arming seam to `RecordingStoreManager`**

Insert into the `RecordingStoreManager` body, after the `tryLoadStore` override and before
`def loadedDevices`:

```scala
  /** Storage device ids passed to lookupStorageDeviceState, in call order.
   *
   *  Lazy for the same reason as armedLookups below.
   */
  lazy val lookupAttempts: mutable.ListBuffer[StorageDeviceId] =
    mutable.ListBuffer[StorageDeviceId]()

  /** Promises queued by armLookup, consumed one per lookup of that device.
   *
   *  Lazy to match storeLoadAttempts. Unlike that field, nothing in StoreManager's constructor
   *  reaches this override today: the constructor's device scan calls tryLoadStore, but a
   *  device check only ever runs from handleEvent. The uniformity is deliberate insurance
   *  against that changing.
   */
  private lazy val armedLookups: mutable.Map[StorageDeviceId, mutable.Queue[Promise[StorageDeviceState]]] =
    mutable.Map[StorageDeviceId, mutable.Queue[Promise[StorageDeviceState]]]()

  /** Arms one lookup of `deviceId` to return a Future the test completes when it chooses.
   *
   *  Call once per lookup the test intends to control, in the order they will be issued.
   *  Lookups beyond the armed ones fall through to the real client, which is what keeps the
   *  tests that rely on a genuine lookup failure working unchanged.
   */
  def armLookup(deviceId: StorageDeviceId): Promise[StorageDeviceState] = synchronized:
    val p = Promise[StorageDeviceState]()
    armedLookups.getOrElseUpdate(deviceId, mutable.Queue[Promise[StorageDeviceState]]()).enqueue(p)
    p

  override protected def lookupStorageDeviceState(
      storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    val armed = synchronized:
      lookupAttempts += storageDeviceId
      armedLookups.get(storageDeviceId).filter(_.nonEmpty).map(_.dequeue())

    armed match
      case Some(p) => p.future
      case None    => super.lookupStorageDeviceState(storageDeviceId)
```

- [ ] **Step 4: Add the device-set id and state helper to the suite**

Insert into `StoreManagerDeviceDiscoverySuite`, immediately after the existing `private val
storeId = ...` declaration:

```scala
  private val deviceSetId = StorageDeviceSetId(UUID.fromString("55555555-5555-5555-5555-555555555555"))

  /** A StorageDeviceState for `deviceId` owned by this manager's host, carrying `stores`.
   *
   *  BootstrapHostId matches the manager's own hostId, which keeps check() off its
   *  host-migration branch. The sizes are arbitrary; nothing under test reads them.
   */
  private def deviceState(deviceId: StorageDeviceId,
                          stores: Map[StoreId, StorageDeviceState.StoreEntry] = Map()): StorageDeviceState =
    StorageDeviceState(deviceId, HostId.BootstrapHostId, 0L, 1024L, stores, deviceSetId)
```

- [ ] **Step 5: Write the characterisation test**

Append to `StoreManagerDeviceDiscoverySuite`:

```scala
  atest("an armed lookup holds the device check open until the test completes it"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    val p = mgr.armLookup(deviceA)

    mgr.testingOnlyCheckAllDevices()

    mgr.lookupAttempts.toList should be(List(deviceA))

    // Drain everything the check queued. A real client read of deviceA fails immediately --
    // deviceA is absent from the storage-devices tree -- so an unarmed check would have
    // released the guard by the time this wait exhausts. Still holding it is what proves the
    // armed promise, and not the client's read, is what the check is waiting on.
    yieldUntil(mgr.testingOnlyActiveDeviceChecks.isEmpty).flatMap: _ =>
      mgr.testingOnlyActiveDeviceChecks should be(Set(deviceA))

      p.failure(new RuntimeException("test-controlled lookup failure"))

      yieldUntil(mgr.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
        // yieldUntil gives up silently, so this is the assertion that turns an exhausted wait
        // into a failure rather than a pass.
        mgr.testingOnlyActiveDeviceChecks should be(empty)
```

**Why the intermediate drain.** Asserting the guard is held immediately after
`testingOnlyCheckAllDevices()` proves nothing: ScalaTest's async EC is single-threaded and is
the test's own thread, and `Future.onComplete` always dispatches through it even for an
already-completed Future. So no callback has run at that point regardless of which Future the
seam returned, and the assertion is unconditionally true. Yielding first is what makes it
discriminating. Verified by mutation — substituting `armLookup(deviceB)` makes this version
fail and the un-yielded version pass.

- [ ] **Step 6: Run the test**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "armed lookup holds the device check open"'`
Expected: PASS. This describes current behaviour; it is not a red test.

- [ ] **Step 7: Run the whole suite**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite'`
Expected: PASS, 14 tests. In particular "a device check that fails does not wedge later checks
of the same device" must still pass — it relies on the unarmed fall-through to `super` reaching
the real client and failing.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Let a test control when a device state lookup completes

Adds an offlineStores testing hook and a per-device queue of armed
promises in RecordingStoreManager. Unarmed lookups fall through to the
real client so the existing tests are unaffected."
```

---

### Task 3: Decide the loaded/unloaded branch at completion

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala` — add the regression test
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` — collapse the two branches in `checkStorageDevice` (currently lines 754-780)

- [ ] **Step 1: Write the failing test**

Append to `StoreManagerDeviceDiscoverySuite`:

```scala
  atest("a check started before its device loads does not mark the loaded device's stores offline"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    mgr.loadedDevices.keySet should be(empty)

    // Arm the lookup this CheckStorageDevice will issue, so it stays in flight while the
    // device loads underneath it.
    val p = mgr.armLookup(deviceA)

    mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    mgr.loadedDevices.keySet should be(empty)
    mgr.testingOnlyActiveDeviceChecks should be(Set(deviceA))

    // The config appears and a later event loads the device while the lookup is outstanding.
    // This is what runtime device discovery made possible and what the dispatch-time branch
    // could not account for.
    writeDevice(hostRoot, "dev0", deviceA)
    mgr.testingOnlyCheckAllDevices()
    mgr.loadedDevices.keySet should be(Set(deviceA))

    // An Active store makes check() a no-op in every one of its branches, so this pins branch
    // selection alone rather than dragging in store creation or transfers.
    p.success(deviceState(
      deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))

    yieldUntil(mgr.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
      // yieldUntil gives up silently, so this is the assertion that turns an exhausted wait into
      // a failure. It also proves the callback ran, without which the negative assertion below
      // would pass vacuously.
      mgr.testingOnlyActiveDeviceChecks should be(empty)

      // The device was loaded before the lookup returned, so its stores must not be marked
      // offline by a decision taken back when it was not. In production nothing would clear
      // them afterwards: tryLoadStore and the LoadStore handler both ran on the way in, and
      // check()'s own deleted-stores pass only removes ids recorded in the device's own
      // offlineStores set, which ids marked by this branch never enter.
      mgr.testingOnlyOfflineStores should not contain storeId
```

- [ ] **Step 2: Run it and watch it fail**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "does not mark the loaded device's stores offline"'`
Expected: FAIL on the final assertion — `Set(StoreId(...)) contained StoreId(...)`. The
dispatch-time `None` branch marked the loaded device's store offline.

- [ ] **Step 3: Collapse the two branches into one lookup**

In `checkStorageDevice`, replace the whole block from `storageDevices.get(storageDeviceId) match`
through the end of the `case None =>` callback (currently lines 754-780) with:

```scala
        lookupStorageDeviceState(storageDeviceId).onComplete: result =>
          synchronized:
            try
              result match
                case Success(remote) =>
                  // Read the device's load state here rather than before the lookup was
                  // issued. Runtime device discovery can load a device while its check is in
                  // flight, and a branch chosen at dispatch time would then mark a loaded
                  // device's stores offline -- ids that tryLoadStore and the LoadStore handler
                  // have just removed, and that nothing else removes again.
                  storageDevices.get(storageDeviceId) match
                    case Some(local) => check(local, remote)
                    case None =>
                      // Find out what stores are on the offline/failed store and add them to our
                      // offlineStores set. We don't want to send "UnknownStore" responses while
                      // the device is down
                      remote.stores.keysIterator.foreach: storeId =>
                        offlineStores += storeId

                case Failure(err) =>
                  val what =
                    if storageDevices.contains(storageDeviceId) then "storage device"
                    else "unloaded storage device"
                  logger.warn(s"Failed to read state for $what $storageDeviceId. It may not " +
                              s"be registered in the storage-devices tree. Error: $err")
            finally
              activeDeviceChecks -= storageDeviceId
```

Leave the `synchronized:` / `if ! activeDeviceChecks.contains(...)` / `activeDeviceChecks +=`
lines above it exactly as they are. The guard still drops colliding requests at this point;
Task 4 changes that.

Leave the large comment block above (currently lines 734-753) untouched for now. It is stale as
of this step and Task 5 rewrites it.

- [ ] **Step 4: Run the test and watch it pass**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "does not mark the loaded device's stores offline"'`
Expected: PASS

- [ ] **Step 5: Run the whole suite**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite'`
Expected: PASS, 15 tests.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Choose a device check's branch when its lookup completes

The branch was taken before the lookup was issued, so a device loaded
while the lookup was outstanding had its stores marked offline by a
decision made when it was not loaded. Nothing cleared them afterwards."
```

---

### Task 4: Defer colliding check requests instead of dropping them

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` — add `deferredDeviceChecks` and its hook, split `checkStorageDevice`, promote `check`
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala` — three new tests, one existing test extended

- [ ] **Step 1: Add the field and its testing hook**

This is observability only — the set stays empty until Step 4, so nothing changes yet.

Add the field immediately after the existing `activeDeviceChecks` declaration (currently line 117):

```scala
  private var deferredDeviceChecks: Set[StorageDeviceId] = Set()
```

Add the hook immediately after `testingOnlyOfflineStores`:

```scala
  /** Testing hook: devices with a check request deferred behind an in-flight lookup. */
  private[aspen] def testingOnlyDeferredDeviceChecks: Set[StorageDeviceId] =
    synchronized(deferredDeviceChecks)
```

- [ ] **Step 1b: Pin the unloaded branch that Task 3 rewrote**

Added after Task 3's spec review. That review mutation-tested the branch Task 3 collapsed and found
that replacing the unloaded arm with `case None => ()` — dropping the offline marking entirely —
survives the **entire 569-test repo suite**. Correctness-table row 1 ("unloaded at dispatch, still
unloaded, lookup succeeds → mark offline") has no coverage at all: `testingOnlyOfflineStores`
arrived only in Task 2 and Task 3's regression test is its sole consumer, asserting the *negative*.
The suite therefore pins the bug's absence but not the behaviour that has to survive, and a later
refactor could delete the `None` branch and stay green.

This is the positive counterpart of Task 3's test — same setup, device never loads, opposite
assertion. Append to `StoreManagerDeviceDiscoverySuite`:

```scala
  atest("a check for a device that never loads marks its stores offline"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    val p = mgr.armLookup(deviceA)

    mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    // Nothing was written under storage-devices/, so the check runs against a device this
    // manager has never loaded -- the case the offline marking exists for.
    mgr.loadedDevices.keySet should be(empty)

    p.success(deviceState(
      deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))

    yieldUntil(mgr.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      mgr.testingOnlyActiveDeviceChecks should be(empty)

      // Suppresses TxUnknownStore and ReadResponse(StoreNotFound) for stores on a device that
      // is down. Deleting this marking is silent in production and, until this test, silent in
      // the suite too.
      mgr.testingOnlyOfflineStores should contain(storeId)
```

Run it before making any Task 4 change: it must **pass** against `f29f063`. It is a
characterisation test of behaviour Task 3 already shipped, not a red-green pair. Then verify it
discriminates by applying the mutant it exists to kill — change the `case None =>` arm in
`checkStorageDevice` to `case None => ()`, confirm this test fails, and restore.

- [ ] **Step 2: Write the two failing tests**

Append to `StoreManagerDeviceDiscoverySuite`:

```scala
  atest("a check request arriving during an in-flight check is deferred, not dropped"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    val p1 = mgr.armLookup(deviceA)
    val p2 = mgr.armLookup(deviceA)

    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyActiveDeviceChecks should be(Set(deviceA))
    mgr.lookupAttempts.toList should be(List(deviceA))

    // Collides with the outstanding lookup. This is the create-storage-device nudge landing
    // during a periodic sweep, and dropping it costs a full checkStorageDevicePeriod.
    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyDeferredDeviceChecks should be(Set(deviceA))
    mgr.lookupAttempts.toList should be(List(deviceA))

    p1.success(deviceState(deviceA))

    yieldUntil(mgr.lookupAttempts.size == 2).map: _ =>
      mgr.lookupAttempts.toList should be(List(deviceA, deviceA))
      mgr.testingOnlyDeferredDeviceChecks should be(empty)
      p2.success(deviceState(deviceA))
      succeed

  atest("a deferred check still runs when the in-flight lookup fails"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    val p1 = mgr.armLookup(deviceA)
    val p2 = mgr.armLookup(deviceA)

    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyCheckAllDevices()
    mgr.testingOnlyDeferredDeviceChecks should be(Set(deviceA))

    // A failed lookup never reaches the completion-time re-read, so the deferral is the only
    // thing that rescues the request that collided with it.
    p1.failure(new RuntimeException("test-controlled lookup failure"))

    yieldUntil(mgr.lookupAttempts.size == 2).map: _ =>
      mgr.lookupAttempts.toList should be(List(deviceA, deviceA))
      mgr.testingOnlyDeferredDeviceChecks should be(empty)
      p2.failure(new RuntimeException("test-controlled lookup failure"))
      succeed
```

- [ ] **Step 3: Run them and watch them fail**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "is deferred, not dropped"'`
Expected: FAIL at `mgr.testingOnlyDeferredDeviceChecks should be(Set(deviceA))` — the set is
empty because the second request was discarded rather than recorded.

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "still runs when the in-flight lookup fails"'`
Expected: FAIL the same way.

- [ ] **Step 4: Promote `check` out of `checkStorageDevice`**

`startDeviceCheck` is about to need it, so it can no longer be a nested def. Move the whole
`def check(local: LocalStorageDeviceState, remote: StorageDeviceState): Unit = { ... }` body out
to a private method of the class, renamed — `check` says nothing at class scope:

```scala
  /** Reconciles a loaded device's on-disk state against the state recorded for it in the
   *  storage-devices tree: deletes stores transferred away, creates Initializing stores, and
   *  starts transfers in. Runs under the instance lock.
   */
  private def reconcileDeviceState(local: LocalStorageDeviceState,
                                   remote: StorageDeviceState): Unit =
```

Keep the body byte-for-byte as it is today, including its `updateHostId` host-migration branch
and its three commented sections. Only the signature line and the name change.

Update the single call site added in Task 3 from `check(local, remote)` to
`reconcileDeviceState(local, remote)`.

- [ ] **Step 5: Split policy from mechanism**

Replace what is left of `checkStorageDevice` — the `synchronized:` block with the guard test,
the guard insertion, and the callback from Task 3 — with these two methods.

**Carry the large comment block that sits above the callback over verbatim**, moving it inside
`startDeviceCheck` above the `lookupStorageDeviceState` call. It is stale by this point and
Task 5 rewrites it. Moving rather than deleting it means the reasoning it records — the only
account in the codebase of when the lookup fails and why a copied config is not one of those
cases — cannot be lost if Task 5 is interrupted.

```scala
  private def checkStorageDevice(storageDeviceId: StorageDeviceId): Unit =
    synchronized:
      if activeDeviceChecks.contains(storageDeviceId) then
        deferredDeviceChecks += storageDeviceId
      else
        startDeviceCheck(storageDeviceId)

  private def startDeviceCheck(storageDeviceId: StorageDeviceId): Unit =
    activeDeviceChecks += storageDeviceId

    lookupStorageDeviceState(storageDeviceId).onComplete: result =>
      synchronized:
        try
          result match
            case Success(remote) =>
              storageDevices.get(storageDeviceId) match
                case Some(local) => reconcileDeviceState(local, remote)
                case None =>
                  remote.stores.keysIterator.foreach: storeId =>
                    offlineStores += storeId

            case Failure(err) =>
              val what =
                if storageDevices.contains(storageDeviceId) then "storage device"
                else "unloaded storage device"
              logger.warn(s"Failed to read state for $what $storageDeviceId. It may not " +
                          s"be registered in the storage-devices tree. Error: $err")
        finally
          activeDeviceChecks -= storageDeviceId
          if deferredDeviceChecks.contains(storageDeviceId) then
            deferredDeviceChecks -= storageDeviceId
            startDeviceCheck(storageDeviceId)
```

Task 5 supplies the scaladoc for both. Leave them bare for this step.

Three properties to preserve exactly, because the tests do not all catch them:

1. The deferral flag is cleared **before** the re-dispatch. One deferral must buy exactly one
   re-check, or a device that keeps colliding never settles.
2. `startDeviceCheck` assumes the caller holds the instance lock and has established that no
   check is active. `checkStorageDevice` is the only caller that establishes it; the
   `finally` re-dispatch relies on the `activeDeviceChecks -=` line directly above it.
3. `activeDeviceChecks` keeps its `Set[StorageDeviceId]` type, so `testingOnlyActiveDeviceChecks`
   and the existing test asserting on it are untouched.

- [ ] **Step 6: Run the two new tests**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "is deferred, not dropped"'`
Expected: PASS

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "still runs when the in-flight lookup fails"'`
Expected: PASS

- [ ] **Step 7: Extend the Task 3 regression test to cover its deferral**

The device-loading event in that test now defers rather than being dropped, so it issues a
second lookup that would otherwise fall through to the real client mid-assertion. Replace the
whole `atest("a check started before its device loads does not mark the loaded device's stores offline")`
body with:

```scala
  atest("a check started before its device loads does not mark the loaded device's stores offline"):
    val hostRoot = newHostDir()
    val mgr = newManager(hostRoot)

    mgr.loadedDevices.keySet should be(empty)

    // Two arms: the lookup held in flight across the load, and the one the deferred request
    // issues once it completes.
    val p1 = mgr.armLookup(deviceA)
    val p2 = mgr.armLookup(deviceA)

    mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    mgr.loadedDevices.keySet should be(empty)
    mgr.testingOnlyActiveDeviceChecks should be(Set(deviceA))

    // The config appears and a later event loads the device while the lookup is outstanding.
    writeDevice(hostRoot, "dev0", deviceA)
    mgr.testingOnlyCheckAllDevices()
    mgr.loadedDevices.keySet should be(Set(deviceA))

    // That event's own check request collided with the outstanding lookup. It is the request
    // the guard used to discard outright, costing a full checkStorageDevicePeriod.
    mgr.testingOnlyDeferredDeviceChecks should be(Set(deviceA))

    // An Active store makes reconcileDeviceState a no-op in every one of its branches, so this
    // pins branch selection alone rather than dragging in store creation or transfers.
    p1.success(deviceState(
      deviceA,
      Map(storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None))))

    yieldUntil(mgr.lookupAttempts.size == 2).map: _ =>
      // yieldUntil gives up silently, so assert its condition first. This also proves the
      // first callback ran, without which the negative assertion below would pass vacuously.
      mgr.lookupAttempts.toList should be(List(deviceA, deviceA))
      mgr.testingOnlyDeferredDeviceChecks should be(empty)

      // The device was loaded before the lookup returned, so its stores must not be marked
      // offline by a decision taken back when it was not. In production nothing would clear
      // them afterwards: tryLoadStore and the LoadStore handler both ran on the way in, and
      // reconcileDeviceState's deleted-stores pass only removes ids recorded in the device's
      // own offlineStores set, which ids marked by this branch never enter.
      mgr.testingOnlyOfflineStores should not contain storeId

      p2.success(deviceState(deviceA))
      succeed
```

Note the assertion order: `lookupAttempts` first. It is the condition `yieldUntil` waited on, and
`yieldUntil` gives up silently — asserting the negative `offlineStores` claim first would let an
exhausted wait pass as a success.

- [ ] **Step 8: Run the whole suite**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite'`
Expected: PASS, 18 tests.

- [ ] **Step 9: Run the full test suite**

Run: `sbt test`
Expected: PASS. `StoreManagerSystemTaskSuite` and anything else driving a `StoreManager` through
`TestNetwork` exercises the new re-dispatch path; a failure there is a real regression, not
flakiness.

- [ ] **Step 10: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Defer colliding device checks rather than dropping them

The guard discarded every check request that arrived while a lookup was
in flight, costing up to checkStorageDevicePeriod -- an hour in Main --
for the create-storage-device nudge and for the request that follows a
device being discovered mid-check. It now records one deferral per device
and the completing check re-dispatches once."
```

---

### Task 5: Rewrite the stale comments

The comment block that documented the two-branch structure is now wrong in roughly half its
claims, and it is the only place some of its reasoning is recorded. Rewriting it is a
deliverable, not cleanup.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` — replace the old dispatch comment with scaladoc on `checkStorageDevice` and `startDeviceCheck`
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala` — scope the `RecordingStoreManager` vacuity warning

- [ ] **Step 1: Replace the carried-over block with scaladoc on the two methods**

Task 4 moved the old block — the `// onComplete, and a finally, in both branches below: ...`
comment, roughly 20 lines — inside `startDeviceCheck`. Delete it now. Its last two paragraphs
survive in the new scaladoc below, with `check` renamed and `getStorageDeviceState` replaced by
the seam; everything before them described the two-branch structure and is dropped.

Put this scaladoc on `checkStorageDevice`:

```scala
  /** Requests a check of one storage device.
   *
   *  At most one lookup per device is outstanding at a time. A request arriving while one is
   *  in flight is deferred rather than dropped: dropping it costs a full
   *  checkStorageDevicePeriod -- an hour as Main configures it -- which is the same cost as
   *  losing the notification message outright. At most one deferral is held per device, so
   *  this still throttles the pile-up of checks that builds up over an offline period.
   */
  private def checkStorageDevice(storageDeviceId: StorageDeviceId): Unit =
```

and this on `startDeviceCheck`:

```scala
  /** Issues the one outstanding lookup for a device and acts on the result.
   *
   *  Caller holds the instance lock and has established that no check is active for this
   *  device.
   *
   *  The loaded/unloaded branch is chosen when the lookup completes, not when it is issued.
   *  Runtime device discovery can load a device while its check is in flight, and a branch
   *  chosen at dispatch time would then mark a loaded device's stores offline -- re-adding ids
   *  that tryLoadStore and the LoadStore handler have just removed, which are the only two
   *  sites that remove them. The re-read is safe because this callback holds the same instance
   *  lock handleEvent does, so no device can be loading while it runs.
   *
   *  The entry must be released on both outcomes of the lookup and on a throw out of the
   *  callback body, hence the finally. Releasing only on success would skip every later check
   *  of that device for the life of the process. A synchronous throw from
   *  lookupStorageDeviceState, before the Future exists, would still leak it -- known, tracked
   *  in TODO.txt, and deliberately not guarded here.
   *
   *  Both lookup failure modes are reachable. The reconcile touches the filesystem and issues
   *  transactions, so it can throw. And the lookup fails whenever the device has no entry in
   *  the storage-devices tree: a config written out-of-band naming an id that was never
   *  registered -- the supported path cannot produce this, since
   *  StorageDeviceManager.createStorageDevice commits the registration before writing the
   *  config file, so its orphan is the reverse one, a registration with no directory (see the
   *  ConfigWriteFailed advice in the cmdline Main) -- or a tree entry removed after the fact,
   *  which no command does today. It also fails on any failure of the metadata read itself,
   *  transient or not, which is the only routinely reachable case.
   *
   *  A copied or moved config is NOT one of these: its device is registered, so the lookup
   *  succeeds. A config carried to another host then takes reconcileDeviceState's hostId
   *  mismatch branch, which is the designed host-migration path, not a warn.
   */
  private def startDeviceCheck(storageDeviceId: StorageDeviceId): Unit =
```

- [ ] **Step 2: Scope the vacuity warning in `RecordingStoreManager`'s scaladoc**

Its scaladoc currently ends:

```
 *  or the manager's `offlineStores`. Those sets therefore stay empty here, so a test asserting
 *  on them would pass vacuously — assert on `storeLoadAttempts` instead.
```

Replace those two lines with:

```
 *  or the manager's `offlineStores`. A test asserting that this override put something in them
 *  would therefore pass vacuously — assert on `storeLoadAttempts` instead. Assertions about
 *  what the *device check* writes to `offlineStores` are a different matter and are not
 *  vacuous: that write comes from startDeviceCheck's callback, which this override does not
 *  touch.
```

- [ ] **Step 3: Verify nothing broke**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite'`
Expected: PASS, 18 tests. Comments only, so any failure means code was deleted by accident.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala \
        src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Document the split device check and scope the suite's vacuity warning

The old dispatch comment described a two-branch structure that no longer
exists. Its account of when the lookup fails, and of why a copied config
is not one of those cases, is carried over -- it is recorded nowhere else."
```

---

### Task 6: Update TODO.txt

**Files:**
- Modify: `TODO.txt`

- [ ] **Step 1: Delete the fixed entry**

Remove the entry at the top of the file, from the line:

```
StoreManager.activeDeviceChecks is keyed by device id only, and load state can
```

through its last bullet:

```
  - Candidate fix: re-read storageDevices.get(storageDeviceId) inside the
    callback rather than branching on it before starting the lookup
```

- [ ] **Step 2: Amend the synchronous-throw entry**

Find the entry beginning `StoreManager.checkStorageDevice leaks its activeDeviceChecks guard on
a synchronous throw` and replace its body with:

```
StoreManager.startDeviceCheck leaks its activeDeviceChecks guard on a
synchronous throw
  - The entry is added before lookupStorageDeviceState is called. Both outcomes
    of the returned Future release it, and so does a throw out of the callback,
    but a throw from lookupStorageDeviceState itself (before the Future exists)
    leaves the device wedged for the life of the process
  - The finally also re-dispatches a deferred check, by calling startDeviceCheck
    again. So a synchronous throw there does double damage: it wedges the device
    and it replaces any exception already propagating out of the try body, which
    is how an exception from reconcileDeviceState would be lost
  - No longer blocked: lookupStorageDeviceState is the seam a test needs to make
    the call throw
```

- [ ] **Step 3: Add the two residuals**

Append to `TODO.txt`:

```
StoreManager.reconcileDeviceState can retry the host-migration path forever
  - On a hostId mismatch it calls updateHostId and then checkStorageDevice
    again. If the remote hostId never converges, that reads a still-mismatched
    state and repeats, with no backoff and no attempt counter
  - activeDeviceChecks is no brake on it. The recursion is dispatched through
    the ExecutionContext by Future.foreach, so it always runs after the callback
    released the guard
  - Pre-existing and unaffected by the deferral work; recorded because it was
    found while tracing that code

StoreManager.offlineStores entries for a device that never appears on disk are
never cleared
  - startDeviceCheck's unloaded branch marks every store the storage-devices
    tree lists for the device. tryLoadStore and the LoadStore handler are the
    only removers, and both require the store to actually load
  - A device registered in the tree whose directory never shows up therefore
    suppresses TxUnknownStore and ReadError.StoreNotFound for its stores for the
    life of the process. Peers get silence where they should get an answer
```

- [ ] **Step 4: Commit**

```bash
git add TODO.txt
git commit -m "Retire the device check guard entry and record what it left behind

The top entry is fixed. The synchronous-throw entry gains the exception
masking the finally's re-dispatch introduces, and is no longer blocked on
a test seam. Two findings from tracing the code are written down."
```

---

## Definition of done

- [ ] `sbt compile` clean
- [ ] `sbt test` passes
- [ ] `StoreManagerDeviceDiscoverySuite` has 18 tests, all passing
- [ ] `checkStorageDevice` contains no `storageDevices.get` call outside the lookup callback
- [ ] `client.getStorageDeviceState` appears exactly twice in `StoreManager.scala`: once inside
      `lookupStorageDeviceState`, once inside `reconcileDeviceState`'s `TransferringIn` branch
      for the transfer source device
- [ ] `TODO.txt` no longer contains the `activeDeviceChecks is keyed by device id only` entry
