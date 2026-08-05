# Device-Check Synchronous Throw Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Release `StoreManager.startDeviceCheck`'s `activeDeviceChecks` guard when `lookupStorageDeviceState` throws synchronously, so a throwing lookup no longer wedges the device for the life of the process, and so the `finally`'s re-dispatch can no longer swallow an exception unwinding from `reconcileDeviceState`.

**Architecture:** The `finally` body moves into a nested `def endDeviceCheck()` that closes over `storageDeviceId`. The call to `lookupStorageDeviceState` is wrapped in a `try`/`catch` that converts a synchronous throw into a `Failure`, which logs a distinct warning and calls `endDeviceCheck()`. That makes `startDeviceCheck` total — it has no synchronous throw path left — which is what makes the `finally`'s recursive re-dispatch safe.

**Tech Stack:** Scala 3, ScalaTest `AsyncFunSuite`, sbt, os-lib.

**Spec:** `docs/superpowers/specs/2026-08-05-device-check-synchronous-throw-design.md`

---

## File Structure

| File | Change |
|---|---|
| `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` | Modify: `storageDevices` invariant comment (~108-110), `lookupStorageDeviceState` scaladoc (~687-698), `startDeviceCheck` scaladoc and body (~774-866) |
| `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala` | Modify: `RecordingStoreManager` seam (~103-135), add `RecordingExecutionContext`, add `newManager` EC parameter (~217-220), add two tests at end of file |
| `TODO.txt` | Modify: delete the top entry (lines 2-14) |

Everything lives in files that already exist. No new files.

---

## Ordering Note

Tasks 2 and 3 write tests that must fail against the **unmodified** `startDeviceCheck`. Task 4 applies the production change and turns them green. Do not reorder: watching each test fail for the specific documented reason is what proves it tests the thing it claims to.

---

### Task 1: Extend the test seam to arm a synchronous throw

No behaviour change and no new test — this is the scaffolding Tasks 2 and 3 need. `armLookup` keeps its signature so its eight existing call sites are untouched.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala:103-135`

- [ ] **Step 1: Replace the armedLookups field and the override**

Find this block (lines 103-135, ending just before `class StoreManagerDeviceDiscoverySuite`):

```scala
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

  def loadedDevices: Map[StorageDeviceId, StoreManager.LocalStorageDeviceState] =
    synchronized(storageDevices)
```

Replace it with:

```scala
  /** One armed lookup: `Right` returns a Future the test completes, `Left` throws before any
   *  Future exists. */
  private type ArmedLookup = Either[Throwable, Promise[StorageDeviceState]]

  /** Lookups queued by armLookup and armLookupThrow, consumed one per lookup of that device.
   *
   *  One queue per device rather than a queue of promises plus a separate set of throws, so the
   *  order in which a test arms a success and a throw is the order the device check sees them.
   *
   *  Lazy to match storeLoadAttempts. Unlike that field, nothing in StoreManager's constructor
   *  reaches this override today: the constructor's device scan calls tryLoadStore, but a
   *  device check only ever runs from handleEvent. The uniformity is deliberate insurance
   *  against that changing.
   */
  private lazy val armedLookups: mutable.Map[StorageDeviceId, mutable.Queue[ArmedLookup]] =
    mutable.Map[StorageDeviceId, mutable.Queue[ArmedLookup]]()

  /** Caller holds this instance's lock. Write path only -- the read path in
   *  lookupStorageDeviceState must go through `armedLookups` directly, since getOrElseUpdate
   *  there would accumulate an empty queue per device looked up.
   */
  private def enqueueArmed(deviceId: StorageDeviceId, armed: ArmedLookup): Unit =
    armedLookups.getOrElseUpdate(deviceId, mutable.Queue[ArmedLookup]()).enqueue(armed)

  /** Arms one lookup of `deviceId` to return a Future the test completes when it chooses.
   *
   *  Call once per lookup the test intends to control, in the order they will be issued.
   *  Lookups beyond the armed ones fall through to the real client, which is what keeps the
   *  tests that rely on a genuine lookup failure working unchanged.
   */
  def armLookup(deviceId: StorageDeviceId): Promise[StorageDeviceState] = synchronized:
    val p = Promise[StorageDeviceState]()
    enqueueArmed(deviceId, Right(p))
    p

  /** Arms one lookup of `deviceId` to throw `error` instead of returning a Future at all.
   *
   *  This is the case no Promise can stage: a failed Promise still yields a Future, and it is
   *  the absence of the Future -- and so of the callback, and so of the callback's finally --
   *  that leaks startDeviceCheck's guard entry.
   */
  def armLookupThrow(deviceId: StorageDeviceId, error: Throwable): Unit = synchronized:
    enqueueArmed(deviceId, Left(error))

  override protected def lookupStorageDeviceState(
      storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
    val armed = synchronized:
      lookupAttempts += storageDeviceId
      armedLookups.get(storageDeviceId).filter(_.nonEmpty).map(_.dequeue())

    armed match
      case Some(Right(p))    => p.future
      case Some(Left(error)) => throw error
      case None              => super.lookupStorageDeviceState(storageDeviceId)

  def loadedDevices: Map[StorageDeviceId, StoreManager.LocalStorageDeviceState] =
    synchronized(storageDevices)

  /** Installs `sds` in the protected device map without it ever having been on disk.
   *
   *  Test 2 needs a LocalStorageDeviceState carrying values the real scan cannot produce -- a
   *  relative devicePath -- so it cannot go through writeDevice and a rescan.
   *
   *  Named for what it does to the load state, not the storage-devices tree: this registers
   *  nothing there, and a lookup of the injected device still resolves however the test arms
   *  it. Refuses to replace an existing entry, so a test that both writes a device to disk and
   *  injects one for the same id fails here rather than in a confusing assertion later.
   */
  def injectLoadedDevice(sds: StoreManager.LocalStorageDeviceState): Unit = synchronized:
    require(!storageDevices.contains(sds.storageDeviceId),
            s"${sds.storageDeviceId} is already loaded; injectLoadedDevice does not replace")
    storageDevices += (sds.storageDeviceId -> sds)
```

Note that the recorded `lookupAttempts += storageDeviceId` happens *before* the dequeue, so an armed throw is still recorded as an attempt. Tests 1 and 2 both depend on that.

`injectLoadedDevice` is deliberately not called `registerDevice`: everywhere else in `StoreManager`, *registration* means the storage-devices tree entry ("It may not be registered in the storage-devices tree", "a registration with no directory"), and this method does the opposite half.

- [ ] **Step 2: Add the recording ExecutionContext**

Test 2 needs to observe which exception escaped the `onComplete` callback. Scala's `Future.onComplete` wraps the callback so that a `NonFatal` throw out of it is passed to `executor.reportFailure`, and the executor is the `ExecutionContext` the `StoreManager` was constructed with.

Insert this immediately after the `RecordingStoreManager` class (after the `injectLoadedDevice` method added in Step 1, before `class StoreManagerDeviceDiscoverySuite`):

```scala
/** Delegates execution to `underlying`, and records every reportFailure call instead of
 *  passing it on.
 *
 *  A NonFatal throw out of a Future callback goes to the ExecutionContext's reportFailure and
 *  nowhere else, so this is the only way for a test to see which exception escaped -- which is
 *  exactly the question when a finally can replace one exception with another. (A fatal
 *  Throwable is rethrown instead, so it never lands here.)
 *
 *  Recording rather than forwarding means the underlying context never prints the throwable.
 *  That is deliberate: a test whose passing state involves an expected throw should not print a
 *  stack trace on every green run. The assertions on `failures` carry the diagnostic instead.
 */
private class RecordingExecutionContext(underlying: ExecutionContext) extends ExecutionContext:
  private val recorded = mutable.ListBuffer[Throwable]()

  def execute(runnable: Runnable): Unit = underlying.execute(runnable)

  def reportFailure(cause: Throwable): Unit = synchronized:
    recorded += cause

  def failures: List[Throwable] = synchronized(recorded.toList)
```

- [ ] **Step 3: Give newManager an ExecutionContext parameter**

Find (lines 217-220):

```scala
  private def newManager(hostRoot: Path, failFirstStoreLoad: Boolean = false): RecordingStoreManager =
    new RecordingStoreManager(client, systemId, hostRoot, executionContext,
                              net.objectCacheFactory, net, net.FinalizerFactory,
                              failFirstStoreLoad)
```

Replace with:

```scala
  private def newManager(hostRoot: Path,
                         failFirstStoreLoad: Boolean = false,
                         ec: ExecutionContext = executionContext): RecordingStoreManager =
    new RecordingStoreManager(client, systemId, hostRoot, ec,
                              net.objectCacheFactory, net, net.FinalizerFactory,
                              failFirstStoreLoad)
```

The default keeps all eighteen existing call sites working unchanged.

- [ ] **Step 4: Add the Paths import**

Find line 16:

```scala
import java.nio.file.{Files, Path}
```

Replace with:

```scala
import java.nio.file.{Files, Path, Paths}
```

`ExecutionContext` and `mutable` are already imported (lines 19-20).

- [ ] **Step 5: Compile and run the suite to confirm nothing regressed**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite'`
Expected: compiles clean, all 18 existing tests PASS. This task adds no test and changes no behaviour; a failure here means the seam refactor broke something.

- [ ] **Step 6: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Let the recording seam arm a synchronous lookup throw

Adds armLookupThrow, a recording ExecutionContext, and injectLoadedDevice.
No test uses them yet.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: Failing test — a synchronous throw releases the guard

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala` (append at end of file, after the `"a deferred check still runs when the in-flight lookup fails"` test)

- [ ] **Step 1: Write the failing test**

Append to the end of the file, at the same two-space indentation as the other `atest` blocks:

```scala
  atest("a lookup that throws synchronously does not wedge the device"):
    val hostRoot = newHostDir()
    writeDevice(hostRoot, "dev0", deviceA)

    val mgr = newManager(hostRoot)
    mgr.loadedDevices.keySet should be(Set(deviceA))

    // A failed Promise cannot stage this: it still yields a Future, so the callback and its
    // finally still run. Only a throw before the Future exists skips them, and the guard entry
    // is added before the lookup call.
    val boom = new RuntimeException("test-controlled synchronous lookup failure")
    mgr.armLookupThrow(deviceA, boom)

    // Treated as a failed lookup, not propagated. testingOnlyHandleHostMessage calls handleEvent
    // directly rather than going through the event loop, so start()'s catch-all is not in the
    // way and a propagating throw would surface right here.
    noException should be thrownBy mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    mgr.lookupAttempts.toList should be(List(deviceA))

    // Release is synchronous on this path -- there is no callback to wait for -- so no
    // yieldUntil.
    mgr.testingOnlyActiveDeviceChecks should be(empty)
    mgr.testingOnlyDeferredDeviceChecks should be(empty)

    // The assertion that distinguishes released from wedged. A leaked entry makes every later
    // check of this device a no-op: checkStorageDevice records a deferral instead of starting a
    // lookup, and the only thing that would clear that deferral is the finally that never ran.
    val p = mgr.armLookup(deviceA)
    mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))

    mgr.lookupAttempts.toList should be(List(deviceA, deviceA))
    mgr.testingOnlyActiveDeviceChecks should be(Set(deviceA))

    // An empty store map makes reconcileDeviceState a no-op in every branch, so the check
    // finishes cleanly rather than leaving the suite's teardown to tidy up after it.
    p.success(deviceState(deviceA))

    yieldUntil(mgr.testingOnlyActiveDeviceChecks.isEmpty).map: _ =>
      // yieldUntil gives up silently, so assert the condition it waited on.
      mgr.testingOnlyActiveDeviceChecks should be(empty)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "throws synchronously"'`

Expected: FAIL. Against the unmodified `startDeviceCheck` the very first assertion goes first — `noException should be thrownBy ...` fails, reporting the `RuntimeException("test-controlled synchronous lookup failure")` propagating out of `handleEvent`.

If instead you see a failure on `testingOnlyActiveDeviceChecks should be(empty)`, the throw is being absorbed somewhere unexpected; stop and investigate before continuing.

- [ ] **Step 3: Commit the failing test**

```bash
git add src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Add a failing test for the synchronous lookup throw

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: Failing test — a callback-body throw is not masked

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala` (append at end of file)

- [ ] **Step 1: Understand the lever (no action)**

This test's lever is `os.Path(local.devicePath)` at `StoreManager.scala:723`, which sits *above* the `try`/`catch` that wraps `os.remove.all` at 726, so a throw there escapes `reconcileDeviceState`.

That it throws is verified, not assumed. os-lib 0.11.5's `Path.scala:574` is:

```scala
require(wrapped.isAbsolute || Path.driveRelative(wrapped), s"$wrapped is not an absolute path")
```

So a relative path yields `IllegalArgumentException("requirement failed: relative-device-dir is not an absolute path")`. Step 2's assertions pin the `is not an absolute path` fragment rather than the exception class.

If that assertion turns out to fail — an os-lib upgrade, say — the fallback is the spec's protected-seam option: make `reconcileDeviceState` `protected` and have `RecordingStoreManager` override it to throw. Everything else in this task is unchanged.

- [ ] **Step 2: Write the failing test**

Append to the end of the file:

```scala
  atest("a throw out of the check callback is not replaced by the re-dispatch's own throw"):
    val hostRoot = newHostDir()
    val recordingEc = new RecordingExecutionContext(executionContext)
    val mgr = newManager(hostRoot, ec = recordingEc)

    // A device the scan could not produce: devicePath is relative, so reconcileDeviceState's
    // deleted-stores pass throws at os.Path(local.devicePath) -- the one line on that path
    // above the try/catch wrapping os.remove.all. offlineStores holds the id that makes the
    // pass iterate at all; the armed remote state below omits it, which is what marks it
    // deleted.
    val local = new StoreManager.LocalStorageDeviceState(
      deviceA, Paths.get("relative-device-dir"), hostRoot.toFile)
    local.offlineStores = Set(storeId)
    mgr.injectLoadedDevice(local)

    val p1 = mgr.armLookup(deviceA)

    // The lookup the re-dispatch issues, armed to throw. Under the old code this throw escapes
    // the finally and replaces the reconcile's, which is the defect under test.
    val redispatchError = new RuntimeException("test-controlled re-dispatch lookup failure")
    mgr.armLookupThrow(deviceA, redispatchError)

    mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))
    mgr.testingOnlyActiveDeviceChecks should be(Set(deviceA))

    // Collides with the outstanding lookup, so the finally has a deferral to re-dispatch.
    mgr.testingOnlyHandleHostMessage(
      CheckStorageDevice(HostId.BootstrapHostId, client.clientId, deviceA))
    mgr.testingOnlyDeferredDeviceChecks should be(Set(deviceA))

    // Matching hostId keeps reconcileDeviceState off its host-migration branch; no stores means
    // storeId counts as deleted and the pass runs.
    p1.success(deviceState(deviceA))

    yieldUntil(mgr.lookupAttempts.size == 2).map: _ =>
      // yieldUntil gives up silently, so assert its condition first. It also proves the
      // re-dispatch happened, without which the assertions below would pass vacuously.
      mgr.lookupAttempts.toList should be(List(deviceA, deviceA))

      // The finally still released, even though the try body threw.
      mgr.testingOnlyActiveDeviceChecks should be(empty)
      mgr.testingOnlyDeferredDeviceChecks should be(empty)

      // The discriminating pair. Old code records exactly one failure and it is
      // redispatchError, because the throw out of the finally replaced the reconcile's. New
      // code absorbs redispatchError inside the re-dispatch and lets the reconcile's escape,
      // so the one recorded failure is os-lib's.
      //
      // Size before content, because this recorder sees every callback the manager runs
      // through its ExecutionContext: a bare non-empty check would also pass on an unrelated
      // failure with the reconcile silently not throwing at all. Matched on the message
      // fragment rather than IllegalArgumentException, which is too common a class to pin
      // anything to.
      recordingEc.failures should have size 1
      recordingEc.failures.head.getMessage should include("is not an absolute path")
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "not replaced by"'`

Expected: FAIL on `mgr.testingOnlyActiveDeviceChecks should be(empty)` — against the unmodified code the re-dispatch's throw leaves `deviceA` in the guard set.

The message assertion is also wrong against old code — it records `redispatchError`, whose message is `test-controlled re-dispatch lookup failure` — but it is not reached, because the guard assertion fails first. That is fine; Task 4 Step 3 is where it is confirmed to pass.

If the failure is instead `recordingEc.failures should have size 1` reporting size 0, with the guard assertions passing, the reconcile did not throw. Re-read Step 1 and take the fallback.

- [ ] **Step 4: Commit the failing test**

```bash
git add src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala
git commit -m "Add a failing test for the masked callback-body throw

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: Release the guard on a synchronous throw

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:809-866`

- [ ] **Step 1: Replace the closing paragraph of startDeviceCheck's scaladoc**

Find (lines 809-817, the last paragraph of the scaladoc plus its closing `*/`):

```scala
   *  The entry must be released on both outcomes of the lookup and on a throw out of the
   *  callback body, hence the finally. That last is not hypothetical: the reconcile touches
   *  the filesystem and issues transactions, so it can throw. It is argued rather than
   *  covered, though -- no test drives a throw out of the callback body. Releasing only on
   *  success would skip every later check of that device for the life of the process. A
   *  synchronous throw from lookupStorageDeviceState, before the Future exists, would still
   *  leak it -- known, tracked in TODO.txt, and deliberately not guarded here -- as would an
   *  ExecutionContext that never runs the callback.
   */
```

Replace with:

```scala
   *  The entry must be released on four exit paths: both outcomes of the lookup, a throw out of
   *  the callback body, and a throw from lookupStorageDeviceState itself. The first three are
   *  the finally. The fourth cannot be -- a throw before the Future exists means no callback is
   *  ever registered and so no finally ever runs -- hence the wrapper around the lookup call,
   *  which treats such a throw as a failed lookup. Releasing on fewer than all four would skip
   *  every later check of that device for the life of the process.
   *
   *  The callback-body path is not hypothetical: the reconcile touches the filesystem and
   *  issues transactions, so it can throw. It and the synchronous throw are both covered by
   *  StoreManagerDeviceDiscoverySuite.
   *
   *  Treating a synchronous throw as a failed lookup rather than letting it propagate leaves
   *  this method with no synchronous throw path at all, and that is what makes the finally's
   *  re-dispatch safe: a call that cannot throw cannot replace an exception already unwinding
   *  out of the try body, which is how a reconcileDeviceState failure used to be lost.
   *
   *  The wrapper is scoped to the lookup call and not to the onComplete registration. A wider
   *  scope would, under an inline or parasitic ExecutionContext, also catch a throw coming back
   *  out of the callback body, reinstating the swallowing just described. What the narrow scope
   *  gives up is an ExecutionContext that rejects the submission or accepts it and never runs
   *  the callback: either still leaks the entry, and neither is guarded here.
   */
```

- [ ] **Step 2: Replace the body of startDeviceCheck**

Delete the whole method — every line from `private def startDeviceCheck` (818) through the
`startDeviceCheck(storageDeviceId)` that closes the `finally` (866), stopping before the blank
line and `def containsStore` that follow. Replace it with:

The two `case` arms inside the `try` are carried over **unchanged**, comments and all; they are
reproduced in full below so this is a straight replacement rather than a surgical edit. The
only differences from the original are four spaces of added indentation and the new outer
`match`.

```scala
  private def startDeviceCheck(storageDeviceId: StorageDeviceId): Unit =
    // Releases the guard and re-dispatches whatever was deferred behind it. Called from the
    // callback's finally and from the synchronous-throw path below; both hold the instance
    // lock. Nested rather than a method on the class because it has no other caller, and
    // because closing over storageDeviceId leaves the two call sites no way to disagree about
    // which device they are releasing.
    //
    // The deferral flag is cleared before the re-dispatch, not after. No test can tell the two
    // apart, because onComplete never runs inline on the ExecutionContexts used today, so the
    // nested callback cannot re-enter this while the flag is still set. Under an inline or
    // parasitic EC the other order recurses without bound.
    def endDeviceCheck(): Unit =
      activeDeviceChecks -= storageDeviceId
      if deferredDeviceChecks.contains(storageDeviceId) then
        deferredDeviceChecks -= storageDeviceId
        startDeviceCheck(storageDeviceId)

    activeDeviceChecks += storageDeviceId

    // Deliberately around the lookup call alone and not the onComplete registration below; the
    // scaladoc has why.
    val lookup =
      try Success(lookupStorageDeviceState(storageDeviceId))
      catch case t: Throwable => Failure(t)

    lookup match
      case Success(pendingState) =>
        pendingState.onComplete: result =>
          synchronized:
            try
              result match
                case Success(remote) =>
                  // Load state re-read here, not at dispatch. See startDeviceCheck's scaladoc.
                  storageDevices.get(storageDeviceId) match
                    case Some(local) => reconcileDeviceState(local, remote)
                    case None =>
                      // A device that has not appeared on disk in this process -- nothing ever
                      // unloads one. Silence its stores rather than sending "UnknownStore".
                      remote.stores.keysIterator.foreach: storeId =>
                        offlineStores += storeId

                // The lookup fails whenever the device has no entry in the storage-devices tree:
                // a config written out-of-band naming an id that was never registered -- the
                // supported path cannot produce this, since
                // StorageDeviceManager.createStorageDevice commits the registration before
                // writing the config file, so its orphan is the reverse one, a registration with
                // no directory (see the ConfigWriteFailed advice in the cmdline Main) -- or a tree
                // entry removed after the fact, which no command does today. It also fails on any
                // failure of the metadata read itself, transient or not, which is the only
                // routinely reachable case.
                //
                // A copied or moved config is NOT one of these: its device is registered, so the
                // lookup succeeds. A config carried to another host then takes
                // reconcileDeviceState's hostId mismatch branch, which is the designed
                // host-migration path, not a warn.
                case Failure(err) =>
                  val what =
                    if storageDevices.contains(storageDeviceId) then "storage device"
                    else "never-loaded storage device"
                  logger.warn(s"Failed to read state for $what $storageDeviceId. It may not " +
                              s"be registered in the storage-devices tree. Error: $err")
            finally endDeviceCheck()

      // A distinct message from the Failure(err) branch above, which explains itself with a
      // missing tree registration. True of a failed read; false of a call that never reached
      // the tree.
      case Failure(err) =>
        logger.warn(s"Lookup of state for storage device $storageDeviceId threw before the " +
                    s"read was dispatched. Error: $err")
        endDeviceCheck()
```

`Success` and `Failure` are already imported at line 39. `scala.util.Try` is deliberately **not** imported — `val lookup` infers `Try[Future[StorageDeviceState]]` from the two branches without it.

- [ ] **Step 3: Run both new tests**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "throws synchronously"'`
Expected: PASS

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite -- -z "not replaced by"'`
Expected: PASS — including both `recordingEc.failures` assertions, which Task 3 Step 3 never reached.

- [ ] **Step 4: Run the whole suite**

Run: `sbt 'testOnly *StoreManagerDeviceDiscoverySuite'`
Expected: all 20 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala
git commit -m "Release the device-check guard on a synchronous lookup throw

The activeDeviceChecks entry was added before lookupStorageDeviceState
was called, and every release lived in the callback's finally. A throw
before the Future existed registered no callback, so the entry stayed
for the life of the process and every later check of that device became
a no-op.

Wrapping the lookup call and treating a throw as a failed lookup also
makes startDeviceCheck total, so the finally's re-dispatch can no longer
replace an exception unwinding out of the try body.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: Note the throw contract on the seam

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:687-698`

- [ ] **Step 1: Add a line to lookupStorageDeviceState's scaladoc**

Find:

```scala
   *  Called while holding the instance lock, so an override must return promptly rather than
   *  block: the lock it holds is the one handleEvent takes.
   */
  protected def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
```

Replace with:

```scala
   *  Called while holding the instance lock, so an override must return promptly rather than
   *  block: the lock it holds is the one handleEvent takes.
   *
   *  May throw rather than return a failed Future. startDeviceCheck treats the two alike, so an
   *  override is free to do either.
   */
  protected def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
```

- [ ] **Step 2: Compile**

Run: `sbt compile`
Expected: success. This is a comment-only change.

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala
git commit -m "Record that lookupStorageDeviceState may throw

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: Drop the TODO entry

**Files:**
- Modify: `TODO.txt:2-14`

- [ ] **Step 1: Delete the top entry**

Delete these lines, which are lines 2 through 14 — the entry itself plus the blank line separating it from the next one. Keep the file's leading blank line 1.

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
  - Low probability, but no longer blocked: lookupStorageDeviceState is the
    seam a test needs to make the call throw

```

After the edit the file must begin with a blank line followed by `ZMQNet.ioThread leaves a half-built host behind when NewHostAvailable fails`.

- [ ] **Step 2: Verify the file head**

Run: `head -4 TODO.txt`
Expected:

```

ZMQNet.ioThread leaves a half-built host behind when NewHostAvailable fails
partway through
  - The send-queue loop now guards each item, so a throw here no longer takes
```

- [ ] **Step 3: Commit**

```bash
git add TODO.txt
git commit -m "Drop the fixed startDeviceCheck guard-leak entry

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 7: Full verification

**Files:** none modified

- [ ] **Step 1: Run the full test suite**

Run: `sbt test`
Expected: all tests PASS. `StoreManager` is broadly depended upon, so this is the check that nothing outside the device-discovery suite noticed the change.

- [ ] **Step 2: Confirm the working tree is clean**

Run: `git status --short`
Expected: no output. Every change is committed.

If `sbt test` fails in a suite unrelated to device discovery, check whether it fails on `main` too before attributing it to this work:

```bash
git stash list   # expect empty
git log --oneline -6
```

---

## Notes for the implementer

**Why `armLookupThrow` and not a failed Promise.** A failed `Promise` still produces a `Future`, so the callback is registered and the `finally` runs. The bug is specifically the absence of the `Future`. The suite already has a test using a failed promise — `"a deferred check still runs when the in-flight lookup fails"` — and it passes against the buggy code. That is the distinction the new seam exists to draw.

**Why the tests use `testingOnlyHandleHostMessage` rather than `testingOnlyCheckAllDevices`.** It targets one named device. Its handler rescans `storageDevicesDir` only when the device is absent from `storageDevices`, then calls `checkStorageDevice` either way, so it works for an injected device (Task 3) and an on-disk one (Task 2) alike, and each trigger issues exactly one check. `testingOnlyCheckAllDevices` sweeps every loaded device, which makes the lookup count both tests assert on depend on how many devices happen to be loaded. (It would *not* trip over Task 3's relative `devicePath`: `checkForNewDevices` iterates the storage-devices directory's children and never reads a loaded device's `devicePath`.)

**Both hooks call `handleEvent` directly**, bypassing the event loop and its catch-all at `StoreManager.scala:334`. That is what lets Task 2 assert `noException should be thrownBy`.

**Do not reorder Tasks 2-4.** Each test must be watched failing against unmodified production code, for the specific reason named in its Step "run to verify it fails".
