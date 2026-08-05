# StoreManager.startDeviceCheck: releasing the guard on a synchronous throw

## Problem

`StoreManager.startDeviceCheck` adds a device to `activeDeviceChecks` before it calls
`lookupStorageDeviceState`:

```scala
private def startDeviceCheck(storageDeviceId: StorageDeviceId): Unit =
  activeDeviceChecks += storageDeviceId

  lookupStorageDeviceState(storageDeviceId).onComplete: result =>
    synchronized:
      try   ...
      finally
        activeDeviceChecks -= storageDeviceId
        if deferredDeviceChecks.contains(storageDeviceId) then
          deferredDeviceChecks -= storageDeviceId
          startDeviceCheck(storageDeviceId)
```

Every release of that guard entry lives in the callback's `finally`, so every release is hostage
to the callback existing. Both outcomes of the returned `Future` reach it, and so does a throw out
of the callback body. A synchronous throw from `lookupStorageDeviceState` itself does not: it
happens before the `Future` exists, so no callback is ever registered and the `finally` never
runs.

The entry then stays for the life of the process. `checkStorageDevice` sees the device in
`activeDeviceChecks`, records a deferral instead of starting a check, and that deferral is
released only by the `finally` that will not run. Every later check of that device -- the periodic
`CheckAllDevices` sweep, an inbound `CheckStorageDevice` message, the re-check that follows a
host-migration claim -- silently becomes a no-op. The device is wedged.

There is a second defect on the same lines. The `finally` re-dispatches a deferred check by
calling `startDeviceCheck` again, and that call can throw synchronously for exactly the reason
above. A throw from inside a `finally` replaces any exception already propagating out of the
`try` body, so a `reconcileDeviceState` failure -- filesystem work and transactions, both able to
throw -- is discarded and replaced by the lookup's exception. The device is wedged and the
diagnostic that would explain why is lost.

Both are recorded in `startDeviceCheck`'s scaladoc as known and deliberately unguarded. The
scaladoc also records that no test drives a throw out of the callback body, so the `finally`'s
correctness rests on argument alone.

Probability is low: the production `lookupStorageDeviceState` is
`client.getStorageDeviceState(storageDeviceId)`, which reports a missing registration or a failed
metadata read as a failed `Future` rather than a throw. The blocker to fixing it was a test lever,
and that is gone -- `lookupStorageDeviceState` is already a `protected` seam that
`StoreManagerDeviceDiscoverySuite`'s `RecordingStoreManager` overrides.

## Chosen behaviour

A synchronous throw is treated as a failed lookup. It releases the guard, re-dispatches any
deferral, logs, and does not propagate.

The alternative -- release the guard and rethrow -- was rejected. From the event loop the throw
would be absorbed by `start()`'s catch-all, but `startDeviceCheck` is also reached from
`reconcileDeviceState`'s host-migration branch through a `Future.foreach` callback, which has no
such catch. More importantly it leaves `startDeviceCheck` able to throw, so the `finally`'s
re-dispatch stays a masking hazard needing a second, separate guard.

Making `startDeviceCheck` total fixes both defects with one change: a method with no synchronous
throw path cannot mask an exception when called from a `finally`.

## Design

### Production change

`src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`

The `finally` body is extracted so both the normal and the throwing path share one release, and
the lookup call is wrapped:

```scala
private def startDeviceCheck(storageDeviceId: StorageDeviceId): Unit =
  def endDeviceCheck(): Unit =
    activeDeviceChecks -= storageDeviceId
    if deferredDeviceChecks.contains(storageDeviceId) then
      deferredDeviceChecks -= storageDeviceId
      startDeviceCheck(storageDeviceId)

  activeDeviceChecks += storageDeviceId

  val lookup =
    try Success(lookupStorageDeviceState(storageDeviceId))
    catch case t: Throwable => Failure(t)

  lookup match
    case Success(f) =>
      f.onComplete: result =>
        synchronized:
          try   ...            // unchanged
          finally endDeviceCheck()

    case Failure(t) =>
      logger.warn(s"Lookup of state for storage device $storageDeviceId threw before " +
                  s"the read was dispatched. Error: $t")
      endDeviceCheck()
```

`endDeviceCheck` is a nested `def` rather than a private method on the class. It has no caller
outside `startDeviceCheck`, and nesting lets it close over `storageDeviceId` instead of taking it
as a parameter, which removes any way for the two halves of a check to disagree about which device
they are releasing.

`Success` and `Failure` are already imported; `scala.util.Try` is not, hence the explicit
`try`/`catch` rather than `Try(...)`.

Three properties this relies on:

**The `catch` wraps only the lookup call, not the `.onComplete` registration.** Widening it would,
under an inline or parasitic `ExecutionContext`, also catch a throw coming back out of the
callback body -- reintroducing the swallowing this change exists to remove. The narrow scope
leaves `.onComplete` itself unguarded, so an `ExecutionContext` that rejects the submission still
leaks the entry. That is the same "an `ExecutionContext` that never runs the callback" case the
scaladoc already records, and it stays recorded rather than fixed.

**Recursion is bounded.** A throwing lookup inside the re-dispatch is absorbed by that call's own
`catch`, and `endDeviceCheck` clears the deferral before dispatching, so depth is bounded by the
at-most-one deferral held per device.

**The locking contract is unchanged.** `endDeviceCheck` touches only guard and deferral state and
has two call sites that both already hold the instance lock: the callback's `synchronized` block,
and the throwing path in `startDeviceCheck`, whose callers hold it.

The separate warn for the synchronous throw is deliberate. The existing `Failure(err)` warn
explains itself with "It may not be registered in the storage-devices tree" -- true for the
failed-`Future` case that is routinely reachable, false for a throw that never reached the tree at
all. Folding the two together would make the common message vaguer to accommodate the rare one.

### Tests

`src/test/scala/org/aspen_ddp/aspen/server/StoreManagerDeviceDiscoverySuite.scala`

`RecordingStoreManager.armedLookups` changes from
`mutable.Map[StorageDeviceId, mutable.Queue[Promise[StorageDeviceState]]]` to a queue of
`Either[Throwable, Promise[StorageDeviceState]]`. `armLookup` enqueues a `Right`, a new
`armLookupThrow` enqueues a `Left`, and the override throws on a `Left` before returning anything.
Unifying the queue rather than adding a second map keeps the ordering between armed successes and
armed throws well defined, which test 2 depends on. It stays a `lazy val`: `StoreManager`'s
constructor runs a device scan that reaches these fields before the subclass's own initializers.

Both tests drive checks through `testingOnlyHandleHostMessage(CheckStorageDevice(deviceId))`,
which targets one named device rather than sweeping every loaded one. Its handler rescans
`storageDevicesDir` only when the device is not already in `storageDevices`, and calls
`checkStorageDevice` either way, so it reaches the code under test whether or not the device is
registered.

**Test 1 -- a synchronous throw releases the guard.** Arm a throw for a device, trigger a check,
and assert the call returns normally with the device in neither `testingOnlyActiveDeviceChecks`
nor `testingOnlyDeferredDeviceChecks`. Release is synchronous on this path, so no `yieldUntil` is
needed. Then arm a normal promise, trigger a second check, and assert a second lookup was
attempted -- `RecordingStoreManager` already records every id passed to
`lookupStorageDeviceState` in call order. That last assertion is what distinguishes released from
wedged, and it is the one that fails against current code.

**Test 2 -- a callback-body throw is no longer masked.** Construct the manager with an
`ExecutionContext` that delegates to the suite's `executionContext` but records every
`reportFailure` call. Scala's `onComplete` routes a throw out of the callback to that method, so
the recorder observes which exception escaped.

Register a device in `storageDevices` -- a `protected var`, so the subclass can reach it -- whose
`devicePath` is relative and whose `offlineStores` holds one store id. Arm two lookups for it: a
promise, then a throw. Trigger a check, taking the guard; trigger a second, recording a deferral;
then complete the promise with a remote state carrying the matching `hostId` and omitting that
store id. The deleted-stores pass reaches `os.Path(local.devicePath)`, which throws
`IllegalArgumentException` on a non-absolute path. That line sits above the `try`/`catch` that
wraps `os.remove.all`, so the throw escapes the reconcile.

A relative `devicePath` is a synthetic input -- a real device scan only produces absolute paths --
but it drives the real unguarded line rather than a stand-in, and it needs no production seam
added for the test's benefit.

Assertions:

- the guard and deferral sets are both empty, and a second lookup was attempted, so the `finally`
  still released and re-dispatched
- the recorded failures contain the reconcile's `IllegalArgumentException` and do not contain the
  armed re-dispatch throwable

The second pair is what discriminates. Against current code the re-dispatch's throw replaces the
reconcile's, so the recorder sees the armed throwable and both halves fail.

### Comments and TODO

Three passages in `startDeviceCheck`'s scaladoc state behaviour this change falsifies:

- the clause naming the synchronous throw as "known, tracked in TODO.txt, and deliberately not
  guarded here" is replaced by why the `catch` is scoped to the lookup call alone. The
  "an ExecutionContext that never runs the callback" clause stays -- the narrow scope leaves
  `.onComplete` unguarded and that remains true.
- "It is argued rather than covered, though -- no test drives a throw out of the callback body"
  goes; test 2 covers it.
- the inline comment reading "it also strands the flag if lookupStorageDeviceState throws
  synchronously" loses that half. The clear-before-dispatch order it defends stays correct, on the
  unbounded-recursion grounds stated alongside it.

`endDeviceCheck` gets a comment covering the shared release-and-re-dispatch contract and the fact
that both call sites hold the instance lock. It is a nested `def`, so this is a plain comment
rather than scaladoc, and it absorbs the existing note on the clear-before-dispatch order, which
moves with the code it explains. `lookupStorageDeviceState`'s scaladoc gains a line saying
a throw is treated as a failed lookup, so an override is free to throw. The entry comes off the
top of `TODO.txt`.

## Out of scope

Three entries in `TODO.txt` sit adjacent to this code and are not addressed:

- `reconcileDeviceState` retrying the host-migration path forever when a remote `hostId` never
  converges. Pre-existing, and unaffected: the retry is paced by the network and runs after the
  guard is released either way.
- `offlineStores` entries for a device that never appears on disk never being cleared. Wants a
  rule for when an absent device is presumed gone rather than down -- a design question, not a
  guard.
- an `ExecutionContext` that never runs the callback. Left leaking, as described above, and left
  documented in the scaladoc.

## Verification

`sbt 'testOnly *StoreManagerDeviceDiscoverySuite'` for the two new tests plus the existing suite,
whose `armLookup` call sites all change shape with the queue's element type. `sbt test` for the
whole suite, since `StoreManager` is broadly depended upon.

Both new tests must be confirmed to fail against the unmodified `startDeviceCheck` before the fix
is applied -- test 1 on the second-lookup assertion, test 2 on the pair of `reportFailure`
assertions.
