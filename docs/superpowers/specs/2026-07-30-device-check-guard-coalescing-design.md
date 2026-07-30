# Design: Coalescing the storage device check guard

Date: 2026-07-30

## Goal

`StoreManager.checkStorageDevice` decides whether a device is loaded *before* it starts the
state lookup, and its `activeDeviceChecks` guard discards any request that arrives while a
lookup is outstanding. Move the decision to lookup completion, and make the guard defer
colliding requests instead of dropping them.

## The bug

`checkStorageDevice` takes the guard, then branches on `storageDevices.get(storageDeviceId)` at
dispatch time and issues one of two near-identical `getStorageDeviceState(...).onComplete`
blocks (`StoreManager.scala:754-780`):

```scala
synchronized:
  if ! activeDeviceChecks.contains(storageDeviceId) then
    activeDeviceChecks += storageDeviceId

    storageDevices.get(storageDeviceId) match
      case Some(local) =>
        client.getStorageDeviceState(storageDeviceId).onComplete: result =>
          synchronized:
            try
              result match
                case Success(remote) => check(local, remote)
                case Failure(err) => logger.warn(...)
            finally
              activeDeviceChecks -= storageDeviceId
      case None =>
        client.getStorageDeviceState(storageDeviceId).onComplete: result =>
          synchronized:
            try
              result match
                case Success(remote) =>
                  remote.stores.keysIterator.foreach: storeId =>
                    offlineStores += storeId          // <-- acts on a stale premise
                case Failure(err) => logger.warn(...)
            finally
              activeDeviceChecks -= storageDeviceId
```

Runtime storage device discovery (2026-07-28) made a device's load state mutable while a check
is in flight. Before that work a device could not go unloaded -> loaded at all, so the
dispatch-time branch could not go stale. Now it can:

1. `CheckStorageDevice` arrives for a device with no config on disk yet. `checkForNewDevices`
   finds nothing, `checkStorageDevice` takes the `None` branch, and a lookup goes out.
2. The config appears. A later event runs `checkForNewDevices`, which loads the device, then
   calls `checkStorageDevice` for it.
3. That call hits `activeDeviceChecks.contains` and is **discarded outright**.
4. The in-flight callback fires and runs the `None` body it was built with, adding the
   now-loaded device's stores to `offlineStores`.

Two distinct defects, from one root cause each:

**The discarded request (the guard drops rather than defers).** Nothing else picks it up. The
newly loaded device is not checked -- no `createNewStore` for its `Initializing` stores, no
`startStoreTransferIn` for its `TransferringIn` stores -- until the next `CheckAllDevices`.

**The stale marking (the branch is chosen too early).** The callback re-adds store ids that
`tryLoadStore` (`StoreManager.scala:316`) and the `LoadStore` handler
(`StoreManager.scala:950`) have just removed, and those two sites are the only things that
remove them. `check`'s deleted-stores branch cannot clean up either: it filters
`local.offlineStores`, which is empty for a device that just loaded cleanly. The entries
persist for the life of the process.

## Impact

`checkStorageDevicePeriod` is `Duration(1, HOURS)` in `Main.scala`. A discarded check therefore
costs up to an hour, the same cost `Main.scala` already reasons about for *lost* notification
messages in two places. A message that arrives and is then silently dropped by the guard is
indistinguishable in effect.

The stale marking is currently benign. `handleEvent` matches `stores.get` before consulting
`offlineStores` for both `TransactionMessage` and `ClientReq`, so a store present in both is
routed to the store. It is wrong state that happens not to be read, not wrong behaviour.

The discarded-request half is not limited to the load-state case in `TODO.txt`. Every
`checkStorageDevice` call colliding with an in-flight lookup is dropped today, including the
`create-storage-device` discovery nudge and the post-transfer nudge that
`updateStateForTransferredStore` sends.

## Design

### 1. Split policy from mechanism

`checkStorageDevice` currently mixes collision policy, lookup dispatch, and result handling.
Separate them, and promote `check` from a local def to a private method, since the caller that
now needs it is `startDeviceCheck` rather than `checkStorageDevice`.

A new field joins `activeDeviceChecks`:

```scala
private var deferredDeviceChecks: Set[StorageDeviceId] = Set()
```

`activeDeviceChecks` keeps its type and meaning, so `testingOnlyActiveDeviceChecks` and the
existing test that asserts on it need no change.

Policy -- the only entry point callers use, and the only place the collision rule lives:

```scala
private def checkStorageDevice(storageDeviceId: StorageDeviceId): Unit =
  synchronized:
    if activeDeviceChecks.contains(storageDeviceId) then
      deferredDeviceChecks += storageDeviceId
    else
      startDeviceCheck(storageDeviceId)
```

Mechanism -- one lookup, one callback, the branch decided at completion:

```scala
/** Caller holds the instance lock and has established that no check is active. */
private def startDeviceCheck(storageDeviceId: StorageDeviceId): Unit =
  activeDeviceChecks += storageDeviceId

  lookupStorageDeviceState(storageDeviceId).onComplete: result =>
    synchronized:
      try
        result match
          case Success(remote) =>
            storageDevices.get(storageDeviceId) match
              case Some(local) => check(local, remote)
              case None =>
                remote.stores.keysIterator.foreach: storeId =>
                  offlineStores += storeId
          case Failure(err) => logger.warn(...)
      finally
        activeDeviceChecks -= storageDeviceId
        if deferredDeviceChecks.contains(storageDeviceId) then
          deferredDeviceChecks -= storageDeviceId
          startDeviceCheck(storageDeviceId)
```

The `Failure` branch keeps both of today's distinct messages by re-reading
`storageDevices.contains` for the wording. The unloaded case is worth keeping distinguishable in
the logs.

### 2. A seam for the lookup

Tests cannot otherwise hold a lookup in flight across a device load. Mirroring the
`protected def tryLoadStore` pattern already used in this file for the same reason:

```scala
protected def lookupStorageDeviceState(storageDeviceId: StorageDeviceId): Future[StorageDeviceState] =
  client.getStorageDeviceState(storageDeviceId)
```

This also supplies what the separate `TODO.txt` entry for the synchronous-throw guard leak was
waiting on -- that entry stays open, but is no longer blocked.

### 3. Rewrite the dispatch comment

The comment block at `StoreManager.scala:734-753` documents the two-branch structure and the
decision not to guard the synchronous throw. Roughly half its claims stop being true. It is
rewritten rather than patched, and is a first-class deliverable of this change rather than
cleanup: its account of when `getStorageDeviceState` fails, and why a copied or moved config is
not one of those cases, is the only place that reasoning is recorded.

## Correctness

The argument rests on one fact: the `.onComplete` body already takes the instance lock, and
`storageDevices` is only ever mutated under that same lock -- from `handleEvent`, or from the
constructor before `start()` exists. The re-read is therefore not a TOCTOU. While the callback
holds the lock, no device can be loading.

Taking `L` as the in-flight lookup:

| # | Scenario | Outcome |
|---|---|---|
| 1 | Unloaded at dispatch, still unloaded, `L` succeeds | Re-read `None` -> mark offline. Unchanged from today. |
| 2 | Unloaded at dispatch, loads during flight, `L` succeeds | Re-read `Some` -> `check` runs. The deferral then fires a fresh lookup and `check` runs again. |
| 3 | Unloaded at dispatch, loads during flight, `L` fails | Re-read never happens, but the deferral re-dispatches. |
| 4 | Loaded throughout, no collision | Unchanged from today. |
| 5 | Nudge collides with the periodic sweep, no load-state change | Deferred and run. Today: dropped, up to an hour late. |

`check`'s host-migration recursion is deliberately absent from that table. It is reached through
`updateHostId(...).foreach`, and `Future.foreach` always dispatches through the
`ExecutionContext` rather than running inline, so the recursive `checkStorageDevice` runs after
the callback's `synchronized` block has exited and released the guard. It is not dropped today
and this change does not alter it.

Cases 2 and 3 are the two orderings of the `TODO.txt` bug. Case 3 is the one a re-read alone
does not cover, since a failed lookup never reaches the re-read; only the deferral rescues it.

Case 2 runs `check` twice back to back. That is safe, and not by accident: `createNewStore`
guards on `creatingStores` and `startStoreTransferIn` guards on `transferringInStoreIds`. It
also already happens today whenever a nudge lands just after a periodic sweep.

Re-dispatch is bounded. `startDeviceCheck` issues a lookup and returns; the deferral flag is
cleared before re-dispatching, so one deferral buys exactly one re-check. `ec` is a real thread
pool rather than a parasitic `ExecutionContext`, so `onComplete` does not run inline and the
`finally` cannot recurse into itself.

### Accepted cost of re-dispatching from the `finally`

If `check` throws *and* the re-dispatched `lookupStorageDeviceState` also throws synchronously,
the second throw replaces the first and the original is lost. This is accepted rather than
guarded. It requires two simultaneous throws, and it belongs to the already-written `TODO.txt`
entry for the synchronous-throw leak, which is extended to record it.

The two alternatives were weighed and rejected. Moving the re-dispatch after the `try/finally`
removes the masking but strands the deferral flag when `check` throws: `startDeviceCheck` does
not consult that flag, so the device gets one spurious extra check later while the real one
still waits a full period. Wrapping the re-dispatch in its own `try/catch` is correct but
delivers the synchronous-throw fix through the back door for one call site only, which is worse
than fixing it properly under its own entry.

## Testing

Two new hooks, mirroring `testingOnlyActiveDeviceChecks`:

```scala
private[aspen] def testingOnlyDeferredDeviceChecks: Set[StorageDeviceId] = synchronized(deferredDeviceChecks)
private[aspen] def testingOnlyOfflineStores: Set[StoreId] = synchronized(offlineStores)
```

`RecordingStoreManager` in `StoreManagerDeviceDiscoverySuite` gains a per-device queue of
promises, each consumed once, plus a record of every lookup issued:

```scala
def armLookup(deviceId: StorageDeviceId): Promise[StorageDeviceState]
val lookupAttempts: mutable.ListBuffer[StorageDeviceId]
```

An empty queue falls through to `super`. That fall-through is what keeps the existing tests
working: they rely on `deviceA` being absent from the `TestNetwork` storage-devices tree so the
real lookup fails with `NoSuchElementException`, and the test that a failed check does not wedge
later checks of the same device depends on exactly that.

`StorageDeviceState` is a plain case class, so a test builds one directly with no
`KeyValueObjectState` involved.

**Test 1 -- the regression.** Arm two lookups for `deviceA`. Send `CheckStorageDevice` with
nothing on disk and assert the guard is held. Write the device and fire `CheckAllDevices`;
assert it loaded and that `deferredDeviceChecks` holds `deviceA` -- this is the request that
today's guard discards. Complete the first promise with a `StorageDeviceState` carrying one
`Active` store and `hostId = HostId.BootstrapHostId`, then `yieldUntil` the callback has run.
Assert `offlineStores` does not contain that store, and that `lookupAttempts` shows the second
lookup.

An `Active` store makes `check` a deliberate no-op -- every branch inside it is empty -- so the
test pins branch selection alone without dragging in store creation or transfers.
`BootstrapHostId` keeps it off the host-migration path.

**Test 2 -- a colliding request is deferred, not dropped.** Device loaded, no load-state change.
Two checks; assert the second is deferred; complete the first; assert a second lookup was
issued.

**Test 3 -- a deferred check survives a failed lookup.** As test 2, but fail the first promise.
This is case 3, the ordering a re-read alone does not cover.

Every one of the three completes a promise and then observes state a callback mutates, so each
needs the suite's `yieldUntil` helper between the two. `yieldUntil` gives up silently, so each
test must also assert the condition it waited on -- otherwise an exhausted wait reads as a pass.

`RecordingStoreManager`'s scaladoc currently warns that `offlineStores` "stays empty here, so a
test asserting on them would pass vacuously". That warning is about the recording
`tryLoadStore` skipping the real bookkeeping. Test 1's assertion is not vacuous -- the write it
guards against comes from the check callback, and on today's code that assertion fails. The
scaladoc must be amended to scope the warning, or the next reader will delete a working test.

## Residuals to record in TODO.txt

- The synchronous-throw entry gains a line: the leak can now also mask an exception thrown by
  `check`, because the re-dispatch runs inside the `finally`.
- `check`'s host-migration recursion can retry unboundedly if the remote `hostId` never
  converges: `check` calls `updateHostId` and then `checkStorageDevice` again, which re-reads a
  still-mismatched state and repeats. The guard is no brake on it, since the recursion is
  dispatched through the `ExecutionContext` and so always runs after the guard is released.
  Pre-existing and unaffected by this change; recorded because it was found while tracing it.
- A legitimate `None`-branch marking is cleared only when the store actually loads. A device
  registered in the storage-devices tree that never appears on disk leaves its stores in
  `offlineStores` for the life of the process, permanently suppressing `TxUnknownStore` and
  `StoreNotFound` for them. Unchanged by this design and out of its scope.

## Out of scope

The `TODO.txt` entry for `checkStorageDevice` leaking its `activeDeviceChecks` guard on a
synchronous throw from the lookup call. It stays a separate entry. Section 2 builds the seam it
was waiting on, so it is unblocked but not addressed here.
