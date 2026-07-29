# Design: Runtime storage device discovery

**Date:** 2026-07-28
**Status:** Approved

## Goal

A storage device registered under a running host stays inert until the host
process restarts. `StoreManager` scans `<hostRoot>/storage-devices/` only in its
constructor, and the periodic `CheckAllDevices` event iterates the already-loaded
`storageDevices` map without ever rescanning the directory. Any pool created on
the new device before the restart has its stores marked offline rather than
instantiated, and `create-storage-device` currently tells the operator to restart
the host.

Two changes fix this:

1. **`StoreManager.checkForNewDevices()`** rescans `storage-devices/` and loads
   anything not already loaded. It is called at construction (replacing the
   inline scan, so the loading logic exists in exactly one place) and from the
   event loop on every `CheckAllDevices`, before the existing per-device
   iteration.
2. **`create-storage-device` sends a `CheckStorageDevice` message** to the owning
   host after registration succeeds, so detection is near-immediate.

Polling is the correctness guarantee; the message is a latency optimization.
Losing it costs at most one check period.

The message matters in practice: `Main.scala` hard-codes
`checkStorageDevicesPeriod = Duration(1, HOURS)`, so polling alone means a new
device can stay inert for an hour.

## Approach

Reuse the existing `CheckStorageDevice` `HostMessage` rather than adding a new
`NewDeviceAdded` type. `TODO.txt` proposes a new message, but `CheckStorageDevice`
already carries exactly the needed payload (`toHost`, `fromClient`, `deviceId`),
is already routed, and is already exercised in production by
`StoreManager.updateStateForTransferredStore`. Reusing it means **no change to
`codec.proto`, `Codec.scala`, `Message.scala`, or the envelope dispatch**.

The semantics fit: "check this device" reasonably implies "discover it if you do
not know it yet". Reuse also closes a latent gap — today a `CheckStorageDevice`
naming a device the host has not loaded falls into `checkStorageDevice`'s `None`
branch and needlessly marks that device's stores offline.

The cost is mild overloading: a `CheckStorageDevice` for a genuinely absent
device (a failed disk) now triggers one wasted directory scan — a `listFiles`
plus a few small YAML parses, bounded by message rate.

## Relevant existing facts

- **`handleEvent` is already `synchronized`** (`StoreManager.scala:751`), so
  calling `checkForNewDevices()` from an event handler mutates `storageDevices`
  under the same lock that background readers (`getDevicePath`, the usage-polling
  task started in `startUsageTracking`) already take. No new locking is required,
  and the thread that mutates `storageDevices` does not change.
- **Partial config reads are not a hazard.** `StorageDeviceManager.createStorageDevice`
  writes `aspen-storage-device-config.yaml` with `StandardOpenOption.CREATE_NEW`
  *after* its transaction commits, and deletes the file on a failed write. A
  config that exists and parses is complete. Even if a scan did read a bad
  config, the existing `catch { case t: Throwable => logger.warn }` in
  `tryLoadDevice` means the device is simply not added and the next scan retries.
- **`tryLoadDevice` is not currently idempotent.** It unconditionally does
  `storageDevices += id -> new LocalStorageDeviceState(...)` and calls
  `tryLoadStore` on every child directory. Re-running it on a loaded device would
  discard `loadedStores`/`offlineStores` tracking and construct a second
  `RocksDBBackend` over a live RocksDB directory.
- **`sendHostMessage` is asynchronous and unflushed.** It only does
  `sendQueue.add` plus an inproc wakeup (`ZMQNet.scala:61`, `:165`); the `zmq-io`
  daemon thread does the real work. `ZMQNet` never sets socket linger and never
  closes its `ZContext`.
- **Bootstrap hosts are pre-resolved** in `MetadataManager`'s constructor
  (`MetadataManager.scala:68-81`), so `SendToHost` to a bootstrap host resolves
  immediately. A non-bootstrap host requires an async `startHostLookup`, during
  which the message sits in a `PendingHostLookup` queue.
- **`Main.scala`'s CLI commands exit promptly** after `awaitAndReport` (commit
  `31c5f39`, "Make CLI commands exit; daemonize all spawned threads").

## Non-goals

- **Discovering new store directories on already-loaded devices.** Store-level
  discovery stays with `checkStorageDevice` (driven by remote
  `StorageDeviceState`) and the `LoadStoreById` event. A store directory can
  exist on disk mid-transfer-in or mid-creation before it should be opened, so
  scanning for them races with `startStoreTransferIn` and `createNewStore`.
- **Detecting device *removal*.** `checkForNewDevices` only ever adds. Handling a
  device that disappears while loaded is a separate problem.
- **Changing `checkStorageDevicesPeriod`.** It stays at one hour, as a fallback
  behind the notification.

## Components

### 1. `StoreManager.checkForNewDevices()`

New private method — the only directory-scanning code in the class.

```scala
/** Scan storage-devices/ and load any device not already loaded.
 *
 *  Called at construction and from the event loop on every CheckAllDevices, so it must be
 *  idempotent -- tryLoadDevice skips devices already in storageDevices. Callers hold the
 *  instance lock: handleEvent is synchronized, and the constructor runs before start().
 */
private def checkForNewDevices(): Unit =
  if ! Files.isDirectory(storageDevicesDir) then
    logger.warn(s"Invalid storage devices directory: $storageDevicesDir")
  else
    // listFiles returns null on an IO error even when isDirectory just succeeded. Left
    // unguarded, the NPE escapes handleEvent and silently kills the event loop thread.
    storageDevicesDir.toFile.listFiles() match
      case null  => logger.warn(s"Failed to list storage devices directory: $storageDevicesDir")
      case files => files.foreach(tryLoadDevice)
```

The `null` guard is a real fix rather than defensive noise. Today the equivalent
NPE could only fail construction; from `handleEvent` — which `start()`'s loop
calls with no `try`/`catch` — it would kill the manager thread outright.

> **Correction (added after implementation).** The second half of that sentence
> was true when this spec was written but is no longer. Task 9b wrapped
> `handleEvent(event)` in `start()`'s loop in a `try`/`catch case t: Throwable`
> that logs at ERROR and continues, so an escaping NPE no longer kills the
> manager thread. The guard was kept for the two reasons that survive: it names
> the directory that could not be listed instead of surfacing a bare NPE, and it
> still covers the constructor call site, which runs outside the event loop and
> is therefore not protected by that catch.

### 2. `tryLoadDevice` gains an idempotency guard

All loading logic stays in `tryLoadDevice`. Only the already-loaded check is
added, replacing the unconditional map insert:

```scala
if sdCfg.aspenSystemId != aspenSystemId then
  logger.warn(s"Storage Device found that does not belong to this Aspen system: $storageDevicePath. Ignoring")
else
  storageDevices.get(sdCfg.storageDeviceId) match
    case Some(existing) =>
      // Already loaded. A second directory claiming the same device id means a duplicate
      // mount or a copied config file -- loading it would open a second backend on live
      // store directories, so report it and leave the original in place.
      if existing.devicePath != storageDevicePath then
        logger.warn(s"Storage device ${sdCfg.storageDeviceId} is already loaded from " +
                    s"${existing.devicePath}; ignoring duplicate at $storageDevicePath")
    case None =>
      // Unchanged: the existing body of StoreManager.scala:246-250 -- construct the
      // LocalStorageDeviceState, insert it into storageDevices, log, and call
      // tryLoadStore on each child of the device directory.
```

Drive-by fix in that unchanged body: `StoreManager.scala:248` logs
`"Loading store $sdFile"` while loading a *device*. Reword to
`"Loading storage device"`.

### 3. Constructor

`StoreManager.scala:143-147` collapses to:

```scala
checkForNewDevices()
```

The two `events.put(...)` lines that follow are unchanged.

### 4. `CheckAllDevices` handler

`StoreManager.scala:841` — discover before iterating:

```scala
case CheckAllDevices() =>
  checkForNewDevices()
  storageDevices.valuesIterator.foreach: sds =>
    checkStorageDevice(sds.storageDeviceId)
```

A device found by the scan is checked within the same event, so its
`Initializing` stores are created immediately rather than after another period.

### 5. `CheckStorageDevice` handler

`StoreManager.scala:804` — the push path:

```scala
case m: CheckStorageDevice =>
  if ! storageDevices.contains(m.deviceId) then
    checkForNewDevices()
  checkStorageDevice(m.deviceId)
```

### 6. `ZMQNet` flush support

```scala
/** Block until messages queued for `hostId` have been handed to their ZMQ socket, or
 *  `timeout` elapses. Returns true if the queues drained.
 *
 *  Not a delivery guarantee -- ZMQ buffers internally and the peer may be down. It
 *  establishes only that nothing is left waiting inside ZMQNet, which is what a
 *  short-lived process would otherwise abandon on exit.
 */
def awaitHostMessagesSent(hostId: HostId, timeout: Duration): Boolean

/** Close the context, allowing buffered outbound data up to `linger` to flush. */
def shutdown(linger: Duration): Unit
```

`awaitHostMessagesSent` polls at roughly 25 ms intervals until all of the
following hold, or the deadline passes:

- `sendQueue` is empty (the IO thread has processed the `SendToHost`)
- the host entry is resolved
- its `odealer` is defined
- its `pendingMessages` queue is empty

Resolution is probed with a new side-effect-free
`MetadataManager.peekHostEntry(hostId): Option[T]`. The existing `getHostEntry`
starts a host lookup when the entry is absent, which a polling loop must not do
repeatedly.

`shutdown` sets `linger` on every connected dealer socket — the per-socket
`setLinger` is always available — and then closes the `ZContext`.

### 7. `create-storage-device` CLI

`Main.scala:1446-1454`. On success, notify the host instead of instructing a
restart:

```scala
awaitAndReport(f):
  case Success(deviceId) =>
    println(s"Created storage device ${deviceId.uuid} at $deviceDirectory")
    // Best-effort nudge so the host loads the device now rather than on its next periodic
    // storage-device check. StoreManager rescans storage-devices/ when a CheckStorageDevice
    // names a device it has not loaded, so no new message type is needed.
    client.sendHostMessage(CheckStorageDevice(hostCfg.hostId, client.clientId, deviceId))
    val flushed = network.awaitHostMessagesSent(hostCfg.hostId, Duration(5, SECONDS))
    network.shutdown(Duration(1, SECONDS))
    if flushed then
      println(s"Sent a device-check notification to host '${hostCfg.name}'. If it does not " +
              "arrive, the host will load the device on its next periodic storage-device check.")
    else
      println(s"Could not reach host '${hostCfg.name}'. It will load the device on its next " +
              "periodic storage-device check.")
  case Failure(err) => reportError(err)
```

The stale comment block at `:1449-1453` and the `"Restart host ..."` line are
removed.

## Error handling and edge cases

| Situation | Behavior |
|---|---|
| `storage-devices/` missing or not a directory | Warn, return. Now recurs once per check period rather than once at startup — a real misconfiguration worth repeating hourly. |
| `listFiles()` returns `null` | Warn, return. New guard; without it the NPE escapes `handleEvent` and kills the event loop thread (see the correction under Components §1 — the loop later gained a catch-all, so the escape is now logged rather than fatal). |
| Device dir exists, no config file yet | Skipped by the existing `Files.exists(sdCfgPath)` guard; picked up on a later scan once the config appears. Covers both the operator-provisioned-but-unregistered case and `createStorageDevice` crashing between commit and write. |
| Unparseable or partial config | Existing `catch Throwable` warns; the device is not added and is retried on the next scan. Self-healing. |
| Foreign `aspenSystemId` | Warn and ignore (unchanged), now once per period. |
| Already loaded, same path | Silent skip. The common case on every scan. |
| Already loaded, different path | Warn and keep the original. Indicates a duplicate mount or copied config; loading it would open a second backend over live store directories. |
| `CheckStorageDevice` for a device not on disk | One wasted scan, then the existing `None` branch marks its stores offline. Otherwise unchanged. |
| Notification lost, or drain times out | Device comes online on the next periodic check. Correctness never depends on the message. |
| Device removed from disk while loaded | Not handled. See non-goals. |

**Concurrency.** `checkForNewDevices` runs only on the event loop thread inside
the already-`synchronized` `handleEvent`, or in the constructor before `start()`.
`tryLoadStore`'s call to `loadStore` enqueues a `LoadStore` event; enqueuing to
the unbounded `LinkedBlockingQueue` from the event loop thread is safe.

## Testing

### New suite: `StoreManagerDeviceDiscoverySuite`

Each test builds a tmpdir containing
`storage-devices/<name>/aspen-storage-device-config.yaml` and constructs a
`StoreManager` rooted there.

Most tests use device directories with **no store subdirectories**, so
`tryLoadStore` iterates nothing and no RocksDB backend is opened. This exercises
all the discovery logic — config parsing, system-id check, the dedup guard, path
handling — without backend involvement.

- Constructor loads a pre-existing device
- A device created after construction is discovered on the next `CheckAllDevices`
- A device already loaded is not reloaded: same `LocalStorageDeviceState`
  instance, no repeat store loads
- A device dir with no config is skipped, then picked up once the config appears
- A foreign `aspenSystemId` is ignored
- A second directory with a duplicate device id is ignored; the original is retained
- A missing `storage-devices/` directory does not throw
- `CheckStorageDevice` naming an unknown-but-on-disk device triggers discovery,
  driven through the existing public `receiveHostMessage` (`StoreManager.scala:710`)
- One test with a populated store directory asserts that stores on a newly
  discovered device are loaded

### Two visibility changes to enable the suite

- `tryLoadStore`: `private` to `protected`, so the final test can use a subclass
  that records store loads instead of opening RocksDB. `stores` and
  `storageDevices` are already `protected`, and `StoreManager` is already mixed
  into (`with SimpleDriverRecoveryMixin`), so this follows existing precedent.
- Add `private[aspen] def testingOnlyEnqueueCheckAllDevices(): Unit =
  events.put(CheckAllDevices())`, matching the existing `testingOnlyHandleEvents()`
  convention. Tests drain the queue with `testingOnlyHandleEvents()`.

`checkStorageDevice` runs against device ids that are not registered in the test
metadata. Those futures fail and are swallowed by the existing `.foreach`, which
does not affect the assertions.

### Not automated: the ZMQNet flush helpers

There is no ZMQNet test harness — the only suite under `zmqnet/` is
`ProtobufMessageCodecSuite` — and exercising the drain requires binding real
sockets, a poor trade in CI. `awaitHostMessagesSent` and `shutdown` are verified
manually through the `./t bootstrap` -> `./t host` -> `./t create-storage-device`
workflow, confirming that the device comes online within seconds rather than
after the hour-long poll.
