# Durable Service Design

## Overview

Aspen's `TaskExecutor` and `DurableTask` framework handles host-specific, finite-duration work with
crash recovery. A complementary abstraction is needed for **persistent, long-running services** that
are not bound to a particular host. These services must remain running somewhere in the cluster at all
times. If the host currently running a service dies or loses network connectivity, another host must
detect the failure and resume the service.

`DurableService` fills this role. It is identified by UUID, stores state in a `KeyValueObject`, and
runs indefinitely — there is no `Future` to completion. All hosts collaborate to ensure each registered
service is always running on exactly one host at a time, using a lease-based ownership model.

---

## Package Structure

```
aspen.compute
  DurableService.scala
  DurableServiceFactory.scala
  DurableServiceExecutor.scala

aspen.compute.impl
  SimpleDurableServiceExecutor.scala
```

---

## Core Types

### `DurableService`

A running service instance. Constructed by `DurableServiceFactory.createService` and begins operating
immediately upon construction — there is no separate `start()` method.

```scala
trait DurableService:
  def shutdown(): Unit
```

`shutdown()` is called by the executor when this host loses its lease (lease expiry detected, or
renewal transaction returns a revision mismatch indicating another host has claimed the service). The
service must stop operating immediately. Because Aspen uses a crash-only model, no general-purpose
`stop()` method is provided. Service implementations that need clean teardown for unit tests may
define a type-specific stop method on their concrete class.

### `DurableServiceFactory`

Registered with the `TypeRegistry` via the standard `RegisteredTypeFactory` mechanism. Located at
runtime using the `typeUUID` stored in the TKVL entry.

```scala
trait DurableServiceFactory extends RegisteredTypeFactory:
  def createService(
    client: AspenClient,
    statePointer: KeyValueObjectPointer,
    state: KeyValueObjectState
  ): DurableService
```

The factory receives the already-read `KeyValueObjectState` so the service can resume without an
extra round-trip. If a specific service implementation needs a `TaskExecutor` or other host-scoped
resources, its factory should capture those at factory construction time.

### `DurableServiceExecutor`

The public interface for registering and unregistering services.

```scala
trait DurableServiceExecutor:
  def registerService(typeUUID: UUID, serviceUUID: UUID, initialState: Map[Key, Array[Byte]]): Future[Unit]
  def unregisterService(serviceUUID: UUID): Future[Unit]
```

---

## Data Model

### Radicle Key

A new key is added to `Radicle`:

```scala
val ServicesTreeKey = Key(Array[Byte](9))
```

This roots a `TieredKeyValueList` for all registered services. A `MetadataTree` is not used here
because services require full scanning and carry richer values than a simple UUID→pointer mapping.

### TKVL Entry Format

- **Key**: service UUID (16 bytes)
- **Value** (encoded):
  - `typeUUID` — 16 bytes — identifies the `DurableServiceFactory` in the `TypeRegistry`
  - `hostId` — 16 bytes — UUID of the host currently running the service; all-zeros = unclaimed
  - `leaseExpirationTimestamp` — `HLCTimestamp` — zero/epoch = immediately claimable
  - `stateObjectPointer` — encoded `KeyValueObjectPointer`

Placing `typeUUID` in the TKVL entry (rather than in the KVO) allows the executor to identify the
correct factory from the scan result alone, without an additional KVO read.

### State KVO

One `KeyValueObject` per service, created at registration. Contains opaque, service-specific state
bytes. The executor never interprets KVO contents — it reads the object and passes the full
`KeyValueObjectState` to `createService`. The service implementation owns the KVO entirely.

---

## Timing Constants

| Constant | Value |
|---|---|
| Lease duration | 15 seconds |
| Renewal interval | 5 seconds |
| Scan interval | random(10, 30) seconds, re-randomized each cycle |
| Claim delay per owned service | random(0, 500ms) × ownedServiceCount |

The randomized scan interval naturally distributes TKVL reads across the cluster. With N hosts each
scanning on an independent random schedule, the expected time for at least one host to scan within
any short window after a lease expires drops rapidly as N grows, meeting the ~20-second recovery
target at 10+ hosts without coordination.

---

## Lease Lifecycle

### Renewal

Each owned service has a dedicated renewal timer firing every 5 seconds. The timer overwrites the
TKVL entry with a refreshed `leaseExpirationTimestamp = now + 15s`, using a revision requirement on
the current TKVL node to make the write atomic.

**On revision mismatch**: another host has claimed the service. Call `service.shutdown()`, cancel
the renewal timer, and remove the service from `ownedServices`.

**On other transaction failure**: retry at the next renewal interval.

**If `now > leaseExpiry`**: the lease has expired regardless of the failure reason. Call
`service.shutdown()`, cancel the timer, and remove from `ownedServices`.

### Scan and Recovery

The scan is wrapped in `ignoreExtraCallsWhileRunning` to prevent read pileups during extended
offline periods. Each cycle:

1. Read the full services TKVL, decoding all entries.

2. **Unregistration check**: for each service in `ownedServices` whose UUID is absent from the TKVL,
   call `service.shutdown()`, cancel its renewal timer, and remove it from `ownedServices`. This is
   how services unregistered from any host are detected and cleaned up on the owning host.

3. **Expired lease detection**: for each TKVL entry where `leaseExpiry < now` and the service is not
   already in `ownedServices`, add it to the candidate list.

4. **Claim attempts**: for each candidate, schedule a claim attempt after a delay of
   `random(0, ownedServices.size × 500ms)`. Hosts with fewer running services have shorter delays
   and win the race more often, spreading service ownership across the cluster.

5. Schedule the next scan at a fresh random interval of 10–30 seconds.

### Claim Attempt

Uses the TKVL node revision captured during the scan — no re-read required. If another host claims
the service between the scan and the attempt, the revision requirement causes the transaction to fail
and the attempt is silently dropped.

On a successful write (`hostId = thisHostId`, `leaseExpiry = now + 15s`):

1. Read the KVO at `stateObjectPointer`.
2. Look up the `DurableServiceFactory` in the `TypeRegistry` using `typeUUID`.
3. Call `factory.createService(client, statePointer, state)` — the service starts immediately.
4. Store the instance in `ownedServices`.
5. Start the 5-second renewal timer for this service.

---

## `SimpleDurableServiceExecutor`

Implements `DurableServiceExecutor`. Created by `StoreManager` during host initialization after
reading the services tree root pointer from Radicle.

```scala
class SimpleDurableServiceExecutor(
  client: AspenClient,
  nodeAllocator: NodeAllocator,
  servicesTreePointer: KeyValueObjectPointer
)
```

The `TypeRegistry` is accessed through the client.

Internal state:

```scala
private var ownedServices: Map[UUID, (DurableService, ScheduledTask)] = Map.empty
// serviceUUID → (running instance, renewal timer)
```

### Timer Cleanup

All timers — the scan loop timer and each per-service renewal timer — must be explicitly cancelled
when the executor is shut down. Lingering callbacks after host shutdown cause intermittent errors in
unit tests and must be avoided. The executor must expose a `shutdown()` method (or equivalent) that
cancels all active timers and calls `service.shutdown()` on all currently-owned services.

---

## `registerService`

Idempotent. Checks the TKVL for the given `serviceUUID` first. If already present, returns without
modification. Otherwise, in a single transaction:

1. Allocates a new KVO with the key-value pairs from `initialState`.
2. Writes a TKVL entry with `typeUUID`, `hostId = zero UUID`, `leaseExpiry = epoch 0`, and
   `stateObjectPointer` pointing to the new KVO.

The TKVL write uses a `DoesNotExist` requirement on the service UUID key, so concurrent
`registerService` calls from multiple hosts are safe — only one transaction succeeds, the others
fail with a revision error and discover the entry already exists on retry.

The entry is immediately claimable. Registration does not claim the service on the registering host
— it competes like any other host on the next scan cycle.

## `unregisterService`

Removes the TKVL entry and deletes the KVO in a single transaction. If this host currently owns the
service, calls `service.shutdown()`, cancels its renewal timer, and removes it from `ownedServices`
before writing the transaction. Other hosts discover the removal on their next scan (unregistration
check, step 2 above) and shut down their instance if needed.

---

## Bootstrap Integration

`Bootstrap.scala` creates an empty services `TieredKeyValueList` using the bootstrap pool's
`NodeAllocator` (consistent with the existing metadata trees for storage pools, hosts, storage
devices, etc.) and stores its root pointer in the Radicle object under `ServicesTreeKey`.

`StoreManager` reads `ServicesTreeKey` from Radicle during startup, constructs a
`SimpleDurableServiceExecutor`, and starts its scan loop. This is analogous to how `StoreManager`
initializes `SimpleTaskExecutor` from `HostState.TaskExecutorRootKey`.
