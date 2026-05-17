# ObjectRegistry Design

## Overview

Add an `ObjectRegistry` class to `org.aspen_ddp.aspen.client` that provides a thin wrapper around a TKVL mapping UUID keys to ObjectPointers. It supports both transactional (prepare-style) and standalone (retry-until-successful) registration, with idempotency handling and duplicate detection.

## Motivation

Several parts of the system maintain TKVLs that map UUIDs to ObjectPointers (storage pools, hosts, storage devices in `SimpleAspenClient`). `ObjectRegistry` extracts this into a reusable abstraction. Future work will consolidate the separate radicle TKVLs into a single system ObjectRegistry, but that migration is out of scope here.

## Class Definition

**Package:** `org.aspen_ddp.aspen.client`
**File:** `ObjectRegistry.scala`

### Constructor

```scala
class ObjectRegistry(
  val client: AspenClient,
  containingObjectPtr: KeyValueObjectPointer,
  treeRootKey: Key
)
```

Internally creates a `KVObjectRootManager(client, treeRootKey, containingObjectPtr)` and wraps it in a `TieredKeyValueList`. Uses `client.clientContext` as the implicit `ExecutionContext`.

### Exception

```scala
class DuplicateRegistration(val objectId: UUID, val existing: ObjectPointer)
    extends Exception
```

Defined within the `ObjectRegistry` companion object. Thrown when `registerObject` finds an existing entry with a different pointer than the one being registered.

### Methods

#### `prepareRegisterObject(objectId: UUID, pointer: ObjectPointer)(using tx: Transaction): Future[Unit]`

For use within an externally-managed transaction. Inserts the UUID-to-pointer mapping with a key-must-not-exist requirement.

1. Call `tkvl.set(Key(objectId), Value(pointer.toArray), requirement = Some(Left(true)))`.
2. After the set future completes, check `tx.result.value` for `Some(Failure(_: KeyAlreadyExists))`. If found, throw the `KeyAlreadyExists` to give the caller an immediate signal before `tx.commit()`.
3. If `tx.result` is not yet failed, return successfully. The transaction may still abort at commit time due to contention, but that is handled by the caller.

#### `registerObject(objectId: UUID, pointer: ObjectPointer): Future[Unit]`

Standalone operation that retries until successful. Handles idempotency: if a retry discovers the key already exists with an identical pointer, it treats the operation as successful (the previous attempt committed but the client didn't know).

Uses `client.retryStrategy.retryUntilSuccessful` directly (not `transactUntilSuccessful`) because the recovery-function variant of the retry strategy cannot signal success — it can only allow retry or stop with an error. Instead, each attempt begins with a pre-check read:

**Each attempt:**
1. Read the key via `tkvl.get(Key(objectId))`.
2. If the key exists and the stored pointer equals the one being registered, return `Future.unit` — the previous attempt committed (idempotent success).
3. If the key exists but the pointer differs, throw `StopRetrying(DuplicateRegistration(objectId, existingPointer))`.
4. If the key does not exist, execute `client.transact` calling `prepareRegisterObject` within the transaction. If the transaction fails due to contention, the retry loop retries from step 1.

#### `getRegisteredObject(objectId: UUID): Future[ObjectPointer]`

Reads the TKVL and decodes the value.

1. Call `tkvl.get(Key(objectId))`.
2. If `None`, fail with `NoSuchElementException`.
3. If `Some(vs)`, return `ObjectPointer(vs.value.bytes)`.

#### `getRegisteredKeyValueObject(objectId: UUID): Future[KeyValueObjectPointer]`

Calls `getRegisteredObject` and casts the result to `KeyValueObjectPointer`.

#### `getRegisteredDataObject(objectId: UUID): Future[DataObjectPointer]`

Calls `getRegisteredObject` and casts the result to `DataObjectPointer`.

## Pointer Equality

`ObjectPointer` has a structural `equals` implementation that compares `id`, `poolId`, `size`, `ida`, and `storePointers`. The idempotency check in `registerObject` uses this directly: `ObjectPointer(vs.value.bytes) == pointer`.

## Tests

**File:** `src/test/scala/org/aspen_ddp/aspen/client/ObjectRegistrySuite.scala`
**Base class:** `IntegrationTestSuite`

Tests use the existing integration test infrastructure which provides a `TestNetwork`, `AspenClient`, and radicle `KeyValueObjectPointer`. A new TKVL tree is created in the radicle object during test setup.

### Test Cases

1. **Register and retrieve** — register a pointer, retrieve it with `getRegisteredObject`, verify equality.
2. **Retrieve typed pointers** — register a `KeyValueObjectPointer`, retrieve via `getRegisteredKeyValueObject`; same for `DataObjectPointer` via `getRegisteredDataObject`.
3. **Get non-existent** — `getRegisteredObject` for an unregistered UUID fails with `NoSuchElementException`.
4. **Idempotent re-registration** — `registerObject` with the same UUID and same pointer succeeds (no error).
5. **Duplicate with different pointer** — `registerObject` with the same UUID but a different pointer fails with `DuplicateRegistration`.
6. **prepareRegisterObject within transaction** — use `prepareRegisterObject` inside a manually-managed transaction, commit, then verify the object is retrievable.
7. **prepareRegisterObject duplicate detection** — call `prepareRegisterObject` for an already-registered UUID and verify it throws `KeyAlreadyExists`.
