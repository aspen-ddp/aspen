# Generalized ObjectRegistry Design

## Overview

Generalize the existing `ObjectRegistry` from a UUID-keyed registry to a generic `Key`-based registry, then provide two typed wrappers: `UUIDObjectRegistry` (keyed by UUID) and `NamespacedObjectRegistry` (keyed by namespace + name strings).

## Motivation

The current `ObjectRegistry` hardcodes UUID keys, but the underlying TKVL operates on `Key` instances. Generalizing to `Key` makes the registry reusable for any key type. Typed wrappers provide ergonomic APIs for common key patterns without duplicating logic.

## Package Structure

All classes move to `org.aspen_ddp.aspen.client.registries`:

- `ObjectRegistry.scala` — generic Key-based registry
- `UUIDObjectRegistry.scala` — UUID wrapper
- `NamespacedObjectRegistry.scala` — namespace+name wrapper

## ObjectRegistry (Generic)

**File:** `src/main/scala/org/aspen_ddp/aspen/client/registries/ObjectRegistry.scala`

### Constructor

```scala
class ObjectRegistry(
  val client: AspenClient,
  containingObjectPtr: KeyValueObjectPointer,
  treeRootKey: Key
)
```

Same as today. Internally creates a TKVL via `KVObjectRootManager`.

### Companion Object

```scala
object ObjectRegistry:
  class DuplicateRegistration(val key: Key, val existing: ObjectPointer)
    extends Exception(s"Duplicate registration for key $key")
```

`DuplicateRegistration` takes a `Key` instead of `UUID`.

### Methods

All methods take `Key` instead of `UUID`. The internal implementation drops the `Key(objectId)` conversions since the input is already a `Key`:

- `getRegisteredObject(key: Key): Future[ObjectPointer]`
- `getRegisteredKeyValueObject(key: Key): Future[KeyValueObjectPointer]`
- `getRegisteredDataObject(key: Key): Future[DataObjectPointer]`
- `prepareRegisterObject(key: Key, pointer: ObjectPointer)(using tx: Transaction): Future[Unit]`
- `registerObject(key: Key, pointer: ObjectPointer): Future[Unit]`

Logic is identical to the current implementation — just replacing `objectId: UUID` parameters with `key: Key` and removing the `Key(objectId)` wrapping.

## UUIDObjectRegistry

**File:** `src/main/scala/org/aspen_ddp/aspen/client/registries/UUIDObjectRegistry.scala`

Wraps an `ObjectRegistry` instance. All methods take `UUID`, convert via `Key(uuid)`, and forward:

```scala
class UUIDObjectRegistry(val client: AspenClient,
                         containingObjectPtr: KeyValueObjectPointer,
                         treeRootKey: Key):

  private val registry = ObjectRegistry(client, containingObjectPtr, treeRootKey)

  def getRegisteredObject(objectId: UUID): Future[ObjectPointer] =
    registry.getRegisteredObject(Key(objectId))

  def getRegisteredKeyValueObject(objectId: UUID): Future[KeyValueObjectPointer] =
    registry.getRegisteredKeyValueObject(Key(objectId))

  def getRegisteredDataObject(objectId: UUID): Future[DataObjectPointer] =
    registry.getRegisteredDataObject(Key(objectId))

  def prepareRegisterObject(objectId: UUID, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    registry.prepareRegisterObject(Key(objectId), pointer)

  def registerObject(objectId: UUID, pointer: ObjectPointer): Future[Unit] =
    registry.registerObject(Key(objectId), pointer)
```

## NamespacedObjectRegistry

**File:** `src/main/scala/org/aspen_ddp/aspen/client/registries/NamespacedObjectRegistry.scala`

Same wrapping pattern. Key formation uses `namespace.name` string format:

```scala
object NamespacedObjectRegistry:
  def makeKey(namespace: String, name: String): Key = Key(s"$namespace.$name")

class NamespacedObjectRegistry(val client: AspenClient,
                               containingObjectPtr: KeyValueObjectPointer,
                               treeRootKey: Key):

  import NamespacedObjectRegistry.makeKey

  private val registry = ObjectRegistry(client, containingObjectPtr, treeRootKey)

  def getRegisteredObject(namespace: String, name: String): Future[ObjectPointer] =
    registry.getRegisteredObject(makeKey(namespace, name))

  def getRegisteredKeyValueObject(namespace: String, name: String): Future[KeyValueObjectPointer] =
    registry.getRegisteredKeyValueObject(makeKey(namespace, name))

  def getRegisteredDataObject(namespace: String, name: String): Future[DataObjectPointer] =
    registry.getRegisteredDataObject(makeKey(namespace, name))

  def prepareRegisterObject(namespace: String, name: String, pointer: ObjectPointer)(using tx: Transaction): Future[Unit] =
    registry.prepareRegisterObject(makeKey(namespace, name), pointer)

  def registerObject(namespace: String, name: String, pointer: ObjectPointer): Future[Unit] =
    registry.registerObject(makeKey(namespace, name), pointer)
```

## Tests

### ObjectRegistrySuite

**File:** `src/test/scala/org/aspen_ddp/aspen/client/registries/ObjectRegistrySuite.scala`

The existing test suite adapted to use `Key` directly instead of `UUID`. All seven existing test cases remain, with `UUID.randomUUID()` replaced by arbitrary `Key` values (e.g. `Key("test-object-1")`). This is the comprehensive test suite since it exercises the real logic.

### UUIDObjectRegistrySuite

**File:** `src/test/scala/org/aspen_ddp/aspen/client/registries/UUIDObjectRegistrySuite.scala`

Basic tests since the wrapper just converts and forwards:

1. **Register and retrieve** — register with UUID, retrieve, verify equality.
2. **Duplicate detection** — register same UUID with different pointer, verify `DuplicateRegistration`.

### NamespacedObjectRegistrySuite

**File:** `src/test/scala/org/aspen_ddp/aspen/client/registries/NamespacedObjectRegistrySuite.scala`

Basic tests:

1. **Register and retrieve** — register with namespace+name, retrieve, verify equality.
2. **Duplicate detection** — register same namespace+name with different pointer, verify `DuplicateRegistration`.
3. **makeKey format** — verify `NamespacedObjectRegistry.makeKey("ns", "obj")` produces `Key("ns.obj")`.
