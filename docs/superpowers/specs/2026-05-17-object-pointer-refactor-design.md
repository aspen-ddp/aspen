# ObjectPointer Refactor Design

## Goal

Remove redundant information from `ObjectPointer` that is available from the associated `StoragePool`. Specifically:

- Remove `ida: IDA` (available as `StoragePool.defaultIDA`)
- Remove `size: Option[Int]` (available as `StoragePool.maxObjectSize`)
- Remove `storePointers: Array[StorePointer]` and the `StorePointer` type entirely
- Add `storePointer: Array[Byte]` — a single byte array shared across all stores (may be empty)

All stores within a pool host every object. No object uses a subset of stores, so the per-store index map is no longer needed.

Backward compatibility with existing encoded pointers is not a concern.

## New ObjectPointer Structure

### Before

```scala
sealed abstract class ObjectPointer(
  val id: ObjectId,
  val poolId: PoolId,
  val size: Option[Int],
  val ida: IDA,
  val storePointers: Array[StorePointer])
```

### After

```scala
sealed abstract class ObjectPointer(
  val id: ObjectId,
  val poolId: PoolId,
  val storePointer: Array[Byte])
```

### Removed Methods

All methods that derived store membership from the per-store pointer array are removed:

- `getStorePointer(storeId)` 
- `getStoreLocater(storeId)`
- `hostingStores`
- `getEncodedDataIndexForStore(storeId)`

Store membership is now derived from the `StoragePool`'s `numberOfStores` and `defaultIDA.width`.

### Subclasses

`DataObjectPointer` and `KeyValueObjectPointer` constructors updated to match the new signature: `(id, poolId, storePointer)`.

## Binary Encoding Format

The new self-describing format:

```
[varint: total length] [byte: type code] [16 bytes: object UUID] [16 bytes: pool UUID] [remaining: storePointer]
```

- `storePointer` bytes consume whatever remains after the type code and two UUIDs (i.e. `totalLength - 1 - 32`).
- If `storePointer` is empty, total length is 33.
- No separate length encoding is needed for the store pointer since it fills the remainder.

## Protobuf Changes

### ObjectPointer Message

```protobuf
message ObjectPointer {
  UUID uuid = 1;
  UUID pool_uuid = 2;
  ObjectType object_type = 3;
  bytes store_pointer = 4;
}
```

The `size`, `ida`, and `repeated StorePointer store_pointers` fields are removed. The `StorePointer` message is deleted.

### TransactionDescription Message

New field added:

```protobuf
message PoolIDA {
  UUID pool_id = 1;
  IDA ida = 2;
}
```

Added to `TransactionDescription`:

```protobuf
repeated PoolIDA pool_idas = <next_field_number>;
```

### AllocateResponse

The `StorePointer` field is removed. The response becomes a simple success/failure signal.

## Types Removed

### StorePointer

The `StorePointer` case class (`common/store/StorePointer.scala`) is deleted entirely. All usages are removed.

### Locater

`Locater` currently wraps `(ObjectId, StorePointer)`. With per-store data gone, it is removed entirely. All usages are replaced with bare `ObjectId`.

## TransactionDescription Changes

### New Fields

```scala
primaryObjectIDA: IDA,
poolIDAMap: Map[PoolId, IDA]
```

`primaryObjectIDA` is technically redundant with `poolIDAMap(primaryObject.poolId)` but keeps the hot path (Paxos setup in `TransactionDriver`) simple and explicit.

### Reworked Methods

- **`primaryObjectDataStores`** — uses `primaryObjectIDA.width` to generate `StoreId(poolId, 0)` through `StoreId(poolId, width-1)`.
- **`allDataStores`** — for each referenced object, looks up `poolIDAMap(ptr.poolId).width` to generate that object's store set.
- **`allHostedObjects(storeId)`** — checks `storeId.poolIndex < poolIDAMap(storeId.poolId).width`.
- **`hostedObjectLocaters(storeId)`** — same check, returns `List[ObjectId]` instead of `List[Locater]`.

### TransactionBuilder (Client Side)

The `buildTranaction` method currently uses `pointer.ida` in several places:

- **Primary object selection:** `maxBy(ptr => ptr.ida)` — selects the object whose IDA has the highest failure tolerance. Changes to `maxBy(ptr => poolIDAMap(ptr.poolId))`.
- **Data distribution:** `pointer.storePointers zip encoded` — distributes IDA-encoded data to per-store updates. Changes to iterate stores `0..ida.width-1` using the pool's IDA width.
- **Data encoding:** `objectPointer.ida.encode(buf)` and `KeyValueOperation.encode(ops, pointer.ida)` — uses pool IDA from `poolIDAMap`.

Since `buildTranaction` is currently synchronous but pool lookups are async, the `poolIDAMap` must be pre-built before calling `buildTranaction`. The map is passed as a parameter to `buildTranaction`. The caller (the `Transaction` commit path) resolves all pool IDAs via `getStoragePool` before invoking the builder.

The builder also populates the new `TransactionDescription` fields (`primaryObjectIDA` and `poolIDAMap`) from this map.

## TransactionDriver (Server Side)

Gets IDA from `txd.primaryObjectIDA` instead of `txd.primaryObject.ida`. The valid acceptor set is derived from `primaryObjectIDA.width` (stores `0..width-1`) rather than iterating `storePointers`.

## ObjectState and Read Path

### ObjectState / KVObjectState

Both gain `ida: IDA` as a constructor parameter. All methods that previously accessed `pointer.ida` (encode, decode, size calculations, `getRebuildDataForStore`) use this field instead.

### Read Path Flow

1. `AspenClient.read()` resolves the pool's IDA via `getStoragePool(pointer.poolId)` before constructing readers.
2. `ReadManager` / `BaseReadDriver` receives IDA at construction for `width`, `consistentRestoreThreshold`, and determining which stores to send read requests to.
3. `DataObjectReader` / `KeyValueObjectReader` receives IDA at construction for `restore()` / `restoreArray()`.
4. The resulting `ObjectState` carries the IDA for later use.

Pool/IDA lookups use async `getStoragePool` calls. Caching can be added within `getStoragePool` in the future.

## Allocation Path

- `AllocationDriver` and backends drop `StorePointer` return values from allocation methods.
- `AllocateResponse` message drops `result: Option[StorePointer]` — becomes a simple success/failure signal.
- `AllocationDriver.receiveAllocationResult` drops the `result` parameter.
- The constructed `ObjectPointer` uses the `storePointer: Array[Byte]` from the allocation request (currently always `Array.empty`).
- `AllocationRecoveryState` drops its `storePointer` field.

## AmoebaFS and TKVL

Sites that currently access `pointer.ida`:

- **`SimpleFileContent`** — uses `pointer.ida.calculateEncodedSegmentLength`. Receives IDA from pool lookup or passed through from context.
- **`KeyValueListNode`** — uses `pointer.ida` for encoded pair size calculations. Receives IDA from pool lookup or passed through from context.
- **`SimpleFileSystem`** — uses `fsRoot.ida` to create allocator. Uses pool's `defaultIDA` instead.

## Codec

Updated for all protobuf changes:

- `encode(ObjectPointer)` / `decode(ObjectPointer)` — simplified to handle new fields only.
- `encode(TransactionDescription)` / `decode(TransactionDescription)` — adds `poolIDAMap` encoding/decoding.
- `encode(AllocateResponse)` / `decode(AllocateResponse)` — removes `StorePointer` handling.
- `StorePointer` encode/decode methods removed.
