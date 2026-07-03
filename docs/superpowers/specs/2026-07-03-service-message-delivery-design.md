# ServiceMessage Delivery to Running DurableServices

**Date:** 2026-07-03
**Status:** Approved

## Purpose

Enable a client to send a message to a `DurableService` that is currently
executing on some host in the Aspen system. Services are located by looking up
the host that holds the service's lease in the `services` TKVL, then delivering a
`ServiceMessage` to that host, which routes it to the running service instance.

Delivery is best-effort, consistent with the rest of the `HostMessage` family:
messages may be lost if the host is offline, the lease has moved, or the service
executor has not yet initialized. Callers that require reliable delivery should
build on durable state polling rather than relying on `ServiceMessage`.

## Background

- `ServiceMessage` is already defined in `common/network/Message.scala` as a
  `HostMessage`:
  ```scala
  final case class ServiceMessage(
    toHost: HostId,
    fromClient: ClientId,
    serviceUUID: UUID,
    encodedContent: Array[Byte]
  ) extends HostMessage
  ```
- `DurableService.receiveMessage(msg: ServiceMessage): Unit` is already declared
  in `compute/DurableService.scala` (currently abstract).
- Each service is registered in the `services` TKVL (rooted at
  `Radicle.ServicesTreeKey`), keyed by `serviceUUID`, with a `ServiceEntry` value
  encoding `typeUUID | hostId | leaseExpiry | statePointer`.
- `SimpleDurableServiceExecutor` owns running instances in a private
  `ownedServices: Map[UUID, (DurableService, ScheduledTask)]`.
- `HostMessage`s flow: network → `ProtobufMessageCodec.decodeMessage` →
  `MessageHandler` → `StoreManager.receiveHostMessage` → `HostMsg` event →
  `StoreManager.handleEvent`.

## Design Decisions

1. **Expired lease handling in `getServiceHost`:** return the *last holder*.
   `getServiceHost` returns `Some(hostId)` whenever the `ServiceEntry` is claimed,
   even if the lease has expired. It returns `None` only when the entry is
   unclaimed (`hostId == ServiceEntry.UnclaimedHostId`).
2. **Delivery surface:** provide both the `getServiceHost` primitive and a
   convenience `sendServiceMessage(serviceUUID, encodedContent)` helper that does
   the lookup and send in one call.
3. **`receiveMessage` default:** give `DurableService.receiveMessage` a default
   no-op implementation so services that do not consume messages compile
   unchanged and only message-consuming services override it.

## Changes

### 1. `src/main/protobuf/codec.proto`

Add the message definition (mirroring `CheckStorageDevice`) and register it as
oneof variant **25** — the next free field number after `unknown_store = 24`:

```protobuf
message ServiceMessage {
  UUID toHost = 1;
  UUID fromClient = 2;
  UUID serviceUUID = 3;
  bytes encodedContent = 4;
}
```
```protobuf
// inside Message.oneof msg:
ServiceMessage service_message = 25;
```

### 2. `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`

Add an `encode`/`decode` pair following the `CheckStorageDevice` pattern:

```scala
def encode(o: ServiceMessage): codec.ServiceMessage =
  codec.ServiceMessage(
    toHost = Some(encodeUUID(o.toHost.uuid)),
    fromClient = Some(encodeUUID(o.fromClient.uuid)),
    serviceUUID = Some(encodeUUID(o.serviceUUID)),
    encodedContent = com.google.protobuf.ByteString.copyFrom(o.encodedContent)
  )

def decode(m: codec.ServiceMessage): ServiceMessage =
  val toHost = HostId(decodeUUID(m.toHost.get))
  val fromClient = ClientId(decodeUUID(m.fromClient.get))
  val serviceUUID = decodeUUID(m.serviceUUID.get)
  ServiceMessage(toHost, fromClient, serviceUUID, m.encodedContent.toByteArray)
```

(The exact generated field names — e.g. `serviceUUID` vs `serviceUuid` — must be
confirmed against ScalaPB output during implementation and matched accordingly.
Confirm the correct `ByteString` import used elsewhere in `Codec.scala`.)

### 3. `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodec.scala`

- In `encodeHostMessage`, add:
  ```scala
  case m: ServiceMessage => codec.Message.Msg.ServiceMessage(Codec.encode(m))
  ```
- In `decodeMessage`, add:
  ```scala
  case codec.Message.Msg.ServiceMessage(r) => Some(Codec.decode(r))
  ```

### 4. `src/main/scala/org/aspen_ddp/aspen/compute/DurableService.scala`

Make `receiveMessage` a default no-op:

```scala
def receiveMessage(msg: ServiceMessage): Unit = ()
```

### 5. `src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceExecutor.scala`

Add to the trait:

```scala
/** Delivers a message to a service running on this host. No-op if the service
 *  is not currently owned by this host. */
def deliverMessage(msg: ServiceMessage): Unit
```

### 6. `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala`

Implement `deliverMessage`, synchronized like all other `ownedServices` access:

```scala
override def deliverMessage(msg: ServiceMessage): Unit = synchronized:
  ownedServices.get(msg.serviceUUID).foreach: (service, _) =>
    service.receiveMessage(msg)
```

### 7. `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`

In `handleEvent`'s `HostMsg(msg)` match, add:

```scala
case m: ServiceMessage =>
  serviceExecutorPromise.future.foreach(_.deliverMessage(m))
```

`.foreach` on the promise's future guards the pre-initialization window (message
dropped, best-effort). The executor's own `synchronized` handles thread safety;
`serviceExecutorPromise.future.foreach` runs its callback on the client execution
context, not the `StoreManager` event thread.

### 8. `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`

Add to the trait:

```scala
private[aspen] def getServiceHost(serviceUUID: UUID): Future[Option[HostId]]

def sendServiceMessage(serviceUUID: UUID, encodedContent: Array[Byte]): Future[Unit] =
  given ExecutionContext = clientContext
  getServiceHost(serviceUUID).map:
    case Some(hostId) =>
      sendHostMessage(ServiceMessage(hostId, clientId, serviceUUID, encodedContent))
    case None => ()
```

`ServiceMessage` must be added to the `common.network` import in this file.

### 9. `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala`

Cache a `servicesTkvl` and implement `getServiceHost`, using the client's own
`radicle` pointer:

```scala
private lazy val servicesTkvl =
  TieredKeyValueList(this, KVObjectRootManager(this, Radicle.ServicesTreeKey, radicle))

override def getServiceHost(serviceUUID: UUID): Future[Option[HostId]] =
  servicesTkvl.get(Key(serviceUUID)).map:
    case None => None
    case Some(vs) =>
      val entry = ServiceEntry.decode(vs.value.bytes)
      if entry.isClaimed then Some(HostId(entry.hostId)) else None
```

Requires importing `ServiceEntry` from `compute`. `KVObjectRootManager`,
`TieredKeyValueList`, `Radicle`, `Key`, and `HostId` are already imported.

### 10. Test client stubs

Both test `AspenClient` implementations must provide `getServiceHost`, matching
how they stub other abstract members with `???`:

- `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`
- `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala`

```scala
private[aspen] def getServiceHost(serviceUUID: UUID): Future[Option[HostId]] = ???
```

## Testing

1. **Codec round-trip** — encode then decode a `ServiceMessage` and assert all
   fields (including `encodedContent` bytes) survive. Placed with the existing
   message codec tests.
2. **`getServiceHost`** (in `DurableServiceSuite`):
   - Register and claim a service; assert `client.getServiceHost(svcUUID)` returns
     `Some(claimingHostId)`.
   - Assert an unregistered / unknown UUID returns `None`.
3. **Delivery** (in `DurableServiceSuite`):
   - Use a `DurableService` that records the received `ServiceMessage`; call
     `executor.deliverMessage(msg)` and assert `receiveMessage` fired with the
     correct message.
   - Call `deliverMessage` for a UUID not in `ownedServices` and assert it is a
     silent no-op (no exception).

The existing test's anonymous `DurableService` (which only implements
`shutdown()`) compiles unchanged thanks to the `receiveMessage` default no-op.

## Out of Scope

- Reliable / retried delivery of `ServiceMessage` (best-effort only).
- Any change to how services are claimed, renewed, or unregistered.
- A response/reply channel from service back to client.
