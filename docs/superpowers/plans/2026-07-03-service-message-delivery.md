# ServiceMessage Delivery to Running DurableServices Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a client locate the host running a `DurableService` (via its lease in the `services` TKVL) and deliver a `ServiceMessage` that is routed to the live service instance's `receiveMessage`.

**Architecture:** `ServiceMessage` is a `HostMessage`. The client looks up the lease-holding host with `getServiceHost` (reads the `services` TKVL), sends the message via the existing `HostMessage` path, and the destination `StoreManager` hands it to its `DurableServiceExecutor`, which delivers it to the owning service instance. Delivery is best-effort, like all `HostMessage`s.

**Tech Stack:** Scala 3, ScalaPB / Protocol Buffers (codec), ScalaTest (`AnyFunSuite` for codec, `IntegrationTestSuite`/`AsyncFunSuite` for service tests), sbt.

---

## Background for the implementer

- Aspen serializes network messages through a single ScalaPB `codec.proto` file and a hand-written `Codec` object. Editing `codec.proto` regenerates Scala classes on the next `sbt compile`.
- `HostMessage` is a `sealed abstract class`; adding/handling variants happens in `ProtobufMessageCodec.encodeHostMessage` / `decodeMessage` and in `StoreManager.handleEvent`. The build does **not** use `-Xfatal-warnings`, so a temporarily non-exhaustive match compiles (with a warning) — but we will complete all matches by the end.
- The `services` TKVL is rooted at `Radicle.ServicesTreeKey`, keyed by `Key(serviceUUID)`, valued with a `ServiceEntry` (`typeUUID | hostId | leaseExpiry | statePointer`). `ServiceEntry.isClaimed` is `hostId != ServiceEntry.UnclaimedHostId`.
- Already present (uncommitted): `ServiceMessage` case class in `Message.scala`, and `DurableService.receiveMessage(msg: ServiceMessage)` (currently abstract — which is why the test suite currently fails to compile).

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `src/main/scala/org/aspen_ddp/aspen/compute/DurableService.scala` | Service interface | Make `receiveMessage` a default no-op |
| `src/main/protobuf/codec.proto` | Wire schema | Add `ServiceMessage` message + oneof variant 25 |
| `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` | Hand-written encode/decode | Add `ServiceMessage` pair |
| `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodec.scala` | Unified message codec | Wire encode + decode |
| `src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceExecutor.scala` | Executor interface | Add `deliverMessage` |
| `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala` | Executor impl | Implement `deliverMessage` |
| `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` | Client API | Add `getServiceHost` (abstract) + `sendServiceMessage` (default) |
| `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala` | Client impl | Implement `getServiceHost` |
| `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala` | Server routing | Route `ServiceMessage` to executor |
| `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` | Test harness client | Real `getServiceHost`; capture outbound host messages |
| `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala` | Read-driver test client | Stub `getServiceHost` with `???` |
| `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala` | Service tests | Add delivery / `getServiceHost` / `sendServiceMessage` tests |
| `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodecSuite.scala` | Codec tests | Add `ServiceMessage` round-trip |

---

## Task 1: Make `DurableService.receiveMessage` a default no-op

This unblocks compilation: the existing `DurableServiceSuite` builds an anonymous `DurableService` that implements only `shutdown()`, which currently fails against the abstract `receiveMessage`.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/DurableService.scala`

- [ ] **Step 1: Change `receiveMessage` to a default no-op**

Replace the file contents with:

```scala
package org.aspen_ddp.aspen.compute

import org.aspen_ddp.aspen.common.network.ServiceMessage

trait DurableService:
  /** Called by the executor when this host loses its lease.
   *  The service must stop operating immediately. */
  def shutdown(): Unit

  /** Called by the executor when a ServiceMessage arrives for this service.
   *  Default is a no-op; services that consume messages override this. */
  def receiveMessage(msg: ServiceMessage): Unit = ()
```

- [ ] **Step 2: Verify compilation**

Run: `sbt compile`
Expected: SUCCESS (there may be non-exhaustive-match warnings for `ServiceMessage` in `ProtobufMessageCodec`/`StoreManager` — those are addressed in later tasks and are not errors).

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/DurableService.scala
git commit -m "Make DurableService.receiveMessage a default no-op"
```

---

## Task 2: Add `ServiceMessage` to the wire codec

**Files:**
- Modify: `src/main/protobuf/codec.proto`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodec.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodecSuite.scala`

- [ ] **Step 1: Write the failing round-trip test**

Append this test to `ProtobufMessageCodecSuite` (inside the class, after the last existing test):

```scala
  test("ServiceMessage - roundtrip via encodeHostMessage"):
    val serviceUUID = testUUID(777)
    val content = Array[Byte](1, 2, 3, 4, 5)
    val original = ServiceMessage(hostId1, clientId1, serviceUUID, content)
    val encoded = ProtobufMessageCodec.encodeHostMessage(original)
    val decoded = ProtobufMessageCodec.decodeMessage(encoded)

    decoded shouldBe defined
    decoded.get shouldBe a[ServiceMessage]
    val result = decoded.get.asInstanceOf[ServiceMessage]
    result.toHost shouldBe original.toHost
    result.fromClient shouldBe original.fromClient
    result.serviceUUID shouldBe original.serviceUUID
    result.encodedContent.toList shouldBe original.encodedContent.toList
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *ProtobufMessageCodecSuite -- -z "ServiceMessage - roundtrip"'`
Expected: FAIL — compilation error (`codec.Message.Msg.ServiceMessage` / `codec.ServiceMessage` do not exist yet) or a `scala.MatchError` from the non-exhaustive `encodeHostMessage`.

- [ ] **Step 3: Add the proto message and oneof variant**

In `src/main/protobuf/codec.proto`, add this message immediately after the `CheckStorageDevice` message (after its closing `}` near line 400):

```protobuf
message ServiceMessage {
  UUID toHost = 1;
  UUID fromClient = 2;
  UUID serviceUUID = 3;
  bytes encodedContent = 4;
}
```

Then, inside `message Message { oneof msg { ... } }`, add the next free field number after `TxUnknownStore unknown_store = 24;`:

```protobuf
    ServiceMessage service_message = 25;
```

- [ ] **Step 4: Add the `Codec` encode/decode pair**

In `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`, add this pair immediately after the `CheckStorageDevice` `decode` method (after the line returning `CheckStorageDevice(toHost, fromClient, deviceId)`). `ByteString` is already imported at the top of the file:

```scala
  def encode(o: ServiceMessage): codec.ServiceMessage =
    codec.ServiceMessage(
      toHost = Some(encodeUUID(o.toHost.uuid)),
      fromClient = Some(encodeUUID(o.fromClient.uuid)),
      serviceUUID = Some(encodeUUID(o.serviceUUID)),
      encodedContent = ByteString.copyFrom(o.encodedContent)
    )

  def decode(m: codec.ServiceMessage): ServiceMessage =
    val toHost = HostId(decodeUUID(m.toHost.get))
    val fromClient = ClientId(decodeUUID(m.fromClient.get))
    val serviceUUID = decodeUUID(m.serviceUUID.get)
    ServiceMessage(toHost, fromClient, serviceUUID, m.encodedContent.toByteArray)
```

Note: the generated ScalaPB field accessors are `toHost`, `fromClient`, `serviceUUID`, `encodedContent` (the proto field names have no underscores, so they are preserved as-is). If `sbt compile` reports different names, match the compiler's names.

- [ ] **Step 5: Wire `ProtobufMessageCodec`**

In `encodeHostMessage`'s `match`, add a case alongside the others:

```scala
      case m: ServiceMessage => codec.Message.Msg.ServiceMessage(Codec.encode(m))
```

In `decodeMessage`'s `m.msg match`, add a case alongside the others (e.g., after the `CheckStorageDevice` case):

```scala
        case codec.Message.Msg.ServiceMessage(r) =>
          Some(Codec.decode(r))
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `sbt 'testOnly *ProtobufMessageCodecSuite -- -z "ServiceMessage - roundtrip"'`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/main/protobuf/codec.proto \
        src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodec.scala \
        src/test/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodecSuite.scala
git commit -m "Add ServiceMessage wire codec + round-trip test"
```

---

## Task 3: Executor delivery — `deliverMessage`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceExecutor.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala`

- [ ] **Step 1: Add a recording hook to the test fixture**

In `DurableServiceSuite`, add a `receivedPromise` var next to the existing promises, reset it in `subFixtureSetup`, and make `stableFactory`'s service record received messages.

Add the import at the top of the file (with the other `common` imports):

```scala
import org.aspen_ddp.aspen.common.network.ServiceMessage
```

Add the promise declaration next to the existing `claimedPromise`/`shutdownPromise` vars:

```scala
  private var receivedPromise: Promise[ServiceMessage] = scala.compiletime.uninitialized
```

Update `subFixtureSetup` to reset it:

```scala
  override def subFixtureSetup(): Unit =
    claimedPromise  = Promise[Unit]()
    shutdownPromise = Promise[Unit]()
    receivedPromise = Promise[ServiceMessage]()
```

Replace `stableFactory` so its created service records messages:

```scala
  private val stableFactory: DurableServiceFactory = new DurableServiceFactory:
    val typeUUID: UUID = fixedTypeUUID
    def createService(c: AspenClient, ptr: KeyValueObjectPointer, state: KeyValueObjectState): DurableService =
      claimedPromise.trySuccess(())
      new DurableService:
        def shutdown(): Unit = shutdownPromise.trySuccess(())
        override def receiveMessage(msg: ServiceMessage): Unit = receivedPromise.trySuccess(msg)
```

- [ ] **Step 2: Write the failing delivery tests**

Append these two tests to `DurableServiceSuite`:

```scala
  atest("deliverMessage routes to the owning service's receiveMessage"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val content = Array[Byte](1, 2, 3)
    val exec = makeExecutor()
    for
      _   <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
      _   <- claimedPromise.future
      _    = exec.deliverMessage(ServiceMessage(testHostId, client.clientId, svcUUID, content))
      msg <- receivedPromise.future
    yield
      exec.shutdown()
      msg.serviceUUID shouldBe svcUUID
      msg.encodedContent.toList shouldBe content.toList

  atest("deliverMessage is a no-op for a service this host does not own"):
    given ExecutionContext = executionContext
    val exec = makeExecutor()
    noException should be thrownBy exec.deliverMessage(
      ServiceMessage(testHostId, client.clientId, UUID.randomUUID(), Array.emptyByteArray))
    exec.shutdown()
    Future.successful(succeed)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `sbt 'testOnly *DurableServiceSuite -- -z "deliverMessage"'`
Expected: FAIL — `deliverMessage` is not a member of `SimpleDurableServiceExecutor` (compilation error).

- [ ] **Step 4: Add `deliverMessage` to the executor trait**

In `src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceExecutor.scala`, add the import and the method to the trait:

```scala
package org.aspen_ddp.aspen.compute

import java.util.UUID
import org.aspen_ddp.aspen.common.network.ServiceMessage
import org.aspen_ddp.aspen.common.objects.Key

import scala.concurrent.Future

trait DurableServiceExecutor:
  /** Idempotent. Creates state KVO and an immediately-claimable TKVL entry.
   *  No-op if the service UUID is already registered. */
  def registerService(
    typeUUID: UUID,
    serviceUUID: UUID,
    initialState: Map[Key, Array[Byte]]
  ): Future[Unit]

  /** Removes the TKVL entry. Calls shutdown() on the service if this host owns it. */
  def unregisterService(serviceUUID: UUID): Future[Unit]

  /** Delivers a message to a service running on this host. No-op if the service
   *  is not currently owned by this host. */
  def deliverMessage(msg: ServiceMessage): Unit
```

- [ ] **Step 5: Implement `deliverMessage` in `SimpleDurableServiceExecutor`**

Add the import (with the other `common` imports at the top of the file):

```scala
import org.aspen_ddp.aspen.common.network.ServiceMessage
```

Add the method to the class body (e.g., immediately after the `shutdown()` method):

```scala
  override def deliverMessage(msg: ServiceMessage): Unit = synchronized:
    ownedServices.get(msg.serviceUUID).foreach: (service, _) =>
      service.receiveMessage(msg)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `sbt 'testOnly *DurableServiceSuite -- -z "deliverMessage"'`
Expected: PASS (both tests)

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/compute/DurableServiceExecutor.scala \
        src/main/scala/org/aspen_ddp/aspen/compute/impl/SimpleDurableServiceExecutor.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala
git commit -m "Add DurableServiceExecutor.deliverMessage + tests"
```

---

## Task 4: Client lookup — `getServiceHost`

`getServiceHost` is abstract on the `AspenClient` trait and implemented in `SimpleAspenClient`. The service tests run against `TestNetwork.TClient`, so that test client gets a **real** implementation too; `BaseReadDriverSuite.TClient` (never touches services) gets a `???` stub.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala`

- [ ] **Step 1: Write the failing `getServiceHost` tests**

Append to `DurableServiceSuite`:

```scala
  atest("getServiceHost returns the claiming host"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val exec = makeExecutor()
    for
      _  <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
      _  <- claimedPromise.future
      oh <- client.getServiceHost(svcUUID)
    yield
      exec.shutdown()
      oh shouldBe Some(testHostId)

  atest("getServiceHost returns None for an unknown service"):
    given ExecutionContext = executionContext
    for
      oh <- client.getServiceHost(UUID.randomUUID())
    yield
      oh shouldBe None
```

- [ ] **Step 2: Run to verify failure**

Run: `sbt 'testOnly *DurableServiceSuite -- -z "getServiceHost"'`
Expected: FAIL — `getServiceHost` is not a member of `AspenClient` (compilation error).

- [ ] **Step 3: Add `getServiceHost` to the `AspenClient` trait**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, add `ServiceMessage` to the `common.network` import (it will also be used in Task 5):

```scala
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, ClientId, ClientResponse, HostMessage, ServiceMessage}
```

Add the abstract method next to the other `private[aspen]` declarations near the end of the trait (e.g., after `sendHostMessage`):

```scala
  private[aspen] def getServiceHost(serviceUUID: UUID): Future[Option[HostId]]
```

- [ ] **Step 4: Implement `getServiceHost` in `SimpleAspenClient`**

Add the import (with the other `compute`/`common` imports):

```scala
import org.aspen_ddp.aspen.compute.ServiceEntry
```

Add a cached TKVL and the method (e.g., after the `sendHostMessage` method):

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

- [ ] **Step 5: Implement `getServiceHost` for real in `TestNetwork.TClient`**

In `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`, add the import (with the other imports at the top of the file):

```scala
import org.aspen_ddp.aspen.compute.ServiceEntry
```

Add to the `TClient` class body (e.g., after `sendHostMessage`):

```scala
    private lazy val servicesTkvl =
      TieredKeyValueList(this, KVObjectRootManager(this, Radicle.ServicesTreeKey, radicle))

    private[aspen] def getServiceHost(serviceUUID: UUID): Future[Option[HostId]] =
      servicesTkvl.get(Key(serviceUUID)).map:
        case None => None
        case Some(vs) =>
          val entry = ServiceEntry.decode(vs.value.bytes)
          if entry.isClaimed then Some(HostId(entry.hostId)) else None
```

(`TieredKeyValueList`, `KVObjectRootManager`, `Radicle`, `Key`, `HostId`, and `Future` are already imported in this file.)

- [ ] **Step 6: Stub `getServiceHost` in `BaseReadDriverSuite.TClient`**

In `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala`, add a stub alongside the other `???` members of `TClient`:

```scala
    private[aspen] def getServiceHost(serviceUUID: UUID): Future[Option[HostId]] = ???
```

If `UUID`, `HostId`, or `Future` are not already imported in that file, add the missing imports (`java.util.UUID`, `org.aspen_ddp.aspen.common.metadata.HostId`, `scala.concurrent.Future`). Verify against the existing imports first — most are already present.

- [ ] **Step 7: Run to verify pass**

Run: `sbt 'testOnly *DurableServiceSuite -- -z "getServiceHost"'`
Expected: PASS (both tests)

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala \
        src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala
git commit -m "Add AspenClient.getServiceHost + tests"
```

---

## Task 5: Client send helper — `sendServiceMessage`

`sendServiceMessage` is a concrete default on the `AspenClient` trait: it looks up the host and sends. We test it by capturing outbound host messages in `TestNetwork`.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala`

- [ ] **Step 1: Capture outbound host messages in `TestNetwork`**

In `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`, add a public buffer to the `TestNetwork` class (e.g., near `var handleDepth = 0`):

```scala
  var capturedHostMessages: List[HostMessage] = Nil
```

Change the client messenger's `sendHostMessage` (currently `def sendHostMessage(msg: HostMessage): Unit = ()` inside `cliMessenger`) to record:

```scala
    def sendHostMessage(msg: HostMessage): Unit = synchronized:
      capturedHostMessages = capturedHostMessages :+ msg
```

- [ ] **Step 2: Write the failing `sendServiceMessage` test**

Append to `DurableServiceSuite`:

```scala
  atest("sendServiceMessage sends a ServiceMessage to the owning host"):
    given ExecutionContext = executionContext
    val svcUUID = UUID.randomUUID()
    val content = Array[Byte](9, 8, 7)
    val exec = makeExecutor()
    for
      _ <- exec.registerService(fixedTypeUUID, svcUUID, Map.empty)
      _ <- claimedPromise.future
      _ <- client.sendServiceMessage(svcUUID, content)
    yield
      exec.shutdown()
      val sent = net.capturedHostMessages.collect:
        case m: ServiceMessage if m.serviceUUID == svcUUID => m
      sent.size shouldBe 1
      sent.head.toHost shouldBe testHostId
      sent.head.fromClient shouldBe client.clientId
      sent.head.encodedContent.toList shouldBe content.toList
```

- [ ] **Step 3: Run to verify failure**

Run: `sbt 'testOnly *DurableServiceSuite -- -z "sendServiceMessage"'`
Expected: FAIL — `sendServiceMessage` is not a member of `AspenClient` (compilation error).

- [ ] **Step 4: Add `sendServiceMessage` to the `AspenClient` trait**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, add the concrete method (e.g., immediately before the `getServiceHost` abstract declaration):

```scala
  def sendServiceMessage(serviceUUID: UUID, encodedContent: Array[Byte]): Future[Unit] =
    given ExecutionContext = clientContext
    getServiceHost(serviceUUID).map:
      case Some(hostId) =>
        sendHostMessage(ServiceMessage(hostId, clientId, serviceUUID, encodedContent))
      case None => ()
```

(`ServiceMessage` was added to the imports in Task 4, Step 3.)

- [ ] **Step 5: Run to verify pass**

Run: `sbt 'testOnly *DurableServiceSuite -- -z "sendServiceMessage"'`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala \
        src/test/scala/org/aspen_ddp/aspen/compute/DurableServiceSuite.scala
git commit -m "Add AspenClient.sendServiceMessage + capture-based test"
```

---

## Task 6: Server routing — deliver `ServiceMessage` in `StoreManager`

This is the final piece of glue: route an inbound `ServiceMessage` to the host's service executor. The behavior it depends on (`deliverMessage`) is already tested in Task 3, and inbound end-to-end routing is not cleanly testable in the current harness (the harness `StoreManager` uses `NoBackgroundTaskManager`, so its own executor never claims services). This task is therefore verified by compilation and by completing the previously non-exhaustive `HostMsg` match.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala`

- [ ] **Step 1: Add the routing case**

In `handleEvent`, the `HostMsg(msg)` branch currently matches `StartStoreTransfer`, `StoreTransferData`, and `CheckStorageDevice`. Add a `ServiceMessage` case:

```scala
      case HostMsg(msg) => msg match
        case m: StartStoreTransfer => startStoreTransferOut(m)
        case m: StoreTransferData => transferDataReceived(m)
        case m: CheckStorageDevice => checkStorageDevice(m.deviceId)
        case m: ServiceMessage =>
          serviceExecutorPromise.future.foreach(_.deliverMessage(m))
```

`ServiceMessage` is available via the existing `import org.aspen_ddp.aspen.common.network.*`. `serviceExecutorPromise.future.foreach` runs on the client execution context and no-ops until the executor is initialized; the executor's own `synchronized` guards delivery.

- [ ] **Step 2: Verify compilation and full test suite**

Run: `sbt compile`
Expected: SUCCESS, and the `ServiceMessage` non-exhaustive-match warnings from Task 1 are now gone (all `HostMessage` variants handled in both `ProtobufMessageCodec` and `StoreManager`).

Run: `sbt 'testOnly *DurableServiceSuite *ProtobufMessageCodecSuite'`
Expected: PASS (all tests, including the pre-existing ones).

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala
git commit -m "Route inbound ServiceMessage to the service executor"
```

---

## Final verification

- [ ] **Run the full test suite**

Run: `sbt test`
Expected: PASS (no regressions).

---

## Notes / limitations

- Delivery is best-effort. A `ServiceMessage` is dropped when: the client's `getServiceHost` returns `None` (unclaimed service); the destination host is offline; the lease has moved to another host; or the destination `StoreManager`'s service executor is not yet initialized.
- Per the approved design, `getServiceHost` returns the **last recorded holder** whenever the entry is claimed, ignoring lease expiry; it returns `None` only for unclaimed/absent entries.
- Server-side inbound routing (Task 6) has no dedicated automated test because the integration harness's `StoreManager` uses `NoBackgroundTaskManager` and never claims services on its own executor. The delegated behavior (`deliverMessage`) is covered by Task 3.
