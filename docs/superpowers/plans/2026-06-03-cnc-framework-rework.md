# CnC Framework Rework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the CnC networking classes with a generic, message-agnostic framework mirroring the `ClientMessenger`/`ServerMessenger` + `MessageHandler` pattern, removing all obsolete CnC messages.

**Architecture:** Generic `CnCFrontend`/`CnCBackend`/`CnCMessageReceiver` interfaces and a consolidated `CnCMessage.scala` live in `common.network`. The ZeroMQ implementations (`ZCnCFrontend`/`ZCnCBackend`) move to `common.network.implementations.zmqnet`, stay standalone (borrowing `ZMQNet`'s `ZContext`), and use a generic byte-carrier wire envelope (`CnCRequest { msgType, payload }`) so the wire layer never changes when future messages are added. The old `server.cnc` package is deleted.

**Tech Stack:** Scala 3, ScalaPB (protobuf codegen via `sbt compile`), ZeroMQ (jeromq), ScalaTest.

---

## Background the implementer needs

- **`Codec.scala` lives in package `org.aspen_ddp.aspen.common.network`** — the same package as the new `CnCRequest`/`CnCReply`/`CnCMessageReceiver`. So `Codec` and the test suite reference those types **without an import**.
- **ScalaPB regenerates the `org.aspen_ddp.aspen.codec` types from `codec.proto` during `sbt compile`.** Changing the proto's CnC messages immediately breaks every Scala file that references the old generated types (`codec.NewStore`, `codec.CnCRequest.Msg.NewStore`, etc.). This is why the proto/Codec/impl/test changes in Task 2 form **one atomic commit** — the tree does not compile in between.
- **`ZMQNet.context` is a public `val context = new ZContext()`** (`ZMQNet.scala:93`). Standalone CnC classes create their own sockets from it.
- **Do NOT touch `StoreManager.scala:52` `ShutdownStore`** — it is an unrelated internal `Event` case class, not a CnC message.
- **`HostState`** has fields `address: String` and `cncPort: Int` (and `hostId, name, dataPort, storeTransferPort, storageDevices`).

---

## File Structure

**Created (Task 1 — `common.network`):**
- `src/main/scala/org/aspen_ddp/aspen/common/network/CnCMessage.scala` — sealed `CnCRequest` base (empty) + `CnCReply` base with nested `Ok`/`Error`.
- `src/main/scala/org/aspen_ddp/aspen/common/network/CnCFrontend.scala` — sender trait.
- `src/main/scala/org/aspen_ddp/aspen/common/network/CnCBackend.scala` — marker trait for the listening side.
- `src/main/scala/org/aspen_ddp/aspen/common/network/CnCMessageReceiver.scala` — receiver trait + `Unhandled` default.

**Created (Task 2 — `common.network.implementations.zmqnet`):**
- `.../implementations/zmqnet/ZCnCFrontend.scala` — ZeroMQ REQ-socket frontend.
- `.../implementations/zmqnet/ZCnCBackend.scala` — ZeroMQ REP-socket backend, takes a `CnCMessageReceiver`.

**Modified (Task 2):**
- `src/main/protobuf/codec.proto` — replace CnC message block.
- `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` — drop `server.cnc` imports + old encode/decode; add `CnCRequest`/`CnCReply` codec.
- `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` — imports, backend construction, dead frontend removal.
- `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` — remove stale import.
- `src/main/scala/org/aspen_ddp/aspen/server/network/Messenger.scala` — remove stale import.
- `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala` — replace CnC tests.
- `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` — remove stale import.
- `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala` — remove stale import.

**Deleted (Task 2):**
- `src/main/scala/org/aspen_ddp/aspen/server/cnc/CnCFrontend.scala`
- `src/main/scala/org/aspen_ddp/aspen/server/cnc/CnCRequest.scala`
- `src/main/scala/org/aspen_ddp/aspen/server/cnc/CnCReply.scala`
- `src/main/scala/org/aspen_ddp/aspen/demo/network/ZCnCFrontend.scala`
- `src/main/scala/org/aspen_ddp/aspen/demo/network/ZCnCBackend.scala`

---

## Task 1: Create the generic CnC framework interfaces

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/CnCMessage.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/CnCFrontend.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/CnCBackend.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/CnCMessageReceiver.scala`

These are all new files in a package that already compiles; nothing references the old `server.cnc` yet changes, so the tree stays green.

- [ ] **Step 1: Create `CnCMessage.scala`**

```scala
package org.aspen_ddp.aspen.common.network

// Base type for all Command-and-Control request messages. No concrete request
// types are currently defined; future CnC messages are added here as case
// classes extending CnCRequest.
sealed abstract class CnCRequest

// Universal reply for all CnC requests.
sealed abstract class CnCReply

object CnCReply:
  case class Ok() extends CnCReply
  case class Error(message: String) extends CnCReply
```

- [ ] **Step 2: Create `CnCFrontend.scala`**

```scala
package org.aspen_ddp.aspen.common.network

import scala.concurrent.Future

// Sender side of the Command-and-Control framework. Implementations send a
// request to a single target host and complete the returned Future with the
// host's reply. The Future fails only on a transport/serialization error; an
// application-level failure is returned as a successful Future containing a
// CnCReply.Error.
trait CnCFrontend:
  def send(msg: CnCRequest): Future[CnCReply]
```

- [ ] **Step 3: Create `CnCBackend.scala`**

```scala
package org.aspen_ddp.aspen.common.network

// Marker for the listening side of the Command-and-Control framework. The
// system is crash-only, so no lifecycle (shutdown) methods are defined.
trait CnCBackend
```

- [ ] **Step 4: Create `CnCMessageReceiver.scala`**

```scala
package org.aspen_ddp.aspen.common.network

import scala.concurrent.Future

// Inbound dispatch for the Command-and-Control framework, mirroring
// MessageHandler. A CnCBackend hands each decoded request here and replies with
// the resolved CnCReply. Concrete handlers pattern-match on the sealed
// CnCRequest type once message types exist.
trait CnCMessageReceiver:
  def onCnCRequestReceived(msg: CnCRequest): Future[CnCReply]

object CnCMessageReceiver:
  // Default receiver for nodes that expose the CnC channel but define no
  // handlers yet. Always replies with an error.
  object Unhandled extends CnCMessageReceiver:
    def onCnCRequestReceived(msg: CnCRequest): Future[CnCReply] =
      Future.successful(CnCReply.Error("unhandled CnC request"))
```

- [ ] **Step 5: Compile**

Run: `sbt compile`
Expected: SUCCESS (new files only; nothing else changed).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/common/network/CnCMessage.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/CnCFrontend.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/CnCBackend.scala \
        src/main/scala/org/aspen_ddp/aspen/common/network/CnCMessageReceiver.scala
git commit -m "Add generic CnC framework interfaces in common.network

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Replace the CnC wire format, implementations, and remove obsolete messages

**This is a single atomic commit.** Because the ScalaPB regeneration couples the proto, `Codec`, the ZMQ implementations, and the tests, the tree will not compile until every step below is done. Make all edits, then run the compile/test gate, then commit once.

**Files:**
- Modify: `src/main/protobuf/codec.proto` (lines 516-550)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (lines 17-18, 1113-1150)
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZCnCFrontend.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZCnCBackend.scala`
- Delete: `src/main/scala/org/aspen_ddp/aspen/demo/network/ZCnCFrontend.scala`, `.../demo/network/ZCnCBackend.scala`
- Delete: `src/main/scala/org/aspen_ddp/aspen/server/cnc/CnCFrontend.scala`, `.../server/cnc/CnCRequest.scala`, `.../server/cnc/CnCReply.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (lines 23, 25, 582-586, 804-806)
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (line 17)
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/network/Messenger.scala` (line 6)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala` (line 14, lines 608-627)
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` (line 29)
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala` (line 19)

- [ ] **Step 1: Rewrite the CnC block in `codec.proto`**

Replace the entire block currently at lines 516-550 (the `NewStore`, `ShutdownStore`, `TransferStore`, `CnCRequest`, `CnCOk`, `CnCError`, `CnCReply` messages) with:

```protobuf
message CnCRequest {
  string msgType = 1;
  bytes  payload = 2;
}

message CnCOk {
}

message CnCError {
  string message = 1;
}

message CnCReply {
  oneof msg {
    CnCOk ok = 1;
    CnCError error = 2;
  }
}
```

Leave the `//---` comment banner at lines 512-514 in place.

- [ ] **Step 2: Update imports in `Codec.scala`**

Delete lines 17-18:
```scala
import org.aspen_ddp.aspen.server.cnc
import org.aspen_ddp.aspen.server.cnc.{NewStore, ShutdownStore, TransferStore}
```
(No replacement import is needed: `CnCRequest`/`CnCReply` are in `Codec`'s own package, `common.network`.)

- [ ] **Step 3: Replace the CnC encode/decode section in `Codec.scala`**

Replace the whole CnC block currently at lines 1113-1150 (from the `// CnC Messages ...` banner through `def decode(m: codec.CnCError): cnc.Error`) with:

```scala
  // CnC Messages -----------------------------------------------------------------

  // Maps a concrete CnCRequest to its (msgType, payload) wire envelope. This is
  // the single place to extend when a new CnC request type is added. No request
  // types are currently defined.
  def encodeCnCRequest(o: CnCRequest): codec.CnCRequest = o match
    // case m: SomeFutureRequest =>
    //   codec.CnCRequest(msgType = "SomeFutureRequest", payload = encode(m).toByteString)
    case _ => throw new UnsupportedOperationException("No CnC request types are defined")

  // Reconstructs a CnCRequest from its wire envelope. Mirror of encodeCnCRequest.
  def decodeCnCRequest(m: codec.CnCRequest): CnCRequest = m.msgType match
    // case "SomeFutureRequest" => decode(codec.SomeFutureRequest.parseFrom(m.payload.toByteArray))
    case other => throw new UnsupportedOperationException(s"Unknown CnC request type: $other")

  def encode(o: CnCReply): codec.CnCReply = o match
    case CnCReply.Ok()       => codec.CnCReply(codec.CnCReply.Msg.Ok(codec.CnCOk()))
    case CnCReply.Error(msg) => codec.CnCReply(codec.CnCReply.Msg.Error(codec.CnCError(message = msg)))

  def decode(m: codec.CnCReply): CnCReply = m.msg match
    case codec.CnCReply.Msg.Ok(_)    => CnCReply.Ok()
    case codec.CnCReply.Msg.Error(e) => CnCReply.Error(e.message)
    case codec.CnCReply.Msg.Empty    => CnCReply.Error("empty CnC reply")
```

- [ ] **Step 4: Create the new `ZCnCFrontend.scala` in zmqnet**

```scala
package org.aspen_ddp.aspen.common.network.implementations.zmqnet

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.HostState
import org.aspen_ddp.aspen.common.network.{Codec, CnCFrontend, CnCReply, CnCRequest}
import org.zeromq.SocketType
import scribe.Logging

import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.{Future, Promise}

object ZCnCFrontend:
  sealed abstract class QMsg
  final case class Request(msg: CnCRequest, promise: Promise[CnCReply]) extends QMsg
  final case class Shutdown(promise: Promise[Unit]) extends QMsg

// ZeroMQ REQ-socket implementation of CnCFrontend. Borrows the ZContext from
// the supplied ZMQNet but owns its own socket and IO thread. One frontend
// targets one host.
class ZCnCFrontend(val network: ZMQNet,
                   val host: HostState) extends CnCFrontend with Logging:

  import ZCnCFrontend.*

  private val msgQueue = new LinkedBlockingQueue[QMsg]()

  private val reqSocket = network.context.createSocket(SocketType.REQ)
  reqSocket.connect(s"tcp://${host.address}:${host.cncPort}")

  private val networkThread = new Thread:
    override def run(): Unit = ioThread()
  networkThread.start()

  private def ioThread(): Unit =
    while true do
      msgQueue.take() match
        case Shutdown(p) =>
          p.success(())
          return
        case Request(msg, p) =>
          try
            reqSocket.send(Codec.encodeCnCRequest(msg).toByteArray)
            val rmsg = reqSocket.recv()
            if rmsg == null then
              p.failure(new Exception("Null CnC reply received"))
            else
              p.success(Codec.decode(codec.CnCReply.parseFrom(rmsg)))
          catch
            case t: Throwable =>
              logger.error(s"CnC request failed: $t", t)
              p.failure(t)

  def send(msg: CnCRequest): Future[CnCReply] =
    val p = Promise[CnCReply]()
    msgQueue.put(Request(msg, p))
    p.future
```

- [ ] **Step 5: Create the new `ZCnCBackend.scala` in zmqnet**

```scala
package org.aspen_ddp.aspen.common.network.implementations.zmqnet

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.{Codec, CnCBackend, CnCMessageReceiver, CnCReply}
import org.zeromq.SocketType
import scribe.Logging

import scala.concurrent.Await
import scala.concurrent.duration.Duration

// ZeroMQ REP-socket implementation of the CnC listening side. Borrows the
// ZContext from the supplied ZMQNet but owns its own socket and service thread.
// Has no knowledge of the application; all handling is delegated to the
// CnCMessageReceiver. The receiver's Future is the completion signal that gates
// the synchronous REP reply.
class ZCnCBackend(val network: ZMQNet,
                  val cncPort: Int,
                  val receiver: CnCMessageReceiver) extends CnCBackend with Logging:

  private val repSocket = network.context.createSocket(SocketType.REP)
  repSocket.bind(s"tcp://*:$cncPort")

  private val serviceThread = new Thread:
    override def run(): Unit = service()
  serviceThread.start()

  private def service(): Unit =
    while true do
      val encodedMessage = repSocket.recv()

      val reply: CnCReply =
        try
          val request = Codec.decodeCnCRequest(codec.CnCRequest.parseFrom(encodedMessage))
          Await.result(receiver.onCnCRequestReceived(request), Duration.Inf)
        catch
          case t: Throwable =>
            logger.error(s"Failed to process CnC request: $t", t)
            CnCReply.Error(t.getMessage)

      repSocket.send(Codec.encode(reply).toByteArray)
```

- [ ] **Step 6: Delete the old implementation and `server.cnc` files**

```bash
git rm src/main/scala/org/aspen_ddp/aspen/demo/network/ZCnCFrontend.scala \
       src/main/scala/org/aspen_ddp/aspen/demo/network/ZCnCBackend.scala \
       src/main/scala/org/aspen_ddp/aspen/server/cnc/CnCFrontend.scala \
       src/main/scala/org/aspen_ddp/aspen/server/cnc/CnCRequest.scala \
       src/main/scala/org/aspen_ddp/aspen/server/cnc/CnCReply.scala
```

- [ ] **Step 7: Update imports in `Main.scala`**

Change line 23 from:
```scala
import org.aspen_ddp.aspen.common.network.MessageHandler
```
to:
```scala
import org.aspen_ddp.aspen.common.network.{CnCMessageReceiver, MessageHandler}
```

Change line 25 from:
```scala
import org.aspen_ddp.aspen.demo.network.{ZCnCBackend, ZCnCFrontend, ZStoreTransferBackend}
```
to:
```scala
import org.aspen_ddp.aspen.common.network.implementations.zmqnet.ZCnCBackend
import org.aspen_ddp.aspen.demo.network.ZStoreTransferBackend
```
(`ZCnCFrontend` is no longer referenced in `Main` after Step 9, so it is intentionally not imported.)

- [ ] **Step 8: Update the `ZCnCBackend` construction in `Main.scala`**

Replace lines 582-586:
```scala
    val cncBackend = new ZCnCBackend(
      nnet,
      client,
      storeManager,
      hostCfg.cncPort)
```
with:
```scala
    val cncBackend = new ZCnCBackend(
      nnet,
      hostCfg.cncPort,
      CnCMessageReceiver.Unhandled)
```

- [ ] **Step 9: Remove the dead `ZCnCFrontend` usage in `Main.scala`**

Replace lines 804-806:
```scala
    for
      hlist <- Future.sequence(hosts.map(getHost))
      frontends = hlist.map(host => new ZCnCFrontend(network, host))
```
with:
```scala
    for
      _ <- Future.sequence(hosts.map(getHost))
```
(Leave the following `//sp <- client.newStoragePool(...)` comment and the `yield` block unchanged.)

- [ ] **Step 10: Remove the stale import in `AspenClient.scala`**

Delete line 17:
```scala
import org.aspen_ddp.aspen.server.cnc.{CnCFrontend, NewStore}
```

- [ ] **Step 11: Remove the stale import in `server/network/Messenger.scala`**

Delete line 6:
```scala
import org.aspen_ddp.aspen.server.cnc.CnCFrontend
```

- [ ] **Step 12: Remove the stale import in `TestNetwork.scala`**

Delete line 29:
```scala
import org.aspen_ddp.aspen.server.cnc.{CnCFrontend, NewStore}
```

- [ ] **Step 13: Remove the stale import in `BaseReadDriverSuite.scala`**

Delete line 19:
```scala
import org.aspen_ddp.aspen.server.cnc.{CnCFrontend, NewStore}
```

- [ ] **Step 14: Replace the CnC tests in `CodecRoundTripSuite.scala`**

Delete the import at line 14:
```scala
import org.aspen_ddp.aspen.server.cnc
```

Delete the four obsolete tests (lines 608-627): `"NewStore round-trip"`, `"ShutdownStore round-trip"`, `"TransferStore round-trip"`, and `"CnCError round-trip"`.

In their place add a single reply round-trip test (this is the new behavior under test — `CnCReply` is in the suite's own package, so no import is needed):

```scala
  test("CnCReply round-trip"):
    val ok: CnCReply = CnCReply.Ok()
    Codec.decode(Codec.encode(ok)) shouldBe a[CnCReply.Ok]

    val err: CnCReply = CnCReply.Error("boom")
    val decodedErr = Codec.decode(Codec.encode(err)).asInstanceOf[CnCReply.Error]
    decodedErr.message shouldBe "boom"
```

- [ ] **Step 15: Compile (regenerates protobuf)**

Run: `sbt compile`
Expected: SUCCESS. (This regenerates `org.aspen_ddp.aspen.codec` from the edited proto and compiles all the edits together.)

- [ ] **Step 16: Run the Codec round-trip suite**

Run: `sbt 'testOnly org.aspen_ddp.aspen.common.network.CodecRoundTripSuite'`
Expected: PASS, including the new `CnCReply round-trip` test; no compile errors from the removed CnC tests.

- [ ] **Step 17: Run the full test suite**

Run: `sbt test`
Expected: PASS (proves the package moves, deletions, and stale-import removals are clean).

- [ ] **Step 18: Commit**

```bash
git add -A
git commit -m "Rework CnC into a generic message-agnostic framework

Move ZCnCFrontend/ZCnCBackend to common.network.implementations.zmqnet
extending the new generic interfaces; ZCnCBackend now delegates to a
CnCMessageReceiver instead of StoreManager. Replace the CnCRequest oneof with
a generic (msgType, payload) envelope and remove the obsolete NewStore /
ShutdownStore / TransferStore messages and the server.cnc package.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Generic `CnCFrontend`/`CnCBackend` interfaces in `common.network` → Task 1, Steps 2-3. ✓
- `ZCnCFrontend`/`ZCnCBackend` in `common.network.implementations.zmqnet` extending the interfaces → Task 2, Steps 4-5. ✓
- `CnCMessageReceiver` with dispatch; `ZCnCBackend` has no `StoreManager` reference → Task 2, Step 5 (constructor takes `CnCMessageReceiver`). ✓
- All obsolete CnC messages removed → Task 2, Steps 1-3, 6, 14. ✓
- Consolidated `CnCMessage.scala` in `common.network` → Task 1, Step 1. ✓
- Generic byte-carrier wire envelope → Task 2, Step 1. ✓
- Reply contract (successful Future carries `CnCReply.Error`; failed Future = transport error) → Task 2, Steps 4-5. ✓
- Call-site cleanup (Main, AspenClient, server Messenger, stale test imports) → Task 2, Steps 7-13. ✓
- Testing: `CnCReply` Codec round-trip + full `sbt test` → Task 2, Steps 14, 16-17. ✓

**Placeholder scan:** No `TBD`/`TODO`/"handle edge cases". The commented `// case ...` lines inside `encodeCnCRequest`/`decodeCnCRequest` are intentional documentation of the extension point, not unfinished work — the active `case _ => throw` is complete.

**Type consistency:** `Codec.encodeCnCRequest`/`decodeCnCRequest` and `Codec.encode(CnCReply)`/`decode(codec.CnCReply)` are referenced consistently in `ZCnCFrontend` (Step 4) and `ZCnCBackend` (Step 5). `CnCReply.Ok()`/`CnCReply.Error(message)` match the definitions in `CnCMessage.scala` (Task 1, Step 1). `CnCMessageReceiver.Unhandled` (Task 1, Step 4) matches the `Main.scala` construction (Step 8). Proto `CnCReply.Msg.Ok`/`Msg.Error`/`Msg.Empty` match the Codec `decode` (Step 3).

## Out of scope

- Defining any new concrete CnC message.
- Integrating CnC into `ZMQNet`'s main DEALER/ROUTER message flow (CnC stays a side-channel).
- Reviving the new-pool / store-transfer flows that previously used CnC.
