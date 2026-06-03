# CnC Framework Rework — Design

Date: 2026-06-03

## Goal

Rework the Command-and-Control (CnC) networking classes into a clean,
**message-agnostic framework** that mirrors the existing
`ClientMessenger` / `ServerMessenger` + `MessageHandler` pattern. All
currently-defined CnC messages are obsolete and are removed. The framework
itself is retained because it is expected to be useful in the future.

After this work, CnC sends and receives no concrete messages — it is a working
skeleton. The ZeroMQ wire plumbing (`ZCnCFrontend` / `ZCnCBackend`) is designed
so that it does **not** change when a future CnC message is added; only the
message definition, its protobuf, the `Codec` dispatch, and the receiver
implementation change.

## Background (current state)

- `server.cnc` defines `CnCFrontend` (trait), `CnCRequest` (`NewStore`,
  `ShutdownStore`, `TransferStore`), and `CnCReply` (`Ok`, `Error`).
- `demo.network.ZCnCFrontend` / `demo.network.ZCnCBackend` are the ZeroMQ
  REQ/REP implementations. CnC is an independent side-channel, separate from
  `ZMQNet`'s main DEALER/ROUTER message flow.
- CnC is effectively dead code: `ZCnCBackend`'s handlers are stubbed (it only
  logs and replies `Ok`, and its `completionQueue.take()` would block forever
  because nothing fills the queue). `newStoragePool` no longer exists; the only
  `ZCnCFrontend` use site (`Main.scala` new-pool) has its actual call commented
  out, and `AspenClient` imports `CnCFrontend`/`NewStore` but never uses them.
- The reference pattern to mirror: `MessageHandler` (in `common.network`) is a
  receiver trait with `on<X>Received(msg): Unit` callbacks; `ClientMessenger`
  (`client.internal.network`) and `ServerMessenger` (`server.network`) are
  sender traits whose ZeroMQ implementations are inner classes of `ZMQNet`.

Because nothing functional depends on the existing CnC messages, removing them
is safe.

## Design decisions

These were settled during brainstorming:

1. **Framework shape: generic envelope** (not per-type `on<X>Received`
   methods). The receiver has a single generic dispatch method returning the
   reply. Concrete handlers, when they exist, pattern-match on the sealed
   `CnCRequest` inside the receiver implementation.
2. **Wiring: standalone, borrow `ZMQNet`'s context.** `ZCnCFrontend` /
   `ZCnCBackend` stay standalone classes that take a `ZMQNet` (to share its
   `ZContext`) and own their own REQ/REP sockets and threads. CnC remains an
   independent side-channel, decoupled from `ZMQNet`'s lifecycle.
3. **Wire format: generic byte-carrier envelope.** `CnCRequest` on the wire is
   `{ string msgType; bytes payload }`. The ZeroMQ layer never changes when
   messages are added.
4. **Type location: `common.network`.** A single consolidated
   `CnCMessage.scala` holds the request/reply base types, alongside the new
   interfaces. The `server.cnc` package is deleted.

## Component design

### Interfaces & message types (`common.network`)

`CnCMessage.scala` (mirrors how `Message.scala` consolidates a hierarchy):

```scala
package org.aspen_ddp.aspen.common.network

sealed abstract class CnCRequest          // empty for now; future messages added here

sealed abstract class CnCReply
object CnCReply:
  case class Ok() extends CnCReply
  case class Error(message: String) extends CnCReply
```

`Ok`/`Error` are nested under `CnCReply` to avoid placing the generic names
`Ok`/`Error` at the top level of `common.network`.

`CnCFrontend.scala` — sender (the impl-specific `def host` from the old trait
is dropped):

```scala
trait CnCFrontend:
  def send(msg: CnCRequest): Future[CnCReply]
```

`CnCMessageReceiver.scala` — receiver (generic dispatch, returns the reply):

```scala
trait CnCMessageReceiver:
  def onCnCRequestReceived(msg: CnCRequest): Future[CnCReply]

object CnCMessageReceiver:
  object Unhandled extends CnCMessageReceiver:
    def onCnCRequestReceived(msg: CnCRequest): Future[CnCReply] =
      Future.successful(CnCReply.Error("unhandled CnC request"))
```

`CnCBackend.scala` — minimal marker trait for the listening side. The system is
crash-only (no clean shutdown), so no lifecycle methods are defined:

```scala
trait CnCBackend
```

**Reply contract:** an application-level failure is a *successful* `Future`
carrying `CnCReply.Error`. A *failed* `Future` indicates a transport or parse
failure only. (This is cleaner than the old behavior of failing the future on
any non-`Ok` reply.)

### Wire format (`codec.proto`)

```protobuf
message CnCRequest {            // replaces the previous oneof
  string msgType = 1;
  bytes  payload = 2;
}
message CnCOk {}
message CnCError { string message = 1; }
message CnCReply {
  oneof msg {
    CnCOk    ok    = 1;
    CnCError error = 2;          // adds Error to the oneof (currently missing)
  }
}
// NewStore / ShutdownStore / TransferStore removed
```

### Codec changes (`Codec.scala`)

- Remove encode/decode for `NewStore`, `ShutdownStore`, `TransferStore` and the
  `server.cnc` imports.
- Add the per-message extension points — the only place that changes when a CnC
  message is later added:

```scala
def encodeCnCRequest(o: CnCRequest): codec.CnCRequest = o match
  // case m: SomeFutureReq => codec.CnCRequest("SomeFutureReq", encode(m).toByteString)
  case _ => throw UnsupportedOperationException("No CnC request types defined")

def decodeCnCRequest(m: codec.CnCRequest): CnCRequest = m.msgType match
  case _ => throw UnsupportedOperationException(s"Unknown CnC request: ${m.msgType}")

def encode(o: CnCReply): codec.CnCReply = o match
  case CnCReply.Ok()       => codec.CnCReply(codec.CnCReply.Msg.Ok(codec.CnCOk()))
  case CnCReply.Error(msg) => codec.CnCReply(codec.CnCReply.Msg.Error(codec.CnCError(msg)))

def decode(m: codec.CnCReply): CnCReply = m.msg match
  case codec.CnCReply.Msg.Ok(_)    => CnCReply.Ok()
  case codec.CnCReply.Msg.Error(e) => CnCReply.Error(e.message)
  case codec.CnCReply.Msg.Empty    => CnCReply.Error("empty CnC reply")
```

### ZeroMQ implementations (`common.network.implementations.zmqnet`)

`ZCnCFrontend(net: ZMQNet, host: HostState) extends CnCFrontend`

- Creates a REQ socket connected to `host.address:host.cncPort` using
  `net.context`. Runs one IO thread with a queue of
  `(CnCRequest, Promise[CnCReply])`.
- `send` enqueues and returns the future. The IO thread:
  `Codec.encodeCnCRequest(msg).toByteArray` → REQ send → recv →
  `Codec.decode(codec.CnCReply.parseFrom(reply))` → complete the promise. The
  promise fails only on transport/parse error; an `Error` reply completes the
  promise successfully with `CnCReply.Error`.

`ZCnCBackend(net: ZMQNet, port: Int, receiver: CnCMessageReceiver) extends CnCBackend`

- Creates a REP socket bound to `tcp://*:port`. Runs one service thread.
- Loop: recv → `Codec.decodeCnCRequest(codec.CnCRequest.parseFrom(bytes))` →
  `receiver.onCnCRequestReceived(req)` → `Await.result(future)` →
  `Codec.encode(reply).toByteArray` → REP send. Parse/decode failures send back
  a `CnCReply.Error`.
- This eliminates the `completionQueue` synchronization hack and the direct
  `StoreManager` dependency entirely: the receiver's `Future` is the completion
  signal that gates the synchronous REP reply.

### Demo / cmdline wiring & cleanup

- `Main.scala` (host startup, ~line 582): construct
  `ZCnCBackend(nnet, hostCfg.cncPort, CnCMessageReceiver.Unhandled)`.
- `Main.scala` (new-pool, ~line 806): remove the dead
  `frontends = hlist.map(host => new ZCnCFrontend(...))` line; the
  `newStoragePool` call it fed is already commented out.
- `Main.scala` (~line 25): update the import to the new package locations
  (`common.network.implementations.zmqnet` for `ZCnCBackend`/`ZCnCFrontend`).
- Remove the now-unused `server.cnc` imports in `AspenClient.scala` and
  `server/network/Messenger.scala`.
- Delete the `server.cnc` package (`CnCFrontend.scala`, `CnCRequest.scala`,
  `CnCReply.scala`).

## Per-message extension recipe (future reference)

When a new CnC message is needed, the touch-points are:

1. Add the case class extending `CnCRequest` in `CnCMessage.scala`.
2. Add its protobuf payload message in `codec.proto`.
3. Add its `encode`/`decode` in `Codec.scala`, plus a case in
   `encodeCnCRequest` / `decodeCnCRequest`.
4. Add a matching case in the `CnCMessageReceiver` implementation.

`ZCnCFrontend` / `ZCnCBackend` do **not** change.

## Testing

`CnCRequest` is `sealed`, so subtypes must live in `CnCMessage.scala`; a true
request round-trip cannot be exercised until a real message exists. Coverage:

- **Codec unit test**: `CnCReply.Ok` and `CnCReply.Error` encode → decode
  round-trip.
- **Compile + full `sbt test`** green, proving the removals and package moves
  are clean.

## Out of scope

- Defining any new concrete CnC message.
- Integrating CnC into `ZMQNet`'s main message flow (it stays a side-channel).
- Reviving the new-pool / store-transfer flows that previously used CnC.
