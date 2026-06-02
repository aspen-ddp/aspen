# ZMQNet Design

## Overview

Create a new, simplified ZeroMQ-based networking implementation (`ZMQNet`) that replaces the existing `ZMQNetwork` class. ZMQNet delegates all metadata resolution (StoreId-to-HostId-to-network-endpoint) to the refactored `MetadataManager` class, keeping the ZMQ-specific code focused solely on socket management, the IO thread, and message encoding/decoding.

## Motivation

`ZMQNetwork` (635 lines in `demo.network`) duplicates metadata resolution logic that has been generalized into `MetadataManager`. It also has a thread-safety issue: resolution callbacks (`hostLookedUp`/`poolLookedUp`) send directly on ZMQ sockets from the client's ExecutionContext thread, violating ZMQ's requirement that all socket I/O happen on a single thread. ZMQNet fixes both issues.

## New Files

1. **`common/network/MessageHandler.scala`** — Dispatch trait for inbound messages.
2. **`common/network/implementations/zmqnet/ZMQNet.scala`** — Main class + companion object.
3. **`common/network/implementations/zmqnet/ProtobufMessageCodec.scala`** — Encoding and decoding. Combines the existing `ProtobufMessageEncoder` encoding logic with the decoding logic currently inline in `ZMQNetwork`.

## MessageHandler Trait

Defined in `common.network`, network-implementation-agnostic:

```scala
trait MessageHandler:
  def onClientResponseReceived(msg: ClientResponse): Unit
  def onClientRequestReceived(msg: ClientRequest): Unit
  def onTransactionMessageReceived(msg: TxMessage): Unit
  def onHostMessageReceived(msg: HostMessage): Unit
```

Replaces the 4 loose callback functions in `ZMQNetwork`'s constructor. `NetworkBridge` in `Main.scala` extends this trait.

## ProtobufMessageCodec

Located in `common.network.implementations.zmqnet`. Contains:

- All existing `encodeMessage` overloads from `ProtobufMessageEncoder` (for `ClientRequest`, `ClientResponse`, `TxMessage`, `HostMessage`, `HostHeartbeat`).
- A general `encodeMessage(msg: Message): Array[Byte]` that dispatches to the type-specific overload. Needed because `MetadataManager` queues `Message` objects (the base type).
- A new `decodeMessage(rawBytes: Array[Byte]): Option[Message]` that handles: reading the 4-byte big-endian length prefix, parsing the protobuf via `codec.Message.parseFrom`, pattern matching on the `Msg` variant, calling `Codec.decode`, and the `TxPrepare` special binary data extraction (object updates and pre-transaction rebuilds appended after the protobuf payload). Returns `None` for unrecognized or empty messages (logged as errors).

## ZMQNet Class

### Companion Object

**`ZMQHostEntry`** — Extends `MetadataManager.HostEntry` with ZMQ-specific state. Created without a socket; the IO thread attaches one later:

```scala
private class ZMQHostEntry(hostId: HostId, name: String, address: String,
                           dataPort: Int, cncPort: Int, storeTransferPort: Int)
  extends MetadataManager.HostEntry(hostId, name, address, dataPort, cncPort, storeTransferPort):

  var odealer: Option[ZMQ.Socket] = None
  var opollItem: Option[PollItem] = None
  val pendingMessages = new ConcurrentLinkedQueue[Message]()
  var lastHeartbeatTime: Long = 0
  var isOnline: Boolean = false
```

**`SendQueueMsg`** — Messages for cross-thread communication with the IO thread:

```scala
private sealed abstract class SendQueueMsg
private case class SendToStore(storeId: StoreId, msg: Message) extends SendQueueMsg
private case class SendToHost(hostId: HostId, msg: Message) extends SendQueueMsg
private case class SendToClient(msg: ClientResponse) extends SendQueueMsg
private case class NewHostAvailable(entry: ZMQHostEntry) extends SendQueueMsg
private case class ProcessPendingMessages(entry: ZMQHostEntry) extends SendQueueMsg
```

**`CliMessenger`** and **`SrvMessenger`** — Inner classes implementing the `ClientMessenger` and `ServerMessenger` traits. All methods queue on the send queue:

```scala
private class CliMessenger(net: ZMQNet) extends ClientMessenger:
  def sendClientRequest(msg: ClientRequest): Unit =
    net.queueForSend(SendToStore(msg.toStore, msg))
  def sendTransactionMessage(msg: TxMessage): Unit =
    net.queueForSend(SendToStore(msg.to, msg))
  def sendTransactionMessages(msgs: List[TxMessage]): Unit =
    msgs.foreach(sendTransactionMessage)
  def sendHostMessage(msg: HostMessage): Unit =
    net.queueForSend(SendToHost(msg.toHost, msg))
  def dropCacheForStore(storeId: StoreId): Unit =
    net.metadataManager.dropStoreMapping(storeId)

private class SrvMessenger(net: ZMQNet) extends ServerMessenger:
  def sendClientResponse(msg: ClientResponse): Unit =
    net.queueForSend(SendToClient(msg))
  def sendTransactionMessage(msg: TxMessage): Unit =
    net.queueForSend(SendToStore(msg.to, msg))
  def sendTransactionMessages(msgs: List[TxMessage]): Unit =
    msgs.foreach(sendTransactionMessage)
  def dropCacheForStore(storeId: StoreId): Unit =
    net.metadataManager.dropStoreMapping(storeId)
```

### Constructor

```scala
class ZMQNet(
  bootstrapConfigFile: os.Path,
  oclientId: Option[ClientId],
  ohostNode: Option[(HostId, Int)],
  heartbeatPeriod: Duration,
  messageHandler: MessageHandler
)
```

Takes `os.Path` instead of pre-parsed `BootstrapConfig.Config` (MetadataManager loads the config itself).

### MetadataManager Integration

ZMQNet implements `MetadataManager.NetworkImplInterface[ZMQHostEntry]`. Neither callback touches ZMQ sockets:

- **`createHostEntry(..., queuedMessages)`**: Creates a `ZMQHostEntry` (no socket). Drains `queuedMessages` (the `EvictingQueue` from MetadataManager) into `entry.pendingMessages` (a `ConcurrentLinkedQueue`). Queues `NewHostAvailable(entry)` on the send queue. Wakes the IO thread. Returns the entry.
- **`storeResolved(hostEntry, storeId, queuedMessages)`**: Drains `queuedMessages` into `hostEntry.pendingMessages`. Queues `ProcessPendingMessages(hostEntry)`. Wakes the IO thread.

The `MetadataManager` is instantiated in the constructor body. During construction, it loads the bootstrap config and calls `createHostEntry` for each bootstrap host — queuing `NewHostAvailable` messages that the IO thread will process when it starts.

### IO Thread Send Queue Processing

**`SendToStore(storeId, msg)` / `SendToHost(hostId, msg)`:**
- Calls `metadataManager.getHostEntryOrQueueMessage(storeId/hostId, msg)`.
- If `Some(entry)` with socket ready: encode via `ProtobufMessageCodec` and send on the dealer socket.
- If `Some(entry)` but socket not yet created: add `msg` to `entry.pendingMessages`.
- If `None`: MetadataManager queued it internally.

**`SendToClient(msg)`:**
- Looks up the client's ZMQ identity in the `clients` map.
- Sends identity frame + encoded message via the ROUTER socket.

**`NewHostAvailable(entry)`:**
- Creates a DEALER socket, sets client identity, connects to `tcp://address:dataPort`.
- Attaches socket and poll item to the entry.
- Sends initial heartbeat (if this is a server node).
- Drains `entry.pendingMessages` — encodes and sends each.
- Rebuilds the poller to include the new socket.

**`ProcessPendingMessages(entry)`:**
- If socket exists: drains `entry.pendingMessages`, encodes and sends each.
- If socket not yet created: no-op. Messages remain in the queue and will be drained when `NewHostAvailable` is processed (ordering is guaranteed since both go through the same send queue, and `createHostEntry` is called before `storeResolved`).

### IO Thread Main Loop

Same structure as `ZMQNetwork`:

1. Poll all dealer sockets + send queue inproc socket + optional router socket, with timeout until next heartbeat.
2. Process inbound dealer messages: call `ProtobufMessageCodec.decodeMessage`, dispatch via `MessageHandler`.
3. Drain send queue: process each `SendQueueMsg` as described above.
4. Process inbound router messages: decode, update client identity tracking (for `ClientRequest` messages), dispatch via `MessageHandler`.
5. If heartbeat interval elapsed: send heartbeat to all connected hosts (if server node), mark hosts offline if no heartbeat received in 3x the period.

### Centralized Decode and Dispatch

A single `decodeAndDispatch` method replaces the duplicated pattern matches in `ZMQNetwork`'s `onDealerMessageReceived` and `onRouterMessageReceived`:

```scala
private def decodeAndDispatch(rawMsg: Array[Byte], routerFrom: Option[Array[Byte]]): Unit =
  ProtobufMessageCodec.decodeMessage(rawMsg).foreach: msg =>
  msg match
    case m: HostHeartbeat =>
      metadataManager.getHostEntry(m.hostId).foreach(_.heartbeatReceived())
    case m: ClientRequest =>
      routerFrom.foreach(addr => updateClientId(m.fromClient, addr))
      messageHandler.onClientRequestReceived(m)
    case m: ClientResponse =>
      messageHandler.onClientResponseReceived(m)
    case m: TxMessage =>
      messageHandler.onTransactionMessageReceived(m)
    case m: HostMessage =>
      messageHandler.onHostMessageReceived(m)
```

The `routerFrom` parameter distinguishes dealer messages (`None`) from router messages (`Some(zmqIdentity)`). For router messages from clients, the client identity is updated for response routing.

### Public API

```scala
val clientId: ClientId        // generated if oclientId is None
val clientMessenger: ClientMessenger
val serverMessenger: ServerMessenger

def startIoThread(client: AspenClient): Unit
  // Calls metadataManager.setAspenClient(client), starts the IO thread

def joinIoThread(): Unit
  // Waits for the IO thread to finish
```

### Thread Safety

- **ZMQ sockets**: IO thread only. This is a fix over `ZMQNetwork`, which sent from ExecutionContext threads in resolution callbacks.
- **MetadataManager**: `synchronized` internally. Called from the IO thread (send queue processing) and the ExecutionContext (resolution callbacks). Short critical sections, no deadlock risk.
- **Send queue (`ConcurrentLinkedQueue`)**: Thread-safe. Written by messenger methods (application threads) and MetadataManager callbacks (ExecutionContext). Read by the IO thread.
- **Per-host `pendingMessages` (`ConcurrentLinkedQueue`)**: Thread-safe. Written by MetadataManager callbacks (via `createHostEntry`/`storeResolved`) and the IO thread (when entry exists but socket is not ready). Read by the IO thread.
- **`clients` map**: IO thread only (updated on router recv, read on router send).
- **Inproc socket pair**: Used to wake the IO thread from `poll()`. The connect-side socket uses `ThreadLocal` so each calling thread gets its own instance.

## Changes to Existing Code

### MetadataManager — Add `dropStoreMapping`

```scala
def dropStoreMapping(storeId: StoreId): Unit =
  synchronized:
    if isBootstrapStore(storeId) then
      refreshBootstrapConfig()
    else
      stores -= storeId
```

The `Messenger` traits' `dropCacheForStore(storeId)` does not pass a `hostId`, so the safety check in `receivedUnknownStoreFromHost` cannot be used. This simpler method matches `ZMQNetwork`'s current behavior (unconditional drop). The safer hostId-aware version can be wired up later when the Messenger traits are updated.

### Main.scala

**`NetworkBridge`** extends `MessageHandler` instead of defining standalone callback methods:

```scala
class NetworkBridge extends MessageHandler:
  var oclient: Option[AspenClient] = None
  var onode: Option[StoreManager] = None
  // same 4 method implementations as today
```

**`createNetwork`** updated signature:

```scala
def createNetwork(bootstrapConfigFile: os.Path,
                  ohostNode: Option[(HostId, Int)],
                  oclientId: Option[ClientId]): (NetworkBridge, ZMQNet) =
  val b = new NetworkBridge
  val heartbeatPeriod = Duration(10, SECONDS)
  (b, new ZMQNet(bootstrapConfigFile, oclientId, ohostNode, heartbeatPeriod, b))
```

Callers that previously passed `BootstrapConfig.Config` now pass `os.Path` to the bootstrap config file.

### Old ZMQNetwork

Stays in `demo.network` unchanged. Can be retired once ZMQNet is validated and Main.scala is switched over.
