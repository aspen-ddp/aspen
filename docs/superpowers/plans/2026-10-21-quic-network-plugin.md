# QUIC Network Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Netty 4.2 QUIC network plugin (`QuicNet`) with mutual TLS, datagrams for small messages, a round-robin stream pool for large ones, zero-copy send and single-copy receive. Make it the CLI's default network, with `ZMQNet` still selectable.

**Architecture:**
- Caller threads resolve the destination, encode into pooled/wrapped `ByteBuf`s, and hand off through a byte-bounded lock-free queue per connection.
- Each QUIC connection is owned by one Netty event loop. That loop drains the queue, writes datagrams or stream frames, and does all receiving.
- Receiving uses an incremental decoder that copies each payload exactly once into its final heap buffer.
- A shared `NetworkPlugin` trait lets the CLI hold either plugin.

**Tech Stack:** Scala 3.7.3, sbt, Netty 4.2.19.Final (`netty-codec-classes-quic`, `netty-codec-native-quic`, `netty-transport-native-epoll`), ScalaPB, BoringSSL via Netty QUIC, scalatest.

**Spec:** `docs/superpowers/specs/2026-10-21-quic-network-plugin-design.md`

## Global Constraints

**Code style and project conventions**
- Scala 3 quiet syntax. Use braces only where they clearly help. Use indented if/then/else and avoid one-line `if a then b`.
- Logging is `scribe.Logging`. Threads come from `common.util.DaemonThreads`. Caching, if any, uses Scaffeine. Filesystem access uses os-lib.
- Tests use `AnyFunSuite with Matchers` (or `AsyncFunSuite`), live under `src/test/scala/org/aspen_ddp/aspen/...`, and are run with `sbt 'testOnly *Name'`.
- The new package is `org.aspen_ddp.aspen.common.network.implementations.quic`.
- `ZMQNet` behavior must not change. `ProtobufMessageCodec` is not modified.
- Nothing outside the plugin is modified for buffer handling: `DataBuffer`, the store, the transaction code, and `Codec.scala` stay as they are. CLI (`cmdline`) changes are allowed.

**Buffers**
- No `ByteBuf` escapes the plugin. Every encode, decode, and drop path releases what it owns.
- Tests run with `ResourceLeakDetector.setLevel(PARANOID)`.
- Payloads are never copied on send.
- On receive, each payload is copied exactly once into a plain heap `ByteBuffer`.

**Sending rules**
- `DatagramThreshold = 1024`. An encoded size of 1024 bytes or less is sent as a datagram, and anything larger goes on a stream. A datagram never falls back to a stream.
- There is no message size cap. The only limit is the JVM's (each payload is at most `Int.MaxValue` bytes). The stream frame length is 8 bytes.
- Never queue to avoid a drop. A full budget means drop, count, and log at debug/trace (rate-limited).
- Default stream pool is `4` unidirectional streams per sending side per connection.

**Identity and TLS**
- Every connection uses mutual TLS against the Aspen CA through `AspenTrustManager`.
- The certificate UUID is **not** compared to the peer's `HostId` or `ClientId`.
- Routing identity is learned from message contents (`ClientRequest.fromClient`, `HostHeartbeat.hostId`). There is no hello message.

**Flush and shutdown**
- Flush means "accepted by QUIC", i.e. the write futures have completed.
- `shutdown(linger)` keeps connections up for `linger`, then sends a QUIC close and stops the event loop group.

**Threading**
- Event loop threads are daemon threads.
- `MessageHandler` callbacks run on the event loop and must not block.

## Review Focus

These are the inputs most likely to bite a real user that the spec implies but does not spell out. Each one's test is added to the owning task:

1. **A host sending to itself.** A host's own `AspenClient` reads stores on the same host, so `QuicNet` dials its own `dataPort`. That must work like any other host. *(Task 6: `host can send to its own stores`)*
2. **A peer dies mid-frame.** A partially received large frame must be released and must not leak. The next connection from that peer must work. *(Task 3: `channelInactive mid-frame releases everything`; Task 6: `peer restart mid-transfer recovers`)*
3. **An unreachable host.** If nothing is listening, the handshake fails or times out. Parked messages are dropped, a later send re-dials, and `awaitPendingMessagesSent` returns `false` instead of hanging. *(Task 5: `handshake failure drops pre-handshake messages and completes barriers`; Task 8: `flush returns false for unreachable host`)*
4. **A message larger than the connection budget on an idle connection.** With no cap, it must still be sent, not permanently dropped. *(Task 5: `message larger than budget is accepted when queue is empty`)*
5. **A CLI client exits while a host is replying.** The `ClientId` → connection entry is removed when the connection closes. Responses to that client are dropped quietly, with no exception escaping the caller thread. *(Task 6: `responses to a departed client are dropped quietly`)*

---

## File Structure

| File | Responsibility |
|---|---|
| `build.sbt` | Netty QUIC and epoll dependencies |
| `common/network/NetworkPlugin.scala` (new) | Trait shared by `ZMQNet` and `QuicNet` |
| `common/network/implementations/zmqnet/ZMQNet.scala` | `extends NetworkPlugin` (no behavior change) |
| `quic/QuicMessageCodec.scala` (new) | Wire format: encode to `Encoded`, parse prefix, assemble message, decode datagram |
| `quic/StreamFrameDecoder.scala` (new) | Incremental per-stream decoder (`ChannelInboundHandler`) |
| `quic/QuicTls.scala` (new) | `QuicSslContext` for server and client from CA, cert, key; ALPN |
| `quic/QuicConfig.scala` (new) | `QuicConfig` tunables and defaults |
| `quic/QuicConnection.scala` (new) | Per-connection send machinery: handoff queue, budget, drain, datagram/stream routing, stream pool, pre-handshake queue, barriers, drop counter |
| `quic/QuicHostEntry.scala` (new) | `MetadataManager.HostEntry` subclass holding the connection handle and liveness |
| `quic/QuicNet.scala` (new) | Event loop group, listener, dialing, messengers, dispatch, client map, heartbeats, flush, shutdown |
| `cmdline/Main.scala` | `--network quic\|zmq`, credentials plumbing, `startHost` split from `run_host` |
| Tests under `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/quic/` | One suite per unit, plus `QuicTestPeer.scala`, a raw Netty QUIC endpoint harness |

(`quic/` = `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/`.)

---

### Task 1: Dependencies and the `NetworkPlugin` seam

**Files:**
- Modify: `build.sbt`
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/NetworkPlugin.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala` (class header only)
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (types only: `onetwork`, `createNetwork`, both `createAspenClient`s return/accept `NetworkPlugin`)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNativeSuite.scala`

**Interfaces:**
- Produces:
```scala
trait NetworkPlugin:
  def clientId: ClientId
  def clientMessenger: client.internal.network.Messenger
  def serverMessenger: server.network.Messenger
  def startIoThread(client: AspenClient): Unit
  def joinIoThread(): Unit
  def awaitPendingMessagesSent(timeout: Duration): Boolean
  def shutdown(linger: Duration): Unit
```
  Names match `ZMQNet`'s existing members exactly, so `ZMQNet` only adds `extends NetworkPlugin`. Each method gets a doc comment carrying the contract from `ZMQNet`'s existing comments, written for any plugin.

- [ ] **Step 1: Write the failing test.** Suite `QuicNativeSuite`, test `native QUIC library loads`: `io.netty.handler.codec.quic.Quic.ensureAvailability()` does not throw, and `Quic.isAvailable shouldBe true`.
- [ ] **Step 2: Run it.** `sbt 'testOnly *QuicNativeSuite'`. Expected: compile failure (`io.netty` not found).
- [ ] **Step 3: Add the dependencies.**
  - `"io.netty" % "netty-codec-classes-quic" % "4.2.19.Final"`
  - `"io.netty" % "netty-codec-native-quic" % "4.2.19.Final" classifier "linux-x86_64"`
  - `"io.netty" % "netty-transport-native-epoll" % "4.2.19.Final" classifier "linux-x86_64"`
  - The `linux-aarch_64` and `osx-*` classifiers can be added alongside, at the builder's choice. Linux x86_64 is the minimum.
- [ ] **Step 4: Run it.** Expected: PASS.
- [ ] **Step 5: Add the `NetworkPlugin` trait and switch the CLI to it.** `ZMQNet extends ... with NetworkPlugin`. In `Main`, change the `onetwork: Option[NetworkPlugin]` type and the `createNetwork` / `createAspenClient` signatures to `NetworkPlugin`. `createNetwork` still constructs `ZMQNet` in this task.
- [ ] **Step 6: Verify.** `sbt test`. Expected: all existing suites pass, `MainSuite` included.
- [ ] **Step 7: Commit.** `git commit -m "Add Netty QUIC dependencies and NetworkPlugin trait"`

---

### Task 2: `QuicMessageCodec`, the wire format

**Files:**
- Create: `quic/QuicMessageCodec.scala`
- Test: `.../quic/QuicMessageCodecSuite.scala`

**Wire format (all big-endian):**
```
datagram     := prefixLen:u32  prefix  section*
stream frame := frameLen:u64   prefixLen:u32  prefix  section*      frameLen = 4 + prefixLen + Σ sectionLen
prefix       := headerLen:u32  header  auxLen:u32  aux  nSections:u32  sectionLen:u32 × nSections
header       := codec.Message protobuf (with bulk data fields emptied — see below)
```

**Payloads that travel as out-of-band sections.** This is my call, and it extends the spec's "any other bulk payload the codec carries":
- **`TxPrepare`**:
  - `aux = nUpdates:u32, (msb:u64 lsb:u64) × nUpdates, nRebuilds:u32, (msb lsb Metadata[EncodedSize]) × nRebuilds`
  - Sections are the update data in order, then the rebuild data in order.
  - The header is `codec.Message.Msg.Prepare(Codec.encode(m))`.
- **`ReadResponse` with `Right(cs)` and `cs.objectData` non-empty**:
  - The header encodes `m.copy(result = Right(cs.copy(objectData = None)))`.
  - There is one section, the data.
  - On decode, one section gives `objectData = Some(section)`.
  - Without this, every read result would be copied twice per side inside protobuf.
- **`StoreTransferData`**:
  - The header encodes `m.copy(data = DataBuffer.Empty)`.
  - There is exactly one section.
- **Every other message**: `aux` is empty and `nSections = 0`.

**Interfaces:**
- Produces:
```scala
object QuicMessageCodec:
  val DatagramThreshold: Int = 1024
  val FrameLengthSize: Int = 8

  /** Owns `prefix` (pooled) until converted or released. `sections` are the sender's DataBuffers, never copied. */
  final class Encoded(val prefix: ByteBuf, val sections: Array[DataBuffer]):
    /** 4 + prefix.readableBytes + Σ sections.size */
    val size: Long
    def isDatagram: Boolean                           // size <= DatagramThreshold
    def toDatagram(alloc: ByteBufAllocator): ByteBuf  // one pooled buffer: prefixLen, prefix, sections copied; consumes this
    def toStreamFrame(alloc: ByteBufAllocator): ByteBuf // CompositeByteBuf: [frameLen u64 + prefixLen u32], prefix, Unpooled.wrappedBuffer(section.asReadOnlyBuffer()) ...; consumes this
    def release(): Unit

  def encode(msg: Message, alloc: ByteBufAllocator): Encoded

  final case class Prefix(header: codec.Message, aux: ByteBuffer, sectionSizes: Array[Int])
  /** `buf` holds exactly the prefix bytes. Does not release. */
  def parsePrefix(buf: ByteBuf): Either[String, Prefix]
  /** sections.length must equal prefix.sectionSizes.length; reconstructs the Message */
  def assemble(prefix: Prefix, sections: Array[DataBuffer]): Either[String, Message]
  /** Whole datagram; copies sections into heap DataBuffers. Does not release `buf`. */
  def decodeDatagram(buf: ByteBuf): Either[String, Message]
```
- The protobuf header is written with `msg.writeTo(new ByteBufOutputStream(buf))` into a buffer sized from `serializedSize`, so there is no intermediate array.
- Everything after the header is the same dispatch as `ProtobufMessageCodec.decodeMessage`. `Left` covers every malformed case, and nothing throws out of the codec.

- [ ] **Step 1: Write the failing tests** in `QuicMessageCodecSuite`. The suite turns on PARANOID leak detection in `beforeAll`. Build one instance of each message type (reuse the fixtures in `CodecRoundTripSuite` where possible).
  - `every message type round-trips through a datagram`: encode, `toDatagram`, `decodeDatagram`, then `shouldBe` the original.
  - `every message type round-trips through a stream frame`: encode, `toStreamFrame`, strip the 8-byte length, split prefix and sections by hand using `parsePrefix`, `assemble`, then `shouldBe` the original.
  - `TxPrepare with 3 updates and 2 rebuilds keeps order and metadata`.
  - `payload slices with non-zero position encode only their visible bytes`: use `DataBuffer(arr).slice(5, 10)`.
  - `empty payloads round-trip`: an empty update, `ReadResponse` with `objectData = None`, and `StoreTransferData` with `DataBuffer.Empty`.
  - `send path does not copy payloads`: `toStreamFrame` is a `CompositeByteBuf`, and the component for a section has `nioBuffer()` that shares content with the source array (write the source array after encoding and observe the change through the frame).
  - `size picks the transport at the boundary`: messages whose `size` is 1023, 1024, and 1025 give `isDatagram` = true, true, false. (Build them with a `StoreTransferData` payload sized to hit the target.)
  - `malformed input returns Left`: truncated prefix, `headerLen` beyond the buffer, an unparseable protobuf, a section count that doesn't match `aux` for `TxPrepare`, and a negative section length.
  - `nothing leaks`: after the tests above, each `Encoded` / frame has `refCnt == 0` after release or consumption.
- [ ] **Step 2: Run it.** `sbt 'testOnly *QuicMessageCodecSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `QuicMessageCodec`** with the signatures above.
- [ ] **Step 4: Run it.** Expected: PASS, with no `LEAK:` lines in the output.
- [ ] **Step 5: Commit.** `git commit -m "Add QUIC wire codec with out-of-band payload sections"`

---

### Task 3: `StreamFrameDecoder`, incremental receive

**Files:**
- Create: `quic/StreamFrameDecoder.scala`
- Test: `.../quic/StreamFrameDecoderSuite.scala`

**Interfaces:**
- Consumes: `QuicMessageCodec.parsePrefix`, `QuicMessageCodec.assemble`, `FrameLengthSize`
- Produces:
```scala
/** One instance per inbound stream (not sharable). Releases every inbound ByteBuf it is given. */
class StreamFrameDecoder(onMessage: Message => Unit,
                         onProtocolError: String => Unit) extends ChannelInboundHandlerAdapter
```

**Algorithm.** The signature doesn't determine it, so here it is. State machine per stream:
1. **`Lengths`**: accumulate 12 bytes into a small pooled cumulation buffer (`frameLen: u64`, `prefixLen: u32`). Validate `prefixLen >= 12` (the minimum prefix) and `4 + prefixLen <= frameLen`.
2. **`Prefix`**: accumulate `prefixLen` bytes, cumulating with a `CompositeByteBuf` so pieces aren't copied. Then call `parsePrefix`. Validate `frameLen == 4 + prefixLen + Σ sectionSizes` and every size `>= 0`. Allocate `ByteBuffer.allocate(size)` for each section, and release the prefix cumulation.
3. **`Section(i, offset)`**: copy `min(readable, remaining)` bytes from the inbound buffer **directly** into `sections(i).array` at `offset`, then advance. When the last section is full, call `assemble`, call `onMessage`, and go back to `Lengths`. A frame with zero sections dispatches as soon as the prefix is parsed.
4. One inbound `ByteBuf` may finish a frame and start the next. Loop until it is empty, then `release()` it in a `finally`.
5. **On any validation or assembly error**: release cumulations, drop the section arrays, call `onProtocolError(reason)`, then `ctx.close()`, and discard further input.
6. **`channelInactive` / `handlerRemoved`**: release any cumulation.

- [ ] **Step 1: Write the failing tests** using Netty `EmbeddedChannel(new StreamFrameDecoder(buf += _, errs += _))`, with PARANOID leak detection. Frames are built with `QuicMessageCodec.encode(...).toStreamFrame`.
  - `single whole frame dispatches one message`.
  - `two frames in one buffer dispatch two messages in order`.
  - `every split point of a TxPrepare frame decodes identically`: for each byte offset `k` in a small frame (two updates, one rebuild), write `[0,k)` and `[k,end)` as separate buffers, then `shouldBe` the original. This covers splits inside the length, prefix, between sections, and inside a payload.
  - `one-byte-at-a-time delivery decodes`.
  - `large payload arrives in many chunks`: a 64 MiB `StoreTransferData` fed in 64 KiB chunks decodes intact.
    - Peak pooled memory stays bounded: `PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory() + usedDirectMemory()` measured during the feed stays under 4 MiB above baseline (only the test's own chunk plus the cumulation are live).
  - `frameLen inconsistent with section sizes closes the stream`: `errs.size shouldBe 1`, `channel.isOpen shouldBe false`, and no messages.
  - `negative section length closes the stream`.
  - `channelInactive mid-frame releases everything` *(Review Focus 2)*: feed half a frame, `channel.close()`, `finishAndReleaseAll()`, and there are no leaks.
  - `every inbound buffer is released`: hold references to the fed buffers and assert `refCnt == 0` after each test.
- [ ] **Step 2: Run it.** `sbt 'testOnly *StreamFrameDecoderSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `StreamFrameDecoder`** per the algorithm.
- [ ] **Step 4: Run it.** Expected: PASS, with no `LEAK:` lines.
- [ ] **Step 5: Commit.** `git commit -m "Add incremental single-copy QUIC stream frame decoder"`

---

### Task 4: `QuicTls`, `QuicConfig`, the raw test peer, and the mTLS tests

**Files:**
- Create: `quic/QuicTls.scala`, `quic/QuicConfig.scala`
- Create (test): `.../quic/QuicTestPeer.scala`, `.../quic/QuicTlsSuite.scala`

**Interfaces:**
- Produces:
```scala
final case class TlsCredentials(caCertificate: X509Certificate,
                                certificate: X509Certificate,
                                privateKey: PrivateKey)
object TlsCredentials:
  def fromPem(caCertificatePem: String, certificatePem: String, privateKeyPem: String): TlsCredentials

object QuicTls:
  val ApplicationProtocol: String = "aspen/1"
  def serverContext(creds: TlsCredentials): QuicSslContext
    // forServer(key, null, cert, ca).trustManager(new AspenTrustManager(ca)).clientAuth(ClientAuth.REQUIRE).applicationProtocols(ApplicationProtocol)
  def clientContext(creds: TlsCredentials): QuicSslContext
    // forClient().keyManager(key, null, cert, ca).trustManager(new AspenTrustManager(ca)).applicationProtocols(ApplicationProtocol)

final case class QuicConfig(
  eventLoopThreads: Int = math.min(Runtime.getRuntime.availableProcessors, 4),
  streamsPerConnection: Int = 4,
  connectionBudgetBytes: Long = 16L * 1024 * 1024,   // handoff queue and pre-handshake queue bound; per-stream high-water mark = this / streamsPerConnection
  idleTimeout: Option[Duration] = None,               // None = heartbeatPeriod * 3
  handshakeTimeout: Duration = Duration(5, SECONDS),
  datagramQueueLength: Int = 1024,
  initialMaxData: Long = 64L * 1024 * 1024,
  initialMaxStreamDataUnidirectional: Long = 16L * 1024 * 1024,
  initialMaxStreamsUnidirectional: Long = 64)
```
  `QuicConfig` also provides `def applyTo[B <: QuicCodecBuilder[B]](b: B, idle: Duration): B`, which sets `maxIdleTimeout`, `initialMaxData`, `initialMaxStreamDataUnidirectional`, `initialMaxStreamsUnidirectional`, and `datagram(datagramQueueLength, datagramQueueLength)`, so the server and client codecs are configured identically.

- `QuicTestPeer` (test-only) is a minimal raw Netty QUIC server or client on loopback built from `QuicTls` and `QuicConfig`, used by Tasks 4 and 5. It:
  - records decoded messages, using `StreamFrameDecoder` on inbound streams and `decodeDatagram` on datagrams;
  - records handshake outcomes;
  - has a `stallStreams: Boolean` that sets `autoRead=false` on inbound streams, so flow control fills;
  - lets each test override `QuicConfig`.

  Whether `QuicTls` uses `AspenTrustManager` directly or needs a helper in `common.network.tls` is decided by what BoringSSL accepts. Either way, verification logic is not copied.

- [ ] **Step 1: Write the failing tests** in `QuicTlsSuite`, with credentials from `TestTLS.ca`. Each test runs a `QuicTestPeer` server and a `QuicTestPeer` client on loopback and asserts the handshake outcome within 5s.
  - `peers with certificates from the same CA connect`, and a datagram and a stream frame both arrive.
  - `server with a certificate from a different CA is rejected by the client`.
  - `client with a certificate from a different CA is rejected by the server`.
  - `client presenting no certificate is rejected`: the client context is built without `keyManager`.
  - `CA certificate presented as an entity certificate is rejected`: the client uses `TestTLS.ca.certificate` and key as its own.
  - `expired certificate is rejected`: sign one with `notAfter` in the past, using the Bouncy Castle builder approach from `TLSSuite`.
  - `two clients sharing one certificate both connect`.
- [ ] **Step 2: Run it.** `sbt 'testOnly *QuicTlsSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `TlsCredentials`, `QuicTls`, `QuicConfig`, and `QuicTestPeer`.**
- [ ] **Step 4: Run it.** Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Add QUIC mutual TLS contexts and loopback test peer"`

---

### Task 5: `QuicConnection`, the per-connection send machinery

**Files:**
- Create: `quic/QuicConnection.scala`
- Test: `.../quic/QuicConnectionSuite.scala`

**Interfaces:**
- Consumes: `QuicMessageCodec.Encoded`, `QuicConfig`, `QuicTestPeer`
- Produces:
```scala
/** All methods except those marked "any thread" must run on `loop`. */
final class QuicConnection(val loop: EventLoop, config: QuicConfig, onClosed: QuicConnection => Unit):
  // any thread
  def submit(enc: Encoded): Boolean        // false = dropped (and released): over budget or closed
  def postBarrier(): io.netty.util.concurrent.Future[Void]  // completes when all writes submitted before it are done or dropped
  def isClosed: Boolean                     // volatile
  def drops: Long                           // AtomicLong, for tests and logging
  // loop
  def attach(ch: QuicChannel): Unit         // handshake succeeded; drains pre-handshake queue
  def handshakeFailed(cause: Throwable): Unit
  def close(): Unit
```

**Behaviors pinned by the spec:**
- **Handoff queue.**
  - `submit` enqueues on a `ConcurrentLinkedQueue` and adds `enc.size` to an `AtomicLong queuedBytes`.
  - It drops if `queuedBytes + size > connectionBudgetBytes`, **except when `queuedBytes == 0`**. That exception is how a message larger than the budget is still sent, since there is no cap.
  - The first enqueue into an empty queue (an `AtomicBoolean drainScheduled` CAS) does `loop.execute(drain)`.
  - Barriers travel through the same queue as a marker item.
- **Drain (on the loop).**
  - Pull everything off the queue and subtract each item's bytes.
  - **Not attached**: move the item into the loop-owned pre-handshake queue, bounded by `connectionBudgetBytes` with the same empty-queue exception. Drop if it's full.
  - **`isDatagram`**: `ch.write(enc.toDatagram(alloc))`. If the peer hasn't announced datagram support, or the size exceeds the peer's max datagram size (from `QuicDatagramExtensionEvent`), drop.
  - **Otherwise**: round-robin over `streamsPerConnection` slots.
    - An empty slot opens with `ch.createStream(QuicStreamType.UNIDIRECTIONAL, null)` and sets `WRITE_BUFFER_WATER_MARK(high = connectionBudgetBytes / streamsPerConnection, low = high / 2)`. It is used only once that future has succeeded, otherwise skipped.
    - A closed slot is reset to empty.
    - Skip non-writable slots. If no slot is usable, release and drop.
  - Record each write future in an in-flight set, removed on completion.
  - After the batch, call `flush()` once on each touched channel.
- **Barrier.** When the drain reaches it: if attached, snapshot the in-flight set and complete when every future in it completes. If still handshaking, hold it until `attach` (after the pre-handshake writes) or `handshakeFailed`.
- **Close and handshake failure.** Release every queued, pre-handshake, and pending item. Complete all barriers. Set `isClosed`. Call `onClosed`. A `submit` after close returns `false` and releases.
- **Drops** increment `drops`, and the drop is logged at debug with rate limiting: at most one line per connection per second, carrying the count since the last line.

- [ ] **Step 1: Write the failing tests** in `QuicConnectionSuite`. Each test uses a `QuicTestPeer` server, plus a client-side `QuicChannel` dialed with the same codec settings and handed to a `QuicConnection` through `attach`. PARANOID leak detection is on.
  - `small messages arrive as datagrams and large ones on streams`: the peer records the transport per message at 1024 and 1025 bytes.
  - `large messages are spread across all 4 streams`: send 40 messages of 64 KiB, and the peer sees 4 distinct inbound stream ids with roughly 10 messages each.
  - `messages submitted before attach are delivered after the handshake, including datagram-sized ones`.
  - `handshake failure drops pre-handshake messages and completes barriers` *(Review Focus 3)*: call `handshakeFailed`, then the barrier is done, `drops` equals the number submitted, and nothing leaks.
  - `handoff over budget drops on the caller thread`: with `connectionBudgetBytes = 1 MiB` and the loop blocked by a latch task, submit 3 messages of 512 KiB. Results are `true, true, false`.
  - `message larger than budget is accepted when queue is empty` *(Review Focus 4)*: a 4 MiB message with a 1 MiB budget is delivered intact.
  - `stalled peer causes drops, not queueing, and recovers`:
    - With `peer.stallStreams = true` and `initialMaxStreamDataUnidirectional = 256 KiB`, send 200 messages of 64 KiB. `drops > 0`, and the loop's pending outbound bytes per stream never exceed high water plus one message.
    - Then un-stall, and new sends are delivered.
  - `a burst from many threads schedules few drains`: 8 threads × 1000 small submits, all delivered, with the drain-execution count (exposed `private[quic]`) much lower than 8000. Assert `< 8000 / 10`.
  - `concurrent submits from one thread keep their order`: 1 thread × 500 stream-sized messages carrying sequence numbers. The peer sees each stream's subsequence in increasing order, and the union is complete.
  - `barrier waits only for earlier writes`: a barrier posted after N sends completes once the N are written, even while another thread keeps submitting.
  - `submit after close returns false and releases`.
- [ ] **Step 2: Run it.** `sbt 'testOnly *QuicConnectionSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `QuicConnection`.**
- [ ] **Step 4: Run it.** Expected: PASS, with no `LEAK:` lines.
- [ ] **Step 5: Commit.** `git commit -m "Add per-connection QUIC send machinery with drop-on-full"`

---

### Task 6: `QuicNet`, event loops, listener, dialing, routing, and dispatch

**Files:**
- Create: `quic/QuicHostEntry.scala`, `quic/QuicNet.scala`
- Test: `.../quic/QuicNetRoutingSuite.scala`, `.../quic/QuicNetFixture.scala`

**Interfaces:**
- Consumes: Tasks 1–5. `MetadataManager`, `MetadataManager.NetworkImplInterface`, `MessageHandler`.
- Produces:
```scala
class QuicHostEntry(hostId, name, address, dataPort, httpPort, storeTransferPort)
    extends MetadataManager.HostEntry(...):
  val connection: AtomicReference[QuicConnection | Null]
  @volatile var lastHeartbeatTime: Long
  @volatile var isOnline: Boolean
  def heartbeatReceived(): Unit; def setOffline(): Unit   // same logging as ZMQHostEntry

class QuicNet(initialBootstrapConfig: BootstrapConfig.Config,
              onBootstrapConfigRefreshed: (String, BootstrapConfig.Config) => Unit,
              val oclientId: Option[ClientId],
              val ohost: Option[(HostId, Int)],     // (hostId, dataPort) — listen iff defined
              val heartbeatPeriod: Duration,
              val messageHandler: MessageHandler,
              tls: TlsCredentials,
              config: QuicConfig = QuicConfig()) extends NetworkPlugin with Logging:
  val metadataManager: MetadataManager[QuicHostEntry]
  private[quic] def connectionCount: Int     // tests
  private[quic] def clientRoutes: Int        // tests: size of ClientId map
```

**Behaviors pinned by the spec:**
- **Construction order.** The event loop group, the client datagram channels, and (if `ohost`) the server channels are created **before** `metadataManager`, because `MetadataManager`'s constructor calls `createHostEntry` for the bootstrap hosts.
  - The group is `MultiThreadIoEventLoopGroup(config.eventLoopThreads, DaemonThreads.factory("aspen-quic"), handlerFactory)`, using `EpollIoHandler.newFactory()` when `Epoll.isAvailable`, else `NioIoHandler.newFactory()`.
  - Listening happens in the constructor, as `ZMQNet` binds its ROUTER there.
- **Listener.**
  - With epoll: bind `eventLoopThreads` `EpollDatagramChannel`s to `dataPort` with `EpollChannelOption.SO_REUSEPORT`. Each pipeline gets one shared `QuicCodecDispatcher` subclass whose `initChannel` adds a `QuicServerCodecBuilder` configured via `config.applyTo`, with `localConnectionIdLength` and `connectionIdAddressGenerator` / id generator from the dispatcher arguments.
  - Without epoll: one `NioDatagramChannel` with a plain server codec.
- **Outbound.** Each loop owns one client datagram channel bound to port 0 with a `QuicClientCodecBuilder`. Each dial uses `QuicChannel.newBootstrap(channelOfLoop).remoteAddress(...).streamHandler(inbound-stream initializer).handler(connection handler).connect()`, with a `handshakeTimeout` guard that calls `handshakeFailed`.
  - Loops are chosen round-robin.
  - The address is resolved with `InetSocketAddress(entry.address, entry.dataPort)` **on the caller thread**, before `loop.execute`.
- **Inbound streams** (both sides) get a `StreamFrameDecoder(dispatch(conn, _), reason => logger.warn(...))`. Datagrams arrive as `ByteBuf` on the `QuicChannel` pipeline, are decoded with `decodeDatagram`, then released.
- **Inbound connections** (server side) get their own `QuicConnection`, attached on `channelActive`, so replies can be sent on them.
- **`dispatch`** mirrors `ZMQNet.decodeAndDispatch`:
  - `HostHeartbeat` → `metadataManager.getHostEntry(hb.hostId).foreach(_.heartbeatReceived())`.
  - `ClientRequest` → record `fromClient → conn` (only if changed) in a `ConcurrentHashMap[ClientId, QuicConnection]`, then `onClientRequestReceived`.
  - Every other type goes to the matching handler, and `HostResponse` is ignored.
  - Exceptions from handlers are caught and logged so the loop survives.
  - When a connection closes, remove the map entries that point to it.
- **Send path** (`sendToHost(entry, msg)`, `sendToStore`, `sendToClient`), all on the caller thread:
  - Resolve through `metadataManager.getHostEntryOrQueueMessage` (`None` means it was parked).
  - Get or install the connection with a CAS on `entry.connection`, replacing it if `isClosed`. Only the winner dials.
  - `QuicMessageCodec.encode(msg, PooledByteBufAllocator.DEFAULT)`, then `conn.submit(enc)`.
  - `sendToClient` with an unknown `ClientId` is a debug-logged drop.
  - Every send path catches and logs exceptions, so a send never throws to the caller.
- **`NetworkImplInterface`.**
  - `createHostEntry` builds a `QuicHostEntry` and sends each queued message through the send path. It does not dial unless there is something to send.
  - `storeResolved` sends the queued messages.
- **Messengers.** `CliMessenger` and `SrvMessenger` have the same shapes as `ZMQNet`'s, including `dropCacheForStore` → `metadataManager.dropStoreMapping`.
- **`startIoThread(client)`** calls `metadataManager.setAspenClient(client)`. The loops are already running.
- **`joinIoThread()`** blocks until the group terminates.

**Fixture.** `QuicNetFixture` builds a `BootstrapConfig.Config` with two hosts on `127.0.0.1`, free UDP ports, and the bootstrap pool's stores split across them. It creates two host-mode `QuicNet`s and one client-mode `QuicNet`, each with fresh `TestTLS.credentials` and a recording `MessageHandler` (a queue per callback, with an `await(n, timeout)` helper). It closes everything after each test. A host's messenger can address stores in the bootstrap pool without an `AspenClient`.

- [ ] **Step 1: Write the failing tests** in `QuicNetRoutingSuite`.
  - `client request reaches the host owning the store`.
  - `host response reaches the requesting client over the client's connection`: the client has no listener, and `clientRoutes shouldBe 1` on the host.
  - `TxMessages flow host to host in both directions`: each direction uses its own outbound connection, so `connectionCount` on each host is 2 (one outbound, one inbound).
  - `HostMessage reaches the target host`.
  - `TxPrepare with 8 MiB of updates arrives intact` (stream path, end to end).
  - `two clients sharing one certificate get only their own responses`.
  - `host can send to its own stores` *(Review Focus 1)*: host A sends a `ClientRequest` to a store on host A.
  - `responses to a departed client are dropped quietly` *(Review Focus 5)*: close the client net and wait for the host's route removal (`clientRoutes == 0`). `sendClientResponse` does not throw, and no message is delivered.
  - `peer restart mid-transfer recovers` *(Review Focus 2)*: while host A streams a 64 MiB message to B, shut B down and start a new B on the same port. A later send from A is delivered, and there are no leaks.
  - `many threads sending to an unconnected host create exactly one connection`: 16 threads start on a latch, and `connectionCount` goes up by exactly 1.
  - `many threads sending concurrently lose nothing under budget`: 8 threads × 200 mixed-size messages to two hosts, all received.
  - `MessageHandler callbacks run on event loop threads`: the thread name starts with `aspen-quic`.
  - `host with eventLoopThreads = 2 serves clients on both loops`: four clients connect, all are served, and inbound connections land on 2 distinct loops (exposed `private[quic]`).
- [ ] **Step 2: Run it.** `sbt 'testOnly *QuicNetRoutingSuite'`. Expected: compile failure.
- [ ] **Step 3: Implement `QuicHostEntry` and `QuicNet`** (except liveness, flush, and shutdown internals). `awaitPendingMessagesSent` and `shutdown` may be stubs that throw `NotImplementedError` until Task 8. The fixture's teardown uses `group.shutdownGracefully` via a `private[quic] def closeForTests()`.
- [ ] **Step 4: Run it.** Expected: PASS, with no `LEAK:` lines.
- [ ] **Step 5: Commit.** `git commit -m "Add QuicNet routing, dialing, listener and dispatch"`

---

### Task 7: Liveness, heartbeats, keepalive, and idle timeout

**Files:**
- Modify: `quic/QuicNet.scala`, `quic/QuicConnection.scala`
- Test: `.../quic/QuicNetLivenessSuite.scala`

**Behaviors:**
- **Heartbeats.** In host mode, each outbound connection's loop schedules a `HostHeartbeat(hostId)` send every `heartbeatPeriod`, plus one immediately on `attach` (matching `ZMQNet`'s initial heartbeat). It is a datagram and goes through `submit`.
- **Offline detection.** A single task on loop 0, every `heartbeatPeriod`, calls `setOffline()` on each `metadataManager.resolvedHostEntries` whose `lastHeartbeatTime <= now - 3 × heartbeatPeriod`. This is the same rule as `ZMQNet.heartbeat`.
- **Idle timeout.** `config.idleTimeout.getOrElse(heartbeatPeriod * 3)` goes to `maxIdleTimeout` on both codecs.
- **Keepalive.** The **dialing** side tracks the last outbound write time per connection. If the connection has been idle for `idleTimeout / 2`, it sends a keepalive:
  - Prefer a QUIC PING if this Netty version exposes one.
  - Otherwise send a 1-byte keepalive datagram. Its prefix byte is `0` (an invalid `prefixLen`), which the receiver recognizes and discards before decode, without logging.
  - The builder picks whichever exists and documents it in the code.

- [ ] **Step 1: Write the failing tests** with `heartbeatPeriod = 200ms`.
  - `hosts mark each other online after connecting`: `isOnline` within 1s.
  - `a host is marked offline after its peer stops`: shut down B (`closeForTests`), and A's entry for B shows `isOnline == false` within 1.5s.
  - `quiet client connection survives beyond the idle timeout`: with `idleTimeout = 600ms` and the client sending nothing for 2s, a host response sent afterwards still reaches the client, and the client's connection count is unchanged.
  - `dead peer's connection is cleaned up and re-dialed`: kill B, wait for A's connection to B to close, restart B, and A's next send is delivered.
  - `keepalive datagrams are not dispatched` (only if the datagram form is used): the handler records nothing during the idle period.
- [ ] **Step 2: Run it.** `sbt 'testOnly *QuicNetLivenessSuite'`. Expected: FAIL.
- [ ] **Step 3: Implement** the behaviors above.
- [ ] **Step 4: Run it.** `sbt 'testOnly *QuicNetLivenessSuite *QuicNetRoutingSuite'`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Add QuicNet heartbeats, keepalive and idle timeout"`

---

### Task 8: Flush and shutdown

**Files:**
- Modify: `quic/QuicNet.scala`
- Test: `.../quic/QuicNetFlushSuite.scala`

**Interfaces:**
- Produces the real `awaitPendingMessagesSent(timeout: Duration): Boolean` and `shutdown(linger: Duration): Unit` (the `NetworkPlugin` contract).

**Behaviors (spec "Flush and shutdown" and "Threading"):**
- **`awaitPendingMessagesSent`** runs on the caller thread against one deadline:
  1. Poll every 25ms until `!metadataManager.hasParkedMessages`.
  2. `postBarrier()` on every live connection: outbound ones from `resolvedHostEntries`, plus inbound ones from the client map's values.
  3. Await all barriers up to the deadline.
  4. Return `true` only if every step finished in time.
  - It must never be called on an event loop. If it is, assert and throw `IllegalStateException`.
- **`shutdown(linger)`**:
  1. Set `@volatile shuttingDown`. After that, every send is a silent drop.
  2. Sleep `linger` on the caller's thread. `QuicConnectionStats` exposes no "unsent bytes" signal, so the full linger is used.
  3. On each connection's own loop, `ch.close(true, 0, Unpooled.EMPTY_BUFFER)`.
  4. Close the datagram channels.
  5. `group.shutdownGracefully(0, 1, SECONDS)`.
  - Exceptions are swallowed and logged at trace, as in `ZMQNet`.

- [ ] **Step 1: Write the failing tests.**
  - `flush returns true after previously sent messages are accepted`: send 50 mixed messages, flush with a 5s timeout. Expect `true`, and all 50 eventually received.
  - `flush covers pre-handshake messages`: send to a host never contacted before, then flush immediately. Expect `true`, and the message is received.
  - `flush covers messages parked on a metadata lookup`: use `LookupRecordingClient` from `MetadataManagerFixture` as the `AspenClient`. Send to a store in an unknown pool, start the flush on another thread, then complete the pool and host lookup promises with the address of a running host. The flush returns `true`, and the message is received.
  - `flush returns false for unreachable host` *(Review Focus 3)*: send to a host entry whose port has no listener. Flush with 500ms gives `false` within 1.5s, no exception, and no hang.
  - `flush does not wait for messages sent after it was called`: a sender thread runs continuously at the time of the call, and the flush still returns `true` within its timeout.
  - `send, flush, shutdown delivers`, the CLI case: a fresh client-mode `QuicNet` sends one `HostMessage`, calls `awaitPendingMessagesSent(5s)` then `shutdown(1s)`, and the host receives it. Repeat 20 times in a loop, and every iteration delivers.
  - `sends after shutdown are dropped without throwing`.
  - `event loop threads are gone after shutdown`: no live thread named `aspen-quic*` within 2s.
- [ ] **Step 2: Run it.** `sbt 'testOnly *QuicNetFlushSuite'`. Expected: FAIL (`NotImplementedError`).
- [ ] **Step 3: Implement** both methods.
- [ ] **Step 4: Run it.** `sbt 'testOnly *QuicNet*'`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Add QuicNet flush and shutdown"`

---

### Task 9: CLI selection, credentials, and end-to-end

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala` (parser cases), `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicEndToEndSuite.scala`

**Interfaces:**
- Produces:
```scala
enum NetworkKind:
  case Quic, Zmq
// Args gains: network: NetworkKind = NetworkKind.Quic
def createNetwork(bootstrapConfig: BootstrapConfig.Config,
                  onRefresh: (String, BootstrapConfig.Config) => Unit,
                  ohost: Option[(HostId, Int)],
                  oclientId: Option[ClientId],
                  tls: TlsCredentials): (NetworkBridge, NetworkPlugin)
final case class RunningHost(host: Host, network: NetworkPlugin, client: AspenClient)
def startHost(hostDir: Path): RunningHost      // everything run_host did up to and including host.start()
// run_host(hostDir) = { val h = startHost(hostDir); h.network.joinIoThread(); 0 }
```

**Behaviors:**
- **The flag.** A top-level `opt[String]("network").valueName("<quic|zmq>")`, declared in `buildParser` before the `cmd`s. Its text is: `Network implementation to use (default: quic)`. The value is validated, and an unknown one is a usage error.
  - `main` stores the selected kind in a `private var networkKind` next to `onetwork`, set once before dispatch on the main thread, with the same thread-confinement reasoning as the comment on `onetwork`.
  - `createNetwork` builds `QuicNet` or `ZMQNet` from it. `ZMQNet` ignores `tls`.
- **Credentials.**
  - For hosts (`startHost`): `TlsCredentials.fromPem(bootstrapCfg.caCertificate, hostCfg.tlsCertificate, hostCfg.tlsPrivateKey)`.
  - For clients (`createAspenClient(session)`): `TlsCredentials.fromPem(session.bootstrapConfig.caCertificate, session.clientConfig.credentials.certificate, session.clientConfig.credentials.privateKey)`.
  - Thread `tls` through the `createAspenClient(bootstrapConfig, onRefresh, onnet)` overload as a new parameter. Every other caller the compiler flags passes the credentials it has.
- **Unchanged.** `drainAndShutdown` keeps calling `awaitPendingMessagesSent(NotificationDrainTimeout)` then `shutdown(NotificationSendLinger)`. Its comments get updated so they no longer name `ZMQNet` specifically, and the same goes for the comment on `NotificationDrainTimeout` / `NotificationSendLinger`.
- **Docs.** If the CLI's usage or help text, or the docs added in `0a57fb0`, describe the network, mention the flag there.

- [ ] **Step 1: Write the failing tests.**
  - In `MainSuite`:
    - `network defaults to quic`: `buildParser.parse(Seq("list-pools", "<cfg>"), Args())` gives `.get.network shouldBe NetworkKind.Quic`. Use whichever simple command and config fixture `MainSuite` already uses.
    - `--network zmq selects ZMQ`.
    - `--network bogus is a usage error`.
  - In `QuicEndToEndSuite`:
    - Setup: `Main.bootstrap(Replication(3, 2), tmp, "127.0.0.1", free data, http, and store-transfer ports)`, then `Main.startHost(tmp.resolve("bootstrap-host"))`, which uses QUIC because that is the default.
    - Open a client with `Main.openClientSession(tmp.resolve("aspen-client-config.yaml"), None)` and `Main.createAspenClient(session)`, then `network.startIoThread(client)`.
    - `end-to-end transaction over QUIC`: allocate a `DataObject` in the bootstrap pool with 1 MiB of data, inserting its pointer into a KV object as the project's allocation rule requires, commit, then read it back. The bytes match.
    - `end-to-end over QUIC with a 32 MiB object`: same, with 32 MiB, so it exercises the stream path in both directions (a `TxPrepare` out, a `ReadResponse` back).
- [ ] **Step 2: Run it.** `sbt 'testOnly *MainSuite *QuicEndToEndSuite'`. Expected: FAIL (no `network` field, no `startHost`).
- [ ] **Step 3: Implement** the flag, `createNetwork` selection, credentials plumbing, and the `startHost` / `run_host` split.
- [ ] **Step 4: Run the full suite.** `sbt test`. Expected: all suites pass, including the existing ZMQ ones. Then run a manual smoke test: `sbt 'runMain org.aspen_ddp.aspen.cmdline.Main --network zmq ...'` against a bootstrapped host still works.
- [ ] **Step 5: Check client handlers for inline blocking work.** Read `BaseAspenClient.receiveClientResponse`'s callees (`rmgr.receive`, `txManager.receive`). If any blocks or does heavy work inline, which now happens on a Netty event loop, add an entry to `Todo.txt` describing it. Don't add a handoff thread.
- [ ] **Step 6: Commit.** `git commit -m "CLI defaults to the QUIC network plugin; add --network flag and end-to-end tests"`
