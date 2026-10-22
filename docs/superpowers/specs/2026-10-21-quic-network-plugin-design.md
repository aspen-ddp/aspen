# QUIC Network Plugin — Design

## Intent

Aspen's only network plugin today is `ZMQNet`, which was built for simplicity and shaped around
the quirks of the ZeroMQ API. It has no tests, no encryption, and copies every payload at least
once on each side. It was never meant for real-world use and will likely be removed once the
QUIC plugin is functional and stable.

This project adds a new network plugin, built on the QUIC support added in Netty 4.2, that is
meant to be the network Aspen actually runs on.

**What "high-performance" means here (user's definition):** bulk data throughput. That means
efficient handling of the large binary payloads (object data in `TxPrepare` and similar) that
accompany the small protobuf-encoded messages. Latency matters, but only in the sense that the
design must not add artificial lag such as batching timers or unnecessary handoffs. Small-message
latency is not otherwise a focus.

## Goals

- A new network plugin built on Netty 4.2 QUIC, implementing the same client/server messenger
  roles `ZMQNet` provides today.
- Mutual TLS on every connection, using the existing Aspen CA and the certificates already issued
  to hosts and clients (`common.network.tls`).
- Encoded messages of **1024 bytes or less** are sent as QUIC datagrams. Larger messages go over
  QUIC streams.
- Large messages are round-robined across a **small fixed pool of streams per connection**. This
  avoids TCP-style head-of-line blocking without paying for a stream per message in memory or
  connection state.
- No back-pressure and no unbounded queueing. When a connection's outbound buffers are full, a
  new message is **dropped**, never queued in memory. Aspen already tolerates lost messages.
- A dedicated codec module, in the spirit of `ProtobufMessageCodec` but working on Netty
  `ByteBuf`s rather than `Array[Byte]`.
- A **flush** method that does not return until every message sent before the call has been
  handed to the operating system for delivery (refined in Flush and shutdown below). The CLI
  depends on this because it often sends and exits immediately.
- A **comprehensive correctness test suite**. `ZMQNet` has essentially none, and this plugin must
  not repeat that.
- The CLI supports both plugins and **defaults to QUIC**.

## Non-goals

- A throughput benchmark or performance test harness. Correctness tests only for now (user's
  decision).
- Binding a peer's claimed identity to its certificate (see Identity below). This is a
  follow-up.
- Changes to `DataBuffer`, the store, the transaction code, or anything else outside the network
  plugin to enable zero-copy receive (user's decision).
- Wire compatibility between the ZMQ and QUIC plugins. A system runs one or the other.
- Removing `ZMQNet`. It stays and keeps working, selectable from the CLI.
- Certificate rotation, revocation, or expiry handling. These are already on the backlog from the
  TLS design.

## Anti-goals

- **Queueing in memory to avoid drops.** Back-pressure is not part of Aspen's model. A full
  connection drops.
- **Added latency for throughput's sake.** No Nagle-style batching timers. Writes are flushed as
  soon as the IO thread has them.
- **Pooled Netty buffers escaping the plugin.** Every `ByteBuf` the plugin allocates or receives
  is released inside the plugin. Nothing outside the plugin ever has to call `release()`.
- **Payload copies on the send path.** Object data must not be copied into an intermediate array
  before hitting the wire.

## Decisions

### Buffer handling (user's decisions on scope; mechanics are my call)

Buffer handling is the main factor in this plugin's performance, so the rules are explicit.

**Send path, zero payload copies.**
- The protobuf portion of a message is encoded directly into a pooled `ByteBuf` from the
  channel's allocator.
- Each payload `DataBuffer` (`ObjectUpdate.data`, `PreTransactionOpportunisticRebuild.data`,
  and any other bulk payload the codec carries) is exposed as a `ByteBuf` with
  `Unpooled.wrappedBuffer(dataBuffer.asReadOnlyBuffer())`, which wraps without copying.
- The header and payload buffers are combined into a single `CompositeByteBuf` that is written
  to the stream. Netty releases it once written.
- Ownership is safe because `DataBuffer` contents are immutable by convention, so referencing
  them until the write completes is fine.
- Datagram-sized messages (≤ 1024 bytes encoded) can simply be encoded into a single pooled
  buffer. Copying is irrelevant at that size.

**Receive path, exactly one payload copy (user's decision).**
- Every payload is copied exactly once, out of Netty buffers into a plain heap `ByteBuffer`
  wrapped in a `DataBuffer`.
- This exists because decoded payloads outlive the handler call. They sit in the store until
  the transaction resolves and may land in caches, and `DataBuffer` has no reference counting.
- Nothing pooled is held once the message is handed to `MessageHandler`.

**Incremental decode on streams (my call, prompted by the no-size-cap decision).** Messages can
be arbitrarily large (see Message size), so the receiver must not collect a whole message in
Netty buffers before decoding it.
- The receiver accumulates only the frame length and the length-prefixed protobuf header. These
  are small and can be cumulated normally.
- Once the header is decoded, it gives every payload section's size. The receiver allocates each
  section's final heap buffer up front.
- From then on, each arriving chunk of stream data is copied directly into the current
  section's final buffer, and that Netty buffer is released immediately.
- When the last section is full, the message is complete and is dispatched.
- Pooled memory held per stream is therefore about one chunk plus the header, whatever the
  message size. Receive flow control keeps moving because data is consumed as it arrives, so
  QUIC flow-control windows can never deadlock a large message.

**Malformed lengths.** Every peer has passed mutual TLS, so an inconsistent length means a bug,
not an attack. The decoder checks that the frame length equals the header length plus the sum
of the section sizes declared in the header. If it doesn't, or if any declared size is negative
or exceeds the JVM limit, the stream is closed as a protocol error and logged.

### Wire format (my call)

- Keep the layout of today's `ProtobufMessageCodec`: a length-prefixed `codec.Message` protobuf
  followed by the raw payload sections that already follow it for `TxPrepare` etc. That layout
  already makes zero-copy send possible.
- On streams, every message is preceded by an **8-byte** big-endian frame length, so many
  messages can share one stream and the total message size is never the limit (see Message
  size).
- If today's layout has any 4-byte length fields that would cap a message section below the JVM
  limit, they become wide enough not to. The individual payload size fields can stay 4-byte,
  since a single `DataBuffer` can't exceed `Int.MaxValue`.
- A datagram carries exactly one message and needs no frame length.
- Any changes needed for `ByteBuf` encoding are allowed, since there is no cross-plugin
  compatibility requirement.
- The new codec lives in its own module in the QUIC plugin's package. The existing
  `ProtobufMessageCodec` is left untouched for `ZMQNet`.

### Datagram vs. stream (user's decision; boundary details are my call)

- Encoded size ≤ **1024 bytes** goes as a QUIC datagram. Anything larger goes on a stream.
- The threshold should be a named constant. 1024 bytes plus QUIC/UDP/IP overhead stays under the
  1200-byte minimum QUIC datagram size, so datagrams never need path MTU discovery.
- If a datagram can't be sent (e.g. the peer doesn't support datagrams, or the send buffer is
  full), it is dropped like any other full-buffer case. It does not fall back to a stream.

### Streams per connection (my call)

- Each side of a connection opens its own pool of **unidirectional** streams for what it sends,
  **default 4**, configurable. Opening is lazy, on first large send.
  - This makes sending symmetric: a host replying to a client over the client's connection uses
    exactly the same machinery as a client sending to a host.
  - It also means each stream has one writer and one reader.
- Large messages are assigned round-robin.
- A message never spans streams.
- Streams are long-lived and reused for the connection's lifetime. They are not opened per
  message.
- If a stream is closed or reset, it is replaced lazily the next time it comes up in the
  rotation.

### Dropping when full (user's decision; mechanism is my call)

- Each connection, and each stream on it, has a bounded outbound buffer budget, implemented with
  Netty's write-buffer high-water mark / `isWritable`.
- A message destined for a stream that is not writable is dropped.
- Each drop is counted per connection and logged at debug/trace, rate-limited so a flood doesn't
  flood the log.
- *My call:* when a stream picked by round-robin isn't writable, try the next writable stream in
  the pool before dropping. Drop only if none are writable. This uses the pool's capacity
  without ever queueing.
- *My call:* writability is checked before the write, so a single large message may overshoot
  the high-water mark. That is accepted. A message's payload bytes are the sender's own
  `DataBuffer`s, wrapped and not copied, so the overshoot adds no extra copy of the payload in
  memory. It only delays when the stream next becomes writable.

### Message size (user's decision)

- **The plugin imposes no message size cap.** The only limits are what the JVM imposes: a
  single payload `DataBuffer` can't exceed about 2 GiB (`Int.MaxValue` bytes). A message may
  carry many payloads, so its total size can be larger than that.
- Neither the sender nor the receiver rejects a message for being large. Correctness for large
  messages relies on the 8-byte frame length (Wire format) and incremental decode (Buffer
  handling).
- Tests must cover a message whose total size is well beyond any typical buffer size, e.g.
  several hundred MiB across multiple payloads. They must also cover a single payload large
  enough to exercise many chunks. The point is to show that incremental decode works and that
  pooled memory doesn't grow with message size.

### Connection model (my call; user didn't object)

- **Dialing.** A node opens an outbound QUIC connection to each host it needs to send to, which
  mirrors `ZMQNet`'s dealer-per-host.
- **Host-to-host.** Each host dials the other, giving two independent connections per pair. This
  is simpler than coordinating ownership of a shared connection, and it matches today's
  behavior.
- **Replies to clients.** Clients have no dialable address. A host sends `ClientResponse`s back
  over the connection the client opened, looked up by `ClientId`, just as `ZMQNet` uses its
  ROUTER identity map today.
- **Listening port.** A host listens for QUIC (UDP) on the existing `dataPort` from its host
  entry. No new config fields are added.
- **Reconnect.** A connection that closes is dropped from the host entry and re-dialed the next
  time something is sent to that host. Messages sent while there is no usable connection are
  dropped, consistent with the no-queueing rule.
- **Messages before the handshake completes.** For the first connection to a host, a message
  sent while the handshake is in flight is held in a small bounded per-host queue and released
  when the handshake completes. It is dropped if the queue is full or the handshake fails. Without
  this, the CLI's very first message to any host would always be lost. The bound is the same
  kind `MetadataManager` already uses (`EvictingQueue`).
- **MetadataManager.** The plugin implements `MetadataManager.NetworkImplInterface` with its own
  `HostEntry` subclass, the same way `ZMQNet` does. Messages waiting on a host or pool lookup keep
  `MetadataManager`'s existing bounded queues.

### Identity (user's decision)

- **The certificate proves membership in this Aspen system and nothing more.** Both sides of
  every connection present certificates and verify them against the Aspen CA, using the existing
  `AspenTrustManager` / `CertificateVerifier` logic.
- The UUID inside the certificate is **not** compared to the peer's claimed `HostId` or
  `ClientId`. Binding identity to the certificate (e.g. stopping a CLI certificate from posing as
  a host) is a follow-up, not part of this project.
- **Routing identity is separate from certificate identity.** The CLI uses an ephemeral random
  `ClientId` per process, so several CLI commands can share one certificate and still each get
  their own replies.
- *My call (revised after review):* there is **no hello message**. Routing identity is learned
  from message contents, the way `ZMQNet.updateClientId` already does it. An inbound
  `ClientRequest` records `fromClient → this connection`, and a `ClientResponse` to that
  `ClientId` goes back over the most recently recorded connection. Host identity for liveness
  comes from `HostHeartbeat.hostId`, as it does today.
- A hello would have to arrive before any datagram could be dispatched. Datagrams and stream
  data can arrive in either order, so gating on a hello would drop exactly the first message the
  pre-handshake queue exists to protect. Inbound messages are never gated. Every message is
  dispatched as soon as it is decoded.
- **TLS context plumbing.** Netty QUIC uses BoringSSL via `QuicSslContextBuilder`, not a JDK
  `SSLContext`. The plugin builds its QUIC SSL context from the same CA certificate, entity
  certificate, and private key that `TLSContext.create` takes, reusing `AspenTrustManager` (or
  its `CertificateVerifier` logic) as the trust manager. If `QuicSslContextBuilder` needs
  `KeyManagerFactory` / `TrustManagerFactory` forms, add small helpers to `common.network.tls`
  rather than duplicating verification logic. The TLS design's anti-goal against plugin-specific
  copies of verification still applies.
- Requests use ALPN with an Aspen-specific protocol id.

### Liveness and heartbeats (my call)

- Keep `HostHeartbeat` messages and the existing online/offline tracking on `HostEntry`, so
  everything that depends on host liveness behaves as it does with `ZMQNet`. Heartbeats are small
  and travel as datagrams.
- Set a QUIC idle timeout on connections, a few heartbeat periods long, so dead connections get
  cleaned up and re-dialed.
- *My call:* `HostHeartbeat` only flows host-to-host. To stop a quiet client connection from
  idling out, the **dialing side keeps every connection it opened alive**: it sends a QUIC PING
  (or a tiny keepalive datagram, if Netty doesn't expose PING) whenever the connection has been
  quiet for about half the idle timeout. This matters because a host can reach a client only
  over the client's own connection.

### Plugin seam (my call)

- Introduce a small shared trait for network plugins covering what the CLI and tests need from a
  network:
  - `clientMessenger`
  - `serverMessenger`
  - `clientId`
  - starting the IO side with an `AspenClient` (today `startIoThread`)
  - blocking until the network stops (today `joinIoThread`)
  - flush
  - shutdown
- Both `ZMQNet` and the new plugin implement it. The CLI holds the trait type instead of `ZMQNet`
  everywhere it currently names `ZMQNet` (`Main.createNetwork`, `onetwork`, `createAspenClient`,
  and so on).
- The builder may adjust method names and signatures, but `ZMQNet`'s behavior must not change.

### Flush and shutdown (user's requirement; details are my call)

- **Guarantee (user's decision): "accepted by QUIC, plus a short shutdown wait."**
- `flush(timeout): Boolean` returns once every message submitted before the call has been
  accepted by the QUIC stack, meaning its stream or datagram write future has completed. That
  covers messages waiting in `MetadataManager` lookup queues, the pre-handshake queues, and any
  submission queue. It returns `false` if the timeout expires first.
- Netty QUIC doesn't expose when quiche has actually put the last packets on the wire, so flush
  doesn't claim that. It also doesn't wait for the peer to acknowledge anything.
- Messages dropped (full buffers, failed lookup, failed handshake) count as no longer pending.
  Flush does not wait for them.
- `shutdown(linger)` keeps the event loop and connections up for up to `linger`, so quiche can
  transmit what flush handed it. Only then does it close connections with a QUIC close frame and
  stop the event loop group. This is the same role ZMQ linger plays today.
  - *My call:* shutdown may end the linger early once no connection has unsent stream data, if
    that's observable (e.g. via connection stats). Otherwise it waits the full `linger`.
- The CLI keeps the same exit pattern: `flush(NotificationDrainTimeout)`, then
  `shutdown(NotificationSendLinger)`.
- Like the rest of Aspen, shutdown is not a clean shutdown protocol (crash-only). The close frame
  is just a best-effort courtesy to the peer.

### Threading (my call)

**The principle.** There are exactly two kinds of threads involved:

- **Caller threads.** These are whatever Aspen threads call the messengers: the client's
  `ExecutionContext` threads, the `Host` event thread, timer threads, and so on. They do as much
  of the per-message CPU work as can safely be done off the I/O path.
- **Netty event loop threads.** These do all socket I/O, all QUIC/TLS processing, and the
  receive path.

The plugin adds no threads of its own and puts no extra handoff hop between the caller and the
event loop that owns the connection.

#### Event loops and connection ownership

- One `MultiThreadIoEventLoopGroup` for the plugin, with **N** loops.
  - It uses the epoll I/O handler on Linux and falls back to NIO elsewhere.
  - Its threads come from `DaemonThreads.factory`, so client processes still exit when their
    work is done, as they do with `ZMQNet` today.
  - *Default N:* `min(availableProcessors, 4)` for hosts, and 1 for CLI client commands.
    Configurable.
- **Each connection is owned by exactly one event loop for its entire life.** That loop also
  owns all of the connection's stream channels, because Netty QUIC places a `QuicChannel` and
  its streams on the event loop of the UDP `DatagramChannel` they run over.
  - Consequently, all per-connection mutable state lives only on that loop and needs no locks:
    stream pool, round-robin cursor, pre-handshake queue, in-flight write bookkeeping, and
    decoder state.
- **Inbound (host side).** The host binds N UDP `DatagramChannel`s to `dataPort`, one per loop,
  using `SO_REUSEPORT` with Netty 4.2's `QuicCodecDispatcher`. The dispatcher routes each packet
  to the channel, and therefore the loop, that owns its QUIC connection, keyed by connection ID.
  - The kernel spreads new connections across the N sockets, so inbound connections spread
    across loops.
  - Where `SO_REUSEPORT` is unavailable (non-epoll), the host binds a single channel and all
    inbound connections share one loop.
- **Outbound (both sides).** Each loop owns one unbound client `DatagramChannel` on an ephemeral
  port. Outbound QUIC connections are created with `QuicChannel.newBootstrap(thatChannel)`.
  - A new outbound connection to a host is assigned to a loop round-robin.
  - Re-dials go through the same assignment, so the new connection may land on a different
    loop.

#### Send path: from an arbitrary thread to the wire

Every messenger call (`sendTransactionMessage`, `sendClientRequest`, `sendClientResponse`,
`sendHostMessage`, …) runs these steps on the **caller's** thread:

1. **Resolve the destination.**
   - Store and host destinations go through `MetadataManager.getHostEntryOrQueueMessage`. That
     method is already `synchronized` and safe from any thread.
     - If it parks the message pending a lookup, the call ends here. When the lookup resolves,
       `MetadataManager` calls `createHostEntry` / `storeResolved` on whatever thread finishes
       the lookup. The plugin's implementation feeds the parked messages into step 2 on that
       thread. These callbacks must only hand off, never block, and never call back into
       `MetadataManager` synchronously.
   - A `ClientResponse` is resolved through a `ConcurrentHashMap[ClientId, connection]`, which
     is populated from inbound `ClientRequest`s (see Identity). An unknown `ClientId` is a drop.
2. **Get the host's connection handle.** Each `HostEntry` holds an atomically updated
   reference to its current connection handle.
   - If there is none, or the connection has closed, the caller installs a new handle with a
     CAS, so exactly one thread wins. The winner submits the dial to the handle's assigned
     loop.
   - Address resolution must not block an event loop. If the host's address needs a DNS
     lookup, it is resolved before the dial is submitted, not on the loop.
   - Callers that lose the race just use the winner's handle.
3. **Encode** into Netty buffers on the caller thread, using the shared
   `PooledByteBufAllocator`, which is safe from any thread and keeps per-thread caches.
   - The protobuf header goes into one pooled buffer. Payload `DataBuffer`s are wrapped without
     copying, and both are combined into one `CompositeByteBuf`.
   - Encoding here keeps protobuf serialization off the event loops and yields the exact
     encoded size, which decides datagram vs. stream.
4. **Hand off** to the connection's owning loop through a per-connection lock-free MPSC queue,
   e.g. JCTools, which Netty already depends on.
   - The caller enqueues `(buffer, kind)`. If the queue went from empty to non-empty, the
     caller schedules **one** drain task on the loop with `eventLoop.execute`, guarded by an
     atomic "drain scheduled" flag.
   - So a burst of sends from many threads costs a single loop wakeup, not one task per
     message, and no timer is ever involved. There is no added latency: the drain task runs as
     soon as the loop is free.
   - The handoff queue is **bounded in bytes**, tracked by an atomic counter, with the same
     budget as the connection's write high-water mark. A message that would exceed the budget
     is released and dropped on the caller thread. This keeps the no-queueing rule true even
     before Netty's own buffers come into play.

Then, on the owning **event loop**, the drain task:

5. **Drains the handoff queue** and, for each message:
   - **Connection still handshaking:** move it into the bounded pre-handshake queue, which is
     loop-owned. Drop if full.
   - **≤ 1024 bytes:** write it to the `QuicChannel` as a datagram.
   - **Larger:** pick a stream round-robin from the sender's stream pool, skipping streams that
     aren't writable. Write the frame (8-byte length plus the composite) to it. If no stream is
     writable, release and drop.
   - Each write's future is recorded in the connection's in-flight bookkeeping, which is used
     by flush.
6. **Flushes once per touched channel**, after the whole batch has been written, so quiche
   packs as much as possible into each burst of packets.

#### Receive path: who reads, decodes, and delivers

All of this runs on the **event loop that owns the connection**:

- Netty reads UDP packets, and quiche decrypts them and turns them into datagram and stream
  data on that loop.
- **Datagrams.** The `QuicChannel` pipeline receives each datagram as a `ByteBuf`. The decoder
  copies its payloads (if any) into heap buffers, releases the `ByteBuf`, and dispatches.
- **Streams.** Each inbound unidirectional stream's pipeline has the incremental decoder (see
  Buffer handling). As stream data arrives, it copies straight into the message's final heap
  buffers and releases each Netty buffer immediately. When the message completes, it
  dispatches.
- **Dispatch** is a direct call to the matching `MessageHandler` method on the event loop
  thread. It is not handed to another executor. This is the same synchronous dispatch `ZMQNet`
  does today on its IO thread.
  - Before dispatch, the loop does plugin bookkeeping: recording `ClientRequest.fromClient →
    connection`, and `HostHeartbeat` liveness updates. Liveness fields on `HostEntry` that are
    read off-loop are `volatile`.
- **MessageHandler contract:** handlers are called on a Netty event loop and **must not
  block**. Today's handlers meet this.
  - On hosts, `Host.receive*` just puts the message into an unbounded `LinkedBlockingQueue`.
  - On clients, `receiveClientResponse` hands the message to the read and transaction
    managers.
  - The builder should confirm that the client-side managers don't do blocking or heavy work
    inline. If they do, record it in `TODO.txt` rather than adding a handoff thread inside the
    plugin, because that thread would add latency to every message.
- The one payload copy on receive therefore happens on the event loop. It's a plain memory copy
  done as data arrives, which is the cheapest place for it.

#### Timers

These are all scheduled on the owning connection's loop with `eventLoop.schedule*`, never on a
separate timer thread:

- host heartbeats over each outbound connection
- keepalive pings
- the idle timeout, handled by quiche
- the shutdown linger

#### Flush, in threading terms

`flush(timeout)` runs on the caller's thread and blocks it, never an event loop:

1. Wait (polling, as `ZMQNet` does) until `MetadataManager` reports no parked messages.
2. Post a **barrier** onto every live connection's handoff queue, which travels the same path as
   a message.
3. When the drain task reaches a barrier, it combines that connection's in-flight write futures
   at that moment and completes the barrier when they all have.
   - If the connection is still handshaking, the barrier completes once the handshake resolves
     and the pre-handshake messages have been written or dropped.
4. `flush` waits on all the barriers, up to `timeout`.

Messages submitted after the barrier was posted are not waited for, which gives exactly the
"sent before the call" semantics.

#### Shutdown, in threading terms

`shutdown(linger)` is called from the caller's thread.

1. It marks the network as shutting down, so new sends are dropped.
2. It waits up to `linger` (see Flush and shutdown).
3. It schedules a QUIC close on each connection's own loop.
4. It calls `shutdownGracefully` on the group with a zero quiet period.

`joinIoThread`'s equivalent blocks until the group terminates.

### CLI

- A command-line option selects the plugin, with values `quic` and `zmq`, **defaulting to
  `quic`** (user's decision). There are no deployed systems to stay compatible with. Any tests or
  scripts that rely on the ZMQ default get updated as part of this work.
- Applies to host processes (`run_host` etc.) and client commands alike.
- QUIC needs the entity's certificate and key: hosts read theirs from `aspen-host-config.yaml`
  and clients from `aspen-client-config.yaml`, where both already live.

### Dependencies

- Netty 4.2's QUIC codec (`netty-codec-classes-quic` / `netty-codec-native-quic`) and the native
  classifier(s) needed for the platforms Aspen runs on, at minimum `linux-x86_64`. The builder
  picks the current 4.2.x release and the exact artifact set.

## Testing

Correctness only, and thorough. Tests run in-process over loopback with real QUIC connections and
real certificates from a test CA (`CertificateAuthority.create`). Coverage must include at least
the following.

**Codec**
- Round-trip of every message type through the `ByteBuf` codec.
- `TxPrepare` with multiple object updates and pre-transaction rebuilds.
- Empty and large payloads.
- Payloads that are slices with a non-zero position.
- Malformed and truncated input is rejected without throwing out of the decoder.
- Leak checks: Netty's `ResourceLeakDetector` at `PARANOID` level in tests, and/or explicit
  `refCnt` assertions, so that no encode, decode, or drop path leaks or double-releases a buffer.

**Datagram/stream boundary**
- Messages just under, at, and just over 1024 encoded bytes go over the expected transport.

**Stream pool**
- Large messages are distributed across the stream pool.
- Messages that arrive in fragments are reassembled correctly. This includes a fragment boundary
  inside the frame length, inside the header, between sections, and inside a payload.
- Inconsistent frame/section lengths close the stream as a protocol error without leaking
  buffers.
- Many concurrent large messages all arrive intact.

**mTLS**
- A peer with a certificate from a different CA is rejected (both directions).
- A peer with no certificate is rejected.
- A CA certificate presented as an entity certificate is rejected.
- An expired certificate is rejected.
- Two clients sharing one certificate but using different `ClientId`s each receive only their
  own responses.

**Routing**
- Client→store requests, host→client responses over the client's connection, host→host
  `TxMessage`s, and `HostMessage`s all reach the right `MessageHandler` callbacks.

**Drop on full**
- With a stalled peer, sends beyond the buffer budget are dropped and counted, not queued, and
  memory stays bounded.
- Normal delivery resumes once the peer drains.

**Connection lifecycle**
- Pre-handshake messages, including datagram-sized ones, are delivered once connected.
- They are dropped if the handshake fails.
- Re-dial after the peer restarts.
- The idle timeout cleans up a dead peer.

**Flush**
- Returns `true` only after previously submitted messages, including ones queued pre-handshake or
  in metadata lookup, have been accepted by the QUIC stack.
- Returns `false` on timeout when a host is unreachable.
- A short-lived "send, flush, shutdown" sequence delivers its message (the CLI case).

**Liveness**
- Heartbeats mark hosts online and offline as `ZMQNet` does.

**Threading**
- Many caller threads sending concurrently to the same and to different hosts lose nothing
  while under budget. Per-connection handoff ordering holds for messages sent by one thread.
- The dial race, where many threads send to an unconnected host at once, produces exactly one
  connection.
- Bursts of sends trigger one drain task, not one per message.
- With the handoff byte budget exceeded, sends drop on the caller thread and nothing leaks.
- `MessageHandler` callbacks run on event loop threads.
- `flush` called while other threads keep sending still returns once the earlier messages are
  done.

**Integration**
- At least one end-to-end test running a real transaction (allocate/update/read) through the
  QUIC plugin between in-process hosts and a client.

## Left to the builder

- Exact class and package names. Suggested: `common.network.implementations.quic`, with
  `QuicNet` and a `ByteBufMessageCodec` alongside the existing `zmqnet` package.
- Default buffer budgets, high-water marks, idle timeout, and QUIC transport
  parameters (initial max data, max streams, datagram queue lengths), as named constants, with
  sensible defaults.
- How the pre-handshake queue and flush bookkeeping are implemented, e.g. an in-flight counter
  or per-write futures.
- The CLI flag's spelling.
- Whether the shared plugin trait lives in `common.network` or elsewhere.
- Netty version and artifact selection.
