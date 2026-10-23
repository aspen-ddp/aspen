# Network Cache, Peer Endpoint Query and Wrong-Host Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist each process's store→host and host→endpoint maps locally. Repair stale host endpoints with a
cheap peer query, and detect cached addresses that now belong to a different host.

**Architecture:**
- A `NetworkCache` trait (`NoCache`, RocksDB) is seeded into and written through by `MetadataManager`.
- `QuicNet` gains three things:
  - an intended-destination HostId in its message envelope
  - `WrongHost`, `HostEndpointQuery` and `HostEndpointAnswer` messages
  - per-host dial-failure tracking that drives the peer query
- `Main` opens a RocksDB cache for hosts and, optionally, for AmoebaFS.

**Tech Stack:** Scala 3, RocksDB (`org.rocksdb`, already a dependency), ScalaPB protobuf (`codec.proto`), Netty QUIC,
ScalaTest.

**Spec:** `docs/superpowers/specs/2026-10-22-network-cache-design.md`

## Global Constraints

- Scala 3 quiet syntax; indented if/then/else; match surrounding comment density and naming (repo `CLAUDE.md`).
- Use os-lib for filesystem work.
- Newer-wins on `HLCTimestamp` is the only conflict rule everywhere.
- Bootstrap stores and hosts are never written to or seeded from the cache. The bootstrap config is authoritative
  for them.
- A host never caches or seeds its own HostId.
- Cache I/O is best-effort. Every write is wrapped so a throw is logged and swallowed ("repair first, log second"),
  and never fails, delays or reorders a send.
- RocksDB writes use no explicit flush or sync.
- New messages are defined in `common/network/Message.scala` and encoded in `Codec.scala` + `codec.proto` with the
  existing `encode`/`decode` naming. QUIC mapping goes in `QuicMessageCodec.toMsg`/`assemble`.
- Peer-query and wrong-host messages must encode to ≤ `QuicMessageCodec.DatagramThreshold` (1024) bytes.
- Defaults:
  - `peerQueryDelay` = 2 minutes
  - `peerQueryInterval` = 2 minutes
  - `peerQueryFanout` = 3
- Peer query and wrong-host detection are QUIC-only. ZMQNet only gets the cache.
- Tests must be fast: no real multi-minute waits; use `GossipConfig.ForTests`-style short timings.
- Run tests with `sbt 'testOnly *SuiteName'`.

## Review Focus

1. **Two processes pointing at one cache directory** (e.g. two AmoebaFS instances): the second must start with
   `NoCache` and a warning, not crash. Test in Task 1.
2. **A cache from another system id**, or one with corrupt records: it must be wiped or skipped, never trusted. Test
   in Task 1.
3. **A stale peer answer arriving after a fresher one, or a lost query:** the older answer must not overwrite the
   newer one, and the query must repeat. Test in Task 5.
4. **A burst of misrouted messages on one connection:** at most one `WrongHost` reply per rate-limit interval. Test
   in Task 6.
5. **The failure clock after an entry is replaced** (by gossip or an answer): it must restart, so the new address
   gets a full `peerQueryDelay` before another query. Test in Task 5.

---

### Task 1: NetworkCache abstraction, NoCache and RocksDB implementation

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/NetworkCache.scala`
- Create: `src/main/scala/org/aspen_ddp/aspen/common/network/RocksDBNetworkCache.scala`
- Modify: `src/main/protobuf/codec.proto` (add `NetworkCacheStore`, `NetworkCacheHost` messages)
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala` (encode/decode for the two records)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/NetworkCacheSuite.scala`

**Interfaces:**
- Produces:
  ```scala
  final case class CachedStore(storeId: StoreId, hostId: HostId, timestamp: HLCTimestamp)
  final case class CachedHost(hostId: HostId, name: String, address: String, dataPort: Int,
                              httpPort: Int, storeTransferPort: Int, timestamp: HLCTimestamp)
  final case class NetworkCacheContent(stores: List[CachedStore], hosts: List[CachedHost])

  trait NetworkCache:
    def load(): NetworkCacheContent
    def putStore(e: CachedStore): Unit
    def removeStore(storeId: StoreId): Unit
    def putHost(e: CachedHost): Unit
    /** Writes each entry only if it is newer than the one held (or none is held) */
    def bulkUpdate(content: NetworkCacheContent): Unit
    def close(): Unit

  object NetworkCache:
    object NoCache extends NetworkCache          // load() is empty, everything else a no-op
    /** RocksDB at `dir`, scoped to `systemId`; NoCache (with a logged warning) if it cannot be opened */
    def open(dir: os.Path, systemId: UUID): NetworkCache
    def directoryFor(hostRoot: os.Path): os.Path = hostRoot / "cache" / "network"
  ```

- [ ] **Step 1: Write the failing tests** in `NetworkCacheSuite`. Each RocksDB test uses a fresh `os.temp.dir()`.
  - `"NoCache loads nothing and ignores writes"`: after `putStore`/`putHost`/`bulkUpdate`, `load()` ==
    `NetworkCacheContent(Nil, Nil)`.
  - `"RocksDB round-trips stores and hosts across reopen"`:
    - `putStore(s1)`, `putHost(h1)`, `close()`.
    - `open(dir, sys)` and `load()` contain exactly `s1` and `h1`, with every field equal.
  - `"removeStore deletes the mapping"`.
  - `"putStore and putHost overwrite unconditionally"`: the later put wins even with an older timestamp. Only
    `bulkUpdate` compares.
  - `"bulkUpdate writes only newer entries"`:
    - held: store at ts 10, host at ts 10.
    - bulk: the same store at ts 5, the same host at ts 20, and a new store at ts 1.
    - `load()` has the store at 10, the host at 20, and the new store.
  - `"a different system id wipes the cache"`: write with `sysA`, reopen with `sysB` → `load()` is empty. Reopen
    with `sysA` → still empty, because the wipe is persistent.
  - `"corrupt records are skipped"`: write a garbage value under a store key using a raw `RocksDB` handle, then
    `load()` returns the valid entries and does not throw.
  - `"a second open of a locked directory degrades to NoCache"`: `open(dir)` twice without closing. The second is
    `NetworkCache.NoCache`.

- [ ] **Step 2: Run them to verify they fail**

  Run: `sbt 'testOnly *NetworkCacheSuite'`. Expected: compilation failure, because `NetworkCache` is not defined.

- [ ] **Step 3: Implement `NetworkCache.scala` and `RocksDBNetworkCache.scala`.**
  - **Key layout:**
    - `'s'` + pool UUID (16 bytes) + store index (1 byte) for stores.
    - `'h'` + host UUID (16 bytes) for hosts.
    - `'m'` + `"system-id"` for the scope marker (16 UUID bytes).
  - **Values:** the new protobuf records, serialized via `Codec`:
    - `NetworkCacheStore { UUID hostId; uint64 timestamp }`
    - `NetworkCacheHost { UUID hostId; string name; string address; uint32 dataPort; uint32 httpPort;
      uint32 storeTransferPort; uint64 timestamp }`
  - **On open:** if the stored system id is absent or different, delete every key with a `deleteRange` over the
    whole keyspace, then write the marker.
  - **`load()`:** iterate everything. Decode failures are logged at warn and skipped.
  - **`bulkUpdate`:** call `load()` once into maps, compare in memory, then write one `WriteBatch` holding only the
    entries that are behind.
  - **Errors:** every method other than `open` and `load` catches `NonFatal` and logs.
  - **Opening:** follow `BufferedConsistentRocksDB` (`new Options().setCreateIfMissing(true)`, `RocksDB.open`).
    `os.makeDir.all(dir)` first.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `sbt 'testOnly *NetworkCacheSuite'`. Expected: all PASS.

- [ ] **Step 5: Commit**
  ```bash
  git add src/main/protobuf/codec.proto src/main/scala/org/aspen_ddp/aspen/common/network/{NetworkCache,RocksDBNetworkCache,Codec}.scala src/test/scala/org/aspen_ddp/aspen/common/network/NetworkCacheSuite.scala
  git commit -m "NetworkCache: pluggable store/host cache with NoCache and RocksDB implementations"
  ```

---

### Task 2: MetadataManager seeding and write-through

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala`
  - add `RecordingNetworkCache`
  - add `newManager(cache, selfHostId)` overload
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerNetworkCacheSuite.scala`

**Interfaces:**
- Consumes: `NetworkCache`, `CachedStore`, `CachedHost`, `NetworkCacheContent` (Task 1).
- Produces:
  - New constructor parameters, added after `networkImplInterface` and before the queue sizes, with defaults:
    - `networkCache: NetworkCache = NetworkCache.NoCache`
    - `selfHostId: Option[HostId] = None`
  - `def hostTimestamp(hostId: HostId): Option[HLCTimestamp]`: the timestamp held for a host, if any.
  - `def endpointAnswer(hostId: HostId): Option[HostEndpointChanged]`:
    - the resolved, timestamped entry for `hostId`, expressed as the gossip message it is equivalent to
    - None for an unresolved host, a bootstrap host with no timestamp, or an unknown host
    - never starts a lookup
  - `endpointChanged(m)` is unchanged in signature, and now also writes through.

- [ ] **Step 1: Write the failing tests** in `MetadataManagerNetworkCacheSuite`. `RecordingNetworkCache` is an
  in-memory `NetworkCache` that serves a preset `load()` and records every put and remove.
  - `"cached store and host are used with no lookup"`:
    - The cache holds `CachedStore(StoreId(unknownPoolId,0), remoteHostId, ts 5)` and
      `CachedHost(remoteHostId, …, ts 5)`.
    - `getHostEntryOrQueueMessage(thatStore, msg)` returns `Some(entry)` with the cached address.
    - `client.lookups` and `client.poolLookups` are empty.
  - `"gossip older than a seeded entry is ignored, newer is applied"`:
    - `storeMoved` at ts 4 leaves the mapping. At ts 6 it changes it.
    - The same pattern holds for `endpointChanged`.
  - `"cached entries for bootstrap stores and hosts are ignored"`:
    - The cache maps `StoreId(poolId,0)` to `remoteHostId`, and holds a `CachedHost(bootstrapHostId, address
      "9.9.9.9")`.
    - The mapping and address stay as the bootstrap config gives them.
  - `"self host entry is neither seeded nor written"`:
    - With `selfHostId = Some(remoteHostId)`, a cached self entry is not seeded.
    - A completed lookup of `remoteHostId` records no `putHost`.
  - `"write-through on every mutation path"`. One assertion per path on the recorded calls:
    - `storeMoved` applied → `putStore`
    - `endpointChanged` applied → `putHost`
    - pool lookup success → `putStore` for every store in the pool
    - host lookup success → `putHost` with the read's timestamp, or with newer gossip's when that won
    - `dropStoreMapping` (non-bootstrap) → `removeStore`
    - `receivedUnknownStoreFromHost` that removes → `removeStore`
    - bootstrap refresh → nothing
  - `"a throwing cache never breaks a send"`: `RecordingNetworkCache` set to throw on every write. A pool lookup
    still resolves and delivers the parked message.
  - `"endpointAnswer"`:
    - Some for a resolved, timestamped host.
    - None for a bootstrap host, a pending lookup and an unknown host.
    - Calling it starts no lookup.

- [ ] **Step 2: Run them to verify they fail**

  Run: `sbt 'testOnly *MetadataManagerNetworkCacheSuite'`. Expected: compilation failure (no `networkCache`
  parameter).

- [ ] **Step 3: Implement in `MetadataManager.scala`.**
  - **Seeding.** In `loadInitialBootstrapConfig`, after the bootstrap hosts are installed, seed from
    `networkCache.load()` under the same monitor, skipping bootstrap stores, bootstrap hosts and `selfHostId`. Cached
    hosts go through `createHostEntry` with a fresh `EvictingQueue(1)`. A throw there is logged and that host is
    skipped.
  - **Writing.** Add private `cacheStore`/`uncacheStore`/`cacheHost` helpers that apply the skip rules and wrap the
    call in `try … catch NonFatal => logger.warn`. Call them on each path listed in the test.
  - **Docs.** Update the scaladoc of `MetadataManager` and `loadInitialBootstrapConfig` to describe the seed.
  - **Existing callers.** `QuicNet` and `ZMQNet` compile unchanged because of the defaults.

- [ ] **Step 4: Run tests to verify they pass, along with the existing MetadataManager suites**

  Run: `sbt 'testOnly *MetadataManager*'`. Expected: all PASS.

- [ ] **Step 5: Commit**
  ```bash
  git commit -am "MetadataManager: seed from and write through to NetworkCache"
  ```
  Add the new suite to the commit with `git add` first.

---

### Task 3: Wire the cache into QuicNet, ZMQNet and Main

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNet.scala`
  - constructor param `networkCache: NetworkCache = NetworkCache.NoCache`, passed to `MetadataManager` along with
    `selfHostId = ohost.map(_._1)`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ZMQNet.scala` (same param)
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala`
  - `createNetwork` opens the cache
  - new `amoebafs` option
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNetFixture.scala`
  - `host(...)` accepts an optional `networkCache`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNetCacheSuite.scala`

**Interfaces:**
- Consumes: `NetworkCache.open`, `NetworkCache.directoryFor` (Task 1); `MetadataManager`'s new params (Task 2).
- Produces:
  - `QuicNet(..., networkCache: NetworkCache = NetworkCache.NoCache)`, appended as the last constructor param.
  - `Main.createNetwork(..., onetworkCacheDir: Option[Path] = None)`.

- [ ] **Step 1: Write the failing test.** In `QuicNetCacheSuite`, `"a host restarted with a warm cache reaches a
  non-bootstrap host without a lookup"`:
  1. Start fixture hosts A and B, plus host C, which is not in the bootstrap config but is in `directory`.
  2. A sends a `HostMessage` to C, which resolves through the directory, using a RocksDB cache in a temp dir.
  3. Restart A with the same cache dir, after removing C from `directory`.
  4. A message from A to C is delivered.

  It can only arrive via the cache, since the lookup would now fail.

- [ ] **Step 2: Run to verify it fails**

  Run: `sbt 'testOnly *QuicNetCacheSuite'`. Expected: compile failure, then delivery timeout once the parameter
  exists but isn't used.

- [ ] **Step 3: Implement the wiring.**
  - **`Main.createNetwork`:**
    - Host mode, when `ohostDir` is present: `NetworkCache.open(NetworkCache.directoryFor(hostDir),
      bootstrapConfig.aspenSystemId)`.
    - Otherwise, when `onetworkCacheDir` is present: `NetworkCache.open(dir, bootstrapConfig.aspenSystemId)`.
    - Otherwise: `NoCache`.
  - **`amoebafs` command:** add `--network-cache-dir <dir>`. The option's help text says the directory must not be
    shared by two AmoebaFS processes. Thread it to `createNetwork` through `createAspenClient`.
  - Leave every other command on `NoCache`.
  - Nobody closes the cache: the architecture is crash-only, and RocksDB recovers its WAL.

- [ ] **Step 4: Run tests**

  Run: `sbt 'testOnly *QuicNetCacheSuite *QuicNet* *ZMQ*'`. Expected: all PASS.

- [ ] **Step 5: Commit**
  ```bash
  git commit -m "Wire NetworkCache into QuicNet, ZMQNet, hosts and amoebafs"
  ```
  Add the new suite to the commit with `git add` first.

---

### Task 4: Envelope destination HostId and the three new messages

**Files:**
- Modify: `src/main/protobuf/codec.proto`
  - add `UUID to_host = 100;` to `Message`, outside the `oneof`
  - add `HostEndpointQuery = 33`, `HostEndpointAnswer = 34` and `WrongHost = 35` to the oneof
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Message.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicMessageCodec.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/StreamFrameDecoder.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/zmqnet/ProtobufMessageCodec.scala`
  - add the three messages so its match stays exhaustive; ZMQNet ignores them on receive
- Modify: the existing call sites of `decodeDatagram`/`StreamFrameDecoder` (QuicNet and tests)
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicMessageCodecSuite.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala`

**Interfaces:**
- Produces:
  ```scala
  // Message.scala
  /** Asks a host for the endpoint it holds for `hostId`. Answered over the connection it arrived on. */
  final case class HostEndpointQuery(hostId: HostId) extends Message
  /** None: the answering host holds no resolved, timestamped entry for `hostId` */
  final case class HostEndpointAnswer(hostId: HostId, endpoint: Option[HostEndpointChanged]) extends Message
  /** The receiving host is `actualHostId`, not `intendedHostId`; the message was dropped */
  final case class WrongHost(intendedHostId: HostId, actualHostId: HostId) extends Message

  // QuicMessageCodec
  final case class Received(msg: Message, toHost: Option[HostId])
  def encode(msg: Message, alloc: ByteBufAllocator, streamOnly: Boolean = false,
             toHost: Option[HostId] = None): Encoded
  def decodeDatagram(buf: ByteBuf): Either[String, Received]
  def assemble(prefix: Prefix, sections: Array[DataBuffer]): Either[String, Received]

  // StreamFrameDecoder
  class StreamFrameDecoder(onMessage: Received => Unit, onProtocolError: String => Unit, ...)
  ```

- [ ] **Step 1: Write the failing tests.**
  - In `QuicMessageCodecSuite`:
    - `"toHost round-trips in datagrams and stream frames"`: encode a `TxPrepare` with sections and a `Read` with
      `toHost = Some(h)`, and decode each via both paths. `received.toHost == Some(h)` and the message is equal.
    - `"absent toHost decodes as None"`.
    - `"peer-query and wrong-host messages fit in a datagram"`: encode each of the three, with a 255-char address in
      the answer. `isDatagram` is true.
  - In `CodecRoundTripSuite`: round-trip each new message, including `HostEndpointAnswer(h, None)`.

- [ ] **Step 2: Run to verify they fail**

  Run: `sbt 'testOnly *QuicMessageCodecSuite *CodecRoundTripSuite'`. Expected: compile failure.

- [ ] **Step 3: Implement.**
  - Encode `to_host` only when `Some`.
  - In `QuicNet`, update the two receive call sites to pass `Received` to `dispatch`. For now `dispatch(conn,
    received)` matches on `received.msg` exactly as before; Task 6 adds the check.
  - `dispatch` ignores the three new messages for now.
  - Adapt the test call sites to `.msg`.

- [ ] **Step 4: Run tests**

  Run: `sbt 'testOnly *QuicMessageCodecSuite *CodecRoundTripSuite *StreamFrameDecoderSuite *Quic*'`. Expected: all
  PASS.

- [ ] **Step 5: Commit**
  ```bash
  git commit -am "QUIC envelope carries the intended destination host; add peer-query and WrongHost messages"
  ```

---

### Task 5: Peer endpoint query

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicConfig.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNet.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNetPeerQuerySuite.scala`

**Interfaces:**
- Consumes:
  - `HostEndpointQuery`, `HostEndpointAnswer` (Task 4)
  - `MetadataManager.endpointAnswer`, `hostTimestamp` (Task 2)
- Produces:
  - `QuicConfig` fields:
    - `peerQueryDelay: Duration = 2.minutes`
    - `peerQueryInterval: Duration = 2.minutes`
    - `peerQueryFanout: Int = 3`
  - A `QuicConfig.ForTests`-style override in the new suite: delay 300 ms, interval 300 ms.
  - Inside `QuicNet`:
    - `private[quic] def requestPeerQuery(hostId: HostId): Unit`: starts an immediate query, ignoring delay and the
      online check. Task 6 uses it.
    - `private[quic] def peerQueriesSentForTests: Long`.
    - `private[quic] def failingSinceForTests(hostId: HostId): Option[Long]`.

- [ ] **Step 1: Write the failing tests** in `QuicNetPeerQuerySuite`, using `QuicNetFixture` with the short config and
  a third host C.
  - `"a stale address is repaired from a peer"`:
    - A's entry for C points at an unused port with ts 1. Seed it with `a.net.metadataManager.endpointChanged`
      before A ever sends to C.
    - B holds C's real endpoint at ts 2, via `b.net.dispatchForTests(HostEndpointChanged(..., ts 2))`.
    - C is online in membership.
    - A sends a `HostMessage` to C repeatedly until one arrives (`eventually`).
    - Then `a.net.metadataManager.hostTimestamp(C) == Some(ts 2)`.
  - `"an unknown or older answer is ignored"`:
    - A holds a stale entry at ts 5.
    - B answers with ts 3, and the other peers answer unknown.
    - After two intervals, A's timestamp is still 5 and queries keep being sent (`peerQueriesSentForTests` grows).
  - `"a host reported offline is not queried"`: crash C, and point A's entry for C at an unused port. After several
    intervals, `peerQueriesSentForTests == 0`.
  - `"the failure clock restarts when the entry is replaced"`:
    - While failing, apply a newer `endpointChanged` that also points at a dead port.
    - `failingSinceForTests(C)` is reset to a value after the replacement.
  - `"a client queries its relays and tops up from other online hosts"`:
    - A client-mode net whose stale target is one of its relays.
    - The query is answered and the entry is repaired.
  - `"queries go out as datagrams"`: the encoded size check covers this; assert `isDatagram` on the query the net
    builds.

- [ ] **Step 2: Run to verify they fail**

  Run: `sbt 'testOnly *QuicNetPeerQuerySuite'`. Expected: compile failure, then FAIL.

- [ ] **Step 3: Implement in `QuicNet`.**
  - **State:**
    - `failingSince: ConcurrentHashMap[HostId, java.lang.Long]` (nanos).
    - `lastQueried: ConcurrentHashMap[HostId, java.lang.Long]`.
  - **Recording failures:**
    - `connectionFor` builds the outbound connection with `onClosed = c => outboundClosed(entry, c)` (as today).
      `outboundClosed` records a failure, `failingSince.putIfAbsent(entry.hostId, now)`, when the closed
      connection never became active (`!c.isActive`).
    - `outboundActive` gets the entry (pass `c => outboundActive(entry, c)`) and removes it from `failingSince`.
    - `endpointChanged` (QuicNet's) removes both maps' entries for the host once the replacement applies.
  - **Scan task:**
    - Scheduled on `loops(0)` every `peerQueryInterval`, started where `relayTask`/`recheckTask` start; cancelled in
      `closeForTests`/`crashForTests`/`shutdown`.
    - For each host with `now - failingSince >= peerQueryDelay`, `reportedOnline(host)`, and `lastQueried` older
      than `peerQueryInterval` → `sendQuery(host)`.
  - **`sendQuery(target)`:**
    - Peers in host mode: `membership.onlineHosts` minus self and target.
    - Peers in client mode: `relays` minus target, topped up with `resolvedHostEntries` reported online, minus
      target.
    - Shuffle, take `peerQueryFanout`, and `sendToHostId(peer, HostEndpointQuery(target))`.
    - Record `lastQueried`.
  - **`dispatch`:**
    - `HostEndpointQuery(h)`: reply `HostEndpointAnswer(h, metadataManager.endpointAnswer(h))` via
      `conn.submit(QuicMessageCodec.encode(...))`, only when `conn != null`.
    - `HostEndpointAnswer(h, Some(e))`: if `metadataManager.hostTimestamp(h).forall(_ < e.timestamp)` then call
      QuicNet's `endpointChanged(e)`. Otherwise ignore. That path writes through to the cache (Task 2).
  - **Spec deviation, note it in the scaladoc:** the spec's "a host asked about itself answers with its own endpoint"
    never arises, since the target is excluded from its own fanout. A host just answers from `MetadataManager` like
    any other.

- [ ] **Step 4: Run tests**

  Run: `sbt 'testOnly *QuicNetPeerQuerySuite *QuicNet*'`. Expected: all PASS.

- [ ] **Step 5: Commit**
  ```bash
  git commit -m "QuicNet: repair stale host endpoints by querying peers"
  ```
  Add the new suite to the commit with `git add` first.

---

### Task 6: Wrong-host detection (TLS identity check and WrongHost reply)

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicConfig.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNet.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicHostEntry.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNetFixture.scala`
  - host credentials are signed for the HostId: `TestTLS.credentials(hostId.uuid)`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/network/implementations/quic/QuicNetWrongHostSuite.scala`

**Interfaces:**
- Consumes:
  - `Received.toHost`, `WrongHost`, `encode(..., toHost)` (Task 4)
  - `requestPeerQuery` (Task 5)
  - `CertificateVerifier.extractId`
- Produces:
  - `QuicConfig` fields:
    - `verifyPeerIdentity: Boolean = true`
    - `wrongHostReplyInterval: Duration = 1.second`
  - `QuicHostEntry` field: `@volatile var knownWrong: Boolean = false`
  - `private[quic] def wrongHostRepliesSentForTests: Long`

- [ ] **Step 1: Write the failing tests** in `QuicNetWrongHostSuite`.
  - `"a cached address answering as another host triggers WrongHost and repair"`:
    - Use `verifyPeerIdentity = false` on A, so only the envelope check is exercised.
    - A's entry for C points at B's port, with ts 1. B holds C's real endpoint at ts 2.
    - A sends a `HostMessage` to C. B's handler never receives it; B drops it and replies.
    - `eventually`, the entry is repaired to ts 2, and a later message reaches C.
  - `"TLS identity mismatch is detected at the handshake"`:
    - The same setup with `verifyPeerIdentity = true`.
    - `wrongHostRepliesSentForTests` on B stays 0, because the connection closed before any message.
    - The entry is still repaired.
  - `"a known-wrong entry sends nothing until repaired"`:
    - With no peer knowing better, messages to C are dropped: B's handler receives none, and B's reply count stays
      ≤ 1.
    - `knownWrong` is true.
  - `"WrongHost replies are rate-limited per connection"`:
    - `wrongHostReplyInterval` = 10 seconds.
    - Dispatch 50 misaddressed messages over one connection to B. Use a `QuicTestPeer` or raw encode with `toHost` of
      a random HostId.
    - `wrongHostRepliesSentForTests == 1`.
  - `"messages without toHost, and client-mode receivers, are never rejected"`: a host receiving a message with
    `toHost = None` dispatches it normally.

- [ ] **Step 2: Run to verify they fail**

  Run: `sbt 'testOnly *QuicNetWrongHostSuite'`. Expected: compile failure, then FAIL.

- [ ] **Step 3: Implement.**
  - **Send:** `sendToHost` encodes with `toHost = Some(entry.hostId)`, and drops (debug log) when `entry.knownWrong`.
  - **Receive, host mode:** in `dispatch`, if `received.toHost` exists and differs from our HostId:
    - Drop the message.
    - Unless this connection replied within `wrongHostReplyInterval`, submit `WrongHost(intended, ourId)` on
      `conn`. Track this per connection, e.g. a field on `QuicConnection` or a `ConcurrentHashMap[QuicConnection,
      Long]` cleaned in `connectionClosed`.
  - **On receiving `WrongHost(intended, _)`:** `markWrong(intended)`.
  - **TLS check:** in `outboundActive(entry, c)`, when `verifyPeerIdentity`:
    - Read `c.attachedChannel.sslEngine().getSession.getPeerCertificates()(0)` and run
      `CertificateVerifier.extractId`.
    - If the result is not `Some(entry.hostId.uuid)`, call `markWrong(entry.hostId)`.
    - Confirm first that host certificates carry the HostId: the bootstrap host's does (`Main.scala:1369`). Check how
      other hosts get theirs (`AspenClient.scala:446` signs a caller-supplied id).
  - **`markWrong(h)`:**
    - Set `knownWrong` on the entry via `peekHostEntry`, and `closeOutbound`.
    - Call `requestPeerQuery(h)`.
    - The scan from Task 5 also treats `knownWrong` hosts as eligible regardless of `reportedOnline` and
      `peerQueryDelay`, repeating every interval.
  - **Replacement:** a replaced entry is a fresh `QuicHostEntry`, so `knownWrong` resets naturally.

- [ ] **Step 4: Run the full network test set**

  Run: `sbt 'testOnly *QuicNetWrongHostSuite *Quic* *MetadataManager* *NetworkCache*'`. Expected: all PASS.

- [ ] **Step 5: Commit**
  ```bash
  git commit -m "QuicNet: detect cached addresses that belong to another host (TLS identity and WrongHost)"
  ```
  Add the new suite to the commit with `git add` first.

---

### Task 7: Documentation and full test run

**Files:**
- Modify: `CLAUDE.md` (Network Layer bullet: mention the NetworkCache, the peer endpoint query and WrongHost in one
  or two lines each)
- Modify: `Todo.txt` (remove the NetworkCache item; leave the slow-path item)

- [ ] **Step 1: Update the docs** as listed above.
- [ ] **Step 2: Run the whole suite**

  Run: `sbt test`. Expected: all PASS, with no new failures compared with `main`.

- [ ] **Step 3: Commit**
  ```bash
  git commit -am "Docs: NetworkCache, peer endpoint query, WrongHost"
  ```
