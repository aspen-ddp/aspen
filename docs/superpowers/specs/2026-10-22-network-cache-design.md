# Network Cache and Peer Endpoint Query

Date: 2026-10-22

## Intent

`MetadataManager` holds two maps that every send depends on:

- `stores`: StoreId → HostId
- `hosts`: HostId → resolved host entry (name, address, data/http/store-transfer ports)

Both live only in memory. A freshly started process fills them through `StoragePool` and `HostState` reads
(`startPoolLookup`, `startHostLookup`), and gossip (`StoreMoved`, `HostEndpointChanged`) keeps them current after
that.

At scale that is a read-amplification problem as much as a latency one. On a 10,000-node system, every restarted
host or long-lived client re-reads connection information it already had the last time it ran, and almost all of
it is unchanged. The aggregate read load from restarts explodes with system size.

This project adds a local, persistent **NetworkCache**. A process starts with those maps already populated from
its last run and goes to the network only for entries it has never seen or that turn out to be wrong. Startup also
gets faster as a side effect.

Trusting cached state means a cached host address can be stale: the host may have moved while this process was
down and it missed the `HostEndpointChanged` gossip. Store mappings already correct themselves (see below), but host
addresses have no such path today. So this project also adds a lightweight **peer endpoint query**: when a host
can't be reached, the process asks a few peers what endpoint they hold for it.

## Goals

- A pluggable `NetworkCache` abstraction with two implementations, `NoCache` and RocksDB.
- `MetadataManager` seeds its store and host maps from the cache at construction and trusts those entries right
  away, exactly as if they had just been read.
- Every change to the in-memory maps is written through to the cache.
- A `bulkUpdate` operation on `NetworkCache`, implemented now, for the follow-on 24-hour slow-path project.
- A peer endpoint query that repairs a stale host address within minutes, without reading the HostState object.

## Non-goals

- **The 24-hour `NetworkCachingService` slow path** (the second item in `Todo.txt`). It's a separate, follow-on
  project. This project only provides `bulkUpdate` for it to call.
- **A HostState drop-and-re-read fallback** for unreachable hosts. We considered it and rejected it. The layers
  are the peer query (minutes), then the 24-hour slow path. An endpoint change is also re-gossiped every 30 minutes
  for 24 hours, so gossip reception is already likely.
- **Peer query over ZMQ.** ZMQNet gets the cache for free through `MetadataManager`, but the peer query is
  QUIC-only. ZMQ is slated for removal.
- **Cache eviction or size management.** The cache holds one small entry per store and per host the process has
  seen. That's bounded by system size and needs no eviction.

## Anti-goals

- **The cache as a source of truth.** Metadata objects stay authoritative, and the bootstrap config stays
  authoritative for bootstrap stores and hosts. The cache may only make things faster. A wrong, missing, corrupt or
  unwritable cache must degrade to today's behavior and never to a stuck or misrouted process.
- **Blocking sends on cache I/O.** Cache writes are best-effort. A failed write is logged and never fails, delays,
  or reorders a send.
- **Dropping a resolved host entry to force a re-read.** `QuicNet.clusterMessage` and `sendClusterMessage` explain
  why: the re-read can wait forever behind the host's own bootstrap stores. The peer query never drops an entry.
- **Peer answers that cost the answering peer a read.** A peer answers only from memory.

## Constraints

- Scala 3, quiet syntax, per the repo `CLAUDE.md`. Use os-lib for filesystem work.
- RocksDB is already a dependency (`server/store/backend/BufferedConsistentRocksDB.scala` uses it). No explicit
  flush or sync on write: it's a cache.
- Timestamps are `HLCTimestamp`, the same ones `MetadataManager` already tracks in `storeTimestamps` and
  `hostTimestamps`. Newer-wins comparison is the only conflict rule, everywhere.
- New network messages follow `common/network/CLAUDE.md`: defined in `common.network.Message`, encoded in `Codec`
  and `codec.proto` (and in `QuicMessageCodec`) with the existing naming pattern.
- Peer query messages must fit in a datagram, at most `QuicMessageCodec.DatagramThreshold` (1024 bytes) encoded,
  so they never take a stream.

## Design

### NetworkCache interface

An abstract, pluggable interface. Entries:

- **Store entry:** StoreId → (HostId, timestamp). The timestamp is that of the `StoragePoolState`/`StoreMoved` value
  the mapping came from.
- **Host entry:** HostId → (name, address, dataPort, httpPort, storeTransferPort, timestamp). The timestamp is that
  of the `HostState`/`HostEndpointChanged` value the entry came from. The name is included because
  `createHostEntry` needs it. The original Todo listed only address and ports.

Operations, at minimum:

- load everything, for use at construction
- upsert a store entry, delete a store entry
- upsert a host entry
- `bulkUpdate(stores, hosts)`: applies a batch of entries, each written only if it's newer than what the cache
  holds

Implementations:

- **`NoCache`:** load returns nothing, and every write and `bulkUpdate` is a no-op. Used by CLI commands, where a
  local cache makes no sense.
- **RocksDB:** used by hosts at `<host-root>/cache/network`, and by long-lived clients such as AmoebaFS. Writes use
  no explicit flush. `bulkUpdate` reads the whole cache into memory in one pass and compares there, rather than
  doing one point lookup per entry, then writes only the entries that are behind. Corrupt or undecodable records are
  skipped with a warning. A database that fails to open is logged, and the process falls back to `NoCache` behavior
  instead of failing to start.

### MetadataManager integration

- The constructor takes a `NetworkCache`, defaulting to `NoCache`, so existing callers and tests are unchanged.
- **System scoping:** the cache records the `aspen-system-id` from the bootstrap config. If that id doesn't match the
  current one at open, the whole cache is discarded and rebuilt. Without this check, a cache left over from another
  or recreated system, carrying higher HLC timestamps, would make the process ignore real gossip.
- **Self entry:** a host never seeds or caches its own HostId. It knows its own endpoint from `ohost`, and
  `Host.reconcileEndpoint` already publishes its endpoint changes.
- **Load order at construction:** load the bootstrap config first, exactly as today, then seed from the cache.
  - Cached store entries go into `stores` and `storeTimestamps`.
  - Cached host entries become resolved entries in `hosts` via `createHostEntry`, with their timestamps in
    `hostTimestamps`.
  - Cached entries for bootstrap stores and bootstrap hosts are ignored: the bootstrap config wins for them. This
    keeps the existing invariant that bootstrap entries carry no timestamp, which `storeMoved` and
    `endpointChanged` rely on.
- **Trust:** seeded entries behave the same as entries from a completed lookup (the user's decision). The first send
  to a cached store goes straight to the cached host with no read. Gossip no newer than a seeded timestamp is
  ignored, as it is today.
- **Write-through:** every path that changes the maps updates the cache.
  - Upserts:
    - `storeMoved`, when it applies
    - `endpointChanged`, when it applies
    - the Success branch of `startPoolLookup` (every store mapping it installs)
    - the Success branch of `startHostLookup` (the entry it builds, whether from the read or from newer gossip)
    - applied peer-query answers
  - Deletes:
    - `dropStoreMapping` (non-bootstrap branch)
    - `receivedUnknownStoreFromHost` when it removes a mapping
  - Bootstrap-config refresh: nothing is written. Bootstrap entries aren't cached.
- **Stale store mappings** need no new mechanism. A host that doesn't have the store replies `UnknownStore`, the
  mapping is dropped (and now also deleted from the cache), and the next send does a fresh pool read.
- Cache writes must not run under conditions that could throw out of the existing retryability and containment
  paths. Wrap each one so a throw is logged and swallowed, following the "repair first, log second" convention the
  file already uses.

### Peer endpoint query (QUIC)

Its purpose is to repair a stale host address quickly and cheaply, without reading the HostState object.

- **Trigger:** outbound dials to a host's current entry have failed continuously for `peerQueryDelay` (configurable,
  default **2 minutes**), *and* the host is reported online. In host mode that report comes from membership; in
  client mode it comes from the relays (`reportedOnline`).
  - That combination is the actual signature of a moved host. It rejoined the cluster over its own outbound
    connections, so it shows as online, but our dials to its old address fail.
  - A host that is really down shows as offline and isn't queried, so deliberately downed hosts cost nothing.
  - "Failure" means a handshake failure or timeout in `QuicNet.connect`.
  - The clock is tracked per HostId in `QuicNet`. It resets when an outbound handshake completes, and when the entry
    is replaced (by gossip or a peer answer), because the replacement is a new address to try. The trigger applies to every host entry, bootstrap hosts included:
  it never drops an entry, so the bootstrap-store deadlock doesn't apply.
- **Query:**
  - A host sends a small `HostEndpointQuery(hostId)` datagram to `peerQueryFanout` (configurable, default 3)
    random hosts that membership reports online, excluding the target.
  - A client sends it to its gossip relays (`relays` in `QuicNet`), since those are the hosts it already holds
    connections to. If the target is itself a relay, or there are fewer than `peerQueryFanout` usable relays, the
    client tops up from other resolved hosts that are reported online.
- **Answer:** the receiving host replies from its in-memory `MetadataManager` entry only, never reading anything. The
  `HostEndpointAnswer` carries the address, ports and timestamp, or "unknown" if it has no resolved, timestamped
  entry. "Unknown" also covers a bootstrap entry, which has no timestamp to compare. A host asked about itself
  answers with its own current endpoint.
- **Apply:** the asker keeps the newest answer. If it's newer than the asker's `hostTimestamps` entry, the asker
  applies it through exactly the same path as `HostEndpointChanged` gossip: `MetadataManager.endpointChanged`, then
  closing the old outbound connection. That path then writes through to the cache. If the answer is not newer, it
  is ignored.
- **Repeat:** datagrams can be lost and peers can be stale too. While the host stays unreachable, the query repeats
  every `peerQueryInterval` (configurable, default 2 minutes), choosing fresh random peers each time. It stops as
  soon as a connection comes up.
- **Cost:** a host that is simply down, rather than moved, costs each peer that keeps trying to reach it a few
  datagrams every couple of minutes. We accept that.
- The settings live with the other QUIC network/gossip configuration (`GossipConfig` or the QUIC config). A test
  configuration with short timings is needed, like `GossipConfig.ForTests`.

### Wrong-host detection

**Problem.** A trusted cached address can be up but belong to a *different* host, for example after a DHCP
reassignment while this process was down. The connection succeeds, so the peer-query trigger never fires, and
messages go to the wrong machine.

Store-addressed messages make it worse, because they currently loop:
1. The wrong host replies `TxUnknownStore`.
2. The sender drops the store mapping and re-reads the pool.
3. The pool names the same HostId, so the message goes back to the same stale address.

There are two independent detectors. Both produce the same sender-side signal.

1. **TLS identity check (QUIC):** on an outbound handshake, compare the peer certificate's UUID with the HostId the
   entry was dialed for. A mismatch closes the connection and counts as a wrong-host signal. Today `QuicTls` /
   `AspenTrustManager` verify only against the CA, and "the certificate UUID is not compared to any routing
   identity". This adds that comparison on the client side of outbound host connections.
   - The builder must confirm how a host certificate's UUID maps to its HostId. The check applies only where a
     HostId-bearing certificate is expected.
2. **`WrongHost` reply (transport-independent, the user's decision):**
   - The sending network plugin adds the intended destination HostId to its envelope, which for QUIC is the
     message prefix. This applies to every host-bound message, store-addressed or host-addressed, with no change to
     any message type.
   - The receiving plugin compares that HostId with its own. On a mismatch it drops the message and replies
     `WrongHost(intendedHostId, actualHostId)` over the same connection. That reply is a new message, small enough
     for a datagram.
   - The check lives in the network plugin, not in `Host` (the user's decision).
   - Client processes never receive host-bound messages, so the check is host-mode only.
   - Replies are rate-limited to at most one per connection per short interval, so a burst of misrouted messages
     doesn't produce a burst of replies.

**Sender handling** of either signal:
- Close the connection.
- Mark the entry as known-wrong, so sends to that HostId park or drop as for an unresolved host rather than going
  to the wrong machine.
- Start the peer query immediately, skipping `peerQueryDelay` and the "reported online" condition, since the
  address is now known to be wrong.
- A peer answer newer than the held timestamp replaces the entry and clears the known-wrong mark, through the usual
  `endpointChanged` path.
- The actual host's identity (`actualHostId`) is also useful: if we hold an entry for that host at a different
  address, the builder may treat this as a hint, but not as authority.
- If no peer knows better, the entry stays known-wrong and the peer query keeps repeating, but only for
  `knownWrongRetryAfter` (default 10 minutes). The periodic scan then clears the mark, and the next send re-dials
  the same address. If it still belongs to another host, the TLS check or a `WrongHost` reply marks it again. This
  covers an address reused only briefly, and bootstrap hosts, for which peers answer unknown.
- Only solicited peer answers are applied: one about a host we queried within the last two `peerQueryInterval`s,
  arriving on one of our outbound connections. Newer-wins makes an applied answer permanent, so an unsolicited one
  with a far-future timestamp could otherwise block every later correction.

**Scope:** QUIC only. ZMQ is slated for removal, but the envelope design is what any future plugin implements.

### Wiring

- **Hosts:** `Main.createNetwork` already receives `ohostDir`. It builds the RocksDB cache at
  `<host-root>/cache/network` and passes it through `QuicNet`/`ZMQNet` to `MetadataManager`.
- **AmoebaFS:** uses the RocksDB cache. Builder's call on where it lives (see below).
- **All other CLI commands:** `NoCache`.

## Calls made during design (not user decisions; check these)

- The host-entry cache record includes the host **name**, because `createHostEntry` needs it.
- Bootstrap stores and hosts are never cached, and cached entries for them are ignored at load.
- `MetadataManager` defaults to `NoCache`, so existing tests are unaffected.
- A RocksDB open failure degrades to `NoCache` instead of failing startup.
- Peer-query defaults: 2-minute trigger, 2-minute repeat, fanout 3. Clients query their relays and top up from
  other online hosts.
- The peer query fires only while the target is reported online. That replaces the earlier "a down host costs a
  few datagrams" trade-off with zero cost for down hosts.
- The cache is scoped to the `aspen-system-id` and discarded on mismatch.
- A host never caches or seeds its own entry.
- Wrong-host handling: QUIC only. A known-wrong entry parks sends rather than delivering them. It triggers an
  immediate peer query, and `WrongHost` replies are rate-limited per connection.
- Accepted residual risk, per the user's decision to drop the re-read: a stale entry that no peer can correct
  stays stale until the 24-hour slow path runs.
- A peer answers "unknown" for bootstrap entries, since they have no timestamp.
- Bootstrap hosts are eligible for the peer query.

## Left to the builder

- The exact `NetworkCache` method signatures and the RocksDB key/value encoding. Reusing protobuf via `Codec` is
  the natural fit.
- Where AmoebaFS's cache directory lives. One option is an `amoebafs` command-line option for the directory, with
  `NoCache` when it's absent. Two AmoebaFS processes must not share one RocksDB directory, because RocksDB takes a
  lock on it.
- Verify that nothing in `MetadataManager` or `QuicNet` assumes a pool lookup has happened before a store mapping is
  used. Seeded store mappings skip the pool read.
- How dial-failure tracking is attached to `QuicHostEntry`/`QuicConnection`, and which loop runs the query timer.
- Test structure. Tests must be fast, with no real 2-minute waits. Cover at least:
  - seeding from the cache, and gossip newer or older than a seeded entry
  - bootstrap entries ignored at load
  - write-through on every listed path
  - `bulkUpdate` newer-only behavior for both implementations
  - a corrupt or unopenable cache degrading to `NoCache`
  - a peer query repairing a stale address end to end
  - a peer query ignoring stale or "unknown" answers
  - a TLS UUID mismatch and a `WrongHost` reply, each leading to a known-wrong entry, an immediate query, and repair
  - the store-addressed misroute loop no longer re-reading the pool forever
