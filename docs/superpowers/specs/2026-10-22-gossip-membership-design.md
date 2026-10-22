# Gossip Membership and Dissemination (scalecube-cluster over QUIC)

Date: 2026-10-22

## Intent

The QUIC network plugin decides host liveness with an all-to-all heartbeat: every host sends a `HostHeartbeat`
to every other resolved host each `heartbeatPeriod`, and `QuicNet` marks a host offline after three missed
periods. Traffic grows with n², so this will not scale and is not a long-term solution.

Separately, some small but important changes reach peers only slowly, through fallback paths
(`MetadataManager.receivedUnknownStoreFromHost`, host/pool re-lookup, bootstrap config refresh):

- a host's network endpoint (address/ports) changed
- a store moved to a different host after a store transfer

The goal is a gossip layer, built on the scalecube-cluster Java library (SWIM-style failure detection plus
gossip), that runs over the existing QUIC message passing and:

1. replaces the QUIC heartbeat entirely as the source of host online/offline/restarted status, and
2. spreads small internally-defined gossip messages quickly, starting with `StoreMoved` and
   `HostEndpointChanged`.

## Goals

- Scalable failure detection for hosts (no O(n²) periodic traffic).
- Fast dissemination of `StoreMoved` and `HostEndpointChanged` to all hosts.
- Detection of a quick peer restart (a few seconds) that triggers a re-dial, replacing what
  `QuicNet.heartbeatFrom` does today.
- Scalecube fully hidden behind one Scala class with a convenient, idiomatic API.
- Fast unit tests: correct, but with no long timeouts or sleeps.

## Non-goals

- Clients do not join the gossip cluster. Only hosts are members.
- ZMQ is untouched. `ZMQNet` keeps its own `HostHeartbeat` loop until ZMQ is removed. Gossip is QUIC-only.
- Gossip is not a source of truth. Metadata objects (host objects, store/pool metadata) stay authoritative.
- No new consumers of online/offline status in this work. Today `QuicHostEntry.isOnline` is only logged and
  tested. Making it available through the new API is enough.

## Anti-goals

- Leaking scalecube types (`Member`, `MembershipEvent`, Reactor `Mono`/`Flux`, scalecube `Message`) into
  Aspen code outside the wrapper and its transport adapter.
- Gossip that can cause harm when it arrives late, twice, out of order, or not at all. It may only make
  things faster.
- Pinning scalecube's member id to the HostId. That breaks scalecube's restart detection (see below).
- Slow tests that rely on default scalecube intervals or fixed sleeps.

## Findings from reading scalecube-cluster (local checkout `../scalecube-cluster`, 2.7.13-SNAPSHOT)

`git diff release-2.7.12 HEAD` shows no changes under `cluster/src/main`, `cluster-api/src/main` or
`transport-api/src/main`, so every finding below holds for the 2.7.12 release too.

- **Address format.** `Transport.ADDRESS_FORMAT` is `(?<host>^.*):(?<port>\d+$)`. Core scalecube parses
  addresses in only two places:
  - `ClusterImpl.createLocalMember` (skipped when `externalHost`/`externalPort` are configured)
  - `MembershipProtocolImpl.cleanUpSeedMembers`, which calls `parsePort` and compares seed strings with
    `InetAddress.getLocalHost()`-derived strings to drop the local address. That is harmless for fake
    addresses but must be verified by a test.

  Only the Netty transports actually connect to host:port. A custom `Transport` can therefore use any string
  of the form `<x>:<digits>`.
- **Restart detection exists.** Unless `memberId` is configured, each process start gets a random UUID
  member id. Membership is keyed by member id. When a host restarts:
  - the new process appears as a new member (ADDED) at the same address
  - pings addressed to the old member id reach the new process. `FailureDetectorImpl.onPing` sees the id
    mismatch and answers `DEST_GONE`, so the old member is declared dead quickly rather than after the
    suspicion timeout.

  Even a fast restart therefore shows up as "old member for this host removed, new member for this host
  added", in either order.
- **The transport must support both `send` and `requestResponse`.** Scalecube uses request/response
  (correlated by `correlationId` in the scalecube `Message`) for sync and metadata fetch.
- **Payloads.** Scalecube's own messages are Java objects serialized by `MessageCodec` (default
  `JdkMessageCodec`). Gossip payloads are arbitrary `Object` data.
- **Defaults** are tuned for production (ping interval 1 s, sync interval 3 s, gossip interval 200 ms,
  suspicion multiplier 5). Tests must override them.

## Decided design

### Membership

- Each host runs one scalecube cluster member.
- **Member address = `<HostId>:0`.** It is not the real IP:port, so a `HostEndpointChanged` never changes a
  member's identity in scalecube. The transport adapter maps the HostId to the current endpoint the same way
  `QuicNet` already does.
- **Member id stays scalecube's random per-start UUID.** Never set it to the HostId.
- **Cold start and missing seeds** *(my call)*: a host that can reach no seed (the first host up, or every
  bootstrap host down) runs as a cluster of one, and scalecube's periodic sync keeps retrying the seeds.
  Until the first sync, peers are reported offline. Nothing in production gates sends on `isOnline`, so
  this is harmless. Hosts not listed in the bootstrap config join through the bootstrap seeds like any
  other host.
- **Seeds = the hosts listed in the bootstrap config** (the `BootstrapConfig` every host already has via
  `MetadataManager` / `GET /bootstrap`), excluding the local host. When the bootstrap config refreshes, the
  seed list for future (re)joins should reflect it.
- **Boot-id re-dial (connection level).** At startup each host picks a random boot id (its scalecube
  member id is fine). Every scalecube transport message carries the sender's HostId and boot id. A receiver
  remembers the last boot id seen per HostId. When a different one arrives, it closes its outbound QUIC
  connection to that host so the next send re-dials.
  - **Why it's needed:** Netty sends no stateless resets, so after a peer restarts our outbound connection to
    it is black-holed until the idle timeout. Scalecube cannot see the new member until our replies (sync
    response, metadata fetch) get through, so scalecube's restart detection alone would wait out the idle
    timeout. The restarted host's first message reaches us on its own fresh connection and carries the new
    boot id, which breaks the deadlock.
  - **Why boot id rather than "message arrived on a new inbound connection":** connection churn never
    changes the boot id. That rules out the false positives the connection-based rule in `heartbeatFrom` has
    (a lost close frame makes an ordinary re-dial look like a restart, and the two sides can bounce
    re-dials back and forth). No per-connection bookkeeping is needed.
  - The first boot id seen from a HostId only records it and closes nothing.
- **Restart detection relies on scalecube.** Once the boot-id re-dial has restored two-way connectivity, the
  wrapper tracks member ids per HostId. If a HostId gets a
  new member id while an old one is still known (or within a short window after the old one was removed),
  it emits a single `restarted(hostId)` event. There is no `HostRestarted` gossip message.

### Transport adapter (scalecube `Transport` over QUIC)

- Implements scalecube's `Transport`: `address()`, `start`, `stop`, `isStopped`, `send`,
  `requestResponse`, `listen`.
- Parses `<HostId>:0` addresses back to a `HostId` and sends one new Aspen host-to-host message (named by
  the builder, e.g. `ClusterTransportMessage(from: HostId, fromBootId, to: HostId, payload: bytes)`) through
  `QuicNet`'s existing host-message path. The payload is the scalecube `Message` encoded with scalecube's
  `MessageCodec` and is opaque to Aspen.
- Inbound messages of this type are decoded and pushed into the adapter's `listen()` stream.
- `requestResponse` is implemented on top of `send` plus `listen` by matching `correlationId`, with a
  timeout as scalecube expects.
- QUIC's existing "drop, never queue, when buffers are full" policy is fine. Scalecube tolerates loss.
- Messages to a host whose endpoint is not yet resolved follow the existing `MetadataManager` parking
  behaviour. Parking is already bounded by `EvictingQueue`, so it cannot grow without limit.
- *(My call)* Sends to the local member's own address are delivered straight to `listen()` without
  touching the network.

### Gossip wrapper (the public Scala API)

One class (name left to the builder, e.g. `common.network.gossip.GossipService` or `HostMembership`) that
owns the scalecube `Cluster` and hides it completely. It offers:

- `isOnline(hostId): Boolean` and a snapshot of online hosts
- registering listeners for `online(hostId)`, `offline(hostId)` and `restarted(hostId)` events
- `broadcast(g: Gossip): Unit`
- registering listeners for received `Gossip` messages

Callbacks must not run on Netty event loops or block scalecube's scheduler. The builder chooses the
execution context, consistent with how the rest of `QuicNet`/`Host` dispatch work. *(My call)* Events
are delivered in order, one at a time, from a single thread per wrapper. That keeps the wrapper's
per-HostId state and de-duplication simple.

### Gossip messages

- `abstract class Gossip extends common.network.Message` (or sealed, the builder's choice), encoded with the
  existing Aspen codec (`Codec.scala` / protobuf) and carried as the payload of a scalecube gossip.
- **Gossip means "your cached copy is stale; re-read it".** *(My call, made during the builder check.)* A
  message names what changed but carries no new values for receivers to apply. Receivers drop the cached
  mapping or entry, and the existing lookup path re-reads it from the authoritative metadata on the next
  send. So no version stamps or new per-host/per-store version state are needed, and late, duplicate or
  reordered gossip costs at most one extra metadata read. It is still far faster than waiting for a send
  to fail and fall back.
- **`StoreMoved(storeId)`**
  - Sent by the host that completes a store transfer, once the transfer is committed (where
    `TransferComplete` is produced today in `Host`).
  - Receivers call `MetadataManager.dropStoreMapping(storeId)`. That already handles bootstrap stores by
    refreshing the bootstrap config.
- **`HostEndpointChanged(hostId)`**
  - At startup a host compares its configured endpoint with its host object. If they differ, it updates
    the host object via a transaction and, after that commits, gossips `HostEndpointChanged`.
  - **This startup comparison and update does not exist today and is new work.** *(My call)* It runs in
    the background after the host is up and does not block startup. It retries on failure or collision
    and logs. If it never succeeds, peers still have the old endpoint and the existing fallbacks apply.
  - Receivers drop the host's cached entry (a new `MetadataManager` operation, analogous to
    `dropStoreMapping`) and close any outbound connection to it, so the next send re-reads the host
    object and dials the new endpoint.

### Changes to QuicNet

- Remove `HostHeartbeat` sending and handling, `livenessTick`'s offline check and heartbeats,
  `heartbeatAll`, `heartbeatFrom`, `heartbeatConnections`, `heartbeatOverdue` and
  `QuicHostEntry.lastHeartbeatTime`/`heartbeatReceived`.
- `QuicHostEntry.isOnline` is driven by the wrapper's online/offline events.
- `heartbeatFrom`'s connection-based restart rule is replaced by the boot-id re-dial (see Membership). The
  wrapper's `restarted(hostId)` event is for consumers and need not re-dial again.
- QUIC keepalive and `idleTimeout` stay. `idleTimeout` currently defaults to `heartbeatPeriod * 3`. The
  builder may keep `heartbeatPeriod` as the source of that value or rename it, but must not change
  the effective default.
- `HostHeartbeat` itself stays in `Message.scala`/codecs while ZMQ uses it.
- In client mode (no host) the gossip wrapper is not created.

### Dependency

- *My call:* depend on scalecube-cluster **2.7.12** from Maven Central (the latest release), not the local
  2.7.13-SNAPSHOT. Only `scalecube-cluster` and `scalecube-cluster-api` (plus their transitive Reactor
  dependencies) are needed, not `scalecube-transport-netty`.

### Configuration

- Scalecube timings (ping interval/timeout, sync interval, gossip interval, suspicion multiplier) are
  exposed through the wrapper's config with production defaults. Tests use small values.

## Testing

All tests use in-process hosts, short scalecube intervals, and wait on events/conditions (with generous
upper bounds that are only hit on failure) instead of fixed sleeps. Required coverage:

- Fake addresses `<HostId>:0` pass `cleanUpSeedMembers` and member creation, and the local host is not
  treated as its own seed incorrectly.
- Transport adapter: `send` and `requestResponse` round trips, including correlation and timeout.
- Hosts join through bootstrap-host seeds and all see each other online.
- A stopped host is reported offline.
- **Fast restart:** a host restarted quickly under the same HostId (the test must use a QUIC idle timeout
  long enough that the timeout can't be what fixes it) yields a boot-id re-dial and `restarted(hostId)` on
  peers, without an intervening offline being required.
- **No false restart:** an ordinary re-dial with the same boot id (for example, forcibly closing a
  connection) does not close the peer's outbound connection or emit `restarted`.
- `StoreMoved` and `HostEndpointChanged` reach all hosts and cause the cached mapping/entry to be
  re-read. Duplicate delivery is harmless.
- Existing `QuicNetLivenessSuite` tests are rewritten against the gossip-driven status or removed where
  they test the heartbeat specifically.

## Left to the builder

- Class, package and message names; the exact Aspen wrapper message for scalecube traffic.
- Whether the scalecube `MessageCodec` stays the JDK codec or is replaced.
- Threading/execution-context details for callbacks.
- The restart detection window and how it de-duplicates events.
