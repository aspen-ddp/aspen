# Design: Rescuing messages parked behind a pool lookup that names an unknown host

Date: 2026-07-30

## Goal

`MetadataManager.startPoolLookup` silently discards messages parked behind a resolving pool
lookup when the pool names a host the process has never looked up. Route those messages to a
host lookup instead of dropping them.

## The bug

While a pool lookup is outstanding, `getHostEntryOrQueueMessage(storeId, msg)` parks messages in
`PendingPoolLookup.storeQueues`, one `EvictingQueue` per store. When the lookup resolves
(`MetadataManager.scala:248-259`):

```scala
case Success(poolState) =>
  synchronized:
    pendingPoolLookups -= storeId.poolId
    poolState.stores.zipWithIndex.foreach: (se, index) =>
      val sid = StoreId(storeId.poolId, index.toByte)
      stores += sid -> se.hostId
      ppl.storeQueues.get(sid).foreach: storeQueue =>
        hosts.get(se.hostId) match
          case None =>                                     // <-- queue discarded
          case Some(e) => e match
            case Left(phl) => phl.drainIntoQueue(storeQueue)
            case Right(hostEntry) => networkImplInterface.storeResolved(hostEntry, sid, storeQueue)
```

The `None` branch — the store's host is in neither the bootstrap config nor any prior lookup —
does nothing. The queue is unreachable afterwards and its messages are lost.

Two earlier statements in the same block make the loss quiet:

- `stores += sid -> se.hostId` runs first, so *later* sends to that store resolve correctly. Only
  the already-parked messages are affected, which is why the failure does not show up as a broken
  store.
- `pendingPoolLookups -= storeId.poolId` runs first, so `hasParkedMessages` goes false the moment
  the block completes. `ZMQNet.awaitPendingMessagesSent` therefore reports a clean drain over
  messages that were dropped rather than sent.

## Impact

The pool path is reached only by store-addressed sends — `ZMQNet.scala:401`, the `SendToStore`
case: transaction messages, reads, allocations, opportunistic rebuilds. It fires when the target
pool is absent from the bootstrap config *and* the store's host has never been looked up, which is
the first traffic a process sends to a freshly created pool.

For a long-running client the damage is bounded. `stores` is populated before the drop, so the
next re-issue from `ReadManager` or `TransactionManager` takes the host path and succeeds; the
cost is a round trip. A short-lived CLI process has no retry, and its exit drain reports success.
That asymmetry is what makes this a bug rather than an accepted limit.

## Design

### 1. Let the caller own the `PendingHostLookup`

Split `startHostLookup` in two. The existing arity keeps its signature and its two call sites in
`getHostEntry` and `getHostEntryOrQueueMessage`; the new one takes a queue the caller has already
seeded:

```scala
private def startHostLookup(hostId: HostId, oMsg: Option[Message]): Unit =
  val phl = new PendingHostLookup(pendingHostLookupQueueSize)
  oMsg.foreach(phl.enqueueMessage)
  startHostLookup(hostId, phl)

private def startHostLookup(hostId: HostId, phl: PendingHostLookup): Unit =
  // today's body, minus the two lines that constructed and seeded phl
```

Behaviour when `oClient` is `None` is unchanged: the method logs and installs nothing, and the
`phl` is discarded. That branch is unreachable from the pool-lookup call site, which already runs
inside a client callback.

### 2. Start a seeded lookup from the `None` branch

```scala
case None =>
  // Seed before starting the lookup: getHostState's continuation can run inline on this
  // thread, and it builds the host entry from phl.messageQueue.
  val phl = new PendingHostLookup(pendingHostLookupQueueSize)
  phl.drainIntoQueue(storeQueue)
  startHostLookup(se.hostId, phl)
```

`drainIntoQueue` already exists and is used by the `Left` branch immediately below; this gives it a
second, symmetric caller.

Three invariants hold, each a plausible way to get this wrong:

- **A pool whose stores share a host starts one lookup, not several.** `startHostLookup` installs
  `hosts += hostId -> Left(phl)` synchronously before returning — or `Right(entry)`, if the
  continuation ran inline — so a later iteration for the same host matches `Some(...)` and takes
  the existing `Left`/`Right` branch. The `None` branch is reachable at most once per host.
- **The message is never untracked.** The drain happens inside the same `synchronized` block that
  removed the pool entry, so `hasParkedMessages` does not dip false while the message is between
  the two queues. This matters to `awaitPendingMessagesSent`, whose ordering comment
  (`ZMQNet.scala:228-230`) depends on exactly this property for the host handoff.
- **Seeding before starting is load-bearing.** `getHostState`'s continuation can run on the calling
  thread — it does in tests under `ExecutionContext.parasitic`, and can in production whenever the
  future is already complete — and `createHostEntry` receives `phl.messageQueue`. Draining first
  guarantees the rescued messages are in the queue the host entry is built from. Starting the
  lookup and draining afterwards would require a second branch to catch the inline case, and
  would drop the messages if that branch were omitted.

### 3. Recording test double

`MetadataManagerFixture.newManager()` currently supplies the thinnest `NetworkImplInterface` that
satisfies the type: `createHostEntry` ignores its `queuedMessages` argument and `storeResolved` is
`() => ()`. Nothing can observe a message arriving, so a test could only assert that a lookup was
started — which would pass against a fix that started the lookup and still dropped the queue.

Replace it with a named class in the fixture:

```scala
class RecordingNetworkImpl extends MetadataManager.NetworkImplInterface[MetadataManager.HostEntry]:
  val delivered: mutable.Map[HostId, mutable.ListBuffer[Message]]
  val storeResolutions: mutable.ListBuffer[(HostId, StoreId)]
```

`createHostEntry` drains `queuedMessages` into `delivered(hostId)` before building the entry;
`storeResolved` appends `(hostEntry.hostId, storeId)` to `storeResolutions` and drains into
`delivered(hostEntry.hostId)`. Draining rather than peeking mirrors what `ZMQNet` does with both
callbacks.

`newManager()` returns `(manager, client, impl)`. The nine existing `val (mgr, client) =
newManager()` sites — four in `MetadataManagerDrainSuite`, five in
`MetadataManagerPeekHostEntrySuite` — become `val (mgr, client, _) = newManager()`. No existing assertion changes meaning: `hasParkedMessages`
inspects only `Left` entries, so draining at `createHostEntry` time is invisible to it.

The fixture also gains a pool-state builder, since nothing there constructs one today:

```scala
protected def poolStateWith(poolId: PoolId, hostIds: HostId*): StoragePoolState
```

It fills in a `Replication` IDA sized to the host list, a fresh `StorageDeviceId` per store, a
`RocksDBConfig()`, and a fresh `StorageDeviceSetId` — none of which `MetadataManager` reads.

### 4. Documentation

`hasParkedMessages`' scaladoc names the failed-lookup drop as the reason the predicate can go
quiet without the message being sent. Add that a message parked on a pool lookup may migrate to a
host lookup, so that drop can now occur one stage later than where the message was originally
parked.

`awaitPendingMessagesSent`'s scaladoc already states the limit in terms that cover both stages
("a failed host or pool lookup drops the entry and the messages parked on it") and needs no
change.

Remove the corresponding entry from `TODO.txt`.

## Testing

New suite `MetadataManagerPoolLookupSuite`, over `MetadataManagerFixture`:

1. **A resolving pool lookup starts a host lookup for a store on an unknown host.** Park a message
   for `StoreId(unknownPoolId, 0)`; resolve the pool with store 0 on `remoteHostId`; assert
   `client.lookups` names `remoteHostId` and `hasParkedMessages` is still true; resolve the host;
   assert the message appears in `delivered(remoteHostId)` and the predicate goes false.

   This is genuinely red against the current code: no lookup is started, `hasParkedMessages` is
   already false, and nothing is delivered.

2. **Two stores on the same unknown host share one lookup.** Park a message for store 0 and store
   1; resolve the pool with both on `remoteHostId`; assert `client.lookups` has exactly one entry;
   resolve the host; assert both messages were delivered.

3. **A store on an already-resolved host still goes through `storeResolved`.** Resolve the pool
   with store 0 on `bootstrapHostId`, which the constructor resolved from the bootstrap config;
   assert the pair is in `storeResolutions`, the message is in `delivered(bootstrapHostId)`, and
   no host lookup was started. A regression guard on the branch this change leaves alone — and the
   first coverage the `Right` branch has had.

4. **A follow-on host-lookup failure still drops the message.** Park, resolve the pool onto
   `remoteHostId`, then fail that host lookup; assert `hasParkedMessages` goes false with nothing
   delivered. Documents the limit this change does not remove, and pins the scaladoc from
   section 4.

## Files affected

| File | Change |
| --- | --- |
| `src/main/.../common/network/MetadataManager.scala` | Split `startHostLookup`; fix the `None` branch; extend `hasParkedMessages`' scaladoc |
| `src/test/.../common/network/MetadataManagerFixture.scala` | `RecordingNetworkImpl`, `poolStateWith`, `newManager()` returns a triple |
| `src/test/.../common/network/MetadataManagerPoolLookupSuite.scala` | New suite, four tests |
| `src/test/.../common/network/MetadataManagerDrainSuite.scala` | Destructure the triple |
| `src/test/.../common/network/MetadataManagerPeekHostEntrySuite.scala` | Destructure the triple |
| `TODO.txt` | Remove the entry |

`ZMQNet`, `Main`, and the codec are untouched.

## Out of scope

- **Store queues the resolved pool never claims.** The loop iterates `poolState.stores`, so a
  queue keyed by a store index the pool does not contain is never visited and is discarded as
  quietly as the bug above. It requires an out-of-range `StoreId`, and there is nowhere to send a
  message addressed to a store the pool says does not exist. Left as is, without even a log line.
- **The failed-lookup drop itself.** Both `startHostLookup` and `startPoolLookup` discard their
  queues on failure. That is the documented, accepted limit; test 4 pins it rather than fixing it.
- **Pre-warming host lookups for stores with no parked messages.** The loop touches a host only
  when a queue exists for its store. Resolving every host in the pool eagerly would be speculative
  work for a caller that may never send to those stores.
