# BaseAspenClient: De-duplicating SimpleAspenClient and TestNetwork.TClient

**Date:** 2026-07-04
**Status:** Approved design, ready for implementation plan

## Problem

`TestNetwork.TClient` (`src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala`) was
originally a minimal implementation of the `AspenClient` interface that deliberately
disabled error-recovery mechanisms (read retries, transaction retransmits, opportunistic
rebuild, caching) so that unit tests would not have failures masked by recovery logic.

As the `AspenClient` interface grew, more and more code had to be duplicated between
`TClient` and the production `SimpleAspenClient`
(`src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala`). Roughly
80% of the two implementations is now byte-for-byte identical, and every new abstract
method added to `AspenClient` must be implemented twice. This duplication is the
maintenance burden being addressed.

## Goal

Consolidate the shared implementation so that:
- The identical plumbing lives in exactly one place.
- The genuine differences (the "recovery dials") are expressed as a small, clearly-named
  set of extension points.
- `TClient`'s no-recovery behavior is preserved exactly.
- Future additions to `AspenClient` are implemented once, not twice.

## What is shared vs. what differs

**Identical in both today** (moves to the base class):
`typeRegistry`, `retryStrategy`, `backgroundTaskManager`, `allocatorManager`,
`objectRegistry`, `namespacedRegistry`, the five `MetadataTree`s, all five pointer
getters, `getAllocator`, `newTransaction`, both `read` overloads, all three `create*`
methods, the four `getXxxId` methods, `receiveClientResponse`, `sendHostMessage`,
`servicesTkvl`/`getServiceHost`, the `attributes` map + `getSystemAttribute`/
`setSystemAttribute`, `clientContext`, and `shutdown`.

**The recovery dials that differ:**

| Dial | SimpleAspenClient | TClient |
|------|-------------------|---------|
| Read driver factory | `SimpleReadDriver.Factory(initialReadDelay, maxReadDelay).apply` | `BaseReadDriver.noErrorRecoveryReadDriver` |
| Tx driver factory | `SimpleClientTransactionDriver.factory(txRetransmitDelay)` | `ClientTransactionDriver.noErrorRecoveryFactory` |
| `opportunisticRebuildManager` | `new SimpleOpportunisticRebuildManager(this)` | `OpportunisticRebuildManager.None` |
| `objectCache` | `new SimpleObjectCache` | `ObjectCache.NoCache` |
| `txStatusCache` | `new TransactionStatusCache(txStatusCacheDuration)` | `TransactionStatusCache.NoCache` |
| Read IDA source | pool-resolved: `getStoragePool(p.poolId).map(_.ida)` | fixed `ida` field (breaks the bootstrap read cycle) |
| `create*` wrapper | `transactUntilSuccessfulWithRecovery(onFail)(prepare)` | `transact(prepare)` — single attempt, so test failures surface |
| Allocator caching | overrides `getCachedAllocator`/`cacheAllocator` to delegate to `allocatorManager` | trait defaults (no caching) |

**Gaps in TClient today** that this refactor fills by inheritance:
`getStoragePoolId`, `getHostId`, `getAllocationGroupId`, and `createStoragePool` are all
currently `???` in TClient. They become concrete inherited implementations (pure gain —
they become usable in tests).

## Approach

Chosen: **extract a shared abstract base class** (`BaseAspenClient`) that both
`SimpleAspenClient` and `TClient` extend. This was chosen over:

- *TClient extends SimpleAspenClient* — rejected: Simple's recovery is baked into
  `private val`s, it carries production-only delay constructor params, and its
  pool-resolving `read()` is exactly what TClient cannot use.
- *Trait mixin* — functionally similar, but the shared state is a web of `val`/`var`
  members with initialization-order dependencies that an abstract class with constructor
  parameters models more naturally and with fewer init-order footguns.

Dedup scope: **maximal** — the `create*` methods are unified through a single overridable
wrapper seam, and the `???` gaps are filled by inheritance.

### Init-order safety principle

Anything a subclass can compute *without* `this` is a **constructor parameter**
(initialized before the base body runs, so managers built in the base body can safely
reference them). Anything that needs `this`, or is genuine differing logic, is an
**overridable member** — and none of those are referenced during construction.

### BaseAspenClient

Location: `org.aspen_ddp.aspen.client.internal.BaseAspenClient` (main scope, so
`SimpleAspenClient` can extend it and `private[aspen]`/`private[client]` trait members
remain accessible/overridable). `abstract class BaseAspenClient(...) extends AspenClient`.

**Constructor parameters:**
- `executionContext: ExecutionContext`
- `radicle: KeyValueObjectPointer`
- `clientId: ClientId`
- `messenger: ClientMessenger`
- `txStatusCache: TransactionStatusCache`
- `objectCache: ObjectCache`
- `readDriverFactory: ReadDriver.Factory`
- `txDriverFactory: ClientTransactionDriver.Factory`
- `userTypeFactories: List[RegisteredTypeFactory]`

**Overridable extension points (abstract; only called post-construction):**
- `def opportunisticRebuildManager: OpportunisticRebuildManager`
- `protected def resolveIda(pointer: ObjectPointer): Future[IDA]`
- `protected def runCreate[T](onFail: Throwable => Future[Unit])(prepare: Transaction => Future[T]): Future[T]`

**Concrete in the base:** everything listed under "Identical in both today" above.
- Both `read` overloads are implemented via `resolveIda`:
  `resolveIda(pointer).flatMap(ida => rmgr.read(pointer, ida, comment).map(_.asInstanceOf[...]))`
- All three `create*` methods route through `runCreate`. `createStoragePool` passes a
  no-op `onFail` (`_ => Future.unit`), which for Simple is behaviorally identical to
  today's `transactUntilSuccessful`.

### SimpleAspenClient (after)

Public constructor signature unchanged (production callers unaffected). Body reduces to a
`super(...)` call plus overrides:

```scala
class SimpleAspenClient(msngr, clientId, executionContext, radicle,
                        txStatusCacheDuration, initialReadDelay, maxReadDelay,
                        txRetransmitDelay, allocationRetransmitDelay,
                        userTypeFactories = Nil)
  extends BaseAspenClient(
    executionContext, radicle, clientId, msngr,
    new TransactionStatusCache(txStatusCacheDuration),
    new SimpleObjectCache,
    new SimpleReadDriver.Factory(initialReadDelay, maxReadDelay).apply,
    SimpleClientTransactionDriver.factory(txRetransmitDelay),
    userTypeFactories):

  override def opportunisticRebuildManager = new SimpleOpportunisticRebuildManager(this)
  override protected def resolveIda(p) = getStoragePool(p.poolId).map(_.ida)
  override protected def runCreate(onFail)(prepare) = transactUntilSuccessfulWithRecovery(onFail)(prepare)

  override def getCachedAllocator(id) = allocatorManager.get(id)
  override def cacheAllocator(a) = allocatorManager.put(a)
```

Note: `allocationRetransmitDelay` is already an unused constructor parameter today; it is
left as-is (out of scope for this refactor).

### TClient (after)

Stays in test scope. Shrinks from ~163 lines to roughly:

```scala
class TClient(executionContext, msngr, radicle, ida, userTypeFactories = Nil)
  extends BaseAspenClient(
    executionContext, radicle, ClientId(new UUID(0,1)), msngr,
    TransactionStatusCache.NoCache,
    ObjectCache.NoCache,
    BaseReadDriver.noErrorRecoveryReadDriver,
    ClientTransactionDriver.noErrorRecoveryFactory,
    userTypeFactories):

  override def opportunisticRebuildManager = OpportunisticRebuildManager.None
  override protected def resolveIda(p) = Future.successful(ida)
  override protected def runCreate(onFail)(prepare) = transact(prepare)
```

All `???` gaps disappear (inherited). No allocator caching (keeps trait defaults),
consistent with its no-caching stance.

## Invariants that must not break

- No read retries, no tx retransmit, no opportunistic rebuild, and no object/tx-status
  caching in TClient — preserved via the disabled dials passed to `super`.
- TClient reads keep using the fixed `ida` (pool-resolution would recurse on the
  bootstrap pool).
- TClient `create*` stay single-attempt so test failures surface rather than being
  retried away.
- `SimpleAspenClient.createStoragePool` via `runCreate(no-op onFail)` is behaviorally
  identical to today's `transactUntilSuccessful`.
- `SimpleAspenClient`'s public constructor signature is unchanged.

## Testing

`TestNetwork`/`TClient` is the backbone of essentially the entire test suite, so the
existing tests are the regression harness for this change.

- Success criterion: full `sbt test` green with zero behavioral change.
- `BaseReadDriverSuite` continues to pass.
- No new tests are required; this is a pure structural consolidation.

## Files touched

- `src/main/scala/org/aspen_ddp/aspen/client/internal/BaseAspenClient.scala` (new)
- `src/main/scala/org/aspen_ddp/aspen/client/internal/SimpleAspenClient.scala` (reduced to super + overrides)
- `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` (`TClient` reduced to super + overrides)
- `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` — unchanged (no trait changes needed)
