# Decouple BaseReadDriver from AspenClient

## Problem

`BaseReadDriver` takes an `AspenClient` constructor parameter but uses only eight
of its members. `BaseReadDriverSuite.TClient` is therefore a hand-rolled
implementation of the entire `AspenClient` interface: 66 lines, roughly 35 of them
`???` stubs that exist solely to satisfy the compiler. Every method added to
`AspenClient` forces an edit to that stub. It is the only hand-rolled `AspenClient`
implementation in the tree -- `TestNetwork.TClient` extends `BaseAspenClient` and so
inherits everything.

## Solution

Declare the eight members in a narrow trait, `ReadDriverClient`, that `AspenClient`
extends. The read driver hierarchy takes that trait instead of `AspenClient`. Since
every `AspenClient` is a `ReadDriverClient`, production wiring is unchanged; only
the declared parameter types move.

### The trait

New file `src/main/scala/org/aspen_ddp/aspen/client/ReadDriverClient.scala`, beside
`ObjectReader.scala` -- the existing precedent for a narrow trait that `AspenClient`
extends. Placing it in package `client` keeps `client` from depending inward on
`client.internal.read`.

```scala
package org.aspen_ddp.aspen.client

/** The slice of a client that a ReadDriver needs. Declared separately from
 *  AspenClient so read-driver tests need not stub the full client interface. */
trait ReadDriverClient:
  val clientId: ClientId
  val txStatusCache: TransactionStatusCache
  def clientContext: ExecutionContext
  def backgroundTaskManager: BackgroundTaskManager
  private[client] def opportunisticRebuildManager: OpportunisticRebuildManager
  private[client] val messenger: Messenger
  private[client] val objectCache: ObjectCache
  private[aspen] def getSystemAttribute(key: String): Option[String]

object ReadDriverClient:
  /** No-op implementation for tests. Follows Messenger.None / ObjectCache.NoCache. */
  class NoOp(val clientId: ClientId) extends ReadDriverClient:
    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache
    def clientContext: ExecutionContext = ExecutionContext.Implicits.global
    def backgroundTaskManager: BackgroundTaskManager = BackgroundTaskManager.NoBackgroundTaskManager
    private[client] def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None
    private[client] val messenger: Messenger = Messenger.None
    private[client] val objectCache: ObjectCache = ObjectCache.NoCache
    private[aspen] def getSystemAttribute(key: String): Option[String] = None
```

Members carry their existing visibility qualifiers verbatim. `private[client]`
remains visible from `client.internal.read`, a subpackage, and from
`BaseReadDriverSuite`, which shares that package. `private[aspen]` likewise.

The no-op lives in the companion object in main rather than in `src/test`, matching
every other narrow trait in the codebase: `Messenger.None`,
`OpportunisticRebuildManager.None`, `ObjectCache.NoCache`,
`TransactionStatusCache.NoCache`, `BackgroundTaskManager.NoBackgroundTaskManager`.
`BaseReadDriver.noErrorRecoveryReadDriver` is existing precedent for test-support
code living in main.

### Member origins

| Member | Used by |
|---|---|
| `messenger` | `BaseReadDriver` -- read requests, opportunistic rebuilds, `dropCacheForStore` |
| `clientId` | `BaseReadDriver` -- outgoing message construction |
| `txStatusCache` | `BaseReadDriver` -- detecting stale locked-write responses |
| `objectCache` | `BaseReadDriver` -- caching successful reads |
| `opportunisticRebuildManager` | `BaseReadDriver` -- `markRepairNeeded` |
| `clientContext` | `BaseReadDriver`, `SimpleReadDriver` |
| `backgroundTaskManager` | `SimpleReadDriver` -- retry scheduling |
| `getSystemAttribute` | `BaseReadDriver.noErrorRecoveryReadDriver` -- naming the test in hung-read logs |

`getSystemAttribute` is the one member that is not a read concern. It is included so
`noErrorRecoveryReadDriver` needs no change; threading the test label through the
factory signature instead would be more churn for no gain.

### Change surface

| File | Change |
|---|---|
| `client/ReadDriverClient.scala` | new |
| `client/AspenClient.scala` | `extends ObjectReader, ReadDriverClient, Logging`; delete the eight now-inherited declarations |
| `client/internal/read/ReadDriver.scala` | `Factory` first parameter `AspenClient` -> `ReadDriverClient` |
| `client/internal/read/BaseReadDriver.scala` | constructor parameter and `noErrorRecoveryReadDriver` parameter -> `ReadDriverClient` |
| `client/internal/read/SimpleReadDriver.scala` | constructor parameter and `Factory.apply` parameter -> `ReadDriverClient` |
| `client/tkvl/SplitFinalizationAction.scala` | drop the unused `BaseReadDriver` import (line 6 is its only occurrence) |
| `test/.../read/BaseReadDriverSuite.scala` | delete `TClient`; use `ReadDriverClient.NoOp(client)`; `mkReader` parameter -> `ReadDriverClient`; trim the now-unused imports |

Unchanged:

- `ReadManager` keeps its `AspenClient`. `driverFactory(client, ...)` still
  typechecks because `AspenClient <: ReadDriverClient`.
- `BaseAspenClient` -- `new ReadManager(this, readDriverFactory)` is unaffected.
- `SimpleAspenClient`, `TestNetwork` -- `TestNetwork.TClient` extends
  `BaseAspenClient` and inherits the members.

Nothing outside `client.internal.read` reads `BaseReadDriver.client`, so widening its
declared type is contained.

### Accepted cost

A reader of `AspenClient.scala` no longer sees `clientId`, `txStatusCache`,
`clientContext`, or `backgroundTaskManager` declared there. A one-line scaladoc on
the `extends` clause pointing at `ReadDriverClient` covers it.

## Verification

Behavior does not change, so no new tests are added -- the test simplification is the
payoff. The compiler is the real check: if `AspenClient` fails to satisfy
`ReadDriverClient`, or a `private[client]` member escapes its scope, it will not
compile.

1. `sbt compile`
2. `sbt 'testOnly *BaseReadDriverSuite'`
3. `sbt 'testOnly *DataObjectReaderSuite'` and `sbt 'testOnly *KeyValueObjectReaderSuite'`
4. `sbt test` -- confirms the `TestNetwork`-based suites still build and pass

## Out of scope

`ClientTransactionDriver` (needs only `messenger`), `SplitFinalizationAction`
(`newTransaction`, `retryStrategy`, `typeRegistry`), and `OpportunisticRebuildManager`
have the same shape of coupling. Their member sets barely overlap with the read
subsystem's, so a single shared trait would not be narrow enough for any one of them.
Per-subsystem traits, later, if the pain justifies it.

`ReadManager` is also left alone. It uses a subset of the same members
(`messenger`, `clientId`, `opportunisticRebuildManager`, `clientContext`,
`backgroundTaskManager`) and could take the trait too, but it holds the client for
the life of the client rather than per-read, and no test stubs it today.
