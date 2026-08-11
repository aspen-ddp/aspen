# ReadDriverClient Trait Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the eight `AspenClient` members that the read driver hierarchy actually uses into a narrow `ReadDriverClient` trait, so `BaseReadDriverSuite` no longer has to stub the entire `AspenClient` interface.

**Architecture:** A new trait `org.aspen_ddp.aspen.client.ReadDriverClient` declares the eight members. `AspenClient` extends it and declares none of them itself, so every existing implementation satisfies it for free. `BaseReadDriver`, `SimpleReadDriver`, and the `ReadDriver.Factory` type alias take `ReadDriverClient` instead of `AspenClient`. `ReadManager` keeps its `AspenClient` and still passes `this` to the factory, which typechecks because `AspenClient <: ReadDriverClient`. A `ReadDriverClient.NoOp` in the trait's companion wires together the no-op singletons that already exist for each member type, replacing the hand-rolled 66-line test stub.

**Tech Stack:** Scala 3, sbt, ScalaTest (`AsyncFunSuite` + `Matchers`), scribe logging.

**Spec:** `docs/superpowers/specs/2026-08-11-read-driver-client-trait-design.md`

## Global Constraints

- Prefer Scala 3 "quiet mode" (significant-indentation) syntax. Braces only where they clearly enhance readability. Existing brace-style files being edited here (`BaseReadDriver.scala`, `SimpleReadDriver.scala`, `ReadDriver.scala`, `BaseReadDriverSuite.scala`) keep their current style — this refactor changes types, not formatting.
- Prefer indented `if/then/else`. Avoid single-line `if a == 1 then value` unless it enhances readability.
- New code uses Scala 3 `extends A, B, C` rather than `extends A with B with C`.
- Do not reformat, reorder, or "improve" lines the task does not name. Every diff in this plan should be reviewable as type-signature churn plus one deletion.
- Run all sbt commands from the repository root (`/workspace`).
- Work happens on the existing branch `read-driver-client-trait`, which already holds the design doc commit.

## Testing Approach — read this before Task 1

This refactor changes **no runtime behavior**. Nothing executes differently
afterward; only declared types move. There is consequently no failing test to
write first, and inventing one would be ceremony rather than verification.

The compiler is the real test here, and it is a strong one:

- If `AspenClient` fails to satisfy `ReadDriverClient`, compilation fails.
- If a `private[client]` or `private[aspen]` member escapes its scope in the
  new trait's package, compilation fails.
- If `AspenClient <: ReadDriverClient` does not hold, `ReadManager`'s
  `driverFactory(client, ...)` call fails to typecheck.

So each task's verification is: `sbt compile`, then the relevant test suites,
then `sbt test` before the final commit. The payoff of the whole change is
Task 3, where 66 lines of stub become one — that deletion is the deliverable,
and the existing six tests in `BaseReadDriverSuite` passing unchanged against
the new no-op is the proof it worked.

Tasks 1 and 2 are each independently compilable and independently revertible.
Do not collapse them.

---

### Task 1: Introduce the ReadDriverClient trait

Create the trait and its no-op, and make `AspenClient` extend it. No consumer
changes yet — after this task the codebase compiles and behaves exactly as
before, with the eight members simply inherited rather than declared.

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/ReadDriverClient.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala` (imports at lines 3, 4, 10, 16; `extends` clause at line 30; declarations at lines 34, 36, 42, 294, 301, 303, 305, 348)

**Interfaces:**
- Consumes: nothing — this is the first task.
- Produces: `org.aspen_ddp.aspen.client.ReadDriverClient`, a trait with these
  eight abstract members, and `ReadDriverClient.NoOp(clientId: ClientId)`, a
  concrete implementation. Tasks 2 and 3 depend on both names exactly as
  spelled here:
  ```scala
  val clientId: ClientId
  val txStatusCache: TransactionStatusCache
  def clientContext: ExecutionContext
  def backgroundTaskManager: BackgroundTaskManager
  private[client] def opportunisticRebuildManager: OpportunisticRebuildManager
  private[client] val messenger: Messenger
  private[client] val objectCache: ObjectCache
  private[aspen] def getSystemAttribute(key: String): Option[String]
  ```

- [ ] **Step 1: Create the trait file**

Create `src/main/scala/org/aspen_ddp/aspen/client/ReadDriverClient.scala` with
exactly this content:

```scala
package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.internal.network.Messenger
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager

import scala.concurrent.ExecutionContext

/** The slice of a client that a ReadDriver needs.
 *
 *  Declared separately from AspenClient so that read-driver tests need not stub the
 *  full client interface. AspenClient extends this trait and declares none of these
 *  members itself, so every AspenClient implementation satisfies it automatically.
 */
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

  /** No-op implementation for tests. Follows the convention set by Messenger.None,
   *  ObjectCache.NoCache, and friends: every member is the existing no-op singleton
   *  for its type, so a test that does not care about a dependency need not name it.
   */
  class NoOp(val clientId: ClientId) extends ReadDriverClient:

    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache

    def clientContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    def backgroundTaskManager: BackgroundTaskManager = BackgroundTaskManager.NoBackgroundTaskManager

    private[client] def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None

    private[client] val messenger: Messenger = Messenger.None

    private[client] val objectCache: ObjectCache = ObjectCache.NoCache

    private[aspen] def getSystemAttribute(key: String): Option[String] = None
```

The trait declares all eight members listed in the Interfaces block, and `NoOp`
implements all eight. If either count is off, Step 6 will not compile.

- [ ] **Step 2: Verify the trait compiles on its own**

Run: `sbt compile`

Expected: SUCCESS. Nothing references `ReadDriverClient` yet, so this only
proves the trait itself is well-formed — that the visibility qualifiers are
legal in package `client` and every imported type resolves.

- [ ] **Step 3: Make AspenClient extend the trait**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala`, change the
`extends` clause on line 30 from:

```scala
trait AspenClient extends ObjectReader with Logging:
```

to:

```scala
/** @see [[ReadDriverClient]] for clientId, txStatusCache, clientContext,
 *       backgroundTaskManager, opportunisticRebuildManager, messenger,
 *       objectCache, and getSystemAttribute, which are declared there.
 */
trait AspenClient extends ObjectReader, ReadDriverClient, Logging:
```

The scaladoc is required, not optional: without it, a reader of this file has
no signal that `clientId` and friends exist.

- [ ] **Step 4: Delete the eight now-inherited declarations**

Still in `AspenClient.scala`, delete each of these lines. They are listed in
file order; deleting from the bottom up keeps the earlier line numbers valid.
Delete only the declaration line itself, leaving surrounding blank lines tidy
(do not leave a doubled blank line where a member was removed between two
others).

```scala
  val clientId: ClientId                                                    // line 34
  val txStatusCache: TransactionStatusCache                                 // line 36
  def clientContext: ExecutionContext                                       // line 42
  def backgroundTaskManager: BackgroundTaskManager                          // line 294
  private[client] def opportunisticRebuildManager: OpportunisticRebuildManager  // line 301
  private[client] val messenger: Messenger                                  // line 303
  private[client] val objectCache: ObjectCache                              // line 305
  private[aspen] def getSystemAttribute(key: String): Option[String]        // line 348
```

Take care at line 348: the very next line is
`private[aspen] def setSystemAttribute(key: String, value: String): Unit`,
which **stays**.

Take care at lines 301-307: `private[aspen] val radicle: KeyValueObjectPointer`
sits at line 307 and **stays**.

- [ ] **Step 5: Prune the four imports that are now unused**

Each of these types appears in `AspenClient.scala` only on a line just deleted,
so its import is now dead. This was verified by grep before the plan was
written; if `sbt compile` in Step 6 disagrees, restore the import it names.

Delete lines 3 and 4 entirely:

```scala
import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.internal.network.Messenger
```

Change line 10 from:

```scala
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, ClientId, ClientResponse, HostMessage, ServiceMessage}
```

to:

```scala
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, ClientResponse, HostMessage, ServiceMessage}
```

Change line 16 from:

```scala
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, uuid2byte}
```

to:

```scala
import org.aspen_ddp.aspen.common.util.uuid2byte
```

`TransactionStatusCache` and `ObjectCache` need no import change — they live in
package `client` alongside `AspenClient`.

- [ ] **Step 6: Verify the whole project still compiles**

Run: `sbt compile`

Expected: SUCCESS. This is the substantive check of the task. `BaseAspenClient`
and `BaseReadDriverSuite.TClient` both implement `AspenClient` and both supply
all eight members; they now satisfy them through the inherited trait instead of
directly, which requires no source change in either file.

If this fails with "class BaseAspenClient needs to be abstract", a member was
deleted from `AspenClient` but not added to `ReadDriverClient` — compare against
the Interfaces block above.

- [ ] **Step 7: Verify the test suite still passes**

Run: `sbt test`

Expected: PASS, with the same suite and test counts as before the change.
Nothing has changed at runtime.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/ReadDriverClient.scala \
        src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala
git commit -m "Add the ReadDriverClient trait and have AspenClient extend it"
```

---

### Task 2: Point the read driver hierarchy at the trait

Widen the declared parameter types from `AspenClient` to `ReadDriverClient`
across the driver hierarchy and the factory type alias. Because
`AspenClient <: ReadDriverClient`, every existing call site keeps working
untouched — including `ReadManager`, which still holds and passes an
`AspenClient`.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadDriver.scala:5,34`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriver.scala:4,15,182`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/SimpleReadDriver.scala:5,18,37`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/SplitFinalizationAction.scala:6`

**Interfaces:**
- Consumes: `ReadDriverClient` from Task 1.
- Produces: `ReadDriver.Factory` retyped to
  `(ReadDriverClient, ObjectPointer, IDA, UUID, String, Boolean) => ReadDriver`,
  and `BaseReadDriver`'s public `val client: ReadDriverClient`. Task 3 relies on
  `BaseReadDriver`'s constructor accepting a `ReadDriverClient`.

- [ ] **Step 1: Retype the factory alias**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadDriver.scala`,
change line 5 from:

```scala
import org.aspen_ddp.aspen.client.{AspenClient, ObjectState, ReadError}
```

to:

```scala
import org.aspen_ddp.aspen.client.{ObjectState, ReadDriverClient, ReadError}
```

and change line 34 from:

```scala
  type Factory = (AspenClient, ObjectPointer, IDA, UUID, String, Boolean) => ReadDriver
```

to:

```scala
  type Factory = (ReadDriverClient, ObjectPointer, IDA, UUID, String, Boolean) => ReadDriver
```

Leave the scaladoc comment above the alias as it is — it documents the trailing
parameters, none of which change.

- [ ] **Step 2: Retype BaseReadDriver**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriver.scala`,
change line 4 from:

```scala
import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, KeyValueObjectState, MetadataObjectState, ObjectState, ReadError}
```

to:

```scala
import org.aspen_ddp.aspen.client.{DataObjectState, KeyValueObjectState, MetadataObjectState, ObjectState, ReadDriverClient, ReadError}
```

change the constructor parameter on line 15 from:

```scala
                               val client: AspenClient,
```

to:

```scala
                               val client: ReadDriverClient,
```

and change the `noErrorRecoveryReadDriver` parameter on line 182 from:

```scala
                                 client: AspenClient,
```

to:

```scala
                                 client: ReadDriverClient,
```

Nothing inside the class body changes. The seven members it reaches through
`client` — `messenger`, `clientId`, `txStatusCache`, `objectCache`,
`opportunisticRebuildManager`, `clientContext`, and `getSystemAttribute` — are
all on the new trait, and `BaseReadDriver` sits in `client.internal.read`, a
subpackage of `client`, so the `private[client]` members remain visible.

- [ ] **Step 3: Retype SimpleReadDriver**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/read/SimpleReadDriver.scala`,
change line 5 from:

```scala
import org.aspen_ddp.aspen.client.AspenClient
```

to:

```scala
import org.aspen_ddp.aspen.client.ReadDriverClient
```

change the `Factory.apply` parameter (line 18) from:

```scala
               client: AspenClient,
```

to:

```scala
               client: ReadDriverClient,
```

and change the class constructor parameter (line 37) from:

```scala
                        client: AspenClient,
```

to:

```scala
                        client: ReadDriverClient,
```

`SimpleReadDriver` reaches `client.clientContext` and
`client.backgroundTaskManager`, both on the new trait.

- [ ] **Step 4: Drop the dead import in SplitFinalizationAction**

`src/main/scala/org/aspen_ddp/aspen/client/tkvl/SplitFinalizationAction.scala`
imports `BaseReadDriver` on line 6 and never uses it — line 6 is the only
occurrence of the name in the file. Delete the line:

```scala
import org.aspen_ddp.aspen.client.internal.read.BaseReadDriver
```

This is unrelated to the retyping but is bundled here because a reviewer of this
task checks `BaseReadDriver`'s importers anyway, and finding a dead one is the
natural moment to remove it.

- [ ] **Step 5: Verify the project compiles**

Run: `sbt compile`

Expected: SUCCESS.

The interesting call site is `ReadManager.read`, at
`src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadManager.scala:110`:

```scala
val driver = driverFactory(client, objectPointer, ida, readUUID, comment, disableOpportunisticRebuild)
```

`ReadManager.client` is still an `AspenClient`, and the factory now wants a
`ReadDriverClient`. This compiles because `AspenClient` extends
`ReadDriverClient` — that subtyping is exactly what Task 1 established, and this
step is what proves it. `ReadManager` is not edited.

Likewise `BaseAspenClient.scala:72`, `new ReadManager(this, readDriverFactory)`,
and `SimpleAspenClient.scala:33`,
`new SimpleReadDriver.Factory(initialReadDelay, maxReadDelay).apply`, both
compile unchanged.

- [ ] **Step 6: Verify the test suite still passes**

Run: `sbt test`

Expected: PASS. `BaseReadDriverSuite` still builds: its `mkReader` declares an
`AspenClient` parameter and passes it to `BaseReadDriver`, which now takes the
wider `ReadDriverClient` — still a legal argument. `TestNetwork.TClient` passes
`BaseReadDriver.noErrorRecoveryReadDriver` as a `ReadDriver.Factory`; the
retyped method still conforms to the retyped alias.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadDriver.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriver.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/read/SimpleReadDriver.scala \
        src/main/scala/org/aspen_ddp/aspen/client/tkvl/SplitFinalizationAction.scala
git commit -m "Take a ReadDriverClient in the read driver hierarchy"
```

---

### Task 3: Retire the hand-rolled TClient stub

This is the payoff. Delete the 66-line `AspenClient` stub and its ~15 supporting
imports, and switch the six existing tests to `ReadDriverClient.NoOp`. The tests
themselves — their inputs, their assertions — do not change at all. That is the
point: if they still pass, the no-op is behaviourally identical to the stub it
replaces.

**Files:**
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala` (imports at lines 1-24; `TClient` at lines 51-117; `mkReader` at line 125; the six `new TClient(client)` call sites at lines 136, 158, 179, 207, 227, 250)

**Interfaces:**
- Consumes: `ReadDriverClient` and `ReadDriverClient.NoOp` from Task 1;
  `BaseReadDriver`'s `ReadDriverClient` constructor parameter from Task 2.
- Produces: nothing — no later task depends on this one.

- [ ] **Step 1: Delete the TClient stub**

In `src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala`,
delete lines 51-117 in their entirety — the whole `class TClient(...) extends
AspenClient { ... }` block, from `class TClient` through its closing brace.

The `object BaseReadDriverSuite` body that remains ends with
`val client = ClientId(cliUUID)` followed by the object's closing brace. Every
other `val` in that object (`awaitDuration`, `objId`, `poolId`, `readUUID`,
`cliUUID`, `ida`, `ds0`, `ds1`, `ds2`, `ptr`, `kvptr`, `rev`, `ref`, `odata`,
`noLocks`, `client`) **stays** — the tests use them.

- [ ] **Step 2: Replace the import block**

Replace lines 1-24 (everything from `package` through
`import scala.language.implicitConversions`) with exactly this:

```scala
package org.aspen_ddp.aspen.client.internal.read

import java.util.UUID
import org.aspen_ddp.aspen.client.{CorruptedObject, DataObjectState, InvalidObject, ReadDriverClient}
import org.aspen_ddp.aspen.common.network.{ClientId, ReadResponse}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionId}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*
import scala.language.implicitConversions
```

Everything dropped was reachable only from `TClient`: `AspenClient`,
`KeyValueObjectState`, `ObjectAllocator`, `ObjectAllocatorId`, `ObjectCache`,
`RetryStrategy`, `StoragePool`, `Transaction`, `TransactionStatusCache`,
`TypeRegistry`, `OpportunisticRebuildManager`, `Messenger`, `AllocationGroupId`,
`ClientResponse`, `HostMessage`, `BackendConfig`, `BackgroundTaskManager`, the
whole `common.metadata` group, `common.ida.IDA`, and `scala.concurrent.Future`.

- [ ] **Step 3: Widen mkReader's parameter**

Change line 125 (the first line of `mkReader`) from:

```scala
  def mkReader(client: AspenClient,
```

to:

```scala
  def mkReader(client: ReadDriverClient,
```

The method body is unchanged, including
`given ec: ExecutionContext = this.client.clientContext` — `clientContext` is on
the new trait.

- [ ] **Step 4: Switch the six construction sites**

Six tests each open with the same line. Replace every occurrence of:

```scala
    val m = new TClient(client)
```

with:

```scala
    val m = ReadDriverClient.NoOp(client)
```

There are exactly six, one each in "Fail with invalid object", "Fail with
corrupted object", "Succeed with errors", "Ignore old revisions", "Use minimum
readTime", and "Successful read with data and locks". No `new` keyword is needed
— Scala 3's universal apply methods construct `NoOp` directly. `client` here is
the `ClientId(cliUUID)` from the companion object, which is what `NoOp` takes.

Change nothing else in any test body.

- [ ] **Step 5: Run the suite**

Run: `sbt 'testOnly *BaseReadDriverSuite'`

Expected: PASS, 6 tests, same names and same results as before the change.

If a test now fails rather than failing to compile, the no-op differs
behaviourally from the old stub — compare `ReadDriverClient.NoOp` member by
member against the deleted `TClient`. The two should agree exactly: both use
`TransactionStatusCache.NoCache`, `ExecutionContext.Implicits.global`,
`BackgroundTaskManager.NoBackgroundTaskManager`,
`OpportunisticRebuildManager.None`, `Messenger.None`, `ObjectCache.NoCache`, and
`None` from `getSystemAttribute`.

- [ ] **Step 6: Run the neighbouring read suites**

Run: `sbt 'testOnly *DataObjectReaderSuite' 'testOnly *KeyValueObjectReaderSuite'`

Expected: PASS. These share the `client.internal.read` package with the file
just edited and would catch a stray import collision introduced in Step 2.

- [ ] **Step 7: Run the full test suite**

Run: `sbt test`

Expected: PASS, with the same totals as the run at the end of Task 2. This is
the last check that the `TestNetwork`-based suites, which drive the real
`BaseAspenClient` through the retyped factory, are unaffected.

- [ ] **Step 8: Commit**

```bash
git add src/test/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriverSuite.scala
git commit -m "Replace BaseReadDriverSuite's AspenClient stub with ReadDriverClient.NoOp"
```

---

### Task 4: Retire the TODO entry

**Files:**
- Modify: `TODO.txt:1-6`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

- [ ] **Step 1: Check what is already uncommitted in TODO.txt**

Run: `git diff TODO.txt`

Expected: a single hunk rewording line 5 of the "Decouple AspenClient and
BaseReadDriver" entry to "Split into a test implementation & 'real' one backed
by AspenClient". This edit predates the branch and is the user's.

That uncommitted edit is *inside the block about to be deleted*, so deleting the
block subsumes it. Do not stash, revert, or otherwise disturb the working tree
beyond the deletion below.

- [ ] **Step 2: Delete the entry**

Remove these five lines from the top of `TODO.txt`, along with the blank line
that follows them, so that "Add required IP/dns-name argument to bootstrap CLI
command" becomes the first entry:

```
Decouple AspenClient and BaseReadDriver
  - Having to stub out every method added to AspenClient is annoying
  - Use pluggable trait that has just the methods BaseReadDriver needs
  - Split into a test implementation & "real" one backed by AspenClient
```

Keep the leading blank line on line 1 — the file starts with one and every other
entry in it is separated the same way.

- [ ] **Step 3: Verify only the intended lines moved**

Run: `git diff TODO.txt`

Expected: a pure deletion of the four entry lines plus one blank separator. No
other hunk. If another hunk appears, the editor reflowed something it should not
have — undo it.

- [ ] **Step 4: Commit**

```bash
git add TODO.txt
git commit -m "Retire the AspenClient/BaseReadDriver decoupling entry"
```

---

## Done

At this point the branch `read-driver-client-trait` holds five commits: the
design doc, then one per task. `git diff main --stat` should show one new file
(`ReadDriverClient.scala`, ~55 lines), one new doc, and net **negative** lines
across the Scala sources — `BaseReadDriverSuite.scala` alone loses about 80.

Use `superpowers:finishing-a-development-branch` to decide how to integrate.
