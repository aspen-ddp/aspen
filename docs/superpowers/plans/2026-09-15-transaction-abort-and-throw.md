# Transaction `abortAndThrow` / `abort` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `Transaction.invalidateTransaction` with a throwing `abortAndThrow` plus a non-throwing `abort`, and split the overloaded `valid` flag into `isEmpty`, so that a transaction poisoned by a failed precondition stops its builder immediately and can never be mistaken for an empty one.

**Architecture:** `TransactionImpl` already fails its shared promise on invalidation and `commit()` short-circuits on a completed promise, so invalidation is already permanent. The change is to the *caller-facing* contract: producers that detect a violation throw (unwinding the builder before it allocates), sinks that record an already-raised failure do not, and the one consumer of `valid` stops treating "aborted" as "empty". Migration is additive first (new members alongside the old), then call-site-by-call-site, then removal.

**Tech Stack:** Scala 3, sbt, ScalaTest `AsyncFunSuite` via the project's `IntegrationTestSuite` base class and its `atest` helper.

**Spec:** `docs/superpowers/specs/2026-09-15-transaction-abort-and-throw-design.md`

## Global Constraints

- Scala 3 "quiet mode" syntax. Braces only where they clearly aid readability. Indented `if/then/else`; avoid single-line `if a == 1 then value` unless it reads better.
- Match the surrounding file's existing style. `KeyValueListNode.scala`, `SimpleDirectoryRootManager.scala`, and `KVObjectRootManager.scala` are brace-heavy; `SimpleFileRootManager.scala`, `Registry.scala`, and `AspenClient.scala` are quiet-mode. Follow the file you are editing, not the repo average.
- `abortAndThrow` returns `Nothing`, not `Unit`.
- Only the first abort reason is propagated; later aborts are silently ignored.
- Run a specific test with: `sbt 'testOnly *SuiteName -- -z "test substring"'`
- The suite is intermittently flaky on timing-sensitive cases. A failure in a suite you did not touch should be re-run once before being treated as a regression.
- Do not change observable error types. `KeyAlreadyExists` must still surface unwrapped from the registry path.

---

### Task 1: Add `abortAndThrow`, `abort`, and `isEmpty`

Purely additive. `invalidateTransaction` and `valid` stay, delegating to the new members, so the whole codebase still compiles and every existing test still passes.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala:61-67`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala:38`, `:128-132`
- Create: `src/test/scala/org/aspen_ddp/aspen/client/TransactionFailureSuite.scala`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `Transaction.abortAndThrow(reason: Throwable): Nothing`, `Transaction.abort(reason: Throwable): Unit`, `Transaction.isEmpty: Boolean`. Tasks 2-6 rely on all three.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/org/aspen_ddp/aspen/client/TransactionFailureSuite.scala`:

```scala
package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite

import scala.concurrent.ExecutionContext

class TransactionFailureSuite extends IntegrationTestSuite:

  atest("abortAndThrow throws the reason it was given"):
    given ExecutionContext = executionContext
    val tx = client.newTransaction()
    val reason = new Exception("boom")

    val thrown = intercept[Exception](tx.abortAndThrow(reason))

    tx.result.failed.map: stored =>
      thrown should be theSameInstanceAs reason
      stored should be theSameInstanceAs reason

  atest("abort records the failure without throwing"):
    given ExecutionContext = executionContext
    val tx = client.newTransaction()
    val reason = new Exception("recorded")

    tx.abort(reason)

    tx.result.failed.map(_ should be theSameInstanceAs reason)

  atest("only the first reason is propagated"):
    given ExecutionContext = executionContext
    val tx = client.newTransaction()
    val first = new Exception("first")

    tx.abort(first)
    tx.abort(new Exception("second"))

    tx.result.failed.map(_ should be theSameInstanceAs first)

  atest("an aborted transaction carrying updates cannot commit"):
    given ExecutionContext = executionContext
    for
      before <- client.read(radicle)
      tx = client.newTransaction()
      _ = tx.setRefcount(radicle, before.refcount, before.refcount.increment())
      _ = tx.abort(new Exception("boom"))
      err <- tx.commit().failed
      after <- client.read(radicle)
    yield
      err.getMessage should be("boom")
      // The guard that matters: the update was staged before the abort and must not land.
      after.refcount should be(before.refcount)

  atest("isEmpty reports staged updates, not abort state"):
    given ExecutionContext = executionContext
    for
      before <- client.read(radicle)
      untouched = client.newTransaction()
      aborted = client.newTransaction()
      _ = aborted.setRefcount(radicle, before.refcount, before.refcount.increment())
      _ = aborted.abort(new Exception("boom"))
      _ <- aborted.commit().failed
    yield
      untouched.isEmpty should be(true)
      // The conflation this replaces: `valid` was false here, which read as "empty".
      aborted.isEmpty should be(false)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *TransactionFailureSuite'`

Expected: compile failure — `value abortAndThrow is not a member of org.aspen_ddp.aspen.client.Transaction` (and likewise `abort`, `isEmpty`).

- [ ] **Step 3: Add the three members to the `Transaction` trait**

In `src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala`, replace lines 61-67:

```scala
  /** Only the first error will be propagated should multiple attempts are made to invalidate the transaction
   *
   */
  def invalidateTransaction(reason: Throwable): Unit

  /** True if one or more updates have been added to the transaction and it has not been invalidated */
  def valid: Boolean
```

with:

```scala
  /** Permanently marks the transaction as unable to commit and throws `reason`.
   *
   *  Use this when a precondition check fails while building a transaction. Throwing unwinds
   *  the builder so no further work -- allocations in particular -- is performed against a
   *  transaction that can never commit. Only the first reason is propagated.
   */
  def abortAndThrow(reason: Throwable): Nothing

  /** Permanently marks the transaction as unable to commit, without throwing.
   *
   *  For recording a failure that has already been raised elsewhere, such as from a callback
   *  on an already-failed Future. Prefer abortAndThrow when this is the site that detects the
   *  failure. Only the first reason is propagated.
   */
  def abort(reason: Throwable): Unit

  /** True if no updates have been added to the transaction. Says nothing about whether the
   *  transaction has been aborted -- inspect `result` for that.
   */
  def isEmpty: Boolean

  /** Only the first error will be propagated should multiple attempts are made to invalidate the transaction
   *
   */
  def invalidateTransaction(reason: Throwable): Unit

  /** True if one or more updates have been added to the transaction and it has not been invalidated */
  def valid: Boolean
```

- [ ] **Step 4: Implement them in `TransactionImpl`**

In `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala`, replace line 38:

```scala
  def valid: Boolean = synchronized { !invalidated && havePendingUpdates }
```

with:

```scala
  def valid: Boolean = synchronized { !invalidated && havePendingUpdates }

  def isEmpty: Boolean = synchronized { !havePendingUpdates }
```

and replace lines 128-132:

```scala
  def invalidateTransaction(reason: Throwable): Unit = synchronized {
    invalidated = true
    if (!promise.isCompleted)
      promise.failure(reason)
  }
```

with:

```scala
  def abort(reason: Throwable): Unit = synchronized {
    invalidated = true
    if (!promise.isCompleted)
      promise.failure(reason)
  }

  def abortAndThrow(reason: Throwable): Nothing =
    abort(reason)
    throw reason

  def invalidateTransaction(reason: Throwable): Unit = abort(reason)
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *TransactionFailureSuite'`

Expected: PASS, 5 tests.

- [ ] **Step 6: Verify nothing else broke**

Run: `sbt Test/compile`

Expected: success. No call site changed yet.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala \
        src/test/scala/org/aspen_ddp/aspen/client/TransactionFailureSuite.scala \
        docs/superpowers/specs/2026-09-15-transaction-abort-and-throw-design.md \
        docs/superpowers/plans/2026-09-15-transaction-abort-and-throw.md
git commit -m "feat: add Transaction.abortAndThrow, abort, and isEmpty

Additive. invalidateTransaction and valid remain, delegating to abort,
so all existing call sites are unaffected."
```

---

### Task 2: Stop `SimpleBaseFile` reading an aborted transaction as empty

This is the task that closes the silent-failure path. `commit()` already succeeds
immediately on an empty transaction and returns the stored failure on an aborted one, so
calling it unconditionally is correct for both cases — the only thing the emptiness check
still decides is *which revision to report*.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleBaseFile.scala:219-224`

**Interfaces:**
- Consumes: `Transaction.isEmpty` from Task 1.
- Produces: nothing new.

- [ ] **Step 1: Confirm the existing guard test passes before the change**

Run: `sbt 'testOnly *TransactionFailureSuite -- -z "isEmpty reports staged updates"'`

Expected: PASS. This is the contract Task 2 depends on. The `SimpleFileContent`
duplicate-segment race that triggers the bug in production needs two concurrent writes
allocating the same segment offset and is not deterministically reproducible; this
contract test plus Task 3's deterministic tkvl test are the regression guards.

- [ ] **Step 2: Replace the conflated branch**

In `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleBaseFile.scala`, replace lines 219-224:

```scala
      def commit(): Future[ObjectRevision] = {
        if (tx.valid)
          tx.commit().map(_ => tx.revision)
        else
          Future.unit.map(_ => arevision) // op added nothing to the transaction
      }
```

with:

```scala
      def commit(): Future[ObjectRevision] = {
        // An empty transaction commits immediately and successfully, leaving the inode
        // revision untouched. Always going through commit() is what keeps an aborted
        // transaction from being mistaken for an empty one -- it returns the stored failure.
        val addedNothing = tx.isEmpty
        tx.commit().map(_ => if addedNothing then arevision else tx.revision)
      }
```

- [ ] **Step 3: Run the file suites**

Run: `sbt 'testOnly *SimpleFileContentSuite *SimpleFileHandleSuite *SimpleFileSystemTestSuite'`

Expected: PASS. The `Flush()` op (`SimpleBaseFile.scala:27-36`) adds nothing and is the
legitimate empty case — it must still report `arevision`.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleBaseFile.scala
git commit -m "fix: stop SimpleBaseFile reporting an aborted transaction as success

tx.valid was false both when an op staged nothing and when the op's
transaction had been aborted. The second case took the no-op branch and
reported the write successful with the pre-operation revision, while the
Write op's writePromise -- completed from tx.result.foreach -- never ran.

Reachable via SimpleFileContent's requireDoesNotExist segment insert."
```

---

### Task 3: Migrate `KeyValueListNode` to `abortAndThrow`

Three sites. Line 334 is the one that strands allocations: it aborts, then falls through
to a split path that calls `allocator.allocateKeyValueObject` at line 386.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala:334`, `:588`, `:622`
- Modify: `src/test/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListSuite.scala`

**Interfaces:**
- Consumes: `Transaction.abortAndThrow` from Task 1.
- Produces: `KeyValueListNode.insert(..., requirement = Some(Left(true)))` now fails its returned `Future` with `KeyAlreadyExists` instead of completing successfully.

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListSuite.scala`, inside the class:

```scala
  atest("insert with requireDoesNotExist fails when the key is already present") {
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](2))

    given tx: Transaction = client.newTransaction()

    for {
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator
      lptr <- alloc.allocateKeyValueObject()

      lst = new KeyValueListNode(client, pool.ida, lptr, ByteArrayKeyOrdering, Key.AbsoluteMinimum,
        tx.revision, ObjectRefcount(0, 1),
        Map(key -> ValueState(value, tx.revision, HLCTimestamp.now)), None)

      err <- lst.insert(key, value, 100, alloc, requirement = Some(Left(true))).failed
      txErr <- tx.result.failed
    } yield {
      // The insert must unwind rather than fall through to the split path, which would
      // allocate a node for a transaction that can never commit.
      err shouldBe a[KeyAlreadyExists]
      txErr shouldBe a[KeyAlreadyExists]
    }
  }
```

Add these imports to the top of the file:

```scala
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.objects.KeyAlreadyExists
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *KeyValueListSuite -- -z "requireDoesNotExist"'`

Expected: FAIL. `insert` currently completes successfully, so `.failed` yields a
`NoSuchElementException: Future.failed not completed with a throwable`.

- [ ] **Step 3: Change line 334**

In `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala`, replace:

```scala
        case Left(req) => if (req) {
          if (node.contents.contains(key))
            tx.invalidateTransaction(new KeyAlreadyExists(key))
          DoesNotExist(key) :: Nil
        } else
          Nil
```

with:

```scala
        case Left(req) => if (req) {
          if (node.contents.contains(key))
            tx.abortAndThrow(new KeyAlreadyExists(key))
          DoesNotExist(key) :: Nil
        } else
          Nil
```

- [ ] **Step 4: Change lines 588 and 622**

Both read:

```scala
          tx.invalidateTransaction(new Exception("Node is not empty"))
```

Replace each with:

```scala
          tx.abortAndThrow(new Exception("Node is not empty"))
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt 'testOnly *KeyValueListSuite'`

Expected: PASS, including the new case.

- [ ] **Step 6: Run the rest of the tkvl suites**

Run: `sbt 'testOnly *TKVLSuite *KeyValueListNodeSplitAtSuite *TieredKeyValueListSplitTreeSuite'`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala \
        src/test/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListSuite.scala
git commit -m "fix: unwind KeyValueListNode inserts that violate their requirement

insert aborted the transaction and kept going. On a full node it then
took the split path and allocated a KeyValue object for a transaction
that could never commit. Matches the NodeSizeExceeded throw already in
the same function."
```

---

### Task 4: Migrate the root managers to `abortAndThrow`

Both abort inside the `yield` of a `for` comprehension over Futures, so the throw becomes
a failed Future — the same shape `AspenClient.transact` and the retry strategies already
handle.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileRootManager.scala:116`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleDirectoryRootManager.scala:127`

**Interfaces:**
- Consumes: `Transaction.abortAndThrow` from Task 1.
- Produces: `RootManager.createInitialNode` now fails its returned `Future` when the node already exists.

- [ ] **Step 1: Change `SimpleFileRootManager.scala:116`**

Replace:

```scala
      onode.foreach: _ =>
        // Check for a race condition where multiple concurrent attempts to
        // create the initial node might clash with each other
        tx.invalidateTransaction(new Exception("Initial TKVL node already exists."))
```

with:

```scala
      onode.foreach: _ =>
        // Check for a race condition where multiple concurrent attempts to
        // create the initial node might clash with each other
        tx.abortAndThrow(new Exception("Initial TKVL node already exists."))
```

- [ ] **Step 2: Change `SimpleDirectoryRootManager.scala:127`**

Replace:

```scala
      onode.foreach: _ =>
        // Check for a race condition where multiple concurrent attempts to
        // create the initial node might clash with each other
        tx.invalidateTransaction(new Exception("Initial TKVL node already exists."))
```

with:

```scala
      onode.foreach: _ =>
        // Check for a race condition where multiple concurrent attempts to
        // create the initial node might clash with each other
        tx.abortAndThrow(new Exception("Initial TKVL node already exists."))
```

- [ ] **Step 3: Run the affected suites**

Run: `sbt 'testOnly *DirectorySuite *SimpleFileContentSuite *SimpleRootManagerDecodeSuite'`

Expected: PASS. `SimpleFileContent.initializeContentTree` drives the file case through
`transactUntilSuccessful`, which retries on a failed attempt with a fresh transaction.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileRootManager.scala \
        src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleDirectoryRootManager.scala
git commit -m "fix: unwind createInitialNode when the root node already exists"
```

---

### Task 5: Delete the hand-rolled throw in `Registry.prepareRegister`

`abortAndThrow` in `KeyValueListNode` (Task 3) now raises `KeyAlreadyExists` directly out
of `tkvl.set`, so the workaround is dead code. It was also subtly wrong: it inspected
whole-transaction failure state, so an abort raised by a different contributor for a
different key made it throw naming the wrong key.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/registries/Registry.scala:40-44`

**Interfaces:**
- Consumes: `KeyValueListNode.insert` failure behaviour from Task 3.
- Produces: no signature change. `prepareRegister` still fails with `KeyAlreadyExists`.

- [ ] **Step 1: Confirm the current behaviour is covered**

Run: `sbt 'testOnly *FileSystemRegistrationSuite -- -z "duplicate name"'`

Expected: PASS before the change. This test asserts `err shouldBe a[KeyAlreadyExists]`
and that the registry still resolves to the first filesystem — it must pass identically
after.

- [ ] **Step 2: Simplify `prepareRegister`**

Replace lines 40-44:

```scala
  def prepareRegister(key: Key, value: Value)(using tx: Transaction): Future[Unit] =
    tkvl.set(key, value, requirement = Some(Left(true))).map: _ =>
      tx.result.value match
        case Some(Failure(_: KeyAlreadyExists)) => throw KeyAlreadyExists(key)
        case _ => ()
```

with:

```scala
  /** Fails with KeyAlreadyExists if the key is taken. The requirement's abortAndThrow
   *  raises it directly, before anything else is staged on the transaction.
   */
  def prepareRegister(key: Key, value: Value)(using tx: Transaction): Future[Unit] =
    tkvl.set(key, value, requirement = Some(Left(true)))
```

- [ ] **Step 3: Remove imports left unused**

Check whether `scala.util.Failure` and `KeyAlreadyExists` are still referenced anywhere
else in the file:

Run: `grep -n "Failure\|KeyAlreadyExists" src/main/scala/org/aspen_ddp/aspen/client/registries/Registry.scala`

Remove whichever of the two imports no longer has a reference. Leave any that do.

- [ ] **Step 4: Run the registry-dependent suites**

Run: `sbt 'testOnly *FileSystemRegistrationSuite *HostManagerSuite *AllocationGroupStateSuite *StorageDeviceSetIntegrationSuite *HostCreationSuite'`

Expected: PASS. Every one of these asserts `err shouldBe a[KeyAlreadyExists]` on a
duplicate name and must be unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/registries/Registry.scala
git commit -m "refactor: drop prepareRegister's hand-rolled KeyAlreadyExists re-throw

abortAndThrow now raises it out of the tkvl insert. The workaround read
whole-transaction failure state rather than its own insert's, so an abort
from another contributor could make it throw naming the wrong key."
```

---

### Task 6: Migrate the sinks to `abort` and remove the old API

The two remaining call sites record a failure already raised elsewhere, from inside a
callback on an already-failed Future. Throwing there would only hand the exception to the
ExecutionContext's reporter, so they take `abort`.

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:265`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleBaseFile.scala:233`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala`

**Interfaces:**
- Consumes: `Transaction.abort` from Task 1.
- Produces: `invalidateTransaction` and `valid` no longer exist on `Transaction`.

- [ ] **Step 1: Change the two sinks**

In `src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala:265` and
`src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleBaseFile.scala:233`, both read:

```scala
    fresult.failed.foreach(err => tx.invalidateTransaction(err))
```

Replace each with:

```scala
    fresult.failed.foreach(err => tx.abort(err))
```

(`SimpleBaseFile.scala:233` is indented six spaces, not four — preserve its indentation.)

- [ ] **Step 2: Verify no callers remain**

Run: `grep -rn "invalidateTransaction\|\.valid\b" --include=*.scala src/`

Expected: only the declarations in `Transaction.scala` and `TransactionImpl.scala`. If
anything else appears, migrate it — a producer takes `abortAndThrow`, a sink takes `abort`
— before continuing.

- [ ] **Step 3: Remove the old members from the trait**

In `src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala`, delete:

```scala
  /** Only the first error will be propagated should multiple attempts are made to invalidate the transaction
   *
   */
  def invalidateTransaction(reason: Throwable): Unit

  /** True if one or more updates have been added to the transaction and it has not been invalidated */
  def valid: Boolean
```

- [ ] **Step 4: Remove the old members and the dead field from `TransactionImpl`**

Delete the `valid` definition:

```scala
  def valid: Boolean = synchronized { !invalidated && havePendingUpdates }
```

Delete the delegating shim:

```scala
  def invalidateTransaction(reason: Throwable): Unit = abort(reason)
```

`invalidated` is now written by `abort` and read by nobody. Delete its declaration at
line 33:

```scala
  private  var invalidated = false
```

and the assignment in `abort`, leaving:

```scala
  def abort(reason: Throwable): Unit = synchronized {
    if (!promise.isCompleted)
      promise.failure(reason)
  }
```

- [ ] **Step 5: Compile**

Run: `sbt Test/compile`

Expected: success. A failure here means a call site was missed in Step 2.

- [ ] **Step 6: Run the full suite**

Run: `sbt test`

Expected: PASS. Re-run once before treating a failure in an untouched timing-sensitive
suite as a regression.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/AspenClient.scala \
        src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleBaseFile.scala \
        src/main/scala/org/aspen_ddp/aspen/client/Transaction.scala \
        src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala
git commit -m "refactor: remove invalidateTransaction and valid

Sinks that record an already-raised failure now use abort. The invalidated
field had no remaining reader once valid was gone -- commit() already
short-circuits on the completed promise."
```

---

## Notes for the executor

- **`SimpleFileSystem.create` changes timing, not outcome.** It makes a single `transact`
  attempt. After Task 3, a taken name short-circuits before its four allocations rather
  than after. The comment at `SimpleFileSystem.scala:54-56` already describes the new
  behaviour; leave it as is.
- **Why throwing is safe.** `AspenClient.transact` wraps `prepare` in try/catch
  (`AspenClient.scala:257-258`); both `ExponentialBackoffRetryStrategy` loops wrap
  `attempt` in try/catch (lines 48-55, 103-110); retries build a fresh transaction each
  attempt; and every `recover` under `client/` and `amoebafs/` is narrowly typed and ends
  in `case other => throw other`. If you add a broad `recover` while working through this
  plan, you reintroduce the swallowing this change removes.
- **Task order matters.** Task 5 depends on Task 3 having landed — deleting the registry
  workaround before `abortAndThrow` reaches `KeyValueListNode` would make duplicate
  registration silently succeed at the API level.
