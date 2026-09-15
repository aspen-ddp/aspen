# Transaction failure semantics: `abortAndThrow` / `abort`

## Problem

`Transaction.invalidateTransaction(reason: Throwable)` marks a transaction as unable
to commit, but returns normally. The builder that called it keeps running.

Contrary to first appearances, an invalidated transaction cannot wrongly commit.
`invalidateTransaction` fails the shared promise (`TransactionImpl.scala:128-132`) and
`commit()` is guarded by `if (!promise.isCompleted)` (`TransactionImpl.scala:145`), so
commit short-circuits and hands back the already-failed future. Invalidation is
permanent and is honoured.

The damage is elsewhere: because the call returns normally, **the failure is easy to
drop**, and the codebase already drops it in three distinct ways.

### 1. `valid` conflates "empty" with "aborted" — a reachable silent failure

`TransactionImpl.scala:38` defines `valid` as `!invalidated && havePendingUpdates`.
Its only consumer, `SimpleBaseFile.scala:220`, reads a `false` as "the op added nothing":

```scala
if (tx.valid) tx.commit().map(_ => tx.revision)
else Future.unit.map(_ => arevision) // op added nothing to the transaction
```

An aborted transaction takes the `else` branch and the operation is reported
**successful** with the pre-operation revision.

This is reachable. `SimpleFileContent.scala:219-222` calls
`tree.set(key, value, Some(Left(true)))` — requireDoesNotExist — on the file
operation's own transaction. On a duplicate segment key, `KeyValueListNode.scala:334`
aborts that transaction. Control returns to `SimpleBaseFile`, `valid` is `false`, and
the write reports success. The `Write` op's `writePromise` is completed from
`tx.result.foreach` (`SimpleFile.scala:83`), which never runs on a failed future, so
that caller waits forever. `SimpleBaseFile.scala:240` already carries a
`"Slow/Hung write. Retry count:"` diagnostic; whether it fires for this reason is not
established, but the mechanism produces exactly that symptom.

### 2. Work continues after abort, stranding allocations

`KeyValueListNode.insert` aborts at line 334 and continues. When the node is full it
takes the split path and calls `allocator.allocateKeyValueObject` at line 386 for a
transaction that can never commit. `SimpleFileContent.scala:218` has already allocated
a data object before the failing `set`. Both objects are allocated and never referenced
by any committed transaction.

### 3. The throw semantics are already wanted, and hand-rolled

`Registry.prepareRegister` (`Registry.scala:40-44`) reconstructs them at the call site:

```scala
tkvl.set(key, value, requirement = Some(Left(true))).map: _ =>
  tx.result.value match
    case Some(Failure(_: KeyAlreadyExists)) => throw KeyAlreadyExists(key)
    case _ => ()
```

This inspects whole-transaction failure state rather than "did my insert fail". Another
contributor aborting with a `KeyAlreadyExists` for a different key makes this throw
naming the wrong key. `SimpleFileSystem.scala:54-56` documents that a taken name "costs
nothing beyond the containing node's read" — true only because of this manual re-throw.

## Why throwing is safe here

- `AspenClient.transact` wraps `prepare` in try/catch and converts to a failed future
  (`AspenClient.scala:257-258`), then aborts the transaction.
- Both `ExponentialBackoffRetryStrategy` loops wrap `attempt` in try/catch
  (lines 48-55, 103-110).
- Retries construct a **fresh** transaction per attempt, so an aborted one is never reused.
- All 18 `recover`/`recoverWith` sites under `client/` and `amoebafs/` are narrowly typed
  and end in `case other => throw other`.
- `KeyValueListNode` **already throws** `NodeSizeExceeded` at lines 327 and 428 — in the
  same function as the abort call. Two "this cannot work" conditions using two different
  mechanisms is the inconsistency being removed.

## Design

Three members on `Transaction`, replacing `invalidateTransaction` and `valid`.

```scala
/** Permanently marks the transaction as unable to commit and throws `reason`.
 *
 *  Use when a precondition check fails while building a transaction. Throwing unwinds
 *  the builder so no further work -- allocations in particular -- is performed against
 *  a transaction that can never commit. Only the first reason is propagated.
 */
def abortAndThrow(reason: Throwable): Nothing

/** Permanently marks the transaction as unable to commit, without throwing.
 *
 *  For recording a failure already raised elsewhere, such as from a callback on an
 *  already-failed Future. Prefer `abortAndThrow` when detecting the failure.
 *  Only the first reason is propagated.
 */
def abort(reason: Throwable): Unit

/** True if no updates have been added. Says nothing about whether the transaction has
 *  been aborted -- inspect `result` for that.
 */
def isEmpty: Boolean
```

`abortAndThrow` returns `Nothing` so it type-checks in expression position and the
compiler knows control does not continue past it.

**Both operations are needed.** Two existing call sites are sinks, not producers:
`AspenClient.scala:265` and `SimpleBaseFile.scala:233` are both
`fresult.failed.foreach(err => tx.invalidateTransaction(err))`. They record an
already-known failure from inside a callback on a failed future; throwing there would
merely hand the exception to the ExecutionContext's reporter. These become `abort`.
Producers that detect a violation become `abortAndThrow`.

**`valid` must be split or the fix is incomplete.** Replacing it with `isEmpty` and
routing `SimpleBaseFile` through `commit()` in both cases is what actually closes the
silent-failure path. `commit()` already succeeds immediately on an empty transaction
(`TransactionImpl.scala:148-152`) and returns the stored failure on an aborted one, so a
single unconditional `commit()` call is correct for both.

## Call site migration

| Site | Becomes |
|---|---|
| `KeyValueListNode.scala:334` | `abortAndThrow` |
| `KeyValueListNode.scala:588` | `abortAndThrow` |
| `KeyValueListNode.scala:622` | `abortAndThrow` |
| `SimpleFileRootManager.scala:116` | `abortAndThrow` |
| `SimpleDirectoryRootManager.scala:127` | `abortAndThrow` |
| `AspenClient.scala:265` | `abort` |
| `SimpleBaseFile.scala:233` | `abort` |
| `Registry.scala:40-44` | workaround deleted; plain `tkvl.set(...)` |
| `SimpleBaseFile.scala:220` | `valid` → `isEmpty`, always call `commit()` |

`invalidateTransaction`, `valid`, and the now-unread `invalidated` field are removed.

## Behaviour change to accept

`SimpleFileSystem.create` makes a single `transact` attempt rather than a retry loop.
With `abortAndThrow`, a taken name short-circuits **before** its four allocations instead
of after. This is strictly better and is what its existing comment already claims
happens. The observable failure is unchanged: `KeyAlreadyExists` still surfaces
unwrapped, so `FileSystemRegistrationSuite:53`, `HostManagerSuite:184`,
`AllocationGroupStateSuite:405`, and the `StorageDeviceSetIntegrationSuite` cases
continue to hold.

## Out of scope

Reproducing the `SimpleFileContent` duplicate-segment race deterministically. The race
requires two concurrent writes allocating the same segment offset. The regression guard
is the transaction-level contract test (an aborted non-empty transaction must fail its
`commit()`), plus the deterministic tkvl test that an insert with requireDoesNotExist
against a present key throws and allocates nothing.
