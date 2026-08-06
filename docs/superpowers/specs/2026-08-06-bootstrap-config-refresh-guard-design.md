# MetadataManager.refreshBootstrapConfig: releasing the guard, and applying the config it fetched

## Problem

`MetadataManager.refreshBootstrapConfig` sets `refreshingBootstrapConfig` before it calls
`client.getBootstrapConfig()`:

```scala
private def refreshBootstrapConfig(): Unit =
  if !refreshingBootstrapConfig then
    logger.info("Refreshing bootstrap config file")
    oClient match
      case None => logger.error(s"Refreshing bootstrap config before AspenClient initialized!")
      case Some(client) =>
        refreshingBootstrapConfig = true
        given ExecutionContext = client.clientContext

        client.getBootstrapConfig().foreach: cfg =>
          try
            atomicWrite(bootstrapConfigFile.toNIO, cfg)
            logger.info(s"Updated bootstrap config written to $bootstrapConfigFile")
          catch
            case err => logger.error(s"Failed to update bootstrap config file $bootstrapConfigFile. Error: $err")
          finally
            synchronized:
              BootstrapConfig.loadBootstrapConfig(bootstrapConfigFile.toIO).hosts.foreach: bsHost =>
                bsHost.stores.foreach: storeId =>
                  stores += storeId -> bsHost.hostId

              refreshingBootstrapConfig = false
```

The only release of that flag lives inside the `.foreach` callback, so the release is hostage both
to the callback running and to everything ahead of it in the `finally` returning normally. Four
paths leave the flag `true` for the life of the process:

1. **The fetch returns a failed `Future`.** `Future.foreach` runs on `Success` only. A failed
   metadata read, or a `BootstrapConfigKey` missing from the radicle, yields a failed `Future`
   with no throw, and the callback simply never runs. This is the routinely reachable one.
2. **`client.getBootstrapConfig()` throws synchronously.** The throw happens before the `Future`
   exists, so no callback is ever registered.
3. **`client.clientContext` throws.** The `given` binding is evaluated after the flag is set and
   before the fetch, with the same consequence.
4. **`BootstrapConfig.loadBootstrapConfig` throws inside the `finally`.** The reset is the line
   *after* it, so a truncated write, a file removed between write and read, or a `FormatError`
   skips the reset. This one is self-inflicted: the config just written is re-read from disk
   rather than parsed from the string already in hand.

Once wedged, the guard at the top short-circuits forever. `dropStoreMapping` on a bootstrap store
does nothing at all -- it takes the `isBootstrapStore` branch, so it does not even fall through to
the `stores -= storeId` the non-bootstrap path would do -- and every later `dropCacheForStore`
from `CliMessenger` or `SrvMessenger` silently declines to correct the stores map. Messages to a
bootstrap store that has moved keep going to the host it left. This is the same wedge class the
2026-07-30 exception-safety work on this file exists to eliminate; that spec listed
`refreshBootstrapConfig` as out of scope on the stated basis that it already had try/catch/finally
covering its write and reload, which paths 1 through 4 falsify.

Two smaller defects sit on the same lines:

- **The reload re-reads the file rather than using the fetched string.** On the write-failure path
  the `catch` logs and the `finally` then loads the *old* file and re-applies the *old* mappings.
  The branch runs and accomplishes nothing.
- **`catch case err =>` catches `Throwable`**, not `NonFatal`, inconsistent with `startHostLookup`
  and `startPoolLookup` two screens below in the same file.

And one gap that is not an exception-safety problem at all: the refresh updates `stores` but never
`bootstrapStores`. The constructor populates both from the config file; the refresh populates one.
A store the refreshed config newly names as a bootstrap store therefore stays absent from
`bootstrapStores`, which puts it on `dropStoreMapping`'s non-bootstrap branch -- the mapping is
dropped and the next message to it goes down `startPoolLookup`, which has to read the bootstrap
pool's state through the client, through the very stores that are unreachable.

## Chosen behaviour

Every exit releases the guard, and the refresh applies the config it fetched rather than
re-reading the file.

A fetch that fails -- by failed `Future` or by synchronous throw -- releases the guard and leaves
the refresh retryable, exactly as `startHostLookup` and `startPoolLookup` treat a failed lookup. A
config that does not parse is not written to disk and releases the guard. A config that parses is
applied to the in-memory maps whether or not the file write succeeded. A failure to obtain the
`ExecutionContext` never acquires the guard in the first place, and propagates.

That last point is the one real behavioural choice here. The write is durability for the next
process start; the in-memory map is what routes the running process. Skipping the update on a
write failure would leave this process routing to the host the moved store just left -- the state
the refresh was called to correct. A restart falls back to the old file and re-learns through the
same `UnknownStore` path, so applying in memory is strictly better than the alternative and
strictly better than today, where a write failure means the branch runs and does nothing.

Parsing happens before the write, for the opposite reason. A config this build cannot parse must
not replace the one on disk: that file is the only thing the next process start has to bootstrap
from, and a failure to parse it there is unrecoverable, unlike a stale mapping here.

## Design

### `refreshBootstrapConfig`

```scala
private def refreshBootstrapConfig(): Unit =
  if !refreshingBootstrapConfig then
    logger.info("Refreshing bootstrap config file")
    oClient match
      case None => logger.error(s"Refreshing bootstrap config before AspenClient initialized!")
      case Some(client) =>
        given ExecutionContext = client.clientContext
        refreshingBootstrapConfig = true

        val fetch =
          try Success(client.getBootstrapConfig())
          catch case t: Throwable => Failure(t)

        fetch match
          case Success(fCfg) =>
            fCfg.onComplete: result =>
              try
                result match
                  case Success(cfg) => applyBootstrapConfig(cfg)
                  case Failure(err) =>
                    logger.error(s"Failed to fetch the bootstrap config. Error: $err", err)
              catch
                case NonFatal(t) =>
                  logger.error(s"Failed to apply the fetched bootstrap config. Error: $t", t)
              finally
                synchronized:
                  refreshingBootstrapConfig = false

          case Failure(t) =>
            refreshingBootstrapConfig = false
            logger.error(s"The getBootstrapConfig call threw. Error: $t", t)
```

Each of the four wedges above is answered, though only three of them by a release. Both outcomes
of the `Future` reach the `finally`, which covers wedges 1 and 4 -- the latter now arriving as a
throw out of `applyBootstrapConfig` by way of the `catch`. A synchronous throw from the fetch
takes the `Failure(t)` branch, covering wedge 2. Wedge 3 is answered by never setting the flag;
see below.

`.foreach` becomes `.onComplete`. That single substitution is the actual fix for path 1, and it is
what `startHostLookup` already uses for the same reason.

The `Failure(t)` branch releases the flag without a `synchronized` of its own because it runs
under the caller's monitor -- `refreshBootstrapConfig` is reached only from `dropStoreMapping` and
`receivedUnknownStoreFromHost`, both of which are `synchronized`. Same contract `startHostLookup`
states for its catch. It also releases *before* it logs, per the repair-first rule that method
records: a logger that throws must not be able to leave the wedge behind.

The wrapper is `Throwable` rather than `NonFatal`, and scoped to the fetch call alone rather than
to the `onComplete` registration. Both are copied from `StoreManager.startDeviceCheck`, whose
scaladoc argues them: widening the scope would not catch a non-fatal throw out of the callback
body -- `Future`'s `Transformation.run` absorbs those and routes them to `reportFailure` -- and
`Throwable` is what keeps a fatal throw from reopening the leak.

**The `given` binding moves above the flag, and that is the whole of the fix for wedge path 3.**
`clientContext` is an abstract `def` on `AspenClient`, so a throw from it is not structurally
impossible, and today it is evaluated with the flag already set. Reading it first means such a
throw leaves the flag untouched and propagates to the caller instead of wedging. It cannot be
brought inside the wrapper without splitting the `ExecutionContext` out of the `given` and
rebinding it in the `Success` branch, which buys nothing: the wrapper's job is to convert a throw
into a released guard, and a throw before the flag is set has no guard to release.

This does leave `refreshBootstrapConfig` able to throw, unlike `startDeviceCheck`, which went to
some length for totality. The difference is that `startDeviceCheck`'s totality is load-bearing --
it is re-dispatched from inside a `finally`, where a throw would mask the exception already
unwinding -- and nothing calls `refreshBootstrapConfig` from a `finally`. Its callers,
`dropStoreMapping` and `receivedUnknownStoreFromHost`, are reached from `dropCacheForStore` in
`TransactionDriver` and `BaseReadDriver`, neither of which is an iteration a propagating throw
would truncate. A propagating throw there is worth having over a silent permanent wedge.

The callback's `catch` exists so the diagnostic survives. Without it, a throw out of
`applyBootstrapConfig` propagates out of the callback, `Transformation` routes it to
`reportFailure`, and what happens next is the `ExecutionContext`'s business -- on some it is
silent. The guard would still be released by the `finally`; the reason for the exception would not
be recorded.

### `applyBootstrapConfig`

```scala
/** Parses `cfg`, writes it to the bootstrap config file, and folds its store placements into
 *  the stores map.
 *
 *  Parses before writing. A config this build cannot parse must not replace the one on disk:
 *  that file is the only thing the next process start has to bootstrap from, and a failure to
 *  parse it there is unrecoverable, unlike the stale mapping a skipped refresh leaves here. The
 *  parse throw propagates to the caller, which logs it and releases the refresh guard.
 *
 *  Takes the monitor for the map update only, leaving the filesystem write outside it, which is
 *  where the write has always been. Called from the fetch callback, which does not hold it.
 */
private def applyBootstrapConfig(cfg: String): Unit =
  val config = BootstrapConfig.parseBootstrapConfig(cfg)

  try
    atomicWrite(bootstrapConfigFile.toNIO, cfg)
    logger.info(s"Updated bootstrap config written to $bootstrapConfigFile")
  catch
    case NonFatal(t) =>
      logger.error(s"Failed to update bootstrap config file $bootstrapConfigFile. Error: $t", t)

  // Applied even when the write failed, and deliberately so. The write is durability for the
  // next process start; this map is what routes the running process. Skipping it on a write
  // failure would leave this process routing to the host the moved store just left, which is
  // the state the refresh was called to correct. A restart falls back to the old file and
  // re-learns through the same UnknownStore path that got us here.
  synchronized:
    mapBootstrapStores(config)
```

The write is the only step with a local `catch`, because it is the only one the method continues
past. The parse has nothing to continue to, so it propagates.

### `mapBootstrapStores`

```scala
/** Folds `config`'s store placements into `stores` and `bootstrapStores`.
 *
 *  Add-only in both directions: a store the config no longer names keeps whatever mapping it
 *  has and stays a bootstrap store. Removal is not modelled because nothing in the system
 *  removes a store from the bootstrap set today, and the failure it would guard against --
 *  routing to a host that no longer hosts the store -- is the one the next UnknownStore
 *  corrects anyway.
 *
 *  bootstrapStores is updated here and not only in the constructor because a bootstrap store
 *  missing from that set takes dropStoreMapping's else branch: the mapping is dropped and the
 *  next message to it goes down startPoolLookup, which has to read the bootstrap pool's state
 *  through the client -- through the very stores that are unreachable. The set is what keeps a
 *  bootstrap store on the refresh path instead of that circle.
 *
 *  Caller must hold this object's monitor, except from the constructor, where no other thread
 *  can see this object yet.
 */
private def mapBootstrapStores(config: BootstrapConfig.Config): Unit =
  config.hosts.foreach: bsHost =>
    bsHost.stores.foreach: storeId =>
      stores += storeId -> bsHost.hostId
      bootstrapStores += storeId
```

### Constructor

The constructor's inline fold becomes a call to the same method, so the constructor and the
refresh can no longer disagree about what makes a store a bootstrap store. That disagreement is
the `bootstrapStores` bug.

```scala
loadInitialBootstrapConfig()

...

private def loadInitialBootstrapConfig(): Unit =
  val config = BootstrapConfig.loadBootstrapConfig(bootstrapConfigFile.toIO)

  mapBootstrapStores(config)

  config.hosts.foreach: bsHost =>
    hosts += bsHost.hostId -> Right(networkImplInterface.createHostEntry(
      bsHost.hostId,
      bsHost.name,
      bsHost.address,
      bsHost.dataPort,
      bsHost.cncPort,
      bsHost.storeTransferPort,
      new EvictingQueue[Message](1)
    ))
```

A method rather than an inline block so the parsed config does not become a retained field. The
call site stays where the current constructor block is, after the `var` declarations it writes,
since those initialize in declaration order.

### `BootstrapConfig.parseBootstrapConfig`

`loadBootstrapConfig` takes a `File` and there is no string-based path, which is why the current
code re-reads what it just wrote. Beside it (`YamlFormat.*` is already imported):

```scala
def parseBootstrapConfig(yaml: String): Config =
  Config.create(loadYamlString(yaml))
```

### `YamlFormat.loadYamlString`

That file is Scala 2 brace style; the addition matches it.

```scala
/** Parses the YAML document contained in the given string. */
def loadYamlString(yaml: String): Object = {
  val doc = new Yaml(new SafeConstructor).load[java.util.AbstractMap[Object, Object]](yaml)

  if (doc == null)
    throw new FormatError("Empty YAML document")

  doc
}
```

`loadYamlFile` is left alone rather than refactored to delegate: it streams the file instead of
holding it in memory, and that is worth keeping. The duplicated null check is two lines.

## Tests

### Fixture seams

`LookupRecordingClient` gains a `getBootstrapConfig` override, mirroring `failLookupWith`:

```scala
private var bootstrapConfigAttempts: Int = 0
private var bootstrapConfigPromises: Map[Int, Promise[String]] = Map()
private var bootstrapConfigFailure: Option[Throwable] = None

/** How many times getBootstrapConfig has been called. The refresh guard is invisible in the
 *  manager's public state -- a wedged guard and a refresh nobody asked for both look like
 *  nothing happening -- so the call count is what distinguishes them. */
def bootstrapConfigFetches: Int

/** The Promise backing the `attempt`-th (1-based) fetch, created on first use.
 *
 *  Keyed by attempt rather than one promise for the subject, as lookupPromise is: a
 *  retryability test needs attempt 1 to fail and attempt 2 to succeed, which a single promise
 *  cannot express. */
def bootstrapConfigPromise(attempt: Int): Promise[String]

/** Makes getBootstrapConfig throw `err` synchronously rather than returning a future. The call
 *  is still counted before the throw, for the reason failLookupWith records first. */
def failBootstrapConfigWith(err: Throwable): Unit
def clearBootstrapConfigFailure(): Unit

override def getBootstrapConfig(): Future[String] = synchronized:
  bootstrapConfigAttempts += 1
  bootstrapConfigFailure match
    case Some(err) => throw err
    case None => bootstrapConfigPromise(bootstrapConfigAttempts).future
```

The fixture's existing lock-ordering invariant carries over unchanged: tests complete these
promises without holding the client's monitor, because `ExecutionContext.parasitic` runs the
manager's continuation inline on the completing thread.

Two more fixture changes:

- `newManager()` copies the `beforeAll` template into a fresh temp directory per call. Today every
  manager shares the one file, which was fine while nothing wrote to it; a successful refresh now
  overwrites it and would leak into later tests. A `newManagerWithConfigFile()` variant returns
  that path as a fourth element for the tests that assert on the file or need to remove its
  directory. Existing call sites in the other four suites are untouched.
- A `bootstrapConfigYaml(hostStates, storeMap)` helper wrapping `generateBootstrapConfig`, so a
  test can build a "the store moved to `remoteHostId`" config.

No production test seam is needed for the write failure. Giving the manager a config file in its
own temp directory and removing that directory after construction makes `atomicWrite`'s
`Files.createTempFile(parentDir, ...)` fail with `NoSuchFileException` -- an `IOException`, so it
takes the write's local catch. That works regardless of whether the tests run as root, which a
permissions-based approach would not.

### New suite: `MetadataManagerBootstrapRefreshSuite`

A new file rather than a group inside `MetadataManagerExceptionSafetySuite`, whose scaladoc scopes
it to what a throw costs in the *lookup* paths. Tests 4 and 5 below are successful-path behaviour
and do not belong under that heading, and splitting six tests that share one seam across two files
to preserve it would be the worse trade.

All six drive the refresh through `mgr.dropStoreMapping(StoreId(poolId, 0))`, the fixture's
bootstrap store.

1. **A fetch that returns a failed Future leaves the refresh retryable.** Fail attempt 1's promise;
   a second `dropStoreMapping` must produce a second fetch.
2. **A getBootstrapConfig call that throws leaves the refresh retryable.** Arm
   `failBootstrapConfigWith`, confirm nothing escapes `dropStoreMapping`, clear it, retry, and the
   count reaches 2.
3. **A config that does not parse leaves the refresh retryable and the file unchanged.** Complete
   attempt 1 with malformed YAML. This is the test that exercises the callback's outer `catch` and
   `finally`.
4. **A refresh in flight coalesces a second request.** Two `dropStoreMapping` calls with attempt 1
   still pending leave the count at 1.
5. **A successful refresh remaps the store and adopts new bootstrap stores.** Complete with a
   config placing the fixture's store on `remoteHostId` plus a second store there.
   `getHostEntryOrQueueMessage(storeId, msg)` must now start a lookup for `remoteHostId` --
   pre-refresh it resolves against `bootstrapHostId` with no lookup at all, so the assertion
   separates cleanly -- and `isBootstrapStore` must be true for the new store.
6. **A write failure still applies the mapping in memory.** As 5, with the config directory
   removed: the remap must land and the guard must release even though the file was not updated.

Everything observed goes through the public surface -- `bootstrapConfigFetches`,
`isBootstrapStore`, `client.lookups`, and the file itself. Nothing reaches into private state.

### What each test proves

Written before the fix, per the rhythm of the surrounding commits. Honest accounting of what fails
pre-fix:

- Tests 1, 2 and 6 fail outright: the wedge, the synchronous wedge, and the write-failure no-op.
- Test 3 fails on both halves. Today the malformed config is written to disk *first*, and then
  `loadBootstrapConfig` throws inside the `finally` and wedges the guard.
- Test 5 partially fails: the remap lands, `isBootstrapStore` on the new store does not.
- **Test 4 passes pre-fix.** It is a regression guard on coalescing the current guard already
  provides, not a demonstration of a bug.

## Out of scope

- **Retry pacing.** Declined deliberately. The guard still coalesces what matters in production: a
  slow fetch is the common failure -- `getBootstrapConfig` reads through the client's read driver,
  which retransmits with exponential backoff and imposes no timeout -- and it holds the guard for
  the whole outage. Only a fast-failing fetch, such as a missing `BootstrapConfigKey`, storms, and
  one radicle read per bootstrap-store cache drop beats a permanently wedged guard.
- **Removal semantics.** `mapBootstrapStores` is add-only, as argued in its scaladoc.
- **Stale bootstrap `HostEntry`s.** The refresh updates store-to-host but never `hosts`, so a
  bootstrap host whose address or ports changed in the fetched config keeps the `HostEntry` the
  constructor built from the old file, and messages keep going to the old endpoint. Fixing it
  means tearing down and rebuilding a live dealer through `NetworkImplInterface`. Recorded in
  TODO.txt as a separate finding.
- The other MetadataManager guards. `pendingPoolLookups` and `hosts` use map presence rather than
  a Boolean, and both were hardened by the 2026-07-30 work; `refreshingBootstrapConfig` is the
  only Boolean guard in the class.

## Files affected

| File | Change |
|---|---|
| `src/main/scala/org/aspen_ddp/aspen/common/network/MetadataManager.scala` | `refreshBootstrapConfig` rewrite; new `applyBootstrapConfig` and `mapBootstrapStores`; constructor dedup |
| `src/main/scala/org/aspen_ddp/aspen/common/metadata/BootstrapConfig.scala` | `parseBootstrapConfig(yaml: String)` |
| `src/main/scala/org/aspen_ddp/aspen/common/util/YamlFormat.scala` | `loadYamlString(yaml: String)` |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerFixture.scala` | `getBootstrapConfig` seam; per-manager config file; `bootstrapConfigYaml` helper |
| `src/test/scala/org/aspen_ddp/aspen/common/network/MetadataManagerBootstrapRefreshSuite.scala` | New; six tests |
| `TODO.txt` | Remove the fixed entry; add the stale-`HostEntry` finding |

## Verification

- `sbt compile`
- `sbt 'testOnly *MetadataManager*'`
- `sbt test`
