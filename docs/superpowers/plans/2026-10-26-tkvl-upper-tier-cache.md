# TKVL Upper-Tier Node Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give each long-lived TKVL instance a pluggable cache of its root and tier >= 1 nodes so repeated navigations skip upper-tier reads, without changing any result.

**Architecture:** A `TierCache` trait (default `LruTierCache`, 5 nodes per tier, Scaffeine; plus `NoCache`) is owned by each `RootManager`. Root managers answer root queries from the cached `RootState` and offer `getFreshRootNode()`. `TierEngine`'s descent helpers take a `cache` parameter (default `NoCache`), read upper-tier nodes through it, and report stale parents with `cache.stale`. A safety net in `TierEngine` retries a failed navigation once with `NoCache`.

**Tech Stack:** Scala 3, Scaffeine, ScalaTest (`IntegrationTestSuite`, `TreeKindSuiteBase`).

**Spec:** `docs/superpowers/specs/2026-10-26-tkvl-upper-tier-cache-design.md`

## Global Constraints

- No caching of tier-0 nodes. The root is cached only when the root node is on tier >= 1.
- Default policy: at most 5 nodes per tier >= 1 (`TierCache.DefaultNodesPerTier = 5`), keyed by `ObjectId`.
- Cached nodes route operations but are never the base of an upper-tier write: `splitTree`, `deleteTree`, `prepareRootUpdate`, `createInitialNode` and the Split/Join/TailRemoval finalization actions read fresh.
- `RootManagerFactory.createRootManager` implementations construct with `NoCache`.
- `RootManager.cache` is abstract, with no default.
- Implementations must be thread-safe (overlapping futures share one instance's cache).
- The safety net covers only navigation, before anything is staged in a transaction.
- Scala 3 quiet syntax, indented if/then/else (CLAUDE.md). Tests: correct, never flaky, as fast as possible (CLAUDE.local.md).
- Run single suites with `sbt 'testOnly *SuiteName'`. Don't use long timeouts on full runs.

## Review Focus

1. Overlapping gets and sets on one warm instance: all results correct and no exception from the cache. Pinned in Task 3 (`concurrent operations on one warm instance`).
2. A tree deleted through **another** instance while this one is warm: `get` returns None, not `BrokenTree`. Pinned in Task 4.
3. A tree deleted and recreated through another instance: the warm instance sees the new contents. Pinned in Task 4.
4. A `set` routed by a stale cached parent (after a split) lands in the right tier-0 node: a fresh instance reads it back. Pinned in Task 3.
5. A root write (`prepareRootUpdate`, `createInitialNode`) on an instance whose cached root predates another instance's root update must commit, not abort against the stale revision. Pinned in Task 2.

---

### Task 1: `TierCache`, `NoCache`, `LruTierCache`

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/TierCache.scala`
- Test: `src/test/scala/org/aspen_ddp/aspen/client/tkvl/TierCacheSuite.scala`

**Interfaces:**
- Produces:
  ```scala
  /** The root as a root manager reads it: the Root, its hosting object's revision and the root node */
  final case class RootState(root: Root, revision: ObjectRevision, node: Option[ListNode])

  trait TierCache:
    def root: Option[RootState]
    def putRoot(r: RootState): Unit          // stores only if r.node.isDefined && r.root.tier >= 1, else dropRoot()
    def dropRoot(): Unit
    def node(tier: Int, id: ObjectId): Option[ListNode]
    def put(tier: Int, node: ListNode): Unit // ignored for tier < 1
    /** A descent found `node`, on `tier`, out of date: evict it, and drop the cached root if `node` is the root node */
    def stale(tier: Int, node: ListNode): Unit
    def clear(): Unit

  object TierCache:
    val DefaultNodesPerTier: Int = 5
    def default(): TierCache = new LruTierCache(DefaultNodesPerTier)

  object NoCache extends TierCache            // stores nothing, every lookup misses
  final class LruTierCache(nodesPerTier: Int) extends TierCache
  ```

- [ ] **Step 1: Write the failing tests** in `TierCacheSuite` (an `IntegrationTestSuite` if building a `Root` needs a client. Use stub `ListNode`s like `TierEngineSuite.StubNode` with distinct pointers):
  - `NoCache stores nothing`: after `put(1, n)` and `putRoot(rs)`, `node(1, n.pointer.id)` and `root` are None.
  - `LruTierCache ignores tier 0`: `put(0, n)` → `node(0, id)` is None. `put(1, n)` → `node(1, id)` is `Some(n)`.
  - `LruTierCache caches only tier >= 1 roots`: `putRoot` with `root.tier == 0` → `root` is None. With tier 1 and a node → `Some`. Then `putRoot` of a tier-0 state → None again.
  - `LruTierCache bounds each tier`: put 6 distinct nodes on tier 1 → at most 5 present (after the cache's cleanup). Tier 2 is unaffected.
  - `stale evicts the node and the root it backs`: with the root node also put on tier 1, `stale(1, rootNode)` → `node(1, id)` None and `root` None. `stale` of a non-root node leaves `root` intact.
  - `clear empties every tier and the root`.
- [ ] **Step 2: Run** `sbt 'testOnly *TierCacheSuite'`. Expected: compile failure (types not defined).
- [ ] **Step 3: Implement** `TierCache.scala`. `LruTierCache` holds a `TrieMap[Int, Cache[ObjectId, ListNode]]` built with `Scaffeine().maximumSize(nodesPerTier).executor(_.run())` so eviction is synchronous and deterministic. Hold the root in an `AtomicReference[Option[RootState]]`.
- [ ] **Step 4: Run** `sbt 'testOnly *TierCacheSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Add TierCache with NoCache and per-tier LRU implementations"`

### Task 2: Root caching in the root managers

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/RootManager.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KVObjectRootManager.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleDirectoryRootManager.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileRootManager.scala`
- Modify: `src/test/scala/org/aspen_ddp/aspen/TestNetwork.scala` (read counting)
- Modify: test stubs in `TierEngineSuite.scala` (`StubRootManager`) and `TKVLSuite.scala` (`StaleOnceRootManager`). Change `numTiers`/`allTiers` in `TreeKindSuiteBase.scala` and `allTiers` in `TierEngineDescentSuite.scala` to use `getFreshRootNode()` (they inspect structure).
- Test: `src/test/scala/org/aspen_ddp/aspen/client/tkvl/KVObjectRootManagerSuite.scala`

**Interfaces:**
- Consumes: Task 1's `TierCache`, `RootState`, `NoCache`.
- Produces:
  ```scala
  // RootManager trait
  def cache: TierCache
  /** getRootNode, bypassing the cache. The result refreshes the cache. */
  def getFreshRootNode(): Future[(Int, KeyOrdering, Option[ListNode])]

  // RootManager companion: the shared cache logic used by all three implementations
  def cachedRead(cache: TierCache, fresh: Boolean)(read: => Future[RootState])
                (using ExecutionContext): Future[RootState]
  // Not fresh and cache.root is Some → that. Otherwise run `read` and then cache.putRoot on the result.

  // Constructors gain a trailing parameter:
  class KVObjectRootManager(client, treeKey, pointer, val cache: TierCache = TierCache.default())
  class SimpleDirectoryRootManager(client, inodePointer, val cache: TierCache = TierCache.default())
  class SimpleFileRootManager(client, inodePointer, val cache: TierCache = TierCache.default())

  // TestNetwork
  /** Runs `f`, counting `client`'s object reads: one per read operation (distinct readUUID), per object */
  def countReads[T](f: => Future[T]): Future[(T, Map[ObjectId, Int])]
  ```
  Each root manager's private `RData` is replaced by `RootState`. Its `getRData` becomes `getRData(fresh: Boolean = false)` and goes through `cachedRead`. `getRoot`, `getRootNode`, `getAllocatorForTier` and `getMaxNodeSize` use the cache. `prepareRootUpdate` and `createInitialNode` call `getRData(fresh = true)`. Factory `createRootManager` passes `NoCache`. The test stubs implement `cache` (`NoCache`, or `real.cache`) and `getFreshRootNode` (delegate).

- [ ] **Step 1: Write the failing tests** in `KVObjectRootManagerSuite`. Build trees with the suite's existing helpers. "Multi-tier" means a small node size and enough keys for tier >= 1. Quiesce with `waitForTransactionsToComplete()` before counting:
  - `a warm multi-tier root is served without reads`: after one `getRootNode()`, `net.countReads(rm.getRootNode())` reports an empty map, and `getRoot()`, `getMaxNodeSize(0)` and `getAllocatorForTier(0)` also report no reads.
  - `a tier-0 root is always read`: on a single-node tree, a second `getRootNode()` reads the hosting object and the root node (count map contains both ids).
  - `getFreshRootNode bypasses and refreshes the cache`: it reads the hosting object even when warm. Afterwards `rm.cache.root` holds the new state.
  - `root writes read fresh` (Review Focus 5): two managers `a` and `b` on one multi-tier tree. Warm `a`. Commit a `b.prepareRootUpdate(sameTier, sameRoot)` so the root entry's revision changes. Then `a.prepareRootUpdate(sameTier, sameRoot)` in a new transaction commits successfully.
- [ ] **Step 2: Run** `sbt 'testOnly *KVObjectRootManagerSuite'`. Expected: compile failure, then FAIL.
- [ ] **Step 3: Implement** the interfaces above in the four main files and `TestNetwork`. `countReads` records `Read` messages with `fromClient == client.clientId` in `cliMessenger.sendClientRequest` while active.
- [ ] **Step 4: Run** `sbt 'testOnly *KVObjectRootManagerSuite *TierEngineSuite *TKVLSuite *TreeKindParitySuite *TierEngineDescentSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Cache the TKVL root in each RootManager"`

### Task 3: Upper-tier node caching in the descent

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/TierEngine.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/TieredKeyValueList.scala`, `DataObjectTKVL.scala` (the `splitTree` warning)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/tkvl/TierCacheTreeSuite.scala` (extends `TreeKindSuiteBase`. Every test runs for both `TreeKind`s)

**Interfaces:**
- Consumes: Task 1 and Task 2 (`rootManager.cache`, `getFreshRootNode`, `net.countReads`).
- Produces, in the `TierEngine` companion:
  - `fetchContainingNode` / `fetchContainingNodePath`: new trailing parameter `cache: TierCache = NoCache`, passed down. The finalization actions keep calling without it.
  - `descend(..., cache: TierCache)`.
  - `fetchNode(client, encoding, ordering, tier: Int, minimum, pointer, blacklist, cache: TierCache)`. `tier` is now the tier of the node being fetched (`currentTier - 1`), not `targetTier`. On tier >= 1 it serves `cache.node(tier, pointer.id)` when present. Otherwise it reads and then calls `cache.put(tier, node)`.
- Behavior:
  - **Staleness**, in both `descend` and `locate`: after stepping from parent P (on tier t+1) to child C, call `cache.stale(t + 1, P)` when C's range does not hold the target (`descend`: `!C.keyInRange(target)`, only when `linksTo.isEmpty`; `locate`: `!C.keyInRange(group's greatest op key)`), or when the read of C fails.
  - The instance methods `withContainingNode`, `locate`/`locateTargets` and `deleteFromContainingNode` pass `rootManager.cache`. `splitTree` and `deleteTree` use `rootManager.getFreshRootNode()` and `NoCache`.
  - `splitTree` calls `rootManager.cache.clear()` on entry and again when `t.result` completes (either outcome).
  - The `splitTree` doc comments on `TieredKeyValueList` and `DataObjectTKVL` gain a **WARNING** paragraph: other long-lived instances of this tree (other clients, processes, or instances in this process) may keep returning entries that moved to the new tree until their caches are cleared (`rootManager.cache.clear()`), or they can be opened with `NoCache`. Handling this is the caller's responsibility.

- [ ] **Step 1: Write the failing tests** in `TierCacheTreeSuite`. Use a node size that gives 3 tiers from a few dozen keys. `rm` is the tree's `KVObjectRootManager`. Quiesce before every count:
  - `a warm lookup reads only tier 0`: set keys, `get(k)` once, then `countReads(get(k))` → exactly one object read, and it is the tier-0 node holding `k` (`tier0Nodes`).
  - `a stale cached parent after a split is evicted`: warm the cache through `get` of the last key. Then insert keys after it until the tier-0 node holding it splits, and quiesce. `get` of a key in the new right node returns its value, and the tier-1 parent from before is no longer in `rm.cache.node(1, id)`. The following `get` of that key reads at most 2 objects (the re-read parent plus the leaf), and the one after that reads exactly 1.
  - `a stale tier-2 node is detected`: warm, then insert until a tier-1 node splits, so the cached tier-2 node lacks the new entry. A `get` beyond the split returns the right value and evicts the cached tier-2 node (or drops the cached root if that node is the root).
  - `a cached parent pointing at a joined-away child is evicted`: warm through `get` of key m. Delete keys so that m's tier-0 node is joined away, and quiesce. `get` of a key that remains returns its value, and the parent is evicted.
  - `a set routed by a stale parent lands correctly` (Review Focus 4): after the stale-split setup, `set` a new key in the new node through the warm tree. A tree opened on a fresh `KVObjectRootManager(client, key, host, NoCache)` reads it back, and `entries()` matches the model.
  - `concurrent operations on one warm instance` (Review Focus 1): `Future.sequence` of interleaved gets and single-key set transactions (distinct keys) on one warm instance. Every get returns the model value, and the final `entries()` matches the model.
  - `splitTree clears the caller's cache`: warm, then `splitTree(k)` and commit. `rm.cache.root` is None or fresh, `get` of a moved key on the original returns None, and the new tree returns it.
- [ ] **Step 2: Run** `sbt 'testOnly *TierCacheTreeSuite'`. Expected: FAIL (reads > 1, nothing evicted).
- [ ] **Step 3: Implement** the behavior above in `TierEngine.scala` and the doc warnings in the two facades.
- [ ] **Step 4: Run** `sbt 'testOnly *TierCacheTreeSuite *TreeKindParitySuite *TierEngineDescentSuite *TKVLSuite *DataObjectTKVLSuite *TKVLBatchSuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Cache upper-tier TKVL nodes during descent"`

### Task 4: Safety net

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/TierEngine.scala`
- Test: `TierCacheTreeSuite.scala`

**Interfaces:**
- Consumes: Tasks 2 and 3.
- Produces (private to `TierEngine`):
  ```scala
  /** Runs `nav` with the instance's cache and root. A BrokenTree or InvalidObject clears the cache and runs `nav`
    * once more with NoCache and a fresh root. Only wrap code that stages nothing in a transaction.
    */
  private def navigate[A](nav: (TierCache, () => Future[(Int, KeyOrdering, Option[ListNode])]) => Future[A]): Future[A]
  ```
  `withContainingNode` wraps only the descent, not `f(node)`. `prepareUpdates` wraps `locateTargets`. `deleteFromContainingNode` wraps only the `fetchContainingNode` step of each `attempt`, not `del(node)`.

- [ ] **Step 1: Write the failing tests** (both kinds):
  - `deleteTree then reuse of the same instance`: warm, `deleteTree(None)`, quiesce. `get` returns None, and setting a key then reading it works.
  - `a tree deleted through another instance` (Review Focus 2): warm `a`, `deleteTree` through `b` (a second `KVObjectRootManager` on the same host/key). `a`'s `get` returns None, and `a`'s `entries()` is empty.
  - `a tree recreated through another instance` (Review Focus 3): continue from that. `b` sets keys again. `a`'s `get` returns them.
  - `tier reduction with a warm cache`: warm a 3-tier tree, delete most keys until the tree drops a tier (`numTiers` shrinks), quiesce. Every remaining key reads correctly, and `entries()` matches.
  - `tier growth with a warm cache`: warm a 2-tier tree, insert until `numTiers` grows, quiesce. Every key reads correctly.
- [ ] **Step 2: Run** `sbt 'testOnly *TierCacheTreeSuite'`. Expected: the deleteTree and deleted-through-another-instance tests FAIL with `BrokenTree`.
- [ ] **Step 3: Implement** `navigate` and wrap the three call sites.
- [ ] **Step 4: Run** `sbt 'testOnly *TierCacheTreeSuite *TKVLSuite *TreeKindParitySuite'`. Expected: PASS.
- [ ] **Step 5: Commit** `git commit -m "Retry TKVL navigation without the cache when it fails"`

### Task 5: Full suite, test fix-ups, docs

**Files:**
- Modify: whichever existing tests fail under the default cache
- Modify: `CLAUDE.md` (TKVL bullet under Key Design Aspects), `Todo.txt` (remove the first item)

- [ ] **Step 1: Run the full suite** `sbt test`.
- [ ] **Step 2: Fix each failure.** If a test depends on exact upper-tier reads or on seeing another instance's structural change immediately, construct its root manager with `NoCache`, or use `getFreshRootNode()` for structural inspection. Any other failure is a bug in Tasks 1–4: fix it there, not in the test.
- [ ] **Step 3: Re-run** `sbt test` until it passes, then run it twice more to check for flakiness. Expected: all PASS each time.
- [ ] **Step 4: Docs.** Add one CLAUDE.md bullet: each `RootManager` owns a `TierCache` (default `LruTierCache`, 5 nodes per tier >= 1, plus the root when on tier >= 1; `NoCache` for tests and finalization actions); stale entries cost only reads, except that `splitTree` through another instance can make warm instances return moved entries. Remove the cache item from `Todo.txt`.
- [ ] **Step 5: Commit** `git commit -m "Run the test suite with the default TKVL cache"`
