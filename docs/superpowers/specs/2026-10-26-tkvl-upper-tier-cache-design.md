# TKVL Upper-Tier Node Cache

## Intent

The TKVL is a general-purpose data structure. In real applications tree instances are long-lived and
frequently reused. Today every operation pays for a full navigation: `getRData` reads the object hosting
the `Root`, then the root node, then `TierEngine` reads one node per tier on the way down to tier 0.
`getAllocatorForTier` and `getMaxNodeSize` each repeat the `getRData` reads. Upper tiers change very
slowly, so almost all of those upper-tier reads are wasted.

Add a cache of upper-tier nodes (and of the root) to the tree instance so repeated navigations of a
long-lived tree skip them. The motivation is general performance, not any one application.

## Goals

- Repeated navigations of a reused tree instance read fewer objects. The common lookup on a warm cache
  reads only the tier-0 node(s) it needs.
- Caching never changes what an operation returns or commits. A stale cache entry costs extra reads,
  never a wrong answer.
- The cache is an interface with pluggable implementations. No single policy is best for every workload.
- A `NoCache` implementation exists for tests that need to see every read.
- The default implementation is the simple policy below, and TKVLs use it unless told otherwise.

## Non-goals / anti-goals

- No caching of tier-0 nodes. Gets, walks and updates always see current tier-0 data.
- Not changing the internal call sites that build a fresh `TieredKeyValueList` / `KVObjectRootManager`
  per operation (MetadataTree, Registry, SimpleStoragePool, the Radicle services tree, ...). They simply get
  no benefit. Making them reuse instances is a separate piece of work.
- No cross-instance or client-wide sharing of cached nodes.
- Anti-goal: any path where a stale cache leads to a wrong result, a spurious `BrokenTree`, or a write
  that is retried forever against the same stale state.

## Where the cache lives

The cache belongs to the tree instance, owned by its `RootManager` (the root manager is the object that
already reads the root, and both facades and `TierEngine` reach the tree through it). Each `RootManager`
implementation (`KVObjectRootManager`, `SimpleDirectoryRootManager`, `SimpleFileRootManager`) accepts a
cache, defaulting to the default implementation. Root managers decoded by `RootManagerFactory`, as in the
finalization actions, get a fresh default cache. That is harmless: those instances are short-lived.
*(My call: user agreed the cache belongs to the long-lived tree instance; owning it in the root manager
is my choice of where.)*

The cache is accessed concurrently by the futures of overlapping operations on one instance, so
implementations must be thread-safe.

## Default policy

*(The Todo suggested an LRU of 5 nodes per tier with whole-tier invalidation; the user approved this
refinement of it.)*

- Per tier >= 1, a cache of at most 5 nodes, keyed by node `ObjectId`. Built on Scaffeine per project
  convention. Caffeine's eviction is approximately LRU (W-TinyLFU); that is acceptable.
- The root is cached separately (the `Root` and the root node), but only when the root node is on tier
  >= 1. An empty tree or a tier-0 root is never cached, because that would amount to caching tier-0
  state: a cached "empty" root would make `get` miss keys that exist.
- **Targeted eviction rather than whole-tier drops.** During a descent, when a cached parent P on tier
  t+1 hands us child C on tier t and either
  - the descent has to scan right from C to reach the target (P's entry is behind a split), or
  - reading C fails (P points at a node a join deleted),

  then evict P only. The rest of tier t+1 stays cached. If P is the cached root, drop the cached root so
  the next operation re-reads it.
- Scans right that start from a freshly read (non-cached) parent evict nothing.
- A split whose finalization action has not yet updated the parent produces the same scan-right signal
  repeatedly until it runs. Each time P is evicted and re-read. That is accepted.

- **Detecting staleness on every tier** *(my call)*. `descend` only scans right on its target tier, so a
  stale node on tier >= 2 would otherwise never be noticed. Whenever a descent steps from parent P to child C
  and C's range does not hold the target (the target is at or past C's right link minimum), P is stale.
  If P is cached, evict it. This check runs at every tier, including the step into tier 0, and is the
  general form of the scan-right rule above. The scan itself still only happens on the target tier.
- **The cached root is one entry** *(my call)*: the `Root`, the root node and the hosting object's revision,
  stored and replaced together so `getRootNode` and `getRoot` always agree. On a hit, `getRoot`,
  `getAllocatorForTier`, `getMaxNodeSize` and `getRootNode` are answered from it. A stale `nodeAllocator`
  is acceptable: slice limits are fixed at creation. A hit skips `getRData`'s "root node unreadable, so
  report an empty tree" fallback. A deleted tree then shows up as a failed read during the descent, and
  the safety net handles it.

## Which operations use the cache

*(My call: cached nodes route operations but are never the base of an upper-tier write.)*

- **Use the cache:** `get`, `getContainingNode`, the walks and `foldLeft`, and the routing of
  `prepareUpdates` (`locate`) and `deleteFromContainingNode`. Each of these writes only tier-0 nodes, and
  those are always read fresh. Upper-tier changes they cause go through finalization actions.
- **Read fresh:** `splitTree` (it writes the upper-tier nodes on its path), `deleteTree` (it deletes the
  upper-tier nodes it finds), `prepareRootUpdate` and `createInitialNode` (see below), and the Split, Join
  and TailRemoval finalization actions. The finalization actions run with `NoCache`.

## Correctness rules

- **Safety net** *(scope is my call)*. It wraps only the descent and routing phase of an operation, before
  anything is staged in a transaction. It never wraps code after a transaction has been staged. It
  triggers when a descent that used any cached node ends in `BrokenTree` or `InvalidObject` (including
  running out of candidates). It then drops the whole cache and retries once with the cache bypassed for
  every tier. A failure on that fresh attempt is reported as today. Other read failures (timeouts and so
  on) are handled as `fetchNode` already handles them: the node is blacklisted and its cached parent
  evicted. They do not trigger the retry. `deleteTree` followed by reuse of the same instance therefore
  sees an empty tree (or the recreated tree), not `BrokenTree`.
- **Races between filling and evicting** *(my call)*. A read that completes after an eviction may put a
  stale node back in the cache. No generation counter is used: the entry only costs extra reads and is
  evicted again on its next use.
- **Root writes read fresh.** `prepareRootUpdate` and `createInitialNode` use the root container's revision
  as a transaction requirement and `createInitialNode` relies on observing the tree as empty. They must
  always read the root fresh, never from the cache, so an aborted write is never retried against the same
  stale revision.
- Tree height changes are tolerated by the existing navigation: after a tier is added the old root is
  still a valid node on its tier, and a scan right on the top tier drops the cached root. After a tier
  reduction or `deleteTree`, reads of deleted nodes fail and trigger eviction or the safety net.
- Tier-0 reads, transaction requirements and finalization actions are unchanged.

## Interface

Pluggable, with at least two implementations:

- the default (above), used by every TKVL unless another is passed;
- `NoCache`: every lookup misses and nothing is stored.

The cache accessor on `RootManager` is abstract, with no default *(my call)*, so a new implementation
can't silently opt out of the default cache. The test-only root managers (`TKVLSuite`, `TierEngineSuite`'s
`StubRootManager`) are updated to provide one.

The exact trait shape (method names, whether the root and node caches are one trait or two, how the
descent reports "this parent was stale") is left to the builder. It must let an alternative policy be
written without touching `TierEngine` navigation logic, and must cover both node kinds (KeyValue and
DataObject trees) since caching sits above `NodeEncoding`.

## Tests

- The suite runs with the default cache, so caching is exercised everywhere.
- Tests that depend on exact upper-tier read behavior and break under caching pass `NoCache` explicitly.
  Use `NoCache` only where it is really needed.
- New tests (at minimum):
  - a warm-cache lookup on a multi-tier tree reads only tier-0 node(s) (count reads);
  - a stale cached parent after a split: correct result, parent evicted, next lookup reads no extra
    nodes once the parent is updated;
  - a stale cached tier-2+ node (e.g. tier-1 split followed by a root split): detected and evicted;
  - a cached parent pointing at a joined-away child: correct result, parent evicted;
  - tier growth and tier reduction with a warm cache: correct results;
  - `deleteTree` then reuse of the same instance (empty / recreated tree): correct results via the
    safety net;
  - `createInitialNode` / root updates on an instance with a cached root are not starved;
  - both tree kinds.
- Project test priorities apply: correct, never flaky, as fast as possible.

## Left to the builder

- Trait and class names, file layout.
- How the descent functions in `TierEngine` (`descend`, `fetchNode`, `locate`, `scanToContainingNode`,
  and the static helpers the finalization actions call) receive the cache.
- Whether shared root-caching logic is factored into a helper for the three root managers.
- How read counting is done in tests.
