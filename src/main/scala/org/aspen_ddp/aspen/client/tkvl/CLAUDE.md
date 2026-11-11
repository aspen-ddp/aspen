### Tiered Key Value List (TKVL)
- Very simple B-Tree like data structure implemented in terms of a hierarchy of linked lists.
- Trees use a KeyOrdering instance to sort keys. Current orderings include Integer, Lexical, and ByteArray
- A tree is KeyValue-object or DataObject based (`TreeKind` in its `Root`); shared logic is `TierEngine` over a
  pluggable `NodeEncoding`. `DataObjectTKVL` is more compact, stores values only (no per-key revisions/requirements)
  and uses whole-node revision checks: intended for single-writer / low-contention use. A second update to one of its
  nodes in a transaction fails with `NodeAlreadyModified`; `getContainingNode`/`TieredKeyValueListNode` are KV-only.
- `prepareUpdates(inserts: List[TKVLInsert], deletes)` changes many keys in one transaction on either kind, updating
  each node once. `TKVLInsert.requireDoesNotExist` makes allocate-and-insert retry loops safe. Writes are per-key
  checked by default; `PutRequirement` (`MustNotExist`, ...) and `TKVLInsert.blind` opt into weaker checks, so a
  blind write needs a pin elsewhere in the transaction or `replaySafe`.
- Each `RootManager` owns a `TierCache` (default `LruTierCache`: 5 nodes per tier >= 1, plus the root when on
  tier >= 1; `NoCache` for tests and finalization actions). Stale entries cost only reads, except that `splitTree`
  through another instance can make warm instances return moved entries. `LruTierCache(anyRoot = true)` also
  keeps tier-0 and empty roots and shares one read among concurrent root misses; writes then read the root fresh.
  Only for a single writer that remembers its own changes (a vdisk's `BlockMap`, whose `refresh` clears it)
- Every time a new node is added to a list, an the minimum value of the new node and a pointer to the new object is
  inserted into the tier above it.
- Tier-0 holds the data, the upper tiers consist exclusively of (minimum_key, object-pointer) pairs
- The root of the tree is replaced every time the current root node is split into two nodes
- FinalizationActions are used to insert/remove entries in upper tiers as nodes are split/joined
- Left to right navigation in a tier is guaranteed to be consistent. Navigation between tiers is not
- The entry walks (`foreach*`, through `TierEngine.walk`) survive joins: a right-neighbour read that fails because a
  join deleted the neighbour (often caused by `fn` itself deleting what it visits) descends again to the node holding
  the last visited key and continues after it, up to `MaxJoinRaceRestarts` restarts in a row without a visited key.
  A delete from inside a walk must go through the tree (`deleteIfPresent`), not the node handed to `fn`: that node is
  the walk's snapshot, so a delete through it neither sees that it empties the node nor registers the upper-tier
  finalization actions
- A join absorbs a node's right neighbor, so a tier's rightmost node is never joined away. A join is also declined when
  the joined node would be larger than the neighbor, which happens when the emptied node's minimum is longer than the
  neighbor's. In both cases the node is left empty and `TailRemovalFinalizationAction` removes it: its predecessor
  (found by a descent through the upper tiers, not a scan of the tier) is pointed at the empty node's link, and its
  upper-tier entry is removed. If the neighbor's minimum is longer than the empty node's by then, the predecessor may
  no longer fit and splits in the normal manner; if it cannot be split, the node stays empty. Removing the upper-tier
  entries lets `JoinFinalizationAction` reduce the root by as many tiers as are left with a single entry. It reduces
  only a two-entry root that still holds the removed node's entry (the action runs at least once), and bumps each
  deleted node's version so that an entry added to it before the commit aborts the reduction.
- The size, split and join decisions of both encodings are in pure `plan*` functions (`KeyValueListNode` over a
  `NodeShape`, `DataObjectNodeEncoding` over a `NodeContent`); `insert`, `rename`, `delete`, `relink` and `splitAt`
  only apply the plan to a Transaction. The separation exists for testing: the node-size arithmetic is subtle and needs
  thousands of randomized cases, which are only practical without a running Aspen system (see `KeyValueNodePlanSuite`
  and `DataObjectNodePlanSuite`). Put new decision logic in the plan functions, not in the executors.
- `prepareUpdates` (both facades, and `KeyValueListNode` for a bare list) applies a batch of inserts and deletes in one
  transaction, updating each node once; `set`/`delete` are one-op batches. `TierEngine` splits the ops among each
  upper-tier node's down pointers and descends in parallel, falling back left like the single-key descent; groups
  landing on the same tier-0 node are merged. `ListBatch` then reads everything first (so an `InvalidObject` from a
  racing join restarts the batch with nothing staged) and stages each node once, aborting the transaction if a later
  node fails. A node can split into several (`SplitMany`, `planMultiSplit` over `SplitPlan.runs`); an empty tree
  gets a list of nodes (`allocateChain`). A node the batch empties absorbs its right neighbor only if the batch leaves
  the neighbor alone; otherwise it is left empty for `TailRemovalFinalizationAction`.
- Every write is checked by default (`PutRequirement.Checked`). On KV trees the planners derive per-key requirements
  from the node as read (`KeyValueListNode.putRequirements`/`deleteRequirements`): a put of a held key requires its
  `KeyRevision`, of an absent key `DoesNotExist`; a delete (single, batch, rename's old key, `deleteOneAtATime`)
  requires the key's `KeyRevision`, and a key the node does not hold is not deleted; rename's new key gets
  `KeyRevision` or `DoesNotExist`. These are per key: no node-level serialization. A competing write fails them as an
  ordinary abort that the caller retries, never `KeyAlreadyExists`. `PutRequirement.Blind` (`TKVLInsert.blind`) opts
  out, for idempotent upserts only; `AtRevision(rev)` requires a given revision.
- `TKVLInsert.requireDoesNotExist` (`MustNotExist`) works on both kinds: KV via `DoesNotExist`, DataObject via the
  node's revision guard (the key's range changes only through an update of its node). Only it fails with
  `KeyAlreadyExists`, when the node read holds the key. On DataObject trees `Checked` and `Blind` add nothing (every
  rewrite requires the node revision); `AtRevision` is KV-only (`UnsupportedRequirement`).
- A node deleted by a join, a tail removal, a root reduction or a DataObject `deleteList` has its version bumped, not just its revision
  locked. A lock leaves the revision unchanged, so a racing removal that read the node as a predecessor could still
  rewrite it after it was deleted and leave a live node linked to a deleted one (batches empty adjacent nodes
  routinely, which exposed this as a hang). Stores now also fail every requirement on a zero-refcount object
  (`RequirementsChecker.loaded`), which closes that race on its own. The bump is kept as a second guard.
