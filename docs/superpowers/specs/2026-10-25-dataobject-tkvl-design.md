# DataObject-backed Tiered Key Value Lists

## Intent

The TKVL (`client/tkvl`) stores every node, at every tier, as a KeyValue object. KeyValue objects
carry per-entry metadata (revision and timestamp) and their own encoding, so the tree has poor
storage efficiency. The inefficiency is a property of the KeyValue object encoding and is independent
of workload. DataObjects are naturally more compact.

Add a second kind of tree whose nodes are DataObjects, for low-contention uses such as a single
writer. Share the important tree logic between both kinds: content management, tree navigation
(descent, scan-right, tier growth/shrink), and the decisions about when to split and join. The
end-user API cannot hide every difference between the two kinds, but the behind-the-scenes logic should.

## Goals

- A tree whose nodes (all tiers) are DataObjects, noticeably more compact than the KeyValue-object tree.
- One implementation of the tree logic, driving both node kinds.
- The tree logic decides *what* to do (insert key, delete key, split here, join these); a node
  *encoding* decides *how* the bytes are laid out. The encoding is a pluggable implementation
  detail, independent of the rest of the design, and invisible to the tree logic and to `Root`.
- `Root` records which kind of tree it stores (an enum). A tree never mixes kinds.
- The existing KeyValue-object tree keeps its behavior.

## Non-goals / anti-goals

- No mixed trees (KV-object nodes and DataObject nodes in one tree).
- Backward compatibility with persisted data is not required (project policy), so the `Root`
  format may change freely, including for the existing KV-object tree.
- Do not hide the semantic difference from end users: DataObject entries have no per-entry
  revision, so per-key revision requirements do not exist for that tree kind.
- A convoluted abstraction is worse than some duplication.
- The `RandomAccessReedSolomon` append-based encoding (insert/tombstone appends until the node is
  full, overwrite after compaction, split when needed) is not part of the first deliverable, but
  the encoding interface must not make it hard to add (the encoding, not the tree logic, owns
  whether an update is a rewrite or an append).

## Behavior

- Concurrency control for DataObject nodes is a whole-node revision requirement. Two writers
  touching the same node conflict. Accepted by the author; the kind is for single-writer and
  low-contention use.
- The user-facing API makes the differences visible (no key-level revision requirements, no
  per-entry `ValueState` revision/timestamp for the DataObject kind).
- A node holds: its entries, its minimum key, and the pointer to its right sibling. A DataObject has
  no side fields, so these live inside the encoded bytes (design call of the brainstorm, see below).
- Upper tiers of a DataObject tree are also DataObjects; their entries map a minimum key to an
  encoded down pointer (to a DataObject node).

## Operations in the first deliverable

The DataObject tree supports the full `TieredKeyValueList` surface (`get`, `set`, `delete`,
`foldLeft`, `foreach`, `foreachFrom`, `foreachInRange`, `splitTree`, `deleteTree`) without per-key
revisions, timestamps or key-level requirements.

`getContainingNode` / `TieredKeyValueListNode` are **not** generalized; they stay KV-object-only.
Their only callers (amoebafs `CreateFileTask.addToDirectory` and `SimpleDirectory.prepareRename`)
need per-key revisions, or need to know if two keys share a node so they can avoid modifying one
object twice in a transaction. The latter need is better served by the separate `prepareUpdates`
todo item. Consequently, a DataObject-tree `set` or `delete` that would modify a node already
modified in the same transaction fails with a clear error rather than producing two updates to
one object. Atomic multi-key changes on DataObject trees wait for `prepareUpdates`.

## Constraints

- Node sizing must continue to respect fixed-sized object and slice limits (`NodeAllocator`,
  `maxNodeSize`); the encoding is responsible for reporting the size an entry/node will occupy.
- Split/join finalization actions, `NodeAllocator` and `RootManager` currently assume
  `KeyValueObjectPointer`; they must work for either kind.
- `KeyValueObjectState` types are used in about 30 files. Callers outside `client/tkvl` should
  be touched only where unavoidable.

## Decided by the author

- Whole-node revision check is fine for DataObject trees.
- Encoding is pluggable and separate from the tree structure.
- `Root` gains an enum for the tree kind.

## Calls made by the brainstorm (to be checked)

- The KV-object tree's on-disk format and behavior are unchanged by the refactor.
- Right pointer and node minimum are stored inside the encoded bytes for DataObject nodes.
- Upper tiers use the same node kind as tier 0.

- Split/join thresholds are in encoded bytes against `maxNodeSize`, as today. The encoding reports
  the size of an entry set and the node overhead; the tree logic compares it with the limit.
- The tree logic asks the node kind for the transaction requirements to use (key-level for
  KV-object nodes, whole-node revision for DataObject nodes); the split/join transaction shape
  (two nodes plus the upper tier) is otherwise the same for both kinds.
- Updating a DataObject node's right pointer or contents is a whole-node rewrite under a revision
  requirement. Finalization actions already retry until successful, so a conflict is retried
  against a freshly read node.
- The tree kind is chosen only when the tree is created (`Root.create` takes a kind, defaulting
  to KV-object), and the `Root` pointer type becomes a sealed type distinguishing the two kinds.
  Decoding a pointer that doesn't match the kind is an error.
- DataObject nodes need only plain whole-object writes, so they work on any pool IDA.
- Existing TKVL tests stay as they are; new tests exercise the shared logic against both kinds.

## Left to the builder

- The node abstraction's exact shape and package layout.
- The first DataObject encoding's byte layout (compactness is the aim).
- How the user-facing API surfaces the differences (separate types vs. shared trait).
- Test strategy; the shared logic should be exercised against both kinds.
