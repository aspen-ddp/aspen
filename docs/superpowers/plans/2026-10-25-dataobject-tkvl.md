# DataObject-backed TKVL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a TKVL whose nodes (every tier) are DataObjects, sharing one implementation of tree navigation/split/join logic with the existing KeyValue-object tree.

**Architecture:** Introduce a kind-agnostic `ListNode` view and a pluggable `NodeEncoding` (read, size, allocate, stage insert/delete/split/join/splitAt/clear). The existing KeyValue code becomes `KeyValueNodeEncoding` with no behavior change; the tree orchestration in `TieredKeyValueList` and the split/join finalization actions are retargeted onto the interface; a new `DataObjectNodeEncoding` plus a `DataObjectTKVL` facade provide the new tree kind. `Root` records a `TreeKind`.

**Tech Stack:** Scala 3 (quiet syntax, indented if/then/else), sbt, munit-style suites already used under `src/test/scala/org/aspen_ddp/aspen/client/tkvl`.

**Spec:** `docs/superpowers/specs/2026-10-25-dataobject-tkvl-design.md`

## Global Constraints

- Backward compatibility with persisted data is not required (`Root` format may change freely).
- The KeyValue-object tree keeps its behavior and public signatures (`TieredKeyValueList`, `TieredKeyValueListNode`, `KeyValueListNode`, `ValueState` callbacks): StoreRepairer, RebuildingStore, cmdline Main and amoebafs compile unchanged apart from the `Root`/`RootManager` signature widening in Task 1.
- Never mix kinds in one tree. Whole-node revision requirement is the only concurrency control for DataObject nodes. No per-key revisions/timestamps for the DataObject kind.
- `getContainingNode` / `TieredKeyValueListNode` stay KeyValue-only.
- Node size limits must hold for DataObject nodes: `maxNodeSize` is compared against encoded object bytes and `NodeAllocator` must reject tiers above the pool's object limit.
- Test priorities: correctness, no flaky tests, speed. Run suites with a 40 second timeout, e.g. `timeout 40 sbt 'testOnly *TKVLSuite'`. Use os-lib, Scaffeine as per CLAUDE.md; prefer Scala 3 quiet syntax.
- `Todo.txt` has uncommitted user edits: stage only the hunk this plan changes (`git add -p`).

## Review Focus

1. Empty DataObject tree: `get`, `delete`, `foreach`, `foldLeft` on a never-written tree return empty/no-op, not errors.
2. Deleting the last key of a non-last DataObject node joins it with its right neighbour and the tree stays walkable.
3. A single entry too large for `maxNodeSize` fails with `NodeSizeExceeded` before anything is written, as in the KV tree.
4. Two `set`/`delete` calls hitting one node in one transaction fail with a clear error instead of staging two updates.
5. Decoding a `Root` whose pointer type disagrees with its `TreeKind` fails; DataObject trees also work on Replication and classic ReedSolomon pools (whole-object writes only).

---

## File Structure

- Create `client/tkvl/TreeKind.scala`: `enum TreeKind { KeyValueObjects, DataObjects }` with `code: Byte` / `fromCode`.
- Create `client/tkvl/ListNode.scala`: `ListNode` trait, `NodeLink`.
- Create `client/tkvl/NodeEncoding.scala`: `NodeEncoding` trait and `SplitPlan` (pure split-point choice shared by both encodings).
- Create `client/tkvl/KeyValueNodeEncoding.scala`: delegates to existing `KeyValueListNode` statics.
- Create `client/tkvl/DataObjectNodeEncoding.scala` and `DataObjectListNode.scala`: the new encoding/node.
- Create `client/tkvl/DataObjectTKVL.scala`: public facade for the DataObject kind.
- Modify `Root.scala`, `RootManager.scala`, `KVObjectRootManager.scala`, `NodeAllocator.scala`, `TieredKeyValueList.scala`, `Split/JoinFinalizationAction.scala`, `KeyValueListNode.scala`, and the two amoebafs root managers (signature widening only).
- Tests under `src/test/scala/org/aspen_ddp/aspen/client/tkvl/`: new `RootKindSuite`, `SplitPlanSuite`, `DataObjectNodeEncodingSuite`, `DataObjectTKVLSuite`, `TreeKindParitySuite`.

(Paths below are relative to `src/main/scala/org/aspen_ddp/aspen/` unless starting with `src/test`.)

---

### Task 1: TreeKind in Root, ObjectPointer root

**Files:** Create `client/tkvl/TreeKind.scala`; Modify `client/tkvl/Root.scala`, `client/tkvl/RootManager.scala`, `client/tkvl/KVObjectRootManager.scala`, `amoebafs/impl/simple/SimpleDirectoryRootManager.scala`, `SimpleFileRootManager.scala`, `SimpleFileContent.scala`, `SimpleDirectory.scala`; Test `src/test/.../client/tkvl/RootKindSuite.scala`.

**Interfaces:**
- Produces: `enum TreeKind` (`KeyValueObjects`, `DataObjects`); `Root(tier: Int, ordering: KeyOrdering, orootObject: Option[ObjectPointer], nodeAllocator: NodeAllocator, kind: TreeKind = TreeKind.KeyValueObjects)`; `RootManager.getRootNode(): Future[(Int, KeyOrdering, Option[ListNode])]` (ListNode arrives in Task 2; until then keep `KeyValueListNode` and widen in Task 2); `RootManager.prepareRootUpdate(newTier: Int, onewRoot: Option[ObjectPointer])`; `Root.create(..., kind: TreeKind = KeyValueObjects)`.

- [ ] **Step 1: Write failing tests in `RootKindSuite`**: (a) encode/decode round-trips a `Root` of each kind with and without `orootObject`; (b) decoding bytes where kind=`DataObjects` but the pointer bytes are a `KeyValueObjectPointer` throws.
- [ ] **Step 2: Run** `timeout 40 sbt 'testOnly *RootKindSuite'`; expect compile failure/FAIL.
- [ ] **Step 3: Implement** `TreeKind` and the `Root` change: kind byte written after `ordering.code`; pointer decoded by kind (`DataObjectPointer(bb)` / `KeyValueObjectPointer(bb)`). Add `Root.rootKeyValuePointer: Option[KeyValueObjectPointer]` for the KV-only callers and widen `prepareRootUpdate` in the three managers; fix the four amoebafs call sites.
- [ ] **Step 4: Run** `timeout 40 sbt 'testOnly *RootKindSuite'` then `sbt compile test:compile`; expect PASS and clean compile.
- [ ] **Step 5: Run the existing suites** `timeout 40 sbt 'testOnly org.aspen_ddp.aspen.client.tkvl.* org.aspen_ddp.aspen.amoebafs.*'`; expect PASS. Commit: `git commit -m "Add TreeKind to TKVL Root"`.

### Task 2: ListNode, SplitPlan

**Files:** Create `client/tkvl/ListNode.scala`, `client/tkvl/NodeEncoding.scala` (SplitPlan only for now); Modify `KeyValueListNode.scala`, `RootManager.scala`; Test `SplitPlanSuite.scala`.

**Interfaces:**
- Produces: `final case class NodeLink(minimum: Key, pointer: ObjectPointer)`; `trait ListNode { def pointer: ObjectPointer; def minimum: Key; def maximum: Option[Key]; def link: Option[NodeLink]; def ordering: KeyOrdering; def revision: ObjectRevision; def refcount: ObjectRefcount; def entries: List[(Key, Value)] /* ascending */; def get(key: Key): Option[Value]; def size: Int; def keyInRange(key: Key): Boolean }`; `KeyValueListNode extends ListNode` (`link` maps `tail`); `object SplitPlan { def keysToMove(sizes: List[(Key, Int)]): List[Key] }` returning the keys that move to the new right node, reproducing the `halfSize` loop currently duplicated in `KeyValueListNode.insert` and `rename` (always moves at least the last key).
- `RootManager.getRootNode` now returns `Option[ListNode]`.

- [ ] **Step 1: Write `SplitPlanSuite`**: given sizes for keys a..e of 10 each, `keysToMove` returns the set `{d, e}`-style upper half (assert exact keys equal what the old loop produced; compute the expectation by hand from the loop in `KeyValueListNode.insert`); a single-key input moves that key; two keys move the last one.
- [ ] **Step 2: Run** `timeout 40 sbt 'testOnly *SplitPlanSuite'`; expect FAIL.
- [ ] **Step 3: Implement** `SplitPlan.keysToMove`, the `ListNode` trait, `KeyValueListNode extends ListNode`; replace both copies of the halfSize loop in `KeyValueListNode` with `SplitPlan.keysToMove`.
- [ ] **Step 4: Run** `timeout 40 sbt 'testOnly org.aspen_ddp.aspen.client.tkvl.* org.aspen_ddp.aspen.amoebafs.*'`; expect PASS (behavior unchanged). Commit.

### Task 3: NodeEncoding interface and KeyValueNodeEncoding

**Files:** Modify `client/tkvl/NodeEncoding.scala`; Create `client/tkvl/KeyValueNodeEncoding.scala`; Modify `client/tkvl/NodeAllocator.scala`; Test `src/test/.../NodeAllocatorLimitSuite.scala` (extend).

**Interfaces:**
- Produces `trait NodeEncoding`:
  - `def kind: TreeKind`
  - `def read(reader: ObjectReader, pointer: ObjectPointer, minimum: Key, ordering: KeyOrdering, comment: String): Future[ListNode]` (also used for `refresh`)
  - `def insert(node: ListNode, key: Key, value: Value, maxNodeSize: Int, allocator: ObjectAllocator, prepareForSplit: (Key, ObjectPointer) => Future[Unit], requirement: Option[Either[Boolean, ObjectRevision]])(using Transaction): Future[Unit]` (DataObject encoding rejects a defined `requirement` with `UnsupportedRequirement`)
  - `def delete(node: ListNode, key: Key, prepareForJoin: (Key, ObjectPointer) => Future[Unit])(using Transaction): Future[Unit]`
  - `def splitAt(node: ListNode, splitAtKey: Key, inclusive: Boolean, optionalDownPointer: Option[ObjectPointer], allocator: ObjectAllocator)(using Transaction): Future[NodeLink]`
  - `def allocate(allocator: ObjectAllocator, entries: Map[Key, Value])(using Transaction): Future[ObjectPointer]` (first node of a tree)
  - `def deleteList(client: AspenClient, head: NodeLink, ordering: KeyOrdering, odeleteEntry: Option[(Key, Value) => Future[Unit]]): Future[Unit]`
  - `def nodeSizeLimit(ida: IDA, maxSliceSize: Option[Int]): Option[Int]`
  - `object NodeEncoding { def apply(kind: TreeKind): NodeEncoding }`
- `NodeAllocator.nodeSizeLimit(ida, maxSliceSize, kind)`; for `DataObjects` it is `ida.maxObjectSize(maxSliceSize)` (object bytes), for KV unchanged. `TierAllocator.createDataObjectPointer` must check the node size when the tree kind is `DataObjects`. `NodeAllocator` gains a `kind: TreeKind` constructor parameter defaulting to KV, encoded in `Root`, not in the allocator bytes.

- [ ] **Step 1: Write failing tests** in `NodeAllocatorLimitSuite`: for each kind, a tier whose `maxNodeSize` exceeds the pool limit makes `validate()` fail with `NodeSizeExceedsAllocatorLimit`; for `DataObjects` the limit is `ida.maxObjectSize(maxSliceSize)` and an allocation of a data object in an over-limit pool fails likewise.
- [ ] **Step 2: Run** `timeout 40 sbt 'testOnly *NodeAllocatorLimitSuite'`; expect FAIL.
- [ ] **Step 3: Implement** the trait; `KeyValueNodeEncoding` delegates every method to the existing `KeyValueListNode` statics (cast `ListNode` to `KeyValueListNode`; `allocate` is `allocateKeyValueObject`); kind-aware limit in `NodeAllocator`.
- [ ] **Step 4: Run** `timeout 40 sbt 'testOnly org.aspen_ddp.aspen.client.tkvl.*'`; expect PASS. Commit.

### Task 4: Retarget orchestration onto NodeEncoding

Split into three commits; the existing suites must pass after each.

**Files:** Modify `client/tkvl/TieredKeyValueList.scala`, `SplitFinalizationAction.scala`, `JoinFinalizationAction.scala`, `KVObjectRootManager.scala`; tests: existing `TKVLSuite`, `KeyValueListSuite`, `TieredKeyValueListSplitTreeSuite`, `KeyValueListNodeSplitAtSuite`, amoebafs suites.

**Interfaces:**
- Consumes: Task 3 `NodeEncoding`.
- Produces: package-private `class TierEngine(client: AspenClient, rootManager: RootManager)` holding the current generic bodies (`get`, `set`, `delete`, `foldLeft`, `walkFrom`, `splitTree`, `deleteTree`, `fetchContainingNodePath`/`descend`/`fetchNode`) typed on `ListNode`, choosing `NodeEncoding(root.kind)`; `TieredKeyValueList` becomes the KV-facing facade over it with unchanged public signatures (it casts nodes to `KeyValueListNode`); `Split/JoinFinalizationAction.addToTransaction(mgr, tier, minimum, node: ObjectPointer, tx)` with the tree kind written into the payload so the factory decodes `DataObjectPointer`/`KeyValueObjectPointer`; `KVObjectRootManager` reads the root node via `NodeEncoding(root.kind).read`.

- [ ] **Step 1 (read side):** move descent and read-only operations (`get`, `foldLeft`, `foreach*`, `getContainingNode`) into `TierEngine`. Run the full tkvl + amoebafs suites; expect PASS. Commit.
- [ ] **Step 2 (write side):** move `set`, both `delete`s, root creation and the Split/Join finalization actions. Run the same suites; expect PASS. Commit.
- [ ] **Step 3 (whole-tree ops):** move `splitTree`, `deleteTree`. Run the same suites plus `*MigratePoolToSetSuite *RebalancingServiceSuite`; expect PASS. Commit.

Each step: the KV-only `delete(key, requiredRevision, requirements)` and `TieredKeyValueListNode` stay in the facade and reach `KeyValueListNode` directly.

### Task 5: DataObject encoding

**Files:** Create `client/tkvl/DataObjectNodeEncoding.scala`, `client/tkvl/DataObjectListNode.scala`; Test `DataObjectNodeEncodingSuite.scala`.

**Interfaces:**
- Consumes: Task 3 trait, `SplitPlan`. Produces `DataObjectNodeEncoding extends NodeEncoding` (`kind = DataObjects`), `DataObjectListNode extends ListNode` backed by a decoded `DataObjectState`.
- Byte layout is the implementer's choice; requirements: self-contained (minimum, optional max/link, entries sorted ascending), varint lengths, no per-entry revision/timestamp, and a pure `encode(entries, minimum, link): Array[Byte]` / `decode` pair plus `encodedSize(entries, minimum, link)` that matches `encode(...).length` exactly.
- Writes use `tx.overwrite(pointer, node.revision, DataBuffer(bytes))` (whole-node rewrite, revision-guarded). Join: rewrite the left node with the right node's entries and link, `tx.setRefcount(rightPointer, rightRefcount, rightRefcount.decrement())` and lock the right revision (`tx.lockRevision`), as the KV join does with its content lock.

- [ ] **Step 1: Write failing tests** (use the in-memory test network helpers of `KeyValueListSuite`): `encode`/`decode` round-trip incl. empty entries, empty value, large key; `encodedSize == encode.length` for random entries; insert into a node under `maxNodeSize` rewrites it; insert that overflows splits per `SplitPlan` and calls `prepareForSplit` with the new node's minimum; deleting the last key of a node with a right link joins; `insert` with a defined `requirement` throws `UnsupportedRequirement`; one entry larger than the node limit throws `NodeSizeExceeded` with nothing staged.
- [ ] **Step 2: Run** `timeout 40 sbt 'testOnly *DataObjectNodeEncodingSuite'`; expect FAIL.
- [ ] **Step 3: Implement** the encoding, including `splitAt` (mirrors the two-node scheme documented in `KeyValueListNode.splitAt`) and `deleteList`. `nodeSizeLimit` is `ida.maxObjectSize(s)`.
- [ ] **Step 4: Run** the suite; expect PASS. Commit.

### Task 6: DataObjectTKVL facade, creation, same-node guard

**Files:** Create `client/tkvl/DataObjectTKVL.scala`; Modify `client/tkvl/KVObjectRootManager.scala` (`createNewTree` takes `kind: TreeKind = KeyValueObjects`), `client/tkvl/Root.scala` (`Root.create(kind)`); Test `DataObjectTKVLSuite.scala`.

**Interfaces:**
- Produces: `class DataObjectTKVL(client: AspenClient, rootManager: RootManager)` with `get(key): Future[Option[Value]]`, `set(key, value)(using Transaction): Future[Unit]`, `delete(key)(using Transaction): Future[Unit]`, `foldLeft[B](z: B)(fn: (B, Map[Key, Value]) => B)`, `foreach(fn: (Key, Value) => Future[Unit])`, `foreachFrom(minKey, fn)`, `foreachInRange(minKey, maxKey, fn)`, `splitTree(splitAtKey, inclusive)(using Transaction): Future[Root]`, `deleteTree(odeleteEntry: Option[(Key, Value) => Future[Unit]])`. Constructing it over a root whose kind is not `DataObjects` throws `WrongTreeKind`.
- Same-node guard: an encoding write to a node already updated in the same `Transaction` throws `NodeAlreadyModified(pointer)` (first check whether the transaction implementation already rejects duplicate updates and, if so, translate that error; otherwise track updated `ObjectId`s per transaction).

- [ ] **Step 1: Write failing tests**: create an empty DataObject tree, `get` on it is `None` and `delete` is a no-op; set/get/delete on single node; set enough keys to force splits and tier growth then read all back in order via `foldLeft` and `foreach`; `foreachFrom`/`foreachInRange` bounds; `splitTree` keeps `< key` on the left; `deleteTree` removes all node objects; two `set` calls hitting the same node in one transaction throw `NodeAlreadyModified`; opening a KV-kind root as `DataObjectTKVL` throws `WrongTreeKind`.
- [ ] **Step 2: Run** `timeout 40 sbt 'testOnly *DataObjectTKVLSuite'`; expect FAIL.
- [ ] **Step 3: Implement** the facade over `TierEngine` (values only, no `ValueState`), the `kind` parameters, and the guard.
- [ ] **Step 4: Run** the suite; expect PASS. Commit.

### Task 7: Parity tests and compactness

**Files:** Create `src/test/.../TreeKindParitySuite.scala`.

- [ ] **Step 1: Write the suite**: one shared body run for both kinds (set/get/delete, splits, joins, tier shrink, `splitTree`, `deleteTree`, interleaved random operations against an in-memory `TreeMap` model with a fixed seed); on Replication and classic ReedSolomon pools; a compactness test asserting the DataObject tree's total node bytes for N fixed entries is smaller than the KV tree's.
- [ ] **Step 2: Run** `timeout 40 sbt 'testOnly *TreeKindParitySuite'`; fix any defects found in earlier tasks (each with its own test) until PASS.
- [ ] **Step 3: Run the whole tkvl and amoebafs suites** once more with `timeout 40` each; expect PASS. Commit.

### Task 8: Docs

**Files:** Modify `CLAUDE.md` (one bullet under Key Design Aspects describing `TreeKind`, `NodeEncoding` and the DataObject tree limits), `Todo.txt` (remove the top item only, via `git add -p`).

- [ ] **Step 1:** make both edits; **Step 2:** `git add -p Todo.txt CLAUDE.md` and commit `Document DataObject-backed TKVL`.
