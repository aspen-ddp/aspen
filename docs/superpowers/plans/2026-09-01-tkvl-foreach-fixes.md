# TKVL foreach Fixes and foreachFrom Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `KeyValueListNode.foreach` and `foreachInRange`, which visit the wrong contents once a tier-0 list spans more than one node, and add a `foreachFrom(minKey, fn)` variant for resuming a walk partway through.

**Architecture:** Three defects and one addition, all in `KeyValueListNode`. `TieredKeyValueList.foreach`/`foreachInRange` only descend to the containing tier-0 node and delegate, so they need no fix — but they do need a `foreachFrom` sibling. Tests come first: the existing suite cannot catch any of this because its trees never split.

**Tech Stack:** Scala 3, ScalaTest (`AsyncFunSuite` via `IntegrationTestSuite`), sbt.

**Spec:** `docs/superpowers/specs/2026-09-01-failed-store-rebuild-design.md` (§6)

## Global Constraints

- Scala 3 "quiet mode" syntax preferred. Braces still used where they clearly enhance readability. `KeyValueListNode.scala` is written in brace style — match the surrounding code rather than converting it.
- Prefer indented `if/then/else`. Avoid single-line `if a == 1 then value` unless it enhances readability.
- Run a single test by substring: `sbt 'testOnly *TestName -- -z "test substring"'`
- Every commit message ends with `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- Do NOT change the fact that `foreach`/`foreachInRange` swallow a failing `fn` (log-and-continue). That behavior is relied upon elsewhere and is handled by the rebuild consumer explicitly. `foreachFrom` must swallow failures the same way, for consistency.

## Background: what is actually wrong

In `KeyValueListNode.foreach` (`src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala:154`), when a node's contents are exhausted and a tail exists, the code reads the next node, builds `nextNode` from it — and then sorts `node.contents`, the node it just finished, rather than `nextNode.contents`:

```scala
val nextNode = new KeyValueListNode(reader, kvos.ida, kvos.pointer, ordering, nodeTail.minimum,
  kvos.revision, kvos.refcount, kvos.contents,
  kvos.right.map(v => KeyValueListPointer(v.bytes)))

val contents = node.contents.toList.sortWith((a,b) => ordering.compare(a._1, b._1) < 0)   // <-- node, should be nextNode

recurse(nextNode, contents)
```

Consequences on a three-node list holding A, B, C:
- A's contents are visited twice (once paired with node A, once paired with node B),
- B's contents are visited once but paired with node C,
- C's contents are never visited at all.

`foreachInRange` (line 195) repeats the same slip, and adds a second: its termination test is

```scala
if ordering.compare(maxKey, minimum) < 0 then
```

`minimum` here is the *current* node's minimum, not the minimum of the node about to be read. The correct comparison is against `nodeTail.minimum`, and it should be `<= 0` rather than `< 0` — the range is `[minKey, maxKey)`, so a next node whose minimum equals `maxKey` contains nothing in range.

The existing `TKVLSuite` test "Many inserts and foreach" (line 93) inserts 100 keys with a 1-byte value. The tree never splits, `tail` is `None`, and the entire buggy branch is dead. Every test in this plan therefore forces a split first and asserts that it happened.

## File Structure

| File | Responsibility |
|---|---|
| `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala` | Tier-0 node walk. Holds all three defects; gains `foreachFrom`. |
| `src/main/scala/org/aspen_ddp/aspen/client/tkvl/TieredKeyValueList.scala` | Descends the tiers to the containing tier-0 node and delegates. Gains a `foreachFrom` sibling to `foreach`. |
| `src/test/scala/org/aspen_ddp/aspen/client/tkvl/TKVLSuite.scala` | Integration tests over a real `TestNetwork`. Gains four splitting-tree tests. |

## Shared test helper

Tasks 1, 3, and 5 each need a multi-node tier-0 list. The suite has no such helper today. Task 1 adds one and the later tasks reuse it, so it is written once, here, and referenced by name afterwards.

Splits are forced with 512 KiB values — this is exactly how the existing "Splitting tree insertion" test (`TKVLSuite.scala:138`) does it. Inserts are sequential rather than via `Future.sequence` so the resulting node boundaries are deterministic.

---

### Task 1: Prove `foreach` is broken on a split tree, then fix it

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala:181` (the `val contents =` line inside `foreach`'s `Success(kvos)` branch)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/tkvl/TKVLSuite.scala` (append before the closing brace of the class)

**Interfaces:**
- Consumes: nothing.
- Produces: `buildSplitTree(numKeys: Int): Future[(TieredKeyValueList, KVObjectRootManager, List[Key])]` — a private helper in `TKVLSuite` returning the tree, its root manager, and the inserted keys in ascending order. Tasks 3 and 5 call it.

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/org/aspen_ddp/aspen/client/tkvl/TKVLSuite.scala`, inside the class, after the last existing test:

```scala
  /** Builds a tier-0 list guaranteed to span several nodes.
   *
   *  Splits are forced with 512 KiB values, the same lever the "Splitting tree insertion"
   *  test uses. Inserts are sequential rather than concurrent so the node boundaries are
   *  deterministic from run to run.
   *
   *  Returns the tree, its root manager (needed to re-read the root after splits), and the
   *  inserted keys in ascending order.
   */
  private def buildSplitTree(numKeys: Int): Future[(TieredKeyValueList, KVObjectRootManager, List[Key])] =
    val treeKey = Key(Array[Byte](0))
    val bigValue = Value(new Array[Byte](512 * 1024))
    val keys = (1 to numKeys).map(i => Key(i)).toList

    def insertSequentially(tree: TieredKeyValueList, remaining: List[Key]): Future[Unit] =
      remaining match
        case Nil => Future.unit
        case key :: rest =>
          val tx: Transaction = client.newTransaction()
          for
            _ <- tree.set(key, bigValue)(using tx)
            _ <- tx.commit()
            _ <- waitForTransactionsToComplete()
            _ <- insertSequentially(tree, rest)
          yield ()

    for
      ikvos <- client.read(radicle)
      pool <- client.getStoragePool(Radicle.poolId)
      alloc = pool.allocator

      tx0 = client.newTransaction()
      ptr <- alloc.allocateKeyValueObject()(using tx0)
      _ = tx0.lockRevision(radicle, ikvos.revision)
      _ <- tx0.commit()
      _ <- waitForTransactionsToComplete()

      nodeAllocator = new SinglePoolNodeAllocator(client, Radicle.poolId)
      tx1 = client.newTransaction()
      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, IntegerKeyOrdering,
        nodeAllocator, Map())(using tx1)
      _ <- tx1.commit()
      _ <- waitForTransactionsToComplete()

      root <- froot
      tree <- root.getTree()
      _ <- insertSequentially(tree, keys)

      // Re-read: splits replace the root, so the tree handle from before the inserts is stale.
      freshTree <- root.getTree()
    yield
      (freshTree, root, keys)

  atest("foreach visits every key exactly once on a split tree") {
    var visits = List[(Key, Boolean)]()

    def record(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      // keyInRange is the check that catches the wrong-node pairing: a key handed to the
      // caller with a node that does not own it is exactly the bug.
      visits = (key, node.keyInRange(key)) :: visits
      Future.unit

    for
      (tree, root, keys) <- buildSplitTree(8)
      (numTiers, _, _) <- root.getRootNode()
      _ <- tree.foreach(record)
    yield
      // Guard the guard: if the tree never split, this test proves nothing.
      numTiers should be >= 1

      val visitedKeys = visits.map(_._1)
      visitedKeys.distinct.sortBy(k => keys.indexOf(k)) should be (keys)
      visitedKeys.length should be (keys.length)
      visits.filterNot(_._2) should be (Nil)
  }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *TKVLSuite -- -z "foreach visits every key exactly once"'`

Expected: FAIL. The precise assertion that trips depends on how the 8 keys distribute across nodes, but it will be one of: `visitedKeys.length` exceeding `keys.length` (the first node is walked twice), the distinct-keys comparison missing the final node's keys, or `visits.filterNot(_._2)` being non-empty (keys paired with a node that does not own them).

If instead the test fails on `numTiers should be >= 1`, the tree did not split — raise the key count in `buildSplitTree(8)` until it does, and use the same count in Tasks 3 and 5.

- [ ] **Step 3: Fix `foreach`**

In `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala`, inside `foreach`'s `case Success(kvos) =>` branch, change the one word `node` to `nextNode`:

```scala
                  val nextNode = new KeyValueListNode(reader, kvos.ida, kvos.pointer, ordering, nodeTail.minimum,
                    kvos.revision, kvos.refcount, kvos.contents,
                    kvos.right.map(v => KeyValueListPointer(v.bytes)))

                  val contents = nextNode.contents.toList.sortWith((a,b) => ordering.compare(a._1, b._1) < 0)

                  recurse(nextNode, contents)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *TKVLSuite -- -z "foreach visits every key exactly once"'`
Expected: PASS

- [ ] **Step 5: Run the whole TKVL suite for regressions**

Run: `sbt 'testOnly *TKVLSuite'`
Expected: PASS, including the pre-existing "Many inserts and foreach" test.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala \
        src/test/scala/org/aspen_ddp/aspen/client/tkvl/TKVLSuite.scala
git commit -m "$(cat <<'EOF'
fix: walk the next node's contents in KeyValueListNode.foreach

foreach re-sorted the contents of the node it had just finished instead of
the one it had just read. On a multi-node tier-0 list that visited the first
node's contents twice, paired every subsequent node's contents with the wrong
node, and never visited the last node's contents at all.

The existing coverage could not catch it: the suite's trees hold 100 one-byte
values, never split, and so never take the branch. The new test forces a split
with 512 KiB values and asserts both that every key is visited exactly once
and that each key arrives paired with a node that owns it.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Fix `foreachInRange`'s node pairing

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala:227-229` (the `val contents = node.contents.filter:` block inside `foreachInRange`)
- Test: covered by Task 3. This task is split from Task 3 only because the two `foreachInRange` defects are independent and a reviewer could accept one and reject the other; if you are executing straight through, Tasks 2 and 3 can be committed together.

**Interfaces:**
- Consumes: nothing.
- Produces: nothing new. Behavior change only.

- [ ] **Step 1: Apply the fix**

In `foreachInRange`'s `case Success(kvos) =>` branch, change `node.contents` to `nextNode.contents`:

```scala
                    val nextNode = new KeyValueListNode(reader, kvos.ida, kvos.pointer, ordering, nodeTail.minimum,
                      kvos.revision, kvos.refcount, kvos.contents,
                      kvos.right.map(v => KeyValueListPointer(v.bytes)))

                    val contents = nextNode.contents.filter:
                          tpl => ordering.compare(tpl._1, minKey) >= 0 && ordering.compare(tpl._1, maxKey) < 0
                        .toList.sortWith((a, b) => ordering.compare(a._1, b._1) < 0)

                    recurse(nextNode, contents)
```

- [ ] **Step 2: Verify it compiles**

Run: `sbt compile`
Expected: success, no new warnings.

- [ ] **Step 3: Do not commit yet**

Task 3 supplies this fix's test and its second half. Commit them together at the end of Task 3.

---

### Task 3: Fix `foreachInRange`'s termination test, with a boundary-spanning test

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala:217` (the `if ordering.compare(maxKey, minimum) < 0 then` line)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/tkvl/TKVLSuite.scala`

**Interfaces:**
- Consumes: `buildSplitTree` from Task 1.
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

Append to `TKVLSuite`, after the test added in Task 1:

```scala
  atest("foreachInRange spans node boundaries and honors the half-open range") {
    var visits = List[(Key, Boolean)]()

    def record(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      visits = (key, node.keyInRange(key)) :: visits
      Future.unit

    for
      (tree, root, keys) <- buildSplitTree(8)
      (numTiers, _, _) <- root.getRootNode()
      // Keys are 1..8 across several nodes; [3, 7) must yield exactly 3, 4, 5, 6 regardless
      // of where the node boundaries happen to fall.
      _ <- tree.foreachInRange(Key(3), Key(7), record)
    yield
      numTiers should be >= 1

      val visitedKeys = visits.map(_._1)
      visitedKeys.length should be (4)
      visitedKeys.toSet should be (Set(Key(3), Key(4), Key(5), Key(6)))
      visits.filterNot(_._2) should be (Nil)
  }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt 'testOnly *TKVLSuite -- -z "foreachInRange spans node boundaries"'`

Expected: FAIL. With Task 2's fix applied but the termination test still comparing against the current node's `minimum`, the walk stops at the wrong node and `visitedKeys` comes back short of the four expected keys.

If you skipped Task 2, expect a different failure — duplicated and mispaired keys — which the same assertions catch.

- [ ] **Step 3: Fix the termination test**

In `foreachInRange`, change the comparison to use the minimum of the node about to be read, and to be inclusive:

```scala
            case Some(nodeTail) =>
              // nodeTail.minimum is the minimum key of the node we are about to read. The
              // range is [minKey, maxKey), so a next node whose minimum is at or above maxKey
              // holds nothing in range and neither does anything to its right.
              if ordering.compare(maxKey, nodeTail.minimum) <= 0 then
                p.success(())
              else
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt 'testOnly *TKVLSuite -- -z "foreachInRange spans node boundaries"'`
Expected: PASS

- [ ] **Step 5: Run the whole TKVL suite**

Run: `sbt 'testOnly *TKVLSuite'`
Expected: PASS

- [ ] **Step 6: Commit Tasks 2 and 3 together**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala \
        src/test/scala/org/aspen_ddp/aspen/client/tkvl/TKVLSuite.scala
git commit -m "$(cat <<'EOF'
fix: correct node pairing and termination in foreachInRange

Two defects, both invisible on a single-node tier-0 list. foreachInRange
filtered the contents of the node it had just finished rather than the one it
had just read, the same slip foreach had. Separately, its termination test
compared maxKey against the current node's minimum instead of the minimum of
the node about to be read, so the walk stopped at the wrong node.

The comparison is now <= against nodeTail.minimum: the range is half-open, so
a next node starting at maxKey holds nothing in range.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Add `KeyValueListNode.foreachFrom`

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala` (add after `foreachInRange`, before the closing `}` of the class)

**Interfaces:**
- Consumes: nothing.
- Produces: `KeyValueListNode.foreachFrom(minKey: Key, fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit]` — visits every entry at or above `minKey`, from this node rightward. Task 5 wraps it; the rebuild consumer (a later plan) calls the wrapper.

**Why this exists rather than reusing `foreachInRange`:** a resume needs "from this key to the end of the tree." Keys are arbitrary-length byte arrays, so there is no maximum key to pass as the upper bound — `Key.AbsoluteMinimum` exists but there is deliberately no `Key.AbsoluteMaximum`.

- [ ] **Step 1: Write the implementation**

Add to `KeyValueListNode`, immediately after `foreachInRange`:

```scala
  /** Visit every entry at or above `minKey`, from this node rightward.
   *
   *  The open-ended counterpart to foreachInRange: keys are arbitrary-length byte arrays, so
   *  there is no maximum key that could serve as an upper bound.
   *
   *  Every node's contents are filtered, not just the head node's. When reached through
   *  TieredKeyValueList.foreachFrom the filter is a no-op past the first node, but filtering
   *  unconditionally keeps this correct when called directly on a node to the left of minKey.
   *
   *  As with foreach and foreachInRange, a failing `fn` is logged and the walk continues.
   */
  def foreachFrom(minKey: Key,
                  fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =
    val p = Promise[Unit]()

    def inRange(node: KeyValueListNode): List[(Key, ValueState)] =
      node.contents.filter(tpl => ordering.compare(tpl._1, minKey) >= 0)
        .toList.sortWith((a, b) => ordering.compare(a._1, b._1) < 0)

    def recurse(node: KeyValueListNode, contents: List[(Key, ValueState)]): Unit = {
      contents.headOption match
        case Some((key, value)) =>
          fn(node, key, value) onComplete:
            case Failure(err) =>
              logger.error(f"Failure in KeyValueListNode.foreachFrom: $err", err)
              recurse(node, contents.tail)
            case Success(_) =>
              recurse(node, contents.tail)

        case None =>
          node.tail match
            case None =>
              p.success(())

            case Some(nodeTail) =>
              reader.read(nodeTail.pointer, s"foreachFrom() KVListNode node ${pointer.id}. Minimum: $minimum.") onComplete {

                case Failure(err) =>
                  p.failure(err)

                case Success(kvos) =>
                  val nextNode = new KeyValueListNode(reader, kvos.ida, kvos.pointer, ordering, nodeTail.minimum,
                    kvos.revision, kvos.refcount, kvos.contents,
                    kvos.right.map(v => KeyValueListPointer(v.bytes)))

                  recurse(nextNode, inRange(nextNode))
              }
    }

    recurse(this, inRange(this))

    p.future
```

- [ ] **Step 2: Verify it compiles**

Run: `sbt compile`
Expected: success.

- [ ] **Step 3: Do not commit yet**

Task 5 supplies the wrapper and the test. Commit them together.

---

### Task 5: Add `TieredKeyValueList.foreachFrom`, with a resume test

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/TieredKeyValueList.scala` (add after `foreachInRange`, which ends at line 316)
- Test: `src/test/scala/org/aspen_ddp/aspen/client/tkvl/TKVLSuite.scala`

**Interfaces:**
- Consumes: `KeyValueListNode.foreachFrom` from Task 4; `buildSplitTree` from Task 1.
- Produces: `TieredKeyValueList.foreachFrom(minKey: Key, fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit]`. This is the entry point the rebuild consumer uses to resume a walk from its checkpoint.

- [ ] **Step 1: Write the failing test**

Append to `TKVLSuite`, after the test added in Task 3:

```scala
  atest("foreachFrom resumes from a key inside a later node") {
    var visits = List[(Key, Boolean)]()

    def record(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      visits = (key, node.keyInRange(key)) :: visits
      Future.unit

    for
      (tree, root, keys) <- buildSplitTree(8)
      (numTiers, _, _) <- root.getRootNode()
      _ <- tree.foreachFrom(Key(5), record)
    yield
      numTiers should be >= 1

      val visitedKeys = visits.map(_._1)
      visitedKeys.length should be (4)
      visitedKeys.toSet should be (Set(Key(5), Key(6), Key(7), Key(8)))
      visits.filterNot(_._2) should be (Nil)
  }

  atest("foreachFrom below the first key visits the whole tree") {
    var visitedKeys = List[Key]()

    def record(node: KeyValueListNode, key: Key, vs: ValueState): Future[Unit] =
      visitedKeys = key :: visitedKeys
      Future.unit

    for
      (tree, root, keys) <- buildSplitTree(8)
      _ <- tree.foreachFrom(Key(0), record)
    yield
      visitedKeys.length should be (keys.length)
      visitedKeys.toSet should be (keys.toSet)
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt 'testOnly *TKVLSuite -- -z "foreachFrom"'`
Expected: FAIL to compile — `value foreachFrom is not a member of TieredKeyValueList`.

- [ ] **Step 3: Add the wrapper**

Add to `TieredKeyValueList`, immediately after `foreachInRange`:

```scala
  /** Visit every entry at or above `minKey`. The open-ended counterpart to foreachInRange,
   *  used to resume an interrupted walk from a checkpointed key. */
  def foreachFrom(minKey: Key,
                  fn: (KeyValueListNode, Key, ValueState) => Future[Unit]): Future[Unit] =

    def nonEmpty(tier: Int, ordering: KeyOrdering, root: KeyValueListNode): Future[Unit] =
      for
        e <- fetchContainingNode(client, tier, 0, ordering, minKey, root, Set())
        node = e match
          case Left(_) => throw new BrokenTree()
          case Right(n) => n
        _ <- node.foreachFrom(minKey, fn)
      yield
        ()

    rootManager.getRootNode().flatMap: t =>
      val (tier, ordering, oroot) = t
      oroot match
        case None => Future.unit
        case Some(root) => nonEmpty(tier, ordering, root)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `sbt 'testOnly *TKVLSuite -- -z "foreachFrom"'`
Expected: PASS, both tests.

- [ ] **Step 5: Run the whole TKVL suite**

Run: `sbt 'testOnly *TKVLSuite'`
Expected: PASS

- [ ] **Step 6: Commit Tasks 4 and 5 together**

```bash
git add src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala \
        src/main/scala/org/aspen_ddp/aspen/client/tkvl/TieredKeyValueList.scala \
        src/test/scala/org/aspen_ddp/aspen/client/tkvl/TKVLSuite.scala
git commit -m "$(cat <<'EOF'
feat: add foreachFrom for resuming a TKVL walk partway through

foreachInRange cannot express "from this key to the end": keys are
arbitrary-length byte arrays, so there is no maximum key to pass as the upper
bound, and none is worth inventing. foreachFrom is the open-ended variant,
added for rebuild checkpoint resumption.

Every node's contents are filtered rather than just the head node's. Reached
through the tiered wrapper the filter is a no-op past the first node, but
filtering unconditionally keeps the node-level call correct when it starts to
the left of minKey.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Confirm the existing callers still behave

**Files:**
- Read only: `src/main/scala/org/aspen_ddp/aspen/compute/Registry.scala:35`, `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala:1034`
- Test: no new tests

**Interfaces:**
- Consumes: everything from Tasks 1-5.
- Produces: nothing.

The three existing callers of these functions were all getting less than they asked for. None of them can have depended on the broken behavior — a caller that silently missed the last node's entries and double-visited the first node's is not a contract anyone wrote against — but the suites that exercise them should be run to confirm nothing was relying on the shape of the bug.

- [ ] **Step 1: Run the suites covering the existing callers**

Run: `sbt 'testOnly *SystemTaskBootstrapSuite *TKVLSuite *MainSuite'`
Expected: PASS

- [ ] **Step 2: Run the full test suite**

Run: `sbt test`
Expected: PASS. If anything fails, check whether it fails on `main` too before assuming this plan caused it:

```bash
git stash && sbt test ; git stash pop
```

- [ ] **Step 3: Commit only if something needed adjusting**

If the full suite passed with no changes, there is nothing to commit and this task is done. If a test needed updating, commit it on its own:

```bash
git add <the adjusted test files>
git commit -m "$(cat <<'EOF'
test: adjust for corrected TKVL walk semantics

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

## Definition of Done

- `sbt test` passes.
- `KeyValueListNode.foreach`, `foreachInRange`, and `foreachFrom` each visit every in-range key exactly once on a tier-0 list spanning several nodes, and pair each key with a node that owns it.
- `TKVLSuite` contains at least one test that asserts the tree actually split (`numTiers should be >= 1`) before asserting anything about walk behavior — without that guard, a future change to the split threshold would silently turn these tests back into single-node tests that prove nothing.
- No change to the log-and-continue treatment of a failing `fn`.
