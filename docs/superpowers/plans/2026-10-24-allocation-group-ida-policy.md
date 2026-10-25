# Allocation Group IDA Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allocation groups require every member to have an IDA exactly equal to the group's, unless the group is created with the `AllowAll` policy.

**Architecture:** `AllocationGroupState` gains an immutable `idaPolicy` and `ida`. `addPool` / `addGroup` check `group.ida == candidate.ida` next to the existing `checkMemberLimit`, before any state changes. The IDA is supplied at group creation (client API and CLI, reusing `create-pool`'s IDA arguments).

**Tech Stack:** Scala 3 (quiet syntax, per `CLAUDE.md`), sbt, ScalaTest, protobuf (`src/main/protobuf/codec.proto`), scopt.

**Spec:** design agreed in conversation (no document): see Global Constraints.

## Global Constraints

- `enum IDAPolicy { ExactMatch, AllowAll }`, default `ExactMatch` at the client and CLI. Both `idaPolicy` and `ida` are fixed at creation.
- `ExactMatch` requires `ida = Some(_)`; `AllowAll` requires `ida = None`. Violations throw `IllegalArgumentException` at creation.
- Equality is `group.ida == candidate.ida` only. Never `IDA.compare` (it compares `failureTolerance` only).
- A child group's candidate IDA is its own `ida`. An `AllowAll` child (no `ida`) cannot join an `ExactMatch` parent. An `AllowAll` parent accepts anything.
- Mismatch throws `MemberIDAMismatch` before any state changes.
- Backward compatibility with persisted data is not required.
- CLI: `create-allocation-group <name> <level> [<ida-type> <width> <read-threshold> <write-threshold>] [--cell-size] [--allow-any-ida] [--max-slice-size]`. IDA args required unless `--allow-any-ida`; the two are mutually exclusive.
- Prefer indented `if/then/else`; Scala 3 quiet syntax.

## Review Focus

- Same IDA type, different width / thresholds / `cellSize` is rejected (Task 1 equality test, Task 2).
- `Replication(3,2)` vs `ReedSolomon(3,2,2)` is rejected (Task 1).
- `AllowAll` child into `ExactMatch` parent is rejected; `ExactMatch` child into `AllowAll` parent is accepted (Task 2).
- Rejected join leaves both group and pool states unchanged (Task 2).
- Creation with `ExactMatch` and no IDA, or `AllowAll` with an IDA, is rejected (Task 2, Task 3).

---

### Task 1: Model, encoding, equality guard

**Files:**
- Create: `src/main/scala/org/aspen_ddp/aspen/common/metadata/IDAPolicy.scala`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupState.scala` (case class, `MemberIDAMismatch`)
- Modify: `src/main/protobuf/codec.proto` (`AllocationGroupState`), `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala:1277-1294`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/ida/IDAEqualitySuite.scala` (create), `src/test/scala/org/aspen_ddp/aspen/common/network/CodecRoundTripSuite.scala:605`

**Interfaces:**
- Produces: `enum IDAPolicy { case ExactMatch, AllowAll }` (package `common.metadata`);
  `AllocationGroupState(..., maxSliceSize: Option[Int] = None, idaPolicy: IDAPolicy = IDAPolicy.AllowAll, ida: Option[IDA] = None)`;
  `AllocationGroupState.MemberIDAMismatch(memberId: UUID, memberIda: Option[IDA], groupIda: Option[IDA]) extends Exception`.

- [ ] **Step 1: Write `IDAEqualitySuite`** asserting `!=` for: `Replication(3,2)` vs `Replication(3,3)`; `Replication(3,2)` vs `Replication(5,2)`; `Replication(3,2)` vs `ReedSolomon(3,2,2)`; `ReedSolomon(5,3,4)` vs `ReedSolomon(5,3,5)`; `RandomAccessReedSolomon(5,3,4,4096)` vs `RandomAccessReedSolomon(5,3,4,8192)`; and `==` for identically constructed values of each type. Add a test showing `Replication(3,2).compare(Replication(3,3)) != 0` is not what `==` reports, i.e. that `compare` is unsuitable (document with a comment).
- [ ] **Step 2: Run** `sbt 'testOnly *IDAEqualitySuite'` — expect PASS (it pins existing behavior).
- [ ] **Step 3: Extend `CodecRoundTripSuite`** (`AllocationGroupState` test at line 605): round-trip an `ExactMatch` state with `ida = Some(RandomAccessReedSolomon(5,3,4,4096))` and an `AllowAll` state with `ida = None`; assert equality of `idaPolicy` and `ida` after `AllocationGroupState(bytes)`. Run it — expect compile FAIL.
- [ ] **Step 4: Implement.** Add `IDAPolicy.scala`; add the two fields to the case class (doc-comment them: fixed at creation, `ida` is `Some` iff `ExactMatch`); add `MemberIDAMismatch` beside `MemberLimitExceedsGroup` with a message naming both IDAs. In `codec.proto` add `enum AllocationGroupIDAPolicy { ALLOCATION_GROUP_IDA_POLICY_EXACT_MATCH = 0; ALLOCATION_GROUP_IDA_POLICY_ALLOW_ALL = 1; }` and fields `idaPolicy = 7`, `optional IDA ida = 8` on `AllocationGroupState`. Encode/decode in `Codec.scala` (follow the `encodeAllocationGroupMemberType` pattern; reuse `encode(o: IDA)` / `decode(m: codec.IDA)`).
- [ ] **Step 5: Run** `sbt 'testOnly *CodecRoundTripSuite *IDAEqualitySuite'` — expect PASS.
- [ ] **Step 6: Commit** `git add -A && git commit -m "Add IDAPolicy and IDA fields to AllocationGroupState"`

### Task 2: Creation and membership enforcement

**Files:**
- Modify: `client/AspenClient.scala:191`, `client/internal/BaseAspenClient.scala:321`, `common/metadata/AllocationGroupState.scala` (`checkMemberLimit` neighbor, `addPool`, `addGroup`) — all under `src/main/scala/org/aspen_ddp/aspen/`
- Test: `src/test/scala/org/aspen_ddp/aspen/common/metadata/AllocationGroupStateSuite.scala`; update every `createAllocationGroup` caller in `AllocationGroupStateSuite`, `GroupObjectAllocatorSuite`, `UpdateAllocationGroupUsageTaskSuite`

**Interfaces:**
- Consumes: Task 1 types.
- Produces: `AspenClient.createAllocationGroup(groupName: String, level: Int, maxSliceSize: Option[Int] = None, idaPolicy: IDAPolicy = IDAPolicy.ExactMatch, ida: Option[IDA] = None): Future[AllocationGroupId]`;
  `private def checkMemberIda(memberId: UUID, memberIda: Option[IDA], group: AllocationGroupState): Unit` (no-op when `group.idaPolicy == AllowAll`, else throws `MemberIDAMismatch` unless `memberIda == group.ida`).

- [ ] **Step 1: Write failing tests** in `AllocationGroupStateSuite` (use `readPoolState().ida` for the bootstrap pool's IDA; create a second pool with a different IDA via `client.createNewStoragePool` as other tests in the repo do, or assert against a group created with a different IDA):
  - `createAllocationGroup ExactMatch without ida fails` (`IllegalArgumentException`); `AllowAll with ida fails`.
  - `addPool accepts a pool whose IDA equals the group's`; `addPool rejects ... differing IDA` expecting `MemberIDAMismatch`, and group/pool states unchanged after the failure.
  - `addPool to an AllowAll group accepts any IDA`.
  - `addGroup accepts child with equal ida`; `rejects child with different ida`; `rejects AllowAll child into ExactMatch parent`; `accepts ExactMatch child into AllowAll parent`.
  - `ExactMatch group persists its ida` (read back with `readGroupState`).
- [ ] **Step 2: Run** `sbt 'testOnly *AllocationGroupStateSuite'` — expect compile/FAIL.
- [ ] **Step 3: Implement** creation validation and field population in `BaseAspenClient.createAllocationGroup` (throw `IllegalArgumentException` before the transaction); add `checkMemberIda`; call it in `addPool` with `Some(ps.ida)` and in `addGroup` with `child.ida`, right after each `checkMemberLimit`. Group `ida` comparison is `memberIda == group.ida` (`Option[IDA]` equality).
- [ ] **Step 4: Update existing callers.** Pass `idaPolicy = IDAPolicy.AllowAll` in each existing `createAllocationGroup(...)` call in the three suites (they mix pools of different IDAs or none); leave the new tests' calls explicit.
- [ ] **Step 5: Run** `sbt 'testOnly *AllocationGroupStateSuite *GroupObjectAllocatorSuite *UpdateAllocationGroupUsageTaskSuite'` — expect PASS.
- [ ] **Step 6: Commit** `git commit -am "Enforce allocation group IDA policy on membership"`

### Task 3: CLI

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/cmdline/Main.scala` (`Args` ~line 89, `createAllocationGroupArgs` 456, dispatch 850, `create_allocation_group` 1575, `formatAllocationGroupState` 2315)
- Test: `src/test/scala/org/aspen_ddp/aspen/cmdline/MainSuite.scala`

**Interfaces:**
- Consumes: Task 2 client API. Reuse `validateIdaType`, `cellSizeOption`, and whatever builds an `IDA` from `idaType/width/readThreshold/writeThreshold/cellSize` for `create-pool` (extract it to a shared helper if inline).
- Produces: `Args.allowAnyIda: Boolean = false`; `create_allocation_group(session, name, level, maxSliceSize, idaPolicy: IDAPolicy, ida: Option[IDA])`.

- [ ] **Step 1: Write failing tests** in `MainSuite`: parse `create-allocation-group g 0 replication 3 2 2` → ExactMatch config; parse `... g 0 --allow-any-ida` → AllowAll; parse `... g 0` (neither) → parse error; parse with both → parse error (use the suite's existing silenced expected-failure pattern); `formatAllocationGroupState` output contains `IDA Policy:` and `IDA:` lines for both policies (`IDA: any` for AllowAll).
- [ ] **Step 2: Run** `sbt 'testOnly *MainSuite'` — expect FAIL.
- [ ] **Step 3: Implement.** Add optional trailing positionals `[<ida-type>] [<width>] [<read-threshold>] [<write-threshold>]` (all-or-none) plus `--allow-any-ida`, `cellSizeOption`; cross-validate in scopt `checkConfig`. Build the `IDA` the same way `create-pool` does; pass through dispatch and `create_allocation_group`. Print `IDA Policy:` and `IDA:` in `formatAllocationGroupState`. Map `MemberIDAMismatch` in `add_pool_to_group` / `add_group_to_group` to a readable error line.
- [ ] **Step 4: Run** `sbt 'testOnly *MainSuite'` — expect PASS.
- [ ] **Step 5: Commit** `git commit -am "Add IDA arguments to create-allocation-group"`

### Task 4: Docs and cleanup

**Files:** Modify `UsageGuide.md` (allocation group section, ~line 164), `Architecture.md` (AllocationGroup mention), `Todo.txt` (remove the top item).

- [ ] **Step 1:** Document the policy, the fixed IDA, `MemberIDAMismatch`, and the new CLI form in `UsageGuide.md`; one sentence in `Architecture.md`.
- [ ] **Step 2:** Delete the first Todo.txt item (the AllocationGroups paragraph and its two sub-bullets).
- [ ] **Step 3: Run the full suite** `sbt test` (40 s hang detection per `CLAUDE.local.md`) — expect all PASS.
- [ ] **Step 4: Commit** `git commit -am "Document allocation group IDA policy"`
