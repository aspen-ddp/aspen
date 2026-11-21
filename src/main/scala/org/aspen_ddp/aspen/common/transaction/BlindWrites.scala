package org.aspen_ddp.aspen.common.transaction

import org.aspen_ddp.aspen.common.objects.{Delete, Insert, Key, KeyValueOperation, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{FullContentLock, KeyRequirement, KeyRevision}

/** The blind-write rule for KeyValue updates, shared by TransactionBuilder (which refuses a violation with
  * BlindKeyValueWrite) and the stores' RequirementsChecker (which refuses it with BlindWrite, so a client that bypasses
  * the builder cannot issue one).
  *
  * Unless the transaction allocates the object (its initial content), a key Delete needs a KeyRevision on its key or a
  * content lock, a change of min, max, left or right needs a required revision, and a key Insert needs a required
  * revision, a content lock or a requirement that checks its key (KeyValueUpdate.checkedKeys). An unconditioned Insert
  * passes at a store that applied a later Delete of its key first and is a no-op at one that applied a later write of
  * it first, so the two orders diverge unreported (transaction-requirements R15). A blind Delete leaves no tombstone
  * and object-level parts carry no timestamp, so neither can be ordered against other writes, and repair cannot see
  * them: a no-op blind Delete leaves no trace for its snapshot and an object-level change that does not move the
  * revision escapes a repair proof's revision lock (repair-process 1.2, Q8; transaction-requirements 2.5). A required
  * revision does not cover a Delete: an Insert of the key moves no revision, so a Delete applied after it here, in
  * lock order or as a non-voter, would pass its re-check and remove the key unreported (transaction-requirements 9.3,
  * R15).
  */
object BlindWrites:
  enum Violation:
    case UncheckedInsert(key: Key)
    case UncheckedDelete(key: Key)
    case UnrevisionedObjectChange(op: KeyValueOperation)

  /** The first operation, in order, that the rule refuses. Callers skip objects the transaction allocates */
  def violation(requiredRevision: Option[ObjectRevision],
                contentLock: Option[FullContentLock],
                requirements: List[KeyRequirement],
                ops: List[KeyValueOperation]): Option[Violation] =
    lazy val revisionChecked = requirements.iterator.collect { case r: KeyRevision => r.key }.toSet
    lazy val keyChecked = KeyValueUpdate.checkedKeys(requirements)
    ops.iterator.flatMap {
      case i: Insert =>
        if requiredRevision.isEmpty && contentLock.isEmpty && !keyChecked.contains(i.key) then
          Some(Violation.UncheckedInsert(i.key))
        else
          None
      case d: Delete =>
        if contentLock.isEmpty && !revisionChecked.contains(d.key) then
          Some(Violation.UncheckedDelete(d.key))
        else
          None
      case op =>
        if requiredRevision.isEmpty then
          Some(Violation.UnrevisionedObjectChange(op))
        else
          None
    }.nextOption()
