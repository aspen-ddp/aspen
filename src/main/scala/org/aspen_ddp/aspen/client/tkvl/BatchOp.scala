package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectRevision, Value}

/** One change of a batch update. A batch holds at most one op per key. */
private[tkvl] sealed abstract class BatchOp:
  def key: Key

private[tkvl] object BatchOp:

  /** Sets `key` to `value`. `requirement` is as for `NodeEncoding.insert` */
  final case class Put(key: Key, value: Value, requirement: PutRequirement) extends BatchOp

  /** Deletes `key`. Nothing is done for a key that is not in the tree, unless `requiredRevision` is given: the key
    * must then be held at that revision, else the batch fails with `KeyNotAtRevision`
    */
  final case class Remove(key: Key, requiredRevision: Option[ObjectRevision] = None) extends BatchOp

  /** The ops of a batch. Throws `IllegalArgumentException` if a key is inserted twice, both inserted and deleted, or
    * deleted at a revision and deleted otherwise or at another revision. A key deleted twice is deleted once.
    */
  def from(inserts: List[TKVLInsert],
           deletes: List[Key],
           deletesAtRevision: List[(Key, ObjectRevision)] = Nil): List[BatchOp] =
    val insertKeys = inserts.map(_.key)
    val distinctInserts = insertKeys.toSet
    if distinctInserts.size != insertKeys.size then
      throw new IllegalArgumentException("A batch update may insert a key only once")
    val atRevision = deletesAtRevision.distinct
    val atRevisionKeys = atRevision.map(_._1)
    if atRevisionKeys.distinct.size != atRevisionKeys.size || deletes.exists(atRevisionKeys.contains) then
      throw new IllegalArgumentException("A batch update may delete a key at a revision only once")
    if (deletes ++ atRevisionKeys).exists(distinctInserts.contains) then
      throw new IllegalArgumentException("A batch update may not both insert and delete a key")

    val puts = inserts.map(i => Put(i.key, i.value, i.requirement))
    puts ++ deletes.distinct.map(Remove(_)) ++ atRevision.map((k, rev) => Remove(k, Some(rev)))

  /** `ops` ascending by `ordering` */
  def sorted(ops: List[BatchOp], ordering: KeyOrdering): List[BatchOp] =
    ops.sortWith((a, b) => ordering.compare(a.key, b.key) < 0)
