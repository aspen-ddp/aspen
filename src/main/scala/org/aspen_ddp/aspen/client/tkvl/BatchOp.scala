package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.{Key, KeyOrdering, ObjectRevision, Value}

/** One change of a batch update. A batch holds at most one op per key. */
private[tkvl] sealed abstract class BatchOp:
  def key: Key

private[tkvl] object BatchOp:

  /** Sets `key` to `value`. `requirement` is as for `NodeEncoding.insert` */
  final case class Put(key: Key, value: Value, requirement: Option[Either[Boolean, ObjectRevision]]) extends BatchOp

  /** Deletes `key`. Nothing is done for a key that is not in the tree. */
  final case class Remove(key: Key) extends BatchOp

  /** The ops of a batch. Throws `IllegalArgumentException` if a key is inserted twice or both inserted and deleted.
    * A key deleted twice is deleted once.
    */
  def from(inserts: List[TKVLInsert], deletes: List[Key]): List[BatchOp] =
    val insertKeys = inserts.map(_.key)
    val distinctInserts = insertKeys.toSet
    if distinctInserts.size != insertKeys.size then
      throw new IllegalArgumentException("A batch update may insert a key only once")
    if deletes.exists(distinctInserts.contains) then
      throw new IllegalArgumentException("A batch update may not both insert and delete a key")

    val puts = inserts.map(i => Put(i.key, i.value, if i.requireDoesNotExist then Some(Left(true)) else None))
    puts ++ deletes.distinct.map(Remove(_))

  /** `ops` ascending by `ordering` */
  def sorted(ops: List[BatchOp], ordering: KeyOrdering): List[BatchOp] =
    ops.sortWith((a, b) => ordering.compare(a.key, b.key) < 0)
