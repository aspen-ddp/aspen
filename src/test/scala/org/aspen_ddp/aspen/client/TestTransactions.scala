package org.aspen_ddp.aspen.client

/** Test-only access to Transaction members restricted to the client package */
object TestTransactions:
  /** `tx` with missed-update tracking disabled, as MissedUpdateFinalizationAction's own error-tree writes are */
  def withoutMissedUpdateTracking(tx: Transaction): Transaction =
    tx.disableMissedUpdateTracking()
    tx

  /** Marks `tx` replay-safe for tests that are not about replay and stage nothing pinned */
  def replaySafe(tx: Transaction): Transaction =
    tx.replaySafe("test")
    tx

  /** The check an Insert of `key` into the object `kvos` was read from needs (stores refuse an Insert that checks
    * nothing of its key): `Exists` if it holds the key, `DoesNotExist` if not
    */
  def keyCheck(kvos: KeyValueObjectState,
               key: org.aspen_ddp.aspen.common.objects.Key): org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRequirement =
    import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
    if kvos.contents.contains(key) then KeyValueUpdate.Exists(key) else KeyValueUpdate.DoesNotExist(key)
