package org.aspen_ddp.aspen.client

/** Test-only access to Transaction members restricted to the client package */
object TestTransactions:
  /** `tx` with missed-update tracking disabled, as MissedUpdateFinalizationAction's own error-tree writes are */
  def withoutMissedUpdateTracking(tx: Transaction): Transaction =
    tx.disableMissedUpdateTracking()
    tx
