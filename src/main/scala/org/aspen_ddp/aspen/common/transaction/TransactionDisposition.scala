package org.aspen_ddp.aspen.common.transaction

object TransactionDisposition extends Enumeration {
  val Undetermined: Value = Value("Undetermined")
  val VoteCommit: Value   = Value("VoteCommit")
  val VoteAbort: Value    = Value("VoteAbort")
}
