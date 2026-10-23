package org.aspen_ddp.aspen.common.transaction

object DataUpdateOperation extends Enumeration {

  /** Appends data to the object by applying a per-store delta to the store's slice.
    *
    * The update payload is an encoded AppendDelta, computed client-side through the pool's IDA,
    * so each store receives its own delta rather than raw appended bytes. The store checks that
    * the delta's expected slice length matches its current slice and, for the store holding the
    * trailer (encoding index k-1) only, that the trailer bytes match. It then zero-extends the
    * slice to the new length and XORs the delta in. A delta that does not match, or cannot be
    * decoded, fails the requirement with AppendMismatch.
    *
    * Valid only under appendable IDAs (Replication and AppendingReedSolomon). Under a classic
    * ReedSolomon pool the result would not reconstruct. Stores cannot tell a pool's IDA from the
    * pointer, so rejecting Append on classic ReedSolomon pools is enforced client-side, the same
    * trust model as overwrite.
    */
  val Append: Value    = Value("Append")

  /** Replaces the object's data with the update data */
  val Overwrite: Value = Value("Overwrite")
}
