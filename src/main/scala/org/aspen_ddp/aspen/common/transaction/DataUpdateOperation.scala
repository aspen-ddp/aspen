package org.aspen_ddp.aspen.common.transaction

object DataUpdateOperation extends Enumeration {

  /** Appends data to the object by applying a per-store delta to the store's slice.
    *
    * The update payload is an encoded AppendDelta, computed client-side through the pool's IDA,
    * so each store receives its own delta rather than raw appended bytes. The store checks that
    * the delta's expected slice length and trailer match its current slice, XORs the delta into
    * the zero-extended slice, and extends it to the new length. A delta that does not match, or
    * cannot be decoded, fails the requirement with AppendMismatch.
    *
    * Valid only under appendable IDAs (Replication and AppendingReedSolomon). Under a classic
    * ReedSolomon pool the result would not reconstruct, so clients reject it before sending.
    */
  val Append: Value    = Value("Append")

  /** Replaces the object's data with the update data */
  val Overwrite: Value = Value("Overwrite")
}
