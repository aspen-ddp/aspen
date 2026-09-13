package org.aspen_ddp.aspen.common.transaction

object DataUpdateOperation extends Enumeration {

  /** Appends the update data to the object's existing data. NOT YET SUPPORTED.
    *
    * Reserved as a placeholder for a future implementation. Every layer rejects it today:
    * TransactionBuilder.append throws AppendNotYetSupported, RequirementsChecker fails the
    * requirement with UnsupportedOperation so the store votes abort, and RequirementsApplyer
    * throws if one somehow reaches it.
    *
    * The operation was applied store-locally: each store appended the update to its own slice.
    * That is correct only under Replication, where every slice is the whole object. Under a
    * slicing IDA such as ReedSolomon each store holds an encoded fragment, so appending raw
    * update bytes to a fragment produces something that does not reconstruct -- and it does so
    * silently, since the transaction commits normally and the damage only surfaces on the next
    * read. A correct implementation has to encode the appended data through the pool's IDA and
    * hand each store its own fragment, which the current object-update path does not do.
    */
  val Append: Value    = Value("Append")

  /** Replaces the object's data with the update data */
  val Overwrite: Value = Value("Overwrite")
}
