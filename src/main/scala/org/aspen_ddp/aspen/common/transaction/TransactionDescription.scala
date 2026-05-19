package org.aspen_ddp.aspen.common.transaction

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.{ClientId, Codec}
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.pool.PoolId
import scala.language.implicitConversions

object TransactionDescription {
  def deserialize(db: DataBuffer): TransactionDescription = {
    Codec.decode(codec.TransactionDescription.parseFrom(db.asReadOnlyBuffer()))
  }
}

final case class TransactionDescription (
  /** Uniquely identifies this transaction */
  transactionId: TransactionId,

  /** Specifies the timestamp of the transaction. After commit, the objects modified by this
    * transaction will use this value as their last-updated timestamp.
    */
  startTimestamp: HLCTimestamp,

  /** Defines the primary object which is used for identifying the peers and quorum threshold used to resolve the transaction.
    *
    * Multiple objects in different pools may be modified by the transaction but only one
    * object is used to define the Paxos quorum used to actually resolve the commit/abort
    * decision. This must be set to the object with the strictest reliability constraints
    * from amongst all of the objects modified by the transaction.
    */
  primaryObject: ObjectPointer,

  /** Specifies the peer within the primary pool responsible for driving the transaction to closure
    *
    * The "Prepare" message may be sent by a non-member of the primary pool but the designated leader
    * is responsible for performing the role of the Paxos Proposer. It's also responsible for ensuring
    * that the list of FinalizationActions are executed. This peer may die or be initially unavailable
    * and therefore require a new leader to be elected but by specifying this up front, we avoid
    * leadership battles that would otherwise be required for every transaction.
    */
  designatedLeaderUID: Byte,

  requirements: List[TransactionRequirement],

  finalizationActions: List[SerializedFinalizationAction],

  /** Specifies which client initiated the transaction. Transaction resolution messages will be sent
    *  here as well as to the participating data stores.
    */
  originatingClient: Option[ClientId] = None,

  /** Specifies an additional set of stores to receive transaction resolution notices. Primary use case
    *  is for notifying stores of the result of an object allocation attempt.
    */
  notifyOnResolution: List[StoreId] = Nil,

  /** Optional set of notes that may be used for debugging transactions.
    *
    */
  notes: List[String] = Nil,

  /** IDA of the pool containing the primary object */
  primaryObjectIDA: IDA,

  /** Maps each pool referenced by the transaction to its IDA */
  poolIDAMap: Map[PoolId, IDA],

  /** Objects being allocated by this transaction */
  allocatingObjects: Set[ObjectId] = Set.empty) {

  def objectRequirements: List[TransactionObjectRequirement] = requirements.flatMap {
    case tor: TransactionObjectRequirement => Some(tor)
    case _ => None
  }

  def allReferencedObjectsSet: Set[ObjectPointer] = objectRequirements.map(_.objectPointer).toSet

  def primaryObjectDataStores: Set[StoreId] =
    (0 until primaryObjectIDA.width).map(i => StoreId(primaryObject.poolId, i.toByte)).toSet

  def allDataStores: Set[StoreId] =
    allReferencedObjectsSet.flatMap: ptr =>
      val ida = poolIDAMap(ptr.poolId)
      (0 until ida.width).map(i => StoreId(ptr.poolId, i.toByte))

  def allHostedObjects(storeId: StoreId): List[ObjectPointer] =
    allReferencedObjectsSet.foldLeft(List[ObjectPointer]()): (l, op) =>
      if op.poolId == storeId.poolId && storeId.poolIndex < poolIDAMap(storeId.poolId).width then
        op :: l
      else
        l

  def shortString: String = {
    val sb = new StringBuilder
    val ol = allReferencedObjectsSet.map(_.shortString).toList.sorted
    sb.append(s"Tx $transactionId: Objects: $ol")
    if (notes.nonEmpty) {
      sb.append("\n")
      notes.reverse.foreach { note =>
        sb.append(s"    $note")
        sb.append("\n")
      }
    }
    sb.toString
  }

  def serialize(): DataBuffer = Codec.encode(this).toByteArray
}
