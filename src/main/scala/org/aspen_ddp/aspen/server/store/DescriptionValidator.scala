package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.*

/** The store's copy of the rules TransactionBuilder applies to a description (transaction-requirements 2.5,
  * store-side validation).
  * A store votes from the description, and a description built without the builder (an older or buggy client, a
  * tool) could otherwise break what the vote relies on:
  *
  *  - The store's own pool entry in poolIDAMap must be the pool's IDA. The driver takes thresholds from the
  *    description, so a smaller writeThreshold would let fewer commit votes decide (R2). Every referenced pool's
  *    stores check its entry, so no pool's threshold can be lowered.
  *  - Every referenced pool has an IDA, primaryObjectIDA is the primary pool's entry, the primary object is
  *    referenced and its pool tolerates no fewer failures than any other (the builder's choice), and the designated
  *    leader is one of its stores. The primary object names the Paxos acceptors.
  *  - At most one content update (DataUpdate, KeyValueUpdate, VersionBump) and one RefcountUpdate per object, no
  *    lock on an object the transaction updates, and no shared lock beside another lock. A second KeyValueUpdate's
  *    keys would otherwise go unlocked (KVWrites keeps the last).
  *  - Every RefcountUpdate advances ObjectRefcount.updateSerial, unless it allocates the object or sets the refcount
  *    it requires (RefcountUpdate.advancesSerial). A recovered vote takes a passing refcount check as not yet
  *    applied, which a later change returning the refcount to the required value would falsify.
  *  - The pin rule (Pins.declared), unless the client marked the description replaySafe. A key pin is taken from
  *    KeyValueUpdate.pinned, which the stores holding that object check (RequirementsChecker, FalsePin): a false one
  *    gets abort votes on its object, so the transaction cannot commit.
  *
  * The replaySafe flag is the one claim stores must trust: no store can prove that a replay is harmless.
  */
object DescriptionValidator:

  /** None when the description is valid, the reason it is refused otherwise.
    *
    * @param poolIDA The IDA of the store's pool (StoreConfig.ida). None skips that check (tests only)
    */
  def validate(txd: TransactionDescription, storeId: StoreId, poolIDA: Option[IDA]): Option[String] =
    pools(txd, storeId, poolIDA).orElse(structure(txd)).orElse(refcounts(txd)).orElse(pin(txd))

  private def pools(txd: TransactionDescription, storeId: StoreId, poolIDA: Option[IDA]): Option[String] =
    val referenced = txd.allReferencedObjectsSet
    val primaryPool = txd.primaryObject.poolId
    def tolerance = txd.primaryObjectIDA.failureTolerance
    poolIDA.flatMap { ida =>
      txd.poolIDAMap.get(storeId.poolId) match
        case Some(named) if named != ida => Some(s"names IDA $named for pool ${storeId.poolId}, whose IDA is $ida")
        case _ => None
    }.orElse {
      referenced.find(p => !txd.poolIDAMap.contains(p.poolId)).map(p => s"names no IDA for pool ${p.poolId}")
    }.orElse {
      if !txd.poolIDAMap.get(primaryPool).contains(txd.primaryObjectIDA) then
        Some(s"primary IDA ${txd.primaryObjectIDA} is not pool $primaryPool's IDA ${txd.poolIDAMap.get(primaryPool)}")
      else if !referenced.exists(_.id == txd.primaryObject.id) then
        Some(s"primary object ${txd.primaryObject.id} is not referenced by a requirement")
      else if txd.poolIDAMap.exists((_, ida) => ida.failureTolerance > tolerance) then
        Some(s"primary pool $primaryPool tolerates fewer failures than another referenced pool")
      else if txd.designatedLeaderUID < 0 || txd.designatedLeaderUID >= txd.primaryObjectIDA.width then
        Some(s"designated leader ${txd.designatedLeaderUID} is not a store of the primary pool")
      else
        None
    }

  private def structure(txd: TransactionDescription): Option[String] =
    txd.objectRequirements.groupBy(_.objectPointer.id).iterator.flatMap { (oid, reqs) =>
      val updates = reqs.count:
        case _: DataUpdate | _: KeyValueUpdate | _: VersionBump => true
        case _ => false
      val refcounts = reqs.count(_.isInstanceOf[RefcountUpdate])
      val exclusive = reqs.count(_.isInstanceOf[ExclusiveRevisionLock])
      val shared = reqs.count(_.isInstanceOf[SharedRevisionLock])
      if updates > 1 then Some(s"updates object $oid more than once")
      else if refcounts > 1 then Some(s"updates the refcount of object $oid more than once")
      else if updates > 0 && exclusive + shared > 0 then Some(s"locks object $oid, which it also updates")
      else if shared > 0 && exclusive + shared > 1 then Some(s"holds a shared lock on object $oid beside another lock")
      else None
    }.nextOption()

  private def refcounts(txd: TransactionDescription): Option[String] =
    txd.requirements.collectFirst:
      case ru: RefcountUpdate if !RefcountUpdate.advancesSerial(ru, txd.allocatingObjects) =>
        s"sets the refcount of object ${ru.objectPointer.id} from ${ru.requiredRefcount} to ${ru.newRefcount} " +
          "without advancing its update serial"

  private def pin(txd: TransactionDescription): Option[String] =
    if txd.replaySafe || Pins.declared(txd.requirements) then None
    else Some("has no pin and is not marked replaySafe")
