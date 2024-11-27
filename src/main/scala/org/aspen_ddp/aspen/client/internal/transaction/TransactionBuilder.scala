package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.internal.allocation.DeletionFinalizationAction
import org.aspen_ddp.aspen.client.{ConflictingRequirements, MultipleDataUpdatesToObject, MultipleRefcountUpdatesToObject}
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects._
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction._
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}

object TransactionBuilder {
  case class KVUpdate(
                       pointer: KeyValueObjectPointer,
                       requiredRevision: Option[ObjectRevision],
                       contentLock: Option[FullContentLock],
                       requirements: List[KeyValueUpdate.KeyRequirement],
                       operations: List[KeyValueOperation])

  case class TransactionData(localUpdates: List[ObjectUpdate],
                             preTransactionRebuilds: List[PreTransactionOpportunisticRebuild])
}

class TransactionBuilder(
                          val transactionId: TransactionId,
                          val chooseDesignatedLeader: ObjectPointer => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions)
                          val clientId: ClientId) {

  import TransactionBuilder._

  private [this] var requirements = List[TransactionRequirement]()

  private [this] var refcountUpdates = Set[ObjectPointer]()
  private [this] var updatingObjects = Set[ObjectPointer]() // Tracks UUIDs of all objects being modified
  private [this] var revisionLocks = Set[ObjectPointer]() // UUIDs of all revision-locked objects
  private [this] var dataObjectUpdates = Map[ObjectPointer, DataBuffer]()
  private [this] var keyValueUpdates = Map[KeyValueObjectPointer, KVUpdate]()

  private [this] var finalizationActions = List[SerializedFinalizationAction]()
  private [this] var notifyOnResolution = Set[StoreId]()
  private [this] var notes = List[String]()
  private [this] var addMissedUpdateTrackingFA = true
  private [this] var missedCommitDelayInMs = 1000
  private [this] val minimumTimestamp = HLCTimestamp.now

  def buildTranaction(opportunisticRebuildManager: OpportunisticRebuildManager): (TransactionDescription,
    Map[StoreId, TransactionData], HLCTimestamp) = synchronized {

    HLCTimestamp.update(minimumTimestamp)

    val startTimestamp = HLCTimestamp.now

    keyValueUpdates.valuesIterator.foreach { kvu =>
      requirements = KeyValueUpdate(kvu.pointer, kvu.requiredRevision, kvu.contentLock, kvu.requirements) :: requirements
    }

    // Ensure the transaction has at least one requirement
    require(requirements.nonEmpty)

    val primaryObject = requirements.flatMap {
      case tor: TransactionObjectRequirement => Some(tor)
      case _ => None
    }.map(_.objectPointer).maxBy(ptr => ptr.ida)

    val designatedLeaderUID = chooseDesignatedLeader(primaryObject)
    val originatingClient = Some(clientId)

    if (addMissedUpdateTrackingFA)
      finalizationActions = MissedUpdateFinalizationAction.createSerializedFA(missedCommitDelayInMs) :: finalizationActions

    val txd = TransactionDescription(transactionId, startTimestamp, primaryObject, designatedLeaderUID,
      requirements, finalizationActions, originatingClient, notifyOnResolution.toList,
      notes)

    var updates = Map[StoreId, TransactionData]()

    def addUpdate(pointer: ObjectPointer, encoded: Array[DataBuffer]): Unit = {
      val mpr = opportunisticRebuildManager.getPreTransactionOpportunisticRebuild(pointer)

      pointer.storePointers zip encoded foreach { x =>
        val (sp, bb) = x
        val storeId = StoreId(pointer.poolId, sp.poolIndex)
        val lu = ObjectUpdate(pointer.id, bb)

        val opr = mpr.get(sp.poolIndex)

        updates.get(storeId) match {
          case None =>
            val lpr = opr match {
              case None => List()
              case Some(pr) => pr :: Nil
            }
            updates += (storeId -> TransactionData(List(lu), lpr))

          case Some(td) =>
            val newLu = lu :: td.localUpdates
            val newLp = opr match {
              case None => td.preTransactionRebuilds
              case Some(pr) => pr :: td.preTransactionRebuilds
            }
            updates += (storeId -> TransactionData(newLu, newLp))
        }
      }
    }

    dataObjectUpdates foreach { t =>
      val (objectPointer, buf) = t

      addUpdate(objectPointer, objectPointer.ida.encode(buf))
    }

    keyValueUpdates.valuesIterator.foreach { kvu =>
      addUpdate(kvu.pointer, KeyValueOperation.encode(kvu.operations, kvu.pointer.ida))
    }

    (txd, updates, startTimestamp)
  }

  def disableMissedUpdateTracking(): Unit = synchronized {
    addMissedUpdateTrackingFA = false
  }

  def setMissedCommitDelayInMs(msec: Int): Unit = synchronized {
    missedCommitDelayInMs = msec
  }

  def append(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): Unit = synchronized {
    //println(s"   TXB Append txid $transactionUUID object ${objectPointer.uuid}")
    if (updatingObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    if (revisionLocks.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)

    updatingObjects += objectPointer
    dataObjectUpdates += (objectPointer -> data)
    requirements = DataUpdate(objectPointer, requiredRevision, DataUpdateOperation.Append) :: requirements
  }

  def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): Unit = synchronized {
    //println(s"   TXB Overwrite txid $transactionUUID object ${objectPointer.uuid}")
    if (updatingObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    if (revisionLocks.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)

    updatingObjects += objectPointer
    dataObjectUpdates += (objectPointer -> data)
    requirements  = DataUpdate(objectPointer, requiredRevision, DataUpdateOperation.Overwrite) :: requirements
  }

  def update(
              pointer: KeyValueObjectPointer,
              requiredRevision: Option[ObjectRevision],
              contentLock: Option[FullContentLock],
              requirements: List[KeyValueUpdate.KeyRequirement],
              operations: List[KeyValueOperation]): Unit = synchronized {
    //println(s"   TXB KV Append txid $transactionUUID object ${pointer.uuid}")
    if (updatingObjects.contains(pointer))
      throw MultipleDataUpdatesToObject(pointer)
    if (revisionLocks.contains(pointer))
      throw ConflictingRequirements(pointer)

    keyValueUpdates += (pointer -> KVUpdate(pointer, requiredRevision, contentLock, requirements, operations))
  }

  def setRefcount(objectPointer: ObjectPointer, requiredRefcount: ObjectRefcount, refcount: ObjectRefcount): Unit = synchronized {
    //println(s"   TXB SetRef txid $transactionUUID object ${objectPointer.uuid}")
    if (refcountUpdates.contains(objectPointer))
      throw MultipleRefcountUpdatesToObject(objectPointer)

    refcountUpdates += objectPointer
    requirements  = RefcountUpdate(objectPointer, requiredRefcount, refcount) :: requirements

    if (refcount.count == 0)
      finalizationActions = DeletionFinalizationAction.createSerializedFA(objectPointer) :: finalizationActions
  }

  def bumpVersion(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit = synchronized {
    //println(s"   TXB BumpVersion txid $transactionUUID object ${objectPointer.uuid}")
    if (updatingObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    if (revisionLocks.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)

    updatingObjects += objectPointer
    requirements = VersionBump(objectPointer, requiredRevision) :: requirements
  }

  def lockRevision(objectPointer: ObjectPointer, requiredRevision: ObjectRevision): Unit = synchronized {
    //println(s"   TXB LockRevision txid $transactionUUID object ${objectPointer.uuid}")
    if (updatingObjects.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)

    revisionLocks += objectPointer
    requirements = RevisionLock(objectPointer, requiredRevision) :: requirements
  }

  def ensureHappensAfter(timestamp: HLCTimestamp): Unit = HLCTimestamp.update(timestamp)

  def timestamp(): HLCTimestamp = synchronized { minimumTimestamp }

  def addFinalizationAction(finalizationActionId: FinalizationActionId, serializedContent: Array[Byte]): Unit = synchronized {
    finalizationActions = SerializedFinalizationAction(finalizationActionId, serializedContent) :: finalizationActions
  }

  def addFinalizationAction(finalizationActionId: FinalizationActionId): Unit = synchronized {
    finalizationActions = SerializedFinalizationAction(finalizationActionId, new Array[Byte](0)) :: finalizationActions
  }

  def addNotifyOnResolution(storesToNotify: Set[StoreId]): Unit = synchronized {
    notifyOnResolution = notifyOnResolution ++ storesToNotify
  }

  def note(note: String): Unit = synchronized {
    notes = note :: notes
  }
}