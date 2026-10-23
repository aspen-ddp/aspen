package org.aspen_ddp.aspen.client.internal.transaction

import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.internal.allocation.DeletionFinalizationAction
import org.aspen_ddp.aspen.client.{AppendNotSupported, AppendPosition, ConflictingRequirements, MultipleDataUpdatesToObject, MultipleRefcountUpdatesToObject}
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction.*
import org.aspen_ddp.aspen.common.util.printStack
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.{AppendableIDA, IDA}
import org.aspen_ddp.aspen.common.pool.PoolId

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
                          val chooseDesignatedLeader: (ObjectPointer, IDA) => Byte, // Uses peer online/offline knowledge to select designated leaders for transactions
                          val clientId: ClientId) {

  import TransactionBuilder._

  private  var requirements = List[TransactionRequirement]()

  private  var refcountUpdates = Set[ObjectPointer]()
  private  var updatingObjects = Set[ObjectPointer]() // Tracks UUIDs of all objects being modified
  private  var revisionLocks = Set[ObjectPointer]() // UUIDs of all revision-locked objects
  private  var dataObjectUpdates = Map[ObjectPointer, DataBuffer]()
  private  var appendUpdates = Map[ObjectPointer, (AppendPosition, DataBuffer)]()
  private  var keyValueUpdates = Map[KeyValueObjectPointer, KVUpdate]()

  private  var finalizationActions = List[SerializedFinalizationAction]()
  private  var notifyOnResolution = Set[StoreId]()
  private  var allocatingObjects = Set[ObjectPointer]()
  private  var notes = List[String]()
  private  var addMissedUpdateTrackingFA = true
  private  var missedCommitDelayInMs = 1000
  private  val minimumTimestamp = HLCTimestamp.now

  def isEmpty: Boolean =
    requirements.isEmpty &&
      refcountUpdates.isEmpty &&
      updatingObjects.isEmpty &&
      dataObjectUpdates.isEmpty &&
      appendUpdates.isEmpty &&
      keyValueUpdates.isEmpty &&
      finalizationActions.isEmpty &&
      allocatingObjects.isEmpty

  def buildTranaction(opportunisticRebuildManager: OpportunisticRebuildManager, poolIDAMap: Map[PoolId, IDA]): (TransactionDescription,
    Map[StoreId, TransactionData], HLCTimestamp) = synchronized {

    HLCTimestamp.update(minimumTimestamp)

    val startTimestamp = HLCTimestamp.now

    // Add "empty" content for any allocating objects that weren't given explicit content.
    // This is required as completely missing content is what we use to attempt to drive transactions
    // to failure during transaction recovery
    allocatingObjects.filter(!updatingObjects.contains(_)).foreach: ptr =>
      updatingObjects += ptr
      requirements  = RefcountUpdate(ptr, ObjectRefcount.Allocating, ObjectRefcount.Default) :: requirements
      ptr match
        case dptr: DataObjectPointer =>
          // The DataUpdate is what causes stores to apply the encoded content. Slicing IDAs encode
          // even empty content to non-empty slices, so without it stores would keep 0-byte slices
          dataObjectUpdates += (dptr -> DataBuffer.Empty)
          requirements = DataUpdate(dptr, ObjectRevision.Allocating, DataUpdateOperation.Overwrite) :: requirements
        case kvptr: KeyValueObjectPointer =>
          keyValueUpdates += (kvptr -> KVUpdate(kvptr, None, None, Nil, Nil))

    keyValueUpdates.valuesIterator.foreach: kvu =>
      requirements = KeyValueUpdate(kvu.pointer, kvu.requiredRevision, kvu.contentLock, kvu.requirements) :: requirements

    // Ensure the transaction has at least one requirement
    require(requirements.nonEmpty)

    val primaryObject = requirements.flatMap {
      case tor: TransactionObjectRequirement => Some(tor)
      case _ => None
    }.map(_.objectPointer).maxBy(ptr => poolIDAMap(ptr.poolId))

    val primaryObjectIDA = poolIDAMap(primaryObject.poolId)
    val designatedLeaderUID = chooseDesignatedLeader(primaryObject, primaryObjectIDA)
    val originatingClient = Some(clientId)

    if (addMissedUpdateTrackingFA)
      finalizationActions = MissedUpdateFinalizationAction.createSerializedFA(missedCommitDelayInMs) :: finalizationActions

    val txd = TransactionDescription(transactionId, startTimestamp, primaryObject, designatedLeaderUID,
      requirements, finalizationActions, originatingClient, notifyOnResolution.toList,
      notes, primaryObjectIDA, poolIDAMap, allocatingObjects.map(_.id))

    var updates = Map[StoreId, TransactionData]()

    def addUpdate(pointer: ObjectPointer, encoded: Array[DataBuffer]): Unit = {
      val mpr = opportunisticRebuildManager.getPreTransactionOpportunisticRebuild(pointer)
      val objIDA = poolIDAMap(pointer.poolId)

      (0 until objIDA.width).foreach { poolIndex =>
        val storeId = StoreId(pointer.poolId, poolIndex.toByte)
        val lu = ObjectUpdate(pointer.id, encoded(poolIndex))

        val opr = mpr.get(poolIndex.toByte)

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

      addUpdate(objectPointer, poolIDAMap(objectPointer.poolId).encode(buf))
    }

    appendUpdates foreach { t =>
      val (objectPointer, (position, buf)) = t

      poolIDAMap(objectPointer.poolId) match
        case ida: (IDA & AppendableIDA) =>
          addUpdate(objectPointer, ida.appendDeltas(position.length, buf.getByteArray).map(_.encode))
        case ida =>
          throw AppendNotSupported(objectPointer, ida)
    }

    keyValueUpdates.valuesIterator.foreach { kvu =>
      addUpdate(kvu.pointer, KeyValueOperation.encode(kvu.operations, poolIDAMap(kvu.pointer.poolId)))
    }

    (txd, updates, startTimestamp)
  }

  def allReferencedPoolIds: Set[PoolId] = synchronized {
    val objectPointers = requirements.flatMap {
      case tor: TransactionObjectRequirement => Some(tor.objectPointer)
      case _ => None
    } ++ keyValueUpdates.values.map(_.pointer) ++ allocatingObjects
    objectPointers.map(_.poolId).toSet
  }

  def missedUpdateTrackingEnabled: Boolean = synchronized {
    addMissedUpdateTrackingFA
  }

  def disableMissedUpdateTracking(): Unit = synchronized {
    addMissedUpdateTrackingFA = false
  }

  def setMissedCommitDelayInMs(msec: Int): Unit = synchronized {
    missedCommitDelayInMs = msec
  }

  /** Appends data at the given position. Whether the pool's IDA supports appends is checked when the
    * transaction is built, since the IDA is not known here.
    */
  def append(position: AppendPosition, data: DataBuffer): Unit = synchronized {
    val objectPointer = position.pointer
    if (updatingObjects.contains(objectPointer))
      throw MultipleDataUpdatesToObject(objectPointer)
    if (revisionLocks.contains(objectPointer))
      throw ConflictingRequirements(objectPointer)

    updatingObjects += objectPointer
    appendUpdates += (objectPointer -> (position, data))
    requirements  = DataUpdate(objectPointer, position.revision, DataUpdateOperation.Append) :: requirements
  }

  def overwrite(objectPointer: ObjectPointer, requiredRevision: ObjectRevision, data: DataBuffer): Unit = synchronized {
    //println(s"   TXB Overwrite txid $transactionUUID object ${objectPointer.uuid}")
    if (updatingObjects.contains(objectPointer)) {
      //printStack()
      throw MultipleDataUpdatesToObject(objectPointer)
    }
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
    if (updatingObjects.contains(pointer)) {
      //printStack()
      throw MultipleDataUpdatesToObject(pointer)
    }
    if (revisionLocks.contains(pointer))
      throw ConflictingRequirements(pointer)

    updatingObjects += pointer
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

  def addAllocatingObject(objectPtr: ObjectPointer): Unit = synchronized {
    allocatingObjects += objectPtr
  }

  def addNotifyOnResolution(storesToNotify: Set[StoreId]): Unit = synchronized {
    notifyOnResolution = notifyOnResolution ++ storesToNotify
  }

  def note(note: String): Unit = synchronized {
    notes = note :: notes
  }
}