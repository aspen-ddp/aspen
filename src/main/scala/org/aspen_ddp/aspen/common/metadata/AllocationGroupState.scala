package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, KeyValueObjectState, ReadError, StopRetrying, Transaction}
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.metadata.management.UpdateAllocationGroupUsageTask
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.WeightedSelector
import org.aspen_ddp.aspen.compute.TaskExecutor

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object AllocationGroupState:

  enum MemberType:
    case Pool, Group

  case class Member(memberType: MemberType,
                    uuid: UUID,
                    maxObjectSize: Option[Int],
                    currentUsage: Long,
                    maximumSize: Long)
  
  /** Thrown if attempting to add a group to one of an equal or lower level */
  class InvalidLevel extends Throwable

  def apply(cfg: Array[Byte]): AllocationGroupState = Codec.decode(codec.AllocationGroupState.parseFrom(cfg))
  def apply(dos: DataObjectState): AllocationGroupState = apply(dos.data.getByteArray)

  def addPool(client: AspenClient,
              poolId: PoolId,
              parentId: AllocationGroupId,
              taskExecutor: TaskExecutor): Future[Unit] =

    def mod(psPtr: KeyValueObjectPointer, psKvos: KeyValueObjectState,
            agsPtr: DataObjectPointer, agsDos: DataObjectState,
            ps: StoragePoolState, ags: AllocationGroupState,
            tx: Transaction): (StoragePoolState, AllocationGroupState) =
      if ps.allocationGroups.contains(parentId.uuid) && ags.members.exists(_.uuid == poolId.uuid) then
        (ps, ags)
      else
        val nps = ps.copy(allocationGroups = parentId.uuid :: ps.allocationGroups)
        val reqs = List(KeyRevision(StoragePoolState.ConfigKey, psKvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, nps.encode()))

        tx.update(psPtr, None, None, reqs, ops)

        val m = Member(MemberType.Pool, ps.poolId.uuid, ps.maxObjectSize, ps.currentUsage, ps.maximumStoreSize)
        val nags = ags.copy(members = m :: ags.members)

        tx.overwrite(agsPtr, agsDos.revision, DataBuffer(nags.toBytes))

        (nps, nags)

    modifyPool(client, poolId, parentId, taskExecutor, mod)

  def removePool(client: AspenClient,
                 poolId: PoolId,
                 parentId: AllocationGroupId,
                 taskExecutor: TaskExecutor): Future[Unit] =

    def mod(psPtr: KeyValueObjectPointer, psKvos: KeyValueObjectState,
            agsPtr: DataObjectPointer, agsDos: DataObjectState,
            ps: StoragePoolState, ags: AllocationGroupState,
            tx: Transaction): (StoragePoolState, AllocationGroupState) =
      if !ps.allocationGroups.contains(parentId.uuid) && !ags.members.exists(_.uuid == poolId.uuid) then
        (ps, ags)
      else
        val nps = ps.copy(allocationGroups = ps.allocationGroups.filter(_ != parentId.uuid))
        val reqs = List(KeyRevision(StoragePoolState.ConfigKey, psKvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, nps.encode()))

        tx.update(psPtr, None, None, reqs, ops)

        val nags = ags.copy(members = ags.members.filter(_.uuid != ps.poolId.uuid))

        tx.overwrite(agsPtr, agsDos.revision, DataBuffer(nags.toBytes))

        (nps, nags)

    modifyPool(client, poolId, parentId, taskExecutor, mod)

  private def modifyPool(client: AspenClient,
                         poolId: PoolId,
                         parentId: AllocationGroupId,
                         taskExecutor: TaskExecutor,
                         mod: (KeyValueObjectPointer, KeyValueObjectState, DataObjectPointer, DataObjectState,
                           StoragePoolState, AllocationGroupState,
                           Transaction) => (StoragePoolState, AllocationGroupState)
                        ): Future[Unit] =
    given ExecutionContext = client.clientContext
    def prep(tx: Transaction): Future[Unit] =
      given Transaction = tx
      for
        psPtr <- client.getStoragePoolPointer(poolId)
        agsPtr <- client.getAllocationGroupPointer(parentId)
        psKvos <- client.read(psPtr)
        agsDos <- client.read(agsPtr)
      yield
        val ps = StoragePoolState(psKvos)
        val ags = AllocationGroupState(agsDos)

        val (nps, nags) = mod(psPtr, psKvos, agsPtr, agsDos, ps, ags, tx)

        if nags.parentGroups.nonEmpty then
          UpdateAllocationGroupUsageTask.prepareTask(poolId.uuid,
            nags.currentUsage, nags.maximumSize, nags.parentGroups.map(_.uuid), taskExecutor)

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)

    client.transactUntilSuccessfulWithRecovery(onFail): tx =>
      prep(tx)


  def addGroup(client: AspenClient,
               childId: AllocationGroupId,
               parentId: AllocationGroupId,
               taskExecutor: TaskExecutor): Future[Unit] =

    def mod(childPtr: DataObjectPointer, childDos: DataObjectState,
            parentPtr: DataObjectPointer, parentDos: DataObjectState,
            child: AllocationGroupState, parent: AllocationGroupState,
            tx: Transaction): (AllocationGroupState, AllocationGroupState) =
      if child.parentGroups.exists(_.uuid == parentId.uuid) && parent.members.exists(_.uuid == childId.uuid) then
        (child, parent)
      else
        if child.level >= parent.level then
          throw new InvalidLevel()
        
        val nchild = child.copy(parentGroups = parentId :: child.parentGroups)
        
        tx.overwrite(childPtr, childDos.revision, DataBuffer(nchild.toBytes))
        
        val m = Member(MemberType.Group, child.groupId.uuid, child.maximumObjectSize, 
          child.currentUsage, child.maximumSize)
        val nags = parent.copy(members = m :: parent.members)

        tx.overwrite(parentPtr, parentDos.revision, DataBuffer(nags.toBytes))

        (nchild, nags)

    modifyGroup(client, childId, parentId, taskExecutor, mod)

  def removeGroup(client: AspenClient,
                  childId: AllocationGroupId,
                  parentId: AllocationGroupId,
                  taskExecutor: TaskExecutor): Future[Unit] =

    def mod(childPtr: DataObjectPointer, childDos: DataObjectState,
            parentPtr: DataObjectPointer, parentDos: DataObjectState,
            child: AllocationGroupState, parent: AllocationGroupState,
            tx: Transaction): (AllocationGroupState, AllocationGroupState) =
      if !child.parentGroups.exists(_.uuid == parentId.uuid) && !parent.members.exists(_.uuid == childId.uuid) then
        (child, parent)
      else
        val nchild = child.copy(parentGroups = child.parentGroups.filter(_ != parentId))

        tx.overwrite(childPtr, childDos.revision, DataBuffer(nchild.toBytes))

        val nparent = parent.copy(members = parent.members.filter(_.uuid != child.groupId.uuid))

        tx.overwrite(parentPtr, parentDos.revision, DataBuffer(nparent.toBytes))

        (nchild, nparent)

    modifyGroup(client, childId, parentId, taskExecutor, mod)
    
  private def modifyGroup(client: AspenClient,
                          childId: AllocationGroupId,
                          parentId: AllocationGroupId,
                          taskExecutor: TaskExecutor,
                          mod: (DataObjectPointer, DataObjectState, DataObjectPointer, DataObjectState,
                            AllocationGroupState, AllocationGroupState,
                            Transaction) => (AllocationGroupState, AllocationGroupState)
                         ): Future[Unit] =
    given ExecutionContext = client.clientContext

    def prep(tx: Transaction): Future[Unit] =
      given Transaction = tx

      for
        childPtr <- client.getAllocationGroupPointer(childId)
        parentPtr <- client.getAllocationGroupPointer(parentId)
        childDos <- client.read(childPtr)
        parentDos <- client.read(parentPtr)
      yield
        val child = AllocationGroupState(childDos)
        val parent = AllocationGroupState(parentDos)

        val (nchild, nparent) = mod(childPtr, childDos, parentPtr, parentDos, child, parent, tx)

        if nparent.parentGroups.nonEmpty then
          UpdateAllocationGroupUsageTask.prepareTask(childId.uuid,
            nparent.currentUsage, nparent.maximumSize, nparent.parentGroups.map(_.uuid), taskExecutor)

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)
      case e: InvalidLevel => throw StopRetrying(e)

    client.transactUntilSuccessfulWithRecovery(onFail): tx =>
      prep(tx)


final case class AllocationGroupState(
                                     groupId: AllocationGroupId,
                                     level: Int,
                                     name: String,
                                     members: List[AllocationGroupState.Member],
                                     parentGroups: List[AllocationGroupId]
                                     ):

  private var wselector: Option[WeightedSelector[AllocationGroupState.Member]] = None

  def selectMemberForAllocation(): Option[AllocationGroupState.Member] =
    val ws = synchronized:
      wselector match
        case Some(o) => o
        case None =>
          wselector = Some(new WeightedSelector[AllocationGroupState.Member](members.map: m =>
            val freeSpace = m.maximumSize - m.currentUsage
            if freeSpace >= 0 then
              (m, freeSpace.toDouble)
            else
              (m, 0.0)
          ))
          wselector.get

    ws.next()

  def toBytes: Array[Byte] = Codec.encode(this).toByteArray

  def currentUsage: Long = members.foldLeft(0L)((acc, m) => acc + m.currentUsage)
  def maximumSize: Long = members.foldLeft(0L)((acc, m) => acc + m.maximumSize)
  
  def newCurrentUsage(uuid: UUID, newUsage: Long): Long =
    members.foldLeft(0L): (acc, m) =>
      val sz = if m.uuid == uuid then newUsage else m.currentUsage
      acc + sz

  def newMaximumSize(uuid: UUID, newMaxSize: Long): Long =
    members.foldLeft(0L): (acc, m) =>
      val sz = if m.uuid == uuid then newMaxSize else m.maximumSize
      acc + sz
  
  /** Returns the smallest maximumObjectSize for all members */
  def maximumObjectSize: Option[Int] =
    members.foldLeft[Option[Int]](None): (acc, m) => 
      acc match
        case None => m.maxObjectSize 
        case Some(min) => 
          if m.maxObjectSize.getOrElse(Int.MaxValue) < min then 
            m.maxObjectSize 
          else 
            acc
      
          
