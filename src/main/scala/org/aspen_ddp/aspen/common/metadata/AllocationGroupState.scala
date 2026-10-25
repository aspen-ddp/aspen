package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, KeyValueObjectState, StopRetrying, Transaction}
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.ida.IDA
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

  /** A pool or child group within an allocation group. Slice and object size limits are not recorded here: a
    * member's limit is checked against the group's when it joins (see `MemberLimitIncompatible`), and the group's
    * own `maxSliceSize` is the only limit code allocating through the group needs.
    */
  case class Member(memberType: MemberType,
                    uuid: UUID,
                    currentUsage: Long,
                    maximumSize: Long)

  object Member:
    def forPool(ps: StoragePoolState): Member =
      Member(MemberType.Pool, ps.poolId.uuid, ps.currentUsage, ps.maximumStoreSize)

    def forGroup(ags: AllocationGroupState): Member =
      Member(MemberType.Group, ags.groupId.uuid, ags.currentUsage, ags.maximumSize)

  /** Thrown if attempting to add a group to one of an equal or lower level */
  class InvalidLevel extends Throwable

  /** Thrown when a pool or group cannot join a group because of its slice limit: the group has a limit and the
    * member's is smaller, or the group has none (`groupLimit == None`) and the member does. Raised before any state
    * changes.
    */
  final case class MemberLimitIncompatible(memberId: UUID, memberLimit: Option[Int], groupLimit: Option[Int])
    extends Exception(s"Member $memberId with maxSliceSize ${memberLimit.getOrElse("unlimited")} cannot join a " +
      s"group whose maxSliceSize is ${groupLimit.getOrElse("unlimited")}")

  /** Thrown when a pool or group cannot join an `ExactMatch` group because its IDA is not equal to the group's, or
    * because it has none (an `AllowAll` child group). Raised before any state changes.
    */
  final case class MemberIDAMismatch(memberId: UUID, memberIda: Option[IDA], groupIda: Option[IDA])
    extends Exception(s"Member $memberId with IDA ${memberIda.getOrElse("none (AllowAll)")} cannot join a " +
      s"group requiring IDA ${groupIda.getOrElse("none")}")

  /** Throws `MemberIDAMismatch` unless a member with IDA `memberIda` may join `group`. Uses `==`, which compares
    * the IDA type and every parameter. `IDA.compare` orders by failure tolerance only and must not be used here.
    */
  private def checkMemberIda(memberId: UUID, memberIda: Option[IDA], group: AllocationGroupState): Unit =
    group.idaPolicy match
      case IDAPolicy.AllowAll => ()
      case IDAPolicy.ExactMatch =>
        if memberIda != group.ida then
          throw MemberIDAMismatch(memberId, memberIda, group.ida)

  /** Throws `MemberLimitIncompatible` unless a member with slice limit `memberLimit` may join `group`. A group's
    * limit is a floor every pool below it meets, so a limited group accepts members with no limit or one at least as
    * large, and an unlimited group accepts only unlimited members.
    */
  private def checkMemberLimit(memberId: UUID, memberLimit: Option[Int], group: AllocationGroupState): Unit =
    val ok = (group.maxSliceSize, memberLimit) match
      case (Some(groupLimit), Some(limit)) => limit >= groupLimit
      case (Some(_), None)                 => true
      case (None, limit)                   => limit.isEmpty
    if !ok then
      throw MemberLimitIncompatible(memberId, memberLimit, group.maxSliceSize)

  def apply(cfg: Array[Byte]): AllocationGroupState = Codec.decode(codec.AllocationGroupState.parseFrom(cfg))
  def apply(dos: DataObjectState): AllocationGroupState = apply(dos.data.getByteArray)

  def addPool(client: AspenClient,
              poolId: PoolId,
              parentId: AllocationGroupId,
              taskExecutor: Option[TaskExecutor] = None): Future[Unit] =

    def mod(psPtr: KeyValueObjectPointer, psKvos: KeyValueObjectState,
            agsPtr: DataObjectPointer, agsDos: DataObjectState,
            ps: StoragePoolState, ags: AllocationGroupState,
            tx: Transaction): Option[(StoragePoolState, AllocationGroupState)] =
      if ps.allocationGroups.contains(parentId.uuid) && ags.members.exists(_.uuid == poolId.uuid) then
        None
      else
        checkMemberLimit(ps.poolId.uuid, ps.effectiveMaxSliceSize, ags)
        checkMemberIda(ps.poolId.uuid, Some(ps.ida), ags)

        val nps = ps.copy(allocationGroups = parentId.uuid :: ps.allocationGroups)
        val reqs = List(KeyRevision(StoragePoolState.ConfigKey, psKvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, nps.encode()))

        tx.update(psPtr, None, None, reqs, ops)

        val nags = ags.copy(members = Member.forPool(ps) :: ags.members)

        tx.overwrite(agsPtr, agsDos.revision, DataBuffer(nags.toBytes))

        Some((nps, nags))

    modifyPool(client, poolId, parentId, taskExecutor, mod)

  def removePool(client: AspenClient,
                 poolId: PoolId,
                 parentId: AllocationGroupId,
                 taskExecutor: Option[TaskExecutor] = None): Future[Unit] =

    def mod(psPtr: KeyValueObjectPointer, psKvos: KeyValueObjectState,
            agsPtr: DataObjectPointer, agsDos: DataObjectState,
            ps: StoragePoolState, ags: AllocationGroupState,
            tx: Transaction): Option[(StoragePoolState, AllocationGroupState)] =
      if !ps.allocationGroups.contains(parentId.uuid) && !ags.members.exists(_.uuid == poolId.uuid) then
        None
      else
        val nps = ps.copy(allocationGroups = ps.allocationGroups.filter(_ != parentId.uuid))
        val reqs = List(KeyRevision(StoragePoolState.ConfigKey, psKvos.contents(StoragePoolState.ConfigKey).revision))
        val ops = List(Insert(StoragePoolState.ConfigKey, nps.encode()))

        tx.update(psPtr, None, None, reqs, ops)

        val nags = ags.copy(members = ags.members.filter(_.uuid != ps.poolId.uuid))

        tx.overwrite(agsPtr, agsDos.revision, DataBuffer(nags.toBytes))

        Some((nps, nags))

    modifyPool(client, poolId, parentId, taskExecutor, mod)

  /** Enroll the usage-cascade task for `childUUID` (a pool or child group) atomically within
   *  the current transaction, when the group has parent groups. Uses the supplied TaskExecutor
   *  when present, otherwise the system durable task path. */
  private def prepareUsageTask(client: AspenClient,
                              childUUID: UUID,
                              nags: AllocationGroupState,
                              taskExecutor: Option[TaskExecutor])
                             (using tx: Transaction): Future[Unit] =
    given ExecutionContext = client.clientContext
    if nags.parentGroups.nonEmpty then
      taskExecutor match
        case Some(exec) =>
          UpdateAllocationGroupUsageTask.prepareTask(childUUID,
            nags.currentUsage, nags.maximumSize, nags.parentGroups.map(_.uuid), exec).map(_ => ())
        case None =>
          UpdateAllocationGroupUsageTask.prepareSystemTask(client, childUUID,
            nags.currentUsage, nags.maximumSize, nags.parentGroups.map(_.uuid))
    else
      Future.unit

  private def modifyPool(client: AspenClient,
                         poolId: PoolId,
                         parentId: AllocationGroupId,
                         taskExecutor: Option[TaskExecutor],
                         mod: (KeyValueObjectPointer, KeyValueObjectState, DataObjectPointer, DataObjectState,
                           StoragePoolState, AllocationGroupState,
                           Transaction) => Option[(StoragePoolState, AllocationGroupState)]
                        ): Future[Unit] =
    given ExecutionContext = client.clientContext

    def prep(tx: Transaction): Future[Unit] =
      given Transaction = tx
      for
        psPtr <- client.getStoragePoolPointer(poolId)
        agsPtr <- client.getAllocationGroupPointer(parentId)
        psKvos <- client.read(psPtr)
        agsDos <- client.read(agsPtr)
        ps = StoragePoolState(psKvos)
        ags = AllocationGroupState(agsDos)
        _ <- mod(psPtr, psKvos, agsPtr, agsDos, ps, ags, tx) match
          case Some((_, nags)) => prepareUsageTask(client, poolId.uuid, nags, taskExecutor)
          case None            => Future.unit
      yield ()

    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      prep(tx)


  def addGroup(client: AspenClient,
               childId: AllocationGroupId,
               parentId: AllocationGroupId,
               taskExecutor: Option[TaskExecutor] = None): Future[Unit] =

    def mod(childPtr: DataObjectPointer, childDos: DataObjectState,
            parentPtr: DataObjectPointer, parentDos: DataObjectState,
            child: AllocationGroupState, parent: AllocationGroupState,
            tx: Transaction): Option[(AllocationGroupState, AllocationGroupState)] =
      if child.parentGroups.exists(_.uuid == parentId.uuid) && parent.members.exists(_.uuid == childId.uuid) then
        None
      else
        if child.level >= parent.level then
          throw new InvalidLevel()

        checkMemberLimit(child.groupId.uuid, child.maxSliceSize, parent)
        checkMemberIda(child.groupId.uuid, child.ida, parent)

        val nchild = child.copy(parentGroups = parentId :: child.parentGroups)

        tx.overwrite(childPtr, childDos.revision, DataBuffer(nchild.toBytes))

        val nags = parent.copy(members = Member.forGroup(child) :: parent.members)

        tx.overwrite(parentPtr, parentDos.revision, DataBuffer(nags.toBytes))

        Some((nchild, nags))

    modifyGroup(client, childId, parentId, taskExecutor, mod)

  def removeGroup(client: AspenClient,
                  childId: AllocationGroupId,
                  parentId: AllocationGroupId,
                  taskExecutor: Option[TaskExecutor] = None): Future[Unit] =

    def mod(childPtr: DataObjectPointer, childDos: DataObjectState,
            parentPtr: DataObjectPointer, parentDos: DataObjectState,
            child: AllocationGroupState, parent: AllocationGroupState,
            tx: Transaction): Option[(AllocationGroupState, AllocationGroupState)] =
      if !child.parentGroups.exists(_.uuid == parentId.uuid) && !parent.members.exists(_.uuid == childId.uuid) then
        None
      else
        val nchild = child.copy(parentGroups = child.parentGroups.filter(_ != parentId))

        tx.overwrite(childPtr, childDos.revision, DataBuffer(nchild.toBytes))

        val nparent = parent.copy(members = parent.members.filter(_.uuid != child.groupId.uuid))

        tx.overwrite(parentPtr, parentDos.revision, DataBuffer(nparent.toBytes))

        Some((nchild, nparent))

    modifyGroup(client, childId, parentId, taskExecutor, mod)

  private def modifyGroup(client: AspenClient,
                          childId: AllocationGroupId,
                          parentId: AllocationGroupId,
                          taskExecutor: Option[TaskExecutor],
                          mod: (DataObjectPointer, DataObjectState, DataObjectPointer, DataObjectState,
                            AllocationGroupState, AllocationGroupState,
                            Transaction) => Option[(AllocationGroupState, AllocationGroupState)]
                         ): Future[Unit] =
    given ExecutionContext = client.clientContext

    def prep(tx: Transaction): Future[Unit] =
      given Transaction = tx

      for
        childPtr <- client.getAllocationGroupPointer(childId)
        parentPtr <- client.getAllocationGroupPointer(parentId)
        childDos <- client.read(childPtr)
        parentDos <- client.read(parentPtr)
        child = AllocationGroupState(childDos)
        parent = AllocationGroupState(parentDos)
        _ <- mod(childPtr, childDos, parentPtr, parentDos, child, parent, tx) match
          case Some((_, nparent)) => prepareUsageTask(client, childId.uuid, nparent, taskExecutor)
          case None               => Future.unit
      yield ()

    client.transactUntilSuccessfulWithRecovery(StopRetrying.unlessCollision): tx =>
      prep(tx)


/** @param idaPolicy whether members must share the group's IDA. Fixed when the group is created.
  * @param ida       the IDA every member must equal: `Some` exactly when `idaPolicy` is `ExactMatch`. Fixed when the
  *                  group is created.
  * @param maxSliceSize the group's slice limit, or `None` for no limit. Fixed when the group is created and allowed
  *                     only with the `ExactMatch` policy. It is a floor: every pool below the group accepts slices
  *                     at least this large, so only pools and groups with no limit or one at least as large may
  *                     join, and an unlimited group accepts only unlimited members. Most code should use the derived
  *                     `maxObjectSize` instead.
  */
final case class AllocationGroupState(
                                     groupId: AllocationGroupId,
                                     level: Int,
                                     name: String,
                                     members: List[AllocationGroupState.Member],
                                     parentGroups: List[AllocationGroupId],
                                     maxSliceSize: Option[Int] = None,
                                     idaPolicy: IDAPolicy = IDAPolicy.AllowAll,
                                     ida: Option[IDA] = None
                                     ):

  private var wselector: Option[WeightedSelector[AllocationGroupState.Member]] = None

  def selectMemberForAllocation(): Option[AllocationGroupState.Member] =
    val ws = synchronized:
      wselector match
        case Some(o) => o
        case None =>
          wselector = Some(new WeightedSelector[AllocationGroupState.Member](members.map: m =>
            if m.maximumSize == 0 then
              // maximumSize of 0 means unconfigured/unlimited
              (m, 1.0)
            else
              val freeSpace = m.maximumSize - m.currentUsage
              if freeSpace > 0 then
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
  
  /** Largest object, in bytes, that can be allocated through this group, or `None` for no limit. Derived from
    * `maxSliceSize` and the group's IDA: every pool below the group accepts an object of this size.
    */
  def maxObjectSize: Option[Int] =
    for
      limit <- maxSliceSize
      i <- ida
    yield i.maxObjectSize(limit)
