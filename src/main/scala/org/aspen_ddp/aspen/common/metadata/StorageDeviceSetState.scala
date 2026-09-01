package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.{AllocationError, AspenClient, DataObjectState, ReadError, StopRetrying, Transaction}
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.objects.Insert
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.KeyRevision
import org.aspen_ddp.aspen.common.util.{byte2long, runBoundedParallel}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

final case class StorageDeviceSetId(uuid: UUID) extends AnyVal

object StorageDeviceSetId:
  val BootstrapStorageDeviceSetId: StorageDeviceSetId = fixed_ids.BootstrapStorageDeviceSetId

object StorageDeviceSetState:
  def apply(cfg: Array[Byte]): StorageDeviceSetState =
    Codec.decode(codec.StorageDeviceSetState.parseFrom(cfg))

  def apply(dos: DataObjectState): StorageDeviceSetState = apply(dos.data.getByteArray)

  /** Cap on concurrent member-set reads while walking a hierarchy. Bounds load on large trees. */
  private[metadata] val MaxConcurrentSetReads: Int = 20

  /** Thrown when attempting to move a device into a set whose level is not 0.
   *  Only level-0 sets hold devices. */
  class NotLevelZero(val setId: StorageDeviceSetId)
      extends Throwable(s"Storage device set ${setId.uuid} is not level 0")

  /** Move `deviceId` from its current level-0 set into `targetSetId` (which must also be
   *  level 0), in a single atomic transaction updating three objects: the device KV
   *  state's `storageDeviceSet` field, the target set's `memberDevices` (device added),
   *  and the old set's `memberDevices` (device removed).
   *
   *  No-op if the device is already in the target set. Fails with NoSuchElementException
   *  if the device or a set object cannot be found, or NotLevelZero if the target set is
   *  not level 0. The old set is updated even if it did not actually list the device
   *  (self-healing toward the correct final state). */
  def moveDevice(client: AspenClient,
                 deviceId: StorageDeviceId,
                 targetSetId: StorageDeviceSetId): Future[Unit] =
    given ExecutionContext = client.clientContext

    def prep(tx: Transaction): Future[Unit] =
      for
        devPtr <- client.getStorageDevicePointer(deviceId)
        devKvos <- client.read(devPtr)
        deviceState = StorageDeviceState(devKvos)
        oldSetId = deviceState.storageDeviceSet
        targetPtr <- client.getStorageDeviceSetPointer(targetSetId)
        targetDos <- client.read(targetPtr)
        target = StorageDeviceSetState(targetDos)
        _ <-
          if target.level != 0 then
            throw new NotLevelZero(targetSetId)
          else if oldSetId == targetSetId then
            Future.unit
          else
            for
              oldPtr <- client.getStorageDeviceSetPointer(oldSetId)
              oldDos <- client.read(oldPtr)
              oldSet = StorageDeviceSetState(oldDos)
            yield
              // Device KV state -> point at the target set
              val newDeviceState = deviceState.copy(storageDeviceSet = targetSetId)
              val devReqs = List(KeyRevision(StorageDeviceState.StateKey,
                devKvos.contents(StorageDeviceState.StateKey).revision))
              val devOps = List(Insert(StorageDeviceState.StateKey, newDeviceState.encode()))
              tx.update(devPtr, None, None, devReqs, devOps)

              // Target set -> add the device (dedup-guarded)
              val newTarget = target.copy(
                memberDevices = deviceId :: target.memberDevices.filter(_ != deviceId))
              tx.overwrite(targetPtr, targetDos.revision, DataBuffer(newTarget.toBytes))

              // Old set -> remove the device (self-healing if absent)
              val newOld = oldSet.copy(
                memberDevices = oldSet.memberDevices.filter(_ != deviceId))
              tx.overwrite(oldPtr, oldDos.revision, DataBuffer(newOld.toBytes))
      yield ()

    def onFail(err: Throwable): Future[Unit] = err match
      case e: NoSuchElementException => throw StopRetrying(e)
      case e: ReadError => throw StopRetrying(e)
      case e: NotLevelZero => throw StopRetrying(e)

    client.transactUntilSuccessfulWithRecovery(onFail): tx =>
      prep(tx)

/** Hierarchical grouping of physical storage, parallel to AllocationGroupState.
 *
 *  The `level` strictly determines the member type:
 *    - level == 0 : a set of devices. `memberDevices` may be non-empty; `memberSets` must be empty.
 *    - level >= 1 : a set of sets.   `memberSets` may be non-empty; `memberDevices` must be empty.
 *
 *  `parent`, if present, must reference a set of a higher level. These invariants are
 *  documented here but enforcement (the mutation/management API) is out of scope for this pass.
 */
final case class StorageDeviceSetState(
    setId: StorageDeviceSetId,
    name: String,
    level: Int,
    parent: Option[StorageDeviceSetId],
    memberDevices: List[StorageDeviceId],
    memberSets: List[StorageDeviceSetId],
    assignedPools: List[PoolId],
    pendingTransfers: List[(StoreId, StorageDeviceId, StorageDeviceId)] = Nil
):
  def toBytes: Array[Byte] = Codec.encode(this).toByteArray

  /** Select `numStores` device ids for a pool, walking this set's hierarchy.
   *
   *  Level-0 sets draw from their member devices. Level-1+ sets distribute the
   *  request evenly across their member sets and recurse. `rng` is injectable so
   *  tests can be deterministic.
   */
  def selectDevicesForPool(
      numStores: Int,
      client: AspenClient,
      rng: Random = new Random()
  ): Future[List[StorageDeviceId]] =
    given ExecutionContext = client.clientContext
    selectDevices(numStores, Set.empty, client.getStorageDeviceSetState, rng)

  /** Select a device to host a store that must be rebuilt from scratch because its
   *  data was lost. The store currently at `failedIndex` in pool `poolId` is the one
   *  being rebuilt; its current size (the pool's per-store usage record, or 0 if absent)
   *  is the amount of free space a candidate device must have.
   *
   *  Only valid for level-0 sets; level-1+ sets fail with `AllocationError` without any
   *  reads. The pool object is read directly via its pointer so the per-store size key
   *  and the config come from a single read. See `selectRebuildDevice` for the selection
   *  policy (failed device hard-excluded, other pool devices soft-excluded, free space
   *  required). All failures are `Future.failed(AllocationError(...))`.
   */
  def selectDeviceForRebuild(
      poolId: PoolId,
      failedIndex: Byte,
      client: AspenClient,
      rng: Random = new Random()
  ): Future[StorageDeviceId] =
    given ExecutionContext = client.clientContext
    if level != 0 then
      Future.failed(AllocationError(
        s"selectDeviceForRebuild only supports level-0 sets; set ${setId.uuid} is level $level"))
    else if memberDevices.isEmpty then
      Future.failed(AllocationError(
        s"StorageDeviceSet ${setId.uuid} (level 0) has no member devices"))
    else
      for
        poolPtr <- client.getStoragePoolPointer(poolId)
        poolKvos <- client.read(poolPtr)
        poolState = StoragePoolState(poolKvos)
        device <-
          if failedIndex < 0 || failedIndex >= poolState.stores.length then
            Future.failed(AllocationError(
              s"failedIndex $failedIndex out of range for pool ${poolId.uuid} (${poolState.stores.length} stores)"))
          else
            val requiredSize = poolKvos.contents
              .get(StoragePoolState.getStoreUsageKey(failedIndex))
              .map(vs => byte2long(vs.value.bytes))
              .getOrElse(0L)
            val failedDevice = poolState.stores(failedIndex).storageDeviceId
            val poolDevices = poolState.stores.map(_.storageDeviceId).toSet
            selectRebuildDevice(
              requiredSize,
              failedDevice,
              poolDevices,
              id => client.getStorageDeviceState(id).map(s => s.totalSize - s.currentUsage),
              rng)
      yield device

  /** Select a device within this set (at any level) to receive a store being migrated here.
   *
   *  `requiredSize` is the store's current size and is a hard requirement. `exclude` holds the
   *  devices already hosting stores of the same pool; they are soft-excluded, so a target set
   *  narrower than the pool's IDA width still works. Fails with `AllocationError` when no
   *  device in the tree has room.
   */
  def selectDeviceForStore(
      requiredSize: Long,
      exclude: Set[StorageDeviceId],
      client: AspenClient,
      rng: Random = new Random()
  ): Future[StorageDeviceId] =
    given ExecutionContext = client.clientContext
    selectDeviceWithSpace(
      requiredSize = requiredSize,
      hardExclude = Set.empty,
      softExclude = exclude,
      lookup = client.getStorageDeviceSetState,
      freeSpaceLookup = id => client.getStorageDeviceState(id).map(s => s.totalSize - s.currentUsage),
      rng = rng)

  /** Every device reachable from this set: `memberDevices` at level 0, the union of the
   *  members' walks above. This is the "is this store already in the target set?" predicate
   *  used by pool migration.
   *
   *  The walk is downward. The cheaper upward walk (device -> its set -> follow `parent`) is
   *  deliberately not used: `parent` is documented as unenforced and `moveDevice` never
   *  maintains it, and -- more decisively -- selection walks downward, so an upward done-check
   *  could fail to recognize a device that selection had just placed a store on, which is an
   *  infinite migration loop rather than a stale read.
   */
  def collectMemberDevices(client: AspenClient): Future[Set[StorageDeviceId]] =
    given ExecutionContext = client.clientContext
    collectDevices(client.getStorageDeviceSetState)

  /** Recursive core of `collectMemberDevices`, depending only on a narrow `lookup` so it is
   *  unit-testable without a full `AspenClient`. */
  private[metadata] def collectDevices(
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState]
  )(using ec: ExecutionContext): Future[Set[StorageDeviceId]] =
    if level == 0 then
      Future.successful(memberDevices.toSet)
    else
      runBoundedParallel(memberSets, StorageDeviceSetState.MaxConcurrentSetReads): id =>
        lookup(id).flatMap(_.collectDevices(lookup))
      .map(_.foldLeft(Set.empty[StorageDeviceId])(_ ++ _))

  /** Recursive core. Depends only on a narrow `lookup` so it is unit-testable
   *  without a full `AspenClient`. `exclude` carries device ids already chosen
   *  earlier in the overall selection, enabling best-effort deduplication.
   */
  private[metadata] def selectDevices(
      numStores: Int,
      exclude: Set[StorageDeviceId],
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
      rng: Random
  )(using ec: ExecutionContext): Future[List[StorageDeviceId]] =
    if numStores <= 0 then
      Future.successful(Nil)
    else if level == 0 then
      selectFromDevices(numStores, exclude, rng)
    else
      selectFromSets(numStores, exclude, lookup, rng)

  private def selectFromDevices(
      numStores: Int,
      exclude: Set[StorageDeviceId],
      rng: Random
  ): Future[List[StorageDeviceId]] =
    if memberDevices.isEmpty then
      Future.failed(AllocationError(s"StorageDeviceSet ${setId.uuid} (level 0) has no member devices"))
    else
      val shuffled = rng.shuffle(memberDevices)
      val preferred = shuffled.filterNot(exclude.contains)
      val ordered = preferred.iterator ++ Iterator.continually(shuffled).flatten
      Future.successful(ordered.take(numStores).toList)

  private def selectFromSets(
      numStores: Int,
      exclude: Set[StorageDeviceId],
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
      rng: Random
  )(using ec: ExecutionContext): Future[List[StorageDeviceId]] =
    if memberSets.isEmpty then
      Future.failed(AllocationError(s"StorageDeviceSet ${setId.uuid} (level $level) has no member sets"))
    else
      val shuffled = rng.shuffle(memberSets)
      // Round-robin visiting order gives per-member-set counts differing by <= 1.
      val visits = Iterator.continually(shuffled).flatten.take(numStores).toList

      // Sequential fold so each visit sees devices chosen by earlier visits,
      // threading the growing exclusion set for best-effort deduplication.
      val folded = visits.foldLeft(Future.successful((List.empty[StorageDeviceId], exclude))):
        (accF, memberSetId) =>
          accF.flatMap: (results, excludeSoFar) =>
            lookup(memberSetId).flatMap: subState =>
              subState.selectDevices(1, excludeSoFar, lookup, rng).map: chosen =>
                val device = chosen.head
                (results :+ device, excludeSoFar + device)

      folded.map(_._1)

  /** Select a device with at least `requiredSize` free bytes, at any level.
   *
   *  Level 0 draws from `memberDevices`; level 1+ shuffles `memberSets` and recurses until
   *  one yields a device. `hardExclude` devices are never chosen. `softExclude` devices are
   *  tried only after every non-excluded candidate has been rejected — soft rather than hard
   *  so that a set narrower than the IDA width still works, for the same reason
   *  `selectDevicesForPool` cycles its device list.
   *
   *  Selection is optimistic and lazy: candidates are tried in random order, reading each
   *  device's free space one at a time via `freeSpaceLookup`, stopping at the first that fits.
   *  A candidate whose lookup fails is skipped. Depends only on `lookup` and `freeSpaceLookup`
   *  so it is unit-testable without a full `AspenClient`. `rng` is injectable so tests can be
   *  deterministic.
   */
  private[metadata] def selectDeviceWithSpace(
      requiredSize: Long,
      hardExclude: Set[StorageDeviceId],
      softExclude: Set[StorageDeviceId],
      lookup: StorageDeviceSetId => Future[StorageDeviceSetState],
      freeSpaceLookup: StorageDeviceId => Future[Long],
      rng: Random
  )(using ec: ExecutionContext): Future[StorageDeviceId] =
    def exhausted: Future[StorageDeviceId] =
      Future.failed(AllocationError(
        s"no device in set ${setId.uuid} has >= $requiredSize free bytes available"))

    if level == 0 then
      if memberDevices.isEmpty then
        Future.failed(AllocationError(s"StorageDeviceSet ${setId.uuid} (level 0) has no member devices"))
      else
        val eligible = memberDevices.filterNot(hardExclude.contains)
        val preferred = rng.shuffle(eligible.filterNot(softExclude.contains))
        val fallback = rng.shuffle(eligible.filter(softExclude.contains))

        def scan(remaining: List[StorageDeviceId]): Future[StorageDeviceId] =
          remaining match
            case Nil => exhausted
            case head :: tail =>
              freeSpaceLookup(head).transformWith:
                case Success(free) if free >= requiredSize => Future.successful(head)
                case _ => scan(tail)

        scan(preferred ++ fallback)
    else
      if memberSets.isEmpty then
        Future.failed(AllocationError(s"StorageDeviceSet ${setId.uuid} (level $level) has no member sets"))
      else
        def scanSets(remaining: List[StorageDeviceSetId]): Future[StorageDeviceId] =
          remaining match
            case Nil => exhausted
            case head :: tail =>
              lookup(head)
                .flatMap(sub => sub.selectDeviceWithSpace(
                  requiredSize, hardExclude, softExclude, lookup, freeSpaceLookup, rng))
                .transformWith:
                  case Success(device) => Future.successful(device)
                  case Failure(_) => scanSets(tail)

        scanSets(rng.shuffle(memberSets))

  /** Select a device to host a store rebuilt from scratch (its data was lost).
   *
   *  Only valid for level-0 sets. The failed device is hard-excluded; devices already hosting
   *  a store in the pool (`poolDevices`) are soft-excluded. This is `selectDeviceWithSpace`
   *  with the level-0 restriction retained -- the refactor beneath it is behavior-preserving,
   *  not a widening of it.
   */
  private[metadata] def selectRebuildDevice(
      requiredSize: Long,
      failedDevice: StorageDeviceId,
      poolDevices: Set[StorageDeviceId],
      freeSpaceLookup: StorageDeviceId => Future[Long],
      rng: Random
  )(using ec: ExecutionContext): Future[StorageDeviceId] =
    if level != 0 then
      Future.failed(AllocationError(
        s"selectDeviceForRebuild only supports level-0 sets; set ${setId.uuid} is level $level"))
    else
      selectDeviceWithSpace(
        requiredSize,
        hardExclude = Set(failedDevice),
        softExclude = poolDevices,
        lookup = _ => Future.failed(new IllegalStateException("a level-0 set must not recurse")),
        freeSpaceLookup = freeSpaceLookup,
        rng = rng)
