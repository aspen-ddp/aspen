package org.aspen_ddp.aspen.common.metadata

import org.aspen_ddp.aspen.client.{AllocationError, AspenClient, DataObjectState}
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.util.byte2long

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Success}

final case class StorageDeviceSetId(uuid: UUID) extends AnyVal

object StorageDeviceSetId:
  val BootstrapStorageDeviceSetId: StorageDeviceSetId = fixed_ids.BootstrapStorageDeviceSetId

object StorageDeviceSetState:
  def apply(cfg: Array[Byte]): StorageDeviceSetState =
    Codec.decode(codec.StorageDeviceSetState.parseFrom(cfg))

  def apply(dos: DataObjectState): StorageDeviceSetState = apply(dos.data.getByteArray)

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
    assignedPools: List[PoolId]
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

  /** Select a device to host a store rebuilt from scratch (its data was lost).
   *
   *  Only valid for level-0 sets. The failed device is hard-excluded (never chosen).
   *  Devices already hosting a store in the pool (`poolDevices`) are soft-excluded:
   *  preferred candidates that are not in `poolDevices` are tried first, falling back
   *  to pool devices only if no preferred device has enough space. Free space is a hard
   *  requirement in both phases.
   *
   *  Selection is optimistic and lazy: candidates are tried in random order, reading
   *  each device's free space one at a time via `freeSpaceLookup`, stopping at the first
   *  that fits. A candidate whose lookup fails is skipped. `rng` is injectable so tests
   *  can be deterministic. Depends only on `freeSpaceLookup` so it is unit-testable
   *  without a full `AspenClient`.
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
    else if memberDevices.isEmpty then
      Future.failed(AllocationError(
        s"StorageDeviceSet ${setId.uuid} (level 0) has no member devices"))
    else
      val eligible = memberDevices.filterNot(_ == failedDevice)
      val preferred = rng.shuffle(eligible.filterNot(poolDevices.contains))
      val fallback = rng.shuffle(eligible.filter(poolDevices.contains))
      val candidates = preferred ++ fallback

      def scan(remaining: List[StorageDeviceId]): Future[StorageDeviceId] =
        remaining match
          case Nil =>
            Future.failed(AllocationError(
              s"no device in set ${setId.uuid} has >= $requiredSize free bytes available for rebuild"))
          case head :: tail =>
            freeSpaceLookup(head).transformWith:
              case Success(free) if free >= requiredSize => Future.successful(head)
              case _ => scan(tail)

      scan(candidates)
