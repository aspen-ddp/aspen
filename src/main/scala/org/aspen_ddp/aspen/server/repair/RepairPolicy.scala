package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.StoragePoolState
import org.aspen_ddp.aspen.common.pool.PoolId

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MINUTES, SECONDS}

/** Pool-scoped repair timing.
  *
  * Timing belongs to the pool rather than the host because staleness risk is a property of the
  * pool's IDA: how many slices it can lose before data is unrecoverable. The host-scoped
  * companion to this record is HostRepairLimits, which bounds load instead.
  *
  * @param scanIntervalFloor            fastest a single store is scanned
  * @param scanIntervalCap              slowest a single store is scanned, reached by repeated
  *                                     doubling after clean scans
  * @param minErrorEntryAgeForDeletion  how long an errorTree or UnackedCommits entry must sit before its
  *                                     object's absence from the allocationTree leads to a read
  *                                     of the object (StoreRepairer.repairAbsent). Absence alone
  *                                     never deletes: only a read failing
  *                                     InvalidObject.provesDeletion drops the slice
  * @param proofAfterDeferrals          passes since its last complete pass (stale passes neither count nor reset
  *                                     it) a repair entry's object may have parts deferred by a lock before the
  *                                     scan proves them with a transaction instead of waiting for the
  *                                     lock to clear (RepairProofs)
  * @param bootstrapSweepInterval       how often a store of the bootstrap pool sweeps its copies of the bootstrap
  *                                     pool's own tree nodes (StoreRepairer.sweepBootstrapTrees), independent of
  *                                     its scan back-off. Writes to those nodes run untracked, so the sweep is the
  *                                     only thing that heals a missed one, and this bounds how long it stays
  *                                     unhealed. Read only from the bootstrap pool's policy; like the floor, a value
  *                                     below RepairService.TickPeriod acts as the tick
  */
case class RepairPolicy(scanIntervalFloor: Duration,
                        scanIntervalCap: Duration,
                        minErrorEntryAgeForDeletion: Duration,
                        proofAfterDeferrals: Int = RepairPolicy.DefaultProofAfterDeferrals,
                        bootstrapSweepInterval: Duration = RepairPolicy.DefaultBootstrapSweepInterval):

  def encode(): Array[Byte] = codec.RepairPolicy(
    scanIntervalFloorSeconds = scanIntervalFloor.toSeconds.toInt,
    scanIntervalCapSeconds = scanIntervalCap.toSeconds.toInt,
    minErrorEntryAgeForDeletionSeconds = minErrorEntryAgeForDeletion.toSeconds.toInt,
    proofAfterDeferrals = proofAfterDeferrals,
    bootstrapSweepIntervalSeconds = bootstrapSweepInterval.toSeconds.toInt
  ).toByteArray

object RepairPolicy:

  /** 5 minutes of deferral at the 30 s scan floor */
  val DefaultProofAfterDeferrals: Int = 10

  /** One walk of the bootstrap pool's small trees per minute per bootstrap store bounds a missed untracked write's
    * staleness window (repair-process 10.1)
    */
  val DefaultBootstrapSweepInterval: Duration = Duration(60, SECONDS)

  val Default: RepairPolicy = RepairPolicy(
    scanIntervalFloor = Duration(30, SECONDS),
    scanIntervalCap = Duration(15, MINUTES),
    minErrorEntryAgeForDeletion = Duration(60, SECONDS))

  /** proto3 scalars read as zero when the field was never written, which is also how a record
    * written by an older version looks after a field is added. Zero therefore means "unset" and
    * yields the default rather than an interval of zero, which would busy-loop the sweep.
    */
  def decode(bytes: Array[Byte]): RepairPolicy =
    val m = codec.RepairPolicy.parseFrom(bytes)

    def seconds(value: Int, dflt: Duration): Duration =
      if value <= 0 then dflt else Duration(value, SECONDS)

    RepairPolicy(
      seconds(m.scanIntervalFloorSeconds, Default.scanIntervalFloor),
      seconds(m.scanIntervalCapSeconds, Default.scanIntervalCap),
      seconds(m.minErrorEntryAgeForDeletionSeconds, Default.minErrorEntryAgeForDeletion),
      if m.proofAfterDeferrals <= 0 then Default.proofAfterDeferrals else m.proofAfterDeferrals,
      seconds(m.bootstrapSweepIntervalSeconds, Default.bootstrapSweepInterval))

  /** Reads the pool's policy. An absent key yields Default -- no pool has this key today and
    * nothing is required to write one, so repair works with zero administration. A failed read
    * fails this future; RepairService is what decides to fall back, since only it knows the
    * last-known-good value.
    */
  def read(client: AspenClient, poolId: PoolId)(using ec: ExecutionContext): Future[RepairPolicy] =
    for
      ptr <- client.getStoragePoolPointer(poolId)
      kvos <- client.read(ptr, "repair policy")
    yield
      kvos.contents.get(StoragePoolState.RepairPolicyKey) match
        case Some(vs) => decode(vs.value.bytes)
        case None => Default
