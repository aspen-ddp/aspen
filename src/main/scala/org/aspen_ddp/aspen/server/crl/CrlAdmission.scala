package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.metadata.fixed_ids
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

/** Backpressure for one device's CRL, shared by the device's stores (each on its own executor, so it is thread-safe).
  *
  * A store admits a prepare for a transaction it has not seen only while the bytes admitted on the device are below
  * `limitBytes` (`CrashRecoveryLog.admissionLimitBytes`), and drops it otherwise: the client retransmits, and that
  * stall is the slowdown. Admitted bytes are released when the transaction's CRL entry is deleted, or when the store
  * forgets a transaction that never reached the CRL. A store that closes, or is replaced by a reload, leaves its
  * entries live in the CRL unless its closer takes them out, so its bytes stay counted, retained for it (`retain`)
  * until they leave the CRL with all of the store's entries (`releaseRetained`: a transfer out's extraction, a
  * dropStore) or the store loads again and, having counted its recovered transactions afresh, releases them (a swap,
  * never a gap: crash-recovery-log 6.1, A2). The check is against the total before the admission, so one transaction
  * may cross the limit: the limit leaves headroom for that.
  *
  * A transaction is counted for its description, its object data (each object once; data arriving after the
  * transaction was admitted is checked against the lane as a new transaction is, A1) and the CRL's static record of
  * it (`TxOverheadBytes`, `ObjectUpdateOverheadBytes`). Transactions recovered at store load are counted the same
  * way, charged without a limit check (the CRL already holds them), so repeated restarts under load cannot stack
  * uncounted live bytes. A crl_save.log state is charged before it is saved, checked against the limit plus one
  * transaction (`tryChargeLoaded`, A3). An accepted gap: a retransmitted prepare arriving past the mark while the
  * store awaits its crl_save.log state is dropped until recovery recreates the transaction.
  *
  * Lanes (cross-document-review X5, G10): a new transaction is admitted only below its receiving store's tier's
  * ceiling (`AdmissionTier`): 3/4 of the limit for data pools, 7/8 for metadata pools, the whole limit for the bootstrap
  * pool. Finalization depends only upward: a data transaction's marker batch writes trees in a metadata pool, a
  * metadata transaction's in the bootstrap pool, and a bootstrap transaction's marker about the bootstrap pool tracks
  * no missed updates (MissedUpdateFinalizationAction.limitTracking). So lower-tier load on a shared device never
  * takes the headroom a higher tier's finalization needs. The tier is the receiving store's own pool's, so nothing in
  * the description is trusted for it. Residual: finalizers in the same tier (TKVL split, join, tail removal) can still
  * stall if that tier's own traffic fills its lane on every device of the pool.
  */
final class CrlAdmission(val limitBytes: Long):
  private val total = new AtomicLong(0)

  /** The total below which a new transaction of `tier` is admitted */
  def ceiling(tier: AdmissionTier): Long = limitBytes - tier.reserveParts * (limitBytes / tier.reserveDivisor)

  /** Admits `bytes` if the total admitted is below the ceiling of `tier` */
  @tailrec
  def tryAdmit(bytes: Long, tier: AdmissionTier = AdmissionTier.Bootstrap): Boolean =
    val current = total.get
    if current >= ceiling(tier) then
      false
    else if total.compareAndSet(current, current + bytes) then
      true
    else
      tryAdmit(bytes, tier)

  /** Adds `bytes` whatever the total: for what the CRL already holds (recovered state) and applied stubs, which
    * must not be dropped */
  def charge(bytes: Long): Unit = total.addAndGet(bytes)

  /** Whether the total is at or past the ceiling of `tier`, so no new transaction of that tier is admitted */
  def full(tier: AdmissionTier = AdmissionTier.Bootstrap): Boolean = total.get >= ceiling(tier)

  def release(bytes: Long): Unit = total.addAndGet(-bytes)

  // What closed stores' live entries are counted for, by store
  private val retained = new ConcurrentHashMap[StoreId, java.lang.Long]()

  /** Keeps `bytes`, which a closing store of the device was admitted for, counted until releaseRetained */
  def retain(storeId: StoreId, bytes: Long): Unit =
    if bytes > 0 then
      retained.merge(storeId, bytes, (a, b) => a + b)

  /** Releases what `storeId` was retained for: its entries have left the CRL, or it is loaded again. Idempotent */
  def releaseRetained(storeId: StoreId): Unit =
    val bytes = retained.remove(storeId)
    if bytes != null then
      total.addAndGet(-bytes)

  /** Charges `bytes`, which `storeId`'s entries hold in the CRL, without a check and retained for it: the device's
    * CRL is opened holding them, whether or not the store loads (crash-recovery-log F8) */
  def chargeRetained(storeId: StoreId, bytes: Long): Unit =
    if bytes > 0 then
      charge(bytes)
      retain(storeId, bytes)

  /** Charges `bytes`, the state of `storeId`'s crl_save.log about to be saved in the CRL, retained for the store, if
    * the total with them stays at most `limitBytes + slack` (crash-recovery-log 6.1, A3; the caller passes the CRL's
    * per-transaction bound). What is already retained for the store is left out of that total: the CRL can hold for a
    * store with a save file only records of that file (an extraction or a load interrupted by a crash), which the
    * load saves again in place, so the two charges count the same records until the store's recovery releases what
    * is retained (A2). Those are read once, on the control thread. For a load replacing a loaded store they may
    * include that store's live admissions (StoreRouter.install's retainAdmissions, if it ran before the read), which
    * are left out of the check too; a retain landing after the read only makes it stricter. One compare-and-set, as
    * tryAdmit: no admission between the check and the charge */
  def tryChargeLoaded(storeId: StoreId, bytes: Long, slack: Long): Boolean =
    val max = if limitBytes > Long.MaxValue - slack then Long.MaxValue else limitBytes + slack
    val own = retainedBytes(storeId)
    @tailrec
    def attempt(): Boolean =
      val current = total.get
      if current - own > max - bytes then
        false
      else if total.compareAndSet(current, current + bytes) then
        true
      else
        attempt()
    val charged = attempt()
    if charged then
      retain(storeId, bytes)
    charged

  /** What `storeId` is retained for */
  def retainedBytes(storeId: StoreId): Long = Option(retained.get(storeId)).map(_.longValue).getOrElse(0L)

  def admittedBytes: Long = total.get

/** Which admission lane a store's new transactions use, by the store's pool (CrlAdmission). The ceiling is
  * `limit - reserveParts * (limit / reserveDivisor)`, computed without overflow for any limit */
enum AdmissionTier(val reserveParts: Long, val reserveDivisor: Long):
  /** Ordinary pools: 3/4 of the limit */
  case Data extends AdmissionTier(1, 4)
  /** Metadata pools (their trees hold data pools' ErrorTree, UnackedCommits and AllocationTree): 7/8 */
  case Metadata extends AdmissionTier(1, 8)
  /** The bootstrap pool: the whole limit */
  case Bootstrap extends AdmissionTier(0, 1)

object AdmissionTier:
  /** The tier of a pool's stores */
  def forPool(poolId: PoolId, isMetadataPool: Boolean): AdmissionTier =
    if poolId == fixed_ids.BootstrapPoolId then
      Bootstrap
    else if isMetadataPool then
      Metadata
    else
      Data

object CrlAdmission:
  /** SimpleCRL's static record of a saved transaction (LogContent.staticDataSize): TxId 33, txd location 14,
    * disposition 1, Paxos state 11, object-update count 4. Rewritten with each save; the latest is the live one
    */
  val TxOverheadBytes: Long = 63

  /** SimpleCRL's record of an applied stub (Stub.StaticSize): its TxId 33 and start timestamp 8. Charged without
    * a check, so stubs are never dropped or held back, but count toward the evacuation budget new prepares are
    * admitted against */
  val AppliedStubBytes: Long = 41

  /** Per object update in that record: object UUID 16 + data location 14 */
  val ObjectUpdateOverheadBytes: Long = 30

  /** What an object update's data costs the CRL */
  def objectUpdateBytes(dataSize: Long): Long = dataSize + ObjectUpdateOverheadBytes

  /** What a transaction state the CRL holds is counted for: its record, description and object data */
  def recoveredBytes(trs: TransactionRecoveryState): Long =
    TxOverheadBytes + trs.serializedTxd.size + trs.objectUpdates.iterator.map(ou => objectUpdateBytes(ou.data.size)).sum

  /** What a store's state in the CRL is counted for: its transaction states and its applied stubs */
  def recoveredBytes(state: StoreRecoveryState): Long =
    state.transactions.iterator.map(recoveredBytes).sum + state.appliedStubs.size * AppliedStubBytes

  /** Admits everything: for tests and stores with no device */
  def Unlimited: CrlAdmission = new CrlAdmission(Long.MaxValue)
