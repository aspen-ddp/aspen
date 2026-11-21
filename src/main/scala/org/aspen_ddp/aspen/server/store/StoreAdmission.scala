package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.network.TxPrepare
import org.aspen_ddp.aspen.common.objects.ObjectId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{ObjectUpdate, TransactionId}
import org.aspen_ddp.aspen.common.util.LogRateLimit
import org.aspen_ddp.aspen.server.crl.{AdmissionTier, CrashRecoveryLog, CrlAdmission, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.transaction.TransactionStatusCache
import scribe.Logging

/** One store's share of its device's CRL admission (`CrlAdmission`): what each transaction it admitted holds, and what
  * its applied stubs are charged. Owned by the store's executor, through its Frontend.
  *
  * @param holdsTx whether the Frontend holds a Tx for a transaction
  * @param oversizedLogPeriod a dropped oversized prepare is warned of once per period at most
  */
final class StoreAdmission(storeId: StoreId,
                           admission: CrlAdmission,
                           tier: AdmissionTier,
                           crl: CrashRecoveryLog,
                           statusCache: TransactionStatusCache,
                           holdsTx: TransactionId => Boolean,
                           oversizedLogPeriod: Long,
                           wallClockMillis: () => Long) extends Logging:

  /** What a transaction this Frontend admitted holds in the device's CRL admission, until released.
    *
    * @param bytes      the bytes charged so far
    * @param dataCounted the objects whose update data is charged: each is charged once, however often it arrives
    * @param serializedTxd the description serialized to size it, kept for the Tx's recovery state until taken
    */
  private final class Admitted(var bytes: Long, var dataCounted: Set[ObjectId], var serializedTxd: Option[DataBuffer])

  private val admitted = scala.collection.mutable.HashMap[TransactionId, Admitted]()

  /** What the stubs are charged to the device's admission (CrlAdmission.AppliedStubBytes each), until released */
  private var stubBytesCharged: Long = 0L

  /** What the transactions whose CRL state a stub replaces were admitted for, released once the stub is durable */
  private val stubReplacedAdmissions = scala.collection.mutable.HashMap[TransactionId, Long]()

  private val oversizedLog = new LogRateLimit(oversizedLogPeriod, wallClockMillis)

  /** Whether a prepare may be handled.
    *
    * A transaction this Frontend admitted (recovered ones included) is, unless its prepare brings object data not yet
    * counted: that data is charged only while the device's admission is below this store's tier's ceiling, as a new
    * transaction is, and the prepare is dropped (Full) otherwise (crash-recovery-log 6.1, A1). Charged unchecked,
    * data arriving after any number of data-less admissions made below the ceiling could each bring up to
    * `maxTransactionBytes`. The client retransmits; a data-less prepare of it is still handled. A known transaction
    * with no admission record is admitted too: it is one whose CRL state was deleted, which saves nothing more and
    * answers with its outcome (Tx.stateDeleted, A4). So is one finalized in the status cache, which is answered from
    * the cache and never reaches the CRL. Any other is counted for its description, object data and CRL record, and
    * admitted only while the device's admission is below this store's tier's ceiling (CrlAdmission lanes: a metadata
    * or bootstrap store keeps room for the marker batches data transactions finalize with). The status cache is
    * host-wide, so a transaction merely started on another store is not known here.
    *
    * Oversized: the transaction's counted bytes, with data already counted for an admitted one, would exceed
    * `crl.maxTransactionBytes`, so saving it would kill the CRL (crash-recovery-log 10.20). Nothing is charged. Data
    * arriving after a data-less prepare is dropped the same way, leaving the store as one that never received it: a commit elsewhere
    * reaches it through missed-update repair, and the data-less admission is released by the usual paths
    */
  def admitPrepare(m: TxPrepare): PrepareAdmission =
    val result = check(m)
    // A full lane asks the CRL to grow (idempotent, any thread); the retransmitted prepare meets the raised limit
    if result == PrepareAdmission.Full then
      crl.requestGrowth()
    result

  private def check(m: TxPrepare): PrepareAdmission =
    val txid = m.txd.transactionId
    admitted.get(txid) match
      case Some(a) =>
        val added = uncounted(a, m.objectUpdates)
        val bytes = dataBytes(added)
        if added.isEmpty then
          PrepareAdmission.Admitted
        else if a.bytes + bytes > crl.maxTransactionBytes then
          oversized(txid, a.bytes + bytes)
        else if admission.tryAdmit(bytes, tier) then
          countData(a, added, bytes)
          PrepareAdmission.Admitted
        else
          PrepareAdmission.Full
      case None if holdsTx(txid) || statusCache.getStatus(txid).exists(_.finalized) =>
        PrepareAdmission.Admitted
      // Checked before serializing: a prepare that will be dropped costs nothing more
      case None if admission.full(tier) => PrepareAdmission.Full
      case None =>
        val stxd = m.txd.serialize()
        val bytes = CrlAdmission.TxOverheadBytes + stxd.size + dataBytes(m.objectUpdates)
        if bytes > crl.maxTransactionBytes then
          oversized(txid, bytes)
        else if admission.tryAdmit(bytes, tier) then
          admitted(txid) = new Admitted(bytes, m.objectUpdates.iterator.map(_.objectId).toSet, Some(stxd))
          PrepareAdmission.Admitted
        else
          PrepareAdmission.Full

  private def oversized(txid: TransactionId, bytes: Long): PrepareAdmission =
    oversizedLog.admit().foreach: held =>
      logger.warn(s"Store $storeId drops transaction $txid: its $bytes CRL bytes here exceed the device CRL's " +
                  s"${crl.maxTransactionBytes} per transaction, so it can never commit with this store. Its " +
                  s"client writes too much to one store in one transaction${LogRateLimit.heldText(held)}")
    PrepareAdmission.Oversized

  /** The CRL bytes CrlAdmission counts for the object data of `updates` */
  private def dataBytes(updates: List[ObjectUpdate]): Long =
    updates.iterator.map(ou => CrlAdmission.objectUpdateBytes(ou.data.size)).sum

  /** The updates of `updates` whose data `a` has not counted yet */
  private def uncounted(a: Admitted, updates: List[ObjectUpdate]): List[ObjectUpdate] =
    updates.filterNot(ou => a.dataCounted.contains(ou.objectId))

  /** Charges `a` for the data of `updates` not counted yet, without a limit check: only for data the CRL already
    * holds (admitRecovered) */
  private def chargeData(a: Admitted, updates: List[ObjectUpdate]): Unit =
    val added = uncounted(a, updates)
    if added.nonEmpty then
      val bytes = dataBytes(added)
      admission.charge(bytes)
      countData(a, added, bytes)

  /** Records that `a` is charged `bytes` for the data of `added` */
  private def countData(a: Admitted, added: List[ObjectUpdate], bytes: Long): Unit =
    a.bytes += bytes
    a.dataCounted ++= added.iterator.map(_.objectId)

  /** Counts a transaction recovered from the CRL, without a limit check (the CRL already holds it), as admitPrepare
    * counts a new one: repeated restarts under load must not stack uncounted live bytes in the device CRL. One whose
    * prepare was admitted while the store awaited its saved state is charged only for data not yet counted
    */
  def admitRecovered(txid: TransactionId, trs: TransactionRecoveryState): Unit =
    admitted.get(txid) match
      case Some(a) => chargeData(a, trs.objectUpdates)
      case None =>
        // As the device's open counted it (CrashRecoveryLog.recoveredBytesByStore), which this replaces
        val a = new Admitted(CrlAdmission.recoveredBytes(trs), trs.objectUpdates.iterator.map(_.objectId).toSet, None)
        admission.charge(a.bytes)
        admitted(txid) = a

  /** The description `admitPrepare` serialized for `txid`, if not taken yet */
  def takeSerializedTxd(txid: TransactionId): Option[DataBuffer] =
    admitted.get(txid).flatMap: a =>
      val s = a.serializedTxd
      a.serializedTxd = None
      s

  /** Releases what `txid` was admitted for, if anything: its CRL entry was deleted, or it is forgotten without one.
    * Idempotent
    */
  def release(txid: TransactionId): Unit =
    admitted.remove(txid).foreach(a => admission.release(a.bytes))

  /** A stub replaces `txid`'s CRL state: what it was admitted for is released once the stub is durable
    * (`stubDurable`) */
  def replacedByStub(txid: TransactionId): Unit =
    admitted.remove(txid).foreach: a =>
      stubReplacedAdmissions(txid) = stubReplacedAdmissions.getOrElse(txid, 0L) + a.bytes

  /** The stub that replaced `txid`'s CRL state is durable */
  def stubDurable(txid: TransactionId): Unit =
    stubReplacedAdmissions.remove(txid).foreach(admission.release)

  def chargeStubs(n: Int): Unit =
    val bytes = n * CrlAdmission.AppliedStubBytes
    admission.charge(bytes)
    stubBytesCharged += bytes

  def releaseStub(): Unit =
    val bytes = math.min(CrlAdmission.AppliedStubBytes, stubBytesCharged)
    admission.release(bytes)
    stubBytesCharged -= bytes

  /** Releases everything this store's transactions were admitted for: it was built and never served. A store that
    * served retains them instead (retain): its records may be live in the CRL */
  def releaseAll(): Unit =
    admitted.valuesIterator.foreach(a => admission.release(a.bytes))
    admitted.clear()
    stubReplacedAdmissions.valuesIterator.foreach(admission.release)
    stubReplacedAdmissions.clear()
    admission.release(stubBytesCharged)
    stubBytesCharged = 0L

  /** Moves everything this store's transactions and stubs were admitted for into what the device's admission
    * retains for the store (CrlAdmission.retain), still counted: the store is closing or replaced, and its entries
    * stay live in the CRL until they leave it or the store's next recovery counts them afresh (recoverState). A stub
    * not yet durable has not replaced its transaction's state: that stays counted too */
  def retain(): Unit =
    admission.retain(storeId, admitted.valuesIterator.map(_.bytes).sum + stubReplacedAdmissions.valuesIterator.sum +
      stubBytesCharged)
    admitted.clear()
    stubReplacedAdmissions.clear()
    stubBytesCharged = 0L
