package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.store.StoreId

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

/** Backpressure for one device's CRL, shared by the device's stores (each on its own executor, so it is thread-safe).
  *
  * A store admits a prepare for a transaction it has not seen only while the bytes admitted on the device are below
  * `limitBytes` (`CrashRecoveryLog.admissionLimitBytes`), and drops it otherwise: the client retransmits, and that
  * stall is the slowdown. Admitted bytes are released when the transaction's CRL entry is deleted, or when the store
  * forgets a transaction that never reached the CRL. A store that closes leaves its entries live in the CRL unless its
  * closer takes them out, so its bytes stay counted, retained for it (`retain`) until they leave the CRL with all of
  * the store's entries (`releaseRetained`: a transfer out's extraction, a dropStore) or the store loads again and
  * counts its recovered transactions afresh. The check is against the total before
  * the admission, so one transaction may cross the limit: the limit leaves headroom for that.
  *
  * A transaction is counted for its description, its object data (each object once, charged without a limit check
  * when it arrives after the transaction was admitted) and the CRL's static record of it (`TxOverheadBytes`,
  * `ObjectUpdateOverheadBytes`). Transactions recovered at store load are counted the same way, charged without a
  * limit check (the CRL already holds them), so repeated restarts under load cannot stack uncounted live bytes. An
  * accepted gap: a retransmitted prepare arriving past the mark while the store awaits its crl_save.log state is
  * dropped until recovery recreates the transaction.
  */
final class CrlAdmission(val limitBytes: Long):
  private val total = new AtomicLong(0)

  /** Admits `bytes` if the total admitted is below the limit */
  @tailrec
  def tryAdmit(bytes: Long): Boolean =
    val current = total.get
    if current >= limitBytes then
      false
    else if total.compareAndSet(current, current + bytes) then
      true
    else
      tryAdmit(bytes)

  /** Adds `bytes` whatever the total: for a transaction already admitted, which must not be dropped */
  def charge(bytes: Long): Unit = total.addAndGet(bytes)

  /** Whether the total is at or past the limit, so no new transaction is admitted */
  def full: Boolean = total.get >= limitBytes

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

  /** What `storeId` is retained for */
  def retainedBytes(storeId: StoreId): Long = Option(retained.get(storeId)).map(_.longValue).getOrElse(0L)

  def admittedBytes: Long = total.get

object CrlAdmission:
  /** SimpleCRL's static record of a saved transaction (LogContent.staticDataSize): TxId 33, txd location 14,
    * disposition 1, Paxos state 11, object-update count 4. Rewritten with each save; the latest is the live one
    */
  val TxOverheadBytes: Long = 63

  /** Per object update in that record: object UUID 16 + data location 14 */
  val ObjectUpdateOverheadBytes: Long = 30

  /** What an object update's data costs the CRL */
  def objectUpdateBytes(dataSize: Long): Long = dataSize + ObjectUpdateOverheadBytes

  /** Admits everything: for tests and stores with no device */
  def Unlimited: CrlAdmission = new CrlAdmission(Long.MaxValue)
