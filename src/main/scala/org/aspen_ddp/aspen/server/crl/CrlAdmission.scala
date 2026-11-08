package org.aspen_ddp.aspen.server.crl

import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

/** Backpressure for one device's CRL, shared by the device's stores (each on its own executor, so it is thread-safe).
  *
  * A store admits a prepare for a transaction it has not seen only while the bytes admitted on the device are below
  * `limitBytes` (`CrashRecoveryLog.admissionLimitBytes`), and drops it otherwise: the client retransmits, and that
  * stall is the slowdown. Admitted bytes are released when the transaction's CRL entry is deleted, when the store
  * forgets a transaction that never reached the CRL, or when the store closes. The check is against the total before
  * the admission, so one transaction may cross the limit: the limit leaves headroom for that.
  *
  * A transaction is counted for its description, its object data (each object once, charged without a limit check
  * when it arrives after the transaction was admitted) and the CRL's static record of it (`TxOverheadBytes`,
  * `ObjectUpdateOverheadBytes`). Accepted gaps: transactions recovered at store load are not counted (the CRL already
  * holds them), and a retransmitted prepare arriving past the mark while the store awaits its crl_save.log state is
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
