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

  def release(bytes: Long): Unit = total.addAndGet(-bytes)

  def admittedBytes: Long = total.get

object CrlAdmission:
  /** Admits everything: for tests and stores with no device */
  def Unlimited: CrlAdmission = new CrlAdmission(Long.MaxValue)
