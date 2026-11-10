package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder.Built
import org.aspen_ddp.aspen.common.objects.ObjectPointer
import org.aspen_ddp.aspen.common.transaction.TransactionObjectRequirement

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** Records every transaction `client` commits until `stop`, as built, and with `enforce` makes an unpinned one fail
  * with UnpinnedTransaction. Tests start it after their setup, so only the transactions of the site under test are
  * checked. The global switch (`TransactionBuilder.enforcePins`) is never touched: suites run in parallel.
  *
  * Finalization actions retry a failed transaction forever, so their tests pass `enforce = false` and assert on
  * `unpinned`: an unpinned transaction then fails the assertion instead of hanging the test.
  */
class PinAudit(client: AspenClient, enforce: Boolean = true):
  private val seen = new ConcurrentLinkedQueue[Built]()

  client.testingOnlyOnBuilt = Some(b => seen.add(b))
  client.testingOnlyEnforcePins = enforce

  def stop(): Unit =
    client.testingOnlyEnforcePins = false
    client.testingOnlyOnBuilt = None

  def built: List[Built] = seen.asScala.toList

  /** `built` without the missed-update marker's writes, which TestNetwork's stores provoke at any time */
  def builtExceptMarks: List[Built] = built.filterNot(PinAudit.isMark)

  /** The transactions with a requirement on `ptr`'s object */
  def touching(ptr: ObjectPointer): List[Built] = built.filter(PinAudit.touches(_, ptr))

  def unpinned: List[Built] = built.filter(b => !b.replaySafe && !b.hasPin)

  /** Transactions whose notes carry the replay-safe mark with `reason` */
  def replaySafeWith(reason: String): List[Built] =
    built.filter(b => b.replaySafe && b.description.notes.contains(s"replay-safe: $reason"))

object PinAudit:
  def isMark(b: Built): Boolean = b.description.notes.contains("replay-safe: missed-update marks are idempotent")

  def touches(b: Built, ptr: ObjectPointer): Boolean =
    b.description.requirements.exists:
      case r: TransactionObjectRequirement => r.objectPointer.id == ptr.id
      case _ => false
