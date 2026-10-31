package org.aspen_ddp.aspen.vdisk

import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/** The owner's in-memory state of one block object. Not thread-safe: every access happens under the
  * `VirtualDisk` monitor, and no Aspen call is made while holding it.
  */
final class BlockState:

  /** True once `pointer` is known: either looked up in the map or set by the map transaction that created it */
  var lookedUp: Boolean = false
  var lookupInFlight: Boolean = false

  var pointer: Option[DataObjectPointer] = None

  /** The object's current revision and length, once a read or a commit has revealed them. The owner's
    * `CleanCache` entry for the block holds content at this revision, within this length
    */
  var revision: Option[ObjectRevision] = None
  var length: Option[Int] = None

  /** Every uncommitted write. A commit's pieces stay here until it succeeds, so `dirty` alone is the overlay a read
    * applies, and newer writes replace the parts of in-flight pieces they overlap
    */
  val dirty: SegmentMap = SegmentMap()

  /** What the commit in flight writes */
  var inFlight: Option[SegmentSnapshot] = None

  /** Every write with a seq up to this one is committed */
  var committedSeq: Long = 0L

  /** Bumped by each commit. A read caches what it fetched only if no commit completed meanwhile */
  var generation: Long = 0L

  var timer: Option[ScheduledTask] = None
  /** Identifies the most recently armed timer, so a stale one that already fired cannot clear a newer one */
  var timerSeq: Long = 0L

  private val waiters = mutable.TreeMap[Long, List[Promise[Unit]]]()

  def hasWaiters: Boolean = waiters.nonEmpty

  /** Completes once a commit covering `seq` has succeeded */
  def awaitCommitted(seq: Long): Future[Unit] =
    if committedSeq >= seq then
      Future.unit
    else
      val p = Promise[Unit]()
      waiters(seq) = p :: waiters.getOrElse(seq, Nil)
      p.future

  /** Removes and returns the waiters that `committedSeq` satisfies */
  def takeSatisfied(): List[Promise[Unit]] =
    val done = waiters.rangeTo(committedSeq).toList
    done.foreach((seq, _) => waiters.remove(seq))
    done.flatMap(_._2)

  def takeAllWaiters(): List[Promise[Unit]] =
    val all = waiters.values.flatten.toList
    waiters.clear()
    all
