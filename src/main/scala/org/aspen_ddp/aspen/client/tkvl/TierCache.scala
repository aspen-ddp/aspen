package org.aspen_ddp.aspen.client.tkvl

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectRevision}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** The root as a root manager reads it: the Root, its hosting object's revision and the root node */
final case class RootState(root: Root, revision: ObjectRevision, node: Option[ListNode])

/** Cache of the upper-tier (tier >= 1) nodes of a tree, and of its root. Tier 0 is never cached.
  *
  * Every fill passes the `generation` its caller captured before starting the read that produced it. A fill whose
  * read started before a `clear()` is ignored, so a read in flight across a clear cannot restore what it cleared.
  */
trait TierCache:
  def root: Option[RootState]

  /** Changes on every `clear()` */
  def generation: Long

  /** Ignored if a `clear()` happened since `generation`. Otherwise stores only if `r.node.isDefined &&
    * r.root.tier >= 1`, and drops the cached root if not.
    */
  def putRoot(r: RootState, generation: Long): Unit

  def dropRoot(): Unit

  def node(tier: Int, id: ObjectId): Option[ListNode]

  /** Ignored for tier < 1, and if a `clear()` happened since `generation` */
  def put(tier: Int, node: ListNode, generation: Long): Unit

  /** A descent found `node`, on `tier`, out of date: evict it, and drop the cached root if `node` is the root node */
  def stale(tier: Int, node: ListNode): Unit

  def clear(): Unit

  /** Until `f` completes every lookup misses and every fill is ignored. Once it does, the cache is cleared before
    * lookups and fills resume, so a fill whose read started before that is ignored too.
    */
  def suspendUntil(f: Future[?]): Unit

object TierCache:
  val DefaultNodesPerTier: Int = 5

  def default(): TierCache = new LruTierCache(DefaultNodesPerTier)

/** Stores nothing, every lookup misses */
object NoCache extends TierCache:
  def root: Option[RootState] = None
  def generation: Long = 0
  def putRoot(r: RootState, generation: Long): Unit = ()
  def dropRoot(): Unit = ()
  def node(tier: Int, id: ObjectId): Option[ListNode] = None
  def put(tier: Int, node: ListNode, generation: Long): Unit = ()
  def stale(tier: Int, node: ListNode): Unit = ()
  def clear(): Unit = ()
  def suspendUntil(f: Future[?]): Unit = ()

/** Keeps the most recently used `nodesPerTier` nodes of each tier >= 1, plus the root */
final class LruTierCache(nodesPerTier: Int) extends TierCache:
  private val tiers = TrieMap.empty[Int, Cache[ObjectId, ListNode]]
  private val rootRef = new AtomicReference[Option[RootState]](None)
  private val clears = new AtomicLong(0)
  private val suspensions = new AtomicInteger(0)

  // Fills, clear() and the end of a suspension hold this lock, so a fill cannot pass the generation and suspension
  // checks and then land after a clear
  private val fillLock = new Object

  // The synchronous executor makes eviction deterministic
  private def tierCache(tier: Int): Cache[ObjectId, ListNode] =
    tiers.getOrElseUpdate(tier, Scaffeine().maximumSize(nodesPerTier).executor(_.run()).build[ObjectId, ListNode]())

  def root: Option[RootState] = if suspensions.get() > 0 then None else rootRef.get()

  def generation: Long = clears.get()

  def putRoot(r: RootState, generation: Long): Unit = fillLock.synchronized:
    if suspensions.get() == 0 && generation == clears.get() then
      if r.node.isDefined && r.root.tier >= 1 then
        rootRef.set(Some(r))
      else
        dropRoot()

  def dropRoot(): Unit = rootRef.set(None)

  def node(tier: Int, id: ObjectId): Option[ListNode] =
    if tier < 1 || suspensions.get() > 0 then None else tiers.get(tier).flatMap(_.getIfPresent(id))

  def put(tier: Int, node: ListNode, generation: Long): Unit = fillLock.synchronized:
    if tier >= 1 && suspensions.get() == 0 && generation == clears.get() then
      tierCache(tier).put(node.pointer.id, node)

  def stale(tier: Int, node: ListNode): Unit =
    tiers.get(tier).foreach(_.invalidate(node.pointer.id))
    if root.exists(_.node.exists(_.pointer.id == node.pointer.id)) then
      dropRoot()

  def clear(): Unit = fillLock.synchronized:
    clears.incrementAndGet()
    tiers.values.foreach(_.invalidateAll())
    dropRoot()

  def suspendUntil(f: Future[?]): Unit =
    suspensions.incrementAndGet()
    // Clearing before resuming advances the generation, so a fill whose read started while suspended stays ignored
    f.onComplete { _ =>
      fillLock.synchronized:
        clear()
        suspensions.decrementAndGet()
    }(using ExecutionContext.parasitic)
