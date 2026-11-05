package org.aspen_ddp.aspen.client.tkvl

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectRevision}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}

/** The root as a root manager reads it: the Root, its hosting object's revision and the root node. `cacheable` is
  * false for a stand-in, such as the empty root given when the root node could not be read.
  */
final case class RootState(root: Root, revision: ObjectRevision, node: Option[ListNode], cacheable: Boolean = true)

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
    * r.root.tier >= 1` (any cacheable root for an `anyRoot` LruTierCache), and drops the cached root if not.
    */
  def putRoot(r: RootState, generation: Long): Unit

  /** Runs `read` for a root lookup that missed. Only an `anyRoot` LruTierCache shares a read among concurrent misses:
    * elsewhere a miss must read state at least as new as the lookup.
    */
  def missRead(read: () => Future[RootState]): Future[RootState] = read()

  /** True for a cache that holds any root, which may lag its tree's writer: writes then read the root fresh */
  def anyRoot: Boolean = false

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

/** Keeps the most recently used `nodesPerTier` nodes of each tier >= 1, plus the root.
  *
  * With `anyRoot` it keeps any root, tier 0 and an empty tree included, and concurrent misses within a generation
  * share one read. That suits only a tree whose single writer remembers its own changes, such as a vdisk's block map:
  * the cached root lags that writer's commits and is refreshed only by `clear()` or a fresh read.
  */
final class LruTierCache(nodesPerTier: Int, override val anyRoot: Boolean = false) extends TierCache:
  private val tiers = TrieMap.empty[Int, Cache[ObjectId, ListNode]]
  private val rootRef = new AtomicReference[Option[RootState]](None)
  private val clears = new AtomicLong(0)
  private val suspensions = new AtomicInteger(0)
  // The anyRoot read in flight, and the generation it started in
  private var sharedRead: Option[(Long, Future[RootState])] = None

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
      if r.cacheable && (anyRoot || r.node.isDefined && r.root.tier >= 1) then
        rootRef.set(Some(r))
      else
        dropRoot()

  override def missRead(read: () => Future[RootState]): Future[RootState] =
    if !anyRoot then
      read()
    else
      val p = Promise[RootState]()
      val joined = synchronized:
        val g = clears.get()
        sharedRead match
          case Some((`g`, f)) => Some(f)
          case _ =>
            sharedRead = Some((g, p.future))
            None
      joined.getOrElse:
        read().onComplete { r =>
          synchronized:
            if sharedRead.exists(_._2 eq p.future) then sharedRead = None
          p.complete(r)
        }(using ExecutionContext.parasitic)
        p.future

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
