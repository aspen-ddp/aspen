package org.aspen_ddp.aspen.client.tkvl

import java.util.concurrent.atomic.AtomicReference

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.aspen_ddp.aspen.common.objects.{ObjectId, ObjectRevision}

import scala.collection.concurrent.TrieMap

/** The root as a root manager reads it: the Root, its hosting object's revision and the root node */
final case class RootState(root: Root, revision: ObjectRevision, node: Option[ListNode])

/** Cache of the upper-tier (tier >= 1) nodes of a tree, and of its root. Tier 0 is never cached. */
trait TierCache:
  def root: Option[RootState]

  /** Stores only if `r.node.isDefined && r.root.tier >= 1`, otherwise drops the cached root */
  def putRoot(r: RootState): Unit

  def dropRoot(): Unit

  def node(tier: Int, id: ObjectId): Option[ListNode]

  /** Ignored for tier < 1 */
  def put(tier: Int, node: ListNode): Unit

  /** A descent found `node`, on `tier`, out of date: evict it, and drop the cached root if `node` is the root node */
  def stale(tier: Int, node: ListNode): Unit

  def clear(): Unit

object TierCache:
  val DefaultNodesPerTier: Int = 5

  def default(): TierCache = new LruTierCache(DefaultNodesPerTier)

/** Stores nothing, every lookup misses */
object NoCache extends TierCache:
  def root: Option[RootState] = None
  def putRoot(r: RootState): Unit = ()
  def dropRoot(): Unit = ()
  def node(tier: Int, id: ObjectId): Option[ListNode] = None
  def put(tier: Int, node: ListNode): Unit = ()
  def stale(tier: Int, node: ListNode): Unit = ()
  def clear(): Unit = ()

/** Keeps the most recently used `nodesPerTier` nodes of each tier >= 1, plus the root */
final class LruTierCache(nodesPerTier: Int) extends TierCache:
  private val tiers = TrieMap.empty[Int, Cache[ObjectId, ListNode]]
  private val rootRef = new AtomicReference[Option[RootState]](None)

  // The synchronous executor makes eviction deterministic
  private def tierCache(tier: Int): Cache[ObjectId, ListNode] =
    tiers.getOrElseUpdate(tier, Scaffeine().maximumSize(nodesPerTier).executor(_.run()).build[ObjectId, ListNode]())

  def root: Option[RootState] = rootRef.get()

  def putRoot(r: RootState): Unit =
    if r.node.isDefined && r.root.tier >= 1 then
      rootRef.set(Some(r))
    else
      dropRoot()

  def dropRoot(): Unit = rootRef.set(None)

  def node(tier: Int, id: ObjectId): Option[ListNode] =
    if tier < 1 then None else tiers.get(tier).flatMap(_.getIfPresent(id))

  def put(tier: Int, node: ListNode): Unit =
    if tier >= 1 then
      tierCache(tier).put(node.pointer.id, node)

  def stale(tier: Int, node: ListNode): Unit =
    tiers.get(tier).foreach(_.invalidate(node.pointer.id))
    if root.exists(_.node.exists(_.pointer.id == node.pointer.id)) then
      dropRoot()

  def clear(): Unit =
    tiers.values.foreach(_.invalidateAll())
    dropRoot()
