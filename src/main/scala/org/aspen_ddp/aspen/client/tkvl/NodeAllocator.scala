package org.aspen_ddp.aspen.client.tkvl

import java.nio.ByteBuffer
import org.aspen_ddp.aspen.client.{AspenClient, ObjectAllocator, ObjectAllocatorId, Transaction}
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectPointer}

import scala.concurrent.{ExecutionContext, Future}

object NodeAllocator:
  val encodedTierSize: Int = ObjectAllocatorId.encodedSize + 4

  def apply(client: AspenClient, bb: ByteBuffer): NodeAllocator =
    var ntiers: Int = bb.get()
    var tiers: List[(ObjectAllocatorId, Int)] = Nil

    while ntiers > 0 do
      ntiers -= 1
      val allocatorId = ObjectAllocatorId(bb)
      val maxNodeSize = bb.getInt()
      tiers = (allocatorId, maxNodeSize) :: tiers

    new NodeAllocator(client, tiers.reverse)

  /** Largest `maxNodeSize` a node of a `kind` tree stored in a pool with this IDA and slice limit may use, or
    * `None` for no limit.
    *
    * DataObject nodes are sized in object bytes, so their limit is the pool's `ida.maxObjectSize(maxSliceSize)`.
    *
    * `KeyValueListNode` sizes a node the way the store measures it: every byte it counts (the per-entry
    * revision, timestamp and varints, the IDA-encoded value slices and the node overhead) is a byte of the
    * slice each store holds. A node of `maxNodeSize` bytes therefore needs a slice limit of at least
    * `maxNodeSize`, so the limit is `maxSliceSize`. It is also kept at or below the pool's `maxObjectSize`
    * (`ida.maxObjectSize(maxSliceSize)`), which for every IDA but a single-data-shard one is the larger of the two.
    */
  def nodeSizeLimit(ida: IDA, maxSliceSize: Option[Int],
                    kind: TreeKind = TreeKind.KeyValueObjects): Option[Int] =
    kind match
      case TreeKind.KeyValueObjects => maxSliceSize.map(s => math.min(s, ida.maxObjectSize(s)))
      case TreeKind.DataObjects => maxSliceSize.map(s => ida.maxObjectSize(s))

  /** Largest `maxNodeSize` a tier of a `kind` tree allocating through `allocator` may use, or `None` for no limit.
    * A pool allocator uses its pool's `nodeSizeLimit`. Any other allocator (an allocation group) only knows its
    * current `maxObjectSize`, so that is the limit here; the slice limit of the pool a node actually lands in is
    * checked when the node is allocated.
    */
  def nodeSizeLimit(allocator: ObjectAllocator, kind: TreeKind): Option[Int] = allocator match
    case p: PoolObjectAllocator => nodeSizeLimit(p.pool.ida, p.pool.maxSliceSize, kind)
    case a => a.maxObjectSize

  /** `preferred`, or the limit when that is smaller. For code choosing its own default node size for a pool; a
    * configured node size is never adjusted, it is checked.
    */
  def fittingNodeSize(preferred: Int, limit: Option[Int]): Int =
    limit match
      case Some(l) if l < preferred => l
      case _ => preferred

  /** Throws `NodeSizeExceedsAllocatorLimit` if `maxNodeSize` is above `limit` */
  def checkNodeSize(tier: Int, maxNodeSize: Int, limit: Option[Int]): Unit =
    limit.foreach: l =>
      if maxNodeSize > l then
        throw NodeSizeExceedsAllocatorLimit(tier, maxNodeSize, l)

  /** Allocator handed out for a tier. After the underlying allocator picks a pool for a new node, checks the
    * tier's node size against that pool's limit. This catches allocators whose reported limit overstates the pool
    * a node lands in, such as a group holding a stale snapshot of a child group's limit. Pool limits never change,
    * so a node that passes stays valid. Only objects of the tree's node kind are checked; the other kind is never a
    * node of a `kind` tree.
    */
  private class TierAllocator(tier: Int, maxNodeSize: Int, kind: TreeKind, val underlying: ObjectAllocator)
    extends ObjectAllocator:
    val client: AspenClient = underlying.client
    val allocatorId: ObjectAllocatorId = underlying.allocatorId

    def maxObjectSize: Option[Int] = underlying.maxObjectSize

    protected[client] def createDataObjectPointer()(using t: Transaction): Future[DataObjectPointer] =
      checked(TreeKind.DataObjects, underlying.createDataObjectPointer())

    protected[client] def createKeyValueObjectPointer()(using t: Transaction): Future[KeyValueObjectPointer] =
      checked(TreeKind.KeyValueObjects, underlying.createKeyValueObjectPointer())

    /** `fptr`, after checking the node size against the pool it landed in when `objectKind` is the tree's kind */
    private def checked[P <: ObjectPointer](objectKind: TreeKind, fptr: Future[P]): Future[P] =
      if objectKind != kind then
        fptr
      else
        given ExecutionContext = client.clientContext
        fptr.flatMap: ptr =>
          client.getStoragePool(ptr.poolId).map: pool =>
            checkNodeSize(tier, maxNodeSize, nodeSizeLimit(pool.ida, pool.maxSliceSize, kind))
            ptr


/** Per-tier node allocation configuration for a TKVL: the allocator each tier allocates nodes from and the
  * tier's `maxNodeSize`. Tiers beyond the configured ones use the last tier's configuration.
  *
  * A tier's `maxNodeSize` may never exceed `NodeAllocator.nodeSizeLimit` of its allocator. `validate` checks every
  * configured tier and `Root.create` calls it. Decoding (`NodeAllocator.apply`, `Root.apply`) is synchronous and
  * does not check; instead `getAllocatorForTier`, which every node insert, split and allocation goes through,
  * re-checks the tier against the allocator's current limit each time and fails with
  * `NodeSizeExceedsAllocatorLimit` before anything is written.
  */
class NodeAllocator(val client:AspenClient, config: Seq[(ObjectAllocatorId, Int)]):

  val tiers: Array[(ObjectAllocatorId, Int)] = config.toArray

  def encodedSize: Int = 1 + (NodeAllocator.encodedTierSize * tiers.length)

  private def tierConfig(tier: Int): (ObjectAllocatorId, Int) =
    if tier < tiers.length then tiers(tier) else tiers.last

  /** Looks up an allocator. Overridable for tests */
  protected def lookupAllocator(allocatorId: ObjectAllocatorId): Future[ObjectAllocator] =
    client.getAllocator(allocatorId)

  /** The tier's allocator, after checking the tier's node size against the allocator's current limit for `kind` */
  private def checkedAllocator(tier: Int, kind: TreeKind): Future[ObjectAllocator] =
    given ExecutionContext = client.clientContext
    val (allocatorId, maxNodeSize) = tierConfig(tier)
    lookupAllocator(allocatorId).map: allocator =>
      NodeAllocator.checkNodeSize(tier, maxNodeSize, NodeAllocator.nodeSizeLimit(allocator, kind))
      allocator

  /** Fails with `NodeSizeExceedsAllocatorLimit` for the first configured tier whose `maxNodeSize` is above its
    * allocator's `NodeAllocator.nodeSizeLimit` for a `kind` tree. Allocators without a limit always pass.
    *
    * Group tiers are only partly checked here: a group has no IDA, so only its current `maxObjectSize` is
    * compared. The full check, against the slice limit of the pool a node lands in, happens when the tier
    * allocates its first node (see `getAllocatorForTier`).
    */
  def validate(kind: TreeKind = TreeKind.KeyValueObjects): Future[Unit] =
    given ExecutionContext = client.clientContext
    tiers.indices.foldLeft(Future.unit): (prev, tier) =>
      prev.flatMap(_ => checkedAllocator(tier, kind).map(_ => ()))

  /** The allocator for new nodes in `tier` of a `kind` tree. Fails with `NodeSizeExceedsAllocatorLimit` if the
    * tier's node size is above the allocator's current limit, and the returned allocator fails the same way if a
    * node lands in a pool whose limit is below the node size.
    */
  def getAllocatorForTier(tier: Int, kind: TreeKind = TreeKind.KeyValueObjects): Future[ObjectAllocator] =
    given ExecutionContext = client.clientContext
    checkedAllocator(tier, kind).map: allocator =>
      new NodeAllocator.TierAllocator(tier, getMaxNodeSize(tier), kind, allocator)

  def getMaxNodeSize(tier: Int): Int = tierConfig(tier)._2

  def encodeInto(bb:ByteBuffer): Unit =
    bb.put(tiers.length.toByte)
    tiers.foreach: tier =>
      tier._1.encodeInto(bb)
      bb.putInt(tier._2)
