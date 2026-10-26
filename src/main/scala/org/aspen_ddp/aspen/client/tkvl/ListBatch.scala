package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.{AbortAfterCommit, ObjectAllocator, ObjectReader, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectId, ObjectPointer}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Batch updates of one tier's list, shared by `TierEngine.prepareUpdates` and `KeyValueListNode.prepareUpdates`.
  *
  * A batch runs in three steps so that every read is done before anything is added to the transaction: `assign`
  * finds the node holding each op, `withJoinSources` reads the right neighbor of each node the batch empties, and
  * `stage` updates each node once. A read failing with `InvalidObject`, as when a racing join deleted a node, can
  * therefore be retried with nothing staged.
  *
  * A node the batch empties absorbs its right neighbor, as a single delete would, only when the batch leaves the
  * neighbor alone. Otherwise the two would both be updated in the transaction, so the node is left empty and the
  * caller removes it from the list (see `TailRemovalFinalizationAction`).
  */
private[tkvl] object ListBatch:

  /** A node holding some of a batch's ops, ascending */
  final case class Assigned(node: ListNode, ops: List[BatchOp])

  /** A node to update and, when the batch empties it, the right neighbor it may absorb */
  final case class Target(node: ListNode, ops: List[BatchOp], joinSource: Option[ListNode])

  /** Scans right from `start` and hands each of `ops`, ascending, to the node whose range holds it. Returns the nodes
    * holding ops, left to right, and every node read, `start` included. Fails with `BelowMinimumError` if the first
    * op is below `start`'s range.
    */
  def assign(reader: ObjectReader, start: ListNode, ops: List[BatchOp]): Future[(List[Assigned], List[ListNode])] =
    given ExecutionContext = reader.client.clientContext
    val ordering = start.ordering

    def scan(node: ListNode,
             ops: List[BatchOp],
             assigned: List[Assigned],
             read: List[ListNode]): Future[(List[Assigned], List[ListNode])] =
      val (here, rest) = node.link match
        case None => (ops, Nil)
        case Some(l) => ops.span(op => ordering.compare(op.key, l.minimum) < 0)
      val nowAssigned = if here.isEmpty then assigned else Assigned(node, here) :: assigned
      val nowRead = node :: read

      node.link match
        case Some(l) if rest.nonEmpty =>
          TierEngine.encodingFor(node).read(reader, l.pointer, l.minimum, ordering,
            s"Batch update scanning right TKVL node ${l.pointer.id}. Minimum: ${l.minimum}").flatMap: right =>
            scan(right, rest, nowAssigned, nowRead)
        case _ =>
          Future.successful((nowAssigned.reverse, nowRead.reverse))

    ops.headOption match
      case Some(op) if ordering.compare(op.key, start.minimum) < 0 =>
        Future.failed(new BelowMinimumError(start.minimum, op.key))
      case _ =>
        Future.delegate(scan(start, ops, Nil, Nil))

  /** True if `ops` delete every entry of `node`, which holds some, and put nothing */
  def empties(node: ListNode, ops: List[BatchOp]): Boolean =
    node.size > 0 && ops.forall(_.isInstanceOf[BatchOp.Remove]) && {
      val removed = ops.iterator.map(_.key).toSet
      node.entries.forall(e => removed.contains(e._1))
    }

  /** `assigned` as targets, each node the batch empties paired with its right neighbor if the batch leaves that
    * neighbor alone. Neighbors already in `read` are not read again.
    */
  def withJoinSources(reader: ObjectReader,
                      assigned: List[Assigned],
                      read: Map[ObjectId, ListNode]): Future[List[Target]] =
    given ExecutionContext = reader.client.clientContext
    val touched = assigned.iterator.map(_.node.pointer.id).toSet

    Future.traverse(assigned): a =>
      a.node.link match
        case Some(l) if empties(a.node, a.ops) && !touched.contains(l.pointer.id) =>
          val fright = read.get(l.pointer.id) match
            case Some(r) => Future.successful(r)
            case None =>
              TierEngine.encodingFor(a.node).read(reader, l.pointer, l.minimum, a.node.ordering,
                s"Batch update joining TKVL node ${l.pointer.id}. Minimum: ${l.minimum}")
          fright.map(r => Target(a.node, a.ops, Some(r)))
        case _ =>
          Future.successful(Target(a.node, a.ops, None))

  /** Updates every target once, in order, and returns the nodes left empty. Reads nothing. A failure once an
    * earlier target is staged aborts `tx`, which would otherwise commit only part of the batch.
    */
  def stage(targets: List[Target],
            maxNodeSize: Int,
            allocator: ObjectAllocator,
            prepareForSplit: (Key, ObjectPointer) => Future[Unit],
            prepareForJoin: (Key, ObjectPointer) => Future[Unit])
           (using tx: Transaction, ec: ExecutionContext): Future[List[ListNode]] =

    def abortStaged(err: Throwable): Unit =
      try tx.abort(err)
      catch case _: AbortAfterCommit => ()

    def loop(rest: List[Target], staged: Boolean, emptied: List[ListNode]): Future[List[ListNode]] = rest match
      case Nil => Future.successful(emptied.reverse)
      case t :: tail =>
        Future.delegate(TierEngine.encodingFor(t.node).applyBatch(t.node, t.ops, maxNodeSize, allocator, t.joinSource,
          prepareForSplit, prepareForJoin)).transformWith:
          case Success(empty) => loop(tail, true, if empty then t.node :: emptied else emptied)
          case Failure(err) =>
            if staged then
              abortStaged(err)
            Future.failed(err)

    loop(targets, false, Nil)

  /** `assign`, `withJoinSources` and `stage` for a list without upper tiers, starting from `start` */
  def update(reader: ObjectReader,
             start: ListNode,
             ops: List[BatchOp],
             maxNodeSize: Int,
             allocator: ObjectAllocator,
             prepareForSplit: (Key, ObjectPointer) => Future[Unit],
             prepareForJoin: (Key, ObjectPointer) => Future[Unit])(using tx: Transaction): Future[List[ListNode]] =
    given ExecutionContext = reader.client.clientContext
    for
      (assigned, read) <- assign(reader, start, ops)
      targets <- withJoinSources(reader, assigned, read.iterator.map(n => n.pointer.id -> n).toMap)
      emptied <- stage(targets, maxNodeSize, allocator, prepareForSplit, prepareForJoin)
    yield
      emptied
