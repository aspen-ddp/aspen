package org.aspen_ddp.aspen.common.eventlog

import java.util.{Collections, IdentityHashMap}

import scala.collection.mutable

import org.aspen_ddp.aspen.common.HLCTimestamp

/** A tail object: its current byte length and the hard cap of the pool it lives in */
final case class TailView(index: Int, length: Int, cap: Option[Int])

/** The full object a late event hits. upperBound is its header's; nextKey is the key of the following full object
  * in loggedEvents (None when it is the last), so the object covers [key, nextKey), the first one also everything
  * before its key. cap is the hard cap of the pool the object lives in.
  */
final case class FullView(key: HLCTimestamp,
                          upperBound: Option[HLCTimestamp],
                          nextKey: Option[HLCTimestamp],
                          records: Vector[LoggedRecord],
                          cap: Option[Int])

/** Everything the planner decides from.
  *
  * @param batch           incoming records sorted by timestamp, type ids assigned
  * @param tailRecords     content of every tail; Some only when compaction is possible
  * @param tailFloor       largest timestamp held by any full object (meaningful only when firstFullKey is defined)
  * @param lateTarget      covering object of the oldest late record, when loaded
  * @param target          full-object target size; tails target target / TailCount
  * @param newObjectCap    hard cap of the pool new full objects are allocated from
  * @param dropReplacement the DroppedEvent record logged in place of a dropped one, with the same timestamp. It is
  *                        called at most once per dropped record (batch or tail) and must be pure and cheap.
  */
final case class PlanInput(batch: Vector[LoggedRecord],
                           tails: Vector[TailView],
                           current: Int,
                           tailRecords: Option[Vector[LoggedRecord]],
                           tailFloor: HLCTimestamp,
                           firstFullKey: Option[HLCTimestamp],
                           lateTarget: Option[FullView],
                           target: Int,
                           newObjectCap: Option[Int],
                           dropReplacement: LoggedRecord => LoggedRecord)

enum LogStep:
  /** One new frame appended to the tail */
  case AppendToTail(index: Int, records: Vector[LoggedRecord])

  /** Every tail emptied; these new full objects inserted; tailFloor set to newTailFloor */
  case Compact(objects: Vector[(FullObjectHeader, Vector[LoggedRecord])], newTailFloor: HLCTimestamp)

  /** The object filed under oldKey rewritten as objects(0), the rest new. objects(0) is rekeyed when its filedKey
    * differs from oldKey; then objects(1)'s key may equal oldKey, so the executor applies the delete of oldKey and
    * the inserts as one batch in which oldKey ends up pointing at objects(1).
    */
  case RewriteFull(oldKey: HLCTimestamp, objects: Vector[(FullObjectHeader, Vector[LoggedRecord])])

  /** A batch or tail record that is not logged. When replacementPlaced, its drop replacement is held by another step
    * of the same plan. Otherwise the replacement itself fit nowhere (its run of equal timestamps already fills an
    * object, or the cap is below one replacement record): the record vanishes without trace and the executor should
    * log a warning.
    */
  case Drop(record: LoggedRecord, replacementPlaced: Boolean)

/** @param consumed exactly the batch records the plan placed or dropped, as the batch's own instances in batch order;
  *                 the rest stay in incomingEvents. Equal records may repeat in a batch, so executors match consumed
  *                 records to incoming entries by reference (eq) or position, not by equality.
  */
final case class LogPlan(steps: Vector[LogStep], consumed: Vector[LoggedRecord], newCurrent: Int)

/** Pure placement decisions for the event log service: tails, compaction, late-event rewrites, drops.
  *
  * With full objects present, a record is late when its timestamp is at or below tailFloor (strictly, so a compacted
  * object's key is always above every existing full object's key, even when the last one holds only records at
  * tailFloor). All size checks use zstd's compress bound, so nothing written ever exceeds its cap. Every dropped
  * record's replacement is placed by timestamp like any other record whenever it fits.
  */
object LogPlanner:

  private type Objects = Vector[(FullObjectHeader, Vector[LoggedRecord])]

  private def limitOf(cap: Option[Int]): Long = cap.map(_.toLong).getOrElse(Long.MaxValue)

  private def frameBound(raw: Long): Long =
    if raw > Int.MaxValue / 2 then Long.MaxValue / 2 else LogRecords.maxFrameSize(raw.toInt).toLong

  private def fitsObject(raw: Long, limit: Long): Boolean = FullObject.overhead + frameBound(raw) <= limit

  private def rawSize(rs: Seq[LoggedRecord]): Long = rs.iterator.map(r => LogRecords.recordSize(r).toLong).sum

  private def sorted(rs: Vector[LoggedRecord]): Vector[LoggedRecord] = rs.sortBy(_.timestamp)

  /** Runs of equal timestamps in sorted records */
  private def groups(rs: Vector[LoggedRecord]): Vector[Vector[LoggedRecord]] =
    val b = Vector.newBuilder[Vector[LoggedRecord]]
    var rest = rs
    while rest.nonEmpty do
      val (g, tail) = rest.span(_.timestamp == rest.head.timestamp)
      b += g
      rest = tail
    b.result()

  /** Splits sorted records into objects at timestamp boundaries. Each object stays within min(target, its cap),
    * except that its first run of equal timestamps may reach the cap. Object 0 uses firstCap, later ones restCap.
    * When that fails, objects are filled to their caps, ignoring the target, which succeeds whenever any split
    * within the caps exists. None when none does.
    */
  private def pack(rs: Vector[LoggedRecord], target: Int, firstCap: Long, restCap: Long)
  : Option[Vector[Vector[LoggedRecord]]] =
    packGreedy(rs, target.toLong, firstCap, restCap).orElse(packGreedy(rs, Long.MaxValue, firstCap, restCap))

  private def packGreedy(rs: Vector[LoggedRecord], target: Long, firstCap: Long, restCap: Long)
  : Option[Vector[Vector[LoggedRecord]]] =
    val parts = Vector.newBuilder[Vector[LoggedRecord]]
    var cur = Vector.empty[LoggedRecord]
    var curRaw = 0L
    var index = 0
    var ok = true
    val it = groups(rs).iterator
    while ok && it.hasNext do
      val g = it.next()
      val gRaw = rawSize(g)
      val cap = if index == 0 then firstCap else restCap
      if cur.nonEmpty && fitsObject(curRaw + gRaw, math.min(target, cap)) then
        cur ++= g
        curRaw += gRaw
      else
        if cur.nonEmpty then
          parts += cur
          index += 1
        if fitsObject(gRaw, if index == 0 then firstCap else restCap) then
          cur = g
          curRaw = gRaw
        else
          ok = false
    if ok then
      if cur.nonEmpty then parts += cur
      Some(parts.result())
    else
      None

  /** One plan's mutable bookkeeping of drops and their replacements */
  private final class Drops(dropReplacement: LoggedRecord => LoggedRecord):
    private val replacementOf = new IdentityHashMap[LoggedRecord, LoggedRecord]()
    private val replacements = Collections.newSetFromMap(new IdentityHashMap[LoggedRecord, java.lang.Boolean]())
    private val dropped = mutable.ArrayBuffer.empty[LoggedRecord]

    def replacement(r: LoggedRecord): LoggedRecord =
      var rep = replacementOf.get(r)
      if rep == null then
        rep = dropReplacement(r)
        require(rep.timestamp == r.timestamp, "a drop replacement keeps the dropped record's timestamp")
        replacementOf.put(r, rep)
        replacements.add(rep)
      rep

    def isReplacement(r: LoggedRecord): Boolean = replacements.contains(r)

    /** Records r as dropped and returns its replacement, to be placed by the caller when it fits */
    def drop(r: LoggedRecord): LoggedRecord =
      dropped += r
      replacement(r)

    /** The Drop steps, each saying whether its replacement made it into the placing steps */
    def steps(placing: Seq[LogStep]): Vector[LogStep] =
      val placed = Collections.newSetFromMap(new IdentityHashMap[LoggedRecord, java.lang.Boolean]())
      placing.foreach {
        case LogStep.AppendToTail(_, rs) => rs.foreach(placed.add)
        case LogStep.Compact(os, _) => os.foreach(_._2.foreach(placed.add))
        case LogStep.RewriteFull(_, os) => os.foreach(_._2.foreach(placed.add))
        case LogStep.Drop(_, _) =>
      }
      dropped.toVector.map(r => LogStep.Drop(r, placed.contains(replacementOf.get(r))))

  /** pack for new objects that always succeeds. Within a run of equal timestamps too large for one object, the records
    * past what fits are dropped and their replacements kept where they still fit. Only reachable with more than a
    * cap's worth of records sharing one timestamp.
    */
  private def packDropping(rs: Vector[LoggedRecord], target: Int, cap: Long, drops: Drops)
  : Vector[Vector[LoggedRecord]] =
    pack(rs, target, cap, cap).getOrElse {
      val kept = Vector.newBuilder[LoggedRecord]
      groups(rs).foreach { g =>
        var raw = 0L
        def keep(r: LoggedRecord): Boolean =
          val n = LogRecords.recordSize(r)
          if fitsObject(raw + n, cap) then
            raw += n
            kept += r
            true
          else
            false
        g.foreach { r =>
          // a replacement that does not fit is just not placed; its original's Drop already exists
          if !keep(r) && !drops.isReplacement(r) then keep(drops.drop(r))
        }
      }
      pack(kept.result(), target, cap, cap).get
    }

  def plan(in: PlanInput): LogPlan =
    val tailCount = in.tails.size
    require(tailCount > 0 && in.tails.indices.forall(i => in.tails(i).index == i), "tails must be indexed 0 until n")
    require(in.current >= 0 && in.current < tailCount, s"current tail ${in.current} out of range")

    val newCap = limitOf(in.newObjectCap)
    val softTarget = (in.target / EventLogState.TailCount).toLong
    val drops = new Drops(in.dropReplacement)
    val consumed = Vector.newBuilder[LoggedRecord]

    def oversized(r: LoggedRecord): Boolean = !fitsObject(LogRecords.recordSize(r), newCap)

    /** Where a batch record lands: itself, or its replacement when too large for any object (None when that is too) */
    def placement(r: LoggedRecord): Option[LoggedRecord] =
      if !oversized(r) then
        Some(r)
      else
        val rep = drops.replacement(r)
        if oversized(rep) then None else Some(rep)

    /** Marks r consumed (dropping it when oversized) and returns the record to place */
    def consume(r: LoggedRecord): Option[LoggedRecord] =
      consumed += r
      if oversized(r) then drops.drop(r)
      placement(r)

    val batch = sorted(in.batch)
    val (late, inOrder) = in.firstFullKey match
      case Some(_) => batch.partition(_.timestamp <= in.tailFloor)
      case None => (Vector.empty, batch)

    val rewrite = planLate(in, late, newCap, consume, drops)

    // ---- tails ----
    val lengths = in.tails.map(_.length.toLong).toArray
    val pending = Array.fill(tailCount)(Vector.empty[LoggedRecord])
    val pendingRaw = Array.fill(tailCount)(0L)
    def tailCap(i: Int): Long = limitOf(in.tails(i).cap)

    def fitsTail(i: Int, raw: Long): Boolean =
      val bound = frameBound(pendingRaw(i) + raw)
      val empty = lengths(i) == 0 && pending(i).isEmpty
      bound <= tailCap(i) - lengths(i) && (empty || lengths(i) + bound <= softTarget)

    def fitsEmptyTail(raw: Long): Boolean = (0 until tailCount).exists(i => frameBound(raw) <= tailCap(i))

    var current = in.current
    var floor: Option[HLCTimestamp] = in.firstFullKey.map(_ => in.tailFloor)
    var compact: Option[LogStep.Compact] = None
    var stopped = false
    var k = 0

    while k < inOrder.size && !stopped do
      val r = inOrder(k)
      placement(r) match
        case None =>
          consume(r)
          k += 1
        case Some(p) =>
          val raw = LogRecords.recordSize(p).toLong
          (current until tailCount).find(i => fitsTail(i, raw)) match
            case Some(i) =>
              consume(r)
              current = i
              pending(i) :+= p
              pendingRaw(i) += raw
              k += 1
            case None if compact.isDefined || in.tailRecords.isEmpty =>
              stopped = true
            case None =>
              // Compact everything in the tails, including this plan's appends, then refill the emptied tails.
              // Records that would land at or below the new floor, or fit no empty tail, join the compaction.
              var set = in.tailRecords.get ++ pending.iterator.flatten
              if set.nonEmpty then
                val m = set.map(_.timestamp).max
                floor = Some(floor.fold(m)(f => if m > f then m else f))
              var folding = true
              while folding && k < inOrder.size do
                val r2 = inOrder(k)
                placement(r2) match
                  case None =>
                    consume(r2)
                    k += 1
                  case Some(p2) if floor.exists(p2.timestamp <= _) || !fitsEmptyTail(LogRecords.recordSize(p2)) =>
                    consume(r2)
                    set :+= p2
                    if floor.forall(p2.timestamp > _) then floor = Some(p2.timestamp)
                    k += 1
                  case Some(_) =>
                    folding = false
              in.firstFullKey.foreach { _ =>
                require(set.forall(_.timestamp > in.tailFloor), "tail records must lie above the tail floor")
              }
              val parts = packDropping(sorted(set), in.target, newCap, drops)
              val objects: Objects = parts.map(rs => FullObjectHeader(rs.head.timestamp, None) -> rs)
              compact = Some(LogStep.Compact(objects, floor.getOrElse(in.tailFloor)))
              for i <- 0 until tailCount do
                lengths(i) = 0
                pending(i) = Vector.empty
                pendingRaw(i) = 0
              current = 0

    val appends = (0 until tailCount).filter(pending(_).nonEmpty).map(i => LogStep.AppendToTail(i, pending(i)))
    val placing = rewrite.toVector ++ compact ++ appends
    LogPlan(drops.steps(placing) ++ placing, consumed.result(), current)

  /** At most one RewriteFull: the late records in lateTarget's range merged into it, split as needed. A record whose
    * merge cannot fit (no split point within the caps) is dropped, its replacement merged when that fits.
    */
  private def planLate(in: PlanInput,
                       late: Vector[LoggedRecord],
                       newCap: Long,
                       consume: LoggedRecord => Option[LoggedRecord],
                       drops: Drops): Option[LogStep.RewriteFull] =
    in.lateTarget.flatMap { t =>
      val isFirst = in.firstFullKey.contains(t.key)
      def covers(ts: HLCTimestamp): Boolean = (isFirst || ts >= t.key) && t.nextKey.forall(ts < _)
      val inRange = late.filter(r => covers(r.timestamp))
      if inRange.isEmpty then
        None
      else
        val firstCap = limitOf(t.cap)
        def packs(rs: Vector[LoggedRecord]): Boolean = pack(sorted(rs), in.target, firstCap, newCap).isDefined

        val candidates = inRange.flatMap(consume)
        val all = t.records ++ candidates
        val merged =
          if packs(all) then
            all
          else
            // add one at a time; a record that cannot merge is dropped and its replacement tried in its place.
            // When the existing content alone no longer packs under the caps, nothing merges.
            val existingPacks = packs(t.records)
            candidates.foldLeft(t.records) { (acc, p) =>
              if existingPacks && packs(acc :+ p) then
                acc :+ p
              else if drops.isReplacement(p) then
                acc
              else
                val rep = drops.drop(p)
                if existingPacks && packs(acc :+ rep) then acc :+ rep else acc
            }
        if merged.size == t.records.size then
          None
        else
          val parts = pack(sorted(merged), in.target, firstCap, newCap).get
          val keys = parts.indices.map { i =>
            if i > 0 then parts(i).head.timestamp
            else if isFirst && parts(0).head.timestamp < t.key then parts(0).head.timestamp
            else t.key
          }
          require(keys.zip(keys.drop(1)).forall(_ < _), "split keys must increase")
          val objects: Objects = parts.indices.toVector.map { i =>
            val upper = if i + 1 < parts.size then Some(keys(i + 1)) else t.upperBound
            FullObjectHeader(keys(i), upper) -> parts(i)
          }
          Some(LogStep.RewriteFull(t.key, objects))
    }
