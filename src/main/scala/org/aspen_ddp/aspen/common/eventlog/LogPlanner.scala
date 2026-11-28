package org.aspen_ddp.aspen.common.eventlog

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
  * @param batch           incoming records, type ids assigned
  * @param tailRecords     content of every tail; Some only when compaction is possible
  * @param tailFloor       largest timestamp held by any full object (meaningful only when firstFullKey is defined)
  * @param lateTarget      covering object of the oldest late record, when loaded
  * @param target          full-object target size; tails target target / TailCount
  * @param newObjectCap    hard cap of the pool new full objects are allocated from
  * @param dropReplacement the DroppedEvent record logged in place of a dropped one (same timestamp)
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
  /** The object filed under oldKey rewritten as objects(0) (rekeyed if its filedKey differs); the rest are new */
  case RewriteFull(oldKey: HLCTimestamp, objects: Vector[(FullObjectHeader, Vector[LoggedRecord])])
  /** The record is not logged; its drop replacement is placed by another step of the same plan (when it fits) */
  case Drop(record: LoggedRecord)

/** @param consumed exactly the batch records the plan placed or dropped; the rest stay in incomingEvents */
final case class LogPlan(steps: Vector[LogStep], consumed: Vector[LoggedRecord], newCurrent: Int)

/** Pure placement decisions for the event log service: tails, compaction, late-event rewrites, drops.
  *
  * With full objects present, a record is late when its timestamp is at or below tailFloor (strictly, so a compacted
  * object's key is always above every existing full object's key, even when the last one holds only records at
  * tailFloor). All size checks use zstd's compress bound, so nothing written ever exceeds its cap.
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
    * None when some run fits no object; an unsplit object within firstCap is the fallback.
    */
  private def pack(rs: Vector[LoggedRecord], target: Int, firstCap: Long, restCap: Long)
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
      if cur.nonEmpty && fitsObject(curRaw + gRaw, math.min(target.toLong, cap)) then
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
    else if fitsObject(rawSize(rs), firstCap) then
      Some(Vector(rs))
    else
      None

  /** pack for new objects that always succeeds: the records of a run of equal timestamps beyond what one object can
    * hold are returned as dropped. Only reachable with more than a cap's worth of records sharing one timestamp.
    */
  private def packDropping(rs: Vector[LoggedRecord], target: Int, cap: Long)
  : (Vector[Vector[LoggedRecord]], Vector[LoggedRecord]) =
    pack(rs, target, cap, cap) match
      case Some(parts) => (parts, Vector.empty)
      case None =>
        val kept = Vector.newBuilder[LoggedRecord]
        val dropped = Vector.newBuilder[LoggedRecord]
        groups(rs).foreach { g =>
          var raw = 0L
          g.foreach { r =>
            val n = LogRecords.recordSize(r)
            if fitsObject(raw + n, cap) then
              raw += n
              kept += r
            else
              dropped += r
          }
        }
        (pack(kept.result(), target, cap, cap).get, dropped.result())

  def plan(in: PlanInput): LogPlan =
    val tailCount = in.tails.size
    require(tailCount > 0 && in.tails.indices.forall(i => in.tails(i).index == i), "tails must be indexed 0 until n")
    require(in.current >= 0 && in.current < tailCount, s"current tail ${in.current} out of range")

    val newCap = limitOf(in.newObjectCap)
    val softTarget = (in.target / EventLogState.TailCount).toLong

    val dropSteps = Vector.newBuilder[LogStep]
    val consumed = Vector.newBuilder[LoggedRecord]

    /** Where a batch record lands: itself, or its replacement when too large for any object (None when that is too) */
    def placement(r: LoggedRecord): Option[LoggedRecord] =
      if fitsObject(LogRecords.recordSize(r), newCap) then
        Some(r)
      else
        val rep = in.dropReplacement(r)
        require(rep.timestamp == r.timestamp, "a drop replacement keeps the dropped record's timestamp")
        if fitsObject(LogRecords.recordSize(rep), newCap) then Some(rep) else None

    /** Marks r consumed and returns the record to place */
    def consume(r: LoggedRecord): Option[LoggedRecord] =
      consumed += r
      val p = placement(r)
      if !p.contains(r) then dropSteps += LogStep.Drop(r)
      p

    val batch = sorted(in.batch)
    val (late, inOrder) = in.firstFullKey match
      case Some(_) => batch.partition(_.timestamp <= in.tailFloor)
      case None => (Vector.empty, batch)

    val rewrite = planLate(in, late, newCap, consume, r => dropSteps += LogStep.Drop(r))

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
              val (parts, dropped) = packDropping(sorted(set), in.target, newCap)
              dropped.foreach(d => dropSteps += LogStep.Drop(d))
              val objects: Objects = parts.map(rs => FullObjectHeader(rs.head.timestamp, None) -> rs)
              compact = Some(LogStep.Compact(objects, floor.getOrElse(in.tailFloor)))
              for i <- 0 until tailCount do
                lengths(i) = 0
                pending(i) = Vector.empty
                pendingRaw(i) = 0
              current = 0

    val appends = (0 until tailCount).filter(pending(_).nonEmpty).map(i => LogStep.AppendToTail(i, pending(i)))
    LogPlan(dropSteps.result() ++ rewrite ++ compact ++ appends, consumed.result(), current)

  /** At most one RewriteFull: the late records in lateTarget's range merged into it, split as needed. A record whose
    * merge cannot fit (no split point within the caps) is dropped, its replacement merged when that fits.
    */
  private def planLate(in: PlanInput,
                       late: Vector[LoggedRecord],
                       newCap: Long,
                       consume: LoggedRecord => Option[LoggedRecord],
                       drop: LoggedRecord => Unit): Option[LogStep.RewriteFull] =
    in.lateTarget.flatMap { t =>
      val isFirst = in.firstFullKey.contains(t.key)
      def covers(ts: HLCTimestamp): Boolean = (isFirst || ts >= t.key) && t.nextKey.forall(ts < _)
      val inRange = late.filter(r => covers(r.timestamp))
      if inRange.isEmpty then
        None
      else
        val firstCap = limitOf(t.cap)
        def packMerged(rs: Vector[LoggedRecord]) = pack(sorted(rs), in.target, firstCap, newCap)

        // (record to merge, the batch original when it may still be dropped on a failed merge)
        val candidates = inRange.flatMap(r => consume(r).map(p => p -> (if p == r then Some(r) else None)))
        val all = t.records ++ candidates.map(_._1)
        val merged =
          if packMerged(all).isDefined then
            all
          else if packMerged(t.records).isEmpty then
            // the existing content no longer packs under the caps: leave it, drop every late record
            candidates.foreach((_, orig) => orig.foreach(drop))
            t.records
          else
            candidates.foldLeft(t.records) { case (acc, (p, orig)) =>
              if packMerged(acc :+ p).isDefined then
                acc :+ p
              else
                orig match
                  case None => acc
                  case Some(r) =>
                    drop(r)
                    val rep = in.dropReplacement(r)
                    if packMerged(acc :+ rep).isDefined then acc :+ rep else acc
            }
        if merged.size == t.records.size then
          None
        else
          val parts = packMerged(merged).get
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
