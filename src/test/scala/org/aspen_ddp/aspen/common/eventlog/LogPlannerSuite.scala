package org.aspen_ddp.aspen.common.eventlog

import scala.util.Random

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.scalatest.funsuite.AnyFunSuite

/** Verifies LogPlanner without an Aspen system. Records are identified by their typeId: batch records use
  * 0..9999, tail records 10000+, full-object records 20000+; a drop replacement carries -1 - typeId of the original.
  */
class LogPlannerSuite extends AnyFunSuite:
  import LogPlanner.*
  import LogStep.*

  private def ts(n: Long): HLCTimestamp = HLCTimestamp(n)

  private def rec(t: Long, id: Int, size: Int, rnd: Option[Random] = None): LoggedRecord =
    val data = new Array[Byte](size)
    rnd.foreach(_.nextBytes(data))
    LoggedRecord(ts(t), id.toShort, data)

  private def replacement(r: LoggedRecord): LoggedRecord =
    LoggedRecord(r.timestamp, (-1 - r.typeId).toShort, new Array[Byte](20))

  private def isReplacement(r: LoggedRecord): Boolean = r.typeId < 0

  private def raw(rs: Seq[LoggedRecord]): Int = rs.map(LogRecords.recordSize).sum

  private def objectBound(rs: Seq[LoggedRecord]): Int = FullObject.overhead + LogRecords.maxFrameSize(raw(rs))

  private def limit(cap: Option[Int]): Long = cap.map(_.toLong).getOrElse(Long.MaxValue)

  private def emptyTails(cap: Option[Int] = None): Vector[TailView] =
    Vector.tabulate(EventLogState.TailCount)(i => TailView(i, 0, cap))

  private def input(batch: Vector[LoggedRecord],
                    tails: Vector[TailView] = emptyTails(),
                    current: Int = 0,
                    tailRecords: Option[Vector[LoggedRecord]] = Some(Vector.empty),
                    tailFloor: HLCTimestamp = HLCTimestamp.Zero,
                    firstFullKey: Option[HLCTimestamp] = None,
                    lateTarget: Option[FullView] = None,
                    target: Int = 10000,
                    newObjectCap: Option[Int] = None): PlanInput =
    PlanInput(batch, tails, current, tailRecords, tailFloor, firstFullKey, lateTarget, target, newObjectCap,
      replacement)

  private def appends(p: LogPlan): Vector[AppendToTail] = p.steps.collect { case s: AppendToTail => s }
  private def compacts(p: LogPlan): Vector[Compact] = p.steps.collect { case s: Compact => s }
  private def rewrites(p: LogPlan): Vector[RewriteFull] = p.steps.collect { case s: RewriteFull => s }
  private def drops(p: LogPlan): Vector[LoggedRecord] = p.steps.collect { case Drop(r) => r }

  private def placed(p: LogPlan): Vector[LoggedRecord] = p.steps.flatMap {
    case AppendToTail(_, rs) => rs
    case Compact(objects, _) => objects.flatMap(_._2)
    case RewriteFull(_, objects) => objects.flatMap(_._2)
    case Drop(_) => Vector.empty
  }

  test("in-order records fill tails in order and advance current at the soft target") {
    val batch = Vector.tabulate(40)(i => rec(100 + i, i, 80))
    val plan = LogPlanner.plan(input(batch))
    val soft = 10000 / EventLogState.TailCount
    val as = appends(plan)
    assert(plan.consumed == batch)
    assert(as.flatMap(_.records) == batch)
    assert(as.map(_.index) == as.indices.toVector)
    assert(as.size > 1)
    // every filled tail stops just short of the soft target: one more record would have passed it
    as.init.zip(as.tail).foreach { (a, next) =>
      assert(LogRecords.maxFrameSize(raw(a.records)) <= soft)
      assert(LogRecords.maxFrameSize(raw(a.records :+ next.records.head)) > soft)
    }
    assert(plan.newCurrent == as.last.index)
    assert(compacts(plan).isEmpty)
  }

  test("a partly filled current tail is filled before moving on") {
    val soft = 10000 / EventLogState.TailCount
    val tails = emptyTails().updated(3, TailView(3, soft - 300, None))
    val batch = Vector.tabulate(5)(i => rec(100 + i, i, 80))
    val plan = LogPlanner.plan(input(batch, tails = tails, current = 3))
    val as = appends(plan)
    assert(as.head.index == 3)
    assert(soft - 300 + LogRecords.maxFrameSize(raw(as.head.records)) <= soft)
    assert(as.tail.forall(_.index == 4))
    assert(plan.newCurrent == as.last.index)
  }

  test("compaction sorts tail records and sets the floor to the largest compacted timestamp") {
    val rnd = new Random(11)
    val tailRecs = rnd.shuffle(Vector.tabulate(50)(i => rec(1000 + i * 3, 10000 + i, 50)))
    val tails = Vector.tabulate(EventLogState.TailCount)(i => TailView(i, 990, Some(1000)))
    val batch = Vector(rec(1010, 0, 50), rec(5000, 1, 50))
    val plan = LogPlanner.plan(input(batch, tails = tails, current = 9, tailRecords = Some(tailRecs),
      target = 1000, newObjectCap = Some(1000)))
    val cs = compacts(plan)
    assert(cs.size == 1)
    val compacted = cs.head.objects.flatMap(_._2)
    // the batch record below the new floor joins the compaction; the later one goes into the emptied tails
    assert(compacted == (tailRecs :+ batch(0)).sortBy(_.timestamp))
    assert(cs.head.newTailFloor == ts(1000 + 49 * 3))
    cs.head.objects.foreach { (h, rs) =>
      assert(h.filedKey == rs.head.timestamp)
      assert(h.upperBound.isEmpty)
      assert(objectBound(rs) <= 1000)
    }
    assert(cs.head.objects.size > 1)
    assert(appends(plan) == Vector(AppendToTail(0, Vector(batch(1)))))
    assert(plan.newCurrent == 0)
    assert(plan.consumed == batch)
  }

  test("records already appended in a plan are folded into a compaction") {
    val tails = Vector.tabulate(EventLogState.TailCount)(i => TailView(i, if i < 9 then 990 else 0, Some(1000)))
    val batch = Vector.tabulate(20)(i => rec(100 + i, i, 80))
    val plan = LogPlanner.plan(input(batch, tails = tails, current = 9, tailRecords = Some(Vector.empty),
      newObjectCap = Some(4000)))
    assert(compacts(plan).size == 1)
    val compacted = compacts(plan).head.objects.flatMap(_._2)
    val appended = appends(plan).flatMap(_.records)
    // tail 9 was filled in this plan, then compacted instead of appended; the rest refill from tail 0
    assert(compacted.nonEmpty && appended.nonEmpty)
    assert(appends(plan).map(_.index) == appends(plan).indices.toVector)
    assert((compacted ++ appended) == batch)
    assert(compacts(plan).head.newTailFloor == compacted.last.timestamp)
  }

  test("compaction without loaded tail content stops the plan") {
    val tails = Vector.tabulate(EventLogState.TailCount)(i => TailView(i, 990, Some(1000)))
    val batch = Vector(rec(100, 0, 50))
    val plan = LogPlanner.plan(input(batch, tails = tails, current = 9, tailRecords = None))
    assert(plan.steps.isEmpty)
    assert(plan.consumed.isEmpty)
    assert(plan.newCurrent == 9)
  }

  test("a record too large for one object is dropped") {
    val big = rec(100, 0, 2000, Some(new Random(5)))
    val plan = LogPlanner.plan(input(Vector(big), newObjectCap = Some(1000)))
    assert(drops(plan) == Vector(big))
    assert(placed(plan) == Vector(replacement(big)))
    assert(plan.consumed == Vector(big))
  }

  private def fullView(key: Long, records: Vector[LoggedRecord], upperBound: Option[Long] = None,
                       nextKey: Option[Long] = None, cap: Option[Int] = None): FullView =
    FullView(ts(key), upperBound.map(ts), nextKey.map(ts), records, cap)

  test("a late record is merged into its covering object; an oversized merge splits at a timestamp boundary and links upper bounds") {
    val existing = Vector.tabulate(10)(i => rec(1000 + i * 10, 20000 + i, 70))
    val target = objectBound(existing)
    val view = fullView(1000, existing, upperBound = Some(1500), nextKey = Some(2000), cap = Some(10000))
    val late = rec(1055, 0, 70)
    val after = rec(3000, 1, 70) // beyond the target's range: stays for a later transaction
    val plan = LogPlanner.plan(input(Vector(late, after), tailFloor = ts(5000), firstFullKey = Some(ts(500)),
      lateTarget = Some(view), target = target, newObjectCap = Some(10000)))
    val rw = rewrites(plan)
    assert(rw.size == 1)
    assert(rw.head.oldKey == ts(1000))
    val objects = rw.head.objects
    assert(objects.size == 2)
    assert(objects.flatMap(_._2) == (existing :+ late).sortBy(_.timestamp))
    assert(objects(0)._1 == FullObjectHeader(ts(1000), Some(objects(1)._2.head.timestamp)))
    assert(objects(1)._1 == FullObjectHeader(objects(1)._2.head.timestamp, Some(ts(1500))))
    objects.foreach((_, rs) => assert(objectBound(rs) <= target))
    assert(plan.consumed == Vector(late))
    assert(appends(plan).isEmpty)
  }

  test("a late merge that fits rewrites the object unsplit") {
    val existing = Vector.tabulate(3)(i => rec(1000 + i * 10, 20000 + i, 70))
    val view = fullView(1000, existing, nextKey = Some(2000))
    val late = rec(1005, 0, 70)
    val plan = LogPlanner.plan(input(Vector(late), tailFloor = ts(5000), firstFullKey = Some(ts(1000)),
      lateTarget = Some(view)))
    assert(rewrites(plan) == Vector(RewriteFull(ts(1000),
      Vector(FullObjectHeader(ts(1000), None) -> (existing :+ late).sortBy(_.timestamp)))))
  }

  test("equal timestamps are never split apart") {
    val existing = Vector.tabulate(10)(i => rec(1000 + (i / 5) * 100, 20000 + i, 70))
    val target = objectBound(existing.take(3))
    val view = fullView(1000, existing, nextKey = Some(2000), cap = Some(10000))
    val late = rec(1050, 0, 70)
    val plan = LogPlanner.plan(input(Vector(late), tailFloor = ts(5000), firstFullKey = Some(ts(500)),
      lateTarget = Some(view), target = target, newObjectCap = Some(10000)))
    val objects = rewrites(plan).head.objects
    assert(objects.map(_._2.map(_.timestamp.asLong).distinct) ==
      Vector(Vector(1000L), Vector(1050L), Vector(1100L)))
    assert(objects.map(_._1.filedKey.asLong) == Vector(1000L, 1050L, 1100L))
  }

  test("a late record with no split point past the cap is dropped") {
    val existing = Vector.tabulate(10)(i => rec(1000, 20000 + i, 70))
    val cap = objectBound(existing) + 10
    val view = fullView(1000, existing, nextKey = Some(2000), cap = Some(cap))
    val late = rec(1000, 0, 70)
    val plan = LogPlanner.plan(input(Vector(late), tailFloor = ts(5000), firstFullKey = Some(ts(1000)),
      lateTarget = Some(view), target = cap, newObjectCap = Some(cap)))
    assert(drops(plan) == Vector(late))
    assert(plan.consumed == Vector(late))
    // the replacement does not fit either, so the object is left alone
    assert(rewrites(plan).isEmpty)
  }

  test("a record older than the first key rekeys the first object") {
    val existing = Vector.tabulate(3)(i => rec(1000 + i * 10, 20000 + i, 70))
    val view = fullView(1000, existing, nextKey = Some(2000))
    val late = rec(400, 0, 70)
    val plan = LogPlanner.plan(input(Vector(late), tailFloor = ts(5000), firstFullKey = Some(ts(1000)),
      lateTarget = Some(view)))
    val rw = rewrites(plan).head
    assert(rw.oldKey == ts(1000))
    assert(rw.objects.head._1.filedKey == ts(400))
    assert(rw.objects.head._2.head == late)
  }

  test("late records without a loaded target stay unconsumed while in-order ones are placed") {
    val late = rec(100, 0, 10)
    val inOrder = rec(9000, 1, 10)
    val plan = LogPlanner.plan(input(Vector(late, inOrder), tailFloor = ts(5000), firstFullKey = Some(ts(50))))
    assert(plan.consumed == Vector(inOrder))
    assert(appends(plan) == Vector(AppendToTail(0, Vector(inOrder))))
  }

  test("a record at the tail floor is late") {
    val existing = Vector(rec(5000, 20000, 10))
    val view = fullView(5000, existing)
    val r = rec(5000, 0, 10)
    val plan = LogPlanner.plan(input(Vector(r), tailFloor = ts(5000), firstFullKey = Some(ts(5000)),
      lateTarget = Some(view)))
    assert(rewrites(plan).head.objects.flatMap(_._2) == Vector(existing.head, r))
  }

  // ---- randomized -------------------------------------------------------------------------------------------------

  private final case class Case(in: PlanInput, plan: LogPlan)

  private def randomCase(rnd: Random): Case =
    val cap: Option[Int] = if rnd.nextInt(5) == 0 then None else Some(200 + rnd.nextInt(64 * 1024 - 200))
    val capOrDefault = cap.getOrElse(64 * 1024)
    val target = math.max(1, math.min(capOrDefault, 100 + rnd.nextInt(capOrDefault)))
    def size(): Int = rnd.nextInt(4) match
      case 0 => rnd.nextInt(2 * capOrDefault + 1)
      case _ => rnd.nextInt(capOrDefault / 8 + 1)
    def tailCap(): Option[Int] = rnd.nextInt(3) match
      case 0 => cap
      case 1 => None
      case _ => Some(200 + rnd.nextInt(capOrDefault))
    val hasFull = rnd.nextBoolean()
    val firstKey = 1000L
    val floor = if hasFull then 5000L else 0L
    // few distinct timestamps so duplicates and equal-timestamp groups are common
    def inOrderTs(): Long = floor + 1 + rnd.nextInt(30) * 7
    val tails = Vector.tabulate(EventLogState.TailCount) { i =>
      val c = tailCap()
      TailView(i, rnd.nextInt(limit(c).min(capOrDefault.toLong).toInt + 1), c)
    }
    val tailRecords =
      if rnd.nextInt(4) == 0 then None
      else Some(Vector.tabulate(rnd.nextInt(20))(i => rec(inOrderTs(), 10000 + i, size() / 4)))
    val lateTarget =
      if hasFull && rnd.nextBoolean() then
        val key = if rnd.nextBoolean() then firstKey else 2000L + rnd.nextInt(5) * 100
        val next = if rnd.nextBoolean() then Some(key + 500 + rnd.nextInt(1000).toLong) else None
        val hi = next.getOrElse(floor + 1)
        val objCap = if rnd.nextBoolean() then cap else Some(capOrDefault * 2)
        // existing content always fits its own object
        var recs = Vector.empty[LoggedRecord]
        var i = 0
        var full = false
        while !full && i < 30 do
          val r = rec(key + rnd.nextLong(hi - key), 20000 + i, size() / 8)
          if objectBound(recs :+ r) <= limit(objCap) then recs :+= r else full = true
          i += 1
        Some(FullView(ts(key), if rnd.nextBoolean() then Some(ts(hi)) else None, next.map(ts),
          recs.sortBy(_.timestamp), objCap))
      else None
    val nLate = if hasFull then rnd.nextInt(5) else 0
    val late = Vector.fill(nLate)(rnd.nextLong(floor + 1))
    val batchTs = (late ++ Vector.fill(rnd.nextInt(25))(inOrderTs())).sorted
    val batch = batchTs.zipWithIndex.map((t, i) => rec(t, i, size()))
    val in = PlanInput(batch, tails, rnd.nextInt(EventLogState.TailCount), tailRecords, ts(floor),
      if hasFull then Some(ts(firstKey)) else None, lateTarget, target, cap, replacement)
    Case(in, LogPlanner.plan(in))

  private def check(c: Case): Unit =
    val Case(in, plan) = c
    val capLimit = limit(in.newObjectCap)
    // caps
    val pendingByTail = appends(plan).groupBy(_.index)
    pendingByTail.foreach((_, as) => assert(as.size == 1, "one frame per tail per plan"))
    val compacted = compacts(plan).nonEmpty
    appends(plan).foreach { a =>
      val length = if compacted then 0 else in.tails(a.index).length
      assert(length + LogRecords.maxFrameSize(raw(a.records)).toLong <= limit(in.tails(a.index).cap))
    }
    compacts(plan).foreach(_.objects.foreach((_, rs) => assert(objectBound(rs) <= capLimit)))
    rewrites(plan).foreach { rw =>
      val t = in.lateTarget.get
      rw.objects.zipWithIndex.foreach { case ((_, rs), i) =>
        assert(objectBound(rs) <= (if i == 0 then limit(t.cap) else capLimit))
      }
    }
    assert(rewrites(plan).size <= 1)
    assert(compacts(plan).size <= 1)
    // keys strictly increase, objects never share a timestamp, headers derive from their records
    def checkObjects(objects: Vector[(FullObjectHeader, Vector[LoggedRecord])]): Unit =
      objects.foreach((_, rs) => assert(rs.nonEmpty && rs == rs.sortBy(_.timestamp)))
      objects.zip(objects.drop(1)).foreach { case ((h1, r1), (h2, r2)) =>
        assert(h1.filedKey < h2.filedKey)
        assert(r1.last.timestamp < r2.head.timestamp)
        assert(h2.filedKey == r2.head.timestamp)
      }
    compacts(plan).foreach { cp =>
      checkObjects(cp.objects)
      cp.objects.foreach((h, rs) => assert(h.filedKey == rs.head.timestamp && h.upperBound.isEmpty))
      val all = cp.objects.flatMap(_._2)
      if in.firstFullKey.isDefined then assert(all.forall(_.timestamp > in.tailFloor))
      if all.nonEmpty then assert(cp.newTailFloor == all.map(_.timestamp).max)
      // nothing in the tails is lost: compacted or dropped
      in.tailRecords.get.foreach(r => assert(all.contains(r) || drops(plan).contains(r)))
      // nothing appended after compaction is at or below the new floor
      appends(plan).flatMap(_.records).foreach(r => assert(r.timestamp > cp.newTailFloor))
    }
    rewrites(plan).foreach { rw =>
      val t = in.lateTarget.get
      checkObjects(rw.objects)
      assert(rw.oldKey == t.key)
      val all = rw.objects.flatMap(_._2)
      assert(t.records.forall(all.contains))
      val (h0, r0) = rw.objects.head
      if in.firstFullKey.contains(t.key) then
        assert(h0.filedKey == (if r0.head.timestamp < t.key then r0.head.timestamp else t.key))
      else
        assert(h0.filedKey == t.key)
      rw.objects.zip(rw.objects.drop(1)).foreach { case ((h1, _), (h2, _)) => assert(h1.upperBound == Some(h2.filedKey)) }
      assert(rw.objects.last._1.upperBound == t.upperBound)
      all.foreach(r => assert(t.nextKey.forall(r.timestamp < _)))
    }
    if in.firstFullKey.isDefined then
      appends(plan).flatMap(_.records).foreach(r => assert(r.timestamp > in.tailFloor))
    // consumption
    val batchSet = in.batch.toSet
    assert(plan.consumed.distinct == plan.consumed)
    assert(plan.consumed.forall(batchSet.contains))
    val placedRecs = placed(plan)
    val dropped = drops(plan)
    assert(dropped.distinct == dropped)
    in.batch.foreach { r =>
      val n = placedRecs.count(_ == r) + dropped.count(_ == r)
      if plan.consumed.contains(r) then assert(n == 1, s"consumed $r in $n steps")
      else assert(n == 0)
      if !dropped.contains(r) then assert(!placedRecs.contains(replacement(r)))
    }
    assert(placedRecs.count(isReplacement) == placedRecs.filter(isReplacement).distinct.size)
    assert(plan.newCurrent >= 0 && plan.newCurrent < EventLogState.TailCount)

  test("no written tail or object exceeds its cap") {
    val rnd = new Random(42)
    for _ <- 0 until 2000 do check(randomCase(rnd))
  }

  test("every batch record is consumed exactly once or left for later") {
    val rnd = new Random(43)
    var consumedSome = 0
    var compactions = 0
    var rewritten = 0
    var droppedSome = 0
    for _ <- 0 until 2000 do
      val c = randomCase(rnd)
      check(c)
      if c.plan.consumed.nonEmpty then consumedSome += 1
      if compacts(c.plan).nonEmpty then compactions += 1
      if rewrites(c.plan).nonEmpty then rewritten += 1
      if drops(c.plan).nonEmpty then droppedSome += 1
    // the generator exercises every path
    assert(consumedSome > 500 && compactions > 50 && rewritten > 50 && droppedSome > 50,
      s"consumed=$consumedSome compactions=$compactions rewrites=$rewritten drops=$droppedSome")
  }
