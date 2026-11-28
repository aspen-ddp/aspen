package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.HLCTimestamp

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/** Late events (timestamp at or below tailFloor) merged into the full objects that cover them */
class EventLogLateEventSuite extends EventLogSuiteBase:

  private val Target = 16 * 1024

  /** Timestamps step by Spacing, so late events can fall between logged ones */
  private val Spacing = 10L

  /** The log after two compactions: two full objects and half a round in the tails */
  private final case class Compacted(base: Long, round: Int, entries: Vector[EventLogEntry], objects: Vector[FullView])

  private final class Data(seed: Int):
    private val rnd = new Random(seed)

    /** Incompressible, so every record of one size has the same size and frame bounds are exact */
    def apply(n: Int): Array[Byte] =
      val a = new Array[Byte](n)
      rnd.nextBytes(a)
      a

  private val data = new Data(7)

  /** Size of a record of n data bytes. typeA gets id 0, which protobuf omits; the late events' typeB does not. */
  private def recordSize(n: Int, typeId: Short = 0): Int =
    LogRecords.recordSize(LoggedRecord(HLCTimestamp.now, typeId, new Array[Byte](n)))

  private def lateSize(n: Int): Int = recordSize(n, typeId = 1)

  private def fits(raw: Long): Boolean = FullObject.overhead + LogRecords.maxFrameSize(raw.toInt) <= Target

  private def rawSize(rs: Seq[LoggedRecord]): Long = rs.map(r => LogRecords.recordSize(r).toLong).sum

  /** Entries logged in one transaction, so one batch moves them all */
  private def logTogether(es: Seq[EventLogEntry]): Future[Unit] =
    client.transact: tx =>
      given Transaction = tx
      tx.replaySafe("event log entries may be duplicated")
      client.prepareSystemEventLogEntries(es)

  /** Fills the tails two and a half times before the service starts, as the compaction test does */
  private def compacted(): Future[Compacted] =
    given ExecutionContext = executionContext
    val base = HLCTimestamp.now.asLong
    val recSize = recordSize(100)
    val perTail = Iterator.from(1).takeWhile(n => LogRecords.maxFrameSize(n * recSize) <= Target / 10).max
    val round = perTail * EventLogState.TailCount
    val entries = (0 until 2 * round + round / 2).toVector
      .map(i => EventLogEntry(HLCTimestamp(base + Spacing * i), typeA, data(100)))
    for
      ptr <- SystemEventLog.statePointer(client)
      _ <- logInGroups(entries, 25)
      _ = makeService(ptr, targetSize = Target)
      _ <- awaitIncomingEmpty()
      objs <- fullObjects()
    yield
      objs.map(o => LogKey.decode(o.key)) shouldBe Vector(HLCTimestamp(base), HLCTimestamp(base + Spacing * round))
      Compacted(base, round, entries, objs)

  /** Logs `late` after the compaction and returns the full objects once they are moved */
  private def logLate(late: Seq[EventLogEntry]): Future[Vector[FullView]] =
    given ExecutionContext = executionContext
    for
      _ <- logTogether(late)
      _ <- awaitIncomingEmpty()
      objs <- fullObjects()
    yield objs

  /** Every object sorted, its tree key its filedKey and its first record's timestamp, within the target */
  private def checkObjects(objs: Vector[FullView]): Unit =
    objs.foreach: o =>
      o.records.map(_.timestamp) shouldBe o.records.map(_.timestamp).sorted
      LogKey.decode(o.key) shouldBe o.header.filedKey
      o.header.filedKey shouldBe o.records.head.timestamp
      o.state.size should be <= Target
    objs.map(_.header.filedKey) shouldBe objs.map(_.header.filedKey).sorted.distinct

  private def expectedLogged(c: Compacted, late: Seq[EventLogEntry]): Future[Unit] =
    given ExecutionContext = executionContext
    for
      inFull <- fullEntries()
      inTails <- tailEntries()
    yield
      inFull shouldBe (c.entries.take(2 * c.round) ++ late).sortBy(_.timestamp)
      multiset(inTails) shouldBe multiset(c.entries.drop(2 * c.round))

  atest("a late event is merged into its covering full object"):
    given ExecutionContext = executionContext
    for
      c <- compacted()
      late = EventLogEntry(HLCTimestamp(c.base + Spacing * (c.round + 3) + 5), typeB, data(10))
      _ = fits(rawSize(c.objects(1).records) + lateSize(10)) shouldBe true
      objs <- logLate(Seq(late))
      _ <- expectedLogged(c, Seq(late))
      st <- readState()
    yield
      objs.size shouldBe 2
      checkObjects(objs)
      objs(0).records shouldBe c.objects(0).records
      objs(0).state.revision shouldBe c.objects(0).state.revision
      objs(1).key shouldBe c.objects(1).key
      objs(1).state.pointer shouldBe c.objects(1).state.pointer
      objs(1).header.upperBound shouldBe None
      objs(1).records.size shouldBe c.objects(1).records.size + 1
      objs(1).records(4).timestamp shouldBe late.timestamp
      st.tailFloor shouldBe HLCTimestamp(c.base + Spacing * (2 * c.round - 1))

  atest("a late event that overflows an object splits it at a timestamp boundary"):
    given ExecutionContext = executionContext
    for
      c <- compacted()
      held = rawSize(c.objects(0).records)
      count = Iterator.from(1).find(n => !fits(held + n.toLong * lateSize(100))).get
      late = (0 until count).map(k => EventLogEntry(HLCTimestamp(c.base + Spacing * k + 5), typeB, data(100)))
      objs <- logLate(late)
      _ <- expectedLogged(c, late)
    yield
      objs.size shouldBe 3
      checkObjects(objs)
      // The first part is the old object rewritten in place, its upper bound the second part's key
      objs(0).state.pointer shouldBe c.objects(0).state.pointer
      objs(0).key shouldBe c.objects(0).key
      objs(0).header.upperBound shouldBe Some(objs(1).header.filedKey)
      objs(1).state.pointer should not be c.objects(0).state.pointer
      objs(1).header.upperBound shouldBe c.objects(0).header.upperBound
      objs(2).state.pointer shouldBe c.objects(1).state.pointer
      objs(2).records shouldBe c.objects(1).records
      objs(0).records.last.timestamp should be < objs(1).header.filedKey

  atest("an event older than every full object rekeys the first object"):
    given ExecutionContext = executionContext
    for
      c <- compacted()
      late = EventLogEntry(HLCTimestamp(c.base - 5), typeB, data(10))
      _ = fits(rawSize(c.objects(0).records) + lateSize(10)) shouldBe true
      objs <- logLate(Seq(late))
      _ <- expectedLogged(c, Seq(late))
    yield
      objs.size shouldBe 2
      checkObjects(objs)
      LogKey.decode(objs(0).key) shouldBe late.timestamp
      objs(0).state.pointer shouldBe c.objects(0).state.pointer
      objs(0).header.upperBound shouldBe None
      objs(0).records.head.timestamp shouldBe late.timestamp
      objs(1).key shouldBe c.objects(1).key

  atest("a rekey that splits files the second part under the old key"):
    given ExecutionContext = executionContext
    for
      c <- compacted()
      recSize = lateSize(100)
      // As many records before the first object as fill one object, so the part after them starts at the old key
      count = Iterator.from(1).takeWhile(n => fits(n.toLong * recSize)).max
      _ = fits(count.toLong * recSize + LogRecords.recordSize(c.objects(0).records.head)) shouldBe false
      late = (0 until count).map(k => EventLogEntry(HLCTimestamp(c.base - Spacing * (count - k)), typeB, data(100)))
      objs <- logLate(late)
      _ <- expectedLogged(c, late)
    yield
      objs.size shouldBe 3
      checkObjects(objs)
      LogKey.decode(objs(0).key) shouldBe late.head.timestamp
      objs(0).state.pointer shouldBe c.objects(0).state.pointer
      objs(0).records.size shouldBe count
      objs(0).header.upperBound shouldBe Some(HLCTimestamp(c.base))
      objs(1).key shouldBe c.objects(0).key
      objs(1).state.pointer should not be c.objects(0).state.pointer
      objs(1).records shouldBe c.objects(0).records
      objs(2).state.pointer shouldBe c.objects(1).state.pointer
