package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.{DataObjectState, RangeReadNotSupported, RangeWriteBeyondEnd, RangeWriteNotSupported, RangedWrite, RangedWriteState, Transaction}
import org.aspen_ddp.aspen.client.internal.OpportunisticRebuildManager
import org.aspen_ddp.aspen.client.internal.transaction.TransactionBuilder
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.ida.{AppendingReedSolomon, IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.network.ClientId
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, TransactionId}
import org.aspen_ddp.aspen.server.repair.{RepairPolicy, StoreRepairer}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.Random

/** End-to-end coverage of ranged writes against live test networks */
trait RangedWriteIntegrationTests:
  this: IntegrationTestSuite =>

  private def ida: IDA = net.ida

  /** Bytes per row for slicing IDAs. Replication has no rows; any row size exercises it equally. */
  private def rowBytes: Int = ida match
    case a: AppendingReedSolomon => a.rowBytes
    case _ => 64

  private def bytesShouldMatch(actual: Array[Byte], expected: Array[Byte]): Unit =
    withClue("length: "):
      actual.length should be (expected.length)
    val mismatch = actual.indices.find(i => actual(i) != expected(i))
    withClue(s"bytes differ at index ${mismatch.getOrElse(-1)}: "):
      mismatch should be (None)

  private def chunkOf(rnd: Random, length: Int): Array[Byte] =
    val a = new Array[Byte](length)
    rnd.nextBytes(a)
    a

  private def sequentially[A, B](items: List[A])(f: A => Future[B]): Future[List[B]] =
    val reversed = items.foldLeft(Future.successful(List.empty[B])): (acc, item) =>
      acc.flatMap(rs => f(item).map(r => r :: rs))
    reversed.map(_.reverse)

  /** Allocates an object holding `content`, linked from the radicle under a key derived from `index` */
  private def allocateWith(index: Int, content: Array[Byte]): Future[DataObjectPointer] =
    given tx: Transaction = client.newTransaction()
    val radicleKey = Key(Array[Byte](0x52, index.toByte))
    tx.update(radicle, None, None, List(KeyValueUpdate.DoesNotExist(radicleKey)),
              List(Insert(radicleKey, Array[Byte](1))))
    for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(DataBuffer(content))
      _ <- tx.commit()
    yield dp

  /** The content after applying non-overlapping writes, zero filling any gap past the end */
  private def applyWrites(content: Array[Byte], writes: List[RangedWrite]): Array[Byte] =
    val newLength = writes.map(_.end.toInt).foldLeft(content.length)(math.max)
    val result = new Array[Byte](newLength)
    System.arraycopy(content, 0, result, 0, content.length)
    writes.foreach(w => System.arraycopy(w.data, 0, result, w.offset, w.data.length))
    result

  private def stateFromFullRead(dp: DataObjectPointer): Future[RangedWriteState] =
    client.read(dp).map(RangedWriteState.fromObject)

  /** Commits `writes` in one transaction using `state`. Returns the object's new revision */
  private def commitWrites(state: RangedWriteState, writes: List[RangedWrite],
                           zeroFill: Boolean = false): Future[ObjectRevision] =
    val tx = client.newTransaction()
    tx.rangedWrites(state, writes, zeroFill)
    tx.commit().map(_ => tx.revision)

  /** Fetches a fresh state with readRangedWriteState and commits `writes` with it */
  private def fetchAndWrite(dp: DataObjectPointer, writes: List[RangedWrite],
                            zeroFill: Boolean = false): Future[ObjectRevision] =
    client.readRangedWriteState(dp, writes).flatMap(s => commitWrites(s, writes, zeroFill))

  private def storeRevisions(id: ObjectId): List[Option[ObjectRevision]] =
    net.stores.map(_.m.get(id).map(_.metadata.revision))

  /** Every store holding `revision` of the object must hold exactly a fresh encode of `expected`, proving the
    * deltas kept parity (and the trailer) correct. At least a write threshold of stores must hold it, and with
    * `requireAll` every store must
    */
  private def slicesShouldEncode(id: ObjectId, revision: ObjectRevision, expected: Array[Byte],
                                 requireAll: Boolean = false): Unit =
    val encoded = ida.encode(expected)
    val current = net.stores.zipWithIndex.filter((s, _) => s.m.get(id).exists(_.metadata.revision == revision))
    withClue(s"stores at $revision: ${storeRevisions(id)}: "):
      if requireAll then
        current.size should be (ida.width)
      else
        current.size should be >= ida.writeThreshold
    current.foreach: (s, i) =>
      withClue(s"slice $i: "):
        bytesShouldMatch(s.m(id).data.getByteArray, encoded(i))

  private def silenceableSubsets: List[Set[Int]] =
    val maxSilenced = ida.width - ida.consistentRestoreThreshold
    (0 to maxSilenced).toList.flatMap: size =>
      (0 until ida.width).toList.combinations(size).map(_.toSet)

  private def storeIds(indices: Iterable[Int]): Set[StoreId] =
    indices.map(i => net.stores(i).storeId).toSet

  /** Reads the object with every tolerable subset of stores silenced. Each read must restore `expected` */
  private def readsWithStoresDown(dp: DataObjectPointer, expected: Array[Byte]): Future[Unit] =
    val reads = sequentially(silenceableSubsets): silenced =>
      net.withReadSilenced(storeIds(silenced))(client.read(dp)).map: s =>
        withClue(s"silenced ${silenced.toList.sorted}: "):
          bytesShouldMatch(s.data.getByteArray, expected)
    reads.map(_ => ())

  /** Host.repair queues its work for the harness's event pump, which is drained by repeatedly running net.handleEvents() on the global execution context until `f` completes */
  private def pumpUntilComplete[T](f: Future[T]): Future[T] =
    if f.isCompleted then
      f
    else
      Future { Thread.sleep(10); net.handleEvents() }.flatMap(_ => pumpUntilComplete(f))

  /** Drives the existing repair path (see AppendIntegrationTests.repairUntilCurrent) until every store holds
    * `revision`
    */
  private def repairUntilCurrent(id: ObjectId, revision: ObjectRevision, attempts: Int = 100): Future[Unit] =
    def current: Boolean = net.stores.forall(_.m.get(id).map(_.metadata.revision).contains(revision))
    if current then
      Future.successful(())
    else if attempts <= 0 then
      Future.failed(new AssertionError(s"repair never brought every store to $revision: ${storeRevisions(id)}"))
    else
      val repairer = StoreRepairer(client, net.smgr)
      for
        _ <- net.waitForTransactionsToComplete()
        _ <- sequentially((0 until ida.width).toList): i =>
               pumpUntilComplete(repairer.scan(net.stores(i).storeId, RepairPolicy.Default))
        _ <- net.waitForTransactionsToComplete()
        _ <- if current then Future.successful(()) else Future(Thread.sleep(50))
        _ <- repairUntilCurrent(id, revision, attempts - 1)
      yield ()

  /** Commits `writes` against an object holding `content`, then checks the full read and every current slice */
  private def writeAndVerify(content: Array[Byte], writes: List[RangedWrite], zeroFill: Boolean = false)
      : Future[Array[Byte]] =
    val expected = applyWrites(content, writes)
    for
      dp <- allocateWith(1, content)
      rev <- fetchAndWrite(dp, writes, zeroFill)
      after <- client.read(dp)
      _ <- net.waitForTransactionsToComplete()
    yield
      after.revision should be (rev)
      bytesShouldMatch(after.data.getByteArray, expected)
      slicesShouldEncode(dp.id, rev, expected)
      after.data.getByteArray

  atest("overwrite inside a row reads back the full object"):
    val rnd = new Random(101)
    val R = rowBytes
    val content = chunkOf(rnd, 3 * R + 17)
    writeAndVerify(content, List(RangedWrite(R + 5, chunkOf(rnd, 7)))).map: after =>
      after.length should be (content.length)

  atest("write spanning rows"):
    val rnd = new Random(102)
    val R = rowBytes
    val content = chunkOf(rnd, 5 * R + 9)
    writeAndVerify(content, List(RangedWrite(R - 10, chunkOf(rnd, 2 * R + 20)))).map: after =>
      after.length should be (content.length)

  atest("write extending past the end"):
    val rnd = new Random(103)
    val R = rowBytes
    val content = chunkOf(rnd, 2 * R + 3)
    val L = content.length
    // Overlaps the tail and extends across a row boundary
    val w1 = RangedWrite(L - 5, chunkOf(rnd, R + 30))
    // Starts exactly at the end
    val w2 = RangedWrite(L + R + 25, chunkOf(rnd, 11))
    // Old length 4R-10 (4 rows), new length 4R-6 leaves under 8 bytes free in the 4th row, so a slicing IDA's
    // trailer moves into a 5th row
    val spill = chunkOf(rnd, 4 * R - 10)
    val w3 = RangedWrite(spill.length - 2, chunkOf(rnd, 6))
    val afterFirst = applyWrites(content, List(w1))
    val expected = applyWrites(afterFirst, List(w2))
    for
      dp <- allocateWith(1, content)
      _ <- fetchAndWrite(dp, List(w1))
      mid <- client.read(dp)
      rev <- fetchAndWrite(dp, List(w2))
      after <- client.read(dp)
      sp <- allocateWith(2, spill)
      spRev <- fetchAndWrite(sp, List(w3))
      spAfter <- client.read(sp)
      _ <- net.waitForTransactionsToComplete()
    yield
      bytesShouldMatch(mid.data.getByteArray, afterFirst)
      bytesShouldMatch(after.data.getByteArray, expected)
      after.data.size should be (L + R + 36)
      slicesShouldEncode(dp.id, rev, expected)
      bytesShouldMatch(spAfter.data.getByteArray, applyWrites(spill, List(w3)))
      slicesShouldEncode(sp.id, spRev, applyWrites(spill, List(w3)))
      succeed

  atest("zero filled gap reads as zeros"):
    val rnd = new Random(104)
    val R = rowBytes
    val content = chunkOf(rnd, 100)
    val gapStart = content.length
    val offset = gapStart + 2 * R + 13
    val w = RangedWrite(offset, chunkOf(rnd, 20))
    writeAndVerify(content, List(w), zeroFill = true).map: after =>
      after.length should be (offset + 20)
      after.slice(gapStart, offset).forall(_ == 0) should be (true)

  atest("zero filled write to an empty object"):
    val rnd = new Random(105)
    val w = RangedWrite(50, chunkOf(rnd, 9))
    writeAndVerify(Array[Byte](), List(w), zeroFill = true).map: after =>
      after.length should be (59)
      after.take(50).forall(_ == 0) should be (true)

  atest("gap without zeroFill fails and leaves the object unchanged"):
    val rnd = new Random(106)
    val content = chunkOf(rnd, 100)
    val writes = List(RangedWrite(5, chunkOf(rnd, 3)), RangedWrite(101, chunkOf(rnd, 4)))
    for
      dp <- allocateWith(1, content)
      before <- client.read(dp)
      state <- client.readRangedWriteState(dp, writes)
      tx = client.newTransaction()
      _ = tx.rangedWrites(state, writes)
      r <- tx.commit().map(_ => None).recover { case e => Some(e) }
      after <- client.read(dp)
    yield
      withClue(s"commit result $r: "):
        r.exists(_.isInstanceOf[RangeWriteBeyondEnd]) should be (true)
      after.revision should be (before.revision)
      bytesShouldMatch(after.data.getByteArray, content)
      succeed

  atest("multiple writes in one call"):
    val rnd = new Random(107)
    val R = rowBytes
    val content = chunkOf(rnd, 4 * R + 21)
    val L = content.length
    // Unsorted: inside a row, touching neighbours, spanning rows, and extending past the end
    val writes = List(
      RangedWrite(L - 4, chunkOf(rnd, R + 3)),
      RangedWrite(3, chunkOf(rnd, 5)),
      RangedWrite(2 * R - 7, chunkOf(rnd, R + 14)),
      RangedWrite(8, chunkOf(rnd, 2)),
      RangedWrite(R - 1, chunkOf(rnd, 2)))
    writeAndVerify(content, writes).map: after =>
      after.length should be (L + R - 1)

  atest("state from a full read and from readRangedWriteState give identical results"):
    val rnd = new Random(108)
    val R = rowBytes
    val content = chunkOf(rnd, 3 * R + 40)
    val writes = List(RangedWrite(R + 1, chunkOf(rnd, R)), RangedWrite(10, chunkOf(rnd, 3)),
                      RangedWrite(content.length - 2, chunkOf(rnd, 30)))
    val expected = applyWrites(content, writes)
    for
      dpFull <- allocateWith(1, content)
      dpRanged <- allocateWith(2, content)
      fullState <- stateFromFullRead(dpFull)
      revFull <- commitWrites(fullState, writes)
      revRanged <- fetchAndWrite(dpRanged, writes)
      afterFull <- client.read(dpFull)
      afterRanged <- client.read(dpRanged)
      _ <- net.waitForTransactionsToComplete()
    yield
      bytesShouldMatch(afterFull.data.getByteArray, expected)
      bytesShouldMatch(afterRanged.data.getByteArray, expected)
      slicesShouldEncode(dpFull.id, revFull, expected)
      slicesShouldEncode(dpRanged.id, revRanged, expected)
      succeed

  atest("stale state aborts and leaves content intact"):
    val rnd = new Random(109)
    val content = chunkOf(rnd, 300)
    val a = List(RangedWrite(10, chunkOf(rnd, 5)))
    val b = List(RangedWrite(200, chunkOf(rnd, 7)))
    val c = List(RangedWrite(20, chunkOf(rnd, 4)))
    for
      dp <- allocateWith(1, content)
      state <- client.readRangedWriteState(dp, a ++ b ++ c)
      // Both racers hold the same revision. Exactly one may commit, even though their ranges do not overlap
      txA = client.newTransaction()
      _ = txA.rangedWrites(state, a)
      txB = client.newTransaction()
      _ = txB.rangedWrites(state, b)
      ra <- txA.commit().map(_ => true).recover(_ => false)
      rb <- txB.commit().map(_ => true).recover(_ => false)
      mid <- client.read(dp)
      // The state used by the race is now stale
      rc <- commitWrites(state, c).map(_ => true).recover(_ => false)
      after <- client.read(dp)
    yield
      withClue(s"A committed=$ra B committed=$rb: "):
        (ra != rb) should be (true)
      val expected = applyWrites(content, if ra then a else b)
      bytesShouldMatch(mid.data.getByteArray, expected)
      rc should be (false)
      after.revision should be (mid.revision)
      bytesShouldMatch(after.data.getByteArray, expected)
      succeed

  atest("a store behind on ranged writes is repaired and later ranged writes still succeed"):
    val rnd = new Random(110)
    val R = rowBytes
    val content = chunkOf(rnd, 6 * R + 5)
    def randomWrite(len: Int): RangedWrite =
      RangedWrite(rnd.nextInt(len), chunkOf(rnd, 1 + rnd.nextInt(R + 10)))
    // Each write extends the object at most past its current end, never leaving a gap
    def writeAll(dp: DataObjectPointer, start: Array[Byte], n: Int): Future[(Array[Byte], ObjectRevision)] =
      (1 to n).foldLeft(Future.successful((start, ObjectRevision.Null))): (acc, _) =>
        acc.flatMap: (cur, _) =>
          val w = randomWrite(cur.length)
          fetchAndWrite(dp, List(w)).map(rev => (applyWrites(cur, List(w)), rev))
    for
      dp <- allocateWith(1, content)
      (first, midRev) <- writeAll(dp, content, 10)
      before <- client.read(dp)
      _ = withClue("expected a store to be behind before repair: "):
            storeRevisions(dp.id).exists(!_.contains(midRev)) should be (true)
      _ <- repairUntilCurrent(dp.id, midRev)
      _ = slicesShouldEncode(dp.id, midRev, first, requireAll = true)
      // Later ranged writes apply as deltas on the repaired slices
      (second, lastRev) <- writeAll(dp, first, 5)
      after <- client.read(dp)
      _ = withClue("expected a store to be behind after the later writes: "):
            storeRevisions(dp.id).exists(!_.contains(lastRev)) should be (true)
      _ <- repairUntilCurrent(dp.id, lastRev)
      _ <- net.waitForTransactionsToComplete()
      _ = slicesShouldEncode(dp.id, lastRev, second, requireAll = true)
      _ <- readsWithStoresDown(dp, second)
    yield
      bytesShouldMatch(before.data.getByteArray, first)
      bytesShouldMatch(after.data.getByteArray, second)
      succeed

  atest("slices match a fresh encode and every tolerated set of down stores restores the content"):
    val rnd = new Random(111)
    val R = rowBytes
    val content = chunkOf(rnd, 7 * R + 30)
    val w1 = List(RangedWrite(3, chunkOf(rnd, 4)), RangedWrite(3 * R - 2, chunkOf(rnd, 2 * R + 4)))
    val w2 = List(RangedWrite(content.length - 1, chunkOf(rnd, R + 2)))
    val w3 = List(RangedWrite(content.length + 3 * R, chunkOf(rnd, 15)), RangedWrite(R, chunkOf(rnd, 1)))
    val expected = applyWrites(applyWrites(applyWrites(content, w1), w2), w3)
    for
      dp <- allocateWith(1, content)
      _ <- fetchAndWrite(dp, w1)
      _ <- stateFromFullRead(dp).flatMap(s => commitWrites(s, w2))
      rev <- fetchAndWrite(dp, w3, zeroFill = true)
      _ <- net.waitForTransactionsToComplete()
      _ = slicesShouldEncode(dp.id, rev, expected)
      _ <- repairUntilCurrent(dp.id, rev)
      _ <- net.waitForTransactionsToComplete()
      _ = slicesShouldEncode(dp.id, rev, expected, requireAll = true)
      _ <- readsWithStoresDown(dp, expected)
    yield succeed

  /** Content whose byte at index i is a function of i alone, so any revision's prefix is predictable */
  private def patternAt(i: Int): Byte = (i * 31 + 7).toByte

  /** Appends 37 byte pattern chunks in the background, starting at `start`, `count` times */
  private def appendPattern(dp: DataObjectPointer, start: Int, count: Int): Future[Unit] =
    client.readAppendPosition(dp).flatMap: pos0 =>
      (0 until count).foldLeft(Future.successful(pos0)): (fpos, n) =>
        fpos.flatMap: pos =>
          val tx = client.newTransaction()
          val chunk = Array.tabulate[Byte](37)(i => patternAt(start + n * 37 + i))
          val next = tx.append(pos, DataBuffer(chunk))
          tx.commit().map(_ => next)
      .map(_ => ())

  private def repeat(times: Int)(f: Int => Future[Unit]): Future[Unit] =
    (0 until times).foldLeft(Future.successful(())): (acc, n) =>
      acc.flatMap(_ => f(n))

  atest("readRangedWriteState never fails with a mixed revision while a writer appends"):
    val R = rowBytes
    val initial = Array.tabulate[Byte](3 * R + 5)(patternAt)
    val writes = List(RangedWrite(3, Array[Byte](1, 2, 3)), RangedWrite(R + 2, Array.fill[Byte](R)(5)),
                      RangedWrite(initial.length - 4, Array.fill[Byte](20)(6)))
    for
      dp <- allocateWith(1, initial)
      appender = appendPattern(dp, initial.length, 25)
      _ <- repeat(25): _ =>
             client.readRangedWriteState(dp, writes).map: state =>
               state.length should be >= initial.length
               writes.foreach: w =>
                 val end = math.min(w.end, state.length.toLong).toInt
                 val expected = (w.offset until end).map(i => patternAt(i)).toList
                 state.oldBytes(w.offset, w.data.length).toList should be (expected)
      _ <- appender
    yield succeed

  atest("readRanges returns every range at one revision"):
    val R = rowBytes
    val initial = Array.tabulate[Byte](3 * R + 5)(patternAt)
    val ranges = List((0, 10), (R, R), (initial.length - 4, 30))
    for
      dp <- allocateWith(1, initial)
      appender = appendPattern(dp, initial.length, 25)
      _ <- repeat(25): _ =>
             client.readRanges(dp, ranges).map: states =>
               states.map(_.revision).distinct.length should be (1)
      _ <- appender
    yield succeed

  atest("two far apart edits in a large object"):
    val rnd = new Random(112)
    val content = chunkOf(rnd, 64 * 1024)
    val writes = List(RangedWrite(100, chunkOf(rnd, 16)), RangedWrite(60000, chunkOf(rnd, 16)))
    writeAndVerify(content, writes).map: after =>
      after.length should be (content.length)


class ReplicationRangedWriteIntegrationSuite extends IntegrationTestSuite with RangedWriteIntegrationTests

class AppendingReedSolomonRangedWriteIntegrationSuite extends IntegrationTestSuite with RangedWriteIntegrationTests:
  override def testIda: IDA = AppendingReedSolomon(5, 3, 4, 64)

class NarrowAppendingReedSolomonRangedWriteIntegrationSuite extends IntegrationTestSuite
  with RangedWriteIntegrationTests:
  override def testIda: IDA = AppendingReedSolomon(4, 2, 3, 8)


class ClassicReedSolomonRangedWriteRejectionSuite extends IntegrationTestSuite:
  override def testIda: IDA = ReedSolomon(5, 3, 4)

  private val content = Array.tabulate[Byte](100)(_.toByte)

  private def allocate(): Future[DataObjectPointer] =
    given tx: Transaction = client.newTransaction()
    for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(DataBuffer(content))
      _ <- tx.commit()
    yield dp

  atest("readRangedWriteState fails with RangeReadNotSupported"):
    for
      dp <- allocate()
      r <- client.readRangedWriteState(dp, List(RangedWrite(0, Array[Byte](1)))).map(_ => None)
             .recover { case e => Some(e) }
    yield
      r.exists(_.isInstanceOf[RangeReadNotSupported]) should be (true)

  atest("committing a ranged write fails with RangeWriteNotSupported and leaves the object unchanged"):
    for
      dp <- allocate()
      before <- client.read(dp)
      tx = client.newTransaction()
      _ = tx.rangedWrite(RangedWriteState.fromObject(before), RangedWrite(10, Array[Byte](1, 2, 3)))
      r <- tx.commit().map(_ => None).recover { case e => Some(e) }
      after <- client.read(dp)
    yield
      withClue(s"commit result $r: "):
        r.exists(_.isInstanceOf[RangeWriteNotSupported]) should be (true)
      after.revision should be (before.revision)
      after.data.getByteArray.toList should be (content.toList)


/** The per-store payload of a ranged write depends on the rows it touches, not on the size of the object */
class RangedWritePayloadSuite extends AnyFunSuite with Matchers:

  private val pointer = DataObjectPointer(ObjectId(new UUID(0, 1)), PoolId(new UUID(0, 0)), Array[Byte]())
  private val revision = ObjectRevision(TransactionId(new UUID(0, 3)))

  private def payloadSizes(ida: IDA, content: Array[Byte], writes: List[RangedWrite]): List[Int] =
    val state = RangedWriteState.fromObject(new DataObjectState(pointer, revision, ObjectRefcount(0, 1),
      HLCTimestamp.now, HLCTimestamp.now, ida, 0, DataBuffer(content)))
    val bldr = new TransactionBuilder(TransactionId(new UUID(0, 2)), (_, _) => 0.toByte, ClientId(new UUID(0, 4)))
    bldr.rangedWrites(state, writes, false)
    val (_, updates, _) = bldr.buildTranaction(OpportunisticRebuildManager.None, Map(pointer.poolId -> ida),
                                               Map(pointer.poolId -> None))
    updates.size should be (ida.width)
    (0 until ida.width).toList.map: i =>
      val td = updates(StoreId(pointer.poolId, i.toByte))
      td.localUpdates.map(_.data.size).sum

  private val rnd = new Random(200)
  private val content = Array.fill[Byte](256 * 1024)(rnd.nextInt.toByte)
  private val writes = List(RangedWrite(1000, Array.fill[Byte](16)(1)), RangedWrite(200000, Array.fill[Byte](16)(2)))

  test("two far apart edits send small per-store payloads with AppendingReedSolomon"):
    val ida = AppendingReedSolomon(5, 3, 4, 64)
    val sliceSize = ida.encode(content)(0).length
    val sizes = payloadSizes(ida, content, writes)
    // Two touched rows: at most one cell per row per store, plus the delta's headers
    withClue(s"payload sizes $sizes against slices of $sliceSize bytes: "):
      sizes.foreach(_ should be < 512)
      sizes.foreach(s => (s * 100) should be < sliceSize)

  test("two far apart edits send small per-store payloads with Replication"):
    val ida = Replication(3, 2)
    val sizes = payloadSizes(ida, content, writes)
    withClue(s"payload sizes $sizes against replicas of ${content.length} bytes: "):
      sizes.foreach(_ should be < 256)
