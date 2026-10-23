package org.aspen_ddp.aspen

import org.aspen_ddp.aspen.client.{AppendNotSupported, AppendPosition, RangeReadNotSupported, Transaction}
import org.aspen_ddp.aspen.common.DataBuffer
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.{AppendingReedSolomon, IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Insert, Key, ObjectId, ObjectRevision}
import org.aspen_ddp.aspen.server.repair.{RepairPolicy, StoreRepairer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.Random

/** End-to-end coverage of data object append and byte-range reads against live test networks. */
trait AppendIntegrationTests:
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

  private def linkToRadicle(tx: Transaction, index: Int): Unit =
    val radicleKey = Key(Array[Byte](0x50, index.toByte))
    tx.update(radicle, None, None, List(KeyValueUpdate.DoesNotExist(radicleKey)),
              List(Insert(radicleKey, Array[Byte](1))))

  /** Allocates an empty DataObject linked from the radicle under a key derived from `index`.
    * The initial content is supplied explicitly (rather than using the bare allocateDataObject())
    * so that a slicing IDA writes the one-row empty object, trailer included.
    */
  private def allocate(index: Int): Future[DataObjectPointer] = allocateWith(index, Array[Byte]())

  /** Allocates an object already holding `content`, linked from the radicle */
  private def allocateWith(index: Int, content: Array[Byte]): Future[DataObjectPointer] =
    given tx: Transaction = client.newTransaction()
    linkToRadicle(tx, index)
    for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(DataBuffer(content))
      _ <- tx.commit()
    yield dp

  private def appendTo(pos: AppendPosition, chunk: Array[Byte]): Future[AppendPosition] =
    val tx = client.newTransaction()
    val next = tx.append(pos, DataBuffer(chunk))
    tx.commit().map(_ => next)

  /** Appends each chunk in turn, carrying the returned position forward */
  private def appendAll(start: AppendPosition, chunks: List[Array[Byte]]): Future[AppendPosition] =
    chunks.foldLeft(Future.successful(start)): (fpos, chunk) =>
      fpos.flatMap(pos => appendTo(pos, chunk))

  private def fullContent(chunks: List[Array[Byte]]): Array[Byte] =
    if chunks.isEmpty then Array[Byte]() else chunks.reduce(_ ++ _)

  private def silenceableSubsets: List[Set[Int]] =
    val maxSilenced = ida.width - ida.consistentRestoreThreshold
    (0 to maxSilenced).toList.flatMap: size =>
      (0 until ida.width).toList.combinations(size).map(_.toSet)

  private def storeIds(indices: Iterable[Int]): Set[StoreId] =
    indices.map(i => net.stores(i).storeId).toSet

  atest("single writer carries the position across 50 appends"):
    val rnd = new Random(1)
    val chunks = List.fill(50)(chunkOf(rnd, 1 + rnd.nextInt(2000)))
    for
      dp <- allocate(1)
      pos <- client.readAppendPosition(dp)
      last <- appendAll(pos, chunks)
      state <- client.read(dp)
      finalPos <- client.readAppendPosition(dp)
    yield
      val expected = fullContent(chunks)
      bytesShouldMatch(state.data.getByteArray, expected)
      last.length should be (expected.length)
      finalPos should be (last)

  atest("independent appender learns the position with a tail read"):
    val rnd = new Random(2)
    val chunks = List.fill(20)(chunkOf(rnd, 1 + rnd.nextInt(700)))
    for
      dp <- allocate(1)
      _ <- sequentially(chunks): chunk =>
             client.readAppendPosition(dp).flatMap(pos => appendTo(pos, chunk))
      state <- client.read(dp)
    yield
      bytesShouldMatch(state.data.getByteArray, fullContent(chunks))
      succeed

  atest("long sequence of 1-byte appends"):
    val chunks = List.tabulate(200)(i => Array[Byte]((i * 13 + 5).toByte))
    for
      dp <- allocate(1)
      pos <- client.readAppendPosition(dp)
      _ <- appendAll(pos, chunks)
      state <- client.read(dp)
    yield
      bytesShouldMatch(state.data.getByteArray, fullContent(chunks))
      succeed

  atest("range reads return exact logical bytes"):
    val rnd = new Random(3)
    val R = rowBytes
    // Build a 20 KiB object by appends
    val chunks = List.fill(20)(chunkOf(rnd, 1024))
    val content = fullContent(chunks)
    val L = content.length

    // Trailer-spill object: fewer than 8 bytes free in the final row, so a slicing IDA's trailer
    // lands in a new row. L % R is in (R-8, R).
    val spillLength = 4 * R - 4
    val spillContent = chunkOf(rnd, spillLength)

    def check(dp: DataObjectPointer, content: Array[Byte], offset: Int, length: Option[Int]): Future[Unit] =
      val len = content.length
      client.readRange(dp, offset, length).map: rs =>
        withClue(s"offset $offset length $length: "):
          val end = length.map(l => math.min(offset.toLong + l, len.toLong)).getOrElse(len.toLong)
          val expected = if offset >= len then Array[Byte]() else content.slice(offset, end.toInt)
          rs.offset should be (offset)
          bytesShouldMatch(rs.data.getByteArray, expected)
          val reachesEnd = length.forall(l => offset.toLong + l >= len)
          if reachesEnd && offset <= len then
            rs.objectLength should be (Some(len))
            rs.appendPosition.map(_.length) should be (Some(len))
          else if offset > len then
            // A window starting past the end of a slicing IDA's slices carries no trailer, so the
            // length may be unknown. It must never be wrong.
            rs.objectLength.foreach(_ should be (len))

    val offsets = List(0, 1, R - 1, R, L - 1, L, L + 100)
    val lengths = List(None, Some(0), Some(1), Some(500), Some(L + 5000))

    for
      dp <- allocate(1)
      pos <- client.readAppendPosition(dp)
      _ <- appendAll(pos, chunks)
      _ <- sequentially(for o <- offsets; l <- lengths yield (o, l))((o, l) => check(dp, content, o, l))
      // A bounded window stopping inside a row, and one whose end is past L
      _ <- check(dp, content, R + 3, Some(R / 2))
      _ <- check(dp, content, 2 * R - 5, Some(R + 7))
      _ <- check(dp, content, L - 10, Some(1000))
      sp <- allocateWith(2, spillContent)
      _ <- check(sp, spillContent, 0, None)
      _ <- check(sp, spillContent, spillLength - 10, Some(14))
      _ <- check(sp, spillContent, spillLength - 10, Some(10))
      _ <- check(sp, spillContent, spillLength - 3, Some(4))
      _ <- check(sp, spillContent, spillLength, Some(4))
      _ <- check(sp, spillContent, spillLength + 1, None)
      _ <- check(sp, spillContent, 3 * R, Some(R + 100))
    yield succeed

  atest("tailing follows new appends"):
    val rnd = new Random(4)
    val chunks = List.fill(15)(chunkOf(rnd, 1 + rnd.nextInt(300)))
    for
      dp <- allocate(1)
      pos <- client.readAppendPosition(dp)
      _ <- chunks.foldLeft(Future.successful((pos, 0))): (acc, chunk) =>
             acc.flatMap: (p, lastSeen) =>
               appendTo(p, chunk).flatMap: next =>
                 client.readRange(dp, lastSeen, None).map: rs =>
                   bytesShouldMatch(rs.data.getByteArray, chunk)
                   rs.appendPosition should be (Some(next))
                   (next, next.length)
    yield succeed

  atest("stale position aborts and leaves content intact"):
    val rnd = new Random(5)
    val base = chunkOf(rnd, 777)
    val a = chunkOf(rnd, 100)
    val b = chunkOf(rnd, 150)
    for
      dp <- allocate(1)
      pos0 <- client.readAppendPosition(dp)
      pos <- appendTo(pos0, base)
      txA = client.newTransaction()
      _ = txA.append(pos, DataBuffer(a))
      txB = client.newTransaction()
      _ = txB.append(pos, DataBuffer(b))
      ra <- txA.commit().map(_ => true).recover(_ => false)
      rb <- txB.commit().map(_ => true).recover(_ => false)
      state <- client.read(dp)
    yield
      withClue(s"A committed=$ra B committed=$rb: "):
        (ra != rb) should be (true)
      val winner = if ra then a else b
      bytesShouldMatch(state.data.getByteArray, base ++ winner)
      succeed

  /** Host.repair queues its work for the harness's event pump, which must run on the test thread */
  private def pumpUntilComplete[T](f: Future[T]): Future[T] =
    if f.isCompleted then
      f
    else
      Future { Thread.sleep(10); net.handleEvents() }.flatMap(_ => pumpUntilComplete(f))

  /** Drives the existing repair path: the missed-update finalization action records the store in the
    * pool's errorTree, and StoreRepairer scans each store's entries, reads the current object and
    * has the host install the store's re-encoded content (trailer included). Retried because the
    * errorTree entry is written asynchronously after the transaction.
    */
  private def repairUntilCurrent(id: ObjectId, revision: ObjectRevision, attempts: Int = 100): Future[Unit] =
    def current: Boolean = net.stores.forall(_.m.get(id).map(_.metadata.revision).contains(revision))
    if current then
      Future.successful(())
    else if attempts <= 0 then
      Future.failed(new AssertionError(
        s"repair never brought every store to $revision: " +
        net.stores.toList.map(_.m.get(id).map(_.metadata.revision))))
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

  atest("a store behind on appends is repaired and later appends still succeed"):
    val rnd = new Random(6)
    val first = List.fill(20)(chunkOf(rnd, 1 + rnd.nextInt(500)))
    val second = List.fill(5)(chunkOf(rnd, 1 + rnd.nextInt(500)))
    for
      dp <- allocate(1)
      pos <- client.readAppendPosition(dp)
      mid <- appendAll(pos, first)
      before <- client.read(dp)
      _ <- repairUntilCurrent(dp.id, mid.revision)
      // Later appends apply as deltas on the repaired slices
      last <- appendAll(mid, second)
      after <- client.read(dp)
      _ <- repairUntilCurrent(dp.id, last.revision)
      _ <- net.waitForTransactionsToComplete()
      results <- sequentially(silenceableSubsets): silenced =>
                   net.withReadSilenced(storeIds(silenced))(client.read(dp)).map(s => silenced -> s.data.getByteArray)
    yield
      bytesShouldMatch(before.data.getByteArray, fullContent(first))
      val expected = fullContent(first ++ second)
      last.length should be (expected.length)
      bytesShouldMatch(after.data.getByteArray, expected)
      results.foreach: (silenced, restored) =>
        withClue(s"silenced ${silenced.toList.sorted}: "):
          bytesShouldMatch(restored, expected)
      succeed


class ReplicationAppendIntegrationSuite extends IntegrationTestSuite with AppendIntegrationTests

class AppendingReedSolomonAppendIntegrationSuite extends IntegrationTestSuite with AppendIntegrationTests:
  override def testIda: IDA = AppendingReedSolomon(5, 3, 4, 64)

class NarrowAppendingReedSolomonAppendIntegrationSuite extends IntegrationTestSuite with AppendIntegrationTests:
  override def testIda: IDA = AppendingReedSolomon(4, 2, 3, 8)


/** KV objects, including partial reads, must keep working in an AppendingReedSolomon pool */
class AppendingReedSolomonBasicIntegrationSuite extends BasicIntegrationSuite:
  override def testIda: IDA = AppendingReedSolomon(5, 3, 4, 64)


class ClassicReedSolomonAppendRejectionSuite extends IntegrationTestSuite:
  override def testIda: IDA = ReedSolomon(5, 3, 4)

  private def allocate(): Future[DataObjectPointer] =
    given tx: Transaction = client.newTransaction()
    for
      pool <- client.getStoragePool(Radicle.poolId)
      dp <- pool.allocator.allocateDataObject(DataBuffer(Array.tabulate[Byte](100)(_.toByte)))
      _ <- tx.commit()
    yield dp

  atest("readAppendPosition fails with RangeReadNotSupported"):
    for
      dp <- allocate()
      r <- client.readAppendPosition(dp).map(_ => None).recover { case e => Some(e) }
      r2 <- client.readRange(dp, 0, None).map(_ => None).recover { case e => Some(e) }
    yield
      r.exists(_.isInstanceOf[RangeReadNotSupported]) should be (true)
      r2.exists(_.isInstanceOf[RangeReadNotSupported]) should be (true)

  atest("committing an append fails with AppendNotSupported"):
    for
      dp <- allocate()
      state <- client.read(dp)
      tx = client.newTransaction()
      _ = tx.append(AppendPosition(dp, state.revision, state.data.size), DataBuffer(Array[Byte](1, 2, 3)))
      r <- tx.commit().map(_ => None).recover { case e => Some(e) }
    yield
      r.exists(_.isInstanceOf[AppendNotSupported]) should be (true)
