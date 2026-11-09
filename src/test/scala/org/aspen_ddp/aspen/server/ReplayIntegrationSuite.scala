package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.network.{TxMessage, TxPrepare}
import org.aspen_ddp.aspen.common.objects.{Delete, Insert, Key, KeyValueObjectPointer, ObjectRevision, Value}
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, TransactionId}

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

/** A transaction's client prepares, re-delivered after every store finalized and forgot it, must not apply it again.
  *
  * The bootstrap host records its stores' CRL state and has recovery drivers, as TracelessCommitsIntegrationSuite's
  * does. Its heartbeat is short so the status cache forgets a transaction quickly.
  */
class ReplayIntegrationSuite extends IntegrationTestSuite:

  override def testIda: IDA = ReedSolomon(5, 3, 4)

  override def testHostOptions: TestNetwork.HostOptions =
    TestNetwork.HostOptions(heartbeatPeriod = Duration(300, MILLISECONDS), recordCrl = true, recoveryDrivers = true)

  private val k = Key(Array[Byte](1))

  private def deadline(): Long = System.nanoTime() + Duration(30, SECONDS).toNanos

  /** One step of the host: its time too with `clock` (TestNetwork.heartbeat), otherwise its events only */
  private def step(clock: Boolean): Unit =
    if clock then net.heartbeat() else net.handleEvents()

  private def pump[T](f: Future[T], clock: Boolean, until: Long = deadline()): Future[T] =
    if f.isCompleted then
      f
    else if System.nanoTime() > until then
      net.printTransactionStatus()
      Future.failed(new AssertionError("Timed out pumping events"))
    else
      Future { Thread.sleep(1); step(clock) }.flatMap(_ => pump(f, clock, until))

  /** Polls `cond`, pumping the host meanwhile */
  private def eventually(what: String, clock: Boolean, until: Long = deadline())(cond: => Future[Boolean]): Future[Unit] =
    cond.flatMap: ok =>
      if ok then
        Future.unit
      else if System.nanoTime() > until then
        net.printTransactionStatus()
        Future.failed(new AssertionError(s"Never happened: $what"))
      else
        Future { Thread.sleep(1); step(clock) }.flatMap(_ => eventually(what, clock, until)(cond))

  /** Completes once no host drives a transaction */
  private def settle(clock: Boolean): Future[Unit] =
    net.handleEvents()
    eventually("transactions completed", clock)(Future.successful(!net.hasTransactions))

  /** Records the client prepares of the transaction `build` makes and commits it */
  private def commitRecorded(build: Transaction => Future[Unit]): Future[TransactionId] =
    val tx = client.newTransaction()
    net.recordTxMessage = (m: TxMessage) => m.transactionId == tx.id && m.isInstanceOf[TxPrepare]
    for
      _ <- build(tx)
      _ <- pump(tx.commit(), clock = false)
      _ <- settle(clock = false)
    yield
      net.recordTxMessage = _ => false
      tx.id

  @volatile private var recorded: List[TxMessage] = Nil

  /** Waits until the host has forgotten `txid` (its status cache entry expired, so every store forgot it), then
    * delivers again every client prepare of it recorded
    */
  private def replayClientPrepares(txid: TransactionId): Future[Unit] =
    for
      _ <- eventually("the status cache forgot the transaction", clock = false):
             Future.successful(net.smgr.control.txStatusCache.getStatus(txid).isEmpty)
    yield
      recorded = recorded ++ net.takeRecordedTxMessages()
      val prepares = recorded.collect:
        case m: TxPrepare if m.fromClient && m.transactionId == txid => m
      assert(prepares.nonEmpty, s"no client prepare of $txid was recorded")
      prepares.foreach(net.deliverTransactionMessage)

  /** Completes once no store of the bootstrap pool holds a Tx for `txid` */
  private def forgottenByStores(txid: TransactionId): Future[Unit] =
    eventually("every store forgot the transaction", clock = true):
      Future.successful(net.stores.forall: b =>
        net.smgr.control.router.loaded.get(b.storeId).forall(!_.frontend.transactions.contains(txid)))

  /** What store `i` holds for key `k` of `ptr`: the revision of its value */
  private def storeKeyRevision(i: Int, ptr: KeyValueObjectPointer): Option[ObjectRevision] =
    net.stores(i).m.get(ptr.id).flatMap(_.kvState).flatMap(_.content.get(k)).map(_.revision)

  test("a replayed pinned transaction does not re-apply its blind delete over a re-inserted key"):
    for
      pool <- client.getStoragePool(Radicle.poolId)
      (x, y) <- client.transactUntilSuccessful: tx =>
                  given Transaction = tx
                  for
                    x <- pool.allocator.allocateDataObject(DataBuffer(Array[Byte](1)))
                    y <- pool.allocator.allocateKeyValueObject(Map(k -> Value(Array[Byte](1))))
                  yield (x, y)
      _ <- settle(clock = false)
      xBefore <- client.read(x)
      // T: the overwrite of x at its revision is the pin; the delete of k from y is blind
      t <- commitRecorded: tx =>
             tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
             tx.update(y, None, None, Nil, List(Delete(k)))
             Future.unit
      yAfterT <- client.read(y)
      _ = yAfterT.contents.keySet should not contain k
      // T2: k is inserted again
      t2 <- client.transactUntilSuccessful: tx =>
              tx.update(y, None, None, List(KeyValueUpdate.DoesNotExist(k)), List(Insert(k, Array[Byte](3))))
              Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t2)))
      _ <- replayClientPrepares(t)
      _ <- settle(clock = true)
      _ <- forgottenByStores(t)
      yAfterReplay <- client.read(y)
    yield
      // Today a store taking the replay proves T committed from x's revision (Tx.proveCommitted), and the stores apply
      // T's blind delete again over T2's k. Pending until a late commit applies nothing on stores that already hold it
      pendingUntilFixed:
        withClue("every store of y still holds k as T2 wrote it: "):
          (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t2)))
        yAfterReplay.contents.get(k).map(_.value.bytes.toSeq) shouldBe Some(Seq[Byte](3))
