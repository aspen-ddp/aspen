package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.network.{TxFinalized, TxMessage, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, ObjectRevision}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDisposition, TransactionId}
import org.aspen_ddp.aspen.server.store.Frontend

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, SECONDS}

/** A commit voter applies in its lock order (transaction-requirements.md 8.3). Replication(3, 2) on the TestNetwork's
  * lockstep hosts, so t = r = 2. T overwrites D with commit votes from stores 0 and 1; store 2 never hears of T. Store
  * 0's copies of T's resolution and finalization are held back. T2, a later refcount change of D that does not chain
  * on T's revision, commits with the votes of stores 1 (which applied T) and 2; store 0 collides on T's lock and votes
  * abort. T2's resolution reaches store 0 before T's. Applied first there, it would raise D's object timestamp past
  * T's start and fail T's re-check, leaving store 0 with neither T's lock nor its update: stores 0 and 2 would agree on the pre-T revision, a read majority
  * without T, and repair would overwrite store 1, the one store holding T. Store 0 instead holds T2 until T applies.
  */
class CommitApplyOrderIntegrationSuite extends IntegrationTestSuite:

  private def deadline(): Long = System.nanoTime() + Duration(30, SECONDS).toNanos

  private def pump[T](f: Future[T], until: Long = deadline()): Future[T] =
    if f.isCompleted then
      f
    else if System.nanoTime() > until then
      net.printTransactionStatus()
      Future.failed(new AssertionError("Timed out pumping events"))
    else
      Future { Thread.sleep(1); net.handleEvents() }.flatMap(_ => pump(f, until))

  private def eventually(what: String)(cond: => Boolean): Future[Unit] =
    val until = deadline()
    def loop(): Future[Unit] =
      if cond then
        Future.unit
      else if System.nanoTime() > until then
        net.printTransactionStatus()
        Future.failed(new AssertionError(s"Never happened: $what"))
      else
        Future { Thread.sleep(1); net.handleEvents() }.flatMap(_ => loop())
    loop()

  private def sid(i: Int): StoreId = net.stores(i).storeId

  private def frontend(i: Int): Option[Frontend] =
    net.hostFor(sid(i)).control.router.loaded.get(sid(i)).map(_.frontend)

  private def vote(i: Int, txid: TransactionId): Option[TransactionDisposition.Value] =
    frontend(i).flatMap(_.transactions.get(txid)).map(_.vote)

  private def allocate(): Future[DataObjectPointer] =
    for
      pool <- client.getStoragePool(Radicle.poolId)
      x <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             tx.replaySafe("test")
             pool.allocator.allocateDataObject(DataBuffer(Array[Byte](1)))
      _ <- eventually("transactions completed")(!net.hasTransactions)
    yield x

  test("a commit voter whose outcome arrives after a later refcount change of its object keeps its update"):
    var heldForStore0 = List[TxMessage]()
    for
      d <- allocate()
      dBefore <- client.read(d)
      // T: led by store 1, never seen by store 2. Store 0 votes and accepts, but its outcome messages are held
      tx = client.newTransaction()
      _ = net.offlineStores = Set(sid(0), sid(2))
      _ = net.droppedTx = Set((sid(2), tx.id))
      _ = net.dropTxMessage =
            case m @ (_: TxResolved | _: TxFinalized) if m.to == sid(0) && m.transactionId == tx.id =>
              heldForStore0 = heldForStore0 :+ m
              true
            case _ => false
      _ = tx.overwrite(d, dBefore.revision, DataBuffer(Array[Byte](2)))
      _ <- pump(tx.commit())
      _ <- eventually("store 1 applied T and store 0 still holds its commit vote"):
             net.stores(1).m.get(d.id).exists(_.metadata.revision == ObjectRevision(tx.id)) &&
               vote(0, tx.id).contains(TransactionDisposition.VoteCommit)
      // T2: a refcount change started after T, led by store 2. Store 0 collides with T's lock
      tx2 = client.newTransaction()
      _ = net.offlineStores = Set(sid(0), sid(1))
      _ = tx2.setRefcount(d, dBefore.refcount, dBefore.refcount.increment())
      _ <- pump(tx2.commit())
      _ = net.offlineStores = Set()
      _ <- eventually("store 0 learned T2"):
             frontend(0).forall(_.transactions.get(tx2.id).forall(_.knownOutcome.contains(true)))
      // T's outcome reaches store 0 at last
      _ = net.dropTxMessage = _ => false
      _ = heldForStore0.foreach(net.deliverTransactionMessage)
      _ = net.droppedTx = Set()
      _ <- eventually("transactions completed")(!net.hasTransactions)
      dAfter <- client.read(d)
    yield
      withClue("store 0 holds T and T2: "):
        net.stores(0).m.get(d.id).map(s => s.metadata.revision -> s.metadata.refcount.count) shouldBe
          Some(ObjectRevision(tx.id) -> (dBefore.refcount.count + 1))
      withClue("a read shows T: ")(dAfter.revision shouldBe ObjectRevision(tx.id))
