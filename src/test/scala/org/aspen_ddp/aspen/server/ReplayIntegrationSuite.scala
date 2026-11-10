package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.{TestTransactions, Transaction}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}
import org.aspen_ddp.aspen.common.network.{TxFinalized, TxMessage, TxPrepare, TxResolved}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Delete, Insert, Key, KeyValueObjectPointer, ObjectId,
  ObjectRevision, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{KeyValueUpdate, TransactionDisposition, TransactionId,
  TransactionStatus}
import org.aspen_ddp.aspen.server.repair.{RepairPolicy, StoreRepairer}

import java.nio.{ByteBuffer, ByteOrder}

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

/** A transaction's client prepares, re-delivered after every store finalized and forgot it, must not apply it again.
  *
  * The bootstrap host records its stores' CRL state and has recovery drivers, as UnackedCommitsIntegrationSuite's
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
      // The designated leader's last: its prepare starts its driver, which here never retransmits
      // (TestNetwork.HostOptions.recoveryDrivers). A store that gets the driver's data-less prepare before its own
      // client prepare withholds its vote for it and answers only a later prepare (Tx.awaitingClientPrepare), so a
      // driver that never asks again would wait forever. A host's drivers retransmit
      val (leader, others) = prepares.partition(m => m.to.poolIndex == m.txd.designatedLeaderUID)
      (others ++ leader).foreach(net.deliverTransactionMessage)

  /** Completes once no store of the bootstrap pool holds a Tx for `txid` */
  private def forgottenByStores(txid: TransactionId): Future[Unit] =
    eventually("every store forgot the transaction", clock = true):
      Future.successful(net.stores.forall: b =>
        net.smgr.control.router.loaded.get(b.storeId).forall(!_.frontend.transactions.contains(txid)))

  /** What store `i` holds for key `k` of `ptr`: the revision of its value */
  private def storeKeyRevision(i: Int, ptr: KeyValueObjectPointer): Option[ObjectRevision] =
    net.stores(i).m.get(ptr.id).flatMap(_.kvState).flatMap(_.content.get(k)).map(_.revision)

  private def sid(i: Int): StoreId = net.stores(i).storeId

  private def storeRevision(i: Int, ptr: DataObjectPointer): Option[ObjectRevision] =
    net.stores(i).m.get(ptr.id).map(_.metadata.revision)

  private def errorKey(i: Int, objectId: ObjectId): Key =
    val bb = ByteBuffer.allocate(17)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(i.toByte)
    bb.putLong(objectId.uuid.getMostSignificantBits)
    bb.putLong(objectId.uuid.getLeastSignificantBits)
    Key(bb.array())

  /** The stores of `stores` with an ErrorTree entry for `objectId` */
  private def errorEntries(stores: List[Int], objectId: ObjectId): Future[List[Int]] =
    for
      pool <- client.getStoragePool(PoolId.BootstrapPoolId)
      found <- Future.sequence(stores.map(i => pool.errorTree.get(errorKey(i, objectId)).map(o => i -> o.isDefined)))
    yield found.collect { case (i, true) => i }

  /** A data object x and a KeyValue object y, with `yContent`, every store holding both */
  private def allocateXY(yContent: Map[Key, Value]): Future[(DataObjectPointer, KeyValueObjectPointer)] =
    for
      pool <- client.getStoragePool(Radicle.poolId)
      xy <- client.transactUntilSuccessful: tx =>
              given Transaction = tx
              tx.replaySafe("test")
              for
                x <- pool.allocator.allocateDataObject(DataBuffer(Array[Byte](1)))
                y <- pool.allocator.allocateKeyValueObject(yContent)
              yield (x, y)
      _ <- settle(clock = false)
    yield xy

  test("a replayed pinned transaction does not re-apply its blind delete over a re-inserted key"):
    for
      pool <- client.getStoragePool(Radicle.poolId)
      (x, y) <- client.transactUntilSuccessful: tx =>
                  given Transaction = tx
                  tx.replaySafe("test")
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
              tx.replaySafe("test")
              tx.update(y, None, None, List(KeyValueUpdate.DoesNotExist(k)), List(Insert(k, Array[Byte](3))))
              Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t2)))
      _ <- replayClientPrepares(t)
      _ <- settle(clock = true)
      _ <- forgottenByStores(t)
      yAfterReplay <- client.read(y)
    yield
      // A store taking the replay proves T committed from x's revision (Tx.proveCommitted). The replay's driver then
      // resolves it as a late commit, and no store applies T's blind delete again over T2's k: none is owed T
      withClue("every store of y still holds k as T2 wrote it: "):
        (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t2)))
      yAfterReplay.contents.get(k).map(_.value.bytes.toSeq) shouldBe Some(Seq[Byte](3))

  test("a replayed pinned KV insert after a later delete of its key changes nothing"):
    for
      (x, y) <- allocateXY(Map())
      xBefore <- client.read(x)
      // T: the overwrite of x at its revision is the pin; the insert of k into y is blind
      t <- commitRecorded: tx =>
             tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
             tx.update(y, None, None, Nil, List(Insert(k, Array[Byte](2))))
             Future.unit
      _ = (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t)))
      // T2: k is deleted, at T's revision of it
      t2 <- client.transactUntilSuccessful: tx =>
              tx.update(y, None, None, List(KeyValueUpdate.KeyRevision(k, ObjectRevision(t))), List(Delete(k)))
              Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe None)
      _ <- replayClientPrepares(t)
      _ <- settle(clock = true)
      _ <- forgottenByStores(t)
      yAfterReplay <- client.read(y)
      xAfterReplay <- client.read(x)
    yield
      // Checked afresh, T fails x's revision; every store proves it committed from x and applies nothing
      withClue("no store of y holds k again: "):
        (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe None)
      (0 until 5).foreach(i => storeRevision(i, x) shouldBe Some(ObjectRevision(t)))
      yAfterReplay.contents.keySet should not contain k
      xAfterReplay.revision shouldBe ObjectRevision(t)

  test("a replayed pinned data overwrite after a later overwrite changes nothing"):
    for
      (x, _) <- allocateXY(Map())
      xBefore <- client.read(x)
      t <- commitRecorded: tx =>
             tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](2)))
             Future.unit
      t2 <- client.transactUntilSuccessful: tx =>
              tx.overwrite(x, ObjectRevision(t), DataBuffer(Array[Byte](3)))
              Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => storeRevision(i, x) shouldBe Some(ObjectRevision(t2)))
      _ <- replayClientPrepares(t)
      _ <- settle(clock = true)
      _ <- forgottenByStores(t)
      xAfterReplay <- client.read(x)
    yield
      // Checked afresh, T fails x's revision, and no object shows its update: every store votes abort
      withClue("every store of x still holds T2's overwrite: "):
        (0 until 5).foreach(i => storeRevision(i, x) shouldBe Some(ObjectRevision(t2)))
      xAfterReplay.revision shouldBe ObjectRevision(t2)
      xAfterReplay.data.getByteArray.toSeq shouldBe Seq[Byte](3)

  test("a store that missed T's prepare and applied a later write to the key skips T's stale checked write"):
    val lagging = 4
    for
      (x, y) <- allocateXY(Map(k -> Value(Array[Byte](1))))
      yBefore <- client.read(y)
      rev0 = yBefore.contents(k).revision
      xBefore <- client.read(x)
      // T: k's overwrite is per-write checked (KeyRevision), which pins it. The lagging store gets none of T's
      // messages (they are recorded) and is reported offline, so it does not lead
      t <- {
        val tx = client.newTransaction()
        net.offlineStores = Set(sid(lagging))
        net.droppedTx = Set((sid(lagging), tx.id))
        net.recordTxMessage = (m: TxMessage) => m.transactionId == tx.id && m.to == sid(lagging)
        tx.update(y, None, None, List(KeyValueUpdate.KeyRevision(k, rev0)), List(Insert(k, Array[Byte](2))))
        for
          _ <- pump(tx.commit(), clock = false)
          _ <- settle(clock = false)
        yield
          net.recordTxMessage = _ => false
          net.offlineStores = Set()
          tx.id
      }
      marked <- errorEntries((0 until 5).toList, y.id)
      _ = withClue("only the lagging store lacks T, and the finalizer marked it: "):
            (0 until 5).foreach: i =>
              if i == lagging then storeKeyRevision(i, y) shouldBe Some(rev0)
              else storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t))
            // The designated leader's store can be marked too: when the client's prepare reaches it after the
            // resolution, it applies the commit only at TxFinalized, too late to send TxCommitted in time
            marked should contain (lagging)
      // T2: a blind write of k, pinned by x, which every store applies, the lagging one included
      t2 <- client.transactUntilSuccessful: tx =>
              tx.overwrite(x, xBefore.revision, DataBuffer(Array[Byte](3)))
              tx.update(y, None, None, Nil, List(Insert(k, Array[Byte](3))))
              Future.successful(tx.id)
      _ <- settle(clock = false)
      _ = (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t2)))
      // Once the host forgot T, its client prepare and then its outcome reach the lagging store late
      _ <- eventually("the status cache forgot T", clock = false):
             Future.successful(net.smgr.control.txStatusCache.getStatus(t).isEmpty)
      late = net.takeRecordedTxMessages()
      prepare = late.collect { case m: TxPrepare if m.fromClient => m }
      outcome = late.collect { case m: TxResolved if m.committed => m }.take(1) ++
                late.collect { case m: TxFinalized if m.committed => m }.take(1)
      _ = withClue("T's client prepare and committed outcome to the lagging store were recorded: "):
            prepare should not be empty
            outcome.size shouldBe 2
      _ = net.droppedTx = Set()
      _ = prepare.foreach(net.deliverTransactionMessage)
      _ = withClue("the lagging store checks T's late prepare afresh: "):
            net.smgr.control.router.loaded.get(sid(lagging)).exists(_.frontend.transactions.contains(t)) shouldBe true
      _ = outcome.foreach(net.deliverTransactionMessage)
      _ <- settle(clock = true)
      _ <- forgottenByStores(t)
      _ = withClue("the lagging store applied the commit's checked write only if k was still as T read it: "):
            storeKeyRevision(lagging, y) shouldBe Some(ObjectRevision(t2))
      stillMarked <- errorEntries((0 until 5).toList, y.id)
      _ = stillMarked should contain (lagging)
      repairer = StoreRepairer(client, net.smgr)
      // Every store's entries: the designated leader's may be marked too
      _ <- eventually("every entry about y was repaired away", clock = true):
             for
               _ <- (0 until 5).foldLeft(Future.unit): (f, i) =>
                      f.flatMap(_ => pump(repairer.scan(sid(i), RepairPolicy.Default), clock = true)).map(_ => ())
               _ <- settle(clock = true)
               errors <- errorEntries((0 until 5).toList, y.id)
             yield errors.isEmpty
      yAfter <- client.read(y)
    yield
      (0 until 5).foreach(i => storeKeyRevision(i, y) shouldBe Some(ObjectRevision(t2)))
      yAfter.contents.get(k).map(_.value.bytes.toSeq) shouldBe Some(Seq[Byte](3))

  test("an aborted data-less transaction re-driven by a recovered commit vote aborts again"):
    val voter = 1
    val blocked = Set(3, 4).map(sid)
    // Whether `m` is an outcome of `txid` (TxResolved or TxFinalized) sent to one of `to`
    def outcomeTo(txid: TransactionId, to: Set[StoreId])(m: TxMessage): Boolean =
      m.transactionId == txid && to.contains(m.to) && (m.isInstanceOf[TxResolved] || m.isInstanceOf[TxFinalized])
    for
      (x, _) <- allocateXY(Map())
      xBefore <- client.read(x)
      // T0 exclusively locks x and commits, but stores 3 and 4 never hear its outcome yet, so they keep its lock. They
      // are reported offline meanwhile, so neither leads it
      t0Outcomes <- {
        val tx = TestTransactions.replaySafe(client.newTransaction())
        net.offlineStores = blocked
        net.dropTxMessage = outcomeTo(tx.id, blocked)
        net.recordTxMessage = outcomeTo(tx.id, blocked)
        tx.lockRevision(x, xBefore.revision)
        for
          _ <- pump(tx.commit(), clock = false)
          _ <- settle(clock = false)
        yield
          net.recordTxMessage = _ => false
          net.takeRecordedTxMessages()
      }
      // T: a data-less refcount change of x. Stores 3 and 4 vote abort on the collision with T0's lock, so T aborts.
      // Store 1 votes commit and never hears the outcome, as a store that crashed right after voting does. The client's
      // prepares to stores 3 and 4 are held until store 1 has voted: on the host's shared status cache, a prepare
      // reaching store 1 after the abort would be answered from it, with no vote
      tx = TestTransactions.replaySafe(client.newTransaction())
      _ = net.offlineStores = blocked + sid(voter)
      _ = net.dropTxMessage = outcomeTo(tx.id, Set(sid(voter)))
      _ = net.droppedTx = blocked.map(_ -> tx.id)
      _ = net.recordTxMessage = (m: TxMessage) => m.transactionId == tx.id && blocked.contains(m.to) &&
            (m match { case p: TxPrepare => p.fromClient; case _ => false })
      _ = tx.setRefcount(x, xBefore.refcount, xBefore.refcount.increment())
      commit = tx.commit()
      voterVote = () => net.crl.getFullRecoveryState(sid(voter)).find(_.txd.transactionId == tx.id)
                    .map(s => (s.disposition, s.status))
      _ <- eventually("store 1 voted on T", clock = false)(Future.successful(voterVote().nonEmpty))
      _ = net.droppedTx = Set()
      _ = net.recordTxMessage = _ => false
      _ = net.takeRecordedTxMessages().foreach(net.deliverTransactionMessage)
      aborted <- pump(commit, clock = false).transform(r => scala.util.Success(r.isFailure))
      _ <- settle(clock = false)
      _ = withClue("T aborted, and store 1 holds its unresolved commit vote: "):
            aborted shouldBe true
            voterVote() shouldBe Some((TransactionDisposition.VoteCommit, TransactionStatus.Unresolved))
      // The collision clears: T0's outcome reaches stores 3 and 4
      _ = net.offlineStores = Set()
      _ = withClue("T0's outcomes to stores 3 and 4 were recorded: ")(t0Outcomes should not be empty)
      _ = t0Outcomes.foreach(net.deliverTransactionMessage)
      _ <- eventually("the status cache forgot T", clock = false):
             Future.successful(net.smgr.control.txStatusCache.getStatus(tx.id).isEmpty)
      _ = (0 until 5).filter(_ != voter).foreach: i =>
            net.smgr.control.router.loaded.get(sid(i)).exists(_.frontend.transactions.contains(tx.id)) shouldBe false
      // Store 1 restarts crash-only and recovers its commit vote. Its recovery driver re-prepares T to stores that
      // forgot it, and whose requirement on x now holds again
      _ = net.dropTxMessage = _ => false
      _ <- pump(net.restartStore(net.stores(voter)), clock = true)
      _ <- forgottenByStores(tx.id)
      _ <- settle(clock = true)
      xAfter <- client.read(x)
    yield
      withClue("no store applied T's refcount change: "):
        (0 until 5).foreach(i => net.stores(i).m(x.id).metadata.refcount shouldBe xBefore.refcount)
      xAfter.refcount shouldBe xBefore.refcount
