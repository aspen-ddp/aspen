package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.{Transaction, TransactionAborted}
import org.aspen_ddp.aspen.common.{DataBuffer, Radicle}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.StorageDeviceSetId
import org.aspen_ddp.aspen.common.network.{TxMessage, TxPrepare}
import org.aspen_ddp.aspen.common.objects.DataObjectPointer
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{TransactionDescription, TransactionHorizon, TransactionId}
import org.aspen_ddp.aspen.server.store.backend.{MapBackend, RocksDBConfig}

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.util.{Failure, Success, Try}

/** Learn-only drivers (paxos-transactions 7.6) end to end, on the TestNetwork's lockstep hosts.
  *
  * A store outside the transaction's primary pool drives without proposing: its proposer id, a pool index, could
  * equal a primary store's. When it alone heard the client and the designated leader is unreachable, its prepares
  * reach the other primary stores, which must still take the transaction over once it is idle there, rather than
  * leave it to expiry at the horizon
  */
class LearnOnlyDriverIntegrationSuite extends IntegrationTestSuite:

  // An idle Tx self-drives after 5 heartbeats (0.5 s), and a Tx reached only by drivers votes abort after 200 ms
  override def testHostOptions: TestNetwork.HostOptions =
    TestNetwork.HostOptions(heartbeatPeriod = Duration(100, MILLISECONDS), recoveryDrivers = true,
      missingDataWait = Duration(200, MILLISECONDS))

  private def deadline(): Long = System.nanoTime() + Duration(15, SECONDS).toNanos

  private def pump[T](f: Future[T], clock: Boolean, until: Long = deadline()): Future[T] =
    if f.isCompleted then
      f
    else if System.nanoTime() > until then
      net.printTransactionStatus()
      Future.failed(new AssertionError("Timed out pumping events"))
    else
      Future { Thread.sleep(1); if clock then net.heartbeat() else net.handleEvents() }.flatMap(_ => pump(f, clock, until))

  private def eventually(what: String, clock: Boolean)(cond: => Boolean): Future[Unit] =
    val until = deadline()
    def loop(): Future[Unit] =
      if cond then
        Future.unit
      else if System.nanoTime() > until then
        net.printTransactionStatus()
        Future.failed(new AssertionError(s"Never happened: $what"))
      else
        Future { Thread.sleep(1); if clock then net.heartbeat() else net.handleEvents() }.flatMap(_ => loop())
    loop()

  private def holds(store: StoreId, txid: TransactionId): Boolean =
    net.hostFor(store).control.router.loaded.get(store).exists(_.frontend.transactions.contains(txid))

  /** A data object in `poolId`, its pointer stored nowhere */
  private def allocateIn(poolId: PoolId): Future[DataObjectPointer] =
    for
      pool <- client.getStoragePool(poolId)
      x <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             tx.replaySafe("test")
             pool.allocator.allocateDataObject(DataBuffer(Array[Byte](1)))
      _ <- eventually("transactions completed", clock = false)(!net.hasTransactions)
    yield x

  test("a secondary-pool store that alone heard the client gets the outcome from a primary store's takeover"):
    @volatile var txd: Option[TransactionDescription] = None
    def leaderOf(t: TransactionDescription) = StoreId(t.primaryObject.poolId, t.designatedLeaderUID)
    for
      poolId <- client.createNewStoragePool("learn-only", Replication(1, 1), None, RocksDBConfig(),
                  StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      secondary = StoreId(poolId, 0)
      // The harness's host never picks a new pool's stores up itself (FsifIndexAllocationStrategySuite.createServedPool)
      load = net.smgr.loadStore(net.storageDeviceId, new MapBackend(secondary), None)
      _ = net.handleEvents()
      _ <- load
      _ <- eventually("transactions completed", clock = false)(!net.hasTransactions)
      x <- allocateIn(Radicle.poolId)
      y <- allocateIn(poolId)
      xs <- client.read(x)
      ys <- client.read(y)
      tx = client.newTransaction()
      // The designated leader is unreachable, and the client's prepares reach only the secondary store
      _ = net.dropTxMessage = (m: TxMessage) => m.transactionId == tx.id && (m match
            case p: TxPrepare =>
              if txd.isEmpty then txd = Some(p.txd)
              p.to == leaderOf(p.txd) || (p.fromClient && p.to.poolId == p.txd.primaryObject.poolId)
            case _ => txd.exists(t => m.to == leaderOf(t)))
      _ = tx.overwrite(x, xs.revision, DataBuffer(Array[Byte](2)))
      _ = tx.overwrite(y, ys.revision, DataBuffer(Array[Byte](2)))
      started = System.currentTimeMillis()
      result <- pump(tx.commit(), clock = true).transform(r => Success(r))
      elapsed = System.currentTimeMillis() - started
      _ <- eventually("the secondary store forgot T", clock = true)(!holds(secondary, tx.id))
    yield
      net.dropTxMessage = _ => false
      withClue("the primary pool is the bootstrap pool: ")(txd.map(_.primaryObject.poolId) shouldBe Some(Radicle.poolId))
      withClue("decided by a primary store's driver, not ended by expiry: "):
        result should matchPattern { case Failure(_: TransactionAborted) => }
        elapsed should be < TransactionHorizon.Horizon.toMillis
