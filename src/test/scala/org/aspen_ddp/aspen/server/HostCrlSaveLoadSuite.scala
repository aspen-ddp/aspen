package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision,
  ObjectType}
import org.aspen_ddp.aspen.common.network.TxPrepare
import org.aspen_ddp.aspen.common.paxos.{PersistentState, ProposalId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription,
  TransactionDisposition, TransactionId, TransactionStatus}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrashRecoveryLogFactory, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.network.Messenger as ServerMessenger
import org.aspen_ddp.aspen.server.host.LockstepThreading
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.store.cache.ObjectCache
import org.aspen_ddp.aspen.server.transaction.{TransactionDriver, TransactionFinalizer}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}

/** A CRL that keeps what it is given in memory, so that recovery state written by loadStore can be recovered */
private class RecordingCRL extends CrashRecoveryLog:
  private var states: Map[StoreId, Map[TransactionId, TransactionRecoveryState]] = Map()

  def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] = synchronized:
    states.getOrElse(storeId, Map()).values.toList

  def closeStore(storeId: StoreId): Future[List[TransactionRecoveryState]] = synchronized:
    val l = getFullRecoveryState(storeId)
    states -= storeId
    Future.successful(l)

  def save(txid: TransactionId, state: TransactionRecoveryState, completionHandler: () => Unit): Unit =
    synchronized:
      states += state.storeId -> (states.getOrElse(state.storeId, Map()) + (txid -> state))
    completionHandler()

  def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = ()

  def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = synchronized:
    states += storeId -> (states.getOrElse(storeId, Map()) - txid)

private class CrlSaveHost(mgrClient: AspenClient,
                          systemId: UUID,
                          hostRootDir: os.Path,
                          execCtx: ExecutionContext,
                          cacheFactory: StoreId => ObjectCache,
                          messenger: ServerMessenger,
                          finalizers: TransactionFinalizer.Factory,
                          crl: CrashRecoveryLog)
  extends Host(mgrClient, HostId.BootstrapHostId, systemId, hostRootDir, execCtx, new LockstepThreading(execCtx), cacheFactory, messenger,
    BackgroundTaskManager.NoBackgroundTaskManager, new CrashRecoveryLogFactory { def createCRL() = crl }, finalizers,
    TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS), Duration(60, SECONDS)):

  def storeOf(storeId: StoreId): Option[Store] = stores.get(storeId)

class HostCrlSaveLoadSuite extends IntegrationTestSuite with HostTestHarness:

  private val poolId = PoolId(new UUID(7, 1))
  private val storeId = StoreId(poolId, 0)
  private val oid = ObjectId(new UUID(7, 2))
  private val ptr = DataObjectPointer(oid, poolId, Array[Byte]())
  private val rev0 = ObjectRevision(TransactionId(new UUID(7, 3)))
  private val txid = TransactionId(new UUID(7, 4))
  private val deviceId = StorageDeviceId(new UUID(7, 5))

  /** Writes a crl_save.log holding txid's commit vote on an overwrite of the object, and returns the store's backend */
  private def storeWithSaveFile(): MapBackend =
    val root = os.temp.dir()
    tempRoots += root
    val storeDir = root / "store"
    os.makeDir.all(storeDir)
    val backend = new MapBackend(storeId):
      override def path: os.Path = storeDir
    backend.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](0)))

    val txd = overwriteTxd(txid, 0)
    val trs = TransactionRecoveryState(storeId, txd.serialize(), List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))),
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState.initial)
    CrashRecoveryLog.saveStoreState(storeId, List(trs), backend.crlSaveFile)
    backend

  private val ida = Replication(3, 2)

  private def overwriteTxd(id: TransactionId, leader: Byte): TransactionDescription =
    TransactionDescription(id, HLCTimestamp(100), ptr, leader,
      List(DataUpdate(ptr, rev0, DataUpdateOperation.Overwrite)), Nil, None, Nil, Nil, ida, Map(poolId -> ida))

  private def newHost(): CrlSaveHost =
    val hostRoot = os.temp.dir()
    tempRoots += hostRoot
    new CrlSaveHost(client, systemId, hostRoot, executionContext, net.objectCacheFactory, net,
      net.FinalizerFactory, new RecordingCRL)

  atest("a store loaded from its crl_save.log recreates its in-flight transactions and their locks"):
    given ExecutionContext = executionContext
    val backend = storeWithSaveFile()
    val host = newHost()
    val loaded = host.loadStore(deviceId, backend, None)

    def recovered: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && host.storeOf(storeId).exists(_.frontend.transactions.contains(txid))

    for
      _ <- yieldUntil(recovered)
      _ <- loaded
    yield
      val store = host.storeOf(storeId).get
      store.frontend.transactions.keySet should be (Set(txid))
      store.objectCache.get(oid).flatMap(_.lockedToTransaction) should be (Some(txid))
      os.exists(backend.crlSaveFile) should be (false)

  atest("a prepare that arrives while the crl_save.log loads is handled after the recovered transactions lock"):
    given ExecutionContext = executionContext
    val backend = storeWithSaveFile()
    val host = newHost()
    val loaded = host.loadStore(deviceId, backend, None)
    // A conflicting transaction led by another store, whose responses go to a store the test network hosts
    val other = TransactionId(new UUID(7, 6))
    host.receiveTransactionMessage(TxPrepare(storeId, net.storeId0, overwriteTxd(other, 1),
      ProposalId.initialProposal(1), List(ObjectUpdate(oid, DataBuffer(Array[Byte](2)))), Nil, true))

    def handled: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && host.storeOf(storeId).exists(_.frontend.transactions.contains(other)) &&
        host.storeOf(storeId).exists(_.objectCache.get(oid).nonEmpty)

    for
      _ <- yieldUntil(handled)
      _ <- loaded
    yield
      val store = host.storeOf(storeId).get
      store.frontend.transactions.keySet should be (Set(txid, other))
      store.objectCache.get(oid).flatMap(_.lockedToTransaction) should be (Some(txid))
