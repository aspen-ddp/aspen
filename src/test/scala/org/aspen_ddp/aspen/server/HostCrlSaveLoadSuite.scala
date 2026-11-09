package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
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
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, TransactionRecoveryState}
import org.aspen_ddp.aspen.server.host.{ControlFixture, LockstepThreading}
import org.aspen_ddp.aspen.server.store.Store
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.transaction.TransactionDriver

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, SECONDS}

class HostCrlSaveLoadSuite extends IntegrationTestSuite with ControlFixture:

  private val poolId = PoolId(new UUID(7, 1))
  private val crlStoreId = StoreId(poolId, 0)
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
    val backend = new MapBackend(crlStoreId):
      override def path: os.Path = storeDir
    backend.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](0)))

    val txd = overwriteTxd(txid, 0)
    val trs = TransactionRecoveryState(crlStoreId, txd.serialize(), List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))),
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState.initial)
    CrashRecoveryLog.saveStoreState(crlStoreId, List(trs), backend.crlSaveFile)
    backend

  private val ida = Replication(3, 2)

  private def overwriteTxd(id: TransactionId, leader: Byte): TransactionDescription =
    TransactionDescription(id, HLCTimestamp(100), ptr, leader,
      List(DataUpdate(ptr, rev0, DataUpdateOperation.Overwrite)), Nil, None, Nil, Nil, ida, Map(poolId -> ida))

  /** The store's backend holding the object, with no crl_save.log */
  private def storeWithObject(): MapBackend =
    val backend = new MapBackend(crlStoreId)
    backend.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](0)))
    backend

  /** txid's commit vote on an overwrite of the object, as the device CRL holds it */
  private def commitVote(): TransactionRecoveryState =
    TransactionRecoveryState(crlStoreId, overwriteTxd(txid, 0).serialize(),
      List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))), TransactionDisposition.VoteCommit,
      TransactionStatus.Unresolved, PersistentState.initial)

  private def newCrlHost(crl: CrashRecoveryLog = new TestNetwork.RecordingCRL): Host =
    val hostRoot = os.temp.dir()
    tempRoots += hostRoot
    new Host(client, HostId.BootstrapHostId, systemId, hostRoot, executionContext,
      new LockstepThreading(executionContext), net.objectCacheFactory, net,
      BackgroundTaskManager.NoBackgroundTaskManager, TestNetwork.TestCRL,
      net.FinalizerFactory, TransactionDriver.noErrorRecoveryFactory, Duration(5, SECONDS), Duration(60, SECONDS),
      // The store is loaded directly, onto a device this host never loads
      seams = Host.Seams(unloadedDeviceCrl = Some(crl)))

  private def storeOf(host: Host): Option[Store] = host.control.router.loaded.get(crlStoreId)

  atest("a store loaded from its crl_save.log recreates its in-flight transactions and their locks"):
    given ExecutionContext = executionContext
    val backend = storeWithSaveFile()
    val host = newCrlHost()
    val loaded = host.loadStore(deviceId, backend, None)

    def recovered: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && storeOf(host).exists(_.frontend.transactions.contains(txid))

    for
      _ <- yieldUntil(recovered)
      _ <- loaded
    yield
      val store = storeOf(host).get
      store.frontend.transactions.keySet should be (Set(txid))
      store.objectCache.get(oid).flatMap(_.lockedToTransaction) should be (Some(txid))
      os.exists(backend.crlSaveFile) should be (false)

  atest("a store loaded with transactions in its device CRL recreates them and their locks"):
    given ExecutionContext = executionContext
    val crl = new TestNetwork.RecordingCRL
    crl.save(txid, commitVote(), () => ())
    // MapBackend reads complete at once: a read issued before the backend's completion handler is set is lost
    val backend = storeWithObject()
    val host = newCrlHost(crl)
    val loaded = host.loadStore(deviceId, backend, None)

    def locked: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && storeOf(host).exists(_.objectCache.get(oid).flatMap(_.lockedToTransaction).contains(txid))

    for
      _ <- yieldUntil(locked)
      _ <- loaded
    yield
      val store = storeOf(host).get
      store.frontend.transactions.keySet should be (Set(txid))
      store.objectCache.get(oid).flatMap(_.lockedToTransaction) should be (Some(txid))

  atest("a prepare that arrives while the crl_save.log loads is handled after the recovered transactions lock"):
    given ExecutionContext = executionContext
    val backend = storeWithSaveFile()
    val host = newCrlHost()
    val loaded = host.loadStore(deviceId, backend, None)
    // A conflicting transaction led by another store, whose responses go to a store the test network hosts
    val other = TransactionId(new UUID(7, 6))
    host.receiveTransactionMessage(TxPrepare(crlStoreId, net.storeId0, overwriteTxd(other, 1),
      ProposalId.initialProposal(1), List(ObjectUpdate(oid, DataBuffer(Array[Byte](2)))), Nil, true))

    def handled: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && storeOf(host).exists(_.frontend.transactions.contains(other)) &&
        storeOf(host).exists(_.objectCache.get(oid).nonEmpty)

    for
      _ <- yieldUntil(handled)
      _ <- loaded
    yield
      val store = storeOf(host).get
      store.frontend.transactions.keySet should be (Set(txid, other))
      store.objectCache.get(oid).flatMap(_.lockedToTransaction) should be (Some(txid))
