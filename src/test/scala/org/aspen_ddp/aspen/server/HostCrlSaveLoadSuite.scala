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
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, CrlClosed, CrlFailed, StoreRecoveryState,
  TransactionRecoveryState}
import org.aspen_ddp.aspen.server.host.{Activity, ClosePurpose, ControlFixture, LockstepThreading, StoreLoader}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.{Frontend, Store, StoreIncarnationFile}
import org.aspen_ddp.aspen.server.store.backend.MapBackend
import org.aspen_ddp.aspen.server.transaction.TransactionDriver

import org.scalatest.compatible

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
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
    // Every store directory has its incarnation file from its creation
    StoreIncarnationFile.writeFresh(storeDir)
    val backend = new MapBackend(crlStoreId):
      override def path: os.Path = storeDir
    backend.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](0)))

    val txd = overwriteTxd(txid, 0)
    val trs = TransactionRecoveryState(crlStoreId, txd.serialize(), List(ObjectUpdate(oid, DataBuffer(Array[Byte](1)))),
      TransactionDisposition.VoteCommit, TransactionStatus.Unresolved, PersistentState.initial)
    CrashRecoveryLog.saveStoreState(crlStoreId, StoreRecoveryState(List(trs)), backend.crlSaveFile)
    backend

  private val ida = Replication(3, 2)

  /** Started MaxStartLead ahead of the host's clock now: a prepare made after the store loads is at or above its
    * forget floor (Frontend.forgetFloorMillis: the load plus MaxStartLead), and not muted as too far ahead
    */
  private def overwriteTxd(id: TransactionId, leader: Byte): TransactionDescription =
    TransactionDescription(id, HLCTimestamp((System.currentTimeMillis() + Frontend.MaxStartLead.toMillis) << 16), ptr,
      leader,
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
      seams = compared(Host.Seams(unloadedDeviceCrl = Some(crl))))

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

  atest("a store loaded from its crl_save.log recovers its applied stubs"):
    given ExecutionContext = executionContext
    val stubbed = TransactionId(new UUID(7, 9))
    val backend = storeWithSaveFile()
    val (_, saved) = CrashRecoveryLog.loadStoreState(backend.crlSaveFile)
    CrashRecoveryLog.saveStoreState(crlStoreId, saved.copy(appliedStubs = Map(stubbed -> HLCTimestamp.now)), backend.crlSaveFile)
    val crl = new TestNetwork.RecordingCRL
    val host = newCrlHost(crl)
    val loaded = host.loadStore(deviceId, backend, None)

    def recovered: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && storeOf(host).exists(_.frontend.transactions.contains(txid))

    for
      _ <- yieldUntil(recovered)
      _ <- loaded
    yield
      val store = storeOf(host).get
      store.frontend.appliedStubs.keySet should be (Set(stubbed))
      store.frontend.transactions.keySet should be (Set(txid))
      crl.getAppliedStubs(crlStoreId).keySet should be (Set(stubbed))

  atest("a store loaded with applied stubs in its device CRL recovers them"):
    given ExecutionContext = executionContext
    val stubbed = TransactionId(new UUID(7, 9))
    val crl = new TestNetwork.RecordingCRL
    crl.save(txid, commitVote(), () => ())
    crl.saveAppliedStub(crlStoreId, stubbed, HLCTimestamp.now, () => ())
    val backend = storeWithObject()
    val host = newCrlHost(crl)
    val loaded = host.loadStore(deviceId, backend, None)

    def recovered: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && storeOf(host).exists(_.frontend.transactions.contains(txid))

    for
      _ <- yieldUntil(recovered)
      _ <- loaded
    yield
      storeOf(host).get.frontend.appliedStubs.keySet should be (Set(stubbed))

  /** A RecordingCRL whose readStoreState answers only when the test completes `answer`, as a busy CRL thread would,
   *  and which records any blocking getFullRecoveryState call */
  private class SlowReadCRL extends TestNetwork.RecordingCRL:
    @volatile var blockingReads = 0
    @volatile var asked = false
    val answer: Promise[Unit] = Promise()
    override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
      blockingReads += 1
      super.getFullRecoveryState(storeId)
    override def readStoreState(storeId: StoreId): Future[StoreRecoveryState] =
      asked = true
      val held = StoreRecoveryState(super.getFullRecoveryState(storeId), getAppliedStubs(storeId))
      answer.future.map(_ => held)(using ExecutionContext.parasitic)
    // Not through readStoreState, as the default is: only the store's recovery read is held
    override def dropStore(storeId: StoreId): Future[Unit] =
      deleteTransactions(storeId,
        StoreRecoveryState(super.getFullRecoveryState(storeId), getAppliedStubs(storeId)).entryIds)

  atest("device-CRL recovery waits for the CRL without blocking and holds messages until it recovers"):
    given ExecutionContext = executionContext
    val crl = new SlowReadCRL
    crl.save(txid, commitVote(), () => ())
    val backend = storeWithObject()
    val host = newCrlHost(crl)
    val closes = closesSeen(host)
    val loaded = host.loadStore(deviceId, backend, None)
    val other = TransactionId(new UUID(7, 6))

    def reading: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && crl.asked

    def handled: Boolean =
      host.testingOnlyHandleEvents()
      storeOf(host).exists(_.frontend.transactions.contains(other))

    for
      _ <- yieldUntil(reading)
      // Sent once the store is built, so the transaction starts after the store's load (Frontend.forgetFloorMillis)
      _ = host.receiveTransactionMessage(TxPrepare(crlStoreId, net.storeId0, overwriteTxd(other, 1),
            ProposalId.initialProposal(1), List(ObjectUpdate(oid, DataBuffer(Array[Byte](2)))), true))
      _ = host.testingOnlyHandleEvents()
      waiting = storeOf(host).map(_.frontend.transactions.keySet)
      _ = crl.answer.success(())
      _ <- yieldUntil(handled)
    yield
      withClue("the store is served and holds the prepare while its read is outstanding: "):
        waiting should be (Some(Set()))
      val store = storeOf(host).get
      store.frontend.transactions.keySet should be (Set(txid, other))
      store.objectCache.get(oid).flatMap(_.lockedToTransaction) should be (Some(txid))
      crl.blockingReads should be (0)

  /** A RecordingCRL whose reads of a store's recovery state throw once `failing`, as a corrupt entry would make it */
  private class ThrowingCRL extends TestNetwork.RecordingCRL:
    @volatile var failing = false
    override def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState] =
      if failing then throw new IllegalStateException("injected recovery failure")
      else super.getFullRecoveryState(storeId)
    // The default drop reads through getFullRecoveryState: the fault is injected into recovery only. A store loaded
    // from its crl_save.log drops its device-CRL entries first, and these tests hold none to drop
    override def dropStore(storeId: StoreId): Future[Unit] =
      if failing then Future.unit else super.dropStore(storeId)

  /** The store's backend holding the object, in a directory of its own, with no crl_save.log */
  private def storeInDir(): (MapBackend, os.Path) =
    val root = os.temp.dir()
    tempRoots += root
    val storeDir = root / "store"
    os.makeDir.all(storeDir)
    // Every store directory has its incarnation file from its creation
    StoreIncarnationFile.writeFresh(storeDir)
    val backend = new MapBackend(crlStoreId):
      override def path: os.Path = storeDir
    backend.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](0)))
    (backend, storeDir)

  /** The close purposes `host`'s copy records took, from now on */
  private def closesSeen(host: Host): () => List[ClosePurpose] =
    val seen = scala.collection.mutable.ListBuffer[ClosePurpose]()
    host.control.copyRegistry.observer = (copies, s) =>
      copies.copies.get(s).map(_.activity).foreach:
        case Activity.Closing(p) => seen += p
        case _ => ()
    () => seen.toList

  /** The store is not served, and was closed as a fail-stop: the close that moves an attached device's copy to lost/
   *  when it ends (this one's device is not loaded, so nothing on disk changes) */
  private def assertRetired(host: Host, loaded: Future[Unit], storeDir: os.Path): Future[compatible.Assertion] =
    given ExecutionContext = executionContext
    val closes = closesSeen(host)
    def retired: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && storeOf(host).isEmpty
    for
      _ <- yieldUntil(retired)
    yield
      withClue("a store that failed to recover part of its transactions is not served: "):
        storeOf(host) should be (None)
      closes() should be (List(ClosePurpose.FailStop))

  atest("a store whose device-CRL recovery throws is not served, and is closed as a fail-stop"):
    val crl = new ThrowingCRL
    crl.save(txid, commitVote(), () => ())
    crl.failing = true
    val (backend, storeDir) = storeInDir()
    val host = newCrlHost(crl)
    assertRetired(host, host.loadStore(deviceId, backend, None), storeDir)

  atest("a store whose recovery from its crl_save.log throws is not served, and is closed as a fail-stop"):
    val crl = new ThrowingCRL
    crl.failing = true
    val backend = storeWithSaveFile()
    val host = newCrlHost(crl)
    assertRetired(host, host.loadStore(deviceId, backend, None), backend.path)

  /** The device's CRL fails `crl`'s outstanding recovery read with `err`: the store is left installed, unmarked and
   *  holding its messages, for the device's unload (DeviceShutdown.crlFailed) or the shutdown to close (CRL-W12) */
  private def assertReadFailureLeavesStore(crl: SlowReadCRL, backend: MapBackend, storeDir: os.Path,
                                           err: Throwable): Future[compatible.Assertion] =
    given ExecutionContext = executionContext
    val host = newCrlHost(crl)
    val closes = closesSeen(host)
    val loaded = host.loadStore(deviceId, backend, None)
    val other = TransactionId(new UUID(7, 6))
    def reading: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && crl.asked
    for
      _ <- yieldUntil(reading)
      store = storeOf(host).get
      _ = crl.answer.failure(err)
      _ = host.testingOnlyHandleEvents()
      _ = host.receiveTransactionMessage(TxPrepare(crlStoreId, net.storeId0, overwriteTxd(other, 1),
            ProposalId.initialProposal(1), List(ObjectUpdate(oid, DataBuffer(Array[Byte](2)))), true))
      _ = host.testingOnlyHandleEvents()
    yield
      withClue("the store is still the one installed: "):
        storeOf(host).exists(_ eq store) should be (true)
      withClue("a CRL failure is the device's, not the store's: "):
        closes() should be (Nil)
      withClue("the store still holds its messages: "):
        store.frontend.transactions.keySet should be (Set())

  for (what, err) <- List("dies" -> (() => CrlFailed(new java.io.IOException("injected"))),
                          "shuts down" -> (() => CrlClosed())) do
    atest(s"a store whose device-CRL recovery read fails as its CRL $what is not closed as a fail-stop (CRL-W12)"):
      val crl = new SlowReadCRL
      crl.save(txid, commitVote(), () => ())
      val (backend, storeDir) = storeInDir()
      assertReadFailureLeavesStore(crl, backend, storeDir, err())

    atest(s"a store loaded from its crl_save.log whose read fails as its CRL $what is not closed as a fail-stop (CRL-W12)"):
      val backend = storeWithSaveFile()
      assertReadFailureLeavesStore(new SlowReadCRL, backend, backend.path, err())

  atest("a store whose device-CRL recovery read fails otherwise is not served, and is closed as a fail-stop"):
    val crl = new SlowReadCRL
    crl.save(txid, commitVote(), () => ())
    val (backend, storeDir) = storeInDir()
    val host = newCrlHost(crl)
    val loaded = host.loadStore(deviceId, backend, None)
    crl.answer.failure(new IllegalStateException("injected read failure"))
    assertRetired(host, loaded, storeDir)

  atest("a prepare that arrives while the crl_save.log loads is handled after the recovered transactions lock"):
    given ExecutionContext = executionContext
    val backend = storeWithSaveFile()
    // Holds the crl_save.log's load into the CRL until the test releases it
    val release = Promise[Unit]()
    @volatile var loading = false
    val crl = new TestNetwork.RecordingCRL:
      override def loadStore(storeId: StoreId, state: StoreRecoveryState): Future[Unit] =
        loading = true
        val loadedIntoCrl = super.loadStore(storeId, state)
        release.future.flatMap(_ => loadedIntoCrl)(using ExecutionContext.parasitic)
    val host = newCrlHost(crl)
    val loaded = host.loadStore(deviceId, backend, None)
    // A conflicting transaction led by another store, whose responses go to a store the test network hosts
    val other = TransactionId(new UUID(7, 6))

    def built: Boolean =
      host.testingOnlyHandleEvents()
      loading

    def handled: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && storeOf(host).exists(_.frontend.transactions.contains(other)) &&
        storeOf(host).exists(_.objectCache.get(oid).nonEmpty)

    for
      _ <- yieldUntil(built)
      // Sent once the store is built, so the transaction starts after the store's load (Frontend.forgetFloorMillis),
      // and while the crl_save.log loads
      _ = host.receiveTransactionMessage(TxPrepare(crlStoreId, net.storeId0, overwriteTxd(other, 1),
            ProposalId.initialProposal(1), List(ObjectUpdate(oid, DataBuffer(Array[Byte](2)))), true))
      _ = host.testingOnlyHandleEvents()
      _ = withClue("the prepare is held while the crl_save.log loads: "):
            storeOf(host).exists(_.frontend.transactions.contains(other)) should be (false)
      _ = release.success(())
      _ <- yieldUntil(handled)
      _ <- loaded
    yield
      val store = storeOf(host).get
      store.frontend.transactions.keySet should be (Set(txid, other))
      store.objectCache.get(oid).flatMap(_.lockedToTransaction) should be (Some(txid))

  /** The store's backend holding the object, in its own directory, with no crl_save.log */
  private def storeInDirectory(): MapBackend =
    val root = os.temp.dir()
    tempRoots += root
    val storeDir = root / "store"
    os.makeDir.all(storeDir)
    // Every store directory has its incarnation file from its creation
    StoreIncarnationFile.writeFresh(storeDir)
    val backend = new MapBackend(crlStoreId):
      override def path: os.Path = storeDir
    backend.bootstrapAllocate(oid, ObjectType.Data, Metadata(rev0, ObjectRefcount(1, 1), HLCTimestamp(1)),
      DataBuffer(Array[Byte](0)))
    backend

  private def loadFloorLead(backend: MapBackend): Future[Long] =
    given ExecutionContext = executionContext
    val host = newCrlHost()
    val loaded = host.loadStore(deviceId, backend, None)
    def served: Boolean =
      host.testingOnlyHandleEvents()
      loaded.isCompleted && storeOf(host).isDefined
    for
      _ <- yieldUntil(served)
      _ <- loaded
    yield
      val frontend = storeOf(host).get.frontend
      frontend.loadFloorMillis - frontend.loadedAtMillis

  // The previous host's clock may lead this one's: a store that arrived from it (StoreLoader.ArrivedAtFile, written
  // at a transfer in or an adoption) refuses, for AcceptorSinceMargin after its load, what it may have forgotten
  atest("a store that arrived from another host loads with AcceptorSinceMargin on its load floor"):
    given ExecutionContext = executionContext
    val backend = storeInDirectory()
    StoreLoader.writeArrivedAt(backend.path)
    loadFloorLead(backend).map(_ should be (RebuildingStore.AcceptorSinceMargin.toMillis))

  atest("a store restarted on its own host loads with MaxStartLead on its load floor"):
    given ExecutionContext = executionContext
    loadFloorLead(storeInDirectory()).map(_ should be (Frontend.MaxStartLead.toMillis))
