package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.server.TestDeviceIo
import org.aspen_ddp.aspen.server.host.{CopyFixtures, CopyLayout, DeviceIo, Phase}
import org.aspen_ddp.aspen.IntegrationTestSuite
import org.scalatest.compatible
import org.aspen_ddp.aspen.client.{AspenClient, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.client.internal.transaction.MissedUpdateMarker
import org.aspen_ddp.aspen.common.network.{CurrentAllocationsReply, TransactionVote, TxMessage, TxPrepareResponse}
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, TransactionDescription, TransactionDisposition, TransactionId}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Key, Metadata, ObjectId, ObjectPointer, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.{DurableFiles, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}
import org.aspen_ddp.aspen.server.store.StoreIncarnationFile
import org.aspen_ddp.aspen.server.store.backend.{Backend, BackendOpener, BackendStateLost, CommitState, CompletionHandler, DefaultBackendOpener, StoreCreation}
import org.aspen_ddp.aspen.server.store.backend.rocksdb.BufferedConsistentRocksDB

import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.Try

/** Exercises the walk against a real RocksDB backend on a temp directory, driven by the
 *  bootstrap pool's real allocation tree. */
class RebuildingStoreSuite extends IntegrationTestSuite:

  private val storeId = StoreId(PoolId.BootstrapPoolId, 0.toByte)

  private lazy val backends: BackendOpener = new DefaultBackendOpener(executionContext)

  /** Creates and opens through the default opener, recording each create, open and onFatal, or fails opens with
   *  BackendStateLost while `lose` is set */
  private final class RecordingOpener extends BackendOpener:
    @volatile var creates: List[(StoreConfig, os.Path)] = Nil
    @volatile var opens: List[(StoreConfig, os.Path)] = Nil
    @volatile var onFatals: List[Throwable => Unit] = Nil
    @volatile var lose: Boolean = false

    def create(storeCfg: StoreConfig, storePath: os.Path, creatingRoot: os.Path, io: DeviceIo): Future[Unit] =
      creates = (storeCfg, storePath) :: creates
      backends.create(storeCfg, storePath, creatingRoot, io)

    def open(storeCfg: StoreConfig, storePath: os.Path, onFatal: Throwable => Unit, io: DeviceIo): Future[Backend] =
      opens = (storeCfg, storePath) :: opens
      onFatals = onFatal :: onFatals
      if lose then
        Future.failed(new BackendStateLost("injected lost staging store"))
      else
        backends.open(storeCfg, storePath, onFatal, io)

  /** A bare device directory: no StorageDeviceConfig, since nothing here loads it as a device. */
  private def deviceDir(): os.Path =
    os.temp.dir(prefix = "rebuilding-store-suite")

  /** Allocate `n` data objects into the bootstrap pool so the allocation tree has content, and
   *  split it across more than one tier-0 node. */
  private def populate(n: Int): Future[List[ObjectId]] =
    given ExecutionContext = executionContext
    val allocated = (0 until n).toList.map: i =>
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        tx.replaySafe("test")
        client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
          val alloc = pool.allocator
          alloc.allocateDataObject(Array[Byte](i.toByte)).map(_.id)
    Future.sequence(allocated)

  /** The rebuild's outcome, or None if it has still not finished after `maxYields` turns of the
   *  test's execution context.
   *
   *  Never awaits an incomplete future. The failure mode this exists for is a rebuild whose
   *  future never completes at all, and awaiting one of those hangs the suite instead of
   *  failing it.
   */
  private def outcomeOrStalled(rebuild: RebuildingStore,
                               maxYields: Int = 5000): Future[Option[Try[Unit]]] =
    given ExecutionContext = executionContext
    def loop(n: Int): Future[Option[Try[Unit]]] = rebuild.complete.value match
      case Some(outcome) => Future.successful(Some(outcome.map(_ => ())))
      case None if n == 0 => Future.successful(None)
      case None => Future(()).flatMap(_ => loop(n - 1))
    loop(maxYields)

  atest("a full walk restores every object and moves the store into place"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 5)
      _ <- rebuild.complete

      // Reopen the rebuilt store and verify the objects are actually retrievable.
      db = new BufferedConsistentRocksDB(CopyFixtures.copyPath(dev, storeId).get)
      // Hand-rolled key encoding: RocksDBBackend.tokey is private and the backend's read path
      // (RocksDBBackend.read) requires a CompletionHandler, which is heavyweight for this check.
      // This couples to the encoding but is the simplest accessible read path. If
      // RocksDBBackend.tokey's encoding changes, this test will read the wrong keys and silently
      // stop proving reconstruction correctness.
      tokey = (id: ObjectId) =>
        val bb = ByteBuffer.allocate(16)
        bb.putLong(0, id.uuid.getMostSignificantBits)
        bb.putLong(8, id.uuid.getLeastSignificantBits)
        bb.array()
      readResults <- Future.sequence(ids.map(id => db.get(tokey(id))))
      _ <- db.close()
    yield
      CopyFixtures.copyPath(dev, storeId) should be(defined)
      val finalPath = CopyFixtures.copyPath(dev, storeId).get
      // The staging directory and its checkpoint are gone once the store is whole.
      os.exists(dev / RebuildingStore.RebuildDirectory / storeId.directoryName) should be(false)
      os.exists(finalPath / RebuildState.stateFilename) should be(false)
      // Moved into rebuilt/, which keeps it offline until its flip commits (rebuild-process 2.2)
      CopyFixtures.phaseOf(dev, storeId).exists(_.isInstanceOf[Phase.Rebuilt]) should be(true)
      // Its whole transaction state, empty, so its first load drops what the device CRL holds for it (rebuild-process 5.6)
      CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(finalPath), storeId) should be(Right(StoreRecoveryState.Empty))
      // A StoreConfig is written up front, so the moved directory is loadable.
      StoreConfig.loadStoreConfig(finalPath / StoreConfig.configFilename).storeId should
        be(storeId)

      // Every allocated object was reconstructed and is readable from the rebuilt store.
      readResults.size should be(20)
      readResults.forall(_.isDefined) should be(true)

  /** Removes `ptr`'s allocation-tree entry, standing in for an allocation whose AllocationFinalizationAction has not
   *  landed yet */
  private def dropEntry(ptr: ObjectPointer): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      client.getStoragePool(ptr.poolId).flatMap(_.allocationTree.delete(Key(ptr.id.toBytes)))

  /** Writes `ptr`'s allocation-tree entry, as its AllocationFinalizationAction would */
  private def landEntry(ptr: ObjectPointer): Future[Unit] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      tx.replaySafe("test")
      client.getStoragePool(ptr.poolId).flatMap(_.allocationTree.set(Key(ptr.id.toBytes), Value(ptr.toArray)))

  private def allocateOne(): Future[ObjectPointer] =
    given ExecutionContext = executionContext
    client.transactUntilSuccessful: tx =>
      given Transaction = tx
      tx.replaySafe("test")
      client.getStoragePool(PoolId.BootstrapPoolId).flatMap(_.allocator.allocateDataObject(Array[Byte](7)))

  /** Holds the finalization of every transaction carrying it until the test completes `release`, so its stores keep
   *  holding it: an allocation whose AllocationFinalizationAction may still be pending */
  private object Blocker extends FinalizationActionFactory with RegisteredTypeFactory:
    val typeUUID: UUID = UUID.fromString("6d1f0c2a-3b4e-4f5a-9c8d-7e6f5a4b3c2d")
    @volatile var release: Promise[Unit] = Promise()
    def createFinalizationAction(client: AspenClient, txd: TransactionDescription,
                                 data: Array[Byte]): FinalizationAction =
      val held = release.future
      new FinalizationAction:
        def complete: Future[Unit] = held
        def execute(): Unit = ()

  override def userTypeFactories: List[RegisteredTypeFactory] = List(Blocker)

  /** Allocates an object whose transaction stays unfinalized until `Blocker.release`, and removes its allocation-tree
   *  entry once its AllocationFinalizationAction wrote it: an allocation the walks cannot see */
  private def allocateHeldWithoutEntry(): Future[ObjectPointer] =
    given ExecutionContext = executionContext
    Blocker.release = Promise()
    def awaitEntry(ptr: ObjectPointer, n: Int): Future[Unit] =
      client.getStoragePool(ptr.poolId).flatMap(_.allocationTree.get(Key(ptr.id.toBytes))).flatMap:
        case Some(_) => Future.unit
        case None if n == 0 => Future.failed(new Exception(s"allocation-tree entry of ${ptr.id} never written"))
        case None => Future(()).flatMap(_ => awaitEntry(ptr, n - 1))
    for
      ptr <- client.transactUntilSuccessful: tx =>
        given Transaction = tx
        tx.replaySafe("test")
        tx.addFinalizationAction(FinalizationActionId(Blocker.typeUUID), Some(Array[Byte]()))
        client.getStoragePool(PoolId.BootstrapPoolId).flatMap(_.allocator.allocateDataObject(Array[Byte](9)))
      _ <- awaitEntry(ptr, 5000)
      _ <- dropEntry(ptr)
    yield ptr

  private def peers: List[StoreId] = List(net.storeId1, net.storeId2)

  private def releaseBlocked(): Future[Unit] =
    Blocker.release.trySuccess(())
    net.waitForTransactionsToComplete()

  atest("an allocation its peers hold unfinalized, with no tree entry, is restored by the sweep (Rebuild F10)"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      _ <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      held <- allocateHeldWithoutEntry()
      listed <- client.currentAllocations(peers, 2, () => false)
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    testingOnlyTrackRestoredKeys = true)
      _ <- rebuild.complete
      _ <- releaseBlocked()
      after <- client.currentAllocations(peers, 2, () => false)
    yield
      listed.keySet should be(peers.toSet)
      listed.values.foreach: allocations =>
        allocations.filter(_.pointers.exists(_.id == held.id)).map(_.committed) should be(List(true))
      after.values.flatten.exists(_.pointers.exists(_.id == held.id)) should be(false)
      rebuild.testingOnlyRestoredKeys.map(_.bytes.toList) should contain(held.id.toBytes.toList)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(true)

  /** The stores' votes on `txid`, or None for a store not holding it */
  private def votes(txid: TransactionId): List[Option[TransactionDisposition.Value]] =
    net.stores.map: s =>
      net.hostFor(s.storeId).control.router.loaded.get(s.storeId)
        .flatMap(_.frontend.transactions.get(txid)).map(_.vote)

  atest("an object read under an undecided commit's write lock is marked against the rebuilt store (RB-W2)"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    def awaitVotes(txid: TransactionId, n: Int): Future[Unit] =
      if votes(txid).forall(_.contains(TransactionDisposition.VoteCommit)) then Future.unit
      else if n == 0 then Future.failed(new Exception(s"the stores never all voted commit on $txid: ${votes(txid)}"))
      else Future(()).flatMap(_ => awaitVotes(txid, n - 1))
    for
      _ <- populate(5)
      locked <- allocateOne()
      free <- allocateOne()
      _ <- net.waitForTransactionsToComplete()
      before <- client.read(locked.asInstanceOf[DataObjectPointer])
      // T overwrites `locked`; every vote on it is dropped, so each store holds its commit vote and T's lock undecided
      tx = client.newTransaction()
      isVote = (m: TxMessage) => m match
                 case r: TxPrepareResponse => r.transactionId == tx.id
                 case _ => false
      _ = net.dropTxMessage = isVote
      _ = net.recordTxMessage = isVote
      _ = net.dropClientResponse =
            case v: TransactionVote => v.transactionId == tx.id
            case _ => false
      _ = tx.overwrite(locked.asInstanceOf[DataObjectPointer], before.revision, DataBuffer(Array[Byte](8)))
      tCommit = tx.commit()
      _ <- awaitVotes(tx.id, 5000)
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId))
      _ <- rebuild.complete
      pool <- client.getStoragePool(PoolId.BootstrapPoolId)
      lockedEntry <- pool.errorTree.get(MissedUpdateMarker.errorKey(storeId, locked.id))
      freeEntry <- pool.errorTree.get(MissedUpdateMarker.errorKey(storeId, free.id))
      _ = withClue("T is still undecided: ")(tCommit.isCompleted should be(false))
      // The votes reach the leader late: T commits
      _ = net.dropTxMessage = _ => false
      _ = net.dropClientResponse = _ => false
      _ = net.recordTxMessage = _ => false
      _ = net.takeRecordedTxMessages().foreach(net.deliverTransactionMessage)
      _ <- tCommit
    yield
      withClue("the copy read under T's lock is recorded for repair: ")(lockedEntry should be(defined))
      withClue("an unlocked copy is not: ")(freeEntry should be(None))

  atest("a listed allocation held undecided while the rebuilding store is silent does not keep the rebuild from finishing (RB-W1)"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    def awaitListedUnresolved(id: ObjectId, n: Int): Future[Unit] =
      client.currentAllocations(peers, 2, () => false).flatMap: listed =>
        if listed.values.forall(_.exists(a => !a.committed && a.pointers.exists(_.id == id))) then Future.unit
        else if n == 0 then Future.failed(new Exception(s"the peers never listed $id unresolved: $listed"))
        else Future(()).flatMap(_ => awaitListedUnresolved(id, n - 1))
    for
      _ <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      // T allocates an object; every vote on it is dropped, so its stores hold it undecided
      tx = client.newTransaction()
      isVote = (m: TxMessage) => m match
                 case r: TxPrepareResponse => r.transactionId == tx.id
                 case _ => false
      _ = net.dropTxMessage = isVote
      _ = net.recordTxMessage = isVote
      _ = net.dropClientResponse =
            case v: TransactionVote => v.transactionId == tx.id
            case _ => false
      _ = tx.replaySafe("test")
      pool <- client.getStoragePool(PoolId.BootstrapPoolId)
      ptr <- pool.allocator.allocateDataObject(Array[Byte](3))(using tx)
      tCommit = tx.commit()
      _ <- awaitListedUnresolved(ptr.id, 5000)
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    pendingWaitLimit = scala.concurrent.duration.Duration.Zero)
      outcome <- outcomeOrStalled(rebuild)
      entry <- pool.errorTree.get(MissedUpdateMarker.errorKey(storeId, ptr.id))
      _ = withClue("T is still undecided: ")(tCommit.isCompleted should be(false))
      _ = net.dropTxMessage = _ => false
      _ = net.dropClientResponse = _ => false
      _ = net.recordTxMessage = _ => false
      _ = net.takeRecordedTxMessages().foreach(net.deliverTransactionMessage)
      _ <- tCommit
      _ <- net.waitForTransactionsToComplete()
    yield
      withClue("the rebuild finishes: ")(outcome should be(Some(scala.util.Success(()))))
      withClue("the unsettled allocation is recorded for repair: ")(entry should be(defined))
      CopyFixtures.copyPath(dev, storeId).isDefined should be(true)

  atest("a listed allocation whose transaction finalizes before the sweep is restored from a read"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      _ <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      held <- allocateHeldWithoutEntry()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    testingOnlyTrackRestoredKeys = true,
                                    testingOnlyAfterQuery = () => releaseBlocked())
      _ <- rebuild.complete
    yield
      rebuild.testingOnlyRestoredKeys.map(_.bytes.toList) should contain(held.id.toBytes.toList)

  atest("an allocation an earlier pass listed, finalized since with its entry behind the checkpoint, is restored on resume (Rebuild F10, G8)"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
    var firstRef: RebuildingStore = null
    for
      _ <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      held <- allocateHeldWithoutEntry()
      // The first pass lists the held allocation, walks, and is cut off before its sweep
      first = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                  checkpointInterval = 1,
                                  testingOnlyAfterWalk = () => { firstRef.cancel(); Future.unit })
      _ = firstRef = first
      outcome <- first.complete.transform(scala.util.Success.apply)
      listed = RebuildState.load(staging).get.pendingAllocations
      // The entry lands and the transaction finalizes, so the next pass's peers list nothing. The entry's timestamp is
      // that of the transaction writing it, which may have started long before the walk did (a late decision), so
      // nothing about it tells the resume to look back
      _ <- landEntry(held)
      _ <- releaseBlocked()
      // And the walk had passed its key
      st = RebuildState.load(staging).get
      _ = RebuildState.save(staging, st.copy(lastRestoredKey = Some(Key(Array.fill[Byte](16)(-1)))))
      resumed = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    testingOnlyTrackRestoredKeys = true)
      _ <- resumed.complete
    yield
      outcome.failed.get should be(RebuildingStore.RebuildCancelled(storeId))
      listed.flatMap(_.pointers).map(_.id) should contain(held.id)
      resumed.testingOnlyRestoredKeys.map(_.bytes.toList) should contain(held.id.toBytes.toList)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(true)

  atest("a listed allocation deleted before its transaction finalizes is settled by a read proving the deletion"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    def delete(ptr: ObjectPointer): Future[Unit] =
      client.transactUntilSuccessful: tx =>
        given Transaction = tx
        client.read(ptr.asInstanceOf[org.aspen_ddp.aspen.common.objects.DataObjectPointer]).map: os =>
          tx.setRefcount(ptr, os.refcount, os.refcount.update(0))
    for
      _ <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      held <- allocateHeldWithoutEntry()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    testingOnlyTrackRestoredKeys = true,
                                    testingOnlyAfterQuery = () => delete(held).flatMap(_ => releaseBlocked()))
      _ <- rebuild.complete
    yield
      rebuild.testingOnlyRestoredKeys.map(_.bytes.toList) should not contain(held.id.toBytes.toList)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(true)

  atest("a rebuild does not complete until a quorum of its peers has listed their pending allocations"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    // Replication(3, 2): width - writeThreshold + 1 = 2, so both peers must answer
    net.dropClientResponse =
      case r: CurrentAllocationsReply => r.fromStore == net.storeId2
      case _ => false
    for
      _ <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId))
      stalled <- outcomeOrStalled(rebuild)
      _ = net.dropClientResponse = _ => false
      _ <- rebuild.complete
    yield
      stalled should be(None)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(true)

  // A pool short of live peers fails the pass into the host's backoff retry instead of holding its slot
  atest("a peer query without its quorum within its timeout fails the pass"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    // Replication(3, 2): both peers must answer, and one never does
    net.dropClientResponse =
      case r: CurrentAllocationsReply => r.fromStore == net.storeId2
      case _ => false
    for
      _ <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    peerQueryTimeout = 100.millis)
      outcome <- within(10.seconds, "the rebuild")(rebuild.complete.transform(scala.util.Success.apply))
    yield
      net.dropClientResponse = _ => false
      outcome.failed.get shouldBe a[RebuildingStore.PeerQueryTimedOut]
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)

  atest("a cancel while waiting for the peers' quorum fails the rebuild"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    net.dropClientResponse =
      case _: CurrentAllocationsReply => true
      case _ => false
    for
      _ <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId))
      stalled <- outcomeOrStalled(rebuild)
      _ = rebuild.cancel()
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      net.dropClientResponse = _ => false
      stalled should be(None)
      outcome.failed.get should be(RebuildingStore.RebuildCancelled(storeId))
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)

  atest("a rebuilt store records that its acceptor covers only transactions started after its rebuild"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      _ <- populate(3)
      _ <- net.waitForTransactionsToComplete()
      before = HLCTimestamp.now
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId))
      _ <- rebuild.complete
      after = HLCTimestamp.now
    yield
      val since = RebuildingStore.readAcceptorSince(CopyFixtures.copyPath(dev, storeId).get)
      since.isDefined should be(true)
      // At least the margin past the start of the rebuild, and no more than the margin past its end
      (since.get - before) should be >= RebuildingStore.AcceptorSinceMargin.asInstanceOf[scala.concurrent.duration.Duration]
      (since.get - after) should be <= RebuildingStore.AcceptorSinceMargin.asInstanceOf[scala.concurrent.duration.Duration]

  atest("a rebuilt store holds its inherited votes until a query past its pass's start plus the horizon and margins"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val horizon = scala.concurrent.duration.FiniteDuration(2, scala.concurrent.duration.SECONDS)
    for
      _ <- populate(3)
      _ <- net.waitForTransactionsToComplete()
      before = System.currentTimeMillis()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
        horizon = horizon)
      _ <- rebuild.complete
      after = System.currentTimeMillis()
    yield
      val gate = RebuildingStore.readInheritedVotes(CopyFixtures.copyPath(dev, storeId).get, horizon)
      val lead = horizon.toMillis + 2 * RebuildingStore.AcceptorSinceMargin.toMillis
      gate.isDefined should be(true)
      // The pass started between the two readings
      (gate.get - before) should be >= lead
      (gate.get - after) should be <= lead

  atest("a rebuild gives the store a fresh incarnation"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val seeded = "00000000-0000-0000-0000-00000000abcd"
    os.write.over(dev / RebuildingStore.RebuildDirectory / storeId.directoryName / StoreIncarnationFile.FileName, seeded,
      createFolders = true)
    for
      _ <- populate(3)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId))
      _ <- rebuild.complete
    yield
      val written = os.read(CopyFixtures.copyPath(dev, storeId).get / StoreIncarnationFile.FileName).trim
      written should not be seeded
      Try(UUID.fromString(written)).isSuccess should be(true)

  atest("an unreadable acceptor-since file reads as now plus the margin, and an absent one as none"):
    given ExecutionContext = executionContext
    val dir = deviceDir()
    val absent = RebuildingStore.readAcceptorSince(dir)
    os.write.over(dir / RebuildingStore.AcceptorSinceFile, "not a timestamp")
    val before = HLCTimestamp.now
    val unreadable = RebuildingStore.readAcceptorSince(dir)
    Future.successful:
      absent should be(None)
      unreadable.isDefined should be(true)
      (unreadable.get - before) should be >= RebuildingStore.AcceptorSinceMargin.asInstanceOf[scala.concurrent.duration.Duration]

  atest("a final directory no rebuild finished fails the rebuild and is left in place"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    // rebuild-process 4.1 (T12): a leftover of an earlier tenancy, Live, not Rebuilt. StoreRebuilds retires
    // such a directory before starting a rebuild; the rebuild itself must not take it for a finished one.
    val leftover = CopyFixtures.placeCopy(dev, storeId, configured = false)
    os.write.over(leftover / "data", "stale")
    for
      _ <- populate(3)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    testingOnlyTrackRestoredKeys = true)
      outcome <- outcomeOrStalled(rebuild)
    yield
      outcome.map(_.isFailure) should be(Some(true))
      rebuild.testingOnlyRestoredKeys should be(Nil)
      os.list(leftover).map(_.last) should be(Seq("data"))

  atest("a phase-param.rebuilt that cannot be read names no episode by throwing, never by answering false (RB-W22)"):
    val dev = deviceDir()
    val episode = Some(UUID.randomUUID())
    val copy = CopyFixtures.placeCopy(dev, storeId, Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(episode)),
      configured = false)
    val param = copy / CopyLayout.paramFile(Phase.Rebuilt("")).get
    // Its stat succeeds and its open fails (EACCES): a read error that says nothing of the episode it names
    os.perms.set(param, "---------")
    // The listing a device's load registers its copies from (and a rebuild's existingCopy is a record of) throws
    val unreadable = Try(CopyLayout.list(dev))
    os.perms.set(param, "rw-------")
    val found = CopyFixtures.existingCopy(dev, storeId)
    Future.successful:
      unreadable.failed.toOption.map(_.isInstanceOf[java.io.IOException]) should be(Some(true))
      found should be(Some((Phase.Rebuilt(episode.get.toString), copy)))
      RebuildingStore.isRebuiltFor(found.get._1, episode) should be(true)
      RebuildingStore.isRebuiltFor(found.get._1, Some(UUID.randomUUID())) should be(false)
      RebuildingStore.isRebuiltFor(found.get._1, None) should be(false)
      RebuildingStore.isRebuiltFor(Phase.Live, episode) should be(false)
      CopyLayout.list(dev / "absent").copies should be(Nil)

  atest("a pre-existing final directory skips straight to completion"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    // A crash between the move and the flip leaves the store in place, in rebuilt/. The rebuild must not
    // start over, and must not fail.
    CopyFixtures.placeCopy(dev, storeId, Phase.Rebuilt(""), configured = false)
    // Staged as a crash just before the move would have left it: the move is not atomic with
    // the flip, so a staging directory carrying a checkpoint can outlive the final directory
    // appearing. Pre-created so that asserting on its removal is an assertion about
    // cleanupStaging rather than about a directory that was never there.
    val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
    os.makeDir.all(staging)
    RebuildState.save(staging, RebuildState(storeId, None, Some(Key(Array[Byte](7))), Nil, Nil))

    for
      _ <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    testingOnlyTrackRestoredKeys = true)
      _ <- rebuild.complete
    yield
      os.exists(staging) should be(false)
      // Nothing was walked: the store is already whole, and re-reading a pool's worth of objects
      // to rewrite a store that already holds them is precisely the work this branch exists to
      // skip.
      rebuild.testingOnlyRestoredKeys should be(Nil)

  atest("finish renames staging into rebuilt with the episode as phase-param"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val episode = UUID.randomUUID()
    for
      _ <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends,
                                    TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId), Some(episode))
      copyId <- rebuild.complete
    yield
      val copies = CopyLayout.list(dev).copies.filter(_.storeId == storeId)
      copies.map(f => (f.phase, f.copyId)) should be(List((Phase.Rebuilt(episode.toString), copyId)))
      copies.head.path should be(CopyLayout.path(dev, storeId, copyId, Phase.Rebuilt(episode.toString)))
      copies.head.path / os.up should be(dev / CopyLayout.RebuiltDirectory)
      CopyLayout.readParam(copies.head.path, Phase.Rebuilt("")) should be(episode.toString)
      os.exists(dev / RebuildingStore.RebuildDirectory / storeId.directoryName) should be(false)
      // Nothing in live/
      os.exists(dev / CopyLayout.LiveDirectory) should be(false)

  // Final review I2: a rename that landed but was not forced completes with the copy's id, reported unforced, so the
  // host records the copy unsynced rather than run a second rebuild over the copy in rebuilt/
  atest("a finish whose rename into rebuilt lands unforced completes with its copy id, reported unforced"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val episode = UUID.randomUUID()
    val rebuiltDir = dev / CopyLayout.RebuiltDirectory
    @volatile var failed = 0
    val sync: os.Path => Unit = d =>
      if d == rebuiltDir && failed == 0 then
        failed += 1
        throw new java.io.IOException("injected sync")
      DurableFiles.syncDirectory(d)
    for
      _ <- populate(2)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends,
                                    TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId), Some(episode), syncDirectory = sync)
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      failed should be(1)
      val copies = CopyLayout.list(dev).copies.filter(_.storeId == storeId)
      copies.map(f => (f.phase, f.copyId)) should be(List((Phase.Rebuilt(episode.toString), copies.head.copyId)))
      outcome should be(scala.util.Success(copies.head.copyId))
      rebuild.placedUnforced.map(_.target) should be(Some(copies.head.path))

  atest("start takes a rebuilt copy of its own episode for done, and fails on any other copy"):
    given ExecutionContext = executionContext
    val episode = UUID.randomUUID()
    def startOver(place: os.Path => os.Path): Future[(Try[UUID], os.Path, RecordingOpener)] =
      val dev = deviceDir()
      val placed = place(dev)
      val opener = new RecordingOpener
      val rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, opener,
                                        TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId), Some(episode),
                                        testingOnlyTrackRestoredKeys = true)
      rebuild.complete.transform(t => scala.util.Success((t, placed, opener)))
    for
      (own, ownCopy, ownOpener) <- startOver(dev =>
        CopyFixtures.placeCopy(dev, storeId, Phase.Rebuilt(episode.toString), configured = false))
      (other, _, otherOpener) <- startOver(dev =>
        CopyFixtures.placeCopy(dev, storeId, Phase.Rebuilt(UUID.randomUUID().toString), configured = false))
      (live, _, liveOpener) <- startOver(dev => CopyFixtures.placeCopy(dev, storeId, Phase.Live, configured = false))
    yield
      own should be(scala.util.Success(CopyFixtures.copyIdOf(ownCopy)))
      ownOpener.creates should be(Nil)
      List(other, live).foreach: outcome =>
        outcome.failed.get shouldBe an[IllegalStateException]
        outcome.failed.get.getMessage should include("holds a store directory that no rebuild finished")
      otherOpener.creates should be(Nil)
      liveOpener.creates should be(Nil)

  /** Shared body of the torn-checkpoint tests: `contents` is written where a checkpoint belongs
   *  and the pass must restart from the beginning rather than throw at its first statement. */
  private def tornCheckpointRestartsTheWalk(contents: String): Future[compatible.Assertion] =
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      _ = os.makeDir.all(staging)
      _ = os.write.over(staging / RebuildState.stateFilename, contents)
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 5,
                                    testingOnlyTrackRestoredKeys = true)
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      // Aspen is crash-only, so this file is what an ordinary termination in an unlucky window
      // leaves behind. Throwing on it would make the store unrebuildable until an operator
      // deleted the file by hand.
      outcome.isSuccess should be(true)
      val restoredBytes = rebuild.testingOnlyRestoredKeys.map(_.bytes.toList).toSet
      val allocatedBytes = ids.map(id => Key(id.toBytes).bytes.toList).toSet
      // From the beginning: every object, not the tail of some range the torn file half-named.
      (allocatedBytes subsetOf restoredBytes) should be(true)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(true)

  atest("an empty checkpoint restarts the walk from the beginning"):
    tornCheckpointRestartsTheWalk("")

  atest("an unparsable checkpoint restarts the walk from the beginning"):
    tornCheckpointRestartsTheWalk("failed-objects: [unclosed")

  atest("a resume skips the objects already restored"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()

      // Stage a checkpoint by hand with a key at a known rank, as a crashed pass would have left behind.
      staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      pstate <- client.getStoragePoolState(storeId.poolId)
      _ <- backends.create(StoreConfig.forPool(storeId, pstate), staging, dev / StoreCreation.CreatingDirectory, TestDeviceIo.of(dev))
      allocatedKeys = ids.map(id => Key(id.toBytes))
      sorted = allocatedKeys.sortWith(ByteArrayKeyOrdering.lt)
      _ = RebuildState.save(staging, RebuildState(storeId, None, Some(sorted(9)), Nil, Nil))

      // The rebuild should resume from that key
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 5,
                                    testingOnlyTrackRestoredKeys = true)
      _ <- rebuild.complete
      restored = rebuild.testingOnlyRestoredKeys
      restoredBytes = restored.map(_.bytes.toList).toSet
      allocatedBytes = allocatedKeys.map(_.bytes.toList).toSet
      expectedBytes = sorted.drop(9).map(_.bytes.toList).toSet
    yield
      // The resume range is inclusive of the checkpointed key -- rebuildWrite is an overwrite,
      // so re-restoring it is free -- and everything before it is skipped. Intersect against
      // the keys we allocated to ignore system objects the fixture brought up.
      (restoredBytes intersect allocatedBytes) should be(expectedBytes)
      // The checkpoint key should be in the restored set (inclusive resume)
      restoredBytes should contain(sorted(9).bytes.toList)

  /** A checkpoint at the 10th allocated key, written by a pass of `checkpointEpisode`, resumed by a rebuild of
   *  `rebuildEpisode`: the allocated keys the rebuild restored, the expected resumed range, and the phase of the copy
   *  it left */
  private def resumeAcrossEpisodes(checkpointEpisode: Option[UUID],
                                   rebuildEpisode: Option[UUID]): Future[(Set[List[Byte]], Set[List[Byte]], Set[List[Byte]], Option[Phase])] =
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      pstate <- client.getStoragePoolState(storeId.poolId)
      _ <- backends.create(StoreConfig.forPool(storeId, pstate), staging, dev / StoreCreation.CreatingDirectory, TestDeviceIo.of(dev))
      sorted = ids.map(id => Key(id.toBytes)).sortWith(ByteArrayKeyOrdering.lt)
      _ = RebuildState.save(staging, RebuildState(storeId, checkpointEpisode, Some(sorted(9)), Nil, Nil))
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    rebuildEpisode, checkpointInterval = 5, testingOnlyTrackRestoredKeys = true)
      _ <- rebuild.complete
    yield
      val allocated = sorted.map(_.bytes.toList).toSet
      (rebuild.testingOnlyRestoredKeys.map(_.bytes.toList).toSet intersect allocated,
       sorted.drop(9).map(_.bytes.toList).toSet,
       allocated,
       CopyFixtures.phaseOf(dev, storeId))

  atest("a resume continues a checkpoint of its own Rebuilding episode"):
    given ExecutionContext = executionContext
    val episode = UUID.randomUUID()
    resumeAcrossEpisodes(Some(episode), Some(episode)).map: (restored, resumed, _, phase) =>
      restored should be(resumed)
      // The finished copy names the episode that produced it
      phase should be(Some(Phase.Rebuilt(episode.toString)))

  atest("a checkpoint of an earlier Rebuilding episode is discarded and the walk starts over (RB-W3)"):
    given ExecutionContext = executionContext
    // The store was re-placed away and back since the checkpoint: objects before its key may have changed while the
    // store lived elsewhere, with no ErrorTree entry left for this copy
    val current = UUID.randomUUID()
    resumeAcrossEpisodes(Some(UUID.randomUUID()), Some(current)).map: (restored, _, allocated, phase) =>
      restored should be(allocated)
      phase should be(Some(Phase.Rebuilt(current.toString)))

  atest("a finished copy of an earlier Rebuilding episode is not taken for this rebuild's output (RB-W3)"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val earlier = UUID.randomUUID()
    val finished = CopyFixtures.placeCopy(dev, storeId, Phase.Rebuilt(earlier.toString), configured = false)
    for
      _ <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      later = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                  Some(UUID.randomUUID()), testingOnlyTrackRestoredKeys = true)
      refused <- later.complete.transform(scala.util.Success.apply)
      own = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                Some(earlier), testingOnlyTrackRestoredKeys = true)
      taken <- own.complete.transform(scala.util.Success.apply)
    yield
      // Not promoted, not overwritten: StoreRebuilds retires such a copy before a rebuild starts
      refused.failed.get shouldBe an[IllegalStateException]
      later.testingOnlyRestoredKeys should be(Nil)
      CopyFixtures.phaseOf(dev, storeId) should be(Some(Phase.Rebuilt(earlier.toString)))
      // The episode that finished it takes it for done
      taken should be(scala.util.Success(CopyFixtures.copyIdOf(finished)))
      own.testingOnlyRestoredKeys should be(Nil)

  atest("the checkpoint is written after the flush, not before"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      _ <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      // A checkpoint every 5 objects rather than every 1000, so the walk actually checkpoints
      // without needing a thousand allocations.
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 5)
      _ <- rebuild.complete
    yield
      // A checkpoint claiming objects that never reached stable storage is the one ordering
      // that loses data, so the order is recorded and asserted rather than inferred.
      val ops = rebuild.testingOnlyCheckpointTrace
      ops should not be empty
      ops.grouped(2).forall(pair => pair == List("flush", "checkpoint")) should be(true)

  atest("an object that cannot be read is recorded and the rebuild does not complete"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      doomed = Key(ids.head.toBytes)
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 5,
                                    testingOnlyFailKeys = Set(doomed))
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      // The pass must not report success with an object missing -- that would flip the store to
      // Active while it is short a slice.
      outcome.isFailure should be(true)
      // The staging directory and its checkpoint survive, so the next device check resumes.
      val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)
      val saved = RebuildState.load(staging).get
      saved.failedObjects should be(List(ids.head))
      // The checkpoint advanced past it: one unreadable object cannot wedge the walk forever.
      saved.lastRestoredKey should not be None

  /** Fails the test instead of hanging it when `f` does not complete within `limit` (real time) */
  private def within[T](limit: FiniteDuration, what: String)(f: Future[T]): Future[T] =
    given ExecutionContext = executionContext
    Future.firstCompletedOf(List(f, client.after(limit).map(_ => fail(s"$what did not complete within $limit"))))

  atest("an object whose read never completes is a failed object, and the rebuild does not stall"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      outcome <- net.withObjectReadsSilenced(Set(ids.head)):
        val rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                          checkpointInterval = 5, objectReadTimeout = 200.millis)
        within(10.seconds, "the rebuild")(rebuild.complete.transform(scala.util.Success.apply))
    yield
      outcome.isFailure should be(true)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)
      val saved = RebuildState.load(dev / RebuildingStore.RebuildDirectory / storeId.directoryName).get
      saved.failedObjects should be(List(ids.head))
      saved.lastRestoredKey should not be None

  // A walk whose every read times out aborts after a run of them, not after MaxFailedObjects
  atest("a run of read timeouts aborts the walk with its checkpoint kept"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      outcome <- net.withObjectReadsSilenced(ids.toSet):
        val rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                          checkpointInterval = 5, objectReadTimeout = 50.millis,
                                          maxConsecutiveReadTimeouts = 1)
        within(10.seconds, "the rebuild")(rebuild.complete.transform(scala.util.Success.apply))
    yield
      outcome.failed.get.getMessage should include("1 object reads in a row timed out")
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)
      // Aborted at the first timeout: the rest of the walk was skipped, and the checkpoint survives for the next pass
      val saved = RebuildState.load(dev / RebuildingStore.RebuildDirectory / storeId.directoryName).get
      saved.failedObjects.size should be(1)

  atest("a restored object ends a run of read timeouts"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      // The first and last of them in walk order: the other 18 are restored between the two timeouts
      sorted = ids.sortWith((a, b) => ByteArrayKeyOrdering.lt(Key(a.toBytes), Key(b.toBytes)))
      silenced = Set(sorted.head, sorted.last)
      outcome <- net.withObjectReadsSilenced(silenced):
        val rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                          checkpointInterval = 5, objectReadTimeout = 50.millis,
                                          maxConsecutiveReadTimeouts = 2)
        within(10.seconds, "the rebuild")(rebuild.complete.transform(scala.util.Success.apply))
    yield
      // Failed by retryFailures, with both objects recorded, not by the run's abort
      outcome.isFailure should be(true)
      outcome.failed.get.getMessage should not include("in a row timed out")
      val saved = RebuildState.load(dev / RebuildingStore.RebuildDirectory / storeId.directoryName).get
      saved.failedObjects.toSet should be(silenced)

  atest("a cancel ends a rebuild whose walk waits on a read that never completes"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    var rebuildRef: RebuildingStore = null
    for
      ids <- populate(5)
      _ <- net.waitForTransactionsToComplete()
      silenced = ids.toSet
      // Cancelled once the walk has sent its first read of a silenced object, so the walk is waiting on it
      _ = net.observeRead = r =>
            if silenced.contains(r.objectPointer.id) && rebuildRef != null then rebuildRef.cancel()
      outcome <- net.withObjectReadsSilenced(silenced):
        rebuildRef = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                         objectReadTimeout = 200.millis)
        within(10.seconds, "the cancelled rebuild")(rebuildRef.complete.transform(scala.util.Success.apply))
      _ = net.observeRead = _ => ()
    yield
      outcome.failed.get should be(RebuildingStore.RebuildCancelled(storeId))
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)

  atest("MaxFailedObjects cap prevents the store from moving into place"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      // Fail enough keys to exceed a low cap
      doomed = ids.take(15).map(id => Key(id.toBytes)).toSet
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 5,
                                    testingOnlyFailKeys = doomed,
                                    maxFailedObjects = 10)
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      // The pass must fail when the cap is exceeded
      outcome.isFailure should be(true)
      // The staging directory and its checkpoint survive, so the next device check resumes
      val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      os.exists(staging) should be(true)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)
      // The checkpoint was written when the cap was hit
      val saved = RebuildState.load(staging).get
      saved.lastRestoredKey should not be None

      // The count is what makes this a test of the cap rather than a second copy of the test
      // above. On the cap path the latch fires at failure 11 -- one past a cap of 10 -- the
      // remaining doomed keys short-circuit, and runPass fails at the abortCause check, so
      // retryFailures never runs and the checkpoint keeps all 11. With the cap raised out of
      // reach all 15 injected failures accumulate instead, and every other assertion here still
      // passes. Named for the mechanism rather than the arithmetic, so the intent survives a
      // change to either number.
      val expectedFailures = 10 + 1
      saved.failedObjects.size should be(expectedFailures)
      outcome.failed.get.getMessage should include("more than 10 unreadable objects")

  atest("an undecodable pointer in the allocation tree is a failed object, not a wedged walk"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    // A key of the right shape -- sixteen bytes, so it names an object -- whose value is not a
    // pointer. The leading 0xff makes Varint.getUnsignedInt read past the end of the buffer, so
    // ObjectPointer's decode throws BufferUnderflowException. Written through the real tree
    // rather than injected, because the decode is what is under test.
    val bogusId = ObjectId(UUID.fromString("77777777-7777-7777-7777-777777777777"))
    val bogusKey = Key(bogusId.toBytes)

    for
      _ <- populate(20)
      _ <- client.transactUntilSuccessful: tx =>
             given Transaction = tx
             tx.replaySafe("test")
             client.getStoragePool(PoolId.BootstrapPoolId).flatMap: pool =>
               pool.allocationTree.set(bogusKey, Value(Array[Byte](-1, -1, -1)))
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 5)
      settled <- outcomeOrStalled(rebuild)
    yield
      // The decode runs in restoreObject's plain body, outside any Future combinator. Before it
      // was made total, the throw was absorbed by ExecutionContext.reportFailure from inside
      // walkFrom's own onComplete recursion: the recursion stopped, walkFrom's promise was never
      // completed, runPass never completed, and the Host rebuild slot was never
      // released. Two of those deadlock a host at the default maxConcurrentRebuilds of 2. So the
      // assertion that matters is that the future completed at all.
      settled.isDefined should be(true)
      // Having completed, it must report the pass as unsuccessful rather than move a store that
      // is short an object into place.
      settled.get.isFailure should be(true)
      val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)
      // Recorded as one more failed object, subject to the existing cap and retry machinery.
      RebuildState.load(staging).get.failedObjects should be(List(bogusId))

  atest("cancel stops a rebuild and leaves its checkpoint for resumption"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
    var rebuildRef: RebuildingStore = null
    var restored = 0
    for
      _ <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      // Cancelled from inside the walk, after a checkpoint has been written, so the cancel
      // cannot race the walk finishing.
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 2,
                                    testingOnlyOnRestored = () =>
                                      restored += 1
                                      if restored == 5 then rebuildRef.cancel())
      _ = rebuildRef = rebuild
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
      // What the cancelled pass left behind, before a new one resumes from it
      checkpointed = RebuildState.load(staging).isDefined
      placed = CopyFixtures.copyPath(dev, storeId).isDefined
      resumed = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId))
      _ <- resumed.complete
    yield
      outcome.failed.get should be(RebuildingStore.RebuildCancelled(storeId))
      checkpointed should be(true)
      placed should be(false)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(true)

  atest("an abort latched while finish closes the backend keeps the staging copy out of place"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
    var rebuildRef: RebuildingStore = null
    for
      _ <- populate(6)
      _ <- net.waitForTransactionsToComplete()
      // As a fail-stop of the staging store's final I/O would: the abort arrives once the walk is done
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 2,
                                    testingOnlyOnFinishClosed = () => rebuildRef.cancel())
      _ = rebuildRef = rebuild
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      outcome.failed.get should be(RebuildingStore.RebuildCancelled(storeId))
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)
      os.exists(staging) should be(true)
      RebuildState.load(staging).isDefined should be(true)

  // A shutdown waits for its cancelled rebuilds, so a cancelled walk must stop reading at once
  // rather than read the rest of the allocation tree as no-ops.
  atest("a cancelled walk issues no further reads and closes promptly"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    var rebuildRef: RebuildingStore = null
    var restored = 0
    var afterCancel: Future[(Unit, Map[ObjectId, Int])] = null
    for
      _ <- populate(60)
      _ <- net.waitForTransactionsToComplete()
      // Read counting starts the moment cancel() returns, inside the walk's own callback, and
      // covers everything up to the rebuild's completion.
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 2,
                                    testingOnlyOnRestored = () =>
                                      restored += 1
                                      if restored == 5 then
                                        rebuildRef.cancel()
                                        afterCancel = net.countReads(
                                          rebuildRef.complete.map(_ => ()).recover { case _ => () }))
      _ = rebuildRef = rebuild
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
      (_, readsAfterCancel) <- afterCancel
    yield
      outcome.failed.get should be(RebuildingStore.RebuildCancelled(storeId))
      restored should be(5)
      readsAfterCancel should be(Map())

  atest("out of space fails the pass with empty failedObjects proving the latch alone stopped it"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      // Inject an out-of-space error on the first object
      doomed = Set(Key(ids.head.toBytes))
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 5,
                                    testingOnlyOutOfSpaceKeys = doomed)
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      // The pass must fail
      outcome.isFailure should be(true)
      // The staging directory and its checkpoint survive
      val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      os.exists(staging) should be(true)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)
      // failedObjects is EMPTY -- proving the latch alone failed the pass, not retryFailures()
      val saved = RebuildState.load(staging).get
      saved.failedObjects should be(Nil)

  atest("the staging store is created, then opened, through the BackendOpener with the pool's backend config"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val opener = new RecordingOpener
    for
      _ <- populate(3)
      _ <- net.waitForTransactionsToComplete()
      pstate <- client.getStoragePoolState(storeId.poolId)
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, opener, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId))
      _ <- rebuild.complete
    yield
      val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
      val expected = List((storeId, StoreConfig.storageBackendFor(pstate.backendConfig), staging))
      opener.creates.map((cfg, path) => (cfg.storeId, cfg.backend, path)) should be(expected)
      opener.opens.map((cfg, path) => (cfg.storeId, cfg.backend, path)) should be(expected)
      os.exists(dev / StoreCreation.CreatingDirectory) && os.list(dev / StoreCreation.CreatingDirectory).nonEmpty should
        be(false)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(true)

  atest("a staging store whose state is lost is discarded, checkpoint and all, and the next rebuild starts over"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
    val opener = new RecordingOpener
    for
      ids <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      // A checkpoint part-way through, as a crashed pass leaves it
      _ = os.makeDir.all(staging)
      sorted = ids.map(id => Key(id.toBytes)).sortWith(ByteArrayKeyOrdering.lt)
      _ = RebuildState.save(staging, RebuildState(storeId, None, Some(sorted(9)), Nil, Nil))
      _ = opener.lose = true
      lost = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, opener, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId))
      outcome <- lost.complete.transform(scala.util.Success.apply)
      stagingLeft = os.exists(staging)
      _ = opener.lose = false
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, opener, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    testingOnlyTrackRestoredKeys = true)
      _ <- rebuild.complete
    yield
      outcome.failed.get shouldBe a[BackendStateLost]
      stagingLeft should be(false)
      // From the beginning: every allocated object, not only those after the discarded checkpoint
      val restoredBytes = rebuild.testingOnlyRestoredKeys.map(_.bytes.toList).toSet
      (restoredBytes intersect sorted.map(_.bytes.toList).toSet) should be(sorted.map(_.bytes.toList).toSet)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(true)

  atest("a fail-stop of the staging store fails the pass and leaves its checkpoint for the next one"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
    val opener = new RecordingOpener
    val failure = new java.io.IOException("injected fail-stop")
    var restored = 0
    for
      _ <- populate(20)
      _ <- net.waitForTransactionsToComplete()
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, opener, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 2,
                                    testingOnlyOnRestored = () =>
                                      restored += 1
                                      if restored == 5 then opener.onFatals.head(failure))
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      outcome.failed.get should be(failure)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)
      RebuildState.load(staging).isDefined should be(true)

  /** Opens through the default opener, wrapping the backend so its rebuildFlush throws an out-of-space error while
   *  `full` is set */
  private final class FillingOpener extends BackendOpener:
    @volatile var full: Boolean = false

    def create(storeCfg: StoreConfig, storePath: os.Path, creatingRoot: os.Path, io: DeviceIo): Future[Unit] =
      backends.create(storeCfg, storePath, creatingRoot, io)

    def open(storeCfg: StoreConfig, storePath: os.Path, onFatal: Throwable => Unit, io: DeviceIo): Future[Backend] =
      given ExecutionContext = executionContext
      backends.open(storeCfg, storePath, onFatal, io).map: b =>
        new Backend:
          val storeId: StoreId = b.storeId
          def path: os.Path = b.path
          def close(): Future[Unit] = b.close()
          def estimateSize(): Long = b.estimateSize()
          def setCompletionHandler(handler: CompletionHandler): Unit = b.setCompletionHandler(handler)
          def bootstrapAllocate(objectId: ObjectId, objectType: ObjectType.Value, metadata: Metadata,
                                data: DataBuffer): Unit = b.bootstrapAllocate(objectId, objectType, metadata, data)
          def bootstrapOverwrite(objectId: ObjectId, data: DataBuffer): Unit = b.bootstrapOverwrite(objectId, data)
          def rebuildWrite(pointer: ObjectPointer, objectType: ObjectType.Value, metadata: Metadata,
                           data: DataBuffer): Unit = b.rebuildWrite(pointer, objectType, metadata, data)
          def rebuildFlush(): Unit =
            if full then throw new java.io.IOException("No space left on device")
            b.rebuildFlush()
          def read(pointer: ObjectPointer): Unit = b.read(pointer)
          def commit(state: CommitState, transactionId: TransactionId): Unit = b.commit(state, transactionId)
          def repair(state: CommitState, complete: Promise[Unit]): Unit = b.repair(state, complete)
          def repairDelete(objectId: ObjectId, storePointer: Array[Byte], complete: Promise[Unit]): Unit =
            b.repairDelete(objectId, storePointer, complete)

  atest("a full device at finish's flush raises the out-of-space alert and keeps the checkpoint (RB-W10)"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
    val opener = new FillingOpener
    val alerts = new java.util.concurrent.LinkedBlockingQueue[scribe.Level]()
    val handler = scribe.handler.LogHandler(scribe.Level.Info): r =>
      val text = r.logOutput.plainText
      if text.contains("REBUILD OUT OF SPACE") && text.contains(dev.toString) then alerts.add(r.level)
    scribe.Logger.root.withHandler(handler).replace()
    val run =
      for
        _ <- populate(6)
        _ <- net.waitForTransactionsToComplete()
        // The walk fit; the final flush is the write that finds the device full
        rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, opener, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                      checkpointInterval = 2,
                                      testingOnlyAfterWalk = () =>
                                        opener.full = true
                                        Future.unit)
        outcome <- rebuild.complete.transform(scala.util.Success.apply)
      yield outcome
    run.transform: result =>
      scribe.Logger.root.withoutHandler(handler).replace()
      result.map: outcome =>
        Backend.isOutOfSpace(outcome.failed.get) should be(true)
        alerts.toArray(Array.empty[scribe.Level]).toList should be(List(scribe.Level.Error))
        CopyFixtures.copyPath(dev, storeId).isDefined should be(false)
        RebuildState.load(staging).isDefined should be(true)

  atest("a failed completion-file write in finish keeps the checkpoint for the next pass (RB-W10)"):
    given ExecutionContext = executionContext
    val dev = deviceDir()
    val staging = dev / RebuildingStore.RebuildDirectory / storeId.directoryName
    for
      _ <- populate(6)
      _ <- net.waitForTransactionsToComplete()
      // A directory where the episode's phase-param goes makes its write, the last before the move, fail
      rebuild = new RebuildingStore(client, storeId, StorageDeviceId.BootstrapStorageDeviceId, dev, backends, TestDeviceIo.of(dev), CopyFixtures.existingCopy(dev, storeId),
                                    checkpointInterval = 2,
                                    testingOnlyOnFinishClosed = () =>
                                      os.makeDir.all(staging / CopyLayout.paramFile(Phase.Rebuilt("")).get / "x"))
      outcome <- rebuild.complete.transform(scala.util.Success.apply)
    yield
      outcome.isFailure should be(true)
      CopyFixtures.copyPath(dev, storeId).isDefined should be(false)
      RebuildState.load(staging).isDefined should be(true)
