package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.{IntegrationTestSuite, TestNetwork}
import org.aspen_ddp.aspen.client.RegisteredTypeFactory
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId, StorageDeviceSetId, StorageDeviceState}
import org.aspen_ddp.aspen.common.network.{CheckStorageDevice, StoreMoved}
import org.aspen_ddp.aspen.common.objects.{Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionDescription
import org.aspen_ddp.aspen.compute.ServiceEntry
import org.aspen_ddp.aspen.compute.systemtask.{CountingSystemTask, SystemTaskExecutorService, SystemTaskServiceState}
import org.aspen_ddp.aspen.server.host.DeviceMetadata.{RebuildOutcome, TransferOutcome}
import org.aspen_ddp.aspen.server.store.backend.RocksDBConfig

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Todo 5.10: a retried call whose earlier attempt committed but was reported TransactionOutcomeUnknown recognizes its
  * own commit. Each test has the stores commit the call's transaction while the client is told Unknown
  * (TestNetwork.reportCommitAsUnknown), and checks the call ends as it would have had the client heard Committed:
  * the same result, nothing done twice, no false error
  */
class CommittedUnknownRetrySuite extends IntegrationTestSuite:

  override def userTypeFactories: List[RegisteredTypeFactory] = List(CountingSystemTask)

  private val storeId = StoreId(PoolId.BootstrapPoolId, 1)

  /** The transactions that allocate an object: the creates and the system-task enrollments */
  private def allocates(txd: TransactionDescription): Boolean = txd.allocatingObjects.nonEmpty

  private def touches(ptr: KeyValueObjectPointer)(txd: TransactionDescription): Boolean =
    txd.allReferencedObjectsSet.exists(_.id == ptr.id)

  private def takeNudges(): List[CheckStorageDevice] =
    net.takeCapturedHostMessages().collect:
      case m: CheckStorageDevice => m

  private def enrolledTasks(): Future[List[(UUID, KeyValueObjectPointer)]] =
    given ExecutionContext = executionContext
    val tkvl = TieredKeyValueList(client, KVObjectRootManager(client, Radicle.ServicesTreeKey, Radicle.pointer))
    for
      vs <- tkvl.get(Key(SystemTaskExecutorService.ServiceUUID))
      enrolled <- SystemTaskServiceState.scan(client, ServiceEntry.decode(vs.get.value.bytes).statePointer)
    yield enrolled

  /** The bootstrap host's executor root is in place: its creation is the only background transaction that
    * allocates, so after this `allocates` matches only the call under test */
  private def ready(): Future[Unit] =
    given ExecutionContext = executionContext
    net.smgr.getTaskExecutor().flatMap(_ => waitForTransactionsToComplete())

  private def retryCreates(): Unit = client.asInstanceOf[TestNetwork.TClient].retryCreates = true

  atest("createSystemDurableTask reported Unknown enrolls one task"):
    given ExecutionContext = executionContext
    for
      _ <- ready()
      _ = net.reportCommitAsUnknown(allocates)
      _ <- client.createSystemDurableTask(CountingSystemTask.typeUUID, CountingSystemTask.initialState(0))
      _ <- waitForTransactionsToComplete()
      enrolled <- enrolledTasks()
    yield
      net.pendingUnknownCommits should be(0)
      enrolled.size should be(1)

  atest("failStorageDevice reported Unknown enrolls one task and does not fail"):
    given ExecutionContext = executionContext
    for
      _ <- ready()
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ = net.reportCommitAsUnknown(allocates)
      _ <- client.failStorageDevice(net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      enrolled <- enrolledTasks()
    yield
      net.pendingUnknownCommits should be(0)
      enrolled.size should be(1)

  atest("transferStore reported Unknown succeeds and nudges the destination"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      dstPtr <- client.getStorageDevicePointer(net.secondDeviceId)
      _ = takeNudges()
      _ = net.reportCommitAsUnknown(touches(dstPtr))
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      nudges = takeNudges()
      src <- client.getStorageDeviceState(StorageDeviceId.BootstrapStorageDeviceId)
      dst <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      net.pendingUnknownCommits should be(0)
      src.stores(storeId).status should be(StorageDeviceState.StoreStatus.TransferringOut)
      dst.stores(storeId).status should be(StorageDeviceState.StoreStatus.TransferringIn)
      dst.stores(storeId).transferId should be(src.stores(storeId).transferId)
      nudges.map(_.deviceId) should be(List(net.secondDeviceId))

  atest("abortStoreTransfer reported Unknown reports the rollback and sends its nudges"):
    given ExecutionContext = executionContext
    for
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      dstPtr <- client.getStorageDevicePointer(net.secondDeviceId)
      _ = takeNudges()
      _ = net.reportCommitAsUnknown(touches(dstPtr))
      rolledBack <- client.abortStoreTransfer(storeId)
      nudges = takeNudges()
      src <- client.getStorageDeviceState(StorageDeviceId.BootstrapStorageDeviceId)
    yield
      net.pendingUnknownCommits should be(0)
      rolledBack should be(true)
      src.stores(storeId).status should be(StorageDeviceState.StoreStatus.Active)
      nudges.map(_.deviceId).toSet should be(Set(StorageDeviceId.BootstrapStorageDeviceId, net.secondDeviceId))

  atest("createHost reported Unknown returns the host rather than KeyAlreadyExists"):
    given ExecutionContext = executionContext
    retryCreates()
    val hostId = HostId(UUID.randomUUID())
    for
      _ <- ready()
      _ = net.reportCommitAsUnknown(allocates)
      created <- client.createHost("unknown-host", "127.0.0.1", 5850, 5851, 5852, hostId)
      state <- client.getHostState(hostId)
    yield
      net.pendingUnknownCommits should be(0)
      created should be(hostId)
      state.name should be("unknown-host")

  atest("createAllocationGroup reported Unknown returns the group rather than KeyAlreadyExists"):
    given ExecutionContext = executionContext
    retryCreates()
    for
      _ <- ready()
      _ = net.reportCommitAsUnknown(allocates)
      groupId <- client.createAllocationGroup("unknown-group", 0, ida = Some(Replication(3, 2)))
      state <- client.getAllocationGroupState(groupId)
    yield
      net.pendingUnknownCommits should be(0)
      state.name should be("unknown-group")

  atest("createNewStoragePool reported Unknown returns the pool and nudges its stores' hosts"):
    given ExecutionContext = executionContext
    retryCreates()
    for
      _ <- ready()
      _ = takeNudges()
      // The pool's own transaction: it allocates the pool object, and writes the bootstrap device's store entries
      devPtr <- client.getStorageDevicePointer(StorageDeviceId.BootstrapStorageDeviceId)
      _ = net.reportCommitAsUnknown(txd => allocates(txd) && touches(devPtr)(txd))
      poolId <- client.createNewStoragePool("unknown-pool", Replication(1, 1), None, RocksDBConfig(),
                                            StorageDeviceSetId.BootstrapStorageDeviceSetId, 0L)
      nudges = takeNudges()
      pool <- client.getStoragePoolState(poolId)
    yield
      net.pendingUnknownCommits should be(0)
      pool.name should be("unknown-pool")
      nudges.map(_.deviceId) should be(List(StorageDeviceId.BootstrapStorageDeviceId))

  atest("a host whose executor root insert is reported Unknown still starts its executor"):
    given ExecutionContext = executionContext
    retryCreates()
    val hostId = HostId(new UUID(1, 1)) // the id TestNetwork.newHost gives its first host
    for
      _ <- ready()
      _ <- client.createHost("second-host", "127.0.0.1", 5860, 5861, 5862, hostId)
      hostPtr <- client.getHostPointer(hostId)
      _ = net.reportCommitAsUnknown(touches(hostPtr))
      host = net.newHost()
      _ <- awaitExecutor(host, System.nanoTime() + 10_000_000_000L)
      kvos <- client.read(hostPtr)
    yield
      net.pendingUnknownCommits should be(0)
      kvos.contents.contains(org.aspen_ddp.aspen.common.metadata.HostState.TaskExecutorRootKey) should be(true)

  /** Drives the network until `host`'s task executor is ready, failing past `deadline` rather than hanging */
  private def awaitExecutor(host: org.aspen_ddp.aspen.server.Host, deadline: Long): Future[Unit] =
    given ExecutionContext = executionContext
    // A newHost is not among the hosts net.handleEvents drives
    host.testingOnlyHandleEvents()
    net.handleEvents()
    if host.getTaskExecutor().isCompleted then Future.unit
    else if System.nanoTime() > deadline then Future.failed(new AssertionError("the host's executor never started"))
    else
      val p = scala.concurrent.Promise[Unit]()
      client.backgroundTaskManager.schedule(scala.concurrent.duration.Duration(20, "ms"))(p.success(()))
      p.future.flatMap(_ => awaitExecutor(host, deadline))

  atest("a transfer completion reported Unknown still announces the move"):
    given ExecutionContext = executionContext
    for
      _ <- ready()
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- client.transferStore(storeId, net.secondDeviceId)
      _ <- waitForTransactionsToComplete()
      poolPtr <- client.getStoragePoolPointer(PoolId.BootstrapPoolId)
      _ = takeNudges()
      _ = net.takeCapturedGossip()
      _ = net.reportCommitAsUnknown(touches(poolPtr))
      outcome <- net.smgr.control.metadata.updateStateForTransferredStore(storeId,
                   StorageDeviceId.BootstrapStorageDeviceId, net.secondDeviceId, net.smgr.gossipRetransmitter)
      nudges = takeNudges()
      moves = net.takeCapturedGossip().collect { case m: StoreMoved => m }
    yield
      net.pendingUnknownCommits should be(0)
      outcome should be(TransferOutcome.PoolRepointed)
      moves.map(_.storeId) should be(List(storeId))
      nudges.map(_.deviceId) should be(List(StorageDeviceId.BootstrapStorageDeviceId))

  atest("a rebuild completion reported Unknown still gossips the store online"):
    given ExecutionContext = executionContext
    @volatile var online: List[StoreId] = Nil
    for
      _ <- ready()
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(net.secondDeviceId)(_.setStoreEntry(storeId, StorageDeviceState.StoreStatus.Rebuilding, None))
      devPtr <- client.getStorageDevicePointer(net.secondDeviceId)
      _ = net.reportCommitAsUnknown(touches(devPtr))
      outcome <- net.smgr.control.metadata.markRebuiltStoreActive(net.secondDeviceId, storeId,
                   s => online = s :: online)
      dev <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      net.pendingUnknownCommits should be(0)
      outcome should be(RebuildOutcome.Adopt)
      dev.stores(storeId).status should be(StorageDeviceState.StoreStatus.Active)
      online should be(List(storeId))

  atest("replaceRebuild reported Unknown succeeds and sends its nudges"):
    given ExecutionContext = executionContext
    for
      _ <- ready()
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(StorageDeviceId.BootstrapStorageDeviceId)(
             _.setStoreEntry(storeId, StorageDeviceState.StoreStatus.Rebuilding, None))
      dstPtr <- client.getStorageDevicePointer(net.secondDeviceId)
      _ = takeNudges()
      _ = net.reportCommitAsUnknown(touches(dstPtr))
      _ <- client.replaceRebuild(storeId, net.secondDeviceId)
      nudges = takeNudges()
      pool <- client.getStoragePoolState(PoolId.BootstrapPoolId)
      dst <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      net.pendingUnknownCommits should be(0)
      pool.stores(storeId.poolIndex).storageDeviceId should be(net.secondDeviceId)
      dst.stores(storeId).status should be(StorageDeviceState.StoreStatus.Rebuilding)
      nudges.map(_.deviceId).toSet should be(Set(StorageDeviceId.BootstrapStorageDeviceId, net.secondDeviceId))

  atest("a rebuild request reported Unknown still checks its device at once"):
    given ExecutionContext = executionContext
    // A store of no pool, on a device the host has not loaded: the device check that follows a request marks it
    // offline, which is how the check is observed
    val lost = StoreId(PoolId(UUID.randomUUID()), 0)
    val router = net.smgr.control.router
    for
      _ <- ready()
      _ <- net.createSecondDevice()
      _ <- waitForTransactionsToComplete()
      _ <- setDeviceState(net.secondDeviceId)(_.setStoreEntry(lost, StorageDeviceState.StoreStatus.Active, None))
      devPtr <- client.getStorageDevicePointer(net.secondDeviceId)
      _ = net.reportCommitAsUnknown(touches(devPtr))
      _ = net.smgr.control.rebuilds.requestRebuild(net.secondDeviceId, lost)
      _ <- pollUntil(System.nanoTime() + 10_000_000_000L)(router.offlineStores.contains(lost))
      dev <- client.getStorageDeviceState(net.secondDeviceId)
    yield
      net.pendingUnknownCommits should be(0)
      dev.stores(lost).status should be(StorageDeviceState.StoreStatus.Rebuilding)
      router.offlineStores should contain(lost)

  /** Drives the network until `condition` holds or `deadline` (System.nanoTime) passes, waiting in real time between
    * rounds for work the client schedules on its background timer, such as a retry's backoff */
  private def pollUntil(deadline: Long)(condition: => Boolean): Future[Unit] =
    given ExecutionContext = executionContext
    net.handleEvents()
    if condition || System.nanoTime() > deadline then Future.unit
    else
      val p = scala.concurrent.Promise[Unit]()
      client.backgroundTaskManager.schedule(scala.concurrent.duration.Duration(10, "ms"))(p.success(()))
      p.future.flatMap(_ => waitForTransactionsToComplete()).flatMap(_ => pollUntil(deadline)(condition))
