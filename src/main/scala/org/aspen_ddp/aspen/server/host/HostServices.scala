package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.common.metadata.HostState
import org.aspen_ddp.aspen.common.network.{ExecuteSystemTask, HostEndpointChanged, ServiceMessage, StoreMoved}
import org.aspen_ddp.aspen.common.objects.{Insert, Key, KeyValueObjectPointer}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, KeyRevision}
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager.ScheduledTask
import org.aspen_ddp.aspen.common.util.runSequentially
import org.aspen_ddp.aspen.compute.{DurableServiceExecutor, ServiceEntry, TaskExecutor}
import org.aspen_ddp.aspen.compute.impl.{SimpleDurableServiceExecutor, SimpleTaskExecutor}
import org.aspen_ddp.aspen.compute.systemtask.{SystemTaskExecutorService, SystemTaskRunner}
import org.aspen_ddp.aspen.server.GossipRetransmitter
import org.aspen_ddp.aspen.server.usage.{StorageDeviceUsageManager, StoragePoolUsageManager}
import scribe.Logging

import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

/** Runs only on the control thread. See HostThreading.scala.
 *
 *  The services a host runs for the rest of the system: the task and durable-service executors, the system task
 *  runner, usage tracking, the endpoint reconcile, and the gossip retransmitter that all StoreMoved and
 *  HostEndpointChanged gossip goes through.
 *
 *  The exceptions: the gossip retransmitter is thread-safe, so its periodic retransmit runs on the
 *  background task thread; `shutdown` is called on the caller's thread and touches only what is safe there. */
class HostServices(ctx: DeviceControl) extends Logging:
  import ctx.given

  private val taskExecutorPromise: Promise[TaskExecutor] = Promise()
  // Runs the DurableServices this host claims. Every host scans the services TKVL for unclaimed or expired
  // entries, with its scan interval scaled by the known host count (net.knownHostCount) so the cluster as a whole
  // scans about every 15 seconds however many hosts there are. See the DurableService scaladoc for details.
  private val serviceExecutorPromise: Promise[SimpleDurableServiceExecutor] = Promise()
  private val systemTaskRunnerPromise: Promise[SystemTaskRunner] = Promise()
  private val poolUsageManager = new StoragePoolUsageManager(ctx.client)
  private val deviceUsageManager = new StorageDeviceUsageManager(ctx.client)
  // Written once, on the control thread; @volatile because shutdown reads it on the caller's thread.
  @volatile private var usageUpdateTask: Option[ScheduledTask] = None

  /** All StoreMoved and HostEndpointChanged gossip goes through here, so hosts that miss the
   *  first broadcast still hear it from a retransmit. */
  private[server] val gossipRetransmitter =
    new GossipRetransmitter(GossipRetransmitter.stateFile(ctx.rootDir), ctx.net.broadcastGossip)

  // Runs on the background task thread: the retransmitter is thread-safe and touches no control state.
  private val gossipRetransmitTask = ctx.backgroundTasks.schedulePeriodic(GossipRetransmitter.Period):
    gossipRetransmitter.retransmit()

  def initializeTaskExecutor(): Unit =
    val hostPointerFuture = ctx.client.getHostPointer(ctx.hostId)

    hostPointerFuture.onComplete:
      case Success(hostPtr) =>
        ctx.client.read(hostPtr).foreach: hostKvos =>
          hostKvos.contents.get(HostState.TaskExecutorRootKey) match
            case Some(vs) =>
              val executorPtr = KeyValueObjectPointer(vs.value.bytes)
              ctx.client.getStoragePool(Radicle.poolId).foreach: pool =>
                val allocator = new PoolObjectAllocator(ctx.client, pool)
                SimpleTaskExecutor(ctx.client, allocator, executorPtr).foreach: executor =>
                  executorReady(executor)

            case None =>
              ctx.client.getStoragePool(Radicle.poolId).foreach: pool =>
                val allocator = new PoolObjectAllocator(ctx.client, pool)

                // The transaction's own machinery (commit, retries) runs on ctx.ec, and its result comes back through
                // the control-context foreach; the prepare closure's combinators run on the control thread
                ctx.client.transactUntilSuccessful { tx =>
                  for
                    executorRoot <- allocator.allocateKeyValueObject(Map())(using tx)
                    currentHostKvos <- ctx.client.read(hostPtr)
                  yield
                    val reqs = List(DoesNotExist(HostState.TaskExecutorRootKey))
                    val ops = List(Insert(HostState.TaskExecutorRootKey, executorRoot.toArray))
                    tx.update(hostPtr, None, None, reqs, ops)
                    executorRoot
                }(using ctx.ec)
                .foreach: executorRoot =>
                  SimpleTaskExecutor(ctx.client, allocator, executorRoot).foreach: executor =>
                    executorReady(executor)

      case Failure(err) =>
        // In test environments or when host is not yet registered, silently skip initialization
        logger.debug(s"TaskExecutor initialization skipped: ${err.getMessage}")

  /** The task executor exists: completes the executor promises and starts what needs it. */
  private def executorReady(executor: TaskExecutor): Unit =
    taskExecutorPromise.success(executor)
    startUsageTracking(executor)
    // hostCount is called on the service executor's own threads: the messenger is thread-safe
    val serviceExec = new SimpleDurableServiceExecutor(ctx.client, ctx.hostId, ctx.backgroundTasks,
      hostCount = () => ctx.net.knownHostCount)
    serviceExecutorPromise.success(serviceExec)
    initializeSystemTaskRunner(executor)

  private def initializeSystemTaskRunner(executor: TaskExecutor): Unit =
    val servicesTkvl = TieredKeyValueList(ctx.client,
      KVObjectRootManager(ctx.client, Radicle.ServicesTreeKey, ctx.client.radicle))
    servicesTkvl.get(Key(SystemTaskExecutorService.ServiceUUID)).foreach:
      case Some(vs) =>
        val statePtr = ServiceEntry.decode(vs.value.bytes).statePointer
        val runner = new SystemTaskRunner(ctx.client, statePtr, ctx.hostId, executor)
        if !systemTaskRunnerPromise.isCompleted then
          systemTaskRunnerPromise.success(runner)
      case None =>
        logger.debug("SystemTaskExecutorService not registered; system task runner not started")

  private def startUsageTracking(executor: TaskExecutor): Unit =
    poolUsageManager.setTaskExecutor(executor)

    usageUpdateTask = Some(ctx.backgroundTasks.scheduleNonConcurrentPollingTask(
      "store-usage-update", Duration(20, SECONDS)):
      // Invoked on the background task thread: the body is delegated to the control thread, which owns the stores
      // and devices it reads.
      Future.delegate:
        val slist = ctx.router.loaded.valuesIterator.toList
        val dlist = ctx.devices.devices.valuesIterator.toList
        val fs = runSequentially(slist): store =>
          poolUsageManager.updateStoreSize(store.storeId, store.estimateSize())
        val fd = runSequentially(dlist): sds =>
          deviceUsageManager.updateDeviceUsage(sds.storageDeviceId, sds.currentUsage, sds.totalSize)

        Future.sequence(List(fs, fd))
    )

  def getTaskExecutor(): Future[TaskExecutor] = taskExecutorPromise.future

  def getServiceExecutor(): Future[DurableServiceExecutor] = serviceExecutorPromise.future

  /** Passes a service message to the durable service executor or a system task to the system task runner, once
   *  each exists. */
  def deliver(m: ServiceMessage | ExecuteSystemTask): Unit =
    m match
      case m: ServiceMessage =>
        serviceExecutorPromise.future.foreach(_.deliverMessage(m))
      case m: ExecuteSystemTask =>
        systemTaskRunnerPromise.future.foreach(_.receive(m))

  /** The SystemTaskHeartbeatEvent: ticks the system task runner if it exists. */
  def systemTaskHeartbeat(): Unit =
    if systemTaskRunnerPromise.isCompleted then
      systemTaskRunnerPromise.future.foreach(_.heartbeat())

  /** Makes this host's HostState endpoint match the configured one, gossiping
   *  HostEndpointChanged once the change has committed.
   *
   *  Completes false, with no transaction, when the stored endpoint already matches. Completes
   *  true after a committed update. A failure is logged and retried every 5 seconds in the
   *  background; the returned Future reflects only the first attempt, so callers must not
   *  depend on it to start the host.
   */
  def reconcileEndpoint(address: String,
                        dataPort: Int,
                        httpPort: Int,
                        storeTransferPort: Int): Future[Boolean] =
    def attempt(): Future[Boolean] =
      ctx.metadata.readHostStateForEndpoint().flatMap: hs =>
        if hs.address == address && hs.dataPort == dataPort &&
           hs.httpPort == httpPort && hs.storeTransferPort == storeTransferPort then
          Future.successful(false)
        else
          // The transaction's own machinery (commit, retries) runs on ctx.ec, and its result comes back through
          // the control-context flatMap; the prepare closure's combinators run on the control thread
          ctx.client.transactUntilSuccessful { tx =>
            for
              hostPtr <- ctx.client.getHostPointer(ctx.hostId)
              kvos <- ctx.client.read(hostPtr)
            yield
              val current = HostState(kvos)
              tx.update(hostPtr, None, None,
                List(KeyRevision(HostState.StateKey, kvos.contents(HostState.StateKey).revision)),
                List(Insert(HostState.StateKey,
                  current.copy(address = address, dataPort = dataPort, httpPort = httpPort,
                               storeTransferPort = storeTransferPort).encode())))
              tx
          }(using ctx.ec)
          // The committed transaction's timestamp is the new HostState value's
          .flatMap(_.result)
          .map: timestamp =>
            logger.info(s"Host endpoint updated to $address:$dataPort")
            gossipRetransmitter.broadcast(
              HostEndpointChanged(ctx.hostId, address, dataPort, httpPort, storeTransferPort, timestamp))
            true

    // Each call is one attempt; a failed one logs and schedules another. The result is the
    // attempt it was called for, so the first call's Future reflects only the first attempt.
    def run(): Future[Boolean] =
      val result = attempt()
      result.failed.foreach: err =>
        logger.warn(s"Failed to reconcile host endpoint, retrying in 5 seconds: $err")
        ctx.scheduleEndpointRetry(() => run())
      result

    run()

  /** Gossips StoreMoved for a store this host has brought back online without repointing the
   *  pool: one whose rebuild has completed, or whose abandoned transfer out has been reinstated.
   *
   *  For a rebuild, the pool was repointed when the rebuild was assigned, but peers only learn it
   *  now, as at the end of a transfer. That was a different transaction, so the pool is read for
   *  the host and timestamp its config value carries. The store came online now, before the read,
   *  which is what clears its StoreOffline. Best-effort: a failed read only means peers find the
   *  store the slow way and keep it offline until their next recheck.
   */
  def gossipStoreOnline(storeId: StoreId): Unit =
    val onlineTimestamp = HLCTimestamp.now
    ctx.client.getStoragePoolStateAndTimestamp(storeId.poolId).onComplete:
      case Success((poolState, timestamp)) =>
        val hostId = poolState.stores(storeId.poolIndex).hostId
        gossipRetransmitter.broadcast(StoreMoved(storeId, hostId, timestamp, onlineTimestamp))
      case Failure(err) =>
        logger.warn(s"Failed to read pool ${storeId.poolId} to gossip that store $storeId is online: $err")

  /** Cancels the periodic tasks and shuts down the executors. Called on the caller's thread, once the control thread
   *  has been told to stop, so nothing here may post to it. */
  def shutdown(): Unit =
    // On the host's ec: the control thread drops what is posted after its shutdown. The executors are thread-safe.
    serviceExecutorPromise.future.foreach(_.shutdown())(using ctx.ec)
    systemTaskRunnerPromise.future.foreach(_.shutdown())(using ctx.ec)
    gossipRetransmitTask.cancel()
    usageUpdateTask.foreach(_.cancel())
