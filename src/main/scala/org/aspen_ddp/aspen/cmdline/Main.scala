package org.aspen_ddp.aspen.cmdline

import org.aspen_ddp.aspen.AmoebaError
import org.aspen_ddp.aspen.amoebafs.FileSystem
import org.aspen_ddp.aspen.amoebafs.impl.simple.SimpleFileSystem
import org.aspen_ddp.aspen.amoebafs.nfs.AmoebaNFS
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.internal.SimpleAspenClient
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.KeyValueListNode
import org.aspen_ddp.aspen.client.registries.Registry.DuplicateRegistration
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.metadata.*
import org.aspen_ddp.aspen.common.rebalancing.RebalancingDurableService
import org.aspen_ddp.aspen.common.network.implementations.zmqnet.ZMQNet
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.DoesNotExist
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, YamlFormat}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.server.crl.simple.SimpleCRL
import org.aspen_ddp.aspen.server.store.Bootstrap
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBBackend, RocksDBConfig}
import org.aspen_ddp.aspen.server.store.cache.SimpleLRUObjectCache
import org.aspen_ddp.aspen.server.transaction.SimpleTransactionDriver
import org.aspen_ddp.aspen.server.*
import org.dcache.nfs.ExportFile
import org.dcache.nfs.v3.xdr.{mount_prot, nfs3_prot}
import org.dcache.nfs.v3.{MountServer, NfsServerV3}
import org.dcache.nfs.v4.xdr.nfs4_prot
import org.dcache.nfs.v4.{MDSOperationExecutor, NFSServerV41}
import org.dcache.nfs.vfs.VirtualFileSystem
import org.dcache.oncrpc4j.rpc.{OncRpcProgram, OncRpcSvcBuilder}
import scribe.Logging
import scribe.format.{FormatterInterpolator, classNameSimple, dateFull, line, mdc, messages, methodName}

import java.io.{File, StringReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, HOURS, MILLISECONDS, SECONDS}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.language.implicitConversions


object Main {

  val AmoebafsKey: Key = Key("amoeba")

  case class Args(mode:String="",
                  hostDirectory:File=null,
                  bootstrapConfigFile:File=null,
                  hostName:String="",
                  storeName:String="",
                  host:String="",
                  port:Int=0,
                  newPoolName: String="",
                  idaType: String="",
                  width:Int=0,
                  readThreshold:Int=0,
                  writeThreshold:Int=0,
                  deviceSetName:String="",
                  maximumStoreSize:Long=0L,
                  setId:String="",
                  newSetName:String="",
                  newSetLevel:Int=0,
                  parentSetName:String="",
                  entityRef:String="")

  class ConfigError(msg: String) extends AmoebaError(msg)

  private case class HostNotFound(hostname: String)
    extends Exception(s"host '$hostname' not found")

  private case class EntityNotFound(kind: String, ref: String)
    extends Exception(s"$kind '$ref' not found")

  class NetworkBridge extends MessageHandler with Logging {
    var oclient: Option[AspenClient] = None
    var onode: Option[StoreManager] = None

    def onClientResponseReceived(msg: ClientResponse): Unit =
      //logger.trace(s"**** Recieved ClientResponse: $msg. $oclient")
      oclient.foreach(_.receiveClientResponse(msg))
    def onClientRequestReceived(msg: ClientRequest): Unit =
      onode.foreach(_.receiveClientRequest(msg))
    def onTransactionMessageReceived(msg: TxMessage): Unit =
      onode.foreach(_.receiveTransactionMessage(msg))
    def onHostMessageReceived(msg: HostMessage): Unit =
      onode.foreach(_.receiveHostMessage(msg))
  }

  def configureLogging(): Unit =
    val logFormat = formatter"$dateFull $classNameSimple.$methodName:$line $messages$mdc"
    scribe.Logger.root
      .clearHandlers()
      .withHandler(formatter = logFormat, minimumLevel = Some(scribe.Level.Trace))
      .replace()

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Args]("demo") {
      head("demo", "0.1")

      cmd("bootstrap").text("Bootstrap a new Amoeba system").
        action( (_,c) => c.copy(mode="bootstrap")).
        children(
          arg[String]("<ida-type>").text("IDA type. Must be Replication or Reed-Solomon").
            action((x, c) => c.copy(idaType = x.toLowerCase())).
            validate { x =>
              val xl = x.toLowerCase
              if xl == "replication" || xl == "reed-solomon" then
                success
              else
                failure("IDA type must be Replication or Reed-Solomon")
            },

          arg[Int]("<read-threshold>").text("Minimum number of slices/replicas that must be read to reconstruct an object").
            action((x, c) => c.copy(readThreshold = x)),

          arg[Int]("<write-threshold>").text("Minimum number of slices/replicas that must be written to successfully write an object").
            action((x, c) => c.copy(writeThreshold = x)),

          arg[Int]("<width>").text("Number of hosts holding slices/replicas").
            action((x, c) => c.copy(width = x)),
        )

      cmd("debug").text("Runs debugging code").
        action((_, c) => c.copy(mode = "debug")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Bootstrap Config file does not exist: $x"))
        )

      cmd("host").text("Starts an Amoeba Storage Host").
        action( (_,c) => c.copy(mode="host")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action( (x, c) => c.copy(bootstrapConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Bootstrap Config file does not exist: $x")),

          arg[File]("<hoste-directory>").text("Host Directory").
            action( (x, c) => c.copy(hostDirectory=x)).
            validate( x => if (x.exists()) success else failure(s"Host directory does not exist: $x"))
        )

      cmd("nfs").text("Launches a Amoeba NFS server").
        action( (_,c) => c.copy(mode="amoeba")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action( (x, c) => c.copy(bootstrapConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x"))
        )

      cmd("rebuild").text("Rebuilds a store").
        action( (_,c) => c.copy(mode="rebuild")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action( (x, c) => c.copy(bootstrapConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<store-identifier>").text("Data Store Identifier. Format is \"pool-uuid:storeNumber\"").
            action((x,c) => c.copy(storeName=x)).
            validate { x =>
              val arr = x.split(":")
              if (arr.length == 2) {
                try {
                  Integer.parseInt(arr(1))
                  success
                } catch {
                  case _: Throwable => failure("Store name must match the format \"pool-name:storeNumber\"")
                }
              }
              else failure("Store name must match the format \"pool-name:storeNumber\"")
            }
        )

      cmd("create-pool").text("Creates a new storage pool").
        action((_, c) => c.copy(mode = "create-pool")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<pool-name>").text("Name of the new Pool").
            action((x, c) => c.copy(newPoolName = x)),

          arg[String]("<ida-type>").text("IDA type. Must be Replication or Reed-Solomon").
            action((x, c) => c.copy(idaType = x.toLowerCase())).
            validate { x =>
              val xl = x.toLowerCase
              if xl == "replication" || xl == "reed-solomon" then
                success
              else
                failure("IDA type must be Replication or Reed-Solomon")
            },

          arg[Int]("<width>").text("Total number of slices/replicas").
            action((x, c) => c.copy(width = x)),

          arg[Int]("<read-threshold>").text("Minimum number of slices/replicas that must be read to reconstruct an object").
            action((x, c) => c.copy(readThreshold = x)),

          arg[Int]("<write-threshold>").text("Minimum number of slices/replicas that must be written to successfully write an object").
            action((x, c) => c.copy(writeThreshold = x)),

          arg[String]("<device-set-name>").text("Name of the storage device set that will host the pool's stores").
            action((x, c) => c.copy(deviceSetName = x)),

          arg[Long]("<maximum-store-size>").optional().text("Maximum per-store size in bytes (0 = default/unbounded)").
            action((x, c) => c.copy(maximumStoreSize = x)),
        )

      cmd("create-device-set").text("Creates a new storage device set").
        action((_, c) => c.copy(mode = "create-device-set")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name>").text("Name of the new storage device set").
            action((x, c) => c.copy(newSetName = x)),

          arg[Int]("<level>").text("Hierarchy level (0 = set of physical devices, 1+ = set of sets)").
            action((x, c) => c.copy(newSetLevel = x)).
            validate(x => if (x >= 0) success else failure("Level must be >= 0")),

          arg[String]("[parent-set-name]").optional().text("Optional name of the parent device set to link into").
            action((x, c) => c.copy(parentSetName = x)),
        )

      cmd("transfer-store").text("Transfers a store to a different storage device").
        action((_, c) => c.copy(mode = "transfer-store")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<store-identifier>").text("Data Store Identifier. Format is \"pool-uuid:storeNumber\"").
            action((x, c) => c.copy(storeName = x)).
            validate { x =>
              val arr = x.split(":")
              if (arr.length == 2) {
                try {
                  UUID.fromString(arr(0))
                  Integer.parseInt(arr(1))
                  success
                } catch {
                  case _: Throwable => failure("Store name must match the format \"pool-uuid:storeNumber\"")
                }
              }
              else failure("Store name must match the format \"pool-uuid:storeNumber\"")
            },
          arg[String]("<target-storage-device-id>").text("UUID of the storage device to receive the store").
            action((x, c) => c.copy(host = x)).
            validate { x =>
              try
                UUID.fromString(x)
                success
              catch
                case _: Throwable => failure("Target storage device id must be a valid UUID")
            },
        )

      cmd("rebalance").text("Rebalances a level-0 storage device set").
        action((_, c) => c.copy(mode = "rebalance")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<storage-device-set-id>").text("UUID of the storage device set to rebalance").
            action((x, c) => c.copy(setId = x)).
            validate { x =>
              try
                UUID.fromString(x)
                success
              catch
                case _: Throwable => failure("Storage device set id must be a valid UUID")
            },
        )

      cmd("list-pools").text("Lists all storage pools").
        action((_, c) => c.copy(mode = "list-pools")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
        )

      cmd("list-hosts").text("Lists all hosts").
        action((_, c) => c.copy(mode = "list-hosts")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
        )

      cmd("list-allocation-groups").text("Lists all allocation groups").
        action((_, c) => c.copy(mode = "list-allocation-groups")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
        )

      cmd("list-device-sets").text("Lists all storage device sets").
        action((_, c) => c.copy(mode = "list-device-sets")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),
        )

      cmd("list-devices").text("Lists all storage devices for a host").
        action((_, c) => c.copy(mode = "list-devices")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<hostname>").text("Name of the host whose devices to list").
            action((x, c) => c.copy(hostName = x)),
        )

      cmd("show-host").text("Displays the full state of a host").
        action((_, c) => c.copy(mode = "show-host")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name-or-uuid>").text("Host name or UUID").
            action((x, c) => c.copy(entityRef = x)),
        )

      cmd("show-device").text("Displays the full state of a storage device").
        action((_, c) => c.copy(mode = "show-device")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<uuid>").text("Storage device UUID").
            action((x, c) => c.copy(entityRef = x)).
            validate { x =>
              try
                UUID.fromString(x)
                success
              catch
                case _: Throwable => failure("Storage device id must be a valid UUID")
            },
        )

      cmd("show-pool").text("Displays the full state of a storage pool").
        action((_, c) => c.copy(mode = "show-pool")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name-or-uuid>").text("Pool name or UUID").
            action((x, c) => c.copy(entityRef = x)),
        )

      cmd("show-device-set").text("Displays the full state of a storage device set").
        action((_, c) => c.copy(mode = "show-device-set")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name-or-uuid>").text("Device set name or UUID").
            action((x, c) => c.copy(entityRef = x)),
        )

      cmd("show-allocation-group").text("Displays the full state of an allocation group").
        action((_, c) => c.copy(mode = "show-allocation-group")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name-or-uuid>").text("Allocation group name or UUID").
            action((x, c) => c.copy(entityRef = x)),
        )

      checkConfig( c => if (c.mode == "") failure("Invalid command") else success )
    }

    parser.parse(args, Args()) match
      case Some(cfg) =>
        def bootstrapConfig: BootstrapConfig.Config =
          println(s"Loading BootstrapConfig ${cfg.bootstrapConfigFile}")
          BootstrapConfig.loadBootstrapConfig(cfg.bootstrapConfigFile)

        def bootstrapConfigPath: os.Path =
          os.Path(cfg.bootstrapConfigFile.toPath)

        try
          //println(s"Config file: $config")
          cfg.mode match
            case "bootstrap" => bootstrap(createIDA(cfg), Paths.get("demo"), 4750, 4751, 4752)
            case "host" => host(bootstrapConfig, bootstrapConfigPath, cfg.hostDirectory.toPath)
            case "amoeba" => amoeba_server(bootstrapConfigPath)
            case "debug" => run_debug_code(bootstrapConfigPath)
            case "rebuild" => rebuild(cfg.storeName, bootstrapConfigPath)
            case "create-pool" => create_pool(bootstrapConfigPath, cfg.newPoolName, createIDA(cfg), cfg.deviceSetName, cfg.maximumStoreSize)
            case "create-device-set" => create_device_set(bootstrapConfigPath, cfg.newSetName, cfg.newSetLevel, cfg.parentSetName)
            case "transfer-store" => transfer_store(bootstrapConfigPath, cfg.storeName, cfg.host)
            case "rebalance" => rebalance(bootstrapConfigPath, cfg.setId)
            case "list-pools"             => list_entries(bootstrapConfigPath, "Storage Pools",     _.listStoragePools(),      _.uuid)
            case "list-hosts"             => list_entries(bootstrapConfigPath, "Hosts",             _.listHosts(),             _.uuid)
            case "list-allocation-groups" => list_entries(bootstrapConfigPath, "Allocation Groups", _.listAllocationGroups(),  _.uuid)
            case "list-device-sets"       => list_entries(bootstrapConfigPath, "Device Sets",       _.listStorageDeviceSets(), _.uuid)
            case "list-devices"           => list_devices(bootstrapConfigPath, cfg.hostName)
            case "show-host"              => show_host(bootstrapConfigPath, cfg.entityRef)
            case "show-device"            => show_device(bootstrapConfigPath, cfg.entityRef)
            case "show-pool"              => show_pool(bootstrapConfigPath, cfg.entityRef)
            case "show-device-set"        => show_device_set(bootstrapConfigPath, cfg.entityRef)
            case "show-allocation-group"  => show_allocation_group(bootstrapConfigPath, cfg.entityRef)
        catch
          case e: YamlFormat.FormatError => println(s"Error loading config file: $e")
          case e: ConfigError => println(s"Error: $e")
      case None =>
  }

  def createIDA(args: Args): IDA =
    require(args.width >= args.readThreshold && args.width >= args.writeThreshold)
    require(args.readThreshold <= args.writeThreshold)
    args.idaType match
      case "replication" => Replication(args.width, args.writeThreshold)
      case "reed-solomon" => ReedSolomon(args.width, args.readThreshold, args.writeThreshold)
      case _ => throw new Exception(s"Invalid IDA type: ${args.idaType}")

  def createNetwork(bootstrapConfigFile: os.Path,
                    ohostNode: Option[(HostId, Int)],
                    oclientId: Option[ClientId]): (NetworkBridge, ZMQNet) = {
    val b = new NetworkBridge

    val heartbeatPeriod = Duration(10, SECONDS)
    (b, new ZMQNet(bootstrapConfigFile, oclientId, ohostNode, heartbeatPeriod, b))
  }

  def createAmoebaClient(bootstrapConfigFile: os.Path,
                         onnet: Option[(NetworkBridge, ZMQNet)]=None): (AspenClient, ZMQNet, KeyValueObjectPointer) = {

    val (networkBridge, nnet) = onnet.getOrElse(createNetwork(bootstrapConfigFile, None, None))

    val txStatusCacheDuration = Duration(10, SECONDS)
    val initialReadDelay = Duration(10, SECONDS)
    val maxReadDelay = Duration(6, SECONDS)
    val txRetransmitDelay = Duration(1, SECONDS)
    val allocationRetransmitDelay = Duration(5, SECONDS)

    val sched = Executors.newScheduledThreadPool(3)
    val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)

    val ret = (new SimpleAspenClient(nnet.clientMessenger, nnet.clientId, ec, Radicle.pointer,
      txStatusCacheDuration,
      initialReadDelay,
      maxReadDelay,
      txRetransmitDelay,
      allocationRetransmitDelay),  nnet, Radicle.pointer)

    networkBridge.oclient = Some(ret._1)

    ret
  }

  def initializeAmoeba(client: AspenClient,
                       radicle: KeyValueObjectPointer,
                       numIndexNodeSegments: Int = 100,
                       fileSegmentSize:Int=1024*1024): Future[FileSystem] = {

    given ExecutionContext = client.clientContext

    def loadFileSystem(kvos: KeyValueObjectState): Future[FileSystem] = kvos.contents.get(AmoebafsKey) match {
      case Some(arr) =>
        println("Amoeba already created")
        SimpleFileSystem.load(client, KeyValueObjectPointer(arr.value.bytes), 3)

      case None =>
        println("Creating Amoeba")
        client.getStoragePool(kvos.pointer.poolId).flatMap { pool =>
          val allocator = new PoolObjectAllocator(client, pool)
          SimpleFileSystem.bootstrap(client, allocator, kvos.pointer, AmoebafsKey)
        }
    }

    client.read(radicle).flatMap(loadFileSystem)
  }

  def run_debug_code(bootstrapConfigFile: os.Path): Unit = {
    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    def randomContent: Array[Byte] =
      val arr = new Array[Byte](16)
      val r = UUID.randomUUID()
      val bb = ByteBuffer.wrap(arr)
      bb.order(ByteOrder.BIG_ENDIAN)
      bb.putLong(r.getMostSignificantBits)
      bb.putLong(r.getLeastSignificantBits)
      arr

    def allocObject(ovalue: Option[ValueState],
                    kvos: KeyValueObjectState,
                    alloc: ObjectAllocator): Future[DataObjectPointer] = ovalue match
      case Some(v) =>
        println("------------- Using existing object -------------")
        Future.successful(ObjectPointer(v.value.bytes).asInstanceOf[DataObjectPointer])
      case None =>
        println("------------- Allocating new Object ------------")
        val tx = client.newTransaction()
        val key = Key(100)
        for
          ptr <- alloc.allocateDataObject(randomContent)(using tx)
          _ = tx.update(kvos.pointer, None, None, DoesNotExist(key) :: Nil, Insert(key, ptr.toArray) :: Nil)
          _ <- tx.commit()
        yield
            ptr


    println("------------ Reading Radicle ---------------")
    for
      kvos <- client.read(radicle)
      _ = println("------------ Getting Storage Pool---------------")
      pool <- client.getStoragePool(kvos.pointer.poolId)
      alloc = pool.allocator
      _ = println("------------ Allocating Data Object ---------------")
      key = Key(100)
      dptr <- allocObject(kvos.contents.get(key), kvos, alloc)

      _ = println("------------ Reading Object r---------------")
      os <- client.read(dptr)

      tx = client.newTransaction()
      _ = tx.overwrite(dptr, os.revision, DataBuffer(randomContent))
      _ = println("------------ Committing random update ---------------")
      _ <- tx.commit()
    yield
      ()
  }

  def amoeba_server(bootstrapConfigFile: os.Path): Unit = {
    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    val f = initializeAmoeba(client, radicle)

    val fs = Await.result(f, Duration(10000, MILLISECONDS))

    val exports = "/ 192.168.64.2(rw)\n"

    val sched = Executors.newScheduledThreadPool(10)
    val ec = ExecutionContext.fromExecutorService(sched)

    val vfs: VirtualFileSystem = new AmoebaNFS(fs, ec)

    val nfsSvc = new OncRpcSvcBuilder().
      withPort(2049).
      withTCP.
      withAutoPublish.
      withWorkerThreadIoStrategy.
      build

    val exportFile = new ExportFile(new StringReader(exports))

    val nfs4 = new NFSServerV41.Builder().
      withExportTable(exportFile).
      withVfs(vfs).
      //withOperationFactory(new MDSOperationFactory).
      withOperationExecutor(new MDSOperationExecutor).
      build

    val nfs3 = new NfsServerV3(exportFile, vfs)
    val mountd = new MountServer(exportFile, vfs)

    //val portmapSvc = new OncRpcEmbeddedPortmap()

    nfsSvc.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), mountd)
    nfsSvc.register(new OncRpcProgram(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3), nfs3)
    nfsSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4)
    nfsSvc.start()

    println("Amoeba NFS server started...")

    Thread.currentThread.join()
  }


  def repair(client: AspenClient, storeManager: StoreManager): Unit =

    def deleteErrorEntry(node: KeyValueListNode, key: Key): Future[Unit] =
      val tx = client.newTransaction()
      val fdelete = node.delete(key)(using tx)
      for
        _ <- fdelete
        _ <- tx.commit()
      yield ()

    def deleteErrorEntryByTimestamp(timestamp: HLCTimestamp,
                                    node: KeyValueListNode,
                                    key: Key): Future[Unit] =
      val tx = client.newTransaction()
      val fdeletePrep = node.delete(key,
        None,
        List(KeyValueUpdate.TimestampLessThan(key, timestamp)),
        (_,_) => Future.unit)(using tx)
      for
        _ <- fdeletePrep
        _ <- tx.commit()
      yield ()

    def step2(pool: StoragePool, storeId: StoreId, ptr: ObjectPointer,
              node: KeyValueListNode, key: Key): Future[Unit] =
      val fos = ptr match
        case kp: KeyValueObjectPointer => client.read(kp)
        case dp: DataObjectPointer => client.read(dp)
      val frepair = Promise[Unit]()
      for
        os <- fos
        _ = storeManager.repair(storeId, os, frepair)
        _ <- frepair.future
        _ <- deleteErrorEntryByTimestamp(os.timestamp, node, key)
      yield
        println(s"**** REPAIR Complete: ${ptr.id}")
        ()

    def step1(ovalue: Option[ValueState], pool: StoragePool, storeId: StoreId,
              node: KeyValueListNode, key: Key): Future[Unit] = ovalue match
      case None =>
        // No object found in the allocation tree. It must have been deleted. Remove error tree entry
        deleteErrorEntry(node, key)
      case Some(value) => step2(pool, storeId, ObjectPointer(value.value.bytes), node, key)

    def repairOne(pool: StoragePool, storeId: StoreId)(node: KeyValueListNode,
                                                       key: Key, value: ValueState): Future[Unit] =
      val bb = ByteBuffer.wrap(key.bytes)
      bb.order(ByteOrder.BIG_ENDIAN)
      bb.get() // storeIndex
      val msb = bb.getLong()
      val lsb = bb.getLong()
      val objectId = ObjectId(new UUID(msb, lsb))
      println(s"**** REPAIRING Object: ${objectId}")
      for
        ovalue <- pool.allocationTree.get(Key(objectId.toBytes))
        _ <- step1(ovalue, pool, storeId, node, key)
      yield
        ()

    println(s"*** Beginning Repair Process ***")
    storeManager.getStoreIds.foreach: storeId =>
      val min = Array[Byte](1)
      val max = Array[Byte](1)
      min(0) = storeId.poolIndex
      max(0) = (storeId.poolIndex + 1).toByte
      for
        pool <- client.getStoragePool(storeId.poolId)
        _ <- pool.errorTree.foreachInRange(Key(min), Key(max), repairOne(pool, storeId))
      yield
        println(s"*** Repair Process Complete for Store ${storeId} ***")
        Future {
          Thread.sleep(30000)
          repair(client, storeManager)
        }


  def host(bootstrapCfg: BootstrapConfig.Config,
           bootstrapConfigFile: os.Path,
           hostDir: Path): Unit = {

    val sched = Executors.newScheduledThreadPool(3)
    val ec = ExecutionContext.fromExecutorService(sched)
    given ExecutionContext = ec

    val cfgFile = hostDir.resolve("aspen-host-config.yaml")

    if ! Files.exists(cfgFile) then
      throw Exception(s"Host config file not found: $cfgFile")

    val hostCfg = HostConfig.loadHostConfig(cfgFile.toFile)
    configureLogging()

    val simpleCrl = hostCfg.crl match {
      case b: HostConfig.SimpleCRL =>
        val crlRoot = hostDir.resolve("crl")
        if ! Files.exists(crlRoot) then
          mkdirectory(crlRoot)
        SimpleCRL.Factory(crlRoot, b.numStreams, b.fileSizeMb * 1024 * 1024)
    }

    val objectCacheFactory = () => new SimpleLRUObjectCache(100)

    val (networkBridge, nnet) = createNetwork(bootstrapConfigFile, Some((hostCfg.hostId, hostCfg.dataPort)), None)

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile, Some((networkBridge, nnet)))

    networkBridge.oclient = Some(client)

    val txFinalizerFactory = new RegisteredTransactionFinalizerFactory(client)
    val txHeartbeatPeriod = Duration(5, SECONDS)
    val txRetryDelay = Duration(100, MILLISECONDS) //
    val txRetryCap = Duration(3, SECONDS)
    val checkStorageDevicesPeriod = Duration(1, HOURS)
    //val allocHeartbeatPeriod   = Duration(3, SECONDS)
    //val allocTimeout           = Duration(4, SECONDS)
    //val allocStatusQueryPeriod = Duration(1, SECONDS)

    val nodeNet = nnet.serverMessenger

    val storeManager = new StoreManager(
      client,
      hostCfg.hostId,
      bootstrapCfg.aspenSystemId,
      hostDir,
      ec,
      objectCacheFactory,
      nodeNet,
      new BackgroundTaskManager(ec),
      simpleCrl,
      txFinalizerFactory,
      SimpleTransactionDriver.factory(txRetryDelay, txRetryCap),
      txHeartbeatPeriod,
      checkStorageDevicesPeriod
    ) with SimpleDriverRecoveryMixin

    networkBridge.onode = Some(storeManager)

    network.startIoThread(client)
    
    storeManager.start()

    /*val cncBackend = new ZCnCBackend(
      nnet,
      hostCfg.cncPort,
      CnCMessageReceiver.Unhandled)*/

    network.joinIoThread()
  }

  def mkdirectory(p: Path): Unit = {
    Files.createDirectories(p)
  }

  def bootstrap(bootstrapIda: IDA,
                baseDirectory: Path, // "demo" directory
                dataPort: Int,
                cncPort: Int,
                storeTransferPort: Int): Unit = {

    val hostDirectory = baseDirectory.resolve("bootstrap-host")

    if Files.exists(hostDirectory) then
      throw new Exception(s"Bootstrap host directory exists: $hostDirectory")

    val sched = Executors.newScheduledThreadPool(1)
    val ec = ExecutionContext.fromExecutorService(sched)
    given ExecutionContext = ec

    val storageDevicesDir = hostDirectory.resolve("storage-devices")
    val bootstrapDevDir = storageDevicesDir.resolve("bootstrap-device")

    Files.createDirectories(hostDirectory)
    Files.createDirectories(storageDevicesDir)
    Files.createDirectories(bootstrapDevDir)

    val aspenSystemId = UUID.randomUUID()

    val hostConfig = HostConfig(
      HostId(UUID.randomUUID()),
      aspenSystemId,
      "bootstrap-host",
      "127.0.0.1",
      dataPort,
      cncPort,
      storeTransferPort,
      HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300)
    )

    Files.write(
      hostDirectory.resolve(HostConfig.configFilename),
      hostConfig.yamlConfig.getBytes(StandardCharsets.UTF_8)
    )

    val storageDevConfig = StorageDeviceConfig(StorageDeviceId(UUID.randomUUID()), aspenSystemId)

    Files.write(
      bootstrapDevDir.resolve(StorageDeviceConfig.configFilename),
      storageDevConfig.yamlConfig.getBytes(StandardCharsets.UTF_8)
    )

    val bootstrapStores =
      for
        poolIndex <- 0 until bootstrapIda.width
      yield

        val storeId = StoreId(PoolId.BootstrapPoolId, poolIndex.toByte)
        val storeRoot = bootstrapDevDir.resolve(storeId.toString)
        val storeConfig = StoreConfig(storeId, StoreConfig.RocksDB())

        println(s"Creating data store $storeId. Path $storeRoot")
        mkdirectory(storeRoot)
        Files.write(
          storeRoot.resolve(StoreConfig.configFilename),
          storeConfig.yamlConfig.getBytes(StandardCharsets.UTF_8)
        )
        new RocksDBBackend(storeRoot, storeId, ec)

    val bootstrapStorageDevice = StorageDeviceState(
      storageDevConfig.storageDeviceId,
      hostConfig.hostId,
      0L,
      0L,
      bootstrapStores.map(backend =>
        backend.storeId -> StorageDeviceState.StoreEntry(StorageDeviceState.StoreStatus.Active, None)
      ).toMap,
      StorageDeviceSetId.BootstrapStorageDeviceSetId
    )

    val bootstrapHost = HostState(
      hostConfig.hostId,
      "bootstrap-host",
      "127.0.0.1",
      hostConfig.dataPort,
      hostConfig.cncPort,
      hostConfig.storeTransferPort,
      Set(storageDevConfig.storageDeviceId)
    )

    val radicle = Bootstrap.initialize(
      aspenSystemId,
      bootstrapIda,
      bootstrapHost,
      bootstrapStorageDevice,
      bootstrapStores.toList)

    // Print yaml representation of Radicle Pointer
    println("# Aspen Radicle Pointer Definition")
    println("radicle:")
    println(s"    uuid:      ${radicle.id}")
    println(s"    pool-uuid: ${radicle.poolId}")
    sched.shutdownNow()
  }

  def rebuild(storeName: String, bootstrapConfigFile: os.Path): Unit = {

    configureLogging()

    val cfg = BootstrapConfig.loadBootstrapConfig(bootstrapConfigFile.toNIO.toFile)
    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)
    
    given ExecutionContext = client.clientContext

    val arr = storeName.split(":")
    val poolUuid = UUID.fromString(arr(0))
    val storeIndex = Integer.parseInt(arr(1))

    var store: Backend = null
    var storeId: StoreId = null

    cfg.hosts.zipWithIndex.foreach: (node, index) =>
      Path.of(s"demo/bootstrap-host/storage-devices/bootstrap-device").toFile.listFiles.toList.foreach: storeFn =>
        val cfg = StoreConfig.loadStoreConfig(storeFn.toPath.resolve("store-config.yaml").toFile)
        if poolUuid == cfg.storeId.poolId && storeIndex == cfg.storeId.poolIndex then
          cfg.backend match {
            case b: StoreConfig.RocksDB =>
              println(s"Rebuilding data store ${cfg.storeId}. Path $storeFn")
              storeId = cfg.storeId
              store = new RocksDBBackend(storeFn.toPath, cfg.storeId, client.clientContext)
          }

    assert(store != null)

    def rebuildObject(node:KeyValueListNode, key: Key, value: ValueState): Future[Unit] =
      def getMetadata(os: ObjectState): (ObjectType.Value, Metadata) = os match
        case kvos: KeyValueObjectState =>
          (ObjectType.KeyValue, Metadata(kvos.revision, kvos.refcount, kvos.timestamp))
        case dos: DataObjectState =>
          (ObjectType.Data, Metadata(dos.revision, dos.refcount, dos.timestamp))
        case _: MetadataObjectState =>
          assert(false, "Unsupported object type!")

      val objectId = ObjectId(key.bytes)
      val ptr = ObjectPointer(value.value.bytes)

      println(f"Rebuilding object: $objectId")

      if ptr.poolId != storeId.poolId then
        return Future.unit

      val fos = ptr match
        case p: KeyValueObjectPointer => client.read(p)
        case p: DataObjectPointer => client.read(p)

      for
        os <- fos
        (objectType, metadata) = getMetadata(os)
        localData = os.getRebuildDataForStore(storeId)
        _ = store.rebuildWrite(os.id, objectType, metadata, localData.getOrElse(DataBuffer()))
      yield
        println(f"Rebuilt object ${os.id}")

    for
      pool <- client.getStoragePool(storeId.poolId)
      allocTree = pool.allocationTree
      _ <- allocTree.foreach(rebuildObject)
    yield
      store.rebuildFlush()
      println("**** Rebuild Complete ****")
      ()
  }

  def create_pool(bootstrapConfigFile: os.Path,
                  poolName: String,
                  ida: IDA,
                  deviceSetName: String,
                  maximumStoreSize: Long): Unit = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = for
      setId <- client.getStorageDeviceSetId(deviceSetName)
      poolId <- client.createNewStoragePool(poolName, ida, None, RocksDBConfig(), setId, maximumStoreSize)
    yield poolId

    val poolId = Await.result(f, Duration(30, SECONDS))

    println("******************************************")
    println(s"* New Pool Created: ${poolId.uuid}")
    println("******************************************")
  }

  def create_device_set(bootstrapConfigFile: os.Path,
                        name: String,
                        level: Int,
                        parentSetName: String): Unit = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    // Resolve the optional parent set name to an id before creating the new set. An empty
    // parentSetName means "no parent" (a top-level set).
    val fParent: Future[Option[StorageDeviceSetId]] =
      if parentSetName.isEmpty then
        Future.successful(None)
      else
        client.getStorageDeviceSetId(parentSetName).map(Some(_))

    val f = for
      parentOpt <- fParent
      setId <- client.createStorageDeviceSet(name, level, parentOpt)
    yield setId

    // Translate the known failure modes into human-readable messages. The client's retry
    // strategy unwraps StopRetrying, so the future fails with the underlying cause.
    def reportError(cause: Throwable): Unit = cause match
      case _: DuplicateRegistration =>
        println(s"Error: a device set named '$name' already exists")
      case _: NoSuchElementException =>
        println(s"Error: parent device set '$parentSetName' not found")
      case e: AspenClient.InvalidDeviceSetLevel =>
        println(s"Error: ${e.getMessage}")
      case e =>
        println(s"Error creating device set: ${e.getMessage}")

    f.onComplete:
      case scala.util.Success(setId) =>
        println("******************************************")
        println(s"* New Device Set Created: ${setId.uuid}")
        println("******************************************")
      case scala.util.Failure(err) => reportError(err)

    Await.ready(f, Duration(30, SECONDS))
  }

  def transfer_store(bootstrapConfigFile: os.Path,
                     storeName: String,
                     targetDeviceIdStr: String): Unit = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val storeId = StoreId(storeName)
    val targetDeviceId = StorageDeviceId(UUID.fromString(targetDeviceIdStr))

    // Validate the pool and target device exist before attempting the transfer. Each
    // lookup is recovered individually so we can report precisely which argument is
    // invalid rather than failing with an opaque error.
    def lookupPoolState: Future[Either[String, StoragePoolState]] =
      client.getStoragePoolState(storeId.poolId)
        .map(Right(_))
        .recover:
          case _: Throwable => Left(f"Storage pool ${storeId.poolId.uuid} not found")

    def lookupTargetDevice: Future[Either[String, StorageDeviceState]] =
      client.getStorageDeviceState(targetDeviceId)
        .map(Right(_))
        .recover:
          case _: Throwable => Left(f"Target storage device ${targetDeviceIdStr} not found")

    // transferStore atomically marks the store as transferring-out on its current
    // device and transferring-in on the target device, then nudges the destination
    // host with a CheckStorageDevice message so it begins the transfer immediately
    // rather than waiting for its next device-state poll.
    def initiateTransfer(): Unit =
      val f = client.transferStore(storeId, targetDeviceId)

      f.foreach: _ =>
        println(f"Store Transfer Initiated: Store: ${storeName} -> Device: ${targetDeviceIdStr}")

      f.failed.foreach: err =>
        println(f"Store Transfer Failed: ${err.getMessage}")

    for
      ePoolState <- lookupPoolState
      eDeviceState <- lookupTargetDevice
    yield
      (ePoolState, eDeviceState) match
        case (Left(msg), _) => println(f"Store Transfer Failed: $msg")
        case (_, Left(msg)) => println(f"Store Transfer Failed: $msg")
        case (Right(poolState), Right(_)) =>
          if storeId.poolIndex < 0 || storeId.poolIndex >= poolState.stores.length then
            println(f"Store Transfer Failed: Invalid store index ${storeId.poolIndex} for pool " +
                    f"${storeId.poolId.uuid} (pool has ${poolState.stores.length} stores)")
          else
            val sourceDeviceId = poolState.stores(storeId.poolIndex).storageDeviceId
            if sourceDeviceId == targetDeviceId then
              println(f"Store Transfer Failed: Source and destination devices are the same " +
                      f"(${targetDeviceIdStr}); nothing to transfer")
            else
              initiateTransfer()
  }

  def rebalance(bootstrapConfigFile: os.Path, setIdStr: String): Unit =
    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val setId = StorageDeviceSetId(UUID.fromString(setIdStr))

    val f = RebalancingDurableService.rebalanceStorageDeviceSet(client, setId)
    f.onComplete:
      case scala.util.Success(_) =>
        println(s"Rebalance enrolled for storage device set $setIdStr")
      case scala.util.Failure(err) =>
        println(s"Rebalance failed to enroll: ${err.getMessage}")

    scala.concurrent.Await.ready(f, scala.concurrent.duration.Duration(30, scala.concurrent.duration.SECONDS))

  /** Resolve a user-supplied entity reference that may be either a UUID or a name.
   *  If `ref` parses as a UUID it is wrapped via `byUuid`; otherwise it is looked up
   *  by name via `byName`. */
  private[cmdline] def resolveRef[A](ref: String,
                                     byUuid: UUID => A,
                                     byName: String => Future[A]): Future[A] =
    try
      val uuid = UUID.fromString(ref)
      Future.successful(byUuid(uuid))
    catch
      case _: IllegalArgumentException => byName(ref)

  /** Resolve a related entity's display name, best-effort. Any failure (missing
   *  reference, read error) yields None so a `show` command still succeeds using the
   *  raw UUID as a fallback. */
  private def optName[A](f: Future[A])(name: A => String)
                        (using ExecutionContext): Future[Option[String]] =
    f.map(a => Some(name(a))).recover { case _ => None }

  private[cmdline] def formatHostState(s: HostState): String =
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Host: ${s.name}"
    lines += s"  UUID:                ${s.hostId.uuid}"
    lines += s"  Address:             ${s.address}"
    lines += s"  Data Port:           ${s.dataPort}"
    lines += s"  CnC Port:            ${s.cncPort}"
    lines += s"  Store Transfer Port: ${s.storeTransferPort}"
    if s.storageDevices.isEmpty then
      lines += "  Storage Devices:     none"
    else
      lines += "  Storage Devices:"
      s.storageDevices.toList.map(_.uuid.toString).sorted.foreach: d =>
        lines += s"    $d"
    lines.mkString("\n")

  /** Format a byte count using binary units (powers of 1024). Sub-KiB values are
   *  rendered as whole bytes; larger values use one decimal place and the largest
   *  unit that keeps the value >= 1.0. */
  private[cmdline] def formatBytes(n: Long): String =
    val units = Array("KiB", "MiB", "GiB", "TiB", "PiB")
    if n < 1024L then
      s"$n B"
    else
      var value = n.toDouble / 1024.0
      var idx = 0
      while value >= 1024.0 && idx < units.length - 1 do
        value /= 1024.0
        idx += 1
      f"$value%.1f ${units(idx)}"

  private[cmdline] def formatDeviceState(s: StorageDeviceState,
                                         hostName: Option[String],
                                         setName: Option[String]): String =
    val host = hostName.getOrElse(s.hostId.uuid.toString)
    val set  = setName.getOrElse(s.storageDeviceSet.uuid.toString)
    val pct  = if s.totalSize > 0 then s.currentUsage.toDouble / s.totalSize * 100.0 else 0.0
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Storage Device: ${s.storageDeviceId.uuid}"
    lines += s"  Host:       $host (${s.hostId.uuid})"
    lines += s"  Device Set: $set (${s.storageDeviceSet.uuid})"
    lines += f"  Usage:      ${formatBytes(s.currentUsage)} / ${formatBytes(s.totalSize)} ($pct%.1f%%)"
    if s.stores.isEmpty then
      lines += "  Stores:     none"
    else
      lines += "  Stores:"
      s.stores.toList.sortBy(_._1.toString).foreach: (storeId, entry) =>
        val xfer = entry.transferDevice.map(d => s" -> ${d.uuid}").getOrElse("")
        lines += s"    $storeId  ${entry.status}$xfer"
    lines.mkString("\n")

  private[cmdline] def formatPoolState(s: StoragePoolState, setName: Option[String]): String =
    val set = setName.getOrElse(s.storageDeviceSet.uuid.toString)
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Pool: ${s.name}"
    lines += s"  UUID:         ${s.poolId.uuid}"
    lines += s"  IDA:          ${s.ida}"
    lines += s"  Max Obj Size: ${s.maxObjectSize.map(_.toString).getOrElse("unbounded")}"
    lines += s"  Device Set:   $set (${s.storageDeviceSet.uuid})"
    lines += s"  Usage:        ${formatBytes(s.currentUsage)}"
    lines += s"  Max Store Sz: ${if s.maximumStoreSize == 0 then "unbounded" else formatBytes(s.maximumStoreSize)}"
    if s.allocationGroups.isEmpty then
      lines += "  Alloc Groups: none"
    else
      lines += "  Alloc Groups:"
      s.allocationGroups.foreach: g =>
        lines += s"    $g"
    if s.stores.isEmpty then
      lines += "  Stores:       none"
    else
      lines += "  Stores:"
      s.stores.zipWithIndex.foreach: (entry, i) =>
        lines += s"    [$i] host ${entry.hostId.uuid}  device ${entry.storageDeviceId.uuid}"
    lines.mkString("\n")

  private[cmdline] def formatDeviceSetState(s: StorageDeviceSetState,
                                            parentName: Option[String]): String =
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Device Set: ${s.name}"
    lines += s"  UUID:     ${s.setId.uuid}"
    lines += s"  Level:    ${s.level}"
    val parent = s.parent match
      case None    => "none"
      case Some(p) => s"${parentName.getOrElse(p.uuid.toString)} (${p.uuid})"
    lines += s"  Parent:   $parent"
    if s.memberDevices.isEmpty then
      lines += "  Member Devices: none"
    else
      lines += "  Member Devices:"
      s.memberDevices.foreach(d => lines += s"    ${d.uuid}")
    if s.memberSets.isEmpty then
      lines += "  Member Sets:    none"
    else
      lines += "  Member Sets:"
      s.memberSets.foreach(m => lines += s"    ${m.uuid}")
    if s.assignedPools.isEmpty then
      lines += "  Assigned Pools: none"
    else
      lines += "  Assigned Pools:"
      s.assignedPools.foreach(p => lines += s"    ${p.uuid}")
    lines += s"  Pending Transfers: ${s.pendingTransfers.length}"
    lines.mkString("\n")

  private[cmdline] def formatAllocationGroupState(s: AllocationGroupState): String =
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Allocation Group: ${s.name}"
    lines += s"  UUID:  ${s.groupId.uuid}"
    lines += s"  Level: ${s.level}"
    lines += s"  Usage: ${formatBytes(s.currentUsage)} / ${formatBytes(s.maximumSize)}"
    if s.members.isEmpty then
      lines += "  Members: none"
    else
      lines += "  Members:"
      s.members.foreach: m =>
        lines += s"    ${m.memberType} ${m.uuid}  ${formatBytes(m.currentUsage)} / ${formatBytes(m.maximumSize)}"
    if s.parentGroups.isEmpty then
      lines += "  Parent Groups: none"
    else
      lines += "  Parent Groups:"
      s.parentGroups.foreach(p => lines += s"    ${p.uuid}")
    lines.mkString("\n")

  def list_devices(bootstrapConfigFile: os.Path, hostname: String): Unit =

    configureLogging()

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    // Resolve the host first. A NoSuchElementException during host resolution means the
    // hostname the user supplied does not exist; map it to a distinct error so a later
    // missing device/set (corrupted metadata) is not mislabeled as "host not found".
    val fHostState: Future[HostState] =
      (for
        hostId    <- client.getHostId(hostname)
        hostState <- client.getHostState(hostId)
      yield hostState).recoverWith:
        case _: NoSuchElementException => Future.failed(HostNotFound(hostname))

    // Fetch each device's state, then resolve the (deduplicated) set ids to names for display.
    val f = for
      hostState <- fHostState
      devStates <- Future.sequence(hostState.storageDevices.toList.map(client.getStorageDeviceState))
      setIds     = devStates.map(_.storageDeviceSet).distinct
      setStates <- Future.sequence(setIds.map(client.getStorageDeviceSetState))
      setNameMap = setStates.map(s => s.setId -> s.name).toMap
    yield
      devStates.map { ds =>
        val setName  = setNameMap.getOrElse(ds.storageDeviceSet, ds.storageDeviceSet.uuid.toString)
        val capacity = formatBytes(ds.totalSize)
        val pct      = if ds.totalSize > 0 then ds.currentUsage.toDouble / ds.totalSize * 100.0 else 0.0
        (ds.storageDeviceId.uuid.toString, setName, capacity, pct)
      }.sortBy(_._1)

    f.onComplete:
      case scala.util.Success(devices) =>
        if devices.isEmpty then
          println(s"No devices found for host '$hostname'")
        else
          val setWidth = devices.map(_._2.length).max
          val capWidth = devices.map(_._3.length).max
          println(s"Devices for host '$hostname'")
          devices.foreach { (uuid, setName, capacity, pct) =>
            val paddedCap = " " * (capWidth - capacity.length) + capacity
            println(f"  $uuid  ${setName.padTo(setWidth, ' ')}  $paddedCap  $pct%5.1f%%")
          }
      case scala.util.Failure(_: HostNotFound) =>
        println(s"Error: host '$hostname' not found")
      case scala.util.Failure(err) =>
        println(s"Error listing devices: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

  def show_host(bootstrapConfigFile: os.Path, ref: String): Unit =
    configureLogging()
    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)
    given ExecutionContext = client.clientContext

    val f =
      (for
        hostId    <- resolveRef(ref, HostId(_), client.getHostId)
        hostState <- client.getHostState(hostId)
      yield formatHostState(hostState)).recoverWith:
        case _: NoSuchElementException => Future.failed(EntityNotFound("host", ref))

    f.onComplete:
      case scala.util.Success(text)                => println(text)
      case scala.util.Failure(e: EntityNotFound)   => println(s"Error: ${e.getMessage}")
      case scala.util.Failure(err)                 => println(s"Error showing host: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

  def show_device(bootstrapConfigFile: os.Path, uuidStr: String): Unit =
    configureLogging()
    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)
    given ExecutionContext = client.clientContext

    val deviceId = StorageDeviceId(UUID.fromString(uuidStr))

    val f =
      (for
        dev      <- client.getStorageDeviceState(deviceId)
        hostName <- optName(client.getHostState(dev.hostId))(_.name)
        setName  <- optName(client.getStorageDeviceSetState(dev.storageDeviceSet))(_.name)
      yield formatDeviceState(dev, hostName, setName)).recoverWith:
        case _: NoSuchElementException => Future.failed(EntityNotFound("storage device", uuidStr))

    f.onComplete:
      case scala.util.Success(text)              => println(text)
      case scala.util.Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case scala.util.Failure(err)               => println(s"Error showing storage device: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

  def show_pool(bootstrapConfigFile: os.Path, ref: String): Unit =
    configureLogging()
    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)
    given ExecutionContext = client.clientContext

    val f =
      (for
        poolId  <- resolveRef(ref, PoolId(_), client.getStoragePoolId)
        pool    <- client.getStoragePoolState(poolId)
        setName <- optName(client.getStorageDeviceSetState(pool.storageDeviceSet))(_.name)
      yield formatPoolState(pool, setName)).recoverWith:
        case _: NoSuchElementException => Future.failed(EntityNotFound("pool", ref))

    f.onComplete:
      case scala.util.Success(text)              => println(text)
      case scala.util.Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case scala.util.Failure(err)               => println(s"Error showing pool: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

  def show_device_set(bootstrapConfigFile: os.Path, ref: String): Unit =
    configureLogging()
    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)
    given ExecutionContext = client.clientContext

    val f =
      (for
        setId <- resolveRef(ref, StorageDeviceSetId(_), client.getStorageDeviceSetId)
        set   <- client.getStorageDeviceSetState(setId)
        parentName <- set.parent match
          case None    => Future.successful(None)
          case Some(p) => optName(client.getStorageDeviceSetState(p))(_.name)
      yield formatDeviceSetState(set, parentName)).recoverWith:
        case _: NoSuchElementException => Future.failed(EntityNotFound("device set", ref))

    f.onComplete:
      case scala.util.Success(text)              => println(text)
      case scala.util.Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case scala.util.Failure(err)               => println(s"Error showing device set: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

  def show_allocation_group(bootstrapConfigFile: os.Path, ref: String): Unit =
    configureLogging()
    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)
    given ExecutionContext = client.clientContext

    val f =
      (for
        groupId <- resolveRef(ref, AllocationGroupId(_), client.getAllocationGroupId)
        group   <- client.getAllocationGroupState(groupId)
      yield formatAllocationGroupState(group)).recoverWith:
        case _: NoSuchElementException => Future.failed(EntityNotFound("allocation group", ref))

    f.onComplete:
      case scala.util.Success(text)              => println(text)
      case scala.util.Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case scala.util.Failure(err)               => println(s"Error showing allocation group: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

  def list_entries[A](bootstrapConfigFile: os.Path,
                      title: String,
                      fetch: AspenClient => Future[List[(String, A)]],
                      idToUuid: A => UUID): Unit =

    configureLogging()

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = fetch(client)

    f.onComplete:
      case scala.util.Success(entries) =>
        if entries.isEmpty then
          println(s"No $title found")
        else
          // Sort by name explicitly for a deterministic listing order, independent
          // of the underlying registry's iteration order.
          val sorted = entries.sortBy(_._1)
          val width = sorted.map(_._1.length).max
          println(title)
          sorted.foreach { (name, id) => println(s"  ${name.padTo(width, ' ')}  ${idToUuid(id)}") }
      case scala.util.Failure(err) =>
        println(s"Error listing ${title.toLowerCase}: ${err.getMessage}")

    Await.ready(f, Duration(30, SECONDS))

}
