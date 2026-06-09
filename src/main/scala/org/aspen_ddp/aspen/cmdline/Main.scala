package org.aspen_ddp.aspen.cmdline

import org.aspen_ddp.aspen.AmoebaError
import org.aspen_ddp.aspen.amoebafs.FileSystem
import org.aspen_ddp.aspen.amoebafs.impl.simple.SimpleFileSystem
import org.aspen_ddp.aspen.amoebafs.nfs.AmoebaNFS
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.internal.SimpleAspenClient
import org.aspen_ddp.aspen.client.internal.allocation.PoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.KeyValueListNode
import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.metadata.*
import org.aspen_ddp.aspen.common.network.implementations.zmqnet.ZMQNet
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.DoesNotExist
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, YamlFormat}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Radicle}
import org.aspen_ddp.aspen.server.crl.simple.SimpleCRL
import org.aspen_ddp.aspen.server.store.Bootstrap
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBBackend}
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
                  hosts:List[String]=Nil)

  class ConfigError(msg: String) extends AmoebaError(msg)

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

          arg[Int]("<width>").text("Number of hostStates holding slices/replicas").
            action((x, c) => c.copy(width = x)),
        )

      cmd("debug").text("Runs debugging code").
        action((_, c) => c.copy(mode = "debug")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Bootstrap Config file does not exist: $x"))
        )

      cmd("host").text("Starts an Amoeba Storage HostState").
        action( (_,c) => c.copy(mode="host")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action( (x, c) => c.copy(bootstrapConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Bootstrap Config file does not exist: $x")),

          arg[File]("<hostState-directory>").text("HostState Directory").
            action( (x, c) => c.copy(hostDirectory=x)).
            validate( x => if (x.exists()) success else failure(s"HostState directory does not exist: $x"))
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

      cmd("new-pool").text("Creates a new storage pool").
        action((_, c) => c.copy(mode = "new-pool")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<new-pool-name>").text("Name of the new Pool").
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

          arg[Int]("<width>").text("Number of hostStates holding slices/replicas").
            action((x, c) => c.copy(width = x)),

          arg[Int]("<read-threshold>").text("Minimum number of slices/replicas that must be read to reconstruct an object").
            action((x, c) => c.copy(readThreshold = x)),

          arg[Int]("<write-threshold>").text("Minimum number of slices/replicas that must be written to successfully write an object").
            action((x, c) => c.copy(writeThreshold = x)),

          arg[Seq[String]]("<hostStates>").text("Comma-separated list of hostState names to hostState the object slice/replicas").
            action((x, c) => c.copy(hosts = x.toList)),
        )

      cmd("transfer-store").text("Transfers a store to a new hostState").
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
            case "hostState" => host(bootstrapConfig, bootstrapConfigPath, cfg.hostDirectory.toPath)
            case "amoeba" => amoeba_server(bootstrapConfigPath)
            case "debug" => run_debug_code(bootstrapConfigPath)
            case "rebuild" => rebuild(cfg.storeName, bootstrapConfigPath)
            case "new-pool" => new_pool(bootstrapConfigPath, cfg.newPoolName, cfg.idaType, cfg.width, cfg.readThreshold, cfg.writeThreshold, cfg.hosts)
            case "transfer-store" => transfer_store(bootstrapConfigPath, cfg.storeName, cfg.host)
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

    val cfgFile = hostDir.resolve("aspen-hostState-config.yaml")

    if ! Files.exists(cfgFile) then
      throw Exception(s"HostState config file not found: $cfgFile")

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

    val hostDirectory = baseDirectory.resolve("bootstrap-hostState")

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
      "bootstrap-hostState",
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
      Path.of(s"demo/bootstrap-hostState/storage-devices/bootstrap-device").toFile.listFiles.toList.foreach: storeFn =>
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

  def new_pool(bootstrapConfigFile: os.Path,
               newPoolName: String,
               idaType: String,
               width: Int,
               readThreshold: Int,
               writeThreshold: Int,
               hosts: List[String]): Unit = {
    println(s"READ $readThreshold, WRITE $writeThreshold")
    require(hosts.length == width)
    require(width >= readThreshold && width >= writeThreshold)
    require(readThreshold <= writeThreshold)

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val ida: IDA = idaType match
      case "replication" => Replication(width, writeThreshold)
      case "reed-solomon" => ReedSolomon(width, readThreshold, writeThreshold)
      case _ => throw new Exception(s"Invalid IDA type: $idaType")

    def getHost(name: String): Future[HostState] =
      client.getHostId(name).flatMap: hostId =>
        client.getHostState(hostId)

    for
      _ <- Future.sequence(hosts.map(getHost))
      //sp <- client.newStoragePool(newPoolName, frontends, ida, RocksDBConfig())
    yield
      println("******************************************")
      //println(f"* New Pool Created: ${sp.poolId}")
      println("******************************************")
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

}
