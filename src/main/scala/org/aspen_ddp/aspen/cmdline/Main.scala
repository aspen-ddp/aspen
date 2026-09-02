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
import org.aspen_ddp.aspen.common.rebalancing.RebalancingDurableService
import org.aspen_ddp.aspen.common.network.implementations.zmqnet.ZMQNet
import org.aspen_ddp.aspen.common.network.*
import org.aspen_ddp.aspen.common.objects.*
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.allocation_group.AllocationGroupId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.DoesNotExist
import org.aspen_ddp.aspen.common.util.{BackgroundTaskManager, DaemonThreads, YamlFormat}
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
import org.yaml.snakeyaml.error.YAMLException
import scopt.OptionDef
import scribe.Logging
import scribe.format.{FormatterInterpolator, classNameSimple, dateFull, line, mdc, messages, methodName}

import java.io.{File, StringReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, Files, Path}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import java.util.concurrent.{Executors, TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{DAYS, Duration, HOURS, MILLISECONDS, MINUTES, SECONDS}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.language.implicitConversions


object Main {

  val AmoebafsKey: Key = Key("amoeba")

  // Default listening ports for a newly created host. Shared by every subcommand that
  // creates one so the ports a host is given do not depend on which command created it.
  val DefaultDataPort: Int = 4750
  val DefaultCnCPort: Int = 4751
  val DefaultStoreTransferPort: Int = 4752

  // How often a running host rescans for storage devices it has not yet loaded. host() gives
  // this to its StoreManager, and create-storage-device quotes it when telling the user how
  // long an un-notified host may take to pick up a new device, so the two cannot drift.
  val CheckStorageDevicesPeriod: Duration = Duration(1, HOURS)

  // How long a terminating command waits for outbound messages to leave ZMQNet, and how long
  // ZMQ is then given to put them on the wire before the process exits.
  val NotificationDrainTimeout: Duration = Duration(5, SECONDS)
  val NotificationSendLinger: Duration = Duration(1, SECONDS)

  // Set by createNetwork so main can drain outbound messages before System.exit. Plain rather
  // than volatile because it is thread-confined: every command builds its network by a direct
  // call on the main thread, and main reads it back on that same thread. A command that built
  // one from a callback would need this made volatile, or the drain would see None and skip.
  private var onetwork: Option[ZMQNet] = None

  case class Args(mode:String="",
                  hostDirectory:File=null,
                  targetDirectory:File=null,
                  bootstrapConfigFile:File=null,
                  hostName:String="",
                  storeName:String="",
                  deviceId:String="",
                  deviceName:String="",
                  address:String="",
                  host:String="",
                  port:Int=0,
                  dataPort:Int=DefaultDataPort,
                  cncPort:Int=DefaultCnCPort,
                  storeTransferPort:Int=DefaultStoreTransferPort,
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
                  newGroupName:String="",
                  poolName:String="",
                  srcGroupName:String="",
                  newGroupLevel:Int=0,
                  entityRef:String="",
                  rebalancePeriod:Option[String]=None,
                  rebalancePeriodUnit:Option[String]=None)

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

  /** Awaits `f` and then runs `report` on the calling thread.
   *
   * Reporting on this thread rather than in an onComplete callback matters because all of
   * Aspen's worker threads are daemon threads: as soon as a command returns, the process
   * exits and anything still queued on the client's ExecutionContext is lost.
   *
   * Returns the process exit code: 0 if the future succeeded, 1 if it failed.
   */
  private def awaitAndReport[T](f: Future[T], timeout: Duration = Duration(30, SECONDS))
                               (report: Try[T] => Unit): Int =
    val result = Await.ready(f, timeout).value.get
    report(result)
    if result.isSuccess then 0 else 1

  /** Gives outbound messages a chance to leave the process before it exits.
   *
   *  CheckStorageDevice nudges are why this exists: a command that sends one and then exits
   *  would otherwise abandon it, costing the receiving host up to CheckStorageDevicesPeriod.
   *  Commands that sent nothing pass through without waiting, and bootstrap -- which builds no
   *  network at all -- skips it entirely.
   *
   *  Never affects the exit code. A timeout means messages may still be held, which is a
   *  latency report rather than a command failure: the receiving host's periodic check remains
   *  the correctness guarantee.
   *
   *  host() blocks in joinIoThread and amoeba_server() in Thread.currentThread.join(); neither
   *  reaches here.
   */
  private def drainAndShutdown(): Unit =
    onetwork.foreach: net =>
      // Nothing here may change the exit code, and shutdown() deliberately races the IO
      // thread's use of the sockets it closes, so swallow whatever comes back: the command's
      // result is already decided and the process is on its way out.
      try
        if !net.awaitPendingMessagesSent(NotificationDrainTimeout) then
          println("Could not confirm all notifications left this process. Affected hosts will " +
                  s"act on their next periodic check, within $CheckStorageDevicesPeriod.")
        net.shutdown(NotificationSendLinger)
      catch
        case _: Throwable => ()

  /** Maps a command failure the CLI reports as a one-line message onto that message.
   *
   *  Deliberately partial, and deliberately not NonFatal: anything outside this set is a bug,
   *  and a bug should reach the user as a stack trace rather than as a tidy sentence. What is
   *  in the set is the set of ways a user can get it wrong -- a config that does not parse, a
   *  config that parses but does not validate, an argument naming something that is not there,
   *  a storage system that does not answer.
   *
   *  YAMLException is the SnakeYAML side of a bad config: ScannerException for a tab indent,
   *  ParserException for an unclosed flow sequence, ComposerException for an undefined alias.
   *  None of them is a FormatError, which covers only Aspen's own validation, so without this
   *  clause a truncated config unwinds out of main and skips both drainAndShutdown and the
   *  explicit exit.
   *
   *  Extracted from main's catch so the mapping is reachable from a test; main itself is not.
   */
  private[cmdline] def commandErrorMessage: PartialFunction[Throwable, String] =
    case e: YamlFormat.FormatError   => s"Error loading config file: $e"
    case e: YAMLException            => s"Error parsing config file: $e"
    case e: ConfigError              => s"Error: $e"
    case _: TimeoutException         => "Error: operation timed out waiting for the storage system to respond"
    case e: IllegalArgumentException => s"Error: ${e.getMessage}"

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Args]("demo") {
      head("demo", "0.1")

      val validPort = (p: Int) =>
        if p >= 1 && p <= 65535 then success else failure(s"Port must be between 1 and 65535: $p")

      // The listening ports of a host being created. Shared by every subcommand that creates
      // one. This must be a def rather than a val: scopt records the owning command on each
      // OptionDef, so every subcommand needs its own instances.
      def portOptions: List[OptionDef[?, Args]] = List(
        opt[Int]("data-port").valueName("<port>").
          text(s"Port for client/store data traffic (default: $DefaultDataPort)").
          validate(validPort).
          action((x, c) => c.copy(dataPort = x)),

        opt[Int]("cnc-port").valueName("<port>").
          text(s"Port for command-and-control messages (default: $DefaultCnCPort)").
          validate(validPort).
          action((x, c) => c.copy(cncPort = x)),

        opt[Int]("store-transfer-port").valueName("<port>").
          text(s"Port for store transfers (default: $DefaultStoreTransferPort)").
          validate(validPort).
          action((x, c) => c.copy(storeTransferPort = x)),
      )

      cmd("bootstrap").text("Bootstrap a new Amoeba system").
        action( (_,c) => c.copy(mode="bootstrap")).
        children(
          // Unlike every other directory argument in this parser, the target is not required
          // to exist: bootstrap creates it. Bootstrapping over an existing system is caught
          // later by the bootstrap-host check in bootstrap() itself.
          arg[File]("<target-directory>").text("Directory the bootstrap host and its stores are created under").
            action((x, c) => c.copy(targetDirectory = x)).
            validate { x =>
              if !x.exists() || x.isDirectory then
                success
              else
                failure(s"Not a directory: $x")
            },

          arg[String]("<address>").
            text("IP address or DNS name other hosts use to reach this bootstrap host").
            action((x, c) => c.copy(address = x)).
            validate(x => validateHostAddress(x).fold(success)(failure)),

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
        ).
        children(portOptions*)

      // OBSOLETE: the "debug" command is defunct and needs rework before it can be
      // re-enabled. run_debug_code is retained for reference but is not reachable.
      //cmd("debug").text("Runs debugging code").
      //  action((_, c) => c.copy(mode = "debug")).
      //  children(
      //    arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
      //      action((x, c) => c.copy(bootstrapConfigFile = x)).
      //      validate(x => if (x.exists()) success else failure(s"Bootstrap Config file does not exist: $x"))
      //  )

      cmd("host").text("Starts an Amoeba Storage Host").
        action( (_,c) => c.copy(mode="host")).
        children(
          // No bootstrap config argument: the host reads it from its own directory so that
          // MetadataManager can keep the file up to date as the bootstrap pool moves.
          arg[File]("<host-directory>").text("Host Directory").
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

      cmd("create-allocation-group").text("Creates a new allocation group").
        action((_, c) => c.copy(mode = "create-allocation-group")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<name>").text("Name of the new allocation group").
            action((x, c) => c.copy(newGroupName = x)),

          arg[Int]("<level>").text("Hierarchy level (0 = group of pools, 1+ = group of groups)").
            action((x, c) => c.copy(newGroupLevel = x)).
            validate(x => if (x >= 0) success else failure("Level must be >= 0")),
        )

      cmd("add-pool-to-group").text("Adds a storage pool to an allocation group").
        action((_, c) => c.copy(mode = "add-pool-to-group")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<pool-name>").text("Name of the pool to add").
            action((x, c) => c.copy(poolName = x)),

          arg[String]("<group-name>").text("Name of the allocation group").
            action((x, c) => c.copy(newGroupName = x)),
        )

      cmd("add-group-to-group").text("Nests one allocation group inside another").
        action((_, c) => c.copy(mode = "add-group-to-group")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<source-group-name>").text("Name of the source (lower-level) allocation group").
            action((x, c) => c.copy(srcGroupName = x)),

          arg[String]("<destination-group-name>").text("Name of the destination allocation group (level must be strictly greater than the source)").
            action((x, c) => c.copy(newGroupName = x)),
        )

      cmd("move-device-to-set").text("Moves a storage device into a different level-0 device set").
        action((_, c) => c.copy(mode = "move-device-to-set")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<device-uuid>").text("UUID of the storage device to move").
            action((x, c) => c.copy(deviceId = x)).
            validate { x =>
              try
                UUID.fromString(x)
                success
              catch
                case _: Throwable => failure("Storage device id must be a valid UUID")
            },

          arg[String]("<set-name-or-uuid>").text("Name or UUID of the target level-0 device set").
            action((x, c) => c.copy(deviceSetName = x)),
        )

      cmd("fail-storage-device").text("Declares a storage device dead and rebuilds its stores elsewhere").
        action((_, c) => c.copy(mode = "fail-storage-device")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<device-uuid>").text("UUID of the storage device to declare failed").
            action((x, c) => c.copy(deviceId = x)).
            validate { x =>
              try
                UUID.fromString(x)
                success
              catch
                case _: Throwable => failure("Storage device id must be a valid UUID")
            },
        )

      // A single trailing slash on <device-name> is tolerated and stripped: tab-completing
      // inside storage-devices/ yields "dev0/". Shared by that argument's action and
      // validate so the stored name cannot drift from the one that was validated.
      val stripTrailingSlash = (s: String) => s.stripSuffix("/")

      cmd("add-host").text("Registers a new host and initializes its root directory").
        action((_, c) => c.copy(mode = "add-host")).
        children(
          // Copied from an existing host by the operator. add-host needs it to reach the
          // running system, and leaves a copy in <host-directory> for the host command.
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          // Unlike the <host-directory> of host and create-storage-device, this one is not
          // required to exist: add-host creates it. An already-initialized directory is
          // caught by the host-config check in HostManager.createHost.
          arg[File]("<host-directory>").text("Directory to initialize as the new host's root").
            action((x, c) => c.copy(hostDirectory = x)).
            validate { x =>
              if !x.exists() || x.isDirectory then
                success
              else
                failure(s"Not a directory: $x")
            },

          arg[String]("<hostname>").text("Name for the new host. Must not already be in use").
            action((x, c) => c.copy(hostName = x)).
            validate(x => if x.trim.nonEmpty then success else failure("Host name must not be empty")),

          arg[String]("<address>").
            text("IP address or DNS name other hosts use to reach this host").
            action((x, c) => c.copy(address = x)).
            validate(x => validateHostAddress(x).fold(success)(failure)),
        ).
        children(portOptions*)

      cmd("create-storage-device").text("Registers a new storage device on a host").
        action((_, c) => c.copy(mode = "create-storage-device")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[File]("<host-directory>").text("Host Directory").
            action((x, c) => c.copy(hostDirectory = x)).
            validate(x => if (x.exists()) success else failure(s"Host directory does not exist: $x")),

          arg[String]("<device-name>").text(s"Name of the already-provisioned device directory under <host-directory>/${StorageDeviceManager.StorageDevicesDirName}").
            action((x, c) => c.copy(deviceName = stripTrailingSlash(x))).
            validate { x =>
              val n = stripTrailingSlash(x)
              if n.nonEmpty && !n.contains("/") && n != "." && n != ".." then success
              else failure(s"Device name must be the bare name of a directory under <host-directory>/${StorageDeviceManager.StorageDevicesDirName}")
            },

          arg[String]("<set-name-or-uuid>").text("Name or UUID of the target level-0 device set").
            action((x, c) => c.copy(deviceSetName = x)),
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

      cmd("system-rebalance-period").text("Displays or sets the automatic rebalancing period").
        action((_, c) => c.copy(mode = "system-rebalance-period")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<period>").optional().
            text("Interval between automatic rebalance sweeps, or \"disabled\". " +
                 "Omit to display the current period").
            action((x, c) => c.copy(rebalancePeriod = Some(x))),

          arg[String]("<unit>").optional().text("minutes, hours, or days").
            action((x, c) => c.copy(rebalancePeriodUnit = Some(x))),
        )

      cmd("migrate-pool").text("Migrates a storage pool to a different storage device set").
        action((_, c) => c.copy(mode = "migrate-pool")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<pool-name-or-uuid>").text("Name or UUID of the pool to migrate").
            action((x, c) => c.copy(poolName = x)),

          arg[String]("<set-name-or-uuid>").text("Name or UUID of the target device set").
            action((x, c) => c.copy(deviceSetName = x)),
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

      checkConfig { c =>
        if c.mode == "" then
          failure("Invalid command")
        else if Set(c.dataPort, c.cncPort, c.storeTransferPort).size != 3 then
          failure("data-port, cnc-port, and store-transfer-port must all be different")
        else if c.mode == "system-rebalance-period" then
          // Validated here rather than in the handler so a malformed period is a usage error,
          // reported before any client or network is built.
          parseRebalancePeriod(c.rebalancePeriod, c.rebalancePeriodUnit) match
            case Left(msg) => failure(msg)
            case Right(_) => success
        else
          success
      }
    }

    val exitCode = parser.parse(args, Args()) match
      case Some(cfg) =>
        def bootstrapConfig: BootstrapConfig.Config =
          println(s"Loading BootstrapConfig ${cfg.bootstrapConfigFile}")
          BootstrapConfig.loadBootstrapConfig(cfg.bootstrapConfigFile)

        // Path arguments are resolved by the JVM against the process cwd, which is the
        // directory the user invoked the CLI from, so they arrive here still relative.
        // os.Path rejects relative paths outright, and the commands taking a directory
        // resolve further paths beneath it. Normalize once here rather than at each use.
        def absPath(f: File): Path = f.toPath.toAbsolutePath.normalize

        def bootstrapConfigPath: os.Path =
          os.Path(absPath(cfg.bootstrapConfigFile))

        try
          //println(s"Config file: $config")
          cfg.mode match
            case "bootstrap" => bootstrap(createIDA(cfg), absPath(cfg.targetDirectory), cfg.address,
                                          cfg.dataPort, cfg.cncPort, cfg.storeTransferPort)
            case "host" => host(absPath(cfg.hostDirectory))
            case "amoeba" => amoeba_server(bootstrapConfigPath)
            // OBSOLETE: see the commented-out "debug" parser entry above.
            //case "debug" => run_debug_code(bootstrapConfigPath)
            case "create-pool" => create_pool(bootstrapConfigPath, cfg.newPoolName, createIDA(cfg), cfg.deviceSetName, cfg.maximumStoreSize)
            case "create-device-set" => create_device_set(bootstrapConfigPath, cfg.newSetName, cfg.newSetLevel, cfg.parentSetName)
            case "create-allocation-group" => create_allocation_group(bootstrapConfigPath, cfg.newGroupName, cfg.newGroupLevel)
            case "add-pool-to-group" => add_pool_to_group(bootstrapConfigPath, cfg.poolName, cfg.newGroupName)
            case "add-group-to-group" => add_group_to_group(bootstrapConfigPath, cfg.srcGroupName, cfg.newGroupName)
            case "move-device-to-set" => move_device_to_set(bootstrapConfigPath, cfg.deviceId, cfg.deviceSetName)
            case "fail-storage-device" => fail_storage_device(bootstrapConfigPath, cfg.deviceId)
            case "add-host" => add_host(bootstrapConfig, bootstrapConfigPath,
                                        absPath(cfg.hostDirectory), cfg.hostName, cfg.address,
                                        cfg.dataPort, cfg.cncPort, cfg.storeTransferPort)
            case "create-storage-device" => create_storage_device(bootstrapConfig, bootstrapConfigPath,
                                                                  absPath(cfg.hostDirectory), cfg.deviceName,
                                                                  cfg.deviceSetName)
            case "transfer-store" => transfer_store(bootstrapConfigPath, cfg.storeName, cfg.host)
            case "rebalance" => rebalance(bootstrapConfigPath, cfg.setId)
            case "system-rebalance-period" =>
              systemRebalancePeriod(bootstrapConfigPath, cfg.rebalancePeriod, cfg.rebalancePeriodUnit)
            case "migrate-pool" => migrate_pool(bootstrapConfigPath, cfg.poolName, cfg.deviceSetName)
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
          // Rethrowing from applyOrElse's default is what keeps an unrecognized exception
          // loud. It still unwinds out of main and still skips the drain, which is the right
          // trade for a bug: the stack trace is the point.
          case e: Throwable =>
            println(commandErrorMessage.applyOrElse(e, (t: Throwable) => throw t))
            1

      // scopt has already printed the usage message
      case None => 1

    // All of Aspen's threads are daemon threads, so the process would exit here anyway.
    // The explicit exit is what carries the status code out to the shell. The drain first is
    // what keeps a nudge sent moments ago from dying with the process.
    drainAndShutdown()
    System.exit(exitCode)
  }

  def createIDA(args: Args): IDA =
    require(args.width >= args.readThreshold && args.width >= args.writeThreshold)
    require(args.readThreshold <= args.writeThreshold)
    args.idaType match
      case "replication" => Replication(args.width, args.writeThreshold)
      case "reed-solomon" => ReedSolomon(args.width, args.readThreshold, args.writeThreshold)
      case _ => throw new Exception(s"Invalid IDA type: ${args.idaType}")

  def createNetwork(bootstrapConfigFile: os.Path,
                    ohost: Option[(HostId, Int)],
                    oclientId: Option[ClientId]): (NetworkBridge, ZMQNet) = {
    val b = new NetworkBridge

    val heartbeatPeriod = Duration(10, SECONDS)
    val net = new ZMQNet(bootstrapConfigFile, oclientId, ohost, heartbeatPeriod, b)
    onetwork = Some(net)
    (b, net)
  }

  def createAmoebaClient(bootstrapConfigFile: os.Path,
                         onnet: Option[(NetworkBridge, ZMQNet)]=None): (AspenClient, ZMQNet, KeyValueObjectPointer) = {

    val (networkBridge, nnet) = onnet.getOrElse(createNetwork(bootstrapConfigFile, None, None))

    val txStatusCacheDuration = Duration(10, SECONDS)
    val initialReadDelay = Duration(10, SECONDS)
    val maxReadDelay = Duration(6, SECONDS)
    val txRetransmitDelay = Duration(1, SECONDS)
    val allocationRetransmitDelay = Duration(5, SECONDS)

    val sched = Executors.newScheduledThreadPool(3, DaemonThreads.factory("aspen-client"))
    val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)

    // Supplied offline because the bootstrap pool's own configuration object cannot be read
    // without it. See SimpleAspenClient.resolveIda.
    val bootstrapIda = BootstrapConfig.loadBootstrapConfig(bootstrapConfigFile.toIO).bootstrapIDA

    val ret = (new SimpleAspenClient(nnet.clientMessenger, nnet.clientId, ec, Radicle.pointer,
      bootstrapIda,
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

  /** OBSOLETE: not reachable from the CLI. Retained for reference; needs rework. */
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

  def amoeba_server(bootstrapConfigFile: os.Path): Int = {
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

    // The NFS server is meant to run indefinitely. Blocking the main thread is what keeps
    // the process alive; every worker thread in the system is a daemon thread.
    Thread.currentThread.join()

    0 // unreachable
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


  def host(hostDir: Path): Int = {

    val sched = Executors.newScheduledThreadPool(3)
    val ec = ExecutionContext.fromExecutorService(sched)
    given ExecutionContext = ec

    // Absolute: os.Path rejects relative paths and <host-directory> is usually given as one.
    val absHostDir = hostDir.toAbsolutePath

    val cfgFile = absHostDir.resolve(HostConfig.configFilename)

    if ! Files.exists(cfgFile) then
      throw Exception(s"Host config file not found: $cfgFile")

    val hostCfg = HostConfig.loadHostConfig(cfgFile.toFile)

    val bsCfgFile = absHostDir.resolve(BootstrapConfig.configFilename)

    if ! Files.exists(bsCfgFile) then
      throw Exception(s"Bootstrap config file not found: $bsCfgFile")

    val bootstrapCfg = BootstrapConfig.loadBootstrapConfig(bsCfgFile.toFile)
    val bootstrapConfigFile = os.Path(bsCfgFile)

    configureLogging()

    val simpleCrl = hostCfg.crl match {
      case b: HostConfig.SimpleCRL =>
        val crlRoot = absHostDir.resolve("crl")
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
    //val allocHeartbeatPeriod   = Duration(3, SECONDS)
    //val allocTimeout           = Duration(4, SECONDS)
    //val allocStatusQueryPeriod = Duration(1, SECONDS)

    val nodeNet = nnet.serverMessenger

    val storeManager = new StoreManager(
      client,
      hostCfg.hostId,
      bootstrapCfg.aspenSystemId,
      absHostDir,
      ec,
      objectCacheFactory,
      nodeNet,
      new BackgroundTaskManager(ec),
      simpleCrl,
      txFinalizerFactory,
      SimpleTransactionDriver.factory(txRetryDelay, txRetryCap),
      txHeartbeatPeriod,
      CheckStorageDevicesPeriod
    ) with SimpleDriverRecoveryMixin

    networkBridge.onode = Some(storeManager)

    network.startIoThread(client)
    
    storeManager.start()

    /*val cncBackend = new ZCnCBackend(
      nnet,
      hostCfg.cncPort,
      CnCMessageReceiver.Unhandled)*/

    // The storage host is meant to run indefinitely. Blocking the main thread on the IO
    // thread is what keeps the process alive; the IO thread itself is a daemon thread.
    network.joinIoThread()

    0 // unreachable
  }

  def mkdirectory(p: Path): Unit = {
    Files.createDirectories(p)
  }

  def bootstrap(bootstrapIda: IDA,
                baseDirectory: Path, // created if it does not already exist
                address: String, // advertised to other hosts; never bound to
                dataPort: Int,
                cncPort: Int,
                storeTransferPort: Int): Int = {

    if isUnreachableAddress(address) then
      println(s"Warning: '$address' is not reachable from other machines. Hosts added to this " +
              "system later will not be able to connect to the bootstrap host.")

    val hostDirectory = baseDirectory.resolve("bootstrap-host")

    if Files.exists(hostDirectory) then
      throw new Exception(s"Bootstrap host directory exists: $hostDirectory")

    val sched = Executors.newScheduledThreadPool(1)
    val ec = ExecutionContext.fromExecutorService(sched)
    given ExecutionContext = ec

    val storageDevicesDir = hostDirectory.resolve(StorageDeviceManager.StorageDevicesDirName)
    val bootstrapDevDir = storageDevicesDir.resolve("bootstrap-device")

    Files.createDirectories(hostDirectory)
    Files.createDirectories(storageDevicesDir)
    Files.createDirectories(bootstrapDevDir)

    val aspenSystemId = UUID.randomUUID()

    val hostConfig = HostConfig(
      HostId(UUID.randomUUID()),
      aspenSystemId,
      "bootstrap-host",
      address,
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
      address,
      hostConfig.dataPort,
      hostConfig.cncPort,
      hostConfig.storeTransferPort,
      Set(storageDevConfig.storageDeviceId)
    )

    // The bootstrap config lives with the host it describes rather than being handed to the
    // host process on the command line: MetadataManager rewrites this file in place as the
    // bootstrap pool moves.
    Files.write(
      hostDirectory.resolve(BootstrapConfig.configFilename),
      BootstrapConfig.generateBootstrapConfig(
        aspenSystemId = aspenSystemId,
        ida = bootstrapIda,
        hostStates = List(bootstrapHost),
        storeMap = bootstrapStores.map(backend => backend.storeId -> hostConfig.hostId).toList
      ).getBytes(StandardCharsets.UTF_8)
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

    0
  }


  def create_pool(bootstrapConfigFile: os.Path,
                  poolName: String,
                  ida: IDA,
                  deviceSetName: String,
                  maximumStoreSize: Long): Int = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = for
      setId <- client.getStorageDeviceSetId(deviceSetName)
      poolId <- client.createNewStoragePool(poolName, ida, None, RocksDBConfig(), setId, maximumStoreSize)
    yield poolId

    // Translate the known failure modes into human-readable messages. The client's retry
    // strategy unwraps StopRetrying, so the future fails with the underlying cause.
    def reportError(cause: Throwable): Unit = cause match
      // KeyAlreadyExists is how a taken name arrives from the registry's transactional
      // registration path; DuplicateRegistration comes only from Registry.register.
      case _: KeyAlreadyExists =>
        println(s"Error: a storage pool named '$poolName' already exists")
      // getStorageDeviceSetId throws this when the name is not registered.
      case _: NoSuchElementException =>
        println(s"Error: device set '$deviceSetName' not found")
      case e =>
        println(s"Error creating storage pool: ${e.getMessage}")

    awaitAndReport(f):
      case Success(poolId) =>
        println("******************************************")
        println(s"* New Pool Created: ${poolId.uuid}")
        println("******************************************")
      case Failure(err) => reportError(err)
  }

  def create_device_set(bootstrapConfigFile: os.Path,
                        name: String,
                        level: Int,
                        parentSetName: String): Int = {

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
      // KeyAlreadyExists is how a taken name arrives from the registry's transactional
      // registration path; DuplicateRegistration comes only from Registry.register.
      case _: KeyAlreadyExists =>
        println(s"Error: a device set named '$name' already exists")
      case _: NoSuchElementException =>
        println(s"Error: parent device set '$parentSetName' not found")
      case e: AspenClient.InvalidDeviceSetLevel =>
        println(s"Error: ${e.getMessage}")
      case e =>
        println(s"Error creating device set: ${e.getMessage}")

    awaitAndReport(f):
      case Success(setId) =>
        println("******************************************")
        println(s"* New Device Set Created: ${setId.uuid}")
        println("******************************************")
      case Failure(err) => reportError(err)
  }

  def create_allocation_group(bootstrapConfigFile: os.Path,
                              name: String,
                              level: Int): Int = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = client.createAllocationGroup(name, level)

    // Translate the known failure mode into human-readable messages. The client's retry
    // strategy unwraps StopRetrying, so the future fails with the underlying cause.
    def reportError(cause: Throwable): Unit = cause match
      // KeyAlreadyExists is how a taken name arrives from the registry's transactional
      // registration path; DuplicateRegistration comes only from Registry.register.
      case _: KeyAlreadyExists =>
        println(s"Error: an allocation group named '$name' already exists")
      case e =>
        println(s"Error creating allocation group: ${e.getMessage}")

    awaitAndReport(f):
      case Success(groupId) =>
        println("******************************************")
        println(s"* New Allocation Group Created: ${groupId.uuid}")
        println("******************************************")
      case Failure(err) => reportError(err)
  }

  def add_pool_to_group(bootstrapConfigFile: os.Path,
                        poolName: String,
                        groupName: String): Int = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = client.addPoolToGroup(poolName, groupName)

    // getStoragePoolId / getAllocationGroupId throw NoSuchElementException when a
    // name is not registered; translate that into a precise message.
    def reportError(cause: Throwable): Unit = cause match
      case _: NoSuchElementException =>
        println(s"Error: pool '$poolName' or allocation group '$groupName' not found")
      case e =>
        println(s"Error adding pool to group: ${e.getMessage}")

    awaitAndReport(f):
      case Success(_) =>
        println(s"Pool '$poolName' added to allocation group '$groupName'")
      case Failure(err) => reportError(err)
  }

  def add_group_to_group(bootstrapConfigFile: os.Path,
                         sourceGroupName: String,
                         destGroupName: String): Int = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = client.addGroupToGroup(sourceGroupName, destGroupName)

    // getAllocationGroupId throws NoSuchElementException when a name is not registered;
    // addGroup throws AllocationGroupState.InvalidLevel when the destination level is not
    // strictly greater than the source level. Translate both into precise messages.
    def reportError(cause: Throwable): Unit = cause match
      case _: NoSuchElementException =>
        println(s"Error: allocation group '$sourceGroupName' or '$destGroupName' not found")
      case _: AllocationGroupState.InvalidLevel =>
        println(s"Error: destination group '$destGroupName' must have a higher level than source group '$sourceGroupName'")
      case e =>
        println(s"Error adding group to group: ${e.getMessage}")

    awaitAndReport(f):
      case Success(_) =>
        println(s"Allocation group '$sourceGroupName' added to allocation group '$destGroupName'")
      case Failure(err) => reportError(err)
  }

  def move_device_to_set(bootstrapConfigFile: os.Path,
                         deviceIdStr: String,
                         setRef: String): Int = {

    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val deviceId = StorageDeviceId(UUID.fromString(deviceIdStr))

    val f = for
      targetSetId <- resolveRef(setRef, StorageDeviceSetId(_), client.getStorageDeviceSetId)
      _           <- client.moveDeviceToSet(deviceId, targetSetId)
    yield ()

    // getStorageDevicePointer / getStorageDeviceSetPointer / getStorageDeviceSetId throw
    // NoSuchElementException for unknown ids/names; moveDevice throws NotLevelZero when the
    // target set is not level 0. Translate both into precise messages.
    def reportError(cause: Throwable): Unit = cause match
      case _: NoSuchElementException =>
        println(s"Error: device '$deviceIdStr' or set '$setRef' not found")
      case _: StorageDeviceSetState.NotLevelZero =>
        println(s"Error: target set '$setRef' must be a level-0 (tier-0) set")
      case e =>
        println(s"Error moving device to set: ${e.getMessage}")

    awaitAndReport(f):
      case Success(_) =>
        println(s"Device '$deviceIdStr' moved to set '$setRef'")
      case Failure(err) => reportError(err)
  }

  def fail_storage_device(bootstrapConfigFile: os.Path,
                          deviceIdStr: String): Int = {

    configureLogging()

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val deviceId = StorageDeviceId(UUID.fromString(deviceIdStr))

    val f = client.failStorageDevice(deviceId)

    // getStorageDevicePointer throws NoSuchElementException for an unknown id;
    // failStorageDevice throws DeviceAlreadyFailed when the device already carries a tombstone.
    def reportError(cause: Throwable): Unit = cause match
      case _: NoSuchElementException =>
        println(s"Error: storage device '$deviceIdStr' not found")
      case _: AspenClient.DeviceAlreadyFailed =>
        println(s"Error: storage device '$deviceIdStr' has already been declared failed")
      case e =>
        println(s"Error failing storage device: ${e.getMessage}")

    awaitAndReport(f):
      case Success(_) =>
        println(s"Storage device '$deviceIdStr' declared failed.")
        println("Its stores have been marked for rebuild onto live devices, but server-side")
        println("reconstruction is NOT YET IMPLEMENTED: nothing acts on those marks, so the")
        println("slices that lived on this device are unavailable until it is. The tombstone")
        println("is one-way.")
        println("Watch the marks with:")
        println(s"  show-device $bootstrapConfigFile $deviceIdStr")
      case Failure(err) => reportError(err)
  }

  def migrate_pool(bootstrapConfigFile: os.Path,
                   poolRef: String,
                   setRef: String): Int =

    configureLogging()

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = for
      poolId      <- resolveRef(poolRef, PoolId(_), client.getStoragePoolId)
      targetSetId <- resolveRef(setRef, StorageDeviceSetId(_), client.getStorageDeviceSetId)
      _           <- client.migratePoolToSet(poolId, targetSetId)
    yield ()

    // getStoragePoolId / getStorageDeviceSetId / getStoragePoolPointer /
    // getStorageDeviceSetPointer all throw NoSuchElementException for unknown names and ids.
    def reportError(cause: Throwable): Unit = cause match
      case _: NoSuchElementException =>
        println(s"Error: pool '$poolRef' or set '$setRef' not found")
      case e =>
        println(s"Error starting pool migration: ${e.getMessage}")

    awaitAndReport(f):
      case Success(_) =>
        println(s"Migration of pool '$poolRef' to set '$setRef' started. " +
          s"Track progress with: show-pool $poolRef")
      case Failure(err) => reportError(err)

  def add_host(bootstrapCfg: BootstrapConfig.Config,
               bootstrapConfigFile: os.Path,
               hostDirectory: Path,
               hostName: String,
               address: String,
               dataPort: Int,
               cncPort: Int,
               storeTransferPort: Int): Int = {

    configureLogging()

    if isUnreachableAddress(address) then
      println(s"Warning: '$address' is not reachable from other machines. Other hosts in this " +
              "system will not be able to connect to this one.")

    val hostDir = hostDirectory.toAbsolutePath.normalize

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = HostManager.createHost(
      client, hostDir, bootstrapConfigFile.toNIO, bootstrapCfg.aspenSystemId,
      hostName, address, dataPort, cncPort, storeTransferPort)

    def reportError(cause: Throwable): Unit = cause match
      case e: HostManager.HostAlreadyConfigured =>
        println(s"Error: ${e.directory} already contains ${HostConfig.configFilename}")
        println("It has already been initialized as a host root directory.")
      case e: HostManager.DirectorySetupFailed =>
        println(s"Error: could not prepare host directory ${e.directory}: ${e.getCause.getMessage}")
        println("No host was registered.")
      // KeyAlreadyExists is how a taken name arrives from the registry's transactional
      // registration path; DuplicateRegistration comes only from Registry.register.
      case _: KeyAlreadyExists =>
        println(s"Error: a host named '$hostName' is already registered")
        println(s"$hostDir was prepared but no host was registered. Re-run with an unused name.")
      case e: HostManager.ConfigWriteFailed =>
        println(s"Error: host ${e.hostId.uuid} was registered but writing ${e.configFile} failed: ${e.getCause.getMessage}")
        println(s"Host '$hostName' exists in the system, but $hostDir is incomplete and the host")
        println("cannot be started until it is finished. Re-running this command will not fix it:")
        println(s"the name '$hostName' is now taken, so a second run fails. Instead, create")
        println(s"${e.configFile} with exactly these contents:")
        print(e.hostConfig.yamlConfig)
      case e =>
        println(s"Error adding host: ${e.getMessage}")

    awaitAndReport(f):
      case Success(hostId) =>
        println(s"Created host '$hostName' (${hostId.uuid}) at $hostDir")
        // A host with no storage devices is registered but has nothing to serve, so name the
        // next step rather than leaving the operator with a host that silently does nothing.
        println("It owns no storage devices yet. Add one with create-storage-device, then start")
        println(s"the host with:  host $hostDir")
      case Failure(err) => reportError(err)
  }

  def create_storage_device(bootstrapCfg: BootstrapConfig.Config,
                            bootstrapConfigFile: os.Path,
                            hostDirectory: Path,
                            deviceName: String,
                            setRef: String): Int = {

    configureLogging()

    val hostConfigFile = hostDirectory.resolve(HostConfig.configFilename)

    if !Files.isRegularFile(hostConfigFile) then
      println(s"Error: host configuration file not found: $hostConfigFile")
      1
    else
      val hostCfg = HostConfig.loadHostConfig(hostConfigFile.toFile)

      // The device directory is required to sit at a fixed location, so the CLI takes the
      // bare name. The <device-name> validator already rejects everything path-shaped, so
      // this always resolves to a direct child; StorageDeviceManager's containment check is
      // a second line of defence for its non-CLI callers rather than the mechanism here.
      val deviceDirectory = StorageDeviceManager.deviceDirectory(hostDirectory, deviceName)

      val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)

      network.startIoThread(client)

      given ExecutionContext = client.clientContext

      val f = for
        setId    <- resolveRef(setRef, StorageDeviceSetId(_), client.getStorageDeviceSetId)
        deviceId <- StorageDeviceManager.createStorageDevice(
                      client, hostCfg, hostDirectory, deviceDirectory,
                      setId, bootstrapCfg.aspenSystemId)
      yield deviceId

      def reportError(cause: Throwable): Unit = cause match
        case e: StorageDeviceManager.WrongAspenSystem =>
          println(s"Error: host config belongs to Aspen system ${e.found}, not ${e.expected}")
        case e: StorageDeviceManager.DeviceDirectoryNotUnderHost =>
          println(s"Error: ${e.directory} must be a direct child of ${e.expectedParent}")
        case e: StorageDeviceManager.DeviceDirectoryNotFound =>
          println(s"Error: storage device directory does not exist: ${e.directory}")
          println("The directory must be provisioned before this command is run.")
        case e: StorageDeviceManager.DeviceAlreadyConfigured =>
          println(s"Error: ${e.directory} already contains ${StorageDeviceConfig.configFilename}")
        // Both ConfigWriteFailed arms leave a committed registration with no directory behind
        // it. There is no device-removal command or client API, so the only executable remedy
        // is to move the orphan out of the set: while it remains in the set's memberDevices,
        // StorageDeviceSetState.selectFromDevices may hand it to createNewStoragePool, placing
        // a store on a device no host will load (StoreManager.tryLoadDevice needs the config
        // file). move-device-to-set removes it from the source set, so a level-0 quarantine set
        // that is assigned no pools takes it out of circulation.
        case e: StorageDeviceManager.ConfigWriteFailed if e.getCause.isInstanceOf[FileAlreadyExistsException] =>
          println(s"Error: ${e.configFile} was created by another caller while device ${e.storageDeviceId.uuid} was being registered.")
          println("Do NOT write that file by hand -- it belongs to another device.")
          println(s"Device ${e.storageDeviceId.uuid} owns no directory and cannot be used, but remains a member of set '$setRef'")
          println("and may be selected for pool placement. Quarantine it with:")
          println(s"  move-device-to-set ${e.storageDeviceId.uuid} <quarantine-level-0-set>")
        case e: StorageDeviceManager.ConfigWriteFailed =>
          println(s"Error: device ${e.storageDeviceId.uuid} was registered but writing ${e.configFile} failed: ${e.getCause.getMessage}")
          // The rollback at StorageDeviceManager's write site is best-effort (a failed delete
          // is swallowed), so a surviving file may be either ours -- partial or empty, and
          // safe to replace -- or a newer registration's, if the delete succeeded and another
          // caller then claimed the directory. The device id inside it is the discriminator.
          println(s"Check whether ${e.configFile} exists. If it exists and does not name device")
          println(s"${e.storageDeviceId.uuid}, it belongs to another device: leave it alone.")
          println("Otherwise create or replace it with exactly these contents:")
          print(StorageDeviceConfig(e.storageDeviceId, bootstrapCfg.aspenSystemId).yamlConfig)
          println(s"If you do not, device ${e.storageDeviceId.uuid} remains a member of set '$setRef' with no")
          println("directory and may be selected for pool placement. Quarantine it with:")
          println(s"  move-device-to-set ${e.storageDeviceId.uuid} <quarantine-level-0-set>")
        case _: StorageDeviceSetState.NotLevelZero =>
          println(s"Error: device set '$setRef' must be a level-0 (tier-0) set")
        case _: NoSuchElementException =>
          println(s"Error: host '${hostCfg.name}' (${hostCfg.hostId.uuid}) or device set '$setRef' not found")
        case e =>
          println(s"Error creating storage device: ${e.getMessage}")

      awaitAndReport(f):
        case Success(deviceId) =>
          println(s"Created storage device ${deviceId.uuid} at $deviceDirectory")
          // Best-effort nudge so the host loads the device now rather than on its next periodic
          // storage-device check. Losing it costs at most one check period and is never a
          // requirement, so nothing here may fail the command. main's drainAndShutdown is what
          // gives the message its chance to leave the process.
          client.sendHostMessage(CheckStorageDevice(hostCfg.hostId, client.clientId, deviceId))
          // A dealer socket accepts a send whether or not the peer is up, so a registered but
          // unreachable host reaches here too. Promise nothing beyond the handoff and name
          // both fallbacks.
          println(s"A running host should load the device shortly, or within " +
                  s"$CheckStorageDevicesPeriod if the notification is lost. A host that is " +
                  "down loads the device when it next starts.")
        case Failure(err) => reportError(err)
  }

  def transfer_store(bootstrapConfigFile: os.Path,
                     storeName: String,
                     targetDeviceIdStr: String): Int = {

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
    //
    // Left carries a pre-flight validation failure; Right means the transfer was
    // successfully initiated. The whole chain is a single future so it can be awaited --
    // returning before it completes would let the process exit before the transfer is
    // ever requested.
    val f: Future[Either[String, Unit]] = for
      ePoolState   <- lookupPoolState
      eDeviceState <- lookupTargetDevice
      result       <- (ePoolState, eDeviceState) match
                        case (Left(msg), _) => Future.successful(Left(msg))
                        case (_, Left(msg)) => Future.successful(Left(msg))
                        case (Right(poolState), Right(_)) =>
                          if storeId.poolIndex < 0 || storeId.poolIndex >= poolState.stores.length then
                            Future.successful(Left(
                              f"Invalid store index ${storeId.poolIndex} for pool " +
                              f"${storeId.poolId.uuid} (pool has ${poolState.stores.length} stores)"))
                          else
                            val sourceDeviceId = poolState.stores(storeId.poolIndex).storageDeviceId
                            if sourceDeviceId == targetDeviceId then
                              Future.successful(Left(
                                f"Source and destination devices are the same " +
                                f"(${targetDeviceIdStr}); nothing to transfer"))
                            else
                              client.transferStore(storeId, targetDeviceId).map(Right(_))
    yield result

    Await.ready(f, Duration(30, SECONDS)).value.get match
      case Success(Right(_)) =>
        println(f"Store Transfer Initiated: Store: ${storeName} -> Device: ${targetDeviceIdStr}")
        0
      case Success(Left(msg)) =>
        println(f"Store Transfer Failed: $msg")
        1
      case Failure(err) =>
        println(f"Store Transfer Failed: ${err.getMessage}")
        1
  }

  def rebalance(bootstrapConfigFile: os.Path, setIdStr: String): Int =
    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val setId = StorageDeviceSetId(UUID.fromString(setIdStr))

    val f = RebalancingDurableService.rebalanceStorageDeviceSet(client, setId)

    awaitAndReport(f):
      case Success(_) =>
        println(s"Rebalance enrolled for storage device set $setIdStr")
      case Failure(err) =>
        println(s"Rebalance failed to enroll: ${err.getMessage}")

  /** Display the automatic rebalancing period, or set it. checkConfig has already rejected a
   *  malformed period, so the Left branch here is belt and braces. */
  def systemRebalancePeriod(bootstrapConfigFile: os.Path,
                            period: Option[String],
                            unit: Option[String]): Int =
    configureLogging()

    val (client, network, radicle) = createAmoebaClient(bootstrapConfigFile)
    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    parseRebalancePeriod(period, unit) match
      case Left(msg) =>
        println(s"Invalid period: $msg")
        1

      case Right(None) =>
        awaitAndReport(RebalancingDurableService.getAutoRebalanceStatus(client)):
          case Success((current, lastSweep)) =>
            println(formatRebalanceStatus(current, lastSweep))
          case Failure(err) =>
            println(s"Could not read the rebalance period: ${err.getMessage}")

      case Right(Some(d)) =>
        awaitAndReport(RebalancingDurableService.setAutoRebalancePeriod(client, d)):
          case Success(_) =>
            println(s"Automatic rebalancing period set to ${formatRebalancePeriod(d)}")
          case Failure(err) =>
            println(s"Could not set the rebalance period: ${err.getMessage}")

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

  /** Validates the advertised address of a host being created. Returns None if the address is
   *  usable, or Some(message) explaining why it is not.
   *
   *  This is the host component of the `tcp://<address>:<port>` endpoints ZMQNet and
   *  ZCnCFrontend build, so anything that would corrupt that string is rejected here rather
   *  than surfacing later as an unexplained connect failure. Nothing is resolved: a system may
   *  well be bootstrapped before its DNS entry exists.
   */
  private[cmdline] def validateHostAddress(address: String): Option[String] =
    val colons = address.count(_ == ':')
    if address.trim.isEmpty then
      Some("Address must not be empty")
    else if address.exists(_.isWhitespace) then
      Some("Address must not contain whitespace")
    else if address.contains("://") then
      Some("Address must not include a URI scheme; pass just the host (e.g. 10.0.0.5)")
    else if address.startsWith("[") && address.endsWith("]") && colons > 0 then
      None // bracketed IPv6 literal
    else if colons == 1 then
      Some("Address must not include a port; use --data-port/--cnc-port/--store-transfer-port")
    else if colons > 1 then
      Some("IPv6 literals must be bracketed, e.g. [fd00::5]")
    else
      None

  /** True for addresses no other machine can reach: loopback and the bind wildcard.
   *
   *  Not an error -- a single-machine demo system is a legitimate use -- but worth saying out
   *  loud, since an unreachable address is invisible until the second host fails to connect.
   */
  private[cmdline] def isUnreachableAddress(address: String): Boolean =
    val a = address.toLowerCase
    a == "localhost" || a.startsWith("127.") || a == "::1" || a == "[::1]" ||
      a == "0.0.0.0" || a == "[::]"

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

  /** Parse the period arguments of `system-rebalance-period`.
   *
   *  `Right(None)` means no period was supplied: the display form. `Right(Some(d))` is the
   *  period to set, where zero disables automatic rebalancing. `Left` carries a message fit
   *  to show the user.
   *
   *  A missing period with a present unit cannot occur -- scopt fills positional arguments in
   *  order -- and is treated as the display form rather than given its own error. */
  private[cmdline] def parseRebalancePeriod(period: Option[String],
                                            unit: Option[String]): Either[String, Option[Duration]] =
    (period, unit) match
      case (None, _) =>
        Right(None)

      case (Some(p), _) if p.equalsIgnoreCase("disabled") =>
        if unit.isDefined then Left("'disabled' takes no unit")
        else Right(Some(Duration.Zero))

      case (Some(_), None) =>
        Left("a unit is required: minutes, hours, or days")

      case (Some(p), Some(u)) =>
        for
          count <- p.toLongOption.toRight("period must be a whole number")
          _ <- if count < 0 then Left("period must not be negative") else Right(())
          timeUnit <- u.toLowerCase match
                        case "minute" | "minutes" => Right(MINUTES)
                        case "hour" | "hours" => Right(HOURS)
                        case "day" | "days" => Right(DAYS)
                        case other => Left(s"unknown unit '$other': expected minutes, hours, or days")
          _ <- checkDurationRange(count, timeUnit)
        yield Some(Duration(count, timeUnit))

  /** Duration(count, unit) throws IllegalArgumentException if the result exceeds ~292 years
   *  (2^63-1 nanoseconds). Check the range before constructing the Duration. */
  private def checkDurationRange(count: Long, unit: java.util.concurrent.TimeUnit): Either[String, Unit] =
    val maxDays = 106751L
    val exceedsLimit = unit match
      case DAYS => count > maxDays
      case HOURS => count > maxDays * 24
      case MINUTES => count > maxDays * 24 * 60
      case _ => false
    if exceedsLimit then Left(s"period is too large (maximum ~292 years)")
    else Right(())

  /** Render an automatic rebalancing period using the largest unit that divides it evenly.
   *  A display helper in the mould of formatBytes. */
  private[cmdline] def formatRebalancePeriod(d: Duration): String =
    val minutes = d.toMinutes
    if minutes == 0 then "disabled"
    else if minutes % (60 * 24) == 0 then pluralUnits(minutes / (60 * 24), "day")
    else if minutes % 60 == 0 then pluralUnits(minutes / 60, "hour")
    else pluralUnits(minutes, "minute")

  private def pluralUnits(n: Long, unit: String): String =
    if n == 1 then s"$n $unit" else s"$n ${unit}s"

  /** Render a wall-clock millisecond value in the local time zone. */
  private[cmdline] def formatWallTime(millis: Long): String =
    java.time.Instant.ofEpochMilli(millis)
      .atZone(java.time.ZoneId.systemDefault())
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

  /** The display form of `system-rebalance-period`. The next-sweep line is omitted when
   *  automatic rebalancing is disabled, because there is no next sweep to report. */
  private[cmdline] def formatRebalanceStatus(period: Duration, lastSweep: HLCTimestamp): String =
    val lines = scala.collection.mutable.ListBuffer[String]()
    lines += s"Automatic rebalancing period: ${formatRebalancePeriod(period)}"
    if lastSweep == HLCTimestamp.Zero then
      lines += "Last sweep:                   never"
      if period > Duration.Zero then
        lines += "Next sweep due:               next poll"
    else
      lines += s"Last sweep:                   ${formatWallTime(lastSweep.wallTime)}"
      if period > Duration.Zero then
        lines += s"Next sweep due:               ${formatWallTime(lastSweep.wallTime + period.toMillis)}"
    lines.mkString("\n")

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
    s.migration.foreach: m =>
      lines += s"  Migration:    ${m.status} -> ${m.targetSet.uuid}"
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

  def list_devices(bootstrapConfigFile: os.Path, hostname: String): Int =

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

    awaitAndReport(f):
      case Success(devices) =>
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
      case Failure(_: HostNotFound) =>
        println(s"Error: host '$hostname' not found")
      case Failure(err) =>
        println(s"Error listing devices: ${err.getMessage}")

  def show_host(bootstrapConfigFile: os.Path, ref: String): Int =
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

    awaitAndReport(f):
      case Success(text)                => println(text)
      case Failure(e: EntityNotFound)   => println(s"Error: ${e.getMessage}")
      case Failure(err)                 => println(s"Error showing host: ${err.getMessage}")

  def show_device(bootstrapConfigFile: os.Path, uuidStr: String): Int =
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

    awaitAndReport(f):
      case Success(text)              => println(text)
      case Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case Failure(err)               => println(s"Error showing storage device: ${err.getMessage}")

  def show_pool(bootstrapConfigFile: os.Path, ref: String): Int =
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

    awaitAndReport(f):
      case Success(text)              => println(text)
      case Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case Failure(err)               => println(s"Error showing pool: ${err.getMessage}")

  def show_device_set(bootstrapConfigFile: os.Path, ref: String): Int =
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

    awaitAndReport(f):
      case Success(text)              => println(text)
      case Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case Failure(err)               => println(s"Error showing device set: ${err.getMessage}")

  def show_allocation_group(bootstrapConfigFile: os.Path, ref: String): Int =
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

    awaitAndReport(f):
      case Success(text)              => println(text)
      case Failure(e: EntityNotFound) => println(s"Error: ${e.getMessage}")
      case Failure(err)               => println(s"Error showing allocation group: ${err.getMessage}")

  def list_entries[A](bootstrapConfigFile: os.Path,
                      title: String,
                      fetch: AspenClient => Future[List[(String, A)]],
                      idToUuid: A => UUID): Int =

    configureLogging()

    val (client, network, _) = createAmoebaClient(bootstrapConfigFile)

    network.startIoThread(client)

    given ExecutionContext = client.clientContext

    val f = fetch(client)

    awaitAndReport(f):
      case Success(entries) =>
        if entries.isEmpty then
          println(s"No $title found")
        else
          // Sort by name explicitly for a deterministic listing order, independent
          // of the underlying registry's iteration order.
          val sorted = entries.sortBy(_._1)
          val width = sorted.map(_._1.length).max
          println(title)
          sorted.foreach { (name, id) => println(s"  ${name.padTo(width, ' ')}  ${idToUuid(id)}") }
      case Failure(err) =>
        println(s"Error listing ${title.toLowerCase}: ${err.getMessage}")

}
