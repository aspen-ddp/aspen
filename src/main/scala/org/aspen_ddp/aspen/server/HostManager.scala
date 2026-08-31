package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Creation and on-disk setup of hosts.
 *
 *  Stateless by design, for the same reason StorageDeviceManager is: nothing here may depend
 *  on running inside the CLI, so the same function can be called when host creation is
 *  automated.
 */
object HostManager:

  /** CRL settings given to a newly created host. Matches what bootstrap gives the bootstrap
   *  host so a host's CRL does not depend on which command created it. Not yet configurable
   *  from the command line. */
  val DefaultCrlNumStreams: Int = 3
  val DefaultCrlFileSizeMb: Int = 300

  class HostAlreadyConfigured(val directory: Path)
    extends Exception(s"$directory already contains ${HostConfig.configFilename}")

  /** Raised when the directory could not be prepared. Nothing is registered when this is
   *  thrown: the whole point of preparing the directory first is that this failure comes
   *  before the transaction. */
  class DirectorySetupFailed(val directory: Path, cause: Throwable)
    extends Exception(s"Could not prepare host directory $directory: ${cause.getMessage}", cause)

  /** Raised when the host registration committed but its config file could not be written.
   *  Carries the id and the config text because together they are what an operator needs to
   *  finish the job by hand. */
  class ConfigWriteFailed(val hostId: HostId,
                          val configFile: Path,
                          val hostConfig: HostConfig,
                          cause: Throwable)
    extends Exception(
      s"Host ${hostId.uuid} was registered but writing $configFile failed: ${cause.getMessage}",
      cause)

  /** Register a new host with the running Aspen system and initialize `hostDirectory` as that
   *  host's root directory.
   *
   *  `bootstrapConfigFile` is copied into the host directory because that is where the `host`
   *  command reads it from -- MetadataManager rewrites the file in place as the bootstrap pool
   *  moves, so each host owns its own copy rather than sharing the operator's.
   *
   *  The new host owns no storage devices. create-storage-device adds those afterwards, and
   *  until it does the host is registered but has nothing to serve.
   *
   *  Ordering is deliberate. The directory is created and the bootstrap config copied in
   *  *before* the transaction, so an unwritable or mistyped path fails without registering
   *  anything; the leftover empty directory is inert and is reused by a later attempt. The
   *  host config file is written last, after the commit, because it is both the only artifact
   *  that needs the committed HostId and the marker that says this directory is an initialized
   *  host: writing it earlier would let a `host` process start against a registration that
   *  does not exist. That leaves one small window -- a crash between commit and that final
   *  write -- which yields a registered host with no devices and no directory. It is inert,
   *  and ConfigWriteFailed carries everything needed to complete it by hand.
   */
  def createHost(client: AspenClient,
                 hostDirectory: Path,
                 bootstrapConfigFile: Path,
                 aspenSystemId: UUID,
                 name: String,
                 address: String,
                 dataPort: Int,
                 cncPort: Int,
                 storeTransferPort: Int)
                (using ExecutionContext): Future[HostId] =

    val hostDir = hostDirectory.toAbsolutePath.normalize
    val hostConfigFile = hostDir.resolve(HostConfig.configFilename)
    val storageDevicesDir = hostDir.resolve(StorageDeviceManager.StorageDevicesDirName)

    if Files.exists(hostConfigFile) then
      Future.failed(new HostAlreadyConfigured(hostDir))

    else
      val setupError =
        try
          // Creates hostDir itself as well, so the directory need not exist beforehand.
          Files.createDirectories(storageDevicesDir)

          // REPLACE_EXISTING: reaching here means the directory holds no host config, so it is
          // not an initialized host, and a bootstrap config left behind by an aborted earlier
          // run is residue rather than something worth preserving.
          Files.copy(bootstrapConfigFile,
                     hostDir.resolve(BootstrapConfig.configFilename),
                     StandardCopyOption.REPLACE_EXISTING)
          None
        catch
          case t: Throwable => Some(new DirectorySetupFailed(hostDir, t))

      setupError match
        case Some(err) => Future.failed(err)

        case None =>
          client.createHost(name, address, dataPort, cncPort, storeTransferPort).map: hostId =>
            val hostConfig = HostConfig(
              hostId,
              aspenSystemId,
              name,
              address,
              dataPort,
              cncPort,
              storeTransferPort,
              HostConfig.SimpleCRL(DefaultCrlNumStreams, DefaultCrlFileSizeMb)
            )
            try
              // CREATE_NEW rather than the default CREATE|TRUNCATE_EXISTING: a full distributed
              // transaction separates the guard above from this write, so a host config that
              // appeared in that window belongs to another caller and must not be clobbered.
              Files.write(hostConfigFile,
                          hostConfig.yamlConfig.getBytes(StandardCharsets.UTF_8),
                          StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
            catch
              case t: Throwable =>
                throw new ConfigWriteFailed(hostId, hostConfigFile, hostConfig, t)
            hostId
