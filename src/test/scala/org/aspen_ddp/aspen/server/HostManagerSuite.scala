package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{BootstrapConfig, HostId, HostState}
import org.aspen_ddp.aspen.common.objects.KeyAlreadyExists
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, LinkOption, Path, SimpleFileVisitor, FileVisitResult}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class HostManagerSuite extends IntegrationTestSuite:

  private val systemId = UUID.fromString("33333333-3333-3333-3333-333333333333")

  /** Temp trees created by the current test, removed by subFixtureTeardown. */
  private val tempRoots = mutable.ListBuffer[Path]()

  private def newTempDir(prefix: String): Path =
    val dir = Files.createTempDirectory(prefix)
    tempRoots += dir
    dir

  override def subFixtureTeardown(): Unit =
    tempRoots.foreach(deleteTree)
    tempRoots.clear()

  /** Removes a tree bottom-up, unlinking symlinks rather than following them. */
  private def deleteTree(root: Path): Unit =
    try
      Files.walkFileTree(root, new SimpleFileVisitor[Path]:
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult =
          Files.deleteIfExists(file)
          FileVisitResult.CONTINUE

        override def visitFileFailed(file: Path, exc: IOException): FileVisitResult =
          Files.deleteIfExists(file)
          FileVisitResult.CONTINUE

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult =
          Files.deleteIfExists(dir)
          FileVisitResult.CONTINUE
      )
    catch
      case _: Throwable => ()

  /** A real bootstrap config for the operator's copy, so tests can assert the copy is not
   *  merely byte-identical but still loadable by the host command. */
  private val bootstrapConfigYaml: String =
    val hs = HostState(HostId(UUID.randomUUID()), "some_other_host", "10.0.0.5",
                       4750, 4751, 4752, Set())
    BootstrapConfig.generateBootstrapConfig(
      aspenSystemId = systemId,
      ida = Replication(1, 1),
      hostStates = List(hs),
      storeMap = List(StoreId(PoolId.BootstrapPoolId, 0.toByte) -> hs.hostId))

  /** Writes the operator's bootstrap config somewhere outside the new host's directory,
   *  which is where it comes from in practice. */
  private def newBootstrapConfigFile(): Path =
    val f = newTempDir("aspen-operator").resolve(BootstrapConfig.configFilename)
    Files.write(f, bootstrapConfigYaml.getBytes(StandardCharsets.UTF_8))
    f

  /** A host directory path that does not yet exist, as add-host expects. */
  private def unusedHostDir(): Path =
    newTempDir("aspen-hm-test").resolve("node_b")

  private def createHost(hostDir: Path,
                         bsCfg: Path,
                         name: String = "node_b")
                        (using ExecutionContext): Future[HostId] =
    HostManager.createHost(client, hostDir, bsCfg, systemId, name, "10.0.0.7", 5750, 5751, 5752)

  atest("creates the directory tree, copies the bootstrap config, and writes the host config"):
    given ExecutionContext = executionContext
    val hostDir = unusedHostDir()
    val bsCfg = newBootstrapConfigFile()
    for
      hostId <- createHost(hostDir, bsCfg)
      _      <- waitForTransactionsToComplete()
      hs     <- client.getHostState(hostId)
    yield
      // Host scans this directory at startup, so an empty one is still required.
      Files.isDirectory(hostDir.resolve(StorageDeviceManager.StorageDevicesDirName)) should be(true)

      val copied = hostDir.resolve(BootstrapConfig.configFilename)
      Files.isRegularFile(copied) should be(true)
      val bs = BootstrapConfig.loadBootstrapConfig(copied.toFile)
      bs.aspenSystemId should be(systemId)

      val cfg = HostConfig.loadHostConfig(hostDir.resolve(HostConfig.configFilename).toFile)
      cfg.hostId should be(hostId)
      cfg.aspenSystemId should be(systemId)
      cfg.name should be("node_b")
      cfg.address should be("10.0.0.7")
      cfg.dataPort should be(5750)
      cfg.cncPort should be(5751)
      cfg.storeTransferPort should be(5752)
      cfg.crl should be(HostConfig.SimpleCRL(HostManager.DefaultCrlNumStreams,
                                             HostManager.DefaultCrlFileSizeMb))

      // The registration and the file must describe the same host; asserting both is what
      // catches one of the two being wired to a different id.
      hs.hostId should be(hostId)
      hs.name should be("node_b")
      hs.storageDevices should be(Set.empty)

  atest("rejects a directory that is already an initialized host"):
    given ExecutionContext = executionContext
    val hostDir = unusedHostDir()
    val bsCfg = newBootstrapConfigFile()
    Files.createDirectories(hostDir)
    Files.write(hostDir.resolve(HostConfig.configFilename),
                "host-id: x\n".getBytes(StandardCharsets.UTF_8))
    for
      before <- client.listHosts()
      err    <- createHost(hostDir, bsCfg).failed
      _      <- waitForTransactionsToComplete()
      after  <- client.listHosts()
    yield
      err shouldBe a[HostManager.HostAlreadyConfigured]
      after should be(before)
      // The guard must fire before anything is copied in, or a rejected run would overwrite
      // the bootstrap config of a host that is already running.
      Files.exists(hostDir.resolve(BootstrapConfig.configFilename)) should be(false)

  atest("reports DirectorySetupFailed, and registers nothing, when the directory cannot be made"):
    given ExecutionContext = executionContext
    val bsCfg = newBootstrapConfigFile()
    // An existing regular file where the host directory should go: createDirectories fails.
    val hostDir = newTempDir("aspen-hm-test").resolve("node_b")
    Files.write(hostDir, "not a directory".getBytes(StandardCharsets.UTF_8))
    for
      before <- client.listHosts()
      err    <- createHost(hostDir, bsCfg).failed
      _      <- waitForTransactionsToComplete()
      after  <- client.listHosts()
    yield
      err shouldBe a[HostManager.DirectorySetupFailed]
      // The whole reason the directory is prepared before the transaction: a bad path costs
      // nothing, rather than leaving a registered host with no directory.
      after should be(before)

  atest("reuses a directory left behind by an aborted earlier run"):
    given ExecutionContext = executionContext
    val hostDir = unusedHostDir()
    val bsCfg = newBootstrapConfigFile()
    // Exactly what a run that failed between the copy and the commit leaves behind.
    Files.createDirectories(hostDir.resolve(StorageDeviceManager.StorageDevicesDirName))
    Files.write(hostDir.resolve(BootstrapConfig.configFilename),
                "stale residue, not a valid config\n".getBytes(StandardCharsets.UTF_8))
    for
      hostId <- createHost(hostDir, bsCfg)
      _      <- waitForTransactionsToComplete()
      hs     <- client.getHostState(hostId)
    yield
      // The stale copy is replaced rather than preserved or tripped over.
      BootstrapConfig.loadBootstrapConfig(
        hostDir.resolve(BootstrapConfig.configFilename).toFile).aspenSystemId should be(systemId)
      HostConfig.loadHostConfig(
        hostDir.resolve(HostConfig.configFilename).toFile).hostId should be(hostId)
      hs.name should be("node_b")

  atest("a name already in use fails after the directory has been prepared"):
    given ExecutionContext = executionContext
    val bsCfg = newBootstrapConfigFile()
    val firstDir = unusedHostDir()
    val secondDir = unusedHostDir()
    for
      _     <- createHost(firstDir, bsCfg, "node_b")
      _     <- waitForTransactionsToComplete()
      err   <- createHost(secondDir, bsCfg, "node_b").failed
      _     <- waitForTransactionsToComplete()
      hosts <- client.listHosts()
    yield
      // What add-host turns into "a host named 'node_b' is already registered".
      err shouldBe a[KeyAlreadyExists]
      hosts.count(_._1 == "node_b") should be(1)
      // The directory is prepared before the transaction, so it survives; the host config is
      // not, which keeps the failed directory from looking like an initialized host.
      Files.isDirectory(secondDir.resolve(StorageDeviceManager.StorageDevicesDirName)) should be(true)
      Files.exists(secondDir.resolve(HostConfig.configFilename)) should be(false)

  atest("reports ConfigWriteFailed, carrying the config text, when the final write fails"):
    given ExecutionContext = executionContext
    val hostDir = unusedHostDir()
    val bsCfg = newBootstrapConfigFile()
    Files.createDirectories(hostDir)
    // Files.exists follows symlinks so the guard sees no host config; the CREATE_NEW write
    // opens with O_EXCL, which fails on the link itself.
    val cfgPath = hostDir.resolve(HostConfig.configFilename)
    Files.createSymbolicLink(cfgPath, hostDir.resolve("no-such-target"))
    recoverToExceptionIf[HostManager.ConfigWriteFailed](createHost(hostDir, bsCfg))
      .flatMap: err =>
        Files.exists(cfgPath, LinkOption.NOFOLLOW_LINKS) should be(true)
        err.getMessage should include(err.hostId.uuid.toString)
        // The config text is the operator's only route to finishing by hand, so it has to
        // describe the host that really was registered.
        err.hostConfig.hostId should be(err.hostId)
        err.hostConfig.yamlConfig should include(err.hostId.uuid.toString)
        waitForTransactionsToComplete().flatMap: _ =>
          client.getHostState(err.hostId).map: hs =>
            hs.name should be("node_b")
