package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.TestTLS
import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.ida.Replication
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState}
import org.aspen_ddp.aspen.common.config.BootstrapConfig
import org.aspen_ddp.aspen.common.network.tls.{CertificateVerifier, Pem}
import org.aspen_ddp.aspen.common.objects.KeyAlreadyExists
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.client.AspenClient

import java.lang.reflect.{InvocationTargetException, Proxy}
import java.security.Signature
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class HostManagerSuite extends IntegrationTestSuite:

  private val systemId = UUID.fromString("33333333-3333-3333-3333-333333333333")

  /** Temp trees created by the current test, removed by subFixtureTeardown. */
  private val tempRoots = mutable.ListBuffer[os.Path]()

  private def newTempDir(prefix: String): os.Path =
    val dir = os.temp.dir(prefix = prefix, deleteOnExit = false)
    tempRoots += dir
    dir

  override def subFixtureTeardown(): Unit =
    tempRoots.foreach(deleteTree)
    tempRoots.clear()

  /** Removes a tree, unlinking symlinks rather than following them. */
  private def deleteTree(root: os.Path): Unit =
    try os.remove.all(root)
    catch case _: Throwable => ()

  /** A real bootstrap config, as fetched by the CLI, so tests can assert the written copy is
   *  not merely byte-identical but still loadable by the host command. */
  private val bootstrapConfigYaml: String =
    val hs = HostState(HostId(UUID.randomUUID()), "some_other_host", "10.0.0.5",
                       4750, 4751, 4752, Set())
    BootstrapConfig.generateBootstrapConfig(
      aspenSystemId = systemId,
      ida = Replication(1, 1),
      hostStates = List(hs),
      storeMap = List(StoreId(PoolId.BootstrapPoolId, 0.toByte) -> hs.hostId),
      caCertificate = TestTLS.caCertificatePem)

  /** A host directory path that does not yet exist, as add-host expects. */
  private def unusedHostDir(): os.Path =
    newTempDir("aspen-hm-test") / "node_b"

  private def createHost(hostDir: os.Path,
                         bsCfg: String,
                         name: String = "node_b",
                         via: AspenClient = client)
                        (using ExecutionContext): Future[HostId] =
    HostManager.createHost(via, hostDir, bsCfg, systemId, name, "10.0.0.7", 5750, 5751, 5752)

  atest("creates the directory tree, writes the bootstrap config, and writes the host config"):
    given ExecutionContext = executionContext
    val hostDir = unusedHostDir()
    val bsCfg = bootstrapConfigYaml
    for
      hostId <- createHost(hostDir, bsCfg)
      _      <- waitForTransactionsToComplete()
      hs     <- client.getHostState(hostId)
    yield
      // Host scans this directory at startup, so an empty one is still required.
      os.isDir(hostDir / StorageDeviceManager.StorageDevicesDirName) should be(true)

      val copied = hostDir / BootstrapConfig.configFilename
      os.isFile(copied) should be(true)
      os.read.bytes(copied) should be(bootstrapConfigYaml.getBytes(StandardCharsets.UTF_8))
      val bs = BootstrapConfig.loadBootstrapConfig(copied)
      bs.aspenSystemId should be(systemId)

      val cfg = HostConfig.loadHostConfig(hostDir / HostConfig.configFilename)
      // Both files hold private keys, so neither may be readable by anyone but the owner.
      val ownerOnly = os.PermSet.fromString("rw-------")
      os.perms(copied) should be(ownerOnly)
      os.perms(hostDir / HostConfig.configFilename) should be(ownerOnly)

      cfg.hostId should be(hostId)
      cfg.aspenSystemId should be(systemId)
      cfg.name should be("node_b")
      cfg.address should be("10.0.0.7")
      cfg.dataPort should be(5750)
      cfg.httpPort should be(5751)
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
    val bsCfg = bootstrapConfigYaml
    os.makeDir.all(hostDir)
    os.write.over(hostDir / HostConfig.configFilename, "host-id: x\n")
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
      os.exists(hostDir / BootstrapConfig.configFilename) should be(false)

  atest("reports DirectorySetupFailed, and registers nothing, when the directory cannot be made"):
    given ExecutionContext = executionContext
    val bsCfg = bootstrapConfigYaml
    // An existing regular file where the host directory should go: createDirectories fails.
    val hostDir = newTempDir("aspen-hm-test") / "node_b"
    os.write.over(hostDir, "not a directory")
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
    val bsCfg = bootstrapConfigYaml
    // Exactly what a run that failed between the copy and the commit leaves behind.
    os.makeDir.all(hostDir / StorageDeviceManager.StorageDevicesDirName)
    os.write.over(hostDir / BootstrapConfig.configFilename, "stale residue, not a valid config\n")
    for
      hostId <- createHost(hostDir, bsCfg)
      _      <- waitForTransactionsToComplete()
      hs     <- client.getHostState(hostId)
    yield
      // The stale copy is replaced rather than preserved or tripped over.
      BootstrapConfig.loadBootstrapConfig(
        hostDir / BootstrapConfig.configFilename).aspenSystemId should be(systemId)
      HostConfig.loadHostConfig(
        hostDir / HostConfig.configFilename).hostId should be(hostId)
      hs.name should be("node_b")

  atest("a name already in use fails after the directory has been prepared"):
    given ExecutionContext = executionContext
    val bsCfg = bootstrapConfigYaml
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
      os.isDir(secondDir / StorageDeviceManager.StorageDevicesDirName) should be(true)
      os.exists(secondDir / HostConfig.configFilename) should be(false)

  atest("reports ConfigWriteFailed, carrying the config text, when the final write fails"):
    given ExecutionContext = executionContext
    val hostDir = unusedHostDir()
    val bsCfg = bootstrapConfigYaml
    os.makeDir.all(hostDir)
    // os.exists follows symlinks so the guard sees no host config; the createNew write
    // creates the file exclusively, which fails on the link itself.
    val cfgPath = hostDir / HostConfig.configFilename
    os.symlink(cfgPath, hostDir / "no-such-target")
    recoverToExceptionIf[HostManager.ConfigWriteFailed](createHost(hostDir, bsCfg))
      .flatMap: err =>
        os.exists(cfgPath, followLinks = false) should be(true)
        err.getMessage should include(err.hostId.uuid.toString)
        // The config text is the operator's only route to finishing by hand, so it has to
        // describe the host that really was registered.
        err.hostConfig.hostId should be(err.hostId)
        err.hostConfig.yamlConfig should include(err.hostId.uuid.toString)
        waitForTransactionsToComplete().flatMap: _ =>
          client.getHostState(err.hostId).map: hs =>
            hs.name should be("node_b")

  atest("writes a host config with a certificate signed for the new host id and a matching key"):
    given ExecutionContext = executionContext
    val hostDir = unusedHostDir()
    val bsCfg = bootstrapConfigYaml
    for
      hostId <- createHost(hostDir, bsCfg)
    yield
      val cfg = HostConfig.loadHostConfig(hostDir / HostConfig.configFilename)
      val cert = Pem.decodeCertificate(cfg.tlsCertificate)
      CertificateVerifier.verify(cert, TestTLS.ca.certificate) should be(hostId.uuid)

      // The private key must be the one whose public half was certified.
      val nonce = "nonce".getBytes(StandardCharsets.UTF_8)
      val signer = Signature.getInstance("SHA256withECDSA")
      signer.initSign(Pem.decodePrivateKey(cfg.tlsPrivateKey))
      signer.update(nonce)
      val sig = signer.sign()
      val verifier = Signature.getInstance("SHA256withECDSA")
      verifier.initVerify(cert.getPublicKey)
      verifier.update(nonce)
      verifier.verify(sig) should be(true)

  /** A client that behaves as `client` in every respect except that certificate signing fails. */
  private def clientWithFailingSigning(cause: Throwable): AspenClient =
    val real = client
    Proxy.newProxyInstance(
      classOf[AspenClient].getClassLoader,
      Array[Class[?]](classOf[AspenClient]),
      (_, method, args) =>
        if method.getName == "createTLSCertificate" then
          Future.failed(cause)
        else
          try method.invoke(real, Option(args).getOrElse(Array.empty[AnyRef])*)
          catch case e: InvocationTargetException => throw e.getCause
    ).asInstanceOf[AspenClient]

  atest("registers nothing when certificate signing fails"):
    given ExecutionContext = executionContext
    val hostDir = unusedHostDir()
    val bsCfg = bootstrapConfigYaml
    val boom = new RuntimeException("signing exploded")
    for
      before <- client.listHosts()
      err    <- createHost(hostDir, bsCfg, via = clientWithFailingSigning(boom)).failed
      _      <- waitForTransactionsToComplete()
      after  <- client.listHosts()
      _      <- client.getHostId("node_b").failed  // the name is not registered
    yield
      err shouldBe a[HostManager.CertificateCreationFailed]
      err.getCause should be theSameInstanceAs boom
      after should be(before)
      os.exists(hostDir / HostConfig.configFilename) should be(false)
