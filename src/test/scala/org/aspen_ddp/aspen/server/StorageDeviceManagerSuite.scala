package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceSetId}

import java.io.{IOException, OutputStream}
import java.net.URI
import java.nio.channels.SeekableByteChannel
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.*
import java.nio.file.spi.FileSystemProvider
import java.nio.file.*
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class StorageDeviceManagerSuite extends IntegrationTestSuite:

  private val systemId = UUID.fromString("22222222-2222-2222-2222-222222222222")
  private val bootstrapSet = StorageDeviceSetId.BootstrapStorageDeviceSetId

  /** Temp trees created by the current test, removed by subFixtureTeardown. */
  private val tempRoots = mutable.ListBuffer[Path]()

  /** Creates a tracked temp directory that teardown will remove. */
  private def newTempDir(prefix: String): Path =
    val dir = Files.createTempDirectory(prefix)
    tempRoots += dir
    dir

  override def subFixtureTeardown(): Unit =
    tempRoots.foreach(deleteTree)
    tempRoots.clear()

  /** Removes a tree bottom-up. Symlinks are unlinked rather than followed, and owner
   *  write/execute is restored on the way down so the read-only device directory left by
   *  the ConfigWriteFailed test can still be traversed and removed. */
  private def deleteTree(root: Path): Unit =
    try
      Files.walkFileTree(root, new SimpleFileVisitor[Path]:
        override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult =
          try
            val perms = Files.getPosixFilePermissions(dir)
            perms.add(PosixFilePermission.OWNER_WRITE)
            perms.add(PosixFilePermission.OWNER_EXECUTE)
            Files.setPosixFilePermissions(dir, perms)
          catch
            case _: Throwable => ()
          FileVisitResult.CONTINUE

        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult =
          Files.deleteIfExists(file)
          FileVisitResult.CONTINUE

        override def visitFileFailed(file: Path, exc: IOException): FileVisitResult =
          FileVisitResult.CONTINUE

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult =
          Files.deleteIfExists(dir)
          FileVisitResult.CONTINUE
      )
    catch
      case _: Throwable => ()

  /** A HostConfig for the TestNetwork's bootstrap host. Only hostId and aspenSystemId
   *  are consulted by createStorageDevice; the rest is filler. */
  private def hostConfig(sysId: UUID = systemId) = HostConfig(
    HostId.BootstrapHostId,
    sysId,
    "testhost",
    "localhost",
    1234,
    1235,
    1236,
    HostConfig.SimpleCRL(numStreams = 3, fileSizeMb = 300))

  /** Creates `<tmp>/host/storage-devices` and returns the host directory. */
  private def newHostDir(): Path =
    val base = newTempDir("aspen-sdm-test")
    val hostDir = base.resolve("host")
    Files.createDirectories(hostDir.resolve(StorageDeviceManager.StorageDevicesDirName))
    hostDir

  atest("deviceDirectory resolves <host-dir>/storage-devices/<name>"):
    Future.successful(
      StorageDeviceManager.deviceDirectory(Paths.get("/srv/aspen/node_a"), "dev2") should be(
        Paths.get("/srv/aspen/node_a/storage-devices/dev2")))

  atest("rejects a host config from a different Aspen system"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    recoverToSucceededIf[StorageDeviceManager.WrongAspenSystem](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(UUID.randomUUID()), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device directory that is a grandchild of storage-devices"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "foo/bar")
    Files.createDirectories(dir)
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotUnderHost](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device name that escapes upward"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "..")
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotUnderHost](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects an absolute device name"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val elsewhere = newTempDir("aspen-elsewhere")
    val dir = StorageDeviceManager.deviceDirectory(hostDir, elsewhere.toString)
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotUnderHost](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device directory that does not exist"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "nope")
    recoverToSucceededIf[StorageDeviceManager.DeviceDirectoryNotFound](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("rejects a device directory that already holds a device config"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    Files.write(dir.resolve(StorageDeviceConfig.configFilename),
                "storage-device-id: x\n".getBytes(StandardCharsets.UTF_8))
    recoverToSucceededIf[StorageDeviceManager.DeviceAlreadyConfigured](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))

  atest("creates the device and writes its config into a plain directory"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    for
      deviceId <- StorageDeviceManager.createStorageDevice(
                    client, hostConfig(), hostDir, dir, bootstrapSet, systemId)
      _        <- waitForTransactionsToComplete()
      ds       <- client.getStorageDeviceState(deviceId)
    yield
      val written = dir.resolve(StorageDeviceConfig.configFilename)
      Files.isRegularFile(written) should be(true)
      val cfg = StorageDeviceConfig.loadHostConfig(written.toFile)
      cfg.storageDeviceId should be(deviceId)
      cfg.aspenSystemId should be(systemId)
      ds.hostId should be(HostId.BootstrapHostId)
      ds.storageDeviceSet should be(bootstrapSet)

  atest("accepts a symlink under storage-devices and writes the config through it"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val elsewhere = newTempDir("aspen-real-device")
    val link = hostDir.resolve(StorageDeviceManager.StorageDevicesDirName).resolve("dev2")
    Files.createSymbolicLink(link, elsewhere)
    for
      deviceId <- StorageDeviceManager.createStorageDevice(
                    client, hostConfig(), hostDir, link, bootstrapSet, systemId)
      _        <- waitForTransactionsToComplete()
    yield
      val written = elsewhere.resolve(StorageDeviceConfig.configFilename)
      Files.isRegularFile(written) should be(true)
      StorageDeviceConfig.loadHostConfig(written.toFile).storageDeviceId should be(deviceId)

  atest("reports ConfigWriteFailed, naming the device, when the write fails"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-xr-xr-x"))
    // A process running as root can still write to a read-only directory.
    assume(!Files.isWritable(dir))
    recoverToExceptionIf[StorageDeviceManager.ConfigWriteFailed](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))
      .flatMap: err =>
        err.getMessage should include(err.storageDeviceId.uuid.toString)
        // The device really was registered -- that is what makes the id actionable.
        waitForTransactionsToComplete().flatMap: _ =>
          client.getStorageDeviceState(err.storageDeviceId).map: ds =>
            ds.hostId should be(HostId.BootstrapHostId)

  atest("removes the config file it created when the write itself fails"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    val configFile = dir.resolve(StorageDeviceConfig.configFilename)
    // Only the two paths handed to createStorageDevice live on the failing filesystem;
    // every assertion below is made against the real one.
    val fs = FailAfterCreateFileSystem()
    recoverToExceptionIf[StorageDeviceManager.ConfigWriteFailed](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), fs.wrap(hostDir), fs.wrap(dir), bootstrapSet, systemId))
      .map: err =>
        // Both halves are needed. The first two show the failure really was the injected
        // mid-write one, with the file already created -- a failure at open would leave
        // nothing to roll back and make the last assertion pass for the wrong reason.
        fs.created should be(List(configFile))
        err.getCause.getMessage should be(FailAfterCreateFileSystem.InjectedWriteFailure)
        Files.exists(configFile) should be(false)

  atest("does not remove a config that appeared during the transaction"):
    given ExecutionContext = executionContext
    val hostDir = newHostDir()
    val dir = StorageDeviceManager.deviceDirectory(hostDir, "dev2")
    Files.createDirectories(dir)
    // Files.exists follows symlinks so the guard sees no config; the CREATE_NEW write
    // opens with O_EXCL, which fails on the link itself -- the same arm a lost race takes.
    val cfg = dir.resolve(StorageDeviceConfig.configFilename)
    Files.createSymbolicLink(cfg, dir.resolve("no-such-target"))
    recoverToExceptionIf[StorageDeviceManager.ConfigWriteFailed](
      StorageDeviceManager.createStorageDevice(
        client, hostConfig(), hostDir, dir, bootstrapSet, systemId))
      .map: err =>
        err.getCause shouldBe a[FileAlreadyExistsException]
        Files.exists(cfg, LinkOption.NOFOLLOW_LINKS) should be(true)


/** A filesystem identical to the default one except that a write to a freshly created file
 *  fails after the file exists: newOutputStream performs the real create, honouring the
 *  caller's open options, then hands back a stream that throws on the first write.
 *
 *  This scaffolding exists because nothing on a real filesystem reaches that arm here. A
 *  read-only directory fails at open, before any file exists, so the rollback has nothing
 *  to remove; a directory or a symlink occupying the config path is either rejected by the
 *  Files.exists guard or fails at open with FileAlreadyExists, which is the one arm that
 *  deliberately does not delete.
 */
private class FailAfterCreateFileSystem extends FileSystem:
  private val delegate = FileSystems.getDefault
  private val prov = FailAfterCreateProvider(this)

  /** Real paths this filesystem created before failing the write, in creation order. */
  var created: List[Path] = Nil

  /** Presents `p` to the code under test as a path belonging to this filesystem. */
  def wrap(p: Path): Path = WrappedPath(this, p)

  override def provider(): FileSystemProvider = prov
  override def close(): Unit = ()
  override def isOpen(): Boolean = true
  override def isReadOnly(): Boolean = false
  override def getSeparator(): String = delegate.getSeparator
  override def getRootDirectories(): java.lang.Iterable[Path] = throw new UnsupportedOperationException
  override def getFileStores(): java.lang.Iterable[FileStore] = throw new UnsupportedOperationException
  override def supportedFileAttributeViews(): java.util.Set[String] = delegate.supportedFileAttributeViews()
  override def getPath(first: String, more: String*): Path = wrap(delegate.getPath(first, more*))
  override def getPathMatcher(syntaxAndPattern: String): PathMatcher = throw new UnsupportedOperationException
  override def getUserPrincipalLookupService(): UserPrincipalLookupService = throw new UnsupportedOperationException
  override def newWatchService(): WatchService = throw new UnsupportedOperationException


private object FailAfterCreateFileSystem:
  /** Message carried by the injected failure, so a test can tell it from any other error. */
  val InjectedWriteFailure = "injected write failure"

  def unwrap(p: Path): Path = p match
    case w: WrappedPath => w.real
    case other => other


/** Delegates everything to the default provider except the create-then-write seam. */
private class FailAfterCreateProvider(fs: FailAfterCreateFileSystem) extends FileSystemProvider:
  import FailAfterCreateFileSystem.unwrap

  private def delegate = FileSystems.getDefault.provider()

  override def newOutputStream(path: Path, options: OpenOption*): OutputStream =
    val real = unwrap(path)
    // A real create with the caller's options, so CREATE_NEW still means CREATE_NEW, and
    // what survives a swallowed rollback is an empty file rather than a fabricated one.
    Files.newOutputStream(real, options*).close()
    fs.created = fs.created :+ real
    new OutputStream:
      override def write(b: Int): Unit =
        throw new IOException(FailAfterCreateFileSystem.InjectedWriteFailure)
      override def write(b: Array[Byte], off: Int, len: Int): Unit =
        throw new IOException(FailAfterCreateFileSystem.InjectedWriteFailure)

  // Files.write routes through newOutputStream above. Anything arriving here is a
  // different write path and must fail loudly rather than pass for the injected failure.
  override def newByteChannel(path: Path,
                              options: java.util.Set[? <: OpenOption],
                              attrs: FileAttribute[?]*): SeekableByteChannel =
    throw new UnsupportedOperationException("newByteChannel")

  override def getScheme(): String = "fail-after-create"
  override def newFileSystem(uri: URI, env: java.util.Map[String, ?]): FileSystem =
    throw new UnsupportedOperationException
  override def getFileSystem(uri: URI): FileSystem = fs
  override def getPath(uri: URI): Path = throw new UnsupportedOperationException
  override def newDirectoryStream(dir: Path, filter: DirectoryStream.Filter[? >: Path]): DirectoryStream[Path] =
    throw new UnsupportedOperationException
  override def createDirectory(dir: Path, attrs: FileAttribute[?]*): Unit =
    Files.createDirectory(unwrap(dir), attrs*)
  override def delete(path: Path): Unit = Files.delete(unwrap(path))
  override def copy(source: Path, target: Path, options: CopyOption*): Unit =
    throw new UnsupportedOperationException
  override def move(source: Path, target: Path, options: CopyOption*): Unit =
    throw new UnsupportedOperationException
  override def isSameFile(a: Path, b: Path): Boolean = Files.isSameFile(unwrap(a), unwrap(b))
  override def isHidden(path: Path): Boolean = Files.isHidden(unwrap(path))
  override def getFileStore(path: Path): FileStore = Files.getFileStore(unwrap(path))
  override def checkAccess(path: Path, modes: AccessMode*): Unit =
    delegate.checkAccess(unwrap(path), modes*)
  override def getFileAttributeView[V <: FileAttributeView](path: Path, tpe: Class[V], options: LinkOption*): V =
    Files.getFileAttributeView(unwrap(path), tpe, options*)
  override def readAttributes[A <: BasicFileAttributes](path: Path, tpe: Class[A], options: LinkOption*): A =
    Files.readAttributes(unwrap(path), tpe, options*)
  override def readAttributes(path: Path, attributes: String, options: LinkOption*): java.util.Map[String, Object] =
    Files.readAttributes(unwrap(path), attributes, options*)
  override def setAttribute(path: Path, attribute: String, value: Object, options: LinkOption*): Unit =
    throw new UnsupportedOperationException


/** A Path that delegates to `real` but reports `fs` as its filesystem, so every
 *  java.nio.file.Files call the code under test makes routes through fs's provider. */
private class WrappedPath(fs: FailAfterCreateFileSystem, val real: Path) extends Path:
  import FailAfterCreateFileSystem.unwrap

  private def rewrap(p: Path): Path = if p == null then null else WrappedPath(fs, p)

  override def getFileSystem(): FileSystem = fs
  override def isAbsolute(): Boolean = real.isAbsolute
  override def getRoot(): Path = rewrap(real.getRoot)
  override def getFileName(): Path = rewrap(real.getFileName)
  override def getParent(): Path = rewrap(real.getParent)
  override def getNameCount(): Int = real.getNameCount
  override def getName(index: Int): Path = rewrap(real.getName(index))
  override def subpath(begin: Int, end: Int): Path = rewrap(real.subpath(begin, end))
  override def startsWith(other: Path): Boolean = real.startsWith(unwrap(other))
  override def endsWith(other: Path): Boolean = real.endsWith(unwrap(other))
  override def normalize(): Path = rewrap(real.normalize)
  override def resolve(other: Path): Path = rewrap(real.resolve(unwrap(other)))
  override def relativize(other: Path): Path = rewrap(real.relativize(unwrap(other)))
  override def toUri(): URI = real.toUri
  override def toAbsolutePath(): Path = rewrap(real.toAbsolutePath)
  override def toRealPath(options: LinkOption*): Path = rewrap(real.toRealPath(options*))
  override def register(watcher: WatchService,
                        events: Array[WatchEvent.Kind[?]],
                        modifiers: WatchEvent.Modifier*): WatchKey =
    throw new UnsupportedOperationException
  override def compareTo(other: Path): Int = real.compareTo(unwrap(other))

  override def equals(other: Any): Boolean = other match
    case w: WrappedPath => real == w.real
    case _ => false
  override def hashCode(): Int = real.hashCode
  override def toString: String = real.toString
