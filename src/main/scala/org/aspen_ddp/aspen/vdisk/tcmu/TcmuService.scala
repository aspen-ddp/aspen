package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.{DiskGeometry, LockableBlockDevice, VDiskError}
import org.aspen_ddp.aspen.vdisk.tcmu.ring.Ring
import org.aspen_ddp.aspen.vdisk.tcmu.scsi.ScsiCommands
import org.aspen_ddp.aspen.vdisk.tcmu.sys.{Libc, Linux}
import scribe.Level

import java.lang.foreign.{Arena, MemorySegment}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success
import scala.util.control.NonFatal

/** The kernel side of TCMU device lifecycle: configfs and the UIO command rings. */
trait TcmuKernel:
  def configfs: Configfs

  /** Maps the command ring of `uio<minor>` (`mapSize` bytes) and opens its notification channel.
    *
    * The returned closer unmaps the ring and closes the channel. It is idempotent. The caller must first stop
    * everything that uses the segment or the channel (`TcmuDevice.stop()`).
    */
  def openRing(minor: Int, mapSize: Long): (MemorySegment, KernelChannel, () => Unit)

/** The real kernel: `/dev/uioN` opened `O_RDWR|O_CLOEXEC`, mapped shared in its own shared arena. */
final class LinuxTcmuKernel(val configfs: Configfs = Configfs()) extends TcmuKernel:

  def openRing(minor: Int, mapSize: Long): (MemorySegment, KernelChannel, () => Unit) =
    val fd = Libc.open(configfs.uioDevicePath(minor).toString, Linux.O_RDWR | Linux.O_CLOEXEC)
    var arena: Arena = null
    try
      arena = Arena.ofShared()
      val segment = Libc.mmapShared(fd, mapSize, arena)
      // The channel owns the fd from here on
      val channel = UioChannel(fd)
      val mapped = arena
      val closed = AtomicBoolean(false)
      val closer = () =>
        if closed.compareAndSet(false, true) then
          // Unmap first: the caller has stopped every user of the segment, and the arena's close makes any stray
          // access throw rather than fault. The channel, and with it the fd, is closed last.
          try mapped.close()
          finally channel.close()
      (segment, channel, closer)
    catch
      case e: Throwable =>
        if arena != null then
          try arena.close()
          catch case NonFatal(c) => e.addSuppressed(c)
        try Libc.close(fd)
        catch case NonFatal(c) => e.addSuppressed(c)
        throw e

/** @param acquireTimeout bounds every disk operation a lifecycle event waits for, since the kernel blocks targetcli
  *                       until it gets the reply: an add's open, acquire and grow (an add that has not finished one
  *                       within this time replies `-ETIMEDOUT` and is torn down), a reconfig's grow (`-ETIMEDOUT`;
  *                       the device keeps serving its old size) and a remove's release (the disk is then closed
  *                       crash-only and the remove replies 0).
  */
final case class TcmuServiceConfig(acquireTimeout: FiniteDuration = 60.seconds)

/** Brings TCMU devices up and down as the kernel adds, removes and reconfigures them.
  *
  * Operations on one minor run one at a time, in the order they were called: a remove or reconfig that arrives
  * while an add is still acquiring its disk waits for that add. A failed add leaves nothing open: the disk is
  * closed crash-only and the ring is unmapped.
  *
  * Statuses are `0` or `-errno`, as the kernel expects in the netlink reply.
  *
  * `log` is the sink for every line this service and the devices it serves log, scribe by default.
  */
final class TcmuService(kernel: TcmuKernel,
                        openDisk: String => Future[LockableBlockDevice],
                        config: TcmuServiceConfig = TcmuServiceConfig(),
                        log: (Level, String) => Unit = FailureLog.toScribe)(using ExecutionContext):
  import TcmuService.*
  import Linux.{EINVAL, EIO, ENODEV, ENOENT, ETIMEDOUT}

  private final class Served(val minor: Int, val uio: UioName, val disk: String, val name: String,
                             val device: LockableBlockDevice, val commands: ScsiCommands, val tcmu: TcmuDevice,
                             val unmap: () => Unit):
    def label: String = TcmuService.label(name, minor, disk)

  /** What a device add has opened so far, for its failure log and teardown. */
  private final class Adding(val minor: Int):
    @volatile var step = "read uio name"
    @volatile var name = "?"
    @volatile var disk = "?"
    @volatile var device: Option[LockableBlockDevice] = None
    @volatile var unmap: Option[() => Unit] = None
    @volatile var tcmu: Option[TcmuDevice] = None
    def where: String = s"device ${label(name, minor, disk)}"

  private final case class KernelView(uio: UioName, size: Long, blockSize: Int, maxSectors: Int, mapSize: Long,
                                      serial: Option[String])

  // Guarded by this
  private val served = mutable.HashMap[Int, Served]()
  /** minor -> completion of the last operation queued for it */
  private val tails = mutable.HashMap[Int, Future[Unit]]()

  def devices: Map[Int, String] = synchronized(served.view.mapValues(_.disk).toMap)

  def onEvent(event: TcmuEvent): Future[Int] =
    val result =
      try
        event match
          case TcmuEvent.Added(_, minor, _) => added(minor, reopen = false)
          case TcmuEvent.Removed(_, minor, _) => removed(minor)
          case TcmuEvent.Reconfig(_, minor, _, change) => reconfig(minor, change)
      catch case NonFatal(e) => Future.failed(e)
    result.map(_ => 0).recover:
      case LifecycleError(errno, _) => -errno
      case NonFatal(e) =>
        log(Level.Error, s"tcmu: $event failed: $e")
        -EIO

  /** Adds every `aspen` UIO device already present, with `reopen`. One failure does not stop the others. */
  def startupScan(): Future[Unit] =
    Future(kernel.configfs.scanUio()).flatMap: found =>
      val adds = found.flatMap: (minor, uio) =>
        if uio.subtype != Subtype then
          log(Level.Debug, s"tcmu: startup scan: uio$minor has subtype ${uio.subtype}; not ours")
          None
        else
          Some(added(minor, reopen = true).recover:
            case NonFatal(e) => log(Level.Warn, s"tcmu: startup scan: uio$minor (${uio.device}) not served: $e"))
      Future.sequence(adds).map(_ => ())

  def added(minor: Int, reopen: Boolean): Future[Unit] = serialized(minor)(add(minor, reopen))

  /** Stops serving, releases the disk, then unmaps. Always succeeds: the kernel removes the device regardless. A
    * release that has not finished within `acquireTimeout` is abandoned and the disk closed crash-only (its lock is
    * not released).
    */
  def removed(minor: Int): Future[Unit] = serialized(minor):
    synchronized(served.remove(minor)) match
      case None =>
        log(Level.Warn, s"tcmu: remove of uio$minor: no such device")
        Future.unit
      case Some(s) =>
        val where = s"device ${s.label}"
        Future(logged(where, "stop serving")(s.tcmu.stop()))
          .flatMap: _ =>
            withTimeout("released", attempt(s.device.release())).recover:
              case LifecycleError(ETIMEDOUT, message) =>
                log(Level.Error, s"tcmu: removing $where: release failed: $message; closing the disk crash-only")
                logged(where, "close disk")(s.device.close())
              case NonFatal(e) => log(Level.Error, s"tcmu: removing $where: release failed: $e")
          .map: _ =>
            logged(where, "unmap")(s.unmap())
            log(Level.Info, s"tcmu: removed $where")

  def reconfig(minor: Int, change: ReconfigChange): Future[Unit] = serialized(minor):
    synchronized(served.get(minor)) match
      case None =>
        log(Level.Error, s"tcmu: reconfig of uio$minor ($change): no such device")
        Future.failed(LifecycleError(ENODEV, s"uio$minor is not served"))
      case Some(s) =>
        val where = s"device ${s.label}"
        val result = change match
          case ReconfigChange.Size(n) => resize(s, n)
          case ReconfigChange.Config(_) => Future.failed(LifecycleError(EINVAL, "changing the cfgstring is not supported"))
          case ReconfigChange.WriteCache(_) =>
            Future.failed(LifecycleError(EINVAL, "changing the write cache setting is not supported"))
          case ReconfigChange.Unknown => Future.failed(LifecycleError(EINVAL, "unknown reconfig attribute"))
        result.recoverWith:
          case NonFatal(e) =>
            val error = asLifecycle(e)
            log(Level.Error, s"tcmu: reconfig of $where ($change) failed: errno ${error.errno}: ${error.message}")
            Future.failed(error)

  private def resize(s: Served, n: Long): Future[Unit] =
    val current = s.commands.settings
    if n < current.size then
      Future.failed(LifecycleError(EINVAL, s"shrinking from ${current.size} to $n is not supported"))
    else if n % current.blockSize != 0 then
      Future.failed(LifecycleError(EINVAL, s"size $n is not a multiple of the block size ${current.blockSize}"))
    else if n == current.size then
      Future.unit
    else
      val grown = if n > s.device.geometry.size then growDisk(s.device, n) else Future.unit
      grown.map: _ =>
        s.commands.resized(n)
        log(Level.Info, s"tcmu: device ${s.label} resized from ${current.size} to $n")

  /** Grows the Aspen disk, within `acquireTimeout` */
  private def growDisk(device: LockableBlockDevice, size: Long): Future[Unit] =
    withTimeout("grown", mapped(device.grow(size)) {
      case _: VDiskError.InvalidGeometry | _: VDiskError.InvalidRequest => EINVAL
    })

  private def add(minor: Int, reopen: Boolean): Future[Unit] =
    val a = Adding(minor)
    val result =
      Future(readKernel(a)).flatMap:
        case None => Future.unit
        case Some(k) =>
          a.step = "open disk"
          // A disk that opens only after the timeout is closed when it does
          mapped(withTimeout("opened", attempt(openDisk(a.disk)), late = _.close())) {
            case _: VDiskError.DiskNotFound => ENOENT
          }.flatMap: d =>
            a.device = Some(d)
            a.step = "check geometry"
            val geometry = d.geometry
            checkGeometry(k, geometry)
            a.step = "acquire"
            mapped(withTimeout("acquired", d.acquire()))(PartialFunction.empty).flatMap: _ =>
              // The size as of the acquire: a concurrent grow-disk may have changed it since the open
              val size = d.geometry.size
              if k.size > size then
                a.step = "grow"
                log(Level.Warn, s"tcmu: ${a.where}: growing Aspen disk ${a.disk} from $size to ${k.size} " +
                  "to match the targetcli size")
                growDisk(d, k.size)
              else
                Future.unit
            .map(_ => serve(a, k, d, geometry, reopen))
    result.recoverWith:
      case NonFatal(e) =>
        val error = asLifecycle(e)
        log(Level.Error, s"tcmu: adding ${a.where} failed at ${a.step}: errno ${error.errno}: ${error.message}")
        teardown(a)
        Future.failed(error)

  /** Steps 1 to 3 of an add: the UIO name, the cfgstring and the configfs attributes. None when the minor is
    * already served under the same UIO name: the startup scan found a device whose ADDED event came after it.
    */
  private def readKernel(a: Adding): Option[KernelView] =
    val parsed =
      try kernel.configfs.uioName(a.minor)
      catch case NonFatal(e) => throw LifecycleError(ENODEV, e.getMessage)
    val uio = parsed.fold(reason => throw LifecycleError(ENODEV, reason), identity)
    a.name = uio.device
    synchronized(served.get(a.minor)) match
      case Some(s) if s.uio == uio =>
        log(Level.Info, s"tcmu: uio${a.minor} (${uio.device}) is already served; nothing to do")
        return None
      case Some(s) => dropStale(a.minor, s, uio)
      case None => ()
    if uio.subtype != Subtype then
      throw LifecycleError(ENODEV, s"subtype is ${uio.subtype}, not $Subtype")
    a.step = "parse cfgstring"
    a.disk = Cfgstring.parse(uio.cfgstring).fold(r => throw LifecycleError(EINVAL, r), identity)
    a.step = "read configfs"
    val c = kernel.configfs
    try
      Some(KernelView(uio, c.size(uio.hba, uio.device), c.hwBlockSize(uio.hba, uio.device),
        c.hwMaxSectors(uio.hba, uio.device), c.mapSize(a.minor), c.unitSerial(uio.hba, uio.device)))
    catch case NonFatal(e) => throw LifecycleError(EIO, e.getMessage)

  /** The kernel reused a served minor for another device: it unregistered the old one without a remove, which it
    * does when an add's reply was not 0. Crash-only: stop serving, close the disk (its lock is not released), unmap.
    */
  private def dropStale(minor: Int, s: Served, now: UioName): Unit =
    val where = s"device ${s.label}"
    log(Level.Warn, s"tcmu: uio$minor is now ${now.device} (${now.cfgstring}); dropping the stale $where")
    synchronized(served.remove(minor)): Unit
    logged(where, "stop serving")(s.tcmu.stop())
    logged(where, "close disk")(s.device.close())
    logged(where, "unmap")(s.unmap())

  private def checkGeometry(k: KernelView, g: DiskGeometry): Unit =
    if k.blockSize != g.logicalBlockSize then
      throw LifecycleError(EINVAL, s"configfs block size ${k.blockSize} does not match the disk's ${g.logicalBlockSize}")
    if g.blockObjectSize <= 0 || g.blockObjectSize % g.logicalBlockSize != 0 then
      throw LifecycleError(EINVAL,
        s"the disk's block object size ${g.blockObjectSize} is not a multiple of its block size ${g.logicalBlockSize}")
    if k.size % k.blockSize != 0 then
      throw LifecycleError(EINVAL, s"configfs size ${k.size} is not a multiple of the block size ${k.blockSize}")

  /** Steps 7 to 10 of an add, once the disk is acquired (and grown). */
  private def serve(a: Adding, k: KernelView, device: LockableBlockDevice, geometry: DiskGeometry,
                    reopen: Boolean): Unit =
    val control = kernel.configfs.control(k.uio.hba, k.uio.device)
    if reopen then
      a.step = "reset ring"
      // As libtcmu: a failed reset is logged and the device is opened anyway
      try control.resetRing()
      catch case NonFatal(e) => log(Level.Warn, s"tcmu: ${a.where}: Could not reset ring: $e")
    a.step = "open ring"
    val (segment, channel, unmap) =
      try kernel.openRing(a.minor, k.mapSize)
      catch case NonFatal(e) => throw LifecycleError(EIO, e.toString)
    a.unmap = Some(unmap)
    val ring = Ring.open(segment).fold(r => throw LifecycleError(EINVAL, s"invalid ring: $r"), identity)
    a.step = "start serving"
    val settings = DeviceSettings(k.size, k.blockSize, k.maxSectors, geometry.blockObjectSize, k.uio.device,
      k.serial.getOrElse(a.disk))
    // Every report names the device, its UIO minor and its disk
    val failures = FailureLog(label(k.uio.device, a.minor, a.disk), log = log)
    val commands = ScsiCommands(device, settings, failures)
    val tcmu = TcmuDevice(k.uio.device, ring, channel, control, commands, failures)
    a.tcmu = Some(tcmu)
    tcmu.start()
    synchronized:
      served(a.minor) = Served(a.minor, k.uio, a.disk, k.uio.device, device, commands, tcmu, unmap)
    log(Level.Info, s"tcmu: serving ${a.where}: ${k.size} bytes, block size ${k.blockSize}")

  /** Crash-only: stop serving, unmap, close the disk. */
  private def teardown(a: Adding): Unit =
    a.tcmu.foreach(t => logged(a.where, "stop serving")(t.stop()))
    a.unmap.foreach(u => logged(a.where, "unmap")(u()))
    a.device.foreach(d => logged(a.where, "close disk")(d.close()))

  private def logged(where: String, what: String)(body: => Unit): Unit =
    try body
    catch case NonFatal(e) => log(Level.Error, s"tcmu: $where: $what failed: $e")

  /** `f`, failed with ETIMEDOUT ("not <what> within ...") if it has not completed within `acquireTimeout`. A
    * success that arrives after the timeout is passed to `late`.
    */
  private def withTimeout[T](what: String, f: Future[T], late: T => Unit = (_: T) => ()): Future[T] =
    val p = Promise[T]()
    val timeout = config.acquireTimeout
    val task = timer.schedule((() => p.tryFailure(LifecycleError(ETIMEDOUT, s"not $what within $timeout"))): Runnable,
      timeout.toNanos, TimeUnit.NANOSECONDS)
    f.onComplete: r =>
      task.cancel(false)
      if !p.tryComplete(r) then
        r.foreach(v => logged(s"a late result ($what)", "clean up")(late(v)))
    p.future

  /** Runs `op` once every earlier operation on `minor` has completed. */
  private def serialized[T](minor: Int)(op: => Future[T]): Future[T] =
    val p = Promise[T]()
    val done = p.future.transform(_ => Success(()))(using ExecutionContext.parasitic)
    val previous = synchronized:
      val prev = tails.getOrElse(minor, Future.unit)
      tails(minor) = done
      prev
    previous.onComplete(_ => p.completeWith(attempt(op)))
    done.onComplete: _ =>
      synchronized:
        if tails.get(minor).exists(_ eq done) then tails.remove(minor): Unit
    p.future

object TcmuService:
  /** `device (uioN, disk D)`: how every log line names a device */
  def label(device: String, minor: Int, disk: String): String = s"$device (uio$minor, disk $disk)"

  /** The UIO subtype of devices this service serves: the handler name in `targetcli /backstores/user:aspen` */
  val Subtype = "aspen"

  /** A lifecycle step failed; `errno` is positive and the reply status is `-errno`. */
  final case class LifecycleError(errno: Int, message: String) extends Exception(message)

  private lazy val timer: ScheduledThreadPoolExecutor =
    val t = ScheduledThreadPoolExecutor(1, r => Thread.ofPlatform().name("tcmu-timeout").daemon(true).unstarted(r))
    t.setRemoveOnCancelPolicy(true)
    t

  private def attempt[T](f: => Future[T]): Future[T] =
    try f
    catch case NonFatal(e) => Future.failed(e)

  private def asLifecycle(e: Throwable): LifecycleError =
    e match
      case l: LifecycleError => l
      case _ => LifecycleError(Linux.EIO, e.toString)

  /** `f`, with a failure that is not already a LifecycleError mapped to one: `errnoOf`, or EIO */
  private def mapped[T](f: => Future[T])(errnoOf: PartialFunction[Throwable, Int])(using ExecutionContext): Future[T] =
    attempt(f).recover:
      case e: LifecycleError => throw e
      case NonFatal(e) => throw LifecycleError(errnoOf.applyOrElse(e, _ => Linux.EIO), e.toString)
