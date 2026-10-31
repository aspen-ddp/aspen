package org.aspen_ddp.aspen.vdisk

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.vdisk.VDiskError.*

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** A [[LockableBlockDevice]] over a named disk. Opening loads the disk without owning it; `acquire` takes
  * ownership through [[VirtualDisk.acquire]]. A disk that has been fenced or has failed stays in place, reporting
  * its state and failing requests, until the next `acquire` replaces it.
  *
  * State changes are serialized on this object's monitor, which is never held across a Future.
  */
final class DiskHandle private (client: AspenClient,
                                name: String,
                                config: VDiskConfig,
                                loaded: DiskGeometry) extends LockableBlockDevice:

  private given ExecutionContext = client.clientContext

  private var disk: Option[VirtualDisk] = None
  /** Geometry to report while there is no owned disk */
  private var lastGeometry: DiskGeometry = loaded
  private var takeover: Option[Future[Unit]] = None
  /** Set for the whole of a `release`, so an `acquire` arriving meanwhile waits for it instead of seeing a disk
    * that is about to be closed
    */
  private var releasing: Option[Future[Unit]] = None
  /** Bumped by `close()` so that a takeover in flight when it ran discards its disk */
  private var generation = 0L

  def geometry: DiskGeometry = synchronized(disk.map(_.geometry).getOrElse(lastGeometry))

  def lockState: LockState = synchronized(disk).map(_.state) match
    case None => LockState.Unlocked
    case Some(DiskState.Serving) => LockState.Locked
    case Some(DiskState.Fenced) => LockState.Fenced
    case Some(DiskState.Failed(reason)) => LockState.Failed(reason)

  private def owned[A](f: VirtualDisk => Future[A]): Future[A] =
    synchronized(disk) match
      case None => Future.failed(NotOwner)
      case Some(d) => f(d)

  def read(offset: Long, length: Int): Future[Array[Byte]] = owned(_.read(offset, length))

  def write(offset: Long, data: Array[Byte], fua: Boolean): Future[Unit] = owned(_.write(offset, data, fua))

  def flush(): Future[Unit] = owned(_.flush())

  def unmap(ranges: List[(Long, Long)]): Future[Unit] = owned(_.unmap(ranges))

  def grow(newSize: Long): Future[Unit] = owned: d =>
    // A repeated RESIZE to the current size is not an error
    if newSize == d.geometry.size && d.state == DiskState.Serving then
      Future.unit
    else
      d.grow(newSize)

  def acquire(): Future[Unit] =
    synchronized(releasing) match
      case Some(r) => r.recover { case _ => () }.flatMap(_ => acquire())
      case None => acquireNow()

  private def acquireNow(): Future[Unit] =
    // Decided under the monitor; the Futures and the old disk's close run after it is released
    val (result, start) = synchronized:
      takeover match
        case Some(f) => (f, None)
        case None =>
          if disk.exists(_.state == DiskState.Serving) then
            (Future.unit, None)
          else
            val p = Promise[Unit]()
            takeover = Some(p.future)
            (p.future, Some((p, disk, generation)))
    start.foreach: (p, old, gen) =>
      old.foreach(_.close())
      VirtualDisk.acquire(client, name, config).onComplete: r =>
        val discarded = synchronized:
          takeover = None
          r match
            case Success(d) if gen == generation =>
              disk = Some(d)
              None
            case Success(d) => Some(d)
            case Failure(_) => None
        discarded.foreach(_.close())
        r match
          case Success(_) if discarded.isDefined => p.failure(Fenced)
          case Success(_) => p.success(())
          case Failure(e) => p.failure(e)
    result

  def release(): Future[Unit] =
    val (result, start) = synchronized:
      releasing match
        case Some(f) => (f, None)
        case None =>
          val p = Promise[Unit]()
          releasing = Some(p.future)
          (p.future, Some(p))
    start.foreach: p =>
      // A failed takeover leaves nothing to wait for; release then closes whatever is held
      val settled = synchronized(takeover).map(_.recover { case _ => () }).getOrElse(Future.unit)
      p.completeWith:
        settled.flatMap: _ =>
          synchronized(disk) match
            case None => Future.unit
            case Some(d) =>
              val flushed = if d.state == DiskState.Serving then d.flush() else Future.unit
              flushed.transform: r =>
                drop(d)
                r
        .transform: r =>
          synchronized { releasing = None }
          r
    result

  /** Crash-only: closes the owned disk without flushing */
  def close(): Unit =
    val old = synchronized:
      generation += 1
      disk
    old.foreach(drop)

  private def drop(d: VirtualDisk): Unit =
    d.close()
    synchronized:
      if disk.exists(_ eq d) then
        lastGeometry = d.geometry
        disk = None

object DiskHandle:

  /** Loads the disk without taking ownership of it */
  def open(client: AspenClient, name: String, config: VDiskConfig = VDiskConfig()): Future[DiskHandle] =
    DiskCatalog.load(client, name).map(info => new DiskHandle(client, name, config, info.geometry))(
      client.clientContext)
