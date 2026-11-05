package org.aspen_ddp.aspen.vdisk.tcmu

import org.aspen_ddp.aspen.vdisk.{DiskGeometry, LockState, LockableBlockDevice}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}

/** A scriptable [[LockableBlockDevice]] for tests.
  *
  * Every call is logged and gets a Promise. With `auto` set the Promise is completed at once with the default
  * result (or the injected failure); otherwise it stays pending until the test completes it through the returned
  * [[FakeBlockDevice.Call]]. `failures` fails a named operation's Future; `throws` makes the named method throw
  * synchronously. `call(n)` gives a Future of the n-th call (0-based), so a test can wait for a call without
  * blocking.
  */
final class FakeBlockDevice(
    @volatile var geo: DiskGeometry = DiskGeometry(8L * 1024 * 1024, 4096, 16384),
    @volatile var auto: Boolean = true) extends LockableBlockDevice:

  import FakeBlockDevice.Call

  @volatile var state: LockState = LockState.Locked
  /** Data returned by a read of (offset, length) */
  @volatile var readData: (Long, Int) => Array[Byte] = (offset, length) =>
    Array.tabulate(length)(i => ((offset + i) % 251).toByte)
  @volatile var failures: Map[String, Throwable] = Map.empty
  @volatile var throws: Map[String, Throwable] = Map.empty

  private val log = ListBuffer[Call]()
  private val waiters = ListBuffer[(Int, Promise[Call])]()
  @volatile var closes = 0

  /** Every call so far, in order */
  def calls: List[Call] = synchronized(log.toList)

  /** Name and arguments of every call so far. Write data is given as a List so it compares by value. */
  def logged: List[(String, List[Any])] = calls.map(c => (c.name, c.args))

  def names: List[String] = calls.map(_.name)

  /** Completes with the n-th call (0-based) once it has been made */
  def call(n: Int): Future[Call] = synchronized:
    if n < log.size then
      Future.successful(log(n))
    else
      val p = Promise[Call]()
      waiters += ((n, p))
      p.future

  private def record[A](name: String, args: List[Any], default: => A): Future[A] =
    throws.get(name).foreach(e => throw e)
    val c = Call(name, args, Promise[Any]())
    val ready = synchronized:
      log += c
      val n = log.size - 1
      val (now, later) = waiters.partition(_._1 == n)
      waiters.clear()
      waiters ++= later
      now.map(_._2)
    if auto then
      failures.get(name) match
        case Some(e) => c.promise.failure(e)
        case None => c.promise.success(default)
    ready.foreach(_.success(c))
    c.promise.future.asInstanceOf[Future[A]]

  def geometry: DiskGeometry = geo

  def read(offset: Long, length: Int): Future[Array[Byte]] =
    record("read", List(offset, length), readData(offset, length))
  def write(offset: Long, data: Array[Byte], fua: Boolean): Future[Unit] =
    record("write", List(offset, data.toList, fua), ())
  def flush(): Future[Unit] = record("flush", Nil, ())
  def unmap(ranges: List[(Long, Long)]): Future[Unit] = record("unmap", List(ranges), ())
  def grow(newSize: Long): Future[Unit] = record("grow", List(newSize), ())
  def acquire(): Future[Unit] = record("acquire", Nil, ())
  def release(): Future[Unit] = record("release", Nil, ())
  def lockState: LockState = state
  def close(): Unit = synchronized(closes += 1)

object FakeBlockDevice:
  /** One device call. A held call is completed by the test with `succeed` or `fail`. */
  final case class Call(name: String, args: List[Any], promise: Promise[Any]):
    def succeed(value: Any = ()): Unit = promise.success(value)
    def fail(e: Throwable): Unit = promise.failure(e)
