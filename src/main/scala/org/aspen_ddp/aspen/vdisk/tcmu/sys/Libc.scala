package org.aspen_ddp.aspen.vdisk.tcmu.sys

import java.lang.foreign.ValueLayout.{ADDRESS, JAVA_INT, JAVA_LONG, JAVA_SHORT}
import java.lang.foreign.{Arena, FunctionDescriptor, Linker, MemoryLayout, MemorySegment}
import java.lang.invoke.MethodHandle
import java.util.function.Consumer

final case class SysError(call: String, errno: Int, message: String)
  extends Exception(s"$call failed: errno $errno ($message)")

object Linux:
  val O_RDONLY = 0
  val O_RDWR = 2
  val O_CLOEXEC = 0x80000
  val PROT_READ = 1
  val PROT_WRITE = 2
  val MAP_SHARED = 1
  val AF_NETLINK = 16
  val SOCK_RAW = 3
  val SOCK_CLOEXEC = 0x80000
  val NETLINK_GENERIC = 16
  val SOL_NETLINK = 270
  val NETLINK_ADD_MEMBERSHIP = 1
  val EINTR = 4
  val ENOENT = 2
  val EAGAIN = 11
  val EINVAL = 22
  val EBUSY = 16
  val ENODEV = 19
  val POLLIN = 1

/** Thin Foreign Function & Memory binding to the handful of libc calls the TCMU frontend needs. */
object Libc:
  import Linux.*

  private val linker = Linker.nativeLinker()
  private val lookup = linker.defaultLookup()
  private val captureOpt = Linker.Option.captureCallState("errno")
  private val stateLayout = Linker.Option.captureStateLayout()
  private val errnoHandle = stateLayout.varHandle(MemoryLayout.PathElement.groupElement("errno"))

  private def fn(name: String, desc: FunctionDescriptor, opts: Linker.Option*): MethodHandle =
    linker.downcallHandle(lookup.find(name).get(), desc, opts*)

  private lazy val openH = fn("open", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT),
    captureOpt, Linker.Option.firstVariadicArg(2))
  private lazy val closeH = fn("close", FunctionDescriptor.of(JAVA_INT, JAVA_INT), captureOpt)
  private lazy val readH = fn("read", FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG), captureOpt)
  private lazy val writeH = fn("write", FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG), captureOpt)
  private lazy val mmapH = fn("mmap",
    FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG, JAVA_INT, JAVA_INT, JAVA_INT, JAVA_LONG), captureOpt)
  private lazy val munmapH = fn("munmap", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG))
  private lazy val socketH = fn("socket", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT), captureOpt)
  private lazy val bindH = fn("bind", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT), captureOpt)
  private lazy val setsockoptH = fn("setsockopt",
    FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT), captureOpt)
  private lazy val sendH = fn("send",
    FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT), captureOpt)
  private lazy val recvH = fn("recv",
    FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT), captureOpt)
  private lazy val pollH = fn("poll", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT), captureOpt)
  private lazy val pipeH = fn("pipe", FunctionDescriptor.of(JAVA_INT, ADDRESS), captureOpt)
  private lazy val geteuidH = fn("geteuid", FunctionDescriptor.of(JAVA_INT))
  private lazy val strerrorH = fn("strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT))

  private def errnoOf(state: MemorySegment): Int =
    errnoHandle.get(state, 0L).asInstanceOf[Int]

  private def strerror(errno: Int): String =
    val p = strerrorH.invokeExact(errno): MemorySegment
    p.reinterpret(Long.MaxValue).getString(0)

  private def fail(call: String, state: MemorySegment): Nothing =
    val e = errnoOf(state)
    throw SysError(call, e, strerror(e))

  /** Runs a call with a fresh errno capture segment. */
  private def withState[T](body: (Arena, MemorySegment) => T): T =
    val arena = Arena.ofConfined()
    try body(arena, arena.allocate(stateLayout))
    finally arena.close()

  private def checkInt(call: String, state: MemorySegment, rc: Int): Int =
    if rc == -1 then fail(call, state) else rc

  /** Retries while the call fails with EINTR. */
  private def retrying(call: String)(attempt: MemorySegment => Long): Long =
    withState: (_, state) =>
      var rc = attempt(state)
      while rc == -1L && errnoOf(state) == EINTR do
        rc = attempt(state)
      if rc == -1L then fail(call, state) else rc

  def open(path: String, flags: Int): Int =
    withState: (arena, state) =>
      val p = arena.allocateFrom(path)
      checkInt("open", state, openH.invokeExact(state, p, flags, 0): Int)

  def close(fd: Int): Unit =
    withState: (_, state) =>
      checkInt("close", state, closeH.invokeExact(state, fd): Int)
      ()

  def read(fd: Int, buf: MemorySegment): Long =
    retrying("read")(state => readH.invokeExact(state, fd, buf, buf.byteSize()): Long)

  def write(fd: Int, buf: MemorySegment): Long =
    retrying("write")(state => writeH.invokeExact(state, fd, buf, buf.byteSize()): Long)

  /** Maps `size` bytes of `fd` read/write and shared. The mapping is unmapped when `arena` closes. */
  def mmapShared(fd: Int, size: Long, arena: Arena): MemorySegment =
    withState: (_, state) =>
      val addr = mmapH.invokeExact(state, MemorySegment.NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0L)
        : MemorySegment
      if addr.address() == -1L then fail("mmap", state)
      val cleanup: Consumer[MemorySegment] = seg =>
        munmapH.invokeExact(seg, size): Int
        ()
      addr.reinterpret(size, arena, cleanup)

  def socket(domain: Int, tpe: Int, protocol: Int): Int =
    withState: (_, state) =>
      checkInt("socket", state, socketH.invokeExact(state, domain, tpe, protocol): Int)

  def bind(fd: Int, addr: MemorySegment): Unit =
    withState: (_, state) =>
      checkInt("bind", state, bindH.invokeExact(state, fd, addr, addr.byteSize().toInt): Int)
      ()

  def setsockoptInt(fd: Int, level: Int, name: Int, value: Int): Unit =
    withState: (arena, state) =>
      val v = arena.allocate(JAVA_INT)
      v.set(JAVA_INT, 0L, value)
      checkInt("setsockopt", state, setsockoptH.invokeExact(state, fd, level, name, v, 4): Int)
      ()

  def send(fd: Int, buf: MemorySegment): Long =
    retrying("send")(state => sendH.invokeExact(state, fd, buf, buf.byteSize(), 0): Long)

  def recv(fd: Int, buf: MemorySegment): Long =
    retrying("recv")(state => recvH.invokeExact(state, fd, buf, buf.byteSize(), 0): Long)

  /** Waits for events on `(fd, events)` pairs; returns each fd's `revents`. EINTR restarts the whole timeout. */
  def poll(fds: Seq[(Int, Int)], timeoutMs: Int): Seq[Int] =
    withState: (arena, state) =>
      val pfds = arena.allocate(8L * fds.size, 8)
      for ((fd, events), i) <- fds.zipWithIndex do
        pfds.set(JAVA_INT, 8L * i, fd)
        pfds.set(JAVA_SHORT, 8L * i + 4, events.toShort)
        pfds.set(JAVA_SHORT, 8L * i + 6, 0.toShort)
      var rc = pollH.invokeExact(state, pfds, fds.size.toLong, timeoutMs): Int
      while rc == -1 && errnoOf(state) == EINTR do
        rc = pollH.invokeExact(state, pfds, fds.size.toLong, timeoutMs): Int
      checkInt("poll", state, rc)
      fds.indices.map(i => pfds.get(JAVA_SHORT, 8L * i + 6) & 0xffff)

  def geteuid(): Int =
    geteuidH.invokeExact(): Int

  /** Test helper: returns (readFd, writeFd). */
  def pipe(): (Int, Int) =
    withState: (arena, state) =>
      val fds = arena.allocate(8)
      checkInt("pipe", state, pipeH.invokeExact(state, fds): Int)
      (fds.get(JAVA_INT, 0L), fds.get(JAVA_INT, 4L))
