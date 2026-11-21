package org.aspen_ddp.aspen.server

import scribe.Logging

import java.io.IOException
import java.lang.foreign.ValueLayout.{ADDRESS, JAVA_INT, JAVA_LONG}
import java.lang.foreign.{Arena, FunctionDescriptor, Linker, MemoryLayout, MemorySegment}
import java.lang.invoke.MethodHandle
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicBoolean

/** Creates files whose every block is allocated, so later writes inside them never allocate and cannot fail with
 *  ENOSPC (the CRL's stream files: crash-recovery-log AS7, 5.1).
 *
 *  On Linux the blocks are reserved with fallocate(2), mode 0, called through FFM: O(extents), nothing written. ext4
 *  keeps reserved clusters for converting the unwritten extents this leaves on their first write, so that conversion
 *  cannot fail with ENOSPC either. `fallocate` rather than glibc's `posix_fallocate`, which falls back to writing zeros
 *  itself and so would hide which path ran. Elsewhere, or where the file system does not support it (EOPNOTSUPP), or
 *  when native access is refused, the file is filled with zeros: portable, and slow only on spinning disks. Both read
 *  as zeros.
 *
 *  Neither helps on a copy-on-write file system (btrfs, ZFS), which allocates on every overwrite: see `isCopyOnWrite`.
 */
object FilePreallocation extends Logging:

  enum Mode:
    /** fallocate where it works, zero-fill otherwise */
    case Auto
    /** fallocate only: throws if it is unavailable. For tests */
    case Fallocate
    /** zero-fill only */
    case ZeroFill

  /** Thrown when fallocate cannot be used here: not Linux, no native access, or the file system does not support it */
  class FallocateUnsupported(reason: String) extends IOException(s"fallocate is unavailable: $reason")

  private val warnedFallback = new AtomicBoolean(false)

  /** Creates `path`, which must not exist, `size` bytes long with every block allocated, and forces it to disk. The
   *  directory entry is not forced: the caller renames the file into place and forces the directory. A failure removes
   *  the partial file before rethrowing. */
  def preallocate(path: os.Path, size: Long, mode: Mode = Mode.Auto): Unit =
    require(size >= 0, s"negative size $size")
    // Checked first, so the cleanup below only ever removes a file this call created. A stat that fails throws
    // (FileStat), creating nothing
    if FileStat.exists(path, followLinks = false) then
      throw new java.nio.file.FileAlreadyExistsException(path.toString)
    try
      mode match
        case Mode.ZeroFill => zeroFill(path, size)
        case Mode.Fallocate => Native.fallocate(path, size)
        case Mode.Auto =>
          try Native.fallocate(path, size)
          catch case e: FallocateUnsupported =>
            if warnedFallback.compareAndSet(false, true) then
              logger.warn(s"Preallocating files by writing zeros: ${e.getMessage}")
            os.remove(path, checkExists = false)
            zeroFill(path, size)
    catch case t: Throwable =>
      try os.remove(path, checkExists = false)
      catch case r: Throwable => t.addSuppressed(r)
      throw t

  /** Why fallocate cannot be called in this process, if it cannot (not Linux, no native access, no such symbol).
   *  A file system without support is found only by a call */
  def fallocateUnavailable: Option[String] = Native.unavailable

  /** Whether `dir` is on a copy-on-write file system, where an overwrite allocates and preallocation does not help */
  def isCopyOnWrite(dir: os.Path): Boolean =
    try
      val t = java.nio.file.Files.getFileStore(dir.toNIO).`type`().toLowerCase
      t == "btrfs" || t == "zfs"
    catch case _: Throwable => false

  private val ZeroChunk = 1024 * 1024

  private def zeroFill(path: os.Path, size: Long): Unit =
    val channel = FileChannel.open(path.toNIO, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    try
      val zeros = ByteBuffer.allocateDirect(ZeroChunk)
      var offset = 0L
      while offset < size do
        zeros.clear()
        zeros.limit(math.min(ZeroChunk.toLong, size - offset).toInt)
        while zeros.hasRemaining do
          offset += channel.write(zeros, offset)
      channel.force(true)
    finally channel.close()

  /** The libc calls fallocate needs, bound on first use. Any failure to bind is FallocateUnsupported */
  private object Native:
    private val O_WRONLY = 1
    private val O_CREAT = 0x40
    private val O_EXCL = 0x80
    private val O_CLOEXEC = 0x80000
    private val EINTR = 4
    private val EOPNOTSUPP = 95

    private final class Handles(val open: MethodHandle, val fallocate: MethodHandle, val fsync: MethodHandle,
                                val close: MethodHandle, val strerror: MethodHandle,
                                val stateLayout: MemoryLayout, val errno: java.lang.invoke.VarHandle)

    private lazy val handles: Either[String, Handles] =
      if !System.getProperty("os.name", "").toLowerCase.startsWith("linux") then
        Left(s"not Linux (${System.getProperty("os.name")})")
      else
        try
          val linker = Linker.nativeLinker()
          val lookup = linker.defaultLookup()
          val capture = Linker.Option.captureCallState("errno")
          val stateLayout = Linker.Option.captureStateLayout()
          def fn(name: String, desc: FunctionDescriptor, opts: Linker.Option*): MethodHandle =
            val symbol = lookup.find(name)
            if symbol.isEmpty then throw new UnsupportedOperationException(s"libc has no $name")
            linker.downcallHandle(symbol.get(), desc, opts*)
          Right(Handles(
            fn("open", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT), capture,
               Linker.Option.firstVariadicArg(2)),
            fn("fallocate", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG), capture),
            fn("fsync", FunctionDescriptor.of(JAVA_INT, JAVA_INT), capture),
            fn("close", FunctionDescriptor.of(JAVA_INT, JAVA_INT), capture),
            fn("strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT)),
            stateLayout,
            stateLayout.varHandle(MemoryLayout.PathElement.groupElement("errno"))))
        catch case t: Throwable => Left(s"cannot bind libc: $t")

    def unavailable: Option[String] = handles.left.toOption

    def fallocate(path: os.Path, size: Long): Unit =
      val h = handles.fold(reason => throw FallocateUnsupported(reason), identity)
      val arena = Arena.ofConfined()
      try
        val state = arena.allocate(h.stateLayout)
        def errno: Int = h.errno.get(state, 0L).asInstanceOf[Int]
        def fail(call: String): Nothing =
          val e = errno
          val msg = (h.strerror.invokeExact(e): MemorySegment).reinterpret(Long.MaxValue).getString(0)
          throw new IOException(s"$call of $path failed: errno $e ($msg)")

        val cpath = arena.allocateFrom(path.toString)
        val fd = h.open.invokeExact(state, cpath, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0x1a4 /* 0644 */): Int
        if fd == -1 then fail("open")
        try
          if size > 0 then
            var rc = h.fallocate.invokeExact(state, fd, 0, 0L, size): Int
            while rc == -1 && errno == EINTR do
              rc = h.fallocate.invokeExact(state, fd, 0, 0L, size): Int
            if rc == -1 then
              if errno == EOPNOTSUPP then
                throw FallocateUnsupported(s"the file system of $path does not support it")
              fail("fallocate")
          if (h.fsync.invokeExact(state, fd): Int) == -1 then fail("fsync")
        finally
          val _ = h.close.invokeExact(state, fd): Int
      finally arena.close()
