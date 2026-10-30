package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException

import scala.collection.mutable
import scala.util.Random

enum Fault:
  case OutOfSpace // IOException("No space left on device")
  case IoError    // IOException("Input/output error")

enum FaultOp:
  case Write, Force, Truncate

/** In-memory FsifFiles with a faithful crash model.
 *
 *  Each file keeps its durable bytes plus the ops pending since its last `force`. Reads see the page
 *  cache (durable + pending). `crash()` returns a new instance holding only the durable image after
 *  applying each pending op, in order, independently with probability 1/2. A kept write may be torn:
 *  a random subset of its 512-byte-aligned sectors (relative to file offset 0) is applied. A kept
 *  write beyond the current size extends the file with a zero-filled gap. Creating a file is durable,
 *  as NioFsifFiles syncs the directory.
 *
 *  Thread-safe: one lock per instance. A faulted op has no effect at all.
 */
final class SimulatedFsifFiles(rnd: Random) extends FsifFiles:
  import SimulatedFsifFiles.*

  private val lock = new Object
  private val files = mutable.Map[String, SimFile]()
  private var crashed = false
  private val oneShot = mutable.ArrayBuffer[Rule]()
  private val persistent = mutable.ArrayBuffer[Rule]()
  /** Ops allowed before every later one fails (crashAfterOps); negative means unlimited */
  private var opsLeft = -1L
  /** Writes, forces and truncates that took effect, across all files */
  private var opsDone = 0L

  def open(name: String): FsifFile = lock.synchronized:
    live()
    val f = files.getOrElseUpdate(name, new SimFile)
    new Handle(name, f)

  def exists(name: String): Boolean = lock.synchronized:
    live()
    files.contains(name)

  def size(name: String): Long = lock.synchronized:
    live()
    files.get(name).map(_.cache.size).getOrElse(0L)

  /** The next matching write/force/truncate throws, then the fault is spent */
  def failNext(fault: Fault, file: String => Boolean = _ => true,
               ops: Set[FaultOp] = FaultOp.values.toSet): Unit = lock.synchronized:
    oneShot += Rule(fault, file, ops, () => true)

  /** Matching ops throw until p returns false. p is evaluated under the instance lock. */
  def failWhile(fault: Fault, file: String => Boolean = _ => true,
                ops: Set[FaultOp] = FaultOp.values.toSet)(p: => Boolean): Unit = lock.synchronized:
    persistent += Rule(fault, file, ops, () => p)

  /** After `n` more writes, forces or truncates (across all files), every later one fails with no effect and
   *  throws, as if the machine had stopped. Call `crash()` afterward for the durable image.
   */
  def crashAfterOps(n: Int): Unit = lock.synchronized:
    require(n >= 0)
    opsLeft = n

  /** Writes, forces and truncates that have taken effect so far, across all files (faulted ones excluded) */
  def opCount: Long = lock.synchronized(opsDone)

  /** New instance holding a copy of this one's durable image, with no pending ops, rules or op limit. Unlike
   *  `crash()`, this instance stays usable and no random draws are made.
   */
  def snapshot(): SimulatedFsifFiles = lock.synchronized:
    live()
    val next = new SimulatedFsifFiles(new Random(0))
    for (name, f) <- files do
      val nf = new SimFile
      nf.durable = f.durable.clone()
      nf.cache.set(nf.durable)
      next.files(name) = nf
    next

  /** New instance holding only the resulting durable image. This instance must not be used afterward. */
  def crash(): SimulatedFsifFiles = lock.synchronized:
    live()
    crashed = true
    val next = new SimulatedFsifFiles(rnd)
    // Sorted so the random draws are deterministic per seed
    for name <- files.keys.toSeq.sorted do
      val f = files(name)
      val image = new Image
      image.set(f.durable)
      for op <- f.pending do
        if rnd.nextBoolean() then
          op match
            case Op.Write(pos, bytes) =>
              if rnd.nextBoolean() then
                image.write(pos, bytes)
              else
                applyTorn(image, pos, bytes)
            case Op.Truncate(size) =>
              image.truncate(size)
      val nf = new SimFile
      nf.durable = image.copyBytes()
      nf.cache.set(nf.durable)
      next.files(name) = nf
    next

  private def applyTorn(image: Image, pos: Long, bytes: Array[Byte]): Unit =
    val end = pos + bytes.length
    var s = pos / SectorSize
    while s * SectorSize < end do
      val from = math.max(pos, s * SectorSize)
      val to = math.min(end, (s + 1) * SectorSize)
      if rnd.nextBoolean() then
        image.write(from, java.util.Arrays.copyOfRange(bytes, (from - pos).toInt, (to - pos).toInt))
      s += 1

  private def live(): Unit =
    if crashed then
      throw new IllegalStateException("SimulatedFsifFiles used after crash()")

  /** Every write, force and truncate passes through here, so op counting has one place to live. */
  private def beginOp(name: String, op: FaultOp): Unit =
    live()
    if opsLeft == 0 then
      throw new IOException("Input/output error (simulated crash: no further I/O)")
    if opsLeft > 0 then
      opsLeft -= 1
    val i = oneShot.indexWhere(r => r.file(name) && r.ops.contains(op))
    if i >= 0 then
      throw ioException(oneShot.remove(i).fault)
    persistent.filterInPlace(r => r.active())
    persistent.find(r => r.file(name) && r.ops.contains(op)).foreach(r => throw ioException(r.fault))
    opsDone += 1

  private def ioException(f: Fault): IOException = f match
    case Fault.OutOfSpace => new IOException("No space left on device")
    case Fault.IoError => new IOException("Input/output error")

  private final class Handle(name: String, f: SimFile) extends FsifFile:
    private var closed = false

    private def check(): Unit =
      live()
      if closed then
        throw new IOException("File closed")

    def size: Long = lock.synchronized:
      check()
      f.cache.size

    def read(position: Long, length: Int): Array[Byte] = lock.synchronized:
      check()
      f.cache.read(position, length)

    def write(position: Long, bytes: Array[Byte]): Unit = lock.synchronized:
      check()
      beginOp(name, FaultOp.Write)
      val copy = bytes.clone()
      f.cache.write(position, copy)
      f.pending += Op.Write(position, copy)

    def truncate(size: Long): Unit = lock.synchronized:
      check()
      beginOp(name, FaultOp.Truncate)
      f.cache.truncate(size)
      f.pending += Op.Truncate(size)

    def force(): Unit = lock.synchronized:
      check()
      beginOp(name, FaultOp.Force)
      f.durable = f.cache.copyBytes()
      f.pending.clear()

    def close(): Unit = lock.synchronized:
      closed = true

object SimulatedFsifFiles:
  val SectorSize = 512

  private case class Rule(fault: Fault, file: String => Boolean, ops: Set[FaultOp], active: () => Boolean)

  private enum Op:
    case Write(position: Long, bytes: Array[Byte])
    case Truncate(size: Long)

  private final class SimFile:
    var durable: Array[Byte] = Array()
    val cache = new Image
    val pending = mutable.ArrayBuffer[Op]()

  /** Growable byte image; gaps created by writes past the end are zero-filled */
  private final class Image:
    private var arr = new Array[Byte](0)
    private var len = 0

    def size: Long = len

    def set(bytes: Array[Byte]): Unit =
      arr = bytes.clone()
      len = arr.length

    def copyBytes(): Array[Byte] = java.util.Arrays.copyOf(arr, len)

    def read(position: Long, length: Int): Array[Byte] =
      if position >= len then
        Array()
      else
        val n = math.min(length.toLong, len - position).toInt
        java.util.Arrays.copyOfRange(arr, position.toInt, position.toInt + n)

    def write(position: Long, bytes: Array[Byte]): Unit =
      val end = (position + bytes.length).toInt
      if end > len then
        ensure(end)
        len = end
      System.arraycopy(bytes, 0, arr, position.toInt, bytes.length)

    def truncate(size: Long): Unit =
      val n = size.toInt
      if n < len then
        java.util.Arrays.fill(arr, n, len, 0.toByte) // so later growth reads zeros
        len = n
      else if n > len then
        ensure(n)
        len = n

    private def ensure(cap: Int): Unit =
      if cap > arr.length then
        arr = java.util.Arrays.copyOf(arr, math.max(cap, arr.length * 2))
