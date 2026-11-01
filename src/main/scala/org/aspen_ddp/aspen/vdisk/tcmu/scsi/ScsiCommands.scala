package org.aspen_ddp.aspen.vdisk.tcmu.scsi

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

import org.aspen_ddp.aspen.vdisk.{LockableBlockDevice, VDiskError}
import org.aspen_ddp.aspen.vdisk.tcmu.DeviceSettings

/** What a TCMU device needs to run one command: never fails, every error becomes a CHECK CONDITION.
  * `dataOut` must be called, if at all, synchronously within `handle`: the data area belongs to the command only
  * until it is completed, and a TCMU device rejects a gather after a ring reset or `stop`.
  */
trait ScsiHandler:
  def handle(cdb: Array[Byte], dataOut: () => Array[Byte], dataInCapacity: Long): Future[(ScsiResult, Array[Byte])]

/** Executes SCSI commands against a [[LockableBlockDevice]], following tcmu-runner's `tcmur_cmd_handler.c`.
  * Non-data commands go to [[Emulation]]. Ranges are checked against the kernel's view of the device (`settings`),
  * which may be smaller than the Aspen disk.
  *
  * Differences from tcmu-runner: a payload whose length does not match the transfer length is INVALID FIELD IN CDB
  * (tcmu-runner: HARDWARE ERROR); an UNMAP whose descriptor data length runs past the parameter list is PARAMETER
  * LIST LENGTH ERROR and nothing is unmapped (tcmu-runner reads past the list); WRITE AND VERIFY writes then
  * flushes instead of reading back; WRITE SAME with the UNMAP bit unmaps only an aligned range whose block is all
  * zero (tcmu-runner also unmaps a non-zero block); COMPARE AND WRITE honors FUA; FORMAT UNIT unmaps the device,
  * which reads back as zeros as tcmu-runner's zero-fill does, without the FORMAT IN PROGRESS state.
  */
final class ScsiCommands(device: LockableBlockDevice, initial: DeviceSettings)(using ExecutionContext)
    extends ScsiHandler:

  private type Response = (ScsiResult, Array[Byte])

  // scsi.h
  private val MaxUnmapLbaCount = 32L * 1024 * 1024  // VPD_MAX_UNMAP_LBA_COUNT
  private val MaxUnmapBlockDescCount = 4            // VPD_MAX_UNMAP_BLOCK_DESC_COUNT
  private val MaxCawLength = 1                      // MAX_CAW_LENGTH
  private val ReportLuns = 0xA0
  private val MaxWriteSameChunk = 1024L * 1024

  @volatile private var current = initial
  private val capacityChanged = new AtomicBoolean(false)

  def settings: DeviceSettings = current

  /** Adopts a new kernel device size and arms a CAPACITY DATA HAS CHANGED unit attention */
  def resized(newSize: Long): Unit =
    current = current.copy(size = newSize)
    capacityChanged.set(true)

  /** Never fails: every error, synchronous or not, becomes a CHECK CONDITION */
  def handle(cdb: Array[Byte], dataOut: () => Array[Byte], dataInCapacity: Long): Future[Response] =
    val result =
      try dispatch(cdb, dataOut, dataInCapacity)
      catch case e: Throwable if !e.isInstanceOf[VirtualMachineError] => Future.failed(e)
    result.recover:
      case e: Throwable if !e.isInstanceOf[VirtualMachineError] =>
        val op = if cdb.isEmpty then -1 else cdb(0) & 0xff
        val line = f"${current.deviceName}: SCSI opcode 0x$op%02x failed: $e"
        e match
          // Expected while fenced or for a bad request: one per command would flood the log
          case VDiskError.Fenced | VDiskError.NotOwner | _: VDiskError.OutOfRange | _: VDiskError.InvalidRequest =>
            scribe.debug(line)
          case _ => scribe.warn(line)
        (ErrorMap.result(e, isWrite(op)), Array.emptyByteArray)

  private def isWrite(op: Int): Boolean =
    op match
      case Op.Write6 | Op.Write10 | Op.Write12 | Op.Write16 | Op.WriteVerify10 | Op.WriteVerify16 |
           Op.WriteSame10 | Op.WriteSame16 | Op.CompareAndWrite | Op.Unmap | Op.FormatUnit |
           Op.SyncCache10 | Op.SyncCache16 => true
      case _ => false

  private def done(r: ScsiResult): Future[Response] = Future.successful((r, Array.emptyByteArray))

  private def dispatch(cdb: Array[Byte], dataOut: () => Array[Byte], dataInCapacity: Long): Future[Response] =
    if cdb.isEmpty then
      done(Sense.InvalidOpcode)
    else
      val op = cdb(0) & 0xff
      val uaExempt = op == Op.Inquiry || op == Op.RequestSense || op == ReportLuns
      if !uaExempt && capacityChanged.compareAndSet(true, false) then
        done(Sense.CapacityChanged)
      else
        val s = current
        Emulation.handle(cdb, s, dataOut) match
          case Some((r, data)) =>
            if data.length > dataInCapacity then
              val n = math.max(0L, dataInCapacity).toInt
              Future.successful((r.copy(readLen = r.readLen.map(math.min(_, n))), data.take(n)))
            else
              Future.successful((r, data))
          case None => dataCommand(op, cdb, s, dataOut, dataInCapacity)

  private def dataCommand(
      op: Int,
      cdb: Array[Byte],
      s: DeviceSettings,
      dataOut: () => Array[Byte],
      dataInCapacity: Long): Future[Response] =
    val known = op match
      case Op.Read6 | Op.Read10 | Op.Read12 | Op.Read16 | Op.Write6 | Op.Write10 | Op.Write12 | Op.Write16 |
           Op.WriteVerify10 | Op.WriteVerify16 | Op.SyncCache10 | Op.SyncCache16 | Op.Unmap |
           Op.WriteSame10 | Op.WriteSame16 | Op.CompareAndWrite | Op.FormatUnit => true
      case _ => false
    if !known then
      done(Sense.InvalidOpcode)
    else if Cdb.length(cdb(0)).exists(cdb.length < _) then
      done(Sense.InvalidFieldInCdb)
    else
      op match
        case Op.Read6 | Op.Read10 | Op.Read12 | Op.Read16 => read(cdb, s, dataInCapacity)
        case Op.Write6 | Op.Write10 | Op.Write12 | Op.Write16 => write(cdb, s, dataOut, verify = false)
        case Op.WriteVerify10 | Op.WriteVerify16 => write(cdb, s, dataOut, verify = true)
        case Op.SyncCache10 | Op.SyncCache16 => device.flush().map(_ => (ScsiResult.Good, Array.emptyByteArray))
        case Op.Unmap => unmap(cdb, s, dataOut)
        case Op.WriteSame10 | Op.WriteSame16 => writeSame(cdb, s, dataOut)
        case Op.CompareAndWrite => compareAndWrite(cdb, s, dataOut)
        case _ => format(s)

  // ---- Checks ----

  /** check_lbas: the blocks must lie within the kernel's device size. `lba` is unsigned. */
  private def inRange(s: DeviceSettings, lba: Long, blocks: Long): Boolean =
    val n = s.lbaCount
    lba >= 0 && blocks <= n && lba <= n - blocks

  /** check_iovec_length */
  private def lengthMatches(s: DeviceSettings, what: String, actual: Long, blocks: Long): Boolean =
    val expected = blocks * s.blockSize
    if actual != expected then
      scribe.warn(s"${s.deviceName}: $what length $actual does not match $blocks blocks of ${s.blockSize} bytes")
    actual == expected

  private def be(a: Array[Byte], offset: Int, len: Int): Long =
    var v = 0L
    for i <- 0 until len do
      v = (v << 8) | (a(offset + i) & 0xffL)
    v

  // ---- READ / WRITE ----

  private def read(cdb: Array[Byte], s: DeviceSettings, dataInCapacity: Long): Future[Response] =
    val lba = Cdb.lba(cdb)
    val blocks = Cdb.transferLength(cdb)
    val bytes = blocks * s.blockSize
    if !lengthMatches(s, "data-in", dataInCapacity, blocks) || bytes > Int.MaxValue then
      done(Sense.InvalidFieldInCdb)
    else if !inRange(s, lba, blocks) then
      done(Sense.LbaOutOfRange)
    else if blocks == 0 then
      Future.successful((ScsiResult.good(0), Array.emptyByteArray))
    else
      device.read(lba * s.blockSize, bytes.toInt).map: data =>
        if data.length != bytes then
          scribe.error(s"${s.deviceName}: device read returned ${data.length} bytes, expected $bytes")
          (Sense.HardwareError, Array.emptyByteArray)
        else
          (ScsiResult.good(data.length), data)

  private def write(cdb: Array[Byte], s: DeviceSettings, dataOut: () => Array[Byte], verify: Boolean)
      : Future[Response] =
    val lba = Cdb.lba(cdb)
    val blocks = Cdb.transferLength(cdb)
    val data = dataOut()
    if !lengthMatches(s, "data-out", data.length, blocks) then
      done(Sense.InvalidFieldInCdb)
    else if !inRange(s, lba, blocks) then
      done(Sense.LbaOutOfRange)
    else if blocks == 0 then
      done(ScsiResult.Good)
    else
      val offset = lba * s.blockSize
      locked(List((offset, data.length.toLong)), caw = false):
        val written = device.write(offset, data, Cdb.fua(cdb))
        if verify then
          written.flatMap(_ => device.flush())
        else
          written
      .map(_ => (ScsiResult.Good, Array.emptyByteArray))

  // ---- UNMAP ----

  /** handle_unmap and handle_unmap_internal, validating every descriptor before the single unmap call */
  private def unmap(cdb: Array[Byte], s: DeviceSettings, dataOut: () => Array[Byte]): Future[Response] =
    val listLen = be(cdb, 7, 2).toInt
    if (cdb(1) & 0x01) != 0 then
      done(Sense.InvalidFieldInCdb) // ANCHOR is not supported
    else if listLen == 0 then
      done(ScsiResult.Good)
    else if listLen < 8 then
      done(Sense.ParamListLength)
    else
      val par = dataOut()
      if par.length < listLen then
        scribe.warn(s"${s.deviceName}: UNMAP parameter list is ${par.length} bytes, PARAMETER LIST LENGTH $listLen")
        done(Sense.ParamListLength)
      else
        unmapDescriptors(s, par, listLen)

  private def unmapDescriptors(s: DeviceSettings, par: Array[Byte], listLen: Int): Future[Response] =
    // A trailing incomplete descriptor is ignored, as tcmu-runner does
    val bddl = be(par, 2, 2).toInt & ~0xF
    if bddl > listLen - 8 then
      scribe.warn(s"${s.deviceName}: UNMAP descriptor data length $bddl exceeds PARAMETER LIST LENGTH $listLen")
      done(Sense.ParamListLength)
    else if bddl == 0 then
      done(ScsiResult.Good)
    else if bddl / 16 > MaxUnmapBlockDescCount then
      done(Sense.InvalidFieldInParamList)
    else
      val descs = List.tabulate(bddl / 16)(i => (be(par, 8 + 16 * i, 8), be(par, 16 + 16 * i, 4)))
      if descs.exists(_._2 > MaxUnmapLbaCount) then
        done(Sense.InvalidFieldInParamList)
      else if descs.exists((lba, n) => !inRange(s, lba, n)) then
        done(Sense.LbaOutOfRange)
      else
        val ranges = descs.filter(_._2 != 0).map((lba, n) => (lba * s.blockSize, n * s.blockSize))
        if ranges.isEmpty then
          done(ScsiResult.Good)
        else
          locked(ranges, caw = false)(device.unmap(ranges)).map(_ => (ScsiResult.Good, Array.emptyByteArray))

  // ---- WRITE SAME ----

  /** handle_writesame: handle_writesame_check, then an unmap of an aligned range or chunked writes */
  private def writeSame(cdb: Array[Byte], s: DeviceSettings, dataOut: () => Array[Byte]): Future[Response] =
    val lba = Cdb.lba(cdb)
    val blocks = Cdb.transferLength(cdb)
    val block = dataOut()
    if block.length != s.blockSize then
      scribe.warn(s"${s.deviceName}: WRITE SAME data-out is ${block.length} bytes, not one ${s.blockSize} byte block")
      done(Sense.InvalidFieldInCdb)
    else if blocks == 0 then
      done(Sense.InvalidFieldInCdb) // WSNZ is set
    else if !inRange(s, lba, blocks) then
      done(Sense.LbaOutOfRange)
    else
      val bs = s.blockSize.toLong
      val range = (lba * bs, blocks * bs)
      val granularity = math.max(1, s.blockObjectSize / s.blockSize)
      val unmapIt = (cdb(1) & 0x08) != 0 && lba % granularity == 0 && blocks % granularity == 0 &&
        block.forall(_ == 0)
      locked(List(range), caw = false):
        if unmapIt then
          device.unmap(List(range))
        else
          // At most 1 MiB, so the chunk array cannot overflow an Int
          val maxChunk = math.min(s.maxSectors.toLong, math.max(1L, MaxWriteSameChunk / s.blockSize))
          val chunkBlocks = math.max(1L, math.min(maxChunk, blocks)).toInt
          val chunk = Array.concat(Seq.fill(chunkBlocks)(block)*)
          def from(next: Long): Future[Unit] =
            if next == blocks then
              Future.unit
            else
              val n = math.min(chunkBlocks.toLong, blocks - next).toInt
              val data = if n == chunkBlocks then chunk else chunk.take(n * s.blockSize)
              device.write((lba + next) * bs, data, fua = false).flatMap(_ => from(next + n))
          from(0)
      .map(_ => (ScsiResult.Good, Array.emptyByteArray))

  // ---- COMPARE AND WRITE ----

  /** handle_caw: NUMBER OF BLOCKS 0 is not an error and touches nothing */
  private def compareAndWrite(cdb: Array[Byte], s: DeviceSettings, dataOut: () => Array[Byte]): Future[Response] =
    val lba = Cdb.lba(cdb)
    val blocks = Cdb.transferLength(cdb)
    if blocks == 0 then
      done(ScsiResult.Good)
    else if blocks > MaxCawLength then
      done(Sense.InvalidFieldInCdb)
    else
      val data = dataOut()
      if !lengthMatches(s, "COMPARE AND WRITE data-out", data.length, blocks * 2) then
        done(Sense.InvalidFieldInCdb)
      else if !inRange(s, lba, blocks) then
        done(Sense.LbaOutOfRange)
      else
        val offset = lba * s.blockSize
        val half = data.length / 2
        val fua = (cdb(1) & 0x08) != 0
        locked(List((offset, half.toLong)), caw = true):
          device.read(offset, half).transformWith:
            case scala.util.Failure(e) =>
              scribe.warn(s"${s.deviceName}: COMPARE AND WRITE read failed: $e")
              Future.successful(ErrorMap.result(e, isWrite = false))
            case scala.util.Success(currentData) =>
              if currentData.length != half then
                scribe.error(s"${s.deviceName}: device read returned ${currentData.length} bytes, expected $half")
                Future.successful(Sense.HardwareError)
              else
                (0 until half).find(i => currentData(i) != data(i)) match
                  case Some(i) => Future.successful(Sense.Miscompare(i))
                  case None => device.write(offset, data.drop(half), fua).map(_ => ScsiResult.Good)
        .map(r => (r, Array.emptyByteArray))

  // ---- FORMAT UNIT ----

  private def format(s: DeviceSettings): Future[Response] =
    val range = (0L, s.lbaCount * s.blockSize)
    locked(List(range), caw = false)(device.unmap(List(range))).map(_ => (ScsiResult.Good, Array.emptyByteArray))

  // ---- Range lock ----

  /** An in-flight modification of `ranges` (byte offset, length); `done` completes when it finishes */
  private final class InFlight(val ranges: List[(Long, Long)], val caw: Boolean):
    val done: Promise[Unit] = Promise()

  private val inFlight = mutable.Set[InFlight]()

  private def overlaps(a: List[(Long, Long)], b: List[(Long, Long)]): Boolean =
    a.exists((ao, al) => b.exists((bo, bl) => ao < bo + bl && bo < ao + al))

  /** Runs `body` once every overlapping in-flight command it conflicts with has finished. A COMPARE AND WRITE
    * conflicts with every overlapping modification; other modifications conflict only with a COMPARE AND WRITE.
    * Each command waits only for those registered before it, so there is no cycle.
    */
  private def locked[A](ranges: List[(Long, Long)], caw: Boolean)(body: => Future[A]): Future[A] =
    val entry = InFlight(ranges, caw)
    val predecessors = synchronized:
      val p = inFlight.iterator.filter(o => (caw || o.caw) && overlaps(o.ranges, ranges)).map(_.done.future).toList
      inFlight += entry
      p
    val result = Future.sequence(predecessors).flatMap(_ => body)
    result.onComplete: _ =>
      synchronized(inFlight -= entry)
      entry.done.success(())
    result
