package org.aspen_ddp.aspen.vdisk.tcmu.scsi

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.US_ASCII

import scala.annotation.tailrec

import org.aspen_ddp.aspen.vdisk.tcmu.DeviceSettings

/** Emulation of the SCSI commands that do not touch disk data, following tcmu-runner's `scsi.c` and
  * `tcmur_cmd_handler.c:handle_sync_cmd` for a handler with no ALUA port.
  *
  * Intentional differences from tcmu-runner: the disk is write-back, so the caching page has WCE set and the mode
  * parameter header has DPOFUA set; logical block provisioning is always enabled with an unmap granularity of one
  * block object; INQUIRY reports SPC-4 and no 3PC (EXTENDED COPY is not implemented); VPD 0xB1 is not offered.
  *
  * REPORT SUPPORTED OPERATION CODES is not emulated: tcmu-runner's MAINTENANCE IN handling covers only REPORT
  * TARGET PORT GROUPS (ALUA), so MAINTENANCE IN is not owned here.
  */
object Emulation:
  // scsi.h
  private val MaxUnmapLbaCount = 32 * 1024 * 1024    // VPD_MAX_UNMAP_LBA_COUNT
  private val MaxUnmapBlockDescCount = 4            // VPD_MAX_UNMAP_BLOCK_DESC_COUNT
  private val MaxCawLength = 1                      // MAX_CAW_LENGTH
  private val MaxWriteSameLength = 0xFFFFFFFFL      // VPD_MAX_WRITE_SAME_LENGTH

  private val ReadCapacity16 = 0x10                 // SERVICE ACTION IN 16 service action

  // Longest unit serial whose length fits the one-byte PAGE LENGTH / DESIGNATOR LENGTH with its NUL
  private val MaxSerialLen = 254
  private val MaxT10SerialLen = 255 - 8 - 1

  private val owned = Set(
    Op.TestUnitReady, Op.RequestSense, Op.Inquiry, Op.ModeSelect6, Op.ModeSense6, Op.StartStop,
    Op.ReadCapacity10, Op.ModeSelect10, Op.ModeSense10)

  private type Response = (ScsiResult, Array[Byte])

  /** Handles `cdb` if it is a non-data command emulated here, or returns `None`. `dataOut` supplies the MODE
    * SELECT parameter list. Data-in responses are truncated to the CDB's allocation length and `readLen` is the
    * number of bytes returned.
    */
  def handle(cdb: Array[Byte], settings: DeviceSettings, dataOut: () => Array[Byte]): Option[Response] =
    if cdb.isEmpty then
      None
    else
      val op = cdb(0) & 0xff
      val isReadCapacity16 = op == Op.ServiceActionIn16 && cdb.length > 1 && (cdb(1) & 0x1f) == ReadCapacity16
      if !owned.contains(op) && !isReadCapacity16 then
        None
      else if Cdb.length(cdb(0)).exists(cdb.length < _) then
        Some(error(Sense.InvalidFieldInCdb))
      else
        Some(op match
          case Op.TestUnitReady => (ScsiResult.Good, Array.emptyByteArray)
          case Op.RequestSense => requestSense(cdb)
          case Op.Inquiry => inquiry(cdb, settings)
          case Op.ModeSense6 => modeSense(cdb, ten = false)
          case Op.ModeSense10 => modeSense(cdb, ten = true)
          case Op.ModeSelect6 => modeSelect(cdb, ten = false, dataOut)
          case Op.ModeSelect10 => modeSelect(cdb, ten = true, dataOut)
          case Op.StartStop => startStop(cdb)
          case Op.ReadCapacity10 => readCapacity10(cdb, settings)
          case _ => readCapacity16(cdb, settings))

  private def error(r: ScsiResult): Response = (r, Array.emptyByteArray)

  private def dataIn(data: Array[Byte], allocLen: Long): Response =
    val n = math.min(data.length.toLong, allocLen).toInt
    (ScsiResult.good(n), data.take(n))

  private def be(a: Array[Byte], offset: Int, len: Int): Long =
    var v = 0L
    for i <- 0 until len do
      v = (v << 8) | (a(offset + i) & 0xffL)
    v

  private def ascii(s: String, maxLen: Int): Array[Byte] = s.getBytes(US_ASCII).take(maxLen)

  // ---- INQUIRY ----

  /** scsi.c:tcmu_emulate_inquiry */
  private def inquiry(cdb: Array[Byte], settings: DeviceSettings): Response =
    val evpd = (cdb(1) & 0x01) != 0
    val page = cdb(2) & 0xff
    val allocLen = be(cdb, 3, 2)
    if !evpd then
      if page != 0 then
        error(Sense.InvalidFieldInCdb)
      else
        dataIn(standardInquiry, allocLen)
    else
      vpdPage(page, settings) match
        case Some(data) => dataIn(data, allocLen)
        case None => error(Sense.InvalidFieldInCdb)

  /** scsi.c:tcmu_emulate_std_inquiry, with VERSION = SPC-4 and no 3PC */
  private def standardInquiry: Array[Byte] =
    val buf = new Array[Byte](36)
    buf(2) = 0x06 // SPC-4
    buf(3) = 0x02 // response data format
    buf(4) = 31   // additional length
    buf(7) = 0x02 // CmdQue
    System.arraycopy("LIO-ORG ".getBytes(US_ASCII), 0, buf, 8, 8)
    java.util.Arrays.fill(buf, 16, 32, 0x20.toByte)
    System.arraycopy("TCMU device".getBytes(US_ASCII), 0, buf, 16, 11)
    System.arraycopy("0002".getBytes(US_ASCII), 0, buf, 32, 4)
    buf

  /** scsi.c:tcmu_emulate_evpd_inquiry */
  private def vpdPage(page: Int, settings: DeviceSettings): Option[Array[Byte]] =
    page match
      case 0x00 => Some(supportedVpdPages)
      case 0x80 => Some(unitSerial(settings))
      case 0x83 => Some(deviceIdentification(settings))
      case 0xB0 => Some(blockLimits(settings))
      case 0xB2 => Some(logicalBlockProvisioning)
      case _ => None

  private def supportedVpdPages: Array[Byte] =
    val pages = Array(0x00, 0x80, 0x83, 0xB0, 0xB2).map(_.toByte)
    val buf = new Array[Byte](16)
    buf(3) = pages.length.toByte
    System.arraycopy(pages, 0, buf, 4, pages.length)
    buf

  /** PAGE LENGTH counts the serial and its NUL. tcmu-runner's zero padding past the page is not returned. */
  private def unitSerial(settings: DeviceSettings): Array[Byte] =
    val serial = ascii(settings.wwn, MaxSerialLen)
    val buf = new Array[Byte](4 + serial.length + 1)
    buf(1) = 0x80.toByte
    buf(3) = (serial.length + 1).toByte
    System.arraycopy(serial, 0, buf, 4, serial.length)
    buf

  /** The T10 vendor id and NAA designators, as tcmu-runner builds them when there is no ALUA port. Its
    * vendor-specific designator, which carries the handler cfgstring, is omitted.
    */
  private def deviceIdentification(settings: DeviceSettings): Array[Byte] =
    val serial = ascii(settings.wwn, MaxT10SerialLen)

    val t10 = new Array[Byte](4 + 8 + serial.length + 1)
    t10(0) = 2 // code set: ASCII
    t10(1) = 1 // designator type: T10 vendor id
    t10(3) = (8 + serial.length + 1).toByte
    System.arraycopy("LIO-ORG ".getBytes(US_ASCII), 0, t10, 4, 8)
    System.arraycopy(serial, 0, t10, 12, serial.length)

    // NAA type 6 with the OpenFabrics IEEE company id 00 14 05, then one hex digit of the wwn per nibble
    val naa = new Array[Byte](20)
    naa(0) = 1 // code set: binary
    naa(1) = 3 // designator type: NAA
    naa(3) = 16
    naa(4) = 0x60
    naa(5) = 0x01
    naa(6) = 0x40
    naa(7) = 0x50
    val digits = settings.wwn.iterator.map(c => Character.digit(c, 16)).filter(_ >= 0)
    var i = 7
    var high = false
    while i < 20 && digits.hasNext do
      val d = digits.next()
      if high then
        naa(i) = (d << 4).toByte
      else
        naa(i) = (naa(i) | d).toByte
        i += 1
      high = !high

    val used = t10.length + naa.length
    val buf = new Array[Byte](4 + used)
    buf(1) = 0x83.toByte
    buf(2) = (used >> 8).toByte
    buf(3) = used.toByte
    System.arraycopy(t10, 0, buf, 4, t10.length)
    System.arraycopy(naa, 0, buf, 4 + t10.length, naa.length)
    buf

  /** Unmap granularity and its alignment are one block object (tcmu-runner: hw_max_sectors and 0) */
  private def blockLimits(settings: DeviceSettings): Array[Byte] =
    val granularity = settings.blockObjectSize / settings.blockSize
    val bb = ByteBuffer.allocate(64)
    bb.put(1, 0xB0.toByte)
    bb.putShort(2, 0x3c)
    bb.put(4, 0x01.toByte) // WSNZ
    bb.put(5, MaxCawLength.toByte)
    bb.putInt(8, settings.maxSectors)  // maximum transfer length
    bb.putInt(12, settings.maxSectors) // optimal transfer length
    bb.putInt(20, MaxUnmapLbaCount)
    bb.putInt(24, MaxUnmapBlockDescCount)
    bb.putInt(28, granularity)
    bb.putInt(32, granularity | 0x80000000) // UGAVALID
    bb.putLong(36, MaxWriteSameLength)
    bb.array()

  /** LBPU, LBPWS, LBPWS10 and LBPRZ: unmapped blocks read as zero */
  private def logicalBlockProvisioning: Array[Byte] =
    val buf = new Array[Byte](64)
    buf(1) = 0xB2.toByte
    buf(3) = 0x04
    buf(5) = 0xE4.toByte
    buf

  // ---- READ CAPACITY ----

  /** tcmur_cmd_handler.c:handle_sync_cmd and scsi.c:tcmu_emulate_read_capacity_10 */
  private def readCapacity10(cdb: Array[Byte], settings: DeviceSettings): Response =
    if (cdb(1) & 0x01) != 0 || (cdb(8) & 0x01) != 0 then
      error(Sense.InvalidFieldInCdb)
    else
      val n = settings.lbaCount
      val bb = ByteBuffer.allocate(8)
      bb.putInt(0, if n < 0x100000000L then (n - 1).toInt else 0xFFFFFFFF)
      bb.putInt(4, settings.blockSize)
      dataIn(bb.array(), 8)

  /** scsi.c:tcmu_emulate_read_capacity_16: LBPME and LBPRZ set */
  private def readCapacity16(cdb: Array[Byte], settings: DeviceSettings): Response =
    val bb = ByteBuffer.allocate(32)
    bb.putLong(0, settings.lbaCount - 1)
    bb.putInt(8, settings.blockSize)
    bb.put(14, 0xC0.toByte)
    dataIn(bb.array(), be(cdb, 10, 4))

  // ---- MODE SENSE / MODE SELECT ----

  /** scsi.c:handle_rwrecovery_page */
  private def rwRecoveryPage: Array[Byte] =
    val buf = new Array[Byte](12)
    buf(0) = 0x01
    buf(1) = 0x0a
    buf

  /** scsi.c:handle_cache_page, with WCE always set */
  private def cachingPage: Array[Byte] =
    val buf = new Array[Byte](20)
    buf(0) = 0x08
    buf(1) = 0x12
    buf(2) = 0x04 // WCE
    buf

  /** scsi.c:handle_control_page */
  private def controlPage: Array[Byte] =
    val buf = new Array[Byte](12)
    buf(0) = 0x0a
    buf(1) = 0x0a
    buf(2) = 0x02 // GLTSD
    buf(5) = 0x40 // TAS
    buf(8) = 0xff.toByte // BUSY TIMEOUT PERIOD: unlimited
    buf(9) = 0xff.toByte
    buf

  /** Current values of every mode page, in page code order (scsi.c:modesense_handlers) */
  private def modePages: List[Array[Byte]] = List(rwRecoveryPage, cachingPage, controlPage)

  private def modePage(code: Int): Option[Array[Byte]] = modePages.find(p => (p(0) & 0x3f) == code)

  /** scsi.c:tcmu_emulate_mode_sense. No block descriptors; DPOFUA set in the header. No field is changeable, so
    * changeable values (PC = 1) are all zero; saved values (PC = 3) are not supported.
    */
  private def modeSense(cdb: Array[Byte], ten: Boolean): Response =
    val pc = (cdb(2) >> 6) & 0x03
    val pageCode = cdb(2) & 0x3f
    val subpage = cdb(3) & 0xff
    val allocLen = if ten then be(cdb, 7, 2) else (cdb(4) & 0xffL)
    val selected =
      if pageCode == 0x3f then
        Some(modePages)
      else if subpage == 0 then
        modePage(pageCode).map(List(_))
      else
        None
    selected match
      case None => error(Sense.InvalidFieldInCdb)
      case Some(_) if pc == 3 => error(Sense.NotSuppSaveParams)
      case Some(pages) =>
        val values =
          if pc == 1 then
            pages.map(p => p.take(2) ++ new Array[Byte](p.length - 2))
          else
            pages
        val headerLen = if ten then 8 else 4
        val buf = new Array[Byte](headerLen + values.map(_.length).sum)
        if ten then
          buf(0) = ((buf.length - 2) >> 8).toByte
          buf(1) = (buf.length - 2).toByte
          buf(3) = 0x10 // DPOFUA
        else
          buf(0) = (buf.length - 1).toByte
          buf(2) = 0x10 // DPOFUA
        values.foldLeft(headerLen): (offset, p) =>
          System.arraycopy(p, 0, buf, offset, p.length)
          offset + p.length
        dataIn(buf, allocLen)

  /** scsi.c:tcmu_emulate_mode_select: nothing can be changed, so each page sent must equal its current value.
    * Unlike tcmu-runner, which takes the page code from reserved CDB byte 2, the pages are read from the
    * parameter list, and SP gives SAVING PARAMETERS NOT SUPPORTED.
    */
  private def modeSelect(cdb: Array[Byte], ten: Boolean, dataOut: () => Array[Byte]): Response =
    val listLen = (if ten then be(cdb, 7, 2) else (cdb(4) & 0xffL)).toInt
    val headerLen = if ten then 8 else 4
    if listLen == 0 then
      (ScsiResult.Good, Array.emptyByteArray)
    else if (cdb(1) & 0x10) == 0 then
      error(Sense.InvalidFieldInCdb)
    else if (cdb(1) & 0x01) != 0 then
      error(Sense.NotSuppSaveParams)
    else
      val data = dataOut()
      if data.length < listLen || listLen < headerLen then
        error(Sense.ParamListLength)
      else
        val list = data.take(listLen)
        val blockDescLen = if ten then be(list, 6, 2) else (list(3) & 0xffL)
        if blockDescLen != 0 then
          error(Sense.InvalidFieldInParamList)
        else
          checkPages(list, headerLen) match
            case Some(r) => error(r)
            case None => (ScsiResult.Good, Array.emptyByteArray)

  @tailrec
  private def checkPages(list: Array[Byte], offset: Int): Option[ScsiResult] =
    if offset == list.length then
      None
    else if offset + 2 > list.length then
      Some(Sense.ParamListLength)
    else
      val end = offset + 2 + (list(offset + 1) & 0xff)
      // PS (bit 7) is reserved in MODE SELECT; SPF (bit 6) would select a subpage, of which there are none
      val code = list(offset) & 0x7f
      modePage(code) match
        case None => Some(Sense.InvalidFieldInParamList)
        case Some(_) if end > list.length => Some(Sense.ParamListLength)
        case Some(current) =>
          val sent = list.slice(offset, end)
          sent(0) = (sent(0) & 0x7f).toByte
          if !java.util.Arrays.equals(sent, current) then
            Some(Sense.InvalidFieldInParamList)
          else
            checkPages(list, end)

  // ---- Other ----

  /** No sense, in fixed format or, with DESC set, descriptor format */
  private def requestSense(cdb: Array[Byte]): Response =
    val data =
      if (cdb(1) & 0x01) != 0 then
        val d = new Array[Byte](8)
        d(0) = 0x72
        d
      else
        Sense.fixed(0, 0)
    dataIn(data, cdb(4) & 0xffL)

  /** scsi.c:tcmu_emulate_start_stop: only START with no power condition is accepted */
  private def startStop(cdb: Array[Byte]): Response =
    if ((cdb(4) >> 4) & 0x0f) != 0 || (cdb(4) & 0x01) == 0 then
      error(Sense.InvalidFieldInCdb)
    else
      (ScsiResult.Good, Array.emptyByteArray)
