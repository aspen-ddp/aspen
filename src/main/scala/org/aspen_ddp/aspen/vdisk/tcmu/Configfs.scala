package org.aspen_ddp.aspen.vdisk.tcmu

import java.nio.file.NoSuchFileException
import scala.util.Try
import scala.util.control.NonFatal

/** The name the kernel gives a TCMU device's UIO node: `tcm-user/<hba>/<device>/<subtype>/<cfgstring>`.
  *
  * `hba` is the part after `tcm-user/`; its configfs directory is `user_<hba>`, as in libtcmu.
  */
final case class UioName(hba: String, device: String, subtype: String, cfgstring: String)

object UioName:
  val Prefix = "tcm-user"

  def parse(s: String): Either[String, UioName] =
    // The cfgstring is everything after the fourth '/' and may contain '/' itself.
    val parts = s.split("/", 5)
    if parts.length != 5 then
      Left(s"invalid uio name '$s': expected tcm-user/<hba>/<device>/<subtype>/<cfgstring>")
    else if parts(0) != Prefix then
      Left(s"invalid uio name '$s': does not start with tcm-user/")
    else if parts(1).isEmpty || parts(2).isEmpty || parts(3).isEmpty then
      Left(s"invalid uio name '$s': empty hba, device or subtype")
    else
      Right(UioName(parts(1), parts(2), parts(3), parts(4)))

/** Reads and writes the configfs (target core) and sysfs (UIO class) files of TCMU devices.
  *
  * `hba` arguments are the `UioName.hba` value; the configfs directory is `user_<hba>`.
  */
class Configfs(core: os.Path = os.root / "sys" / "kernel" / "config" / "target" / "core",
                     uioClass: os.Path = os.root / "sys" / "class" / "uio",
                     dev: os.Path = os.root / "dev"):

  private def devDir(hba: String, device: String): os.Path = core / s"user_$hba" / device

  /** Overridden by tests to record or fail writes. */
  protected def writeFile(path: os.Path, value: String): Unit =
    // java.nio: os-lib's writes always create or truncate, and configfs attributes must be opened WRITE only
    // (as libtcmu's open(O_WRONLY) does), so a missing attribute is NoSuchFileException.
    val out = java.nio.file.Files.newOutputStream(path.toNIO, java.nio.file.StandardOpenOption.WRITE)
    try out.write(value.getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    finally out.close()

  private def readText(path: os.Path, expected: String): String =
    try os.read(path)
    catch case e: Exception =>
      throw new java.io.IOException(s"cannot read $path (expected $expected): ${e.getMessage}", e)

  private def readLong(path: os.Path, radix: Int, expected: String): Long =
    val text = readText(path, expected).trim
    val digits = if radix == 16 && (text.startsWith("0x") || text.startsWith("0X")) then text.substring(2) else text
    try java.lang.Long.parseLong(digits, radix)
    catch case _: NumberFormatException =>
      throw new java.io.IOException(s"cannot parse $path (expected $expected): '$text'")

  private def positiveInt(p: os.Path): Int =
    val v = readLong(p, 10, "an integer from 1 to 2147483647")
    if v < 1 || v > Int.MaxValue then
      throw new java.io.IOException(s"bad value in $p (expected an integer from 1 to ${Int.MaxValue}): $v")
    v.toInt

  def hwBlockSize(hba: String, device: String): Int =
    val p = devDir(hba, device) / "attrib" / "hw_block_size"
    positiveInt(p)

  def hwMaxSectors(hba: String, device: String): Int =
    val p = devDir(hba, device) / "attrib" / "hw_max_sectors"
    positiveInt(p)

  /** The `Size:` field of `info`, found as `tcmu_cfgfs_dev_get_info_u64` does: " Size: " then an unsigned integer. */
  def size(hba: String, device: String): Long =
    val p = devDir(hba, device) / "info"
    val text = readText(p, "a ' Size: <bytes>' field")
    val marker = " Size: "
    val idx = text.indexOf(marker)
    if idx < 0 then
      throw new java.io.IOException(s"cannot find \"$marker\" in $p (expected a Size field): '${text.trim}'")
    val digits = text.substring(idx + marker.length).takeWhile(_.isDigit)
    if digits.isEmpty then
      throw new java.io.IOException(s"cannot parse the Size field in $p (expected an unsigned integer): '${text.trim}'")
    try java.lang.Long.parseUnsignedLong(digits)
    catch case _: NumberFormatException =>
      throw new java.io.IOException(s"cannot parse the Size field in $p (expected an unsigned integer): '$digits'")

  /** The VPD unit serial, or None when the file is absent or has no value. */
  def unitSerial(hba: String, device: String): Option[String] =
    val p = devDir(hba, device) / "wwn" / "vpd_unit_serial"
    if !os.isFile(p) then
      None
    else
      val text = readText(p, "a unit serial").trim
      // The kernel prints "T10 VPD Unit Serial Number: <serial>"
      val serial = text.substring(text.lastIndexOf(':') + 1).trim
      Option.when(serial.nonEmpty)(serial)

  def action(hba: String, device: String, name: String, value: Int): Unit =
    writeFile(devDir(hba, device) / "action" / name, value.toString)

  def control(hba: String, device: String): RingControl =
    new RingControl:
      /** `block_dev 1`, `reset_ring 1`, `block_dev 0` as libtcmu does. Without `block_dev` the kernel cannot
        * reset, so the reset is skipped. The device is unblocked even when `reset_ring` fails; that failure is
        * then rethrown.
        */
      def resetRing(): Unit =
        val blockDev = devDir(hba, device) / "action" / "block_dev"
        val blocked =
          try
            action(hba, device, "block_dev", 1)
            true
          catch case _: NoSuchFileException =>
            scribe.warn(s"$blockDev does not exist: not resetting the ring of $hba/$device")
            false
        if blocked then
          var failure: Throwable = null
          try action(hba, device, "reset_ring", 1)
          catch case NonFatal(t) =>
            scribe.error(s"reset_ring failed for $hba/$device", t)
            failure = t
          try action(hba, device, "block_dev", 0)
          catch case NonFatal(t) =>
            if failure == null then
              failure = t
            else
              failure.addSuppressed(t)
          if failure != null then
            throw failure

  private def uioDir(minor: Int): os.Path = uioClass / s"uio$minor"

  def uioName(minor: Int): Either[String, UioName] =
    UioName.parse(readText(uioDir(minor) / "name", "tcm-user/<hba>/<device>/<subtype>/<cfgstring>").trim)

  def mapSize(minor: Int): Long =
    readLong(uioDir(minor) / "maps" / "map0" / "size", 16, "a hexadecimal size")

  def uioDevicePath(minor: Int): os.Path = dev / s"uio$minor"

  /** Every `uioN` whose name parses as a tcm-user device. */
  def scanUio(): List[(Int, UioName)] =
    if !os.isDir(uioClass) then
      Nil
    else
      os.list(uioClass).toList.flatMap: p =>
        val n = p.last
        if n.startsWith("uio") && n.drop(3).nonEmpty && n.drop(3).forall(_.isDigit) && os.isFile(p / "name") then
          val minor = n.drop(3).toInt
          Try(uioName(minor)).fold(
            { e =>
              scribe.warn(s"skipping $n: ${e.getMessage}")
              None
            },
            _.toOption.map(minor -> _))
        else
          None
