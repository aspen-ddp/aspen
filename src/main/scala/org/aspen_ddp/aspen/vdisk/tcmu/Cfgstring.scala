package org.aspen_ddp.aspen.vdisk.tcmu

/** The handler-specific part of a TCMU device's cfgstring: `aspen/<disk>`. */
object Cfgstring:
  private val Prefix = "aspen/"

  val ConfigDesc: String = "aspen/<disk>: <disk> is the name of an Aspen virtual disk (aspen list-disks)"

  /** The disk name. Anything from the first `;` is dropped. */
  def parse(cfg: String): Either[String, String] =
    if !cfg.startsWith(Prefix) then
      Left("cfgstring must start with aspen/")
    else
      val rest = cfg.substring(Prefix.length)
      val disk = rest.takeWhile(_ != ';')
      // The same rule DiskCatalog.create enforces on names.
      if disk.contains('@') then
        Left("the @<socket> form was removed with tcmu-runner; use aspen/<disk>")
      else if disk.isEmpty then
        Left("cfgstring has an empty disk name; expected aspen/<disk>")
      else
        Right(disk)
