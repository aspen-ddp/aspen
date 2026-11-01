package org.aspen_ddp.aspen.vdisk.tcmu

/** What SCSI emulation needs to know about one TCMU device.
  *
  * @param size            device size in bytes
  * @param blockSize       logical block size in bytes
  * @param maxSectors      configfs `hw_max_sectors`: the maximum transfer length in logical blocks
  * @param blockObjectSize size in bytes of the disk's block objects, a multiple of `blockSize`
  * @param deviceName      the configfs device name
  * @param wwn             the unit serial: configfs `wwn/vpd_unit_serial` if present, otherwise the disk name
  */
final case class DeviceSettings(
  size: Long,
  blockSize: Int,
  maxSectors: Int,
  blockObjectSize: Int,
  deviceName: String,
  wwn: String):

  def lbaCount: Long = size / blockSize
