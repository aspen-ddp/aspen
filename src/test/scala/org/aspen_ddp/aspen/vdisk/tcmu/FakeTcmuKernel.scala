package org.aspen_ddp.aspen.vdisk.tcmu

import java.lang.foreign.MemorySegment
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** A [[TcmuKernel]] for tests: a temp-dir configfs and sysfs tree, with one [[FakeKernel]] ring per added device.
  *
  * `events` records, in order, every configfs write (`"<file> <value>"`, e.g. `"reset_ring 1"`), every
  * `openRing` (`"openRing <minor>"`) and every first call of a closer (`"unmap <minor>"`). A `reset_ring 1` write
  * resets the device's FakeKernel ring as the kernel would. `failOpenRing` makes `openRing` throw for those minors.
  */
final class FakeTcmuKernel extends TcmuKernel:
  val root: os.Path = os.temp.dir(prefix = "fake-tcmu")
  val core: os.Path = root / "core"
  val uio: os.Path = root / "uio"
  val dev: os.Path = root / "dev"
  os.makeDir.all(core)
  os.makeDir.all(uio)
  os.makeDir.all(dev)

  private val eventLog = ConcurrentLinkedQueue[String]()
  private val kernels = TrieMap[Int, FakeKernel]()
  /** configfs device dir -> minor */
  private val dirs = TrieMap[os.Path, Int]()
  private val opened = TrieMap[Int, AtomicBoolean]()
  @volatile var failOpenRing: Set[Int] = Set.empty

  val configfs: Configfs = new Configfs(core, uio, dev):
    override protected def writeFile(path: os.Path, value: String): Unit =
      eventLog.add(s"${path.last} $value")
      super.writeFile(path, value)
      if path.last == "reset_ring" && value == "1" then
        dirs.get(path / os.up / os.up).flatMap(kernels.get).foreach(_.resetRing())

  def events: List[String] = eventLog.asScala.toList

  /** Creates the configfs, sysfs and kernel-ring state of one TCMU device. */
  def addDevice(minor: Int, device: String, cfgstring: String, size: Long, blockSize: Int = 4096,
                maxSectors: Int = 1024, hba: String = "1",
                serial: Option[String] = None): FakeKernel =
    val d = core / s"user_$hba" / device
    os.makeDir.all(d / "attrib")
    os.makeDir.all(d / "action")
    os.makeDir.all(d / "wwn")
    os.write.over(d / "attrib" / "hw_block_size", s"$blockSize\n")
    os.write.over(d / "attrib" / "hw_max_sectors", s"$maxSectors\n")
    os.write.over(d / "info", s"Status: ACTIVATED  Config: $cfgstring  Size: $size  MaxDataAreaMB: 8\n")
    for a <- List("block_dev", "reset_ring") do os.write.over(d / "action" / a, "")
    serial.foreach(s => os.write.over(d / "wwn" / "vpd_unit_serial", s"T10 VPD Unit Serial Number: $s\n"))
    val k = FakeKernel()
    val u = uio / s"uio$minor"
    os.makeDir.all(u / "maps" / "map0")
    os.write.over(u / "name", s"tcm-user/$hba/$device/$cfgstring\n")
    os.write.over(u / "maps" / "map0" / "size", s"0x${k.segment.byteSize().toHexString}\n")
    kernels(minor) = k
    dirs(d) = minor
    k

  def kernel(minor: Int): FakeKernel = kernels(minor)

  /** True once `openRing` returned a ring for `minor`. */
  def wasOpened(minor: Int): Boolean = opened.contains(minor)

  /** True while a ring opened for `minor` has not been closed. */
  def isMapped(minor: Int): Boolean = opened.get(minor).exists(!_.get)

  def openRing(minor: Int, mapSize: Long): (MemorySegment, KernelChannel, () => Unit) =
    eventLog.add(s"openRing $minor")
    if failOpenRing.contains(minor) then
      throw java.io.IOException(s"injected openRing failure for uio$minor")
    val k = kernels.getOrElse(minor, throw java.io.IOException(s"no uio$minor"))
    require(mapSize == k.segment.byteSize(), s"map size $mapSize != ${k.segment.byteSize()}")
    val closed = AtomicBoolean(false)
    opened(minor) = closed
    val closer = () =>
      if closed.compareAndSet(false, true) then
        eventLog.add(s"unmap $minor")
        k.close()
    (k.segment, k.channel, closer)

  /** Releases every ring and the temp tree. */
  def cleanup(): Unit =
    for (minor, k) <- kernels do
      if !opened.get(minor).exists(_.get) then Try(k.close())
    Try(os.remove.all(root))
