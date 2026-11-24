package org.aspen_ddp.aspen.server

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Ratchet for the device I/O isolation project: control-side code under `server/host`, `server/rebuild` and
 *  `server/transfer` must not touch the disk directly; it goes through a `DeviceIo` lane. `baseline` is the number
 *  of forbidden calls each file still has. It only goes down: a task that converts a file lowers its entry (to
 *  removal) in the same commit, and "baseline has no slack" fails until it does. Comment lines are skipped as in
 *  FileStatUsageSuite. Files named `*Disk.scala`, `DeviceIo.scala`, `ThreadedDeviceIo.scala` and
 *  `LockstepDeviceIo.scala` are the places the I/O lives and are not counted. */
class DeviceIoUsageSuite extends AnyFunSuite with Matchers:
  private val root = os.pwd / "src" / "main" / "scala" / "org" / "aspen_ddp" / "aspen" / "server"
  private val dirs = List("host", "rebuild", "transfer")
  private val forbidden =
    raw"\bos\.(remove|move|list|read|write|makeDir|copy|walk|size|stat|exists|isDir|isFile|perms)\b|DurableFiles\.|FileStat\.|DeviceMarkers\.|saveStoreState|loadValidStoreState|RebuildState\.(load|save|remove)|getFreeSpace|getTotalSpace|getUsableSpace".r
  private val allowedNames = Set("DeviceIo.scala", "ThreadedDeviceIo.scala", "LockstepDeviceIo.scala")

  private val baseline: Map[String, Int] = Map(
    "host/CopyLayout.scala" -> 10,
    "host/CopyMoves.scala" -> 3,
    "host/DeviceDiscovery.scala" -> 12,
    "host/DeviceShutdown.scala" -> 4,
    "host/HostCopies.scala" -> 2,
    "host/JoinCeiling.scala" -> 3,
    "host/LocalDevices.scala" -> 2,
    "host/StoreRebuilds.scala" -> 5,
    "host/StoreTransfers.scala" -> 3,
    "host/StoreTransfersOut.scala" -> 4,
    "rebuild/RebuildingStore.scala" -> 10,
    "rebuild/StoreRebuild.scala" -> 1,
    "transfer/StoreTransferManager.scala" -> 5
  )

  private def counts: Map[String, Int] =
    val found =
      for
        d <- dirs
        file <- os.walk(root / d).filter(_.ext == "scala").toList
        if !file.last.endsWith("Disk.scala") && !allowedNames.contains(file.last)
        n = os.read.lines(file).count: line =>
          val t = line.trim
          if t.startsWith("//") || t.startsWith("*") || t.startsWith("/*") then false
          else
            val code = line.indexOf("//") match
              case -1 => line
              case i => line.take(i)
            forbidden.findFirstIn(code).isDefined
        if n > 0
      yield file.relativeTo(root).toString -> n
    found.toMap

  test("no device I/O in control code beyond the baseline"):
    val over = counts.filter((f, n) => n > baseline.getOrElse(f, 0))
    withClue(s"files over their baseline (file -> count, baseline): ") {
      over.map((f, n) => f -> (n, baseline.getOrElse(f, 0))) shouldBe empty
    }

  test("baseline has no slack"):
    val actual = counts
    withClue("baseline must equal today's counts; lower an entry when a file is converted: ") {
      baseline should be(actual)
    }
