package org.aspen_ddp.aspen.server.store.backend

import java.util.UUID

import scala.util.control.NonFatal

import org.aspen_ddp.aspen.server.{DurableFiles, FileStat, StoreConfig}
import org.aspen_ddp.aspen.server.store.StoreIncarnationFile
import scribe.Logging

/** Creates store directories atomically, for every backend (systemic-loss-protection 4.1, row D5).
 *
 *  A store directory reaches its final path only complete: the backend's files, initialized and closed, its
 *  incarnation file and its `StoreConfig`, all durable. It is built in a scratch directory under the device's
 *  `creating/` and renamed into place, so a crash leaves either nothing at the target or the whole store. Opening therefore never creates: a
 *  store path whose backend files are missing or blank is a store that lost them, and its backend reports
 *  `BackendStateLost`. Rebuilds and transfers already arrive this way (staging, then a move).
 *
 *  `creating/` is invisible to the device scan (a store's config file sits one level below it), and whatever it
 *  holds when a device loads was left by a create that crashed, so `DeviceDiscovery` deletes it.
 */
object StoreCreation extends Logging:
  /** Device-root directory of in-progress creates */
  val CreatingDirectory = "creating"

  /** Builds `storeCfg`'s store in a scratch directory under `creatingRoot`, with `init` writing (and closing) the
   *  backend's files there, writes the config, forces the tree, and renames it to `target`, durably. `target` must not
   *  exist: the rename refuses to replace it. `creatingRoot` must be on `target`'s file system. On any failure the
   *  scratch copy is removed, best effort, and the error rethrown; `target` is untouched. Blocks on I/O.
   */
  def createAtomically(storeCfg: StoreConfig, target: os.Path, creatingRoot: os.Path)(init: os.Path => Unit): Unit =
    val scratch = creatingRoot / s"${storeCfg.storeId.directoryName}-${UUID.randomUUID()}"
    try
      // A stat that fails throws (FileStat): never a rename over a target that may be there
      if FileStat.exists(target) then
        throw new java.nio.file.FileAlreadyExistsException(target.toString)
      os.makeDir.all(scratch)
      init(scratch)
      // The store's identity, made with it: a load never creates one (StoreIncarnationFile)
      StoreIncarnationFile.writeFresh(scratch)
      DurableFiles.write(scratch / StoreConfig.configFilename, storeCfg.yamlConfig)
      DurableFiles.syncTree(scratch)
      os.makeDir.all(target / os.up)
      // atomicMove is a rename(2), which on Linux would replace an empty directory at target: checked above
      os.move(scratch, target, atomicMove = true)
      DurableFiles.syncDirectory(target / os.up)
      DurableFiles.syncDirectory(creatingRoot)
    catch
      case e: Throwable =>
        try os.remove.all(scratch)
        catch case NonFatal(r) => e.addSuppressed(r)
        throw e
