package org.aspen_ddp.aspen.server.rebuild

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.loadYamlFile
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat}
import org.aspen_ddp.aspen.server.host.{CopyLayout, Phase}
import org.aspen_ddp.aspen.server.store.StoreIncarnationFile
import scribe.Logging

import java.util.UUID
import scala.util.control.NonFatal

/** A rebuild's staging-directory disk work: the checkpoint's reads and writes, finish's completion files and move, and
 *  the staging directory's removal. Every call blocks on the disk, so RebuildingStore runs each as a
 *  `JobKind.Staging` job on the destination device's DeviceIo, never on the client context or the control thread. */
object RebuildDisk extends Logging:

  /** Write the checkpoint into `dir`, replacing any existing one.
   *
   *  Written to a temp name alongside it and renamed into place rather than overwritten. Aspen
   *  is crash-only -- there is no clean shutdown -- so a crash between a truncate and the write
   *  that follows it is the ordinary termination mode landing in an unlucky window, not an
   *  exotic case, and it would leave an empty or partial `rebuild-state.yaml` behind. The temp
   *  file is in the same directory, hence the same filesystem, so the rename is atomic: a
   *  reader sees either the old checkpoint or the new one.
   */
  def save(dir: os.Path, state: RebuildState): Unit =
    os.write.over(dir / RebuildState.tempFilename, state.yamlConfig)
    os.move(dir / RebuildState.tempFilename, dir / RebuildState.stateFilename,
            replaceExisting = true, atomicMove = true)

  /** Delete the checkpoint from `dir`, along with any temp file a crashed save left behind. Either may be missing:
   *  `os.remove` without `checkExists` is `Files.deleteIfExists`, so a missing file is no error. */
  def removeAll(dir: os.Path): Unit =
    os.remove(dir / RebuildState.stateFilename)
    os.remove(dir / RebuildState.tempFilename)

  /** No inherited vote is held any more (Frontend.removeInheritedStateWhenDone). Not forced: a reload that still
   *  finds the file queries again. Blocks on the disk: the Frontend runs it as a device-lane job. */
  def removeInheritedVotes(storePath: os.Path): Unit =
    try os.remove(storePath / RebuildingStore.InheritedVotesFile)
    catch case NonFatal(_) => ()

  def loadRebuildState(file: os.Path): RebuildState =
    RebuildState.create(loadYamlFile(file))

  /** Read the checkpoint from `dir`, or None if there is none or it cannot be read.
   *
   *  Total by design. An unreadable checkpoint -- torn by a crash predating the atomic save
   *  above, or corrupt on disk -- is reported as "no checkpoint" rather than thrown, because
   *  `load` is the first statement of a rebuild pass and a throw there makes the store
   *  permanently unrebuildable without an operator deleting the file by hand. The cost of
   *  discarding it is a restart of the walk and nothing more: `rebuildWrite` is a plain
   *  overwrite, so re-restoring already-restored objects is free. That is the correct direction
   *  under the checkpoint rule -- lose work, never claim work that never reached stable storage.
   *
   *  A stat that fails is no absence (FileStat): it throws, failing the pass with the checkpoint kept for the next,
   *  rather than discard the walk done so far.
   */
  def load(dir: os.Path): Option[RebuildState] =
    val f = dir / RebuildState.stateFilename
    if !FileStat.exists(f) then
      None
    else
      try
        Some(loadRebuildState(f))
      catch
        case NonFatal(t) =>
          logger.warn(s"Discarding unreadable rebuild checkpoint $f; the rebuild will restart " +
                      s"from the beginning: $t")
          None

  /** Removes the staging directory `staging`, whole, if it is there. Throws on a failed stat or removal */
  def cleanup(staging: os.Path): Unit =
    if FileStat.exists(staging) then os.remove.all(staging)

  /** finish's disk work once the staging backend is closed: the completion files, the checkpoint's removal, and the
   *  rename of `staging` into `rebuilt/` under a fresh copy id, which it returns.
   *
   *  `inheritedVotesAfter` is the time written as the store's inherited-votes gate
   *  (RebuildingStore.inheritedVotesAfter). `syncDirectory` forces the rename's directories. A rename that landed but
   *  could not be forced is reported to `onUnforced` and the copy id returned all the same: the copy is in place */
  def complete(staging: os.Path,
               devicePath: os.Path,
               storeId: StoreId,
               episode: Option[UUID],
               inheritedVotesAfter: Long,
               syncDirectory: os.Path => Unit,
               onUnforced: DurableFiles.MoveNotForced => Unit): UUID =
    // The rebuilt store has no Paxos state: it takes no part in transactions started before now (plus the margin)
    RebuildingStore.writeAcceptorSince(staging)
    // Its old incarnation's commit votes may still count, with none of their locks held here: the store votes abort
    // on what they may lock until its peers' held votes, complete from this time on, release them
    RebuildingStore.writeInheritedVotes(staging, inheritedVotesAfter)
    // A new life for the store, replacing any incarnation the staging directory carried
    StoreIncarnationFile.writeFresh(staging)
    // Its whole transaction state: its first load drops whatever the device CRL holds under its id
    RebuildingStore.writeEmptyCrlState(staging, storeId)
    // Last write before the move: the copy's episode, which its location in rebuilt/ reads (T2). Kept out of service
    // there until its flip commits
    val rebuilt = Phase.Rebuilt(RebuildingStore.rebuildCompleteContent(episode))
    for
      file <- CopyLayout.paramFile(rebuilt)
      p <- CopyLayout.param(rebuilt)
    do DurableFiles.write(staging / file, p)
    // The checkpoint, and a temp file a crash caught between its write and its rename, are kept out of the
    // directory that is about to become a live store
    removeAll(staging)
    // A fresh copy id, as every copy takes when it first enters a phase directory (P4)
    val copyId = UUID.randomUUID()
    val finalPath = CopyLayout.path(devicePath, storeId, copyId, rebuilt)
    val dir = finalPath / os.up
    if !FileStat.exists(dir) then
      os.makeDir.all(dir)
      DurableFiles.syncDirectory(devicePath)
    // Durable before the store is reported in place: a device shut down and moved after this must come back with the
    // store in rebuilt/, or an adopting host refuses it. A failed rename leaves staging whole for the next pass. One
    // that landed but was not forced is in place all the same: reported (placedUnforced), so the host records it
    // unsynced and flips it once a device check has forced it, rather than rebuild the store over it
    try
      DurableFiles.move(staging, finalPath, syncDirectory)
      logger.info(s"Rebuild of $storeId: store in place at $finalPath")
    catch case t: DurableFiles.MoveNotForced =>
      logger.error(s"Rebuild of $storeId: store in place at $finalPath, but its move could not be forced: $t")
      onUnforced(t)
    copyId
