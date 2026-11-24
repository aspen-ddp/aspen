package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.FileStat
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}
import org.aspen_ddp.aspen.server.store.backend.Backend
import scribe.Logging

import scala.util.control.NonFatal

/** A transfer's disk work (StoreTransfers, StoreTransfersOut), as plain functions of paths. Each runs as one job on the
 *  management lane of the device it touches; its result is applied on the control thread after a recheck. They touch
 *  no control state. */
object TransferDisk extends Logging:

  /** Removes the staging directory `staging` of one receive attempt, whole. Throws on a failure (logged by the apply:
   *  the device's next load sweeps `transferring-in/` anyway) */
  def removeStaging(staging: os.Path): Unit = os.remove.all(staging)

  /** What startIn's check of an Arrived copy found and did (checkArrived) */
  enum ArrivedCheck:
    /** It belongs to this transfer and its save file is valid: its pool update resumes */
    case Usable
    /** Its save file could not be read (CrashRecoveryLog.isUnreadable) or stat'd: nothing is said of the copy, which
     *  is neither resumed nor deleted */
    case Unreadable(err: Throwable)
    /** It belonged to another transfer or its save file was unusable (`why`): deleted rename-first (T13) */
    case Deleted(why: String)
    /** As Deleted, but the delete did not answer the copy gone (CopyDisk.delete): it stays where it is */
    case NotDeleted(why: String)

  /** startIn's check of the Arrived copy at `copyPath` on the device at `devicePath`: `mismatch` is why it belongs to
   *  another transfer, if it does; otherwise its save file is validated. A copy found unusable is deleted
   *  rename-first (CopyDisk.delete) in the same job */
  def checkArrived(devicePath: os.Path, copyPath: os.Path, storeId: StoreId, mismatch: Option[String]): ArrivedCheck =
    val unusable: Either[Throwable, Option[String]] =
      mismatch match
        case Some(why) => Right(Some(why))
        case None =>
          try
            CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(copyPath), storeId) match
              case Right(_) => Right(None)
              case Left(err) => Right(Some(s"its CRL save file is no longer usable: $err"))
          catch case t: Throwable => Left(t)
    unusable match
      case Left(err) => ArrivedCheck.Unreadable(err)
      case Right(None) => ArrivedCheck.Usable
      case Right(Some(why)) =>
        if CopyDisk.delete(devicePath, copyPath, s"an unusable arrived copy of store $storeId") then
          ArrivedCheck.Deleted(why)
        else
          ArrivedCheck.NotDeleted(why)

  /** What a transfer-in promotion did (promote) */
  enum Promotion:
    /** The staged copy's save file is unusable or could not be read: nothing moved */
    case Invalid(err: Throwable)
    /** The move into arrived/ was tried: its result */
    case Moved(result: MoveResult)

  /** A received copy's promotion (T8), in one job: the staged copy at `stagedPath`'s CRL save file is validated (the
   *  source still holds the state, so a missing, damaged or unreadable file is never promoted), then it is moved to
   *  `target` in `to` (CopyDisk.relocate through CopyMoves' seams). The attempt's `staging` directory is removed
   *  whole after a Durable move or a refused promotion (failure logged); after an Unsynced one it stays, as the resync
   *  forces it */
  def promote(storeId: StoreId, stagedPath: os.Path, staging: os.Path, target: os.Path, to: Phase,
              syncDirectory: os.Path => Unit, rename: (os.Path, os.Path) => Unit): Promotion =
    val valid =
      try CrashRecoveryLog.loadValidStoreState(Backend.crlSaveFile(stagedPath), storeId).left.map(e => e: Throwable)
      catch case t: Throwable => Left(t)
    val promotion = valid match
      case Left(err) => Promotion.Invalid(err)
      case Right(_) => Promotion.Moved(CopyDisk.relocate(stagedPath, target, to, syncDirectory, rename))
    promotion match
      case Promotion.Moved(MoveResult.Unsynced(_)) => ()
      case _ =>
        try removeStaging(staging)
        catch case NonFatal(t) =>
          logger.error(s"Failed to remove the staging directory $staging of the transfer in of store $storeId: $t")
    promotion

  /** T9's disk steps for the Arrived copy at `from`: its arrival (StoreLoader.ArrivedAtFile) written durably while it
   *  is still Arrived and never loaded, then the move to `target` in `to` (CopyDisk.relocate). A failed write moves
   *  nothing: NotMoved */
  def bringArrivedLive(from: os.Path, target: os.Path, to: Phase, syncDirectory: os.Path => Unit,
                       rename: (os.Path, os.Path) => Unit): MoveResult =
    try
      LoadDisk.writeArrivedAt(from)
      CopyDisk.relocate(from, target, to, syncDirectory, rename)
    catch case NonFatal(t) =>
      logger.error(s"Failed to record the arrival of the copy at $from before moving it live. It stays offline. " +
                   s"Error: $t")
      MoveResult.NotMoved(t)

  /** What a transfer out's extraction of a closed store found at its copy (inspectSaveFile) */
  enum SaveFileState:
    /** No directory: nothing to extract */
    case NoCopy
    /** A valid crl_save.log is there: it ships unchanged */
    case Valid
    /** None is there: the device CRL's entries are extracted into it */
    case Absent
    /** One is there but unusable, or a stat or read failed (nothing said of it): the extraction fails */
    case Failed(err: Throwable)

  /** extractUnloaded's reads of the closed copy at `copyPath`: its directory, then its save file. A stat that fails
   *  is Failed, never an absence */
  def inspectSaveFile(copyPath: os.Path, storeId: StoreId): SaveFileState =
    try
      if !FileStat.isDir(copyPath) then
        SaveFileState.NoCopy
      else
        val saveFile = Backend.crlSaveFile(copyPath)
        if !FileStat.exists(saveFile) then
          SaveFileState.Absent
        else
          CrashRecoveryLog.loadValidStoreState(saveFile, storeId) match
            case Right(_) => SaveFileState.Valid
            case Left(err) => SaveFileState.Failed(err)
    catch case t: Throwable => SaveFileState.Failed(t)

  /** Writes `state`, the store's device-CRL entries, as the copy's crl_save.log at `saveFile`, durably */
  def saveExtracted(storeId: StoreId, state: StoreRecoveryState, saveFile: os.Path): Unit =
    CrashRecoveryLog.saveStoreState(storeId, state, saveFile)
