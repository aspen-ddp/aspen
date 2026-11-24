package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.store.{StoreId, StoreIncarnation}
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat, StoreConfig}
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}
import org.aspen_ddp.aspen.server.rebuild.RebuildingStore
import org.aspen_ddp.aspen.server.store.StoreIncarnationFile

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/** What a load's config read found at a Live copy's path (LoadDisk.probeStoreConfig) */
enum ConfigRead:
  case Found(cfg: StoreConfig)
  /** No config is there (known absent). `isDir`: the copy's directory is there (a stat that fails reads false) */
  case Absent(isDir: Boolean)
  /** A stat of the config failed, or it could not be read or parsed: held back and retried (awaitUnreadRetry) */
  case Unreadable(err: Throwable)

/** Every file a LoadStore reads before its Store is built (StoreLoader.handleLoad), read in one job. Each field is
 *  as the load decides on it (StoreLoader.applyLoadInputs):
 *  - `hasSaveFile`: whether `crl_save.log` is there; a failed stat is a Failure, never an absence;
 *  - `savedRead`: the save file's content (Right: valid or Left(why it is unusable); None when there is none), or
 *    Left with the error of a read that says nothing of it (CrashRecoveryLog.isUnreadable);
 *  - `incarnation`: the store's identity (StoreIncarnationFile.load; IncarnationLost when known absent);
 *  - `rebuiltGates`: a rebuilt store's acceptor start and inherited-vote gate, None each when known absent;
 *  - `arrivedAt`: its last arrival from another host (readArrivedAt) */
final case class LoadInputs(hasSaveFile: Try[Boolean],
                            savedRead: Either[Throwable, Either[Exception, Option[StoreRecoveryState]]],
                            incarnation: Try[StoreIncarnation],
                            rebuiltGates: Try[(Option[HLCTimestamp], Option[Long])],
                            arrivedAt: Option[Long])

/** A store load's disk work (StoreLoader), as plain functions of paths. Each runs as one job on the store's device's
 *  management lane; its result is applied on the control thread after a recheck. They touch no control state. */
object LoadDisk:

  /** The store config of the copy at `storePath` */
  def readStoreConfig(storePath: os.Path): Try[StoreConfig] =
    Try(StoreConfig.loadStoreConfig(storePath / StoreConfig.configFilename))

  /** tryLoadStore's read of the copy at `storePath`: its config, or why there is none. Every existence test is
   *  FileStat's: a stat that fails is no absence, so it takes the unreadable-config retry like a failed read */
  def probeStoreConfig(storePath: os.Path): ConfigRead =
    try
      if FileStat.exists(storePath / StoreConfig.configFilename) then
        readStoreConfig(storePath).fold(ConfigRead.Unreadable(_), ConfigRead.Found(_))
      else
        // Only logged, so a directory whose stat fails is passed over quietly
        ConfigRead.Absent(Try(FileStat.isDir(storePath)).getOrElse(false))
    catch case t: Throwable => ConfigRead.Unreadable(t)

  /** Whether `path` is there: None when its stat fails (FileStat.existence) */
  def existence(path: os.Path): Option[Boolean] = FileStat.existence(path)

  /** Every read handleLoad makes for the store `storeId` whose backend is at `storePath`, save file `saveFile`. Never
   *  throws: each read's failure is its field's */
  def readLoadInputs(storePath: os.Path, saveFile: os.Path, storeId: StoreId,
                     horizon: FiniteDuration): LoadInputs =
    val hasSaveFile = Try(FileStat.exists(saveFile))
    val incarnation = Try(StoreIncarnationFile.load(storePath, storeId))
    val rebuiltGates = Try((RebuildingStore.readAcceptorSince(storePath),
                            RebuildingStore.readInheritedVotes(storePath, horizon)))
    val savedRead: Either[Throwable, Either[Exception, Option[StoreRecoveryState]]] =
      if !hasSaveFile.getOrElse(false) then
        Right(Right(None))
      else
        try Right(CrashRecoveryLog.loadValidStoreState(saveFile, storeId).map(Some(_)))
        catch case t: Throwable => Left(t)
    LoadInputs(hasSaveFile, savedRead, incarnation, rebuiltGates, readArrivedAt(storePath))

  /** Writes StoreLoader.ArrivedAtFile durably: this host's wall clock now */
  def writeArrivedAt(storePath: os.Path): Unit =
    DurableFiles.write(storePath / StoreLoader.ArrivedAtFile, System.currentTimeMillis().toString)

  /** The store's last arrival from another host, if any. A file that cannot be stat'd or read counts as an arrival
   *  now: later than the truth, which only refuses more transactions. Only a known absence (FileStat) is none */
  def readArrivedAt(storePath: os.Path): Option[Long] =
    val file = storePath / StoreLoader.ArrivedAtFile
    if FileStat.existence(file).contains(false) then
      None
    else
      Some(Try(os.read(file).trim.toLong).getOrElse(System.currentTimeMillis()))
