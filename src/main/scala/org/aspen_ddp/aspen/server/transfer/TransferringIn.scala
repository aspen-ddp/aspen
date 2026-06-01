package org.aspen_ddp.aspen.server.transfer

import scribe.Logging
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.StorageDeviceId
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.BackgroundTaskManager

import java.nio.file.Path
import java.util.UUID
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration, SECONDS}

object TransferringIn:
  val TransferDirectory = "transferring-in"

class TransferringIn( val client: AspenClient,
                      val storeId: StoreId,
                      val storageDeviceId: StorageDeviceId,
                      val devicePath: Path ) extends Logging:

  import TransferringIn.*

  val transferUUID: UUID = UUID.randomUUID()
  
  private val completionPromise: Promise[TransferringIn] = Promise()
  private val transferInPath = os.Path(devicePath) / TransferDirectory / storeId.directoryName
  private val finalPath = os.Path(devicePath) / storeId.directoryName
  
  def complete: Future[TransferringIn] = completionPromise.future

  if os.exists(finalPath) then
    throw new Exception(s"Store path already exists! $finalPath")

  private def deleteTransferPath(): Unit =
    try
      os.remove.all(transferInPath)
    catch
      case t: Throwable => logger.error(f"Failed to remove transfer-in directory $transferInPath. Error: $t")
      
  if os.exists(transferInPath) then
    deleteTransferPath()

  os.makeDir.all(transferInPath)

  private var done = false
  private var lastDataReceivedTimestamp = HLCTimestamp.now
  private val extractionProcess = os.spawn(
    cmd = ("jar", "--extract"),
    cwd = transferInPath
  )
  
  logger.info(f"Started transfer of store $storeId")

  private def fail(errorMessage: String): Unit =
    done = true
    bgTask.cancel()
    if extractionProcess.isAlive() then
      extractionProcess.destroy()
    deleteTransferPath()
    val msg = f"Transfer ailed for store $storeId: $errorMessage"
    logger.info(msg)
    completionPromise.failure(new Exception(msg))

  private val bgTask: BackgroundTaskManager.ScheduledTask = client.backgroundTaskManager.
    schedulePeriodic(Duration(30, SECONDS)):
      if !done then
        if !extractionProcess.isAlive() then
          fail("Extraction process died")
        else
          if HLCTimestamp.now - lastDataReceivedTimestamp > Duration(30, SECONDS) then
            fail("No data received with timeout window")

  def dataReceived(db: DataBuffer): Unit =
    if !done && extractionProcess.isAlive() then

      lastDataReceivedTimestamp = HLCTimestamp.now

      if db.isEmpty then
        done = true
        bgTask.cancel()
        extractionProcess.stdin.flush()
        extractionProcess.stdin.close()
        logger.info(f"Transfer completed for store $storeId")
        extractionProcess.wait() // TODO: Move this to a background thread?

        val tout = transferInPath / TransferringOut.MarkerFile

        if os.exists(tout) then
          os.remove(tout)

        os.move(transferInPath, finalPath)

        completionPromise.success(this)
      else
        extractionProcess.stdin.write(db.getByteArray)
