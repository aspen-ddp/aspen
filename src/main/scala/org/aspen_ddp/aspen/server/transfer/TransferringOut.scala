package org.aspen_ddp.aspen.server.transfer

import org.apache.logging.log4j.scala.Logging
import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.metadata.{HostId, StorageDeviceId}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.network.StoreTransferData
import org.aspen_ddp.aspen.common.store.StoreId

import java.nio.file.Path
import java.util.UUID
import scala.concurrent.{Future, Promise}

object TransferringOut:
  val MarkerFile = "transferring-out"

class TransferringOut( val client: AspenClient,
                       val fromDevice: StorageDeviceId,
                       val devicePath: Path,
                       val storeId: StoreId,
                       val toHost: HostId,
                       val toDevice: StorageDeviceId,
                       val timestamp: HLCTimestamp,
                       val transferUUID: UUID ) extends Logging:

  import TransferringOut.*

  private val storePath = os.Path(devicePath) / storeId.directoryName
  private val completionPromise: Promise[TransferringOut] = Promise()

  def complete: Future[TransferringOut] = completionPromise.future

  // Create marker file so the server knows not to load this store
  os.write.over(storePath / MarkerFile, "")

  logger.info(s"Beginning transfer of store $storeId on storage device $fromDevice to storage device $toDevice on hostState $toHost")

  private val creationProcess = os.spawn(
    cmd = ("jar", "--create"),
    cwd = storePath
  )

  private val sendThread = new Thread:
    override def run(): Unit = readThread()

  sendThread.start()

  def abort(): Unit = creationProcess.destroy()

  private def readThread(): Unit =
    val buff = new Array[Byte](1024 * 1024)

    def readFully(): (Int, Boolean) =
      var pos = 0
      var nread = 0
      while nread != -1 && pos < buff.length do
        nread = creationProcess.stdout.read(buff, pos, buff.length - pos)
        if nread != -1 then
          pos += nread
      (pos, nread == -1)

    def sendData(arr: Array[Byte]): Unit =
      client.sendHostMessage(StoreTransferData(
        toHost,
        client.clientId,
        transferUUID,
        DataBuffer(arr)
      ))

    var eof = false

    try
      while !eof do
        val (nread, eofFound) = readFully()
        eof = eofFound

        val arr = if nread == buff.length then
          buff
        else
          java.util.Arrays.copyOfRange(buff, 0, nread)

        sendData(arr)

        if eof && arr.nonEmpty then
          sendData(new Array[Byte](0))
    catch
      case t: Throwable => logger.warn(s"Transfer out of store $storeId failed. Error: $t")

    completionPromise.success(this)
