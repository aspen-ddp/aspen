package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec
import org.aspen_ddp.aspen.server.DurableFiles

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import scala.concurrent.{Future, Promise}

object CrashRecoveryLog:
  /** Writes `trsList` to `saveFilePath`, replacing it atomically and forcing it to disk before
   *  returning (DurableFiles): the file is what lets a closed store, possibly on a disk pulled
   *  right after its shutdown, resume its transactions wherever it is next loaded. */
  def saveStoreState(storeId: StoreId,
                     trsList: List[TransactionRecoveryState],
                     saveFilePath: os.Path): Unit =
    DurableFiles.writeWith(saveFilePath): fos =>
      val header = new Array[Byte](17)

      val storeIdHeader = ByteBuffer.wrap(header)
      storeIdHeader.order(ByteOrder.BIG_ENDIAN)
      storeIdHeader.putLong(storeId.poolId.uuid.getMostSignificantBits)
      storeIdHeader.putLong(storeId.poolId.uuid.getLeastSignificantBits)
      storeIdHeader.put(storeId.poolIndex)

      fos.write(header)

      def write(trs: TransactionRecoveryState): Unit =
        val entry = codec.StoreCRLEntry(trs = Some(Codec.encode(trs)))

        val length = new Array[Byte](8)
        val msg = entry.toByteArray

        ByteBuffer.wrap(length).order(ByteOrder.BIG_ENDIAN).putLong(msg.size)

        fos.write(length)
        fos.write(msg)

      trsList.foreach(trs => write(trs))

  /** Thrown by loadStoreState for a file that is shorter than its own framing says: a write
   *  that never finished, or a file truncated by a crash. */
  class TruncatedSaveFile(path: os.Path, what: String)
    extends Exception(s"CRL save file $path is truncated: $what")

  /** Reads a file written by saveStoreState. Strict: a file cut short anywhere -- including a
   *  zero-length one -- throws TruncatedSaveFile rather than loading as fewer transactions, so a
   *  caller deciding whether a store's state is intact can rely on it. */
  def loadStoreState(saveFilePath: os.Path): (StoreId, List[TransactionRecoveryState]) =
    var trsList: List[TransactionRecoveryState] = Nil
    val fc = os.read.channel(saveFilePath)
    try
      def readFully(buf: ByteBuffer, what: String): Unit =
        while buf.hasRemaining do
          if fc.read(buf) < 0 then
            throw TruncatedSaveFile(saveFilePath, what)
        buf.position(0)

      val header = ByteBuffer.allocate(17)
      header.order(ByteOrder.BIG_ENDIAN)
      readFully(header, "header")
      val msb = header.getLong()
      val lsb = header.getLong()
      val idx = header.get()

      val storeId = StoreId(org.aspen_ddp.aspen.common.pool.PoolId(new UUID(msb, lsb)), idx)

      while fc.position() < fc.size() do
        val lbb = ByteBuffer.allocate(8)
        lbb.order(ByteOrder.BIG_ENDIAN)
        readFully(lbb, "entry length")
        val entryLength = lbb.getLong
        if entryLength < 0 || entryLength > fc.size() - fc.position() then
          throw TruncatedSaveFile(saveFilePath, s"entry of $entryLength bytes")

        val entryBytes = new Array[Byte](entryLength.toInt)
        readFully(ByteBuffer.wrap(entryBytes), "entry")

        val m = codec.StoreCRLEntry.parseFrom(entryBytes)
        trsList = Codec.decode(m.trs.get) :: trsList

      (storeId, trsList)
    finally
      fc.close()

  /** Thrown, or carried by a failed load, when a store's CRL save file cannot be used. */
  class InvalidSaveFile(path: os.Path, reason: String)
    extends Exception(s"CRL save file $path is unusable: $reason")

  /** Loads the save file at `saveFilePath` as `storeId`'s state. Left for any file that is
   *  missing, unreadable, truncated, or whose header names a different store: in every such case
   *  the transactions it held are not recoverable from it. */
  def loadValidStoreState(saveFilePath: os.Path,
                          storeId: StoreId): Either[InvalidSaveFile, List[TransactionRecoveryState]] =
    try
      val (fileStoreId, trs) = loadStoreState(saveFilePath)
      if fileStoreId == storeId then
        Right(trs)
      else
        Left(InvalidSaveFile(saveFilePath, s"it belongs to store $fileStoreId, not $storeId"))
    catch
      case t: Throwable => Left(InvalidSaveFile(saveFilePath, t.toString))


trait CrashRecoveryLog {
  def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState]

  def closeStore(storeId: StoreId): Future[List[TransactionRecoveryState]]

  def loadStore(storeId: StoreId,
                trsList: List[TransactionRecoveryState]): Future[Unit] =

    import scala.concurrent.ExecutionContext.Implicits.global

    def saveT(trs: TransactionRecoveryState): Future[Unit] =
      val p = Promise[Unit]()
      def c(): Unit =
        p.success(())
      save(trs.txd.transactionId, trs, c)
      p.future

    Future.sequence(trsList.map(saveT)).map(_ => ())

  def save(txid: TransactionId, state: TransactionRecoveryState, completionHandler: () => Unit): Unit

  def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit

  def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit
}
