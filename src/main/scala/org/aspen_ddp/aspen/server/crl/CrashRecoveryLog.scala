package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.UUID
import scala.concurrent.{Future, Promise}

object CrashRecoveryLog:
  def saveStoreState(storeId: StoreId,
                     trsList: List[TransactionRecoveryState],
                     saveFilePath: Path): Unit =

    val fos = Files.newOutputStream(saveFilePath,
      StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE)

    val header = new Array[Byte](17)

    val storeIdHeader = ByteBuffer.wrap(header)
    storeIdHeader.order(ByteOrder.BIG_ENDIAN)
    storeIdHeader.putLong(storeId.poolId.uuid.getMostSignificantBits)
    storeIdHeader.putLong(storeId.poolId.uuid.getLeastSignificantBits)
    storeIdHeader.put(storeId.poolIndex)

    fos.write(header)

    def write(trs: TransactionRecoveryState): Unit =
      val builder = codec.StoreCRLEntry.newBuilder()
      builder.setTrs(Codec.encode(trs))

      val length = new Array[Byte](8)
      val msg = builder.build.toByteArray

      ByteBuffer.wrap(length).order(ByteOrder.BIG_ENDIAN).putLong(msg.size)

      fos.write(length)
      fos.write(msg)

    trsList.foreach(trs => write(trs))

    fos.close()

  def loadStoreState(saveFilePath: Path): (StoreId, List[TransactionRecoveryState]) =
    var trsList: List[TransactionRecoveryState] = Nil
    val fc = Files.newByteChannel(saveFilePath)

    val header = ByteBuffer.allocate(17)
    header.order(ByteOrder.BIG_ENDIAN)
    fc.read(header)
    header.position(0)
    val msb = header.getLong()
    val lsb = header.getLong()
    val idx = header.get()

    val storeId = StoreId(org.aspen_ddp.aspen.common.pool.PoolId(new UUID(msb, lsb)), idx)

    while fc.position() < fc.size() do
      val lbb = ByteBuffer.allocate(8)
      lbb.order(ByteOrder.BIG_ENDIAN)
      fc.read(lbb)
      lbb.position(0)
      val entryLength = lbb.getLong

      val entry = ByteBuffer.allocate(entryLength.toInt)
      entry.order(ByteOrder.BIG_ENDIAN)
      fc.read(entry)
      entry.position(0)

      val m = codec.StoreCRLEntry.parseFrom(entry)
      trsList = Codec.decode(m.getTrs) :: trsList

    (storeId, trsList)


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
