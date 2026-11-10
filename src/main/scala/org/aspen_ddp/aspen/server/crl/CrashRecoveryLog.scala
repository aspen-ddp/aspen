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
  /** Where a device's CRL files live */
  def deviceCrlDir(devicePath: os.Path): os.Path = devicePath / "crl"

  /** Creates a new device's crl directory and forces it to disk. Device creation must do this: a device that holds
   *  stores but has no crl directory is taken to have lost its CRL, and its stores are rebuilt. */
  def createDeviceCrlDir(devicePath: os.Path): Unit =
    os.makeDir.all(deviceCrlDir(devicePath))
    DurableFiles.syncDirectory(devicePath)

  /** Writes `state` (transactions and applied stubs) to `saveFilePath`, replacing it atomically and forcing it to disk
   *  before returning (DurableFiles): the file is what lets a closed store, possibly on a disk pulled right after its
   *  shutdown, resume its transactions wherever it is next loaded. */
  def saveStoreState(storeId: StoreId,
                     state: StoreRecoveryState,
                     saveFilePath: os.Path): Unit =
    DurableFiles.writeWith(saveFilePath): fos =>
      val header = new Array[Byte](17)

      val storeIdHeader = ByteBuffer.wrap(header)
      storeIdHeader.order(ByteOrder.BIG_ENDIAN)
      storeIdHeader.putLong(storeId.poolId.uuid.getMostSignificantBits)
      storeIdHeader.putLong(storeId.poolId.uuid.getLeastSignificantBits)
      storeIdHeader.put(storeId.poolIndex)

      fos.write(header)

      def write(entry: codec.StoreCRLEntry): Unit =
        val length = new Array[Byte](8)
        val msg = entry.toByteArray

        ByteBuffer.wrap(length).order(ByteOrder.BIG_ENDIAN).putLong(msg.size)

        fos.write(length)
        fos.write(msg)

      state.transactions.foreach(trs => write(codec.StoreCRLEntry(trs = Some(Codec.encode(trs)))))
      state.appliedStubs.foreach(txid => write(codec.StoreCRLEntry(appliedStub = Some(Codec.encodeUUID(txid.uuid)))))

  /** Thrown by loadStoreState for a file that is shorter than its own framing says: a write
   *  that never finished, or a file truncated by a crash. */
  class TruncatedSaveFile(path: os.Path, what: String)
    extends Exception(s"CRL save file $path is truncated: $what")

  /** Reads a file written by saveStoreState. Strict: a file cut short anywhere -- including a
   *  zero-length one -- throws TruncatedSaveFile rather than loading as fewer transactions, so a
   *  caller deciding whether a store's state is intact can rely on it. */
  def loadStoreState(saveFilePath: os.Path): (StoreId, StoreRecoveryState) =
    var trsList: List[TransactionRecoveryState] = Nil
    var stubs: Set[TransactionId] = Set()
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
        (m.trs, m.appliedStub) match
          case (Some(trs), None) => trsList = Codec.decode(trs) :: trsList
          case (None, Some(uuid)) => stubs += TransactionId(Codec.decodeUUID(uuid))
          case _ => throw InvalidSaveFile(saveFilePath, "an entry is neither a transaction nor an applied stub")

      (storeId, StoreRecoveryState(trsList.reverse, stubs))
    finally
      fc.close()

  /** Thrown, or carried by a failed load, when a store's CRL save file cannot be used. */
  class InvalidSaveFile(path: os.Path, reason: String)
    extends Exception(s"CRL save file $path is unusable: $reason")

  /** Loads the save file at `saveFilePath` as `storeId`'s state. Left for any file that is
   *  missing, unreadable, truncated, or whose header names a different store: in every such case
   *  the transactions it held are not recoverable from it. */
  def loadValidStoreState(saveFilePath: os.Path,
                          storeId: StoreId): Either[InvalidSaveFile, StoreRecoveryState] =
    try
      val (fileStoreId, trs) = loadStoreState(saveFilePath)
      if fileStoreId == storeId then
        Right(trs)
      else
        Left(InvalidSaveFile(saveFilePath, s"it belongs to store $fileStoreId, not $storeId"))
    catch
      case t: Throwable => Left(InvalidSaveFile(saveFilePath, t.toString))


trait CrashRecoveryLog {
  /** The transactions held for `storeId`. May block on the CRL's own thread: stores recover through readStoreState */
  def getFullRecoveryState(storeId: StoreId): List[TransactionRecoveryState]

  /** The applied stubs held for `storeId` (StoreRecoveryState). May block on the CRL's own thread, as
    * getFullRecoveryState does */
  def getAppliedStubs(storeId: StoreId): Set[TransactionId]

  /** Everything held for `storeId`, its transactions and its applied stubs, which stay in the CRL: removed only by
    * deleteTransactions, which records the deletion. A transfer out writes them to its save file first, so a crash or
    * failed save in between leaves the CRL holding them. The default reads getFullRecoveryState and getAppliedStubs,
    * for CRLs whose reads do not block. */
  def readStoreState(storeId: StoreId): Future[StoreRecoveryState] =
    Future.successful(StoreRecoveryState(getFullRecoveryState(storeId), getAppliedStubs(storeId)))

  /** Saves `state`, read from a crl_save.log, as `storeId`'s: its transactions and its applied stubs */
  def loadStore(storeId: StoreId,
                state: StoreRecoveryState): Future[Unit] =

    import scala.concurrent.ExecutionContext.Implicits.global

    def saveT(trs: TransactionRecoveryState): Future[Unit] =
      val p = Promise[Unit]()
      def c(): Unit =
        p.success(())
      save(trs.txd.transactionId, trs, c)
      p.future

    def saveStub(txid: TransactionId): Future[Unit] =
      val p = Promise[Unit]()
      saveAppliedStub(storeId, txid, () => p.success(()))
      p.future

    // Stubs first: a stub's save replaces a transaction's state of the same id, which a later save then keeps
    Future.sequence(state.appliedStubs.toList.map(saveStub)).flatMap: _ =>
      Future.sequence(state.transactions.map(saveT)).map(_ => ())

  def save(txid: TransactionId, state: TransactionRecoveryState, completionHandler: () => Unit): Unit

  def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit

  /** Deletes the transaction state held for `txid` of `storeId`. An applied stub of it stays */
  def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit

  /** Saves an applied stub for `txid` of `storeId`, in place of any transaction state the store holds for it, in one
    * write. `completionHandler` runs, on the CRL's thread, once it is durable. Never subject to CrlAdmission: a stub is
    * a few dozen bytes, and the stores that write one are concluding a transaction (StoreRecoveryState) */
  def saveAppliedStub(storeId: StoreId, txid: TransactionId, completionHandler: () => Unit): Unit

  /** Deletes the applied stub held for `txid` of `storeId`, if any. Its transaction state, if any, stays */
  def deleteAppliedStub(storeId: StoreId, txid: TransactionId): Unit

  /** Deletes `txids` of `storeId`, transaction states and applied stubs alike, completing once the deletion is
    * durable: a restart does not recover them. An entry not held in memory is still deleted from the files. The
    * default only deletes them, for CRLs that keep nothing on disk. */
  def deleteTransactions(storeId: StoreId, txids: List[TransactionId]): Future[Unit] =
    txids.foreach: txid =>
      deleteTransaction(storeId, txid)
      deleteAppliedStub(storeId, txid)
    Future.unit

  /** Drops every entry of `storeId` for good, applied stubs included, completing once that is durable. For a store
    * whose state is discarded (it is rebuilt), or no longer on the device: the caller must already have made it
    * unservable (`rebuild-required`) or know it is not here. Only the entries held when it is called are dropped:
    * entries saved after the call, by the store loaded again (transferred back), are kept, however long the drop
    * takes. The default keeps that by reading through readStoreState, which a CRL must answer in order with its
    * saves. */
  def dropStore(storeId: StoreId): Future[Unit] =
    readStoreState(storeId).flatMap(state => deleteTransactions(storeId, state.entryIds))(
      using scala.concurrent.ExecutionContext.parasitic)

  /** The stores the CRL holds entries for, applied stubs included. The default holds none, for CRLs that keep nothing
    * on disk. */
  def storesWithEntries(): Future[Set[StoreId]] = Future.successful(Set())

  /** Stops the CRL's threads. Blocks until they have stopped */
  def shutdown(): Unit = ()

  /** How many bytes of live state the CRL admits */
  def admissionLimitBytes: Long = Long.MaxValue
}
