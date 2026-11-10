package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.crl.{CrashRecoveryLog, StoreRecoveryState}
import org.aspen_ddp.aspen.server.store.backend.Backend
import org.aspen_ddp.aspen.server.transfer.{StoreTransfer, StoreTransferService}

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

/** A StoreTransferService whose transfers move no bytes and finish only when the test says so.
 *
 *  The real StoreTransferManager needs a live sending host, a TLS socket and a `jar` subprocess,
 *  none of which exists here, but the interesting half of a transfer for these tests is what Host
 *  does once the Future from beginTransferIn resolves. beginTransferOut is unimplemented on
 *  purpose: nothing in this suite ships a store out, and a silently-working stub would hide it if
 *  something started to.
 */
class StagedTransferService extends StoreTransferService:
  private val calls = mutable.ArrayBuffer[(StoreId, UUID, os.Path, Promise[Unit])]()
  private val cancels = mutable.ArrayBuffer[(StoreId, UUID)]()

  /** Every cancelTransferIn call, in order. */
  def cancelledTransfersIn: Vector[(StoreId, UUID)] = synchronized(cancels.toVector)

  /** Every beginTransferIn call, in order. */
  def transfersIn: Vector[(StoreId, UUID, os.Path)] =
    synchronized(calls.map((sid, uuid, dir, _) => (sid, uuid, dir)).toVector)

  /** The stores a transfer in has been begun for. */
  def startedTransfersIn: Set[StoreId] = transfersIn.map(_._1).toSet

  private def latest(storeId: StoreId): Option[(StoreId, UUID, os.Path, Promise[Unit])] =
    synchronized(calls.findLast(_._1 == storeId))

  /** Resolves `storeId`'s latest transfer as if every byte had arrived. Leaves behind what
   *  extraction would have: `<dir>/<storeId.directoryName>`, still carrying the source's
   *  transfer-out marker and the CRL save file its close wrote, for Host's promotion step to
   *  move into place. `crlValid = false` leaves a truncated save file instead. */
  def finishTransferIn(storeId: StoreId, crlValid: Boolean = true): Unit =
    latest(storeId).foreach: (sid, _, dir, p) =>
      val staged = dir / sid.directoryName
      os.makeDir.all(staged)
      os.write.over(staged / StoreTransfer.TransferOutMarkerFile, "")
      if crlValid then
        CrashRecoveryLog.saveStoreState(sid, StoreRecoveryState.Empty, Backend.crlSaveFile(staged))
      else
        os.write.over(Backend.crlSaveFile(staged), "")
      p.success(())

  /** Fails `storeId`'s latest transfer, as a timed-out or truncated stream would. */
  def failTransferIn(storeId: StoreId): Unit =
    latest(storeId).foreach(_._4.failure(new Exception(s"Simulated failure of transfer in of $storeId")))

  def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path): Future[Unit] =
    synchronized:
      val p = Promise[Unit]()
      calls += ((storeId, transferUUID, destinationDirectory, p))
      p.future

  def cancelTransferIn(storeId: StoreId, transferUUID: UUID): Unit =
    val p = synchronized:
      cancels += ((storeId, transferUUID))
      calls.find(c => c._1 == storeId && c._2 == transferUUID).map(_._4)
    p.foreach(_.tryFailure(new Exception(s"Cancelled transfer in of $storeId")))

  def beginTransferOut(storeId: StoreId, transferUUID: UUID, sourceDirectory: os.Path,
                       destHost: String, destPort: Int, timestamp: HLCTimestamp): Unit =
    throw new UnsupportedOperationException("StagedTransferService does not send stores")

  // Sends exist only when a test declares one with markSending: this service never ships bytes.
  private var sending = Set[StoreId]()
  private val outCancels = mutable.ArrayBuffer[StoreId]()

  /** Makes `storeId` read as transferring out until cancelTransferOut is called for it. */
  def markSending(storeId: StoreId): Unit = synchronized:
    sending += storeId

  /** Every cancelTransferOut call, in order. */
  def cancelledTransfersOut: Vector[StoreId] = synchronized(outCancels.toVector)

  def cancelTransferOut(storeId: StoreId): Unit = synchronized:
    outCancels += storeId
    sending -= storeId

  def isTransferringOut(storeId: StoreId): Boolean = synchronized(sending.contains(storeId))

  def transferringOutStores: Set[StoreId] = synchronized(sending)


/** A StoreTransferService whose sends move no bytes and end only when the test says so, for
 *  tests where this host is the transfer's source. beginTransferIn is unimplemented on purpose,
 *  as beginTransferOut is in StagedTransferService.
 */
class StagedSendService extends StoreTransferService:
  private var sending = Set[StoreId]()
  private var begun = Vector[(StoreId, UUID)]()

  /** Every beginTransferOut call, in order. */
  def transfersOut: Vector[(StoreId, UUID)] = synchronized(begun)

  /** Ends `storeId`'s send, as a failure or a completion would: it is no longer transferring out. */
  def endTransferOut(storeId: StoreId): Unit = synchronized:
    sending -= storeId

  def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path): Future[Unit] =
    throw new UnsupportedOperationException("StagedSendService does not receive stores")

  def cancelTransferIn(storeId: StoreId, transferUUID: UUID): Unit = ()

  def beginTransferOut(storeId: StoreId, transferUUID: UUID, sourceDirectory: os.Path,
                       destHost: String, destPort: Int, timestamp: HLCTimestamp): Unit = synchronized:
    begun :+= (storeId, transferUUID)
    sending += storeId

  def cancelTransferOut(storeId: StoreId): Unit = endTransferOut(storeId)

  def isTransferringOut(storeId: StoreId): Boolean = synchronized(sending.contains(storeId))

  def transferringOutStores: Set[StoreId] = synchronized(sending)
