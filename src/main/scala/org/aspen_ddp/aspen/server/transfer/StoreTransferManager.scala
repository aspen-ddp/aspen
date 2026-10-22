package org.aspen_ddp.aspen.server.transfer

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.implementations.quic.TlsCredentials
import org.aspen_ddp.aspen.common.network.tls.TLSContext
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import scribe.Logging

import java.io.InputStream
import java.net.{InetSocketAddress, SocketException}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import javax.net.ssl.{SSLServerSocket, SSLSocket}
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*

object StoreTransferManager:
  val DefaultChunkSize: Int = 1024 * 1024
  val DefaultIoTimeout: FiniteDuration = 90.seconds

  /** 17-byte encoded StoreId followed by the 16-byte transfer UUID (msb, lsb; big-endian) */
  val HeaderSize: Int = 33

  /** The 17-byte encoded StoreId, sent only after the whole archive was produced successfully */
  val TrailerSize: Int = 17

  def encodeHeader(storeId: StoreId, transferUUID: UUID): Array[Byte] =
    val bb = ByteBuffer.allocate(HeaderSize)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(storeId.toBytes)
    bb.putLong(transferUUID.getMostSignificantBits)
    bb.putLong(transferUUID.getLeastSignificantBits)
    bb.array()

  private def decodeHeader(arr: Array[Byte]): (StoreId, UUID) =
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    val storeId = StoreId(PoolId(new UUID(bb.getLong, bb.getLong)), bb.get)
    (storeId, new UUID(bb.getLong, bb.getLong))

  /** One pending transfer-in. Mutable fields are guarded by the owning manager's lock. `done` is set
   *  by whichever party (connection thread, re-registration, close) settles the transfer first. */
  private class Registration(val transferUUID: UUID,
                             val destinationDirectory: os.Path,
                             val promise: Promise[Unit]):
    var activeSocket: Option[SSLSocket] = None
    var extractor: Option[os.SubProcess] = None
    var done: Boolean = false

/** Sends and receives whole store directories as `jar` archives over one mutual-TLS TCP
 *  connection per transfer attempt.
 *
 *  Wire format: 33-byte header, the archive, then the 17-byte StoreId as a trailer. The trailer is
 *  written only after the sender's archiver exits successfully, so a stream that ends without it,
 *  or whose final 17 bytes are not the StoreId, is incomplete.
 */
class StoreTransferManager(port: Int,
                           tls: TlsCredentials,
                           chunkSize: Int = StoreTransferManager.DefaultChunkSize,
                           ioTimeout: FiniteDuration = StoreTransferManager.DefaultIoTimeout)
  extends StoreTransferService with Logging:

  import StoreTransferManager.*

  private val sslContext = TLSContext.create(tls.caCertificate, tls.certificate, tls.privateKey)

  private val serverSocket: SSLServerSocket =
    val ss = sslContext.getServerSocketFactory.createServerSocket().asInstanceOf[SSLServerSocket]
    ss.setNeedClientAuth(true)
    ss.bind(new InetSocketAddress(port))
    ss

  private var registrations: Map[StoreId, Registration] = Map()

  def boundPort: Int = serverSocket.getLocalPort

  private val acceptThread = new Thread(() => acceptLoop(), s"store-transfer-accept-$boundPort")
  acceptThread.setDaemon(true)
  acceptThread.start()

  /** Tests only: closes the server socket and aborts live transfers */
  def close(): Unit =
    try serverSocket.close() catch case _: Throwable => ()
    val regs = synchronized:
      val live = registrations.toList.filter((_, reg) => !reg.done)
      live.foreach((_, reg) => reg.done = true)
      registrations = Map()
      live
    regs.foreach((storeId, reg) => abort(storeId, reg, "transfer manager closed"))

  def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path): Future[Unit] =
    val reg = new Registration(transferUUID, destinationDirectory, Promise())
    val previous = synchronized:
      val prev = registrations.get(storeId).filter(!_.done)
      prev.foreach(_.done = true)
      registrations -= storeId
      prev
    previous.foreach(p => abort(storeId, p, s"superseded by transfer $transferUUID"))
    synchronized:
      registrations += storeId -> reg
    logger.info(s"Awaiting transfer in of store $storeId, transfer $transferUUID")
    reg.promise.future

  def beginTransferOut(storeId: StoreId, transferUUID: UUID, sourceDirectory: os.Path,
                       destHost: String, destPort: Int, timestamp: HLCTimestamp): Unit = ???

  def isTransferringOut(storeId: StoreId): Boolean = ???

  def transferringOutStores: Set[StoreId] = ???

  private def acceptLoop(): Unit =
    try
      while true do
        val sock = serverSocket.accept().asInstanceOf[SSLSocket]
        val t = new Thread(() => handleIncoming(sock), s"store-transfer-in-${sock.getRemoteSocketAddress}")
        t.setDaemon(true)
        t.start()
    catch
      case _: SocketException if serverSocket.isClosed => ()
      case t: Throwable => logger.error(s"Store transfer accept loop on port $boundPort died: $t")

  private def closeQuietly(sock: SSLSocket): Unit =
    try sock.close() catch case _: Throwable => ()

  private def handleIncoming(sock: SSLSocket): Unit =
    val matched =
      try
        sock.setSoTimeout(ioTimeout.toMillis.toInt)
        val in = sock.getInputStream
        val header = in.readNBytes(HeaderSize)
        if header.length != HeaderSize then
          logger.warn(s"Store transfer connection from ${sock.getRemoteSocketAddress} closed before sending a header")
          None
        else
          val (storeId, transferUUID) = decodeHeader(header)
          val reg = synchronized:
            registrations.get(storeId) match
              case Some(r) if r.transferUUID == transferUUID && r.activeSocket.isEmpty && !r.done =>
                r.activeSocket = Some(sock)
                Some(r)
              case _ => None
          if reg.isEmpty then
            logger.warn(s"Rejecting store transfer connection from ${sock.getRemoteSocketAddress} for store " +
              s"$storeId, transfer $transferUUID: no matching pending transfer")
          reg.map(r => (storeId, r, in))
      catch
        case t: Throwable =>
          logger.warn(s"Store transfer connection from ${sock.getRemoteSocketAddress} failed before matching: $t")
          None

    matched match
      case None => closeQuietly(sock)
      case Some((storeId, reg, in)) =>
        try
          receive(storeId, reg, sock, in)
        catch
          case t: Throwable => fail(storeId, reg, t.toString)

  private def storeDirectory(storeId: StoreId, reg: Registration): os.Path =
    reg.destinationDirectory / storeId.directoryName

  private def receive(storeId: StoreId, reg: Registration, sock: SSLSocket, in: InputStream): Unit =
    val storeDir = storeDirectory(storeId, reg)
    os.remove.all(storeDir)
    os.makeDir.all(storeDir)

    val extractor = os.spawn(cmd = ("jar", "--extract"), cwd = storeDir, stderr = os.Pipe)
    val aborted = synchronized:
      if !reg.done then
        reg.extractor = Some(extractor)
      reg.done
    if aborted then
      // Aborted between matching and spawning; abort() could not see this extractor or directory
      extractor.destroy()
      os.remove.all(storeDir)
    else
      logger.info(s"Receiving store $storeId, transfer ${reg.transferUUID}, from ${sock.getRemoteSocketAddress}")

      // Everything but the final TrailerSize bytes goes to the extractor; those are held back
      // until EOF shows whether they are the trailer.
      val buf = new Array[Byte](chunkSize + TrailerSize)
      var held = 0
      var eof = false
      while !eof do
        val n = in.read(buf, held, buf.length - held)
        if n == -1 then
          eof = true
        else
          held += n
          if held > TrailerSize then
            extractor.stdin.write(buf, 0, held - TrailerSize)
            System.arraycopy(buf, held - TrailerSize, buf, 0, TrailerSize)
            held = TrailerSize

      if held != TrailerSize || !buf.take(TrailerSize).sameElements(storeId.toBytes) then
        fail(storeId, reg, "stream ended without a valid trailer")
      else
        extractor.stdin.close()
        extractor.waitFor()
        val exitCode = extractor.exitCode()
        if exitCode != 0 then
          fail(storeId, reg, s"extractor exited with code $exitCode: ${extractor.stderr.text().trim}")
        else
          if claim(storeId, reg) then
            closeQuietly(sock)
            logger.info(s"Received store $storeId, transfer ${reg.transferUUID}")
            reg.promise.success(())

  /** Marks `reg` settled and drops it from the registrations. False if it was already settled. */
  private def claim(storeId: StoreId, reg: Registration): Boolean = synchronized:
    if reg.done then
      false
    else
      reg.done = true
      if registrations.get(storeId).exists(_ eq reg) then
        registrations -= storeId
      true

  private def fail(storeId: StoreId, reg: Registration, cause: String): Unit =
    if claim(storeId, reg) then
      abort(storeId, reg, cause)

  /** Tears down a settled registration: closes its connection, destroys its extractor, removes the
   *  partially extracted directory and fails its future. */
  private def abort(storeId: StoreId, reg: Registration, cause: String): Unit =
    val (sock, extractor) = synchronized((reg.activeSocket, reg.extractor))
    sock.foreach(closeQuietly)
    extractor.foreach: p =>
      if p.isAlive() then
        p.destroy()
    if sock.isDefined then
      try
        os.remove.all(storeDirectory(storeId, reg))
      catch
        case t: Throwable => logger.error(s"Failed to remove partial store directory for $storeId: $t")
    val msg = s"Transfer in of store $storeId, transfer ${reg.transferUUID}, failed: $cause"
    logger.warn(msg)
    reg.promise.tryFailure(new Exception(msg))
