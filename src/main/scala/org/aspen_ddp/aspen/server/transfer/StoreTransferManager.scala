package org.aspen_ddp.aspen.server.transfer

import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.implementations.quic.TlsCredentials
import org.aspen_ddp.aspen.common.network.tls.TLSContext
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import scribe.Logging

import java.io.InputStream
import java.net.{InetSocketAddress, Socket, SocketException}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import java.util.zip.CRC32
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import javax.net.ssl.{SSLServerSocket, SSLSocket}
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.*

object StoreTransferManager:
  val DefaultChunkSize: Int = 1024 * 1024
  val DefaultIoTimeout: FiniteDuration = 90.seconds

  /** 17-byte encoded StoreId followed by the 16-byte transfer UUID (msb, lsb; big-endian) */
  val HeaderSize: Int = 33

  /** 17-byte encoded StoreId, the 8-byte archive length, then the 4-byte CRC32 of the archive
   *  (big-endian). Sent only after the whole archive was produced successfully. */
  val TrailerSize: Int = 29

  def encodeHeader(storeId: StoreId, transferUUID: UUID): Array[Byte] =
    val bb = ByteBuffer.allocate(HeaderSize)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(storeId.toBytes)
    bb.putLong(transferUUID.getMostSignificantBits)
    bb.putLong(transferUUID.getLeastSignificantBits)
    bb.array()

  def encodeTrailer(storeId: StoreId, archiveLength: Long, archiveCrc: Long): Array[Byte] =
    val bb = ByteBuffer.allocate(TrailerSize)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.put(storeId.toBytes)
    bb.putLong(archiveLength)
    bb.putInt(archiveCrc.toInt)
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

  /** One transfer-out attempt. `socket`, `process` and `abortCause` are guarded by the owning
   *  manager's lock; `lastProgress` (System.nanoTime) is read by the watchdog. */
  private class Outbound(val transferUUID: UUID, val timestamp: HLCTimestamp):
    @volatile var lastProgress: Long = System.nanoTime()
    var socket: Option[Socket] = None
    var process: Option[os.SubProcess] = None
    var abortCause: Option[String] = None

/** Sends and receives whole store directories as `jar` archives over one mutual-TLS TCP
 *  connection per transfer attempt.
 *
 *  Wire format: 33-byte header, the archive, then a 29-byte trailer of the StoreId, the archive
 *  length and the archive's CRC32. The trailer is written only after the sender's archiver exits
 *  successfully, so a stream that ends without it, or whose final 29 bytes do not match the
 *  StoreId and the archive bytes actually received, is incomplete or corrupt.
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

  /** Serializes creation and removal of store directories. Guards `directoryOwners`, which maps each
   *  store directory to the registration that last created it. A registration removes a directory
   *  only while it is still the owner, so a settled attempt can never delete the directory a newer
   *  attempt has set up at the same path. Never acquired while holding the manager lock. */
  private val directoryLock = new Object
  private var directoryOwners: Map[os.Path, Registration] = Map()

  /** The current transfer-out attempt per store */
  private var active: Map[StoreId, Outbound] = Map()

  /** Newest transfer-out timestamp seen per store. Kept after a successful attempt so that a
   *  retransmitted request cannot start a second send; forgotten after a failed one so that the
   *  same request can be retried. */
  private var lastTimestamp: Map[StoreId, HLCTimestamp] = Map()

  def boundPort: Int = serverSocket.getLocalPort

  /** Closes the connection of any outbound attempt that has made no write progress within ioTimeout */
  private val watchdog: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor: r =>
    val t = new Thread(r, s"store-transfer-watchdog-$boundPort")
    t.setDaemon(true)
    t
  private val watchdogPeriod = math.max(1L, ioTimeout.toMillis / 4)
  watchdog.scheduleAtFixedRate(() => checkProgress(), watchdogPeriod, watchdogPeriod, TimeUnit.MILLISECONDS)

  private val acceptThread = new Thread(() => acceptLoop(), s"store-transfer-accept-$boundPort")
  acceptThread.setDaemon(true)
  acceptThread.start()

  /** Tests only: closes the server socket and aborts live transfers */
  def close(): Unit =
    try serverSocket.close() catch case _: Throwable => ()
    watchdog.shutdownNow()
    val (regs, outs) = synchronized:
      val live = registrations.toList.filter((_, reg) => !reg.done)
      live.foreach((_, reg) => reg.done = true)
      registrations = Map()
      (live, active.values.toList)
    regs.foreach((storeId, reg) => abort(storeId, reg, "transfer manager closed"))
    outs.foreach(abortOutbound(_, "transfer manager closed"))

  def beginTransferIn(storeId: StoreId, transferUUID: UUID, destinationDirectory: os.Path): Future[Unit] =
    val reg = new Registration(transferUUID, destinationDirectory, Promise())
    val previous = synchronized:
      val prev = registrations.get(storeId).filter(!_.done)
      prev.foreach(_.done = true)
      registrations += storeId -> reg
      prev
    previous.foreach(p => abort(storeId, p, s"superseded by transfer $transferUUID"))
    logger.info(s"Awaiting transfer in of store $storeId, transfer $transferUUID")
    reg.promise.future

  def cancelTransferIn(storeId: StoreId, transferUUID: UUID): Unit =
    val reg = synchronized:
      registrations.get(storeId).filter(r => r.transferUUID == transferUUID && !r.done)
    reg.foreach(fail(storeId, _, "cancelled"))

  /** Starts sending `sourceDirectory` unless a request with the same or a newer timestamp has already
   *  been seen for this store. A newer request aborts the active attempt for the store. */
  def beginTransferOut(storeId: StoreId, transferUUID: UUID, sourceDirectory: os.Path,
                       destHost: String, destPort: Int, timestamp: HLCTimestamp): Unit =
    val out = new Outbound(transferUUID, timestamp)
    val (started, previous) = synchronized:
      if lastTimestamp.get(storeId).exists(_ >= timestamp) then
        (false, None)
      else
        lastTimestamp += storeId -> timestamp
        val prev = active.get(storeId)
        active += storeId -> out
        (true, prev)

    if !started then
      logger.debug(s"Ignoring transfer out of store $storeId, transfer $transferUUID: not newer than the last request")
    else
      previous.foreach(p => abortOutbound(p, s"superseded by transfer $transferUUID"))
      val t = new Thread(() => send(storeId, out, sourceDirectory, destHost, destPort), s"store-transfer-out-$storeId")
      t.setDaemon(true)
      t.start()

  def isTransferringOut(storeId: StoreId): Boolean = synchronized(active.contains(storeId))

  def transferringOutStores: Set[StoreId] = synchronized(active.keySet)

  private def send(storeId: StoreId, out: Outbound, sourceDirectory: os.Path, host: String, port: Int): Unit =
    val raw = new Socket()
    var succeeded = false
    try
      out.lastProgress = System.nanoTime()
      if !attach(out, raw) then
        throw new Exception("aborted before connecting")
      raw.connect(new InetSocketAddress(host, port), ioTimeout.toMillis.toInt)
      val sock = sslContext.getSocketFactory.createSocket(raw, host, port, true).asInstanceOf[SSLSocket]
      sock.startHandshake()
      val sockOut = sock.getOutputStream

      def write(arr: Array[Byte], len: Int): Unit =
        sockOut.write(arr, 0, len)
        out.lastProgress = System.nanoTime()

      write(encodeHeader(storeId, out.transferUUID), HeaderSize)

      // os.spawn leaks a throwing shutdown hook if the process cannot start, so check the cwd first
      if !os.isDir(sourceDirectory) then
        throw new Exception(s"source directory $sourceDirectory does not exist")
      val p = os.spawn(cmd = ("jar", "--create", "--no-manifest", "."), cwd = sourceDirectory, stderr = os.Pipe)
      if !attach(out, p) then
        throw new Exception("aborted before archiving")
      logger.info(s"Sending store $storeId, transfer ${out.transferUUID}, to $host:$port")

      val buf = new Array[Byte](chunkSize)
      val crc = new CRC32
      var length = 0L
      var n = p.stdout.read(buf)
      while n != -1 do
        if n > 0 then
          write(buf, n)
          crc.update(buf, 0, n)
          length += n
        n = p.stdout.read(buf)

      p.waitFor()
      val exitCode = p.exitCode()
      if exitCode != 0 then
        // No trailer, so the receiver rejects what it got
        logger.warn(s"Transfer out of store $storeId, transfer ${out.transferUUID}, failed: archiver exited " +
          s"with code $exitCode: ${p.stderr.text().trim}")
      else
        write(encodeTrailer(storeId, length, crc.getValue), TrailerSize)
        sockOut.flush()
        sock.close()
        succeeded = true
        logger.info(s"Sent store $storeId, transfer ${out.transferUUID}")
    catch
      case t: Throwable =>
        val cause = synchronized(out.abortCause).getOrElse(t.toString)
        logger.warn(s"Transfer out of store $storeId, transfer ${out.transferUUID}, failed: $cause")
    finally
      synchronized:
        out.process.foreach: p =>
          if p.isAlive() then
            p.destroy()
        if active.get(storeId).exists(_ eq out) then
          active -= storeId
        // A failed attempt forgets its timestamp so the receiver's retransmit of the same request
        // can retry it. A failure before the receiver accepted the connection never fails the
        // receiver, so it never sends a newer request, and without this every retransmit would
        // be ignored. Equality leaves a newer timestamp alone, which is the case for an attempt
        // aborted by supersession. A successful attempt keeps it, so a late retransmit cannot
        // start a second send.
        if !succeeded && lastTimestamp.get(storeId).contains(out.timestamp) then
          lastTimestamp -= storeId
      try raw.close() catch case _: Throwable => ()

  /** Records the attempt's socket so it can be aborted. False if the attempt was already aborted. */
  private def attach(out: Outbound, sock: Socket): Boolean = synchronized:
    if out.abortCause.isEmpty then
      out.socket = Some(sock)
    out.abortCause.isEmpty

  /** Records the attempt's archiver so it can be aborted. False if the attempt was already aborted. */
  private def attach(out: Outbound, p: os.SubProcess): Boolean = synchronized:
    out.process = Some(p)
    out.abortCause.isEmpty

  /** Unblocks the attempt's thread by closing its connection and destroying its archiver. The
   *  thread then logs the failure and drops its `active` entry. */
  private def abortOutbound(out: Outbound, cause: String): Unit =
    val (sock, process) = synchronized:
      if out.abortCause.isEmpty then
        out.abortCause = Some(cause)
      (out.socket, out.process)
    sock.foreach(s => try s.close() catch case _: Throwable => ())
    process.foreach: p =>
      if p.isAlive() then
        p.destroy()

  /** Never throws: a throw out of a scheduleAtFixedRate task silently cancels every later run, which
   *  would leave outbound attempts with no send timeout at all. */
  private def checkProgress(): Unit =
    try
      val now = System.nanoTime()
      val stalled = synchronized(active.values.filter(o => now - o.lastProgress > ioTimeout.toNanos).toList)
      stalled.foreach(abortOutbound(_, s"no write progress for $ioTimeout"))
    catch
      case t: Throwable => logger.error(s"Store transfer watchdog on port $boundPort failed a progress check: $t")

  /** Runs until the server socket is closed. Any other failure, such as a transient EMFILE from
   *  accept(), is logged and the loop carries on after a short pause: the socket stays bound, so
   *  exiting would leave senders connecting to a port nobody serves. */
  private def acceptLoop(): Unit =
    while !serverSocket.isClosed do
      try
        val sock = serverSocket.accept().asInstanceOf[SSLSocket]
        try
          val t = new Thread(() => handleIncoming(sock), s"store-transfer-in-${sock.getRemoteSocketAddress}")
          t.setDaemon(true)
          t.start()
        catch
          case t: Throwable =>
            closeQuietly(sock)
            throw t
      catch
        case _: SocketException if serverSocket.isClosed => ()
        case t: Throwable =>
          logger.error(s"Store transfer accept loop on port $boundPort failed; continuing: $t")
          try Thread.sleep(100) catch case _: InterruptedException => ()

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
    val (extractor, aborted) = directoryLock.synchronized:
      os.remove.all(storeDir)
      os.makeDir.all(storeDir)
      directoryOwners += storeDir -> reg
      val p = os.spawn(cmd = ("jar", "--extract"), cwd = storeDir, stderr = os.Pipe)
      val aborted = synchronized:
        if !reg.done then
          reg.extractor = Some(p)
        reg.done
      (p, aborted)
    if aborted then
      // Aborted between matching and spawning; abort() could not see this extractor or directory
      extractor.destroy()
      removeOwnedDirectory(storeId, reg)
    else
      logger.info(s"Receiving store $storeId, transfer ${reg.transferUUID}, from ${sock.getRemoteSocketAddress}")

      // Everything but the final TrailerSize bytes goes to the extractor; those are held back
      // until EOF shows whether they are the trailer.
      val buf = new Array[Byte](chunkSize + TrailerSize)
      val crc = new CRC32
      var length = 0L
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
            crc.update(buf, 0, held - TrailerSize)
            length += held - TrailerSize
            System.arraycopy(buf, held - TrailerSize, buf, 0, TrailerSize)
            held = TrailerSize

      if held != TrailerSize || !buf.take(TrailerSize).sameElements(encodeTrailer(storeId, length, crc.getValue)) then
        fail(storeId, reg, "stream ended without a valid trailer")
      else
        extractor.stdin.close()
        extractor.waitFor()
        val exitCode = extractor.exitCode()
        if exitCode != 0 then
          fail(storeId, reg, s"extractor exited with code $exitCode: ${extractor.stderr.text().trim}")
        else
          if claim(storeId, reg) then
            directoryLock.synchronized:
              if directoryOwners.get(storeDir).exists(_ eq reg) then
                directoryOwners -= storeDir
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

  /** Removes the store directory `reg` created, unless a newer attempt has since recreated it */
  private def removeOwnedDirectory(storeId: StoreId, reg: Registration): Unit =
    val storeDir = storeDirectory(storeId, reg)
    directoryLock.synchronized:
      if directoryOwners.get(storeDir).exists(_ eq reg) then
        directoryOwners -= storeDir
        try
          os.remove.all(storeDir)
        catch
          case t: Throwable => logger.error(s"Failed to remove partial store directory for $storeId: $t")

  /** Tears down a settled registration: closes its connection, destroys its extractor, removes the
   *  partially extracted directory and fails its future. */
  private def abort(storeId: StoreId, reg: Registration, cause: String): Unit =
    val (sock, extractor) = synchronized((reg.activeSocket, reg.extractor))
    sock.foreach(closeQuietly)
    extractor.foreach: p =>
      if p.isAlive() then
        p.destroy()
    removeOwnedDirectory(storeId, reg)
    val msg = s"Transfer in of store $storeId, transfer ${reg.transferUUID}, failed: $cause"
    logger.warn(msg)
    reg.promise.tryFailure(new Exception(msg))
