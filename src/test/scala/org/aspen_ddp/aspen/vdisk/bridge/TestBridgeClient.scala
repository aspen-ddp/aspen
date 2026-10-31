package org.aspen_ddp.aspen.vdisk.bridge

import java.io.IOException
import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future, Promise}

/** A blocking-socket bridge client with its own encoder and decoder, written from the wire table rather than from
  * `Protocol`, so the tests check the server against the specification. Requests are written by a single writer
  * thread, so `request` never blocks the caller and requests reach the socket in call order. A reader thread
  * completes a Promise per requestId and records the order in which replies arrived.
  */
final class TestBridgeClient(path: os.Path):

  import TestBridgeClient.*

  private val channel = SocketChannel.open(StandardProtocolFamily.UNIX)
  channel.connect(UnixDomainSocketAddress.of(path.toNIO))

  private val promises = new ConcurrentHashMap[Long, Promise[Response]]()
  private val ids = new AtomicLong(1)

  /** Request ids in the order their replies arrived */
  val arrival = new ConcurrentLinkedQueue[Long]()

  /** Completed when the server closes the connection (or this client does) */
  val eof: Promise[Unit] = Promise[Unit]()

  private val writer: ExecutorService = Executors.newSingleThreadExecutor: r =>
    val t = new Thread(r, "test-bridge-writer")
    t.setDaemon(true)
    t

  private val reader = new Thread(() => readLoop(), "test-bridge-reader")
  reader.setDaemon(true)
  reader.start()

  def nextId(): Long = ids.getAndIncrement()

  private def promiseFor(id: Long): Promise[Response] =
    val fresh = Promise[Response]()
    val existing = promises.putIfAbsent(id, fresh)
    val p = if existing == null then fresh else existing
    if eof.isCompleted then
      p.tryFailure(new IOException("connection closed"))
    p

  private def readFully(buf: ByteBuffer): Boolean =
    while buf.hasRemaining do
      if channel.read(buf) < 0 then
        return false
    true

  private def readLoop(): Unit =
    try
      var more = true
      while more do
        val header = ByteBuffer.allocate(24)
        if !readFully(header) then
          more = false
        else
          header.flip()
          val magic = header.getShort(0) & 0xFFFF
          val op = header.get(3) & 0xFF
          val status = header.getInt(4)
          val id = header.getLong(8)
          val len = header.getInt(16)
          require(magic == 0xA5D2, f"bad response magic $magic%04X")
          val payload = ByteBuffer.allocate(len)
          if !readFully(payload) then
            more = false
          else
            arrival.add(id)
            promiseFor(id).trySuccess(Response(op, status, id, payload.array()))
    catch
      case _: IOException => ()
    finally
      eof.trySuccess(())
      val failure = new IOException("connection closed")
      promises.values().forEach(p => p.tryFailure(failure))

  private def writeAll(bytes: Array[Byte]): Unit =
    val buf = ByteBuffer.wrap(bytes)
    while buf.hasRemaining do
      channel.write(buf)

  /** Queues raw bytes for the writer thread */
  def sendRaw(bytes: Array[Byte]): Future[Unit] =
    val p = Promise[Unit]()
    writer.execute: () =>
      try
        writeAll(bytes)
        p.success(())
      catch
        case t: Throwable => p.failure(t)
    p.future

  def request(op: Int, id: Long, flags: Int = 0, offset: Long = 0, length: Int = 0,
              payload: Array[Byte] = Array.empty): Future[Response] =
    val result = promiseFor(id)
    val b = ByteBuffer.allocate(32 + payload.length)
    b.putShort(0xA5D1.toShort).put(1.toByte).put(op.toByte).putInt(flags).putLong(id).putLong(offset)
      .putInt(length).putInt(payload.length).put(payload)
    sendRaw(b.array()).failed.foreach(t => result.tryFailure(t))(scala.concurrent.ExecutionContext.parasitic)
    result.future

  def call(op: Int, flags: Int = 0, offset: Long = 0, length: Int = 0,
           payload: Array[Byte] = Array.empty): Response =
    Await.result(request(op, nextId(), flags, offset, length, payload), 10.seconds)

  def open(name: String, size: Long, blockSize: Int): Future[Response] =
    request(Ops.Open, nextId(), offset = size, length = blockSize, payload = name.getBytes("UTF-8"))

  def write(offset: Long, data: Array[Byte], fua: Boolean = false): Future[Response] =
    request(Ops.Write, nextId(), flags = if fua then 1 else 0, offset = offset, length = data.length, payload = data)

  def read(offset: Long, length: Int): Future[Response] =
    request(Ops.Read, nextId(), offset = offset, length = length)

  /** Abrupt close, as if the process died */
  def close(): Unit =
    try channel.close()
    catch case _: IOException => ()
    writer.shutdownNow()

object TestBridgeClient:

  final case class Response(op: Int, status: Int, requestId: Long, payload: Array[Byte]):
    def message: String = new String(payload, "UTF-8")

  /** Numbers from the wire table */
  object Ops:
    val Open = 1
    val Read = 2
    val Write = 3
    val Flush = 4
    val Unmap = 5
    val Resize = 6
    val Lock = 7
    val Unlock = 8
    val LockState = 9

  object Statuses:
    val Ok = 0
    val InvalidRequest = 1
    val OutOfRange = 2
    val Fenced = 3
    val NotOwner = 4
    val DiskFailed = 5
    val NotFound = 6
    val SizeMismatch = 7
    val ProtocolError = 8
    val Internal = 9
