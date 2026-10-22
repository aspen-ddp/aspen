package org.aspen_ddp.aspen.common.network.implementations.quic

import io.netty.channel.{ChannelFuture, ChannelFutureListener, ChannelHandler, ChannelHandlerContext,
  ChannelInboundHandlerAdapter, ChannelOption, EventLoop, WriteBufferWaterMark}
import io.netty.handler.codec.quic.{QuicChannel, QuicDatagramExtensionEvent, QuicStreamChannel, QuicStreamType}
import io.netty.util.concurrent.{Future as NFuture, Promise}
import org.aspen_ddp.aspen.common.network.implementations.quic.QuicMessageCodec.Encoded
import scribe.Logging

import java.util.concurrent.{ConcurrentLinkedQueue, RejectedExecutionException, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/** The send side of one QUIC connection: a lock-free handoff from caller threads to the connection's event loop,
  * a bounded pre-handshake queue, datagram writes, and a round-robin pool of unidirectional streams.
  *
  * Nothing is ever queued to avoid a drop. A message is dropped (released, counted in `drops`, logged at debug
  * with rate limiting) when the handoff or pre-handshake budget is exceeded, when no stream is writable, when the
  * peer cannot take the datagram, when its write fails, or when the connection closes before it is written.
  *
  * All methods except those marked "any thread" must run on `loop`. `loop` must be the event loop of the
  * `QuicChannel` later passed to `attach`.
  *
  * Datagram support is learned from Netty's `QuicDatagramExtensionEvent`, which `pipelineHandler` records. On an
  * outbound connection `attach`, called from the connect future, installs it in time: Netty completes the connect
  * promise and then fires the event in the same call. On an inbound connection the event fires before
  * `channelActive`, so the owner must add `pipelineHandler` to the `QuicChannel` pipeline at channel init.
  *
  * Liveness hooks (all on `loop`): `onActive` runs once the connection is attached and the pre-handshake queue
  * has been written, which is where the owner sends its initial heartbeat and starts keepalive (`startKeepalive`).
  * The keepalive timer is cancelled when the connection closes.
  */
final class QuicConnection(val loop: EventLoop,
                           config: QuicConfig,
                           onClosed: QuicConnection => Unit,
                           onActive: QuicConnection => Unit = _ => ()) extends Logging:
  import QuicConnection.*

  private val budget = config.connectionBudgetBytes
  private val nStreams = config.streamsPerConnection
  private val streamHighWater = math.max(1L, budget / nStreams).min(Int.MaxValue.toLong).toInt
  private val streamWaterMark = new WriteBufferWaterMark(streamHighWater / 2, streamHighWater)

  // ---- Shared between threads ----------------------------------------------------------------------------------
  private val handoff = new ConcurrentLinkedQueue[AnyRef]()
  private val queuedBytes = new AtomicLong(0)
  private val drainScheduled = new AtomicBoolean(false)
  private val dropCount = new AtomicLong(0)
  private val dropsSinceLog = new AtomicLong(0)
  private val lastDropLog = new AtomicLong(System.nanoTime() - DropLogIntervalNanos)
  private val drainCount = new AtomicLong(0)
  @volatile private var closed = false
  @volatile private var drainedCount = 0L
  @volatile private var pendingTotal = 0L
  @volatile private var maxPending = 0L

  // ---- Loop-owned ----------------------------------------------------------------------------------------------
  @volatile private var channel: QuicChannel = null // written on the loop; read off-loop only for drop logging
  private var active = false // attached and the pre-handshake queue has been written
  @volatile private var activated = false // set after onActive has returned; read by isActive
  private val preHandshake = new java.util.ArrayDeque[AnyRef]()
  private var preHandshakeBytes = 0L
  private var maxDatagramLength = -1 // -1: the peer has not announced datagram support
  private val slots = Array.fill(nStreams)(new Slot)
  private var cursor = 0
  private val inFlight = new java.util.HashSet[ChannelFuture]()
  private val waitingBarriers = new java.util.HashSet[Promise[Void]]()
  private var datagramsTouched = false
  private val touchedStreams = new java.util.ArrayList[QuicStreamChannel](nStreams)
  private var lastWriteNanos = System.nanoTime() // last outbound datagram or stream write, keepalives included
  private var keepaliveNanos = 0L // 0: keepalive not started
  private var keepaliveTask: NFuture[?] = null

  private val drainTask: Runnable = () => drain()
  private val activateTask: Runnable = () => activate()

  /** Records the peer's datagram support. See the class doc for where it must be installed. */
  val pipelineHandler: ChannelHandler = new ChannelInboundHandlerAdapter:
    override def userEventTriggered(ctx: ChannelHandlerContext, evt: Any): Unit =
      evt match
        case e: QuicDatagramExtensionEvent => maxDatagramLength = e.maxLength()
        case _ => ()
      ctx.fireUserEventTriggered(evt)

  // ==============================================================================================================
  // Any thread
  // ==============================================================================================================

  /** Hands `enc` to the loop. Any thread. Returns false if it was dropped (and released): over budget or closed. */
  def submit(enc: Encoded): Boolean =
    if closed then
      drop(enc, "connection closed")
      false
    else if !reserve(enc.size) then
      drop(enc, "handoff budget exceeded")
      false
    else
      handoff.add(enc)
      scheduleDrain()
      if closed then
        // Raced with close, which may already have emptied the queue. Whoever polls an item releases it.
        releaseHandoff()
        false
      else
        true

  /** Any thread. Completes when every write submitted before it has completed or been dropped. */
  def postBarrier(): NFuture[Void] =
    val p = loop.newPromise[Void]()
    if closed then
      p.trySuccess(null)
    else
      handoff.add(new Barrier(p))
      scheduleDrain()
      if closed then
        releaseHandoff()
    p

  /** Any thread */
  def isClosed: Boolean = closed

  /** Any thread. True once `onActive` has returned; stays true after close. */
  def isActive: Boolean = activated

  /** Any thread. Messages dropped so far. */
  def drops: Long = dropCount.get()

  /** Times the drain task has run */
  private[quic] def drainExecutions: Long = drainCount.get()

  /** Messages and barriers taken off the handoff queue by the loop */
  private[quic] def drainedItems: Long = drainedCount

  /** Bytes written to streams whose write futures have not completed yet */
  private[quic] def pendingStreamBytes: Long = pendingTotal

  /** The most bytes ever pending on a single stream, sampled after each stream write */
  private[quic] def maxPendingStreamBytes: Long = maxPending

  private def reserve(size: Long): Boolean =
    var done = false
    var ok = false
    while !done do
      val cur = queuedBytes.get()
      if cur != 0 && cur + size > budget then
        done = true
      else if queuedBytes.compareAndSet(cur, cur + size) then
        ok = true
        done = true
    ok

  private def scheduleDrain(): Unit =
    if drainScheduled.compareAndSet(false, true) then
      try
        loop.execute(drainTask)
      catch
        case _: RejectedExecutionException =>
          // The loop is gone, so nothing will ever drain. Behave as closed; onClosed cannot run without the loop.
          drainScheduled.set(false)
          closed = true
          releaseHandoff()

  /** Releases everything in the handoff queue. Any thread, only once `closed` is set. */
  private def releaseHandoff(): Unit =
    var item = handoff.poll()
    while item != null do
      item match
        case enc: Encoded =>
          queuedBytes.addAndGet(-enc.size)
          drop(enc, "connection closed")
        case b: Barrier => b.promise.trySuccess(null)
      item = handoff.poll()

  /** Any thread */
  private def drop(enc: Encoded, reason: String): Unit =
    enc.release()
    countDrop(reason)

  /** Any thread */
  private def countDrop(reason: String): Unit =
    dropCount.incrementAndGet()
    dropsSinceLog.incrementAndGet()
    val now = System.nanoTime()
    val last = lastDropLog.get()
    if now - last >= DropLogIntervalNanos && lastDropLog.compareAndSet(last, now) then
      val n = dropsSinceLog.getAndSet(0)
      logger.debug(s"QUIC connection ${remoteName}: dropped $n message(s) since last report (latest: $reason)")

  private def remoteName: String =
    val ch = channel
    if ch == null then "(unattached)" else String.valueOf(ch.remoteSocketAddress())

  // ==============================================================================================================
  // Loop
  // ==============================================================================================================

  /** The handshake succeeded. Writes the pre-handshake queue in a following loop task, after Netty has fired the
    * datagram extension event. If this connection is already closed the channel is closed.
    */
  def attach(ch: QuicChannel): Unit =
    assert(loop.inEventLoop(), "attach must run on the connection's loop")
    if closed then
      ch.close()
    else
      channel = ch
      if ch.pipeline().context(pipelineHandler) == null then
        ch.pipeline().addLast(pipelineHandler)
      ch.closeFuture().addListener((_: ChannelFuture) => close())
      loop.execute(activateTask)

  def handshakeFailed(cause: Throwable): Unit =
    assert(loop.inEventLoop(), "handshakeFailed must run on the connection's loop")
    if !closed then
      logger.debug(s"QUIC handshake failed: $cause")
      close()

  /** Drops everything not yet written, completes all barriers, closes the channel and calls `onClosed` once */
  def close(): Unit =
    assert(loop.inEventLoop(), "close must run on the connection's loop")
    if !closed then
      closed = true
      releaseHandoff()
      var item = preHandshake.poll()
      while item != null do
        item match
          case enc: Encoded => drop(enc, "connection closed")
          case b: Barrier => b.promise.trySuccess(null)
        item = preHandshake.poll()
      preHandshakeBytes = 0
      waitingBarriers.forEach(_.trySuccess(null))
      waitingBarriers.clear()
      if keepaliveTask != null then
        keepaliveTask.cancel(false)
        keepaliveTask = null
      inFlight.clear() // Netty fails and releases these as the channel closes
      if channel != null then
        channel.close()
      try
        onClosed(this)
      catch
        case NonFatal(e) => logger.warn(s"QUIC onClosed callback failed: $e")

  private def activate(): Unit =
    if !closed && !active then
      active = true
      var item = preHandshake.poll()
      while item != null do
        process(item)
        item = preHandshake.poll()
      preHandshakeBytes = 0
      flushTouched()
      try
        onActive(this)
      catch
        case NonFatal(e) => logger.warn(s"QUIC onActive callback failed: $e")
      activated = true

  /** Sends a keepalive whenever nothing has been written for `idle`, until the connection closes.
    *
    * Netty QUIC 4.2.19 exposes no way to send a QUIC PING frame (`QuicChannel` has no ping or keep-alive method),
    * so the keepalive is a 1-byte datagram holding `KeepaliveByte` (0). DATAGRAM frames are ack-eliciting, so it
    * resets both sides' idle timers. A real message datagram is at least 4 bytes (its u32 prefixLen), so a single
    * 0 byte is never a message (read as a prefixLen it is invalid); the receiver checks `isKeepalive` and discards
    * it, silently, before decode. It is written directly rather than through `submit`: it is loop-side, carries no
    * payload, and must not count against or be dropped by the message budget.
    */
  def startKeepalive(idle: Duration): Unit =
    assert(loop.inEventLoop(), "startKeepalive must run on the connection's loop")
    if !closed && keepaliveNanos == 0 then
      keepaliveNanos = math.max(1L, idle.toNanos)
      scheduleKeepalive()

  private def scheduleKeepalive(): Unit =
    val delay = math.max(0L, lastWriteNanos + keepaliveNanos - System.nanoTime())
    try keepaliveTask = loop.schedule((() => keepaliveCheck()): Runnable, delay, TimeUnit.NANOSECONDS)
    catch case _: RejectedExecutionException => () // the loop is shutting down

  private def keepaliveCheck(): Unit =
    if !closed then
      if System.nanoTime() - lastWriteNanos >= keepaliveNanos then
        sendKeepalive()
      scheduleKeepalive()

  private def sendKeepalive(): Unit =
    val ch = channel
    if maxDatagramLength >= 1 && ch != null then
      val buf = ch.alloc().buffer(1).writeByte(KeepaliveByte)
      ch.writeAndFlush(buf) // Netty releases buf if the write fails
    // Counted even when not sent, so a peer without datagram support is not retried in a tight loop
    lastWriteNanos = System.nanoTime()

  private def drain(): Unit =
    drainCount.incrementAndGet()
    // Cleared before polling, so an item added after this point schedules another drain
    drainScheduled.set(false)
    if closed then
      releaseHandoff()
    else
      var n = 0L
      var item = handoff.poll()
      while item != null do
        n += 1
        item match
          case enc: Encoded => queuedBytes.addAndGet(-enc.size)
          case _ => ()
        if active then
          process(item)
        else
          holdUntilAttached(item)
        item = handoff.poll()
      drainedCount += n
      flushTouched()

  private def holdUntilAttached(item: AnyRef): Unit = item match
    case enc: Encoded =>
      if preHandshakeBytes != 0 && preHandshakeBytes + enc.size > budget then
        drop(enc, "pre-handshake budget exceeded")
      else
        preHandshakeBytes += enc.size
        preHandshake.add(enc)
    case b: Barrier => preHandshake.add(b)

  private def process(item: AnyRef): Unit = item match
    case b: Barrier => startBarrier(b.promise)
    case enc: Encoded =>
      try
        if enc.isDatagram then
          sendDatagram(enc)
        else
          sendStream(enc)
      catch
        case NonFatal(e) =>
          // The conversions release what they own on failure; release() is a no-op once consumed
          drop(enc, s"write failed: $e")

  private def sendDatagram(enc: Encoded): Unit =
    if maxDatagramLength < 0 then
      drop(enc, "peer does not support datagrams")
    else if enc.size > maxDatagramLength then
      drop(enc, s"datagram of ${enc.size} bytes exceeds peer maximum $maxDatagramLength")
    else
      val ch = channel
      val f = ch.write(enc.toDatagram(ch.alloc()))
      lastWriteNanos = System.nanoTime()
      datagramsTouched = true
      track(f, null, 0)

  private def sendStream(enc: Encoded): Unit =
    val slot = pickStream()
    if slot == null then
      drop(enc, "no writable stream")
    else
      val s = slot.stream
      val size = enc.size
      val f = s.write(enc.toStreamFrame(s.alloc()))
      lastWriteNanos = System.nanoTime()
      if !touchedStreams.contains(s) then
        touchedStreams.add(s)
      track(f, slot, size)
      if slot.pending > maxPending then
        maxPending = slot.pending

  /** Round-robin from `cursor`, skipping slots that are opening, closed or not writable.
    *
    * Writable means fewer than `streamHighWater` bytes are pending on the stream. Netty QUIC stream channels do not
    * use `WRITE_BUFFER_WATER_MARK`: their `isWritable` is quiche's stream capacity, which the congestion window
    * bounds, so it turns false after a few packets' worth of data on a fresh connection. The high-water mark is
    * therefore enforced here, from the bytes whose write futures have not completed. The check happens before the
    * write, so one message may overshoot it.
    */
  private def pickStream(): Slot =
    var i = 0
    var found: Slot = null
    while found == null && i < nStreams do
      val idx = (cursor + i) % nStreams
      val s = usable(idx)
      if s != null && s.pending < streamHighWater then
        found = s
        cursor = (idx + 1) % nStreams
      i += 1
    found

  /** The slot at `idx` if its stream is open, opening a new stream for an empty or closed slot */
  private def usable(idx: Int): Slot =
    var slot = slots(idx)
    if slot.stream != null && !slot.stream.isOpen then
      slot = new Slot // closed or reset: replace lazily
      slots(idx) = slot
    if slot.stream == null then
      if slot.opening == null then
        slot.opening = channel.createStream(QuicStreamType.UNIDIRECTIONAL, null)
      val f = slot.opening
      // Netty normally completes the open synchronously when called on the loop
      if f.isDone then
        slot.opening = null
        if f.isSuccess then
          val s = f.getNow
          // Set as specified, though Netty QUIC streams ignore it; see pickStream
          s.config().setOption(ChannelOption.WRITE_BUFFER_WATER_MARK, streamWaterMark)
          slot.stream = s
    if slot.stream != null then slot else null

  private def track(f: ChannelFuture, slot: Slot, size: Long): Unit =
    if slot != null then
      slot.pending += size
      pendingTotal += size
    if f.isDone then
      writeDone(f, slot, size)
    else
      inFlight.add(f)
      f.addListener: (_: ChannelFuture) =>
        inFlight.remove(f)
        writeDone(f, slot, size)

  private def writeDone(f: ChannelFuture, slot: Slot, size: Long): Unit =
    if slot != null then
      slot.pending -= size
      pendingTotal -= size
    if !f.isSuccess then
      // Netty releases the message of a failed write
      countDrop(s"write failed: ${f.cause()}")

  private def startBarrier(p: Promise[Void]): Unit =
    if inFlight.isEmpty then
      p.trySuccess(null)
    else
      val snapshot = inFlight.toArray(new Array[ChannelFuture](0))
      waitingBarriers.add(p)
      var remaining = snapshot.length
      val listener: ChannelFutureListener = _ =>
        remaining -= 1
        if remaining == 0 then
          waitingBarriers.remove(p)
          p.trySuccess(null)
      snapshot.foreach(_.addListener(listener))

  private def flushTouched(): Unit =
    if datagramsTouched then
      datagramsTouched = false
      channel.flush()
    if !touchedStreams.isEmpty then
      touchedStreams.forEach(_.flush())
      touchedStreams.clear()

object QuicConnection:
  private val DropLogIntervalNanos: Long = TimeUnit.SECONDS.toNanos(1)

  /** The single byte of a keepalive datagram. See `startKeepalive`. */
  val KeepaliveByte: Int = 0

  /** Whether a received datagram is a keepalive. Does not consume or release `buf`. */
  def isKeepalive(buf: io.netty.buffer.ByteBuf): Boolean =
    buf.readableBytes() == 1 && buf.getByte(buf.readerIndex()) == KeepaliveByte

  private final class Barrier(val promise: Promise[Void])

  /** One stream of the pool. Loop-owned. */
  private final class Slot:
    var opening: NFuture[QuicStreamChannel] = null
    var stream: QuicStreamChannel = null
    var pending: Long = 0
