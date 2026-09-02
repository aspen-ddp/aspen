package org.aspen_ddp.aspen.common

import java.io.{File, IOException, PrintWriter, StringWriter}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.*
import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReferenceArray}
import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

package object util {
  import scala.language.implicitConversions
  import scala.Conversion

  given Conversion[UUID, Array[Byte]] = uuid => {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(0, uuid.getMostSignificantBits)
    bb.putLong(8, uuid.getLeastSignificantBits)
    bb.array()
  }

  def uuid2byte(uuid: UUID): Array[Byte] = {
    val bb = ByteBuffer.allocate(16)
    bb.putLong(0, uuid.getMostSignificantBits)
    bb.putLong(8, uuid.getLeastSignificantBits)
    bb.array()
  }

  def byte2uuid(arr: Array[Byte]): UUID = {
    val bb = ByteBuffer.wrap(arr)
    val msb = bb.getLong()
    val lsb = bb.getLong()
    new UUID(msb, lsb)
  }

  def uuids2byte(uuids: List[UUID]): Array[Byte] = {
    val bb = ByteBuffer.allocate(16 * uuids.length)
    uuids.foreach: uuid =>
      bb.putLong(uuid.getMostSignificantBits)
      bb.putLong(uuid.getLeastSignificantBits)
    bb.array()
  }

  def byte2uuids(arr: Array[Byte]): List[UUID] = {
    require(arr.length % 16 == 0)
    
    val bb = ByteBuffer.wrap(arr)

    @tailrec
    def recurse(count: Int, uuids: List[UUID]): List[UUID] =
      if count * 16 == arr.length then
        uuids.reverse
      else
        val msb = bb.getLong()
        val lsb = bb.getLong()
        recurse(count + 1, new UUID(msb, lsb) :: uuids)

    recurse(0, Nil)
  }

  def int2byte(i: Int): Array[Byte] = {
    val arr = new Array[Byte](4)
    val bb = ByteBuffer.wrap(arr)
    bb.putInt(i)
    arr
  }
  def byte2int(arr: Array[Byte]): Int = ByteBuffer.wrap(arr).getInt()

  def long2byte(l: Long): Array[Byte] = {
    val arr = new Array[Byte](8)
    val bb = ByteBuffer.wrap(arr)
    bb.putLong(l)
    arr
  }
  def byte2long(arr: Array[Byte]): Long = ByteBuffer.wrap(arr).getLong()

  def getStack: String = {
    val e = new Exception("printing stack")
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    e.printStackTrace(pw)
    sw.toString
  }

  def printStack(): Unit = println(getStack)

  def db2string(db: DataBuffer): String = {
    val enc = java.util.Base64.getEncoder
    enc.encodeToString(db.getByteArray)
  }
  def printableArray(arr: Array[Byte]): String = {
    val enc = java.util.Base64.getEncoder
    enc.encodeToString(arr)
  }

  def deleteDirectory(directoryToBeDeleted: File): Boolean = {
    val allContents = directoryToBeDeleted.listFiles
    if allContents != null then
      for (file <- allContents)
        deleteDirectory(file)
    directoryToBeDeleted.delete
  }
  
  def someOrThrow[U, T <: Throwable](o: Future[Option[U]], exceptionToThrow: => T)(using ec: ExecutionContext): Future[U] = o.map:
    case None => throw exceptionToThrow
    case Some(u) => u

  def atomicWrite(targetFile: Path, content: String): Unit =
    // 1. Get the parent directory.
    // CRITICAL: The temp file MUST be on the same partition/volume as the target file,
    // otherwise a native atomic rename is physically impossible.
    val parentDir = Option(targetFile.getParent).getOrElse(Paths.get("."))

    // 2. Create a temporary file in that same directory
    val tempFile = Files.createTempFile(parentDir, "atomic-", ".tmp")

    try
      // 3. Write content to the temporary file
      Files.writeString(tempFile, content, StandardCharsets.UTF_8)

      // 4. Atomically replace the target file with the temp file
      Files.move(
        tempFile,
        targetFile,
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE
      )
    catch
      case ex: IOException =>
        // 5. Clean up the temporary file if anything failed prior to the atomic move
        try
          Files.deleteIfExists(tempFile)
        catch
          case cleanupEx: IOException => ex.addSuppressed(cleanupEx)
        throw ex

  def runSequentially[A, B](items: Seq[A])(f: A => Future[B])(implicit ec: ExecutionContext): Future[Seq[B]] =
    items.foldLeft(Future.successful(Vector.empty[B])): (accFuture, item) =>
      accFuture.flatMap: accResults =>
        // This body only executes after the previous Future finishes
        f(item).map(result => accResults :+ result)

  /**
   * Executes the future-returning function `f` over `items` with at most `maxConcurrent`
   * invocations in flight at any instant. Results are returned in the same order as `items`
   * (matching Future.sequence semantics), regardless of completion order.
   *
   * Fails fast: on the first failure the returned Future fails immediately with that error. The
   * remaining workers are not cancelled, however (Scala Futures are not cancellable), so they may
   * continue to pull from the shared index and start and run further invocations of `f`.
   *
   * A fixed pool of min(maxConcurrent, n) worker chains pull from a shared atomic index, giving a
   * true sliding window (a new invocation starts the instant one finishes) rather than a
   * chunk-barrier where the slowest item in a batch stalls the next batch.
   */
  def runBoundedParallel[A, B](items: Seq[A], maxConcurrent: Int)
                              (f: A => Future[B])
                              (implicit ec: ExecutionContext): Future[Seq[B]] =
    require(maxConcurrent > 0, s"maxConcurrent must be > 0, got $maxConcurrent")
    val arr = items.toIndexedSeq
    if arr.isEmpty then
      Future.successful(Vector.empty)
    else
      val results   = new AtomicReferenceArray[B](arr.length)
      val nextIndex = new AtomicInteger(0)

      def worker(): Future[Unit] =
        val i = nextIndex.getAndIncrement()
        if i >= arr.length then
          Future.unit
        else
          f(arr(i)).flatMap: b =>
            results.set(i, b)
            worker()

      val workers = (0 until math.min(maxConcurrent, arr.length)).map(_ => worker())
      Future.sequence(workers).map(_ => Vector.tabulate(arr.length)(results.get))

  /** Default threshold past which an outstanding invocation is treated as a stall worth reporting.
   *
   *  Deliberately far longer than any polling period in the system -- the longest is the hourly
   *  storage device check -- so that an alarm means something is wrong rather than something is
   *  slow.
   */
  val DefaultStallAfter: FiniteDuration = Duration(15, MINUTES)

  /** Reported when a tick arrives while an earlier invocation has been outstanding longer than
   *  the stall threshold.
   *
   *  @param name             identifies the polling task
   *  @param outstandingFor   how long the OLDEST outstanding invocation has been running
   *  @param inFlight         invocations outstanding when the tick arrived
   *  @param suppressedTicks  ticks dropped since the guard was last completely idle
   *  @param startedExtra     true if this tick was allowed to start an additional invocation
   *                          anyway, false if it was dropped
   */
  case class SingleFlightStall(name: String,
                               outstandingFor: FiniteDuration,
                               inFlight: Int,
                               suppressedTicks: Int,
                               startedExtra: Boolean)

  def logSingleFlightStall(s: SingleFlightStall): Unit =
    val action =
      if s.startedExtra then
        s"starting an additional invocation (${s.inFlight + 1} now in flight)"
      else
        s"tick dropped; ${s.suppressedTicks} dropped since the last idle period"
    scribe.error(
      s"POLLING TASK STALLED: ${s.name} has had an invocation outstanding for " +
      s"${s.outstandingFor.toSeconds}s. Either the operation it polls is genuinely offline, or " +
      s"its Future was orphaned and will never complete. $action")

  /** Runs `fn` on demand, dropping calls that arrive while an earlier one is still outstanding.
   *
   *  Intended for `schedulePeriodic`, so that polling reads do not pile up while the thing being
   *  polled is offline. Reads retry indefinitely rather than timing out, so without this a
   *  20-second poll across a day-long outage would issue thousands of redundant reads.
   *
   *  Total by construction. A tick releases its slot however it ends -- value, failed Future, or
   *  synchronous throw -- because only an invocation that is genuinely still running has any
   *  claim to suppress the next one. The predecessor released the slot only from `fn.foreach`,
   *  so a throw or a failure left the guard permanently closed over nothing, and callers had to
   *  carry a hand-written "must never throw nor fail" contract to stay safe. They no longer do.
   *
   *  The mode that remains, and cannot be designed away here: a Future that never completes.
   *  That is indistinguishable from a slow one, and suppressing ticks is the CORRECT response
   *  when a read is genuinely outstanding against an offline store. So this does not guess -- it
   *  reports. Past `stallAfter`, every suppressed tick raises `onStall`, which turns an
   *  invisible permanent wedge into a diagnosable one. Raising `maxInFlight` above 1 additionally
   *  lets a stalled task make progress; see that parameter.
   *
   *  @param name         identifies the task in stall reports
   *  @param stallAfter   how long an invocation may run before suppressed ticks raise `onStall`
   *  @param maxInFlight  hard cap on concurrent invocations, and the ONLY thing bounding them.
   *                      At the default of 1 this is a strict single-flight guard: a stalled
   *                      invocation is reported but never joined, exactly as before. Above 1, a
   *                      tick may start an additional invocation once `stallAfter` has passed
   *                      since the most recent one started, up to the cap -- trading a bounded,
   *                      constant number of duplicate reads for liveness against an orphaned
   *                      Future. Prefer this to a timeout that re-arms the guard: a timeout's
   *                      worst case grows with the outage (a 20 second poll under a 5 minute
   *                      timeout issues 288 reads across a day offline), while this one is
   *                      `maxInFlight` reads no matter how long the outage lasts. Only raise it
   *                      for work that is safe to duplicate, since an abandoned invocation is not
   *                      cancelled and may still complete later.
   *  @param clock        nanosecond time source; injectable for testing
   *  @param onStall      stall handler; defaults to logging at ERROR
   */
  def boundedSingleFlight[T](name: String,
                             stallAfter: FiniteDuration = DefaultStallAfter,
                             maxInFlight: Int = 1,
                             clock: () => Long = () => System.nanoTime(),
                             onStall: SingleFlightStall => Unit = logSingleFlightStall)
                            (fn: => Future[T])
                            (using ec: ExecutionContext): () => Unit =
    require(maxInFlight >= 1, s"maxInFlight must be at least 1, got $maxInFlight")

    val nextId = new AtomicLong(0)

    object tracker:
      // (id, start time), most recently started first, so the oldest is last. Identified by id
      // rather than by start time because two invocations can share a timestamp.
      private var inFlight: List[(Long, Long)] = Nil
      private var suppressed = 0

      /** Reserve a slot. Caller must hold the monitor. */
      private def reserve(now: Long): Long =
        val id = nextId.getAndIncrement()
        inFlight = (id, now) :: inFlight
        id

      def call(): Unit =
        val now = clock()

        // Decide under the lock, act outside it: neither `fn` nor `onStall` is ours to run while
        // holding a monitor. Evaluating `fn` under the lock is what made the predecessor's
        // synchronous-throw mode a deadlock risk as well as a wedge.
        val (toStart, toReport) = synchronized:
          inFlight.lastOption match
            case None =>
              (Some(reserve(now)), None)

            case Some((_, oldestStart)) =>
              val outstanding = Duration.fromNanos(now - oldestStart)

              if outstanding < stallAfter then
                suppressed += 1
                (None, None)
              else
                // Something is stalled, so this tick is reported either way. Whether it also gets
                // to START anything is gated on the NEWEST invocation, not the oldest: gating on
                // the oldest would let consecutive ticks run straight to the cap seconds apart,
                // whereas each extra should cost a full stallAfter of waiting. With a 10 minute
                // threshold and a cap of 3 that is one invocation at t=0, t=10, t=20 -- and if
                // the first was merely slow and finishes at t=12, the third never happens.
                val sinceNewest = Duration.fromNanos(now - inFlight.head._2)
                val admit = inFlight.size < maxInFlight && sinceNewest >= stallAfter

                if admit then
                  val ev = SingleFlightStall(name, outstanding, inFlight.size, suppressed,
                                             startedExtra = true)
                  (Some(reserve(now)), Some(ev))
                else
                  suppressed += 1
                  val ev = SingleFlightStall(name, outstanding, inFlight.size, suppressed,
                                             startedExtra = false)
                  (None, Some(ev))

        toReport.foreach(onStall)
        toStart.foreach(launch)

      private def launch(id: Long): Unit =
        val f =
          try fn
          catch case NonFatal(t) => Future.failed(t)

        f.onComplete: outcome =>
          synchronized:
            inFlight = inFlight.filterNot(_._1 == id)
            if inFlight.isEmpty then
              suppressed = 0
          outcome.failed.foreach: t =>
            scribe.warn(s"$name: invocation failed: $t")

    tracker.call

}
