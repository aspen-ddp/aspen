package org.aspen_ddp.aspen.server.store.backend.fsif

import java.io.IOException
import java.security.SecureRandom
import java.util.UUID

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

/** The device journal's writer (spec 4.3, 4.5, 4.8): the only writer of the ring's files `<n>.fj` in `files`.
  *
  * '''Threads.''' Every public method may be called from any thread. One lock guards all state and is never held
  * across I/O or an `Owner` call. Each write (a file's header when the writer enters it, then one batch) is one
  * blocking task handed to `run`: production passes the device's journal thread, tests a ManualEc or a direct runner.
  * One batch is in flight at a time; appends made meanwhile form the next batch, as far as they fit the current file.
  * `forceAfterWrite` forces after each header and batch write, for an `FsifFiles` that is not DSYNC (tests'
  * SimulatedFsifFiles); production passes false with DSYNC files. Each append's Future completes through its
  * attachment's `Owner.complete` (the store's serial executor). `roomAvailable`, `checkpointRequested` and
  * `journalFailed` are called on whichever thread made the change, never under the lock.
  *
  * '''Start.''' `start` None is a fresh ring: the writer enters file 0 with first serial 0. `Some(tail)` follows a
  * device-load replay after which no batch is needed: the writer enters the file after `tail.file` before its first
  * batch (never appending after a possibly torn tail), continuing at `tail.nextSerial`.
  *
  * '''Entering a file.''' The ring is used in order. The writer enters the next file only when it is free: every
  * unreleased attachment's oldest needed serial (the lowest serial of its records above its covered serial; none for
  * an idle one, which pins nothing) lies above the last serial of that file's previous pass. A file this writer has
  * not yet written holds nothing needed. Entry rewrites the header (fresh salt, `first` = the next serial,
  * `oldestNeeded` = the minimum oldest needed serial, else `first`) before the batch. A file once free stays free
  * until it is entered: later records get higher serials and covered serials only rise.
  *
  * '''Reservations.''' A reservation of `bytes` (tagged record bytes, `DeviceJournalFormat.taggedRecordSize`) is for
  * one append of at most that many record bytes, made while the reservation is held; it is returned by `unreserve`
  * with the same `bytes` once that append completes. Let `c = cost(bytes)`, the padded size of a batch holding only
  * those records (`padded(20 + bytes)`). A reservation is refused when `c` exceeds one file less its header (`C`),
  * and otherwise charged `2c - 4096` against the room: the space left in the current file plus `C` for each free file
  * in ring order after it, less the charges already reserved. That charge covers the item and the unusable tail it
  * may leave: when the writer takes the next batch, an append that does not fit what is left of the current file
  * leaves at most `c - 4096` there (both are multiples of 4096), and each append leaves at most one such tail. A
  * batch of several appends is never larger than the sum of their `c`. So writing a batch reduces the room by at most
  * the charges of the appends in it, the reserved appends still waiting always fit what remains (their charges do),
  * and an append made under a reservation is written without waiting for any file to become free: when the oldest
  * waiting one does not fit the current file, its charge `2c - 4096 >= c` exceeds what is left there, so a free file
  * follows. A reservation stays charged until `unreserve` (or until its attachment is released and its queued
  * appends are batched), so between a batch's write and the unreserve its bytes count twice: conservative. Small
  * appends (records up to 4076 bytes) are charged exactly their one block. An append made without a reservation
  * keeps no such promise and waits in order for room; production makes none.
  *
  * '''Pressure.''' When no file after the current one is free and the current file is at least 3/4 full, when a
  * reservation was refused and has not had its room yet, or when the next batch waits for a file, each owner whose
  * oldest needed serial lies in the oldest pinned file gets `checkpointRequested`, once per oldest needed serial and
  * again after each `covered` (a finished checkpoint that left it pinning). A refused attachment gets
  * `roomAvailable` once the room covers the charge it was refused.
  *
  * '''Failure.''' A failed write (or a `run` that throws) fails the journal: `failed` becomes Some, every pending
  * append fails with it, each unreleased attachment's owner gets `journalFailed` once, and later appends and
  * reservations fail at once.
  */
final class FsifDeviceJournal(files: FsifFiles, val sizing: JournalSizing, start: Option[Tail],
                              run: Runnable => Unit, forceAfterWrite: Boolean):
  import FsifDeviceJournal.*

  private val lock = new Object
  private val count = sizing.files
  private val fileSize = sizing.fileSize
  private val header = DeviceJournalFormat.HeaderSize.toLong
  /** A file's usable bytes */
  private val usable = fileSize - header
  private val random = new SecureRandom

  // Guarded by lock
  /** Last serial this writer wrote into each file in its current pass; -1 for none */
  private val lastOf = Array.fill(count)(-1L)
  /** The current file. Before the first entry it is the one before the entry file, with nothing left in it */
  private var cur = start match
    case Some(t) => t.file
    case None => count - 1
  private var entered = false
  private var pos = fileSize
  private var salt = 0L
  private var nextSerial = start.map(_.nextSerial).getOrElse(0L)
  private var tail = nextSerial - 1
  private val attachments = mutable.HashMap[UUID, Att]()
  private val queue = mutable.ArrayDeque[Pending]()
  private var inFlight = false
  private var failure: Option[Throwable] = None
  private var closed = false
  /** Minimum oldest needed serial over unreleased attachments; NoSerial for none */
  private var minNeeded = NoSerial
  private var reservedTotal = 0L
  /** Attachments refused a reservation that have not had roomAvailable since */
  private val refused = mutable.LinkedHashSet[Att]()

  // Touched only by write tasks, which run one at a time, ordered by the lock; and by close once none can run
  private val handles = new Array[FsifFile](count)

  def attach(attachment: UUID, directory: os.Path, owner: Owner): Unit = lock.synchronized:
    failure.foreach(t => throw new IOException("the device journal has failed", t))
    if closed then
      throw new IOException("the device journal is closed")
    require(!attachments.contains(attachment), s"attachment $attachment is already attached")
    attachments(attachment) = new Att(attachment, directory, owner)

  def tailSerial: Long = lock.synchronized(tail)

  def failed: Option[Throwable] = lock.synchronized(failure)

  def tryReserve(attachment: UUID, bytes: Long): Boolean =
    require(bytes >= 0, s"negative reservation $bytes")
    val c = cost(bytes)
    val (ok, notes) = lock.synchronized:
      val a = attachments.getOrElse(attachment, throw new IllegalArgumentException(s"unknown attachment $attachment"))
      if failure.isDefined || closed || a.released || c > usable then
        (false, Nil)
      else
        val charge = 2 * c - 4096
        if charge <= room() then
          a.reserved += charge
          reservedTotal += charge
          (true, Nil)
        else
          a.refusedCharge = charge
          refused += a
          (false, pressureNotes())
    notes.foreach(_())
    ok

  def unreserve(attachment: UUID, bytes: Long): Unit =
    val charge = 2 * cost(bytes) - 4096
    val notes = lock.synchronized:
      attachments.get(attachment) match
        case None => Nil
        case Some(a) =>
          val back = math.min(charge, a.reserved)
          a.reserved -= back
          reservedTotal -= back
          roomNotes()
    notes.foreach(_())

  def append(attachment: UUID, records: List[JournalRecord]): Future[Long] =
    val bytes = records.iterator.map(r => DeviceJournalFormat.taggedRecordSize(r).toLong).sum
    val (result, write, notes) = lock.synchronized:
      failure match
        case Some(t) => (Future.failed(t), None, Nil)
        case None =>
          attachments.get(attachment) match
            case _ if closed => (Future.failed(new IOException("the device journal is closed")), None, Nil)
            case None => (Future.failed(new IllegalArgumentException(s"unknown attachment $attachment")), None, Nil)
            case Some(_) if cost(bytes) > usable =>
              (Future.failed(new IllegalArgumentException(s"an append of $bytes bytes exceeds a journal file")),
                None, Nil)
            case Some(a) =>
              val p = Promise[Long]()
              queue += Pending(a, records, bytes, p)
              a.queued += 1
              val w = nextWrite()
              // Waiting for a file: ask for checkpoints (rare)
              val n = if w.isEmpty && !inFlight then pressureNotes() else Nil
              (p.future, w, n)
    notes.foreach(_())
    write.foreach(submit)
    result

  def covered(attachment: UUID, serial: Long): Unit =
    val (write, notes) = lock.synchronized:
      attachments.get(attachment) match
        case None => (None, Nil)
        case Some(a) =>
          val wasHead = a.head
          while a.serials.nonEmpty && a.serials.head <= serial do
            a.serials.removeHead()
          if a.head != wasHead && wasHead == minNeeded then
            recomputeMinNeeded()
          // A checkpoint finished: one that left its owner still pinning (its snapshot predated head) is asked again
          a.requestedFor = NoSerial
          (nextWrite(), roomNotes() ++ pressureNotes())
    notes.foreach(_())
    write.foreach(submit)

  def release(attachment: UUID): Unit =
    val (write, notes) = lock.synchronized:
      attachments.get(attachment).foreach(releaseLocked)
      (nextWrite(), roomNotes() ++ pressureNotes())
    notes.foreach(_())
    write.foreach(submit)

  def releaseDirectory(directory: os.Path): Unit =
    val (write, notes) = lock.synchronized:
      attachments.values.filter(_.directory == directory).toList.foreach(releaseLocked)
      (nextWrite(), roomNotes() ++ pressureNotes())
    notes.foreach(_())
    write.foreach(submit)

  /** Pending appends fail; a write in flight finishes first, then the files are closed */
  def close(): Unit =
    val (pending, closeNow) = lock.synchronized:
      if closed then
        (Nil, false)
      else
        closed = true
        val ps = queue.toList
        queue.clear()
        (ps, !inFlight)
    val t = new IOException("the device journal is closed")
    pending.foreach(p => p.att.owner.complete(() => p.promise.failure(t)))
    if closeNow then
      closeHandles()

  // ---- Internals: everything below that is not a write task runs under the lock

  private def cost(bytes: Long): Long = (20 + bytes + 4095) / 4096 * 4096

  /** Free files after the current one, in ring order (before the first entry, the current one too) */
  private def freeRun(): Int =
    val limit = if entered then count - 1 else count
    var k = 0
    while k < limit && lastOf((cur + 1 + k) % count) < minNeeded do
      k += 1
    k

  private def room(): Long = (fileSize - pos) + freeRun() * usable - reservedTotal

  private def recomputeMinNeeded(): Unit =
    minNeeded = attachments.valuesIterator.map(_.head).foldLeft(NoSerial)(math.min)

  private def releaseLocked(a: Att): Unit =
    if !a.released then
      a.released = true
      val wasHead = a.head
      a.serials.clear()
      refused -= a
      if wasHead == minNeeded then
        recomputeMinNeeded()
      if a.queued == 0 then
        dropReleased(a)

  /** A released attachment with no queued append: its reservations go back */
  private def dropReleased(a: Att): Unit =
    reservedTotal -= a.reserved
    a.reserved = 0
    attachments.remove(a.id)

  /** The next write, if one can start now. Takes the queue's longest prefix that fits the current file, entering the
    * next file first when the oldest pending append does not fit and that file is free */
  private def nextWrite(): Option[Write] =
    if inFlight || failure.isDefined || closed || queue.isEmpty then
      None
    else
      var entry: Option[(Int, FileHeader)] = None
      if cost(queue.head.bytes) > fileSize - pos then
        val f = (cur + 1) % count
        if lastOf(f) < minNeeded then
          salt = random.nextLong()
          cur = f
          entered = true
          pos = header
          lastOf(f) = -1
          val oldest = if minNeeded == NoSerial then nextSerial else minNeeded
          entry = Some(f -> FileHeader(sizing.journalId, salt, nextSerial, oldest))
      if cost(queue.head.bytes) > fileSize - pos then
        None
      else
        val taken = mutable.ListBuffer[Pending]()
        var bytes = 0L
        while queue.nonEmpty && cost(bytes + queue.head.bytes) <= fileSize - pos do
          bytes += queue.head.bytes
          taken += queue.removeHead()
        val serial = nextSerial
        nextSerial += 1
        lastOf(cur) = serial
        val at = pos
        pos += cost(bytes)
        taken.foreach: p =>
          val a = p.att
          a.queued -= 1
          if !a.released then
            if a.serials.isEmpty then
              if minNeeded == NoSerial then
                minNeeded = serial
              a.serials += serial
            else if a.serials.last != serial then
              a.serials += serial
          else if a.queued == 0 then
            dropReleased(a)
        inFlight = true
        Some(Write(entry, cur, at, serial, salt, taken.toList))

  /** roomAvailable for each refused attachment whose refused charge now fits */
  private def roomNotes(): List[() => Unit] =
    if refused.isEmpty then
      Nil
    else
      val r = room()
      val ready = refused.filter(_.refusedCharge <= r).toList
      ready.foreach(refused -= _)
      ready.map(a => () => a.owner.roomAvailable())

  /** checkpointRequested for the owners pinning the oldest pinned file, when under pressure */
  private def pressureNotes(): List[() => Unit] =
    if failure.isDefined || closed then
      Nil
    else
      val k = freeRun()
      val limit = if entered then count - 1 else count
      val blocked = queue.nonEmpty && !inFlight
      val full = k == 0 && entered && (pos - header) * 4 >= usable * 3
      if k >= limit || !(full || blocked || refused.nonEmpty) then
        Nil
      else
        val last = lastOf((cur + 1 + k) % count)
        attachments.valuesIterator.filter(a => !a.released && a.head <= last && a.head != a.requestedFor).toList.map:
          a =>
            a.requestedFor = a.head
            () => a.owner.checkpointRequested()

  private def submit(w: Write): Unit =
    try run(() => write(w))
    catch case NonFatal(t) => fail(t, w.pending)

  private def handle(f: Int): FsifFile =
    if handles(f) == null then
      handles(f) = files.open(DeviceJournalScan.fileName(f))
    handles(f)

  /** The write task */
  private def write(w: Write): Unit =
    val error =
      try
        w.entry.foreach: (f, h) =>
          val hd = handle(f)
          hd.write(0, DeviceJournalFormat.encodeHeader(h))
          if forceAfterWrite then hd.force()
        val tagged = w.pending.iterator.flatMap(p => p.records.map(TaggedRecord(p.att.id, _))).toVector
        val hd = handle(w.file)
        hd.write(w.position, DeviceJournalFormat.encodeBatch(sizing.journalId, w.salt, Batch(w.serial, tagged)))
        if forceAfterWrite then hd.force()
        None
      catch case NonFatal(t) => Some(t)
    error match
      case Some(t) => fail(t, w.pending)
      case None => written(w)

  private def written(w: Write): Unit =
    val (next, notes, closeNow) = lock.synchronized:
      inFlight = false
      tail = w.serial
      val nw = nextWrite()
      (nw, roomNotes() ++ pressureNotes(), closed && !inFlight)
    w.pending.foreach(p => p.att.owner.complete(() => p.promise.success(w.serial)))
    notes.foreach(_())
    if closeNow then
      closeHandles()
    next.foreach(submit)

  private def fail(t: Throwable, inFlightPending: List[Pending]): Unit =
    val (pending, owners, closeNow) = lock.synchronized:
      inFlight = false
      if failure.isDefined then
        (inFlightPending, Nil, closed)
      else
        failure = Some(t)
        val ps = inFlightPending ++ queue
        queue.clear()
        refused.clear()
        val os = attachments.valuesIterator.filter(!_.released).map(_.owner).toList.distinct
        (ps, os, closed)
    pending.foreach(p => p.att.owner.complete(() => p.promise.failure(t)))
    owners.foreach(_.journalFailed(t))
    if closeNow then
      closeHandles()

  private def closeHandles(): Unit =
    for i <- 0 until count do
      if handles(i) != null then
        try handles(i).close()
        catch case NonFatal(_) => ()
        handles(i) = null

object FsifDeviceJournal:
  trait Owner:
    /** Runs `task` on the store's serial executor */
    def complete(task: Runnable): Unit
    /** Room has returned since a refused tryReserve */
    def roomAvailable(): Unit
    /** Checkpoint now: this attachment's records pin the oldest file the writer needs */
    def checkpointRequested(): Unit
    def journalFailed(t: Throwable): Unit

  private val NoSerial = Long.MaxValue

  private final class Att(val id: UUID, val directory: os.Path, val owner: Owner):
    /** Serials of the batches holding this attachment's records above its covered serial, ascending */
    val serials = mutable.ArrayDeque[Long]()
    var reserved = 0L
    /** Appends queued and not yet in a batch */
    var queued = 0
    var released = false
    var refusedCharge = 0L
    /** The oldest needed serial at the last checkpointRequested */
    var requestedFor = NoSerial

    /** Oldest needed serial; NoSerial when idle */
    def head: Long = if serials.isEmpty then NoSerial else serials.head

  private final case class Pending(att: Att, records: List[JournalRecord], bytes: Long, promise: Promise[Long])

  /** `entry`: the file entered and its new header, written before the batch */
  private final case class Write(entry: Option[(Int, FileHeader)], file: Int, position: Long, serial: Long,
                                 salt: Long, pending: List[Pending])
