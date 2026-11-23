package org.aspen_ddp.aspen.server.store.backend.rocksdb

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.backend.{Completion, CompletionHandler, Read, RetryScheduler}
import org.aspen_ddp.aspen.server.TestDeviceIo
import org.aspen_ddp.aspen.server.host.DeviceIo
import org.rocksdb.{RocksDBException, Status}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object RocksDBBackendReadSuite:
  val storeId: StoreId = StoreId(PoolId(new UUID(0, 7)), 0)
  val objectId: ObjectId = ObjectId(new UUID(0, 8))
  val pointer: DataObjectPointer = DataObjectPointer(objectId, storeId.poolId, Array[Byte]())

  def status(code: Status.Code): RocksDBException =
    new RocksDBException(s"injected $code", new Status(code, Status.SubCode.None, s"injected $code"))

  /** Runs each retry at once, recording its delay */
  final class ImmediateScheduler extends RetryScheduler:
    val delays: mutable.ArrayBuffer[FiniteDuration] = mutable.ArrayBuffer()
    def schedule(delay: FiniteDuration, task: Runnable): Unit =
      synchronized { delays += delay }
      task.run()

  /** Fails the first `failures.size` reads with the given errors, in order. Later reads return `stored` when given,
    * else read the database. */
  final class FailingReads(dir: os.Path, failures: Seq[Throwable], scheduler: RetryScheduler,
                           stored: Option[Array[Byte]], io: DeviceIo)
    extends RocksDBBackend(dir, storeId, io, RetryScheduler.ReadRetryDelays, scheduler):
    private val remaining = mutable.Queue(failures*)
    @volatile var attempts = 0
    override protected def getValue(key: Array[Byte]): Future[Option[Array[Byte]]] =
      val next = synchronized { attempts += 1; if remaining.isEmpty then None else Some(remaining.dequeue()) }
      next match
        case Some(err) => Future.failed(err)
        case None => stored.fold(super.getValue(key))(v => Future.successful(Some(v)))

class RocksDBBackendReadSuite extends AnyFunSuite with Matchers:
  import RocksDBBackendReadSuite.*

  private def readOnce(failures: Seq[Throwable],
                       storedValue: Option[Array[Byte]] = None,
                       store: Boolean = true,
                       io: Option[TestDeviceIo] = None): (Either[Any, ReadError.Value], FailingReads, ImmediateScheduler) =
    val dir = os.temp.dir()
    try
      RocksDBBackend.initializeStore(dir)
      val scheduler = ImmediateScheduler()
      val backend = FailingReads(dir, failures, scheduler, storedValue, io.getOrElse(TestDeviceIo.of(dir)))
      try
        if store then
          backend.bootstrapAllocate(objectId, ObjectType.Data,
            Metadata(ObjectRevision(TransactionId(new UUID(0, 9))), ObjectRefcount(0, 1), HLCTimestamp(1L)),
            DataBuffer(Array[Byte](1, 2, 3)))
        val completed = Promise[Completion]()
        backend.setCompletionHandler(new CompletionHandler:
          override def complete(op: Completion): Unit = completed.trySuccess(op))
        backend.read(pointer)
        Await.result(completed.future, 10.seconds) match
          case r: Read => (r.result, backend, scheduler)
          case other => fail(s"Expected a Read, got $other")
      finally Await.result(backend.close(), 10.seconds)
    finally os.remove.all(dir)

  test("a read failing with a transient RocksDB status is retried and returns the object (X-W14)"):
    val (result, backend, scheduler) = readOnce(Seq(status(Status.Code.IOError), status(Status.Code.Busy)))
    result.isLeft should be(true)
    backend.attempts should be(3)
    scheduler.delays.toSeq should be(RetryScheduler.ReadRetryDelays.take(2))

  test("a read failing with a non-RocksDB exception is retried"):
    val (result, backend, _) = readOnce(Seq(new java.io.IOException("Too many open files")))
    result.isLeft should be(true)
    backend.attempts should be(2)

  test("a read failing with a Corruption status reports CorruptedObject without a retry"):
    val (result, backend, scheduler) = readOnce(Seq(status(Status.Code.Corruption)))
    result should be(Right(ReadError.CorruptedObject))
    backend.attempts should be(1)
    scheduler.delays should be(empty)

  test("a read failing persistently reports CorruptedObject once its retries are spent"):
    val n = RetryScheduler.ReadRetryDelays.size + 1
    val (result, backend, scheduler) = readOnce(Seq.fill(n)(status(Status.Code.IOError)))
    result should be(Right(ReadError.CorruptedObject))
    backend.attempts should be(n)
    scheduler.delays.toSeq should be(RetryScheduler.ReadRetryDelays)

  test("a value too short to decode reports CorruptedObject"):
    val (result, _, scheduler) = readOnce(Nil, storedValue = Some(Array[Byte](0, 1)), store = false)
    result should be(Right(ReadError.CorruptedObject))
    scheduler.delays should be(empty)

  test("every read attempt is counted on the device and ended, whether it succeeds, fails or is retried"):
    val dir = os.temp.dir()
    val io = new TestDeviceIo(dir, readBound = 1)
    val ok = readOnce(Nil, io = Some(io))
    ok._1.isLeft should be(true)
    io.reads.get should be(0)
    val retried = readOnce(Seq(status(Status.Code.IOError), new java.io.IOException("EMFILE")), io = Some(io))
    retried._1.isLeft should be(true)
    retried._2.attempts should be(3)
    io.reads.get should be(0)
    val corrupt = readOnce(Seq(status(Status.Code.Corruption)), io = Some(io))
    corrupt._1 should be(Right(ReadError.CorruptedObject))
    io.reads.get should be(0)
    val exhausted = readOnce(Seq.fill(RetryScheduler.ReadRetryDelays.size + 1)(status(Status.Code.IOError)), io = Some(io))
    exhausted._1 should be(Right(ReadError.CorruptedObject))
    io.reads.get should be(0)
    os.remove.all(dir)

  test("a read of a closed database is ended, and the backend reports the device's bound"):
    val dir = os.temp.dir()
    try
      RocksDBBackend.initializeStore(dir)
      val io = new TestDeviceIo(dir, readBound = 1)
      val backend = new RocksDBBackend(dir, storeId, io)
      backend.readsOverBound should be(false)
      io.beginRead()
      backend.readsOverBound should be(true)
      io.endRead()
      val completed = Promise[Completion]()
      backend.setCompletionHandler(new CompletionHandler:
        override def complete(op: Completion): Unit = completed.trySuccess(op))
      Await.result(backend.close(), 10.seconds)
      backend.read(pointer)
      Await.result(completed.future, 10.seconds) match
        case r: Read => r.result should be(Right(ReadError.CorruptedObject))
        case other => fail(s"Expected a Read, got $other")
      io.reads.get should be(0)
    finally os.remove.all(dir)
