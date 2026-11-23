package org.aspen_ddp.aspen.server.store.backend.rocksdb

import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, ReadError}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionId
import org.aspen_ddp.aspen.server.store.backend.{Completion, CompletionHandler, Read, RetryScheduler}
import org.aspen_ddp.aspen.server.TestDeviceIo
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
                           stored: Option[Array[Byte]])
    extends RocksDBBackend(dir, storeId, TestDeviceIo.of(dir), RetryScheduler.ReadRetryDelays, scheduler):
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
                       store: Boolean = true): (Either[Any, ReadError.Value], FailingReads, ImmediateScheduler) =
    val dir = os.temp.dir()
    try
      RocksDBBackend.initializeStore(dir)
      val scheduler = ImmediateScheduler()
      val backend = FailingReads(dir, failures, scheduler, storedValue)
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
