package org.aspen_ddp.aspen.server.store.backend

import org.aspen_ddp.aspen.FileBasedTests
import org.rocksdb.{WriteBatch, WriteOptions}

import java.nio.file.Path
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*

object BufferedConsistentRocksDBSuite:

  case class InjectedWriteFailure() extends Throwable

  /** Allows tests to make the underlying RocksDB write fail on demand */
  class FailableRocksDB(path: Path)(using ExecutionContext) extends BufferedConsistentRocksDB(path):
    @volatile var failWrites = false

    override protected def writeBatch(writeOpts: WriteOptions, batch: WriteBatch): Unit =
      if failWrites then
        throw InjectedWriteFailure()
      else
        super.writeBatch(writeOpts, batch)

  /** ExecutionContext that queues tasks and only runs them when explicitly told to.
    *
    * This lets the tests park a commit mid-flight and issue reads against the database while the
    * pending WriteBatch has yet to reach RocksDB.
    */
  class ManualExecutionContext extends ExecutionContext:
    private val tasks = new mutable.ArrayBuffer[Runnable]()

    override def execute(runnable: Runnable): Unit = synchronized:
      tasks += runnable

    override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()

    def pendingCount: Int = synchronized:
      tasks.size

    /** Runs the queued task at the given index, leaving all other queued tasks untouched */
    def runTask(idx: Int): Unit =
      val task = synchronized:
        tasks.remove(idx)
      task.run()

    def runAll(): Unit =
      while pendingCount > 0 do
        runTask(0)

class BufferedConsistentRocksDBSuite extends FileBasedTests:

  import BufferedConsistentRocksDBSuite.*

  private def key(i: Int): Array[Byte] = Array(i.toByte)

  private def value(s: String): Array[Byte] = s.getBytes("UTF-8")

  private def str(o: Option[Array[Byte]]): Option[String] = o.map(new String(_, "UTF-8"))

  private def awaitGet(f: Future[Option[Array[Byte]]]): Option[String] =
    str(Await.result(f, 5.seconds))

  private def withDB(fn: (FailableRocksDB, ManualExecutionContext) => Unit): Unit =
    val ec = new ManualExecutionContext
    val db = new FailableRocksDB(Path.of(tdir.getPath, "rocksdb"))(using ec)
    try
      fn(db, ec)
    finally
      db.failWrites = false
      ec.runAll() // drain outstanding writes before closing
      db.close()
      ec.runAll()

  test("get sees a put whose write batch is still in flight") {
    withDB { (db, ec) =>
      db.put(key(1), value("first"))

      ec.pendingCount should be(1) // the commit is queued but has not written to RocksDB

      val fget = db.get(key(1))
      ec.runTask(1) // run only the read, leaving the commit in flight

      awaitGet(fget) should be(Some("first"))
    }
  }

  test("get sees a put queued behind an in-flight commit") {
    withDB { (db, ec) =>
      db.put(key(1), value("first")) // starts a commit
      db.put(key(2), value("second")) // buffered in the next batch, no commit started

      ec.pendingCount should be(1)

      val fget = db.get(key(2))
      ec.runTask(1)

      awaitGet(fget) should be(Some("second"))
    }
  }

  test("get does not resurrect a key deleted by an in-flight batch") {
    withDB { (db, ec) =>
      val fput = db.put(key(1), value("first"))
      ec.runAll()
      Await.result(fput, 5.seconds)

      db.delete(key(1))

      val fget = db.get(key(1))
      ec.runTask(1)

      awaitGet(fget) should be(None)
    }
  }

  test("a queued write supersedes an in-flight write of the same key") {
    withDB { (db, ec) =>
      db.put(key(1), value("old")) // in-flight batch
      db.put(key(1), value("new")) // queued batch

      val fget = db.get(key(1))
      ec.runTask(1)

      awaitGet(fget) should be(Some("new"))
    }
  }

  test("a queued delete supersedes an in-flight write of the same key") {
    withDB { (db, ec) =>
      db.put(key(1), value("old")) // in-flight batch
      db.delete(key(1)) // queued batch

      val fget = db.get(key(1))
      ec.runTask(1)

      awaitGet(fget) should be(None)
    }
  }

  test("get falls through to the database once the batch has been written") {
    withDB { (db, ec) =>
      val fput = db.put(key(1), value("first"))
      ec.runAll()
      Await.result(fput, 5.seconds)

      val fget = db.get(key(1))
      ec.runAll()

      awaitGet(fget) should be(Some("first"))
    }
  }

  test("pending writes are discarded once their batch has been written") {
    withDB { (db, ec) =>
      val fput = db.put(key(1), value("buffered"))
      ec.runAll()
      Await.result(fput, 5.seconds)

      // Bypasses the write buffer entirely, so a read can only observe this value if the
      // completed batch is no longer being consulted by get()
      db.rebuildWrite(key(1), value("direct"))

      val fget = db.get(key(1))
      ec.runAll()

      awaitGet(fget) should be(Some("direct"))
    }
  }

  test("a failed write completes its future with the error") {
    withDB { (db, ec) =>
      db.failWrites = true

      val fput = db.put(key(1), value("lost"))
      ec.runAll()

      intercept[InjectedWriteFailure] {
        Await.result(fput, 5.seconds)
      }
    }
  }

  test("a failed write does not block subsequent writes") {
    withDB { (db, ec) =>
      db.failWrites = true
      val ffailed = db.put(key(1), value("lost"))
      ec.runAll()
      Await.ready(ffailed, 5.seconds)

      db.failWrites = false
      val fput = db.put(key(2), value("second"))
      ec.runAll()
      Await.result(fput, 5.seconds)

      val fget = db.get(key(2))
      ec.runAll()

      awaitGet(fget) should be(Some("second"))
    }
  }

  test("a write queued behind a failed write is still committed") {
    withDB { (db, ec) =>
      db.failWrites = true
      val ffailed = db.put(key(1), value("lost")) // in-flight batch, will fail
      val fqueued = db.put(key(2), value("second")) // queued behind it

      ec.runTask(0) // run the failing commit, which must still chain to the queued batch
      db.failWrites = false
      ec.runAll()

      Await.ready(ffailed, 5.seconds)
      ffailed.value.map(_.isFailure) should be(Some(true))
      Await.result(fqueued, 5.seconds)

      val fget = db.get(key(2))
      ec.runAll()

      awaitGet(fget) should be(Some("second"))
    }
  }

  test("a failed write is not visible to reads") {
    withDB { (db, ec) =>
      val fput = db.put(key(1), value("committed"))
      ec.runAll()
      Await.result(fput, 5.seconds)

      db.failWrites = true
      val ffailed = db.put(key(1), value("lost"))
      ec.runAll()
      Await.ready(ffailed, 5.seconds)

      db.failWrites = false
      val fget = db.get(key(1))
      ec.runAll()

      awaitGet(fget) should be(Some("committed"))
    }
  }

  test("a commit that cannot begin completes its future rather than hanging") {
    withDB { (db, ec) =>
      val fput = db.put(key(1), value("first"))

      db.close() // makes beginOperation throw before the write is attempted
      ec.runAll()

      intercept[BufferedConsistentRocksDB.DBClosed] {
        Await.result(fput, 5.seconds)
      }
    }
  }

