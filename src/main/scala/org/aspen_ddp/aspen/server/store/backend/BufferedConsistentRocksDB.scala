package org.aspen_ddp.aspen.server.store.backend

import org.rocksdb.{FlushOptions, Options, RocksDB, WriteBatch, WriteOptions}
import scribe.Logging

import java.nio.file.Path
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

object BufferedConsistentRocksDB {
  case class DBClosed() extends Throwable

  /** Mirrors the content of a WriteBatch. A value of None represents a pending deletion. */
  private type PendingOps = mutable.HashMap[ArraySeq[Byte], Option[Array[Byte]]]
}

/** RocksDB Key-Value store were all puts & deletes return Futures to data-at-rest on disk.
  *
  *  All blocking put/get/delete operations on the RocksDB database are delegated to Future {} blocks handled by the implicit
  *  ExecutionContext. This should be tuned to support the desired number of background threads.
  *
  *  While a synchronous commit is outstanding, all put/delete operations are buffered in a WriteBatch. When the outstanding
  *  commit completes, the commit of the next write batch is immediately started if it contains any operations. When a put/delete
  *  is done and no outstanding commit exists, a commit for that single operation is immediately started.
  *
  *  Buffered operations are not visible to RocksDB until their WriteBatch has been written, so get() must consult them
  *  before falling through to the database. Note that the key and value arrays handed to put/delete are retained until
  *  their batch reaches disk and must not be modified by the caller in the interim.
  *
  */
class BufferedConsistentRocksDB(val dbPath:Path)(using ec: ExecutionContext) extends Logging {

  import BufferedConsistentRocksDB._

  private val db: RocksDB = {
    val options = new Options().setCreateIfMissing(true)
    try {
      RocksDB.open(options, dbPath.toString)
    } finally {
      options.close()
    }
  }

  private var nextBatch = new WriteBatch()
  private var nextPromise = Promise[Unit]()
  private var commitInProgress = false
  private var closing: Option[Promise[Unit]] = None
  private var outsandingOpCount = 0

  // Shadows the content of nextBatch and of the batch currently being written, respectively. These hold
  // only the operations that RocksDB cannot yet see, so their size is bounded by the write latency.
  private var nextPending: PendingOps = mutable.HashMap()
  private var committingPending: PendingOps = mutable.HashMap()

  def estimateSize(): Long = db.getLongProperty("rocksdb.total-sst-files-size")

  private def pendingKey(key: Array[Byte]): ArraySeq[Byte] = ArraySeq.unsafeWrapArray(key)

  /** Returns the result of the most recent buffered operation on the key, if any.
    *
    * The outer Option indicates whether a buffered operation exists for the key and the inner one carries its
    * result: Some(value) for a pending put and None for a pending delete.
    */
  private def pendingRead(key: Array[Byte]): Option[Option[Array[Byte]]] = synchronized {
    val k = pendingKey(key)
    nextPending.get(k).orElse(committingPending.get(k))
  }

  private def doNextCommit(): Future[Unit] = {
    val nbatch = nextBatch
    val npromise = nextPromise
    committingPending = nextPending
    nextBatch = new WriteBatch()
    nextPromise = Promise[Unit]()
    nextPending = mutable.HashMap()
    commitInProgress = true
    commit(nbatch, npromise)
    npromise.future
  }

  private def beginOperation(): Unit = synchronized {
    if (closing.isDefined) throw DBClosed()
    outsandingOpCount += 1
  }
  private def endOperation(): Unit = synchronized {
    outsandingOpCount -= 1
    if (outsandingOpCount == 0)
      closing.foreach(p => p.success(()))
  }


  /** Sole point at which batches reach RocksDB. Overridable so tests may inject write failures. */
  protected def writeBatch(writeOpts: WriteOptions, batch: WriteBatch): Unit =
    db.write(writeOpts, batch)

  private def commit(batch:WriteBatch, promise: Promise[Unit]): Unit = Future {
    val opCount = batch.count()

    val result = Try {
      val writeOpts = new WriteOptions()
      writeOpts.setSync(true)

      try {
        beginOperation()
        try {
          writeBatch(writeOpts, batch)
        } finally {
          endOperation()
        }
      } finally {
        writeOpts.close()
        batch.close()
      }
    }

    result match
      case Failure(err) => logger.error(s"RocksDB write of $opCount operations to $dbPath failed: $err")
      case Success(_) => ()

    // Chain through to the next commit if pending writes exist. A failed batch is never retried so,
    // either way, reads must now fall through to the content of the database itself.
    synchronized {
      committingPending = mutable.HashMap()

      if (nextBatch.count() > 0)
        doNextCommit()
      else
        commitInProgress = false
    }

    // Deliberately completed after the state above is restored so that callbacks reacting to the
    // result find the database ready to accept further writes
    promise.complete(result)
  }

  def bootstrapPut(key: Array[Byte], value: Array[Byte]): Unit = {
    val fcommit = synchronized {
      nextBatch.put(key, value)
      nextPending(pendingKey(key)) = Some(value)
      doNextCommit()
    }
    Await.result(fcommit, 10.seconds)
  }
  def bootstrapGet(key: Array[Byte]): Array[Byte] = synchronized {
    db.get(key)
  }


  def rebuildWrite(key: Array[Byte], value: Array[Byte]): Unit = synchronized {
    db.put(key, value)
  }

  def rebuildFlush(): Unit = synchronized {
    val flushOptions = new FlushOptions()
    flushOptions.setWaitForFlush(true)
    flushOptions.setAllowWriteStall(true)
    db.flush(flushOptions)
  }

  def put(key: Array[Byte], value: Array[Byte]): Future[Unit] = synchronized {
    nextBatch.put(key, value)
    nextPending(pendingKey(key)) = Some(value)
    val fcommit = nextPromise.future

    if (!commitInProgress)
      doNextCommit()

    fcommit
  }

  def delete(key: Array[Byte]): Future[Unit] = synchronized {
    nextBatch.delete(key)
    nextPending(pendingKey(key)) = None
    val fcommit = nextPromise.future

    if (!commitInProgress)
      doNextCommit()

    fcommit
  }

  def get(key: Array[Byte]): Future[Option[Array[Byte]]] = Future {
    beginOperation()
    try
      pendingRead(key) match
        case Some(pending) => pending
        case None => Option(db.get(key))
    finally
      endOperation()
  }

  def foreach(fn: (Array[Byte], Array[Byte]) => Unit): Future[Unit] = Future {
    beginOperation()
    val iterator = db.newIterator()
    try {
      iterator.seekToFirst()
      while (iterator.isValid) {
        fn(iterator.key(), iterator.value())
        iterator.next()
      }
    } finally {
      endOperation()
      iterator.close()
    }
  }

  def close(): Future[Unit] = synchronized {

    val p = Promise[Unit]()
    closing = Some(p)

    if (outsandingOpCount == 0)
      p.success(())

    p.future.andThen{ case _ =>
      db.close()
      nextBatch.close()
    }
  }
}
