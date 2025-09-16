package org.aspen_ddp.aspen.server.store.backend

import org.rocksdb.{FlushOptions, Options, RocksDB, WriteBatch, WriteOptions}

import java.nio.file.Path
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.*

object BufferedConsistentRocksDB {
  case class DBClosed() extends Throwable
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
  */
class BufferedConsistentRocksDB(val dbPath:Path)(using ec: ExecutionContext) {

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

  private def doNextCommit(): Future[Unit] = {
    val nbatch = nextBatch
    val npromise = nextPromise
    nextBatch = new WriteBatch()
    nextPromise = Promise[Unit]()
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


  private def commit(batch:WriteBatch, promise: Promise[Unit]): Unit = Future {
    val writeOpts = new WriteOptions()
    writeOpts.setSync(true)

    beginOperation()
    try {
      db.write(writeOpts, batch)
    } finally {
      endOperation()
      writeOpts.close()
      batch.close()
    }

    promise.success(())

    // Chain through to the next commit if pending writes exist
    synchronized {
      if (nextBatch.count() > 0)
        doNextCommit()
      else
        commitInProgress = false
    }
  }

  def bootstrapPut(key: Array[Byte], value: Array[Byte]): Unit = {
    val fcommit = synchronized {
      nextBatch.put(key, value)
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
    val fcommit = nextPromise.future

    if (!commitInProgress)
      doNextCommit()

    fcommit
  }

  def delete(key: Array[Byte]): Future[Unit] = synchronized {
    nextBatch.delete(key)
    val fcommit = nextPromise.future

    if (!commitInProgress)
      doNextCommit()

    fcommit
  }

  def get(key: Array[Byte]): Future[Option[Array[Byte]]] = Future {
    beginOperation()
    val value = db.get(key)
    endOperation()

    Option(value)
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
