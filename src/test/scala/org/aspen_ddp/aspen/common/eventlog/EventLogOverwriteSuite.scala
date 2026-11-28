package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.client.Transaction
import org.aspen_ddp.aspen.common.ida.{IDA, ReedSolomon}

import scala.concurrent.{ExecutionContext, Future}

/** The event log service on classic ReedSolomon, which cannot append: tails are rewritten whole */
class EventLogOverwriteSuite extends EventLogServiceSuite:

  override def testIda: IDA = ReedSolomon(5, 3, 4)

  atest("tails on classic ReedSolomon are rewritten whole"):
    given ExecutionContext = executionContext
    makeExecutor()
    val first = (0 until 5).map(i => entry(i))
    val second = (5 until 10).map(i => entry(i))
    // One transaction each, so each set is moved by one batch and written as one frame
    def logAll(es: Seq[EventLogEntry]): Future[Unit] =
      client.transact: tx =>
        given Transaction = tx
        tx.replaySafe("event log entries may be duplicated")
        client.prepareSystemEventLogEntries(es)
    def records(st: StateView, es: Seq[EventLogEntry]): Vector[LoggedRecord] =
      es.toVector.sortBy(_.timestamp).map(e => LoggedRecord(e.timestamp, st.typeIds(e.typeUUID), e.data))
    for
      st0 <- readState()
      pool <- client.getStoragePool(st0.tails.pointers.head.poolId)
      _ <- logAll(first)
      _ <- awaitIncomingEmpty()
      _ <- logAll(second)
      _ <- awaitIncomingEmpty()
      st <- readState()
      tail <- client.read(st0.tails.pointers.head)
    yield
      pool.ida shouldBe ReedSolomon(5, 3, 4)
      st.tails.current shouldBe 0
      val expected = LogRecords.encodeFrame(records(st, first)) ++ LogRecords.encodeFrame(records(st, second))
      tail.data.getByteArray.toSeq shouldBe expected.toSeq
