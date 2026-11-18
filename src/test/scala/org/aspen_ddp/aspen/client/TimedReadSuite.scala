package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer, ObjectId}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.scalatest.compatible.Assertion

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.*

/** client.withReadTimeout: every read method of the timed view is bounded, and untimed reads are unaffected */
class TimedReadSuite extends IntegrationTestSuite:

  private def allStores = net.stores.map(_.storeId).toSet

  /** No store replies to its reads, so the object need not exist */
  private def dataPtr = DataObjectPointer(ObjectId(new UUID(7, 7)), Radicle.poolId)

  private def timesOut(name: String, f: => Future[Any]): Future[Assertion] =
    f.map(r => fail(s"$name returned $r instead of timing out")).recover:
      case _: ReadTimeout => succeed

  atest("a timed read of a reachable object succeeds") {
    client.withReadTimeout(5.seconds).read(radicle).map(_.pointer should be (radicle))
  }

  atest("every timed read method fails with ReadTimeout while the stores are silent") {
    val r = client.withReadTimeout(100.millis)
    net.withReadSilenced(allStores):
      for
        _ <- timesOut("read", r.read(radicle))
        _ <- timesOut("readKeys", r.readKeys(radicle, List(Key("x"))))
        _ <- timesOut("readKeyRange", r.readKeyRange(radicle, Key("a"), Key("z"),
                                                     org.aspen_ddp.aspen.common.objects.ByteArrayKeyOrdering))
        _ <- timesOut("readMetadata", r.readMetadata(radicle))
        _ <- timesOut("readOptional", r.readOptional(radicle))
        _ <- timesOut("readRange", r.readRange(dataPtr, 0, Some(4)))
        _ <- timesOut("readRanges", r.readRanges(dataPtr, List((0, 4))))
        a <- timesOut("readAppendPosition", r.readAppendPosition(dataPtr))
      yield a
  }

  atest("withReadTimeout(Duration.Inf) is the client itself; other non-positive or non-finite durations are refused") {
    client.withReadTimeout(Duration.Inf) should be theSameInstanceAs client
    for d <- List(Duration.Zero, -1.second, Duration.MinusInf, Duration.Undefined) do
      an [IllegalArgumentException] should be thrownBy client.withReadTimeout(d)
    Future.successful(succeed)
  }

  /** Fails the test instead of hanging it when `f` does not complete within `limit` */
  private def within[T](limit: FiniteDuration, what: String)(f: Future[T]): Future[T] =
    Future.firstCompletedOf(List(f, client.after(limit).map(_ => fail(s"$what did not complete within $limit"))))

  atest("a timed read bounds the pool lookup too, and an untimed load of the same pool is not failed by it") {
    // A pool the client has not loaded: its state can only be read through the silenced bootstrap stores
    val unloadedPool = PoolId(new UUID(9, 9))
    val ptr = KeyValueObjectPointer(ObjectId(new UUID(9, 10)), unloadedPool)
    net.readSilencedStores = allStores
    val untimedLoad = client.getStoragePool(unloadedPool)
    for
      _ <- within(5.seconds, "the timed read")(timesOut("read", client.withReadTimeout(100.millis).read(ptr)))
      _ = untimedLoad.isCompleted should be (false)
      _ = net.readSilencedStores = Set()
      // The pool does not exist, so the shared load ends with whatever says so, never with the timed read's timeout
      outcome <- within(5.seconds, "the untimed pool load")(untimedLoad.transform(t => scala.util.Success(t)))
    yield
      outcome.failed.toOption.collect { case e: ReadTimeout => e } should be (None)
  }

