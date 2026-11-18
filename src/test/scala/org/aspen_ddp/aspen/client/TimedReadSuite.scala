package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.common.Radicle
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, ObjectId}
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
