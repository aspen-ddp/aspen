package org.aspen_ddp.aspen.examples

import org.aspen_ddp.aspen.IntegrationTestSuite
import org.aspen_ddp.aspen.client.{KeyValueObjectState, ReadTimeout}
import org.aspen_ddp.aspen.common.objects.KeyValueObjectPointer

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*

/** The timed-reads example of UsageGuide.md ("Timed reads"), run as a normal test so the guide can never drift from
  * a working API. `readIfAvailable` is reproduced in the guide verbatim.
  */
class TimedReadsSuite extends IntegrationTestSuite:

  atest("timed reads give up instead of waiting for unreachable stores") {

    // ---- UsageGuide.md: Timed reads (begin) ----
    given ExecutionContext = client.clientContext

    // Reads through `patient` give up after half a second rather than waiting for the stores to come back
    val patient = client.withReadTimeout(500.millis)

    def readIfAvailable(ptr: KeyValueObjectPointer): Future[Option[KeyValueObjectState]] =
      patient.read(ptr).map(Some(_)).recover:
        case _: ReadTimeout => None // no answer in time; the object may well exist
    // ---- UsageGuide.md: Timed reads (end) ----

    val everyStore = net.stores.map(_.storeId).toSet
    for
      reachable <- readIfAvailable(radicle)
      unreachable <- net.withReadSilenced(everyStore)(readIfAvailable(radicle))
    yield
      reachable.map(_.pointer) should be (Some(radicle))
      unreachable should be (None)
  }
