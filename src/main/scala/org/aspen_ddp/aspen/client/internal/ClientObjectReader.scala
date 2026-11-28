package org.aspen_ddp.aspen.client.internal

import org.aspen_ddp.aspen.client.*
import org.aspen_ddp.aspen.common.objects.{DataObjectPointer, Key, KeyOrdering, KeyValueObjectPointer, ObjectPointer}

import scala.concurrent.Future
import scala.concurrent.duration.{Deadline, Duration, FiniteDuration}

/** A view of the client's reads: every read is the client's (BaseAspenClient's read bodies), with this view's time
  * limit and, when `settled`, settled (ReadManager.read). AspenClient.withReadTimeout returns one bounding each read
  * by a timeout, settledReader an untimed settled one, and settledReaderBy a settled one whose reads all end by one
  * deadline
  */
final class ClientObjectReader(val client: BaseAspenClient,
                               val limit: ClientObjectReader.Limit,
                               val settled: Boolean) extends ObjectReader:
  import ClientObjectReader.Limit

  /** Runs `read` with the timeout of a read starting now. Past the deadline the read fails at once with ReadTimeout */
  private def timed[T](pointer: ObjectPointer)(read: Duration => Future[T]): Future[T] =
    limit match
      case Limit.Untimed => read(Duration.Inf)
      case Limit.Each(timeout) => read(timeout)
      case Limit.By(deadline) =>
        val left = deadline.timeLeft
        if left > Duration.Zero then read(left) else Future.failed(new ReadTimeout(pointer))

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] =
    timed(pointer)(t => client.read(pointer, comment, t, settled))

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] =
    timed(pointer)(t => client.read(pointer, comment, t, settled))

  def readKeys(pointer: KeyValueObjectPointer, keys: List[Key], comment: String): Future[KeyValueObjectState] =
    timed(pointer)(t => client.readKeys(pointer, keys, comment, t, settled))

  def readKeyRange(pointer: KeyValueObjectPointer, startKey: Key, endKey: Key, ordering: KeyOrdering,
                   comment: String): Future[KeyValueObjectState] =
    timed(pointer)(t => client.readKeyRange(pointer, startKey, endKey, ordering, comment, t, settled))

  def readMetadata(pointer: ObjectPointer, comment: String): Future[MetadataObjectState] =
    timed(pointer)(t => client.readMetadata(pointer, comment, t, settled))

  def readRange(pointer: DataObjectPointer, offset: Int, length: Option[Int],
                comment: String): Future[DataObjectRangeState] =
    timed(pointer)(t => client.readRange(pointer, offset, length, comment, t, settled))

  def readRanges(pointer: DataObjectPointer, ranges: List[(Int, Int)],
                 comment: String): Future[List[DataObjectRangeState]] =
    timed(pointer)(t => client.readRanges(pointer, ranges, comment, t, settled))

  def readRangedWriteState(pointer: DataObjectPointer, writes: List[RangedWrite],
                           comment: String): Future[RangedWriteState] =
    timed(pointer)(t => client.readRangedWriteState(pointer, writes, comment, t, settled))

  def readAppendPosition(pointer: DataObjectPointer, comment: String): Future[AppendPosition] =
    timed(pointer)(t => client.readAppendPosition(pointer, comment, t, settled))

object ClientObjectReader:

  /** How long a read of the view may take */
  enum Limit:
    /** No limit (Duration.Inf) */
    case Untimed
    /** Each read gives up after `timeout` */
    case Each(timeout: FiniteDuration)
    /** Each read gives up at `deadline`; one starting past it fails at once */
    case By(deadline: Deadline)
