package org.aspen_ddp.aspen.common.eventlog

import java.nio.ByteBuffer

import org.aspen_ddp.aspen.common.HLCTimestamp

/** Header of a full log object: the key it is filed under and the optional exclusive upper bound of its range */
final case class FullObjectHeader(filedKey: HLCTimestamp, upperBound: Option[HLCTimestamp])

/** A full log object is a fixed-size header (1 presence byte, 8 filedKey, 8 upperBound) followed by one frame */
object FullObject:

  /** Header size in bytes. Planners add this to every size check. */
  val overhead: Int = 1 + 8 + 8

  def encode(h: FullObjectHeader, records: Seq[LoggedRecord]): Array[Byte] =
    val frame = LogRecords.encodeFrame(records)
    val bb = ByteBuffer.allocate(overhead + frame.length)
    bb.put(if h.upperBound.isDefined then 1.toByte else 0.toByte)
    bb.putLong(h.filedKey.asLong)
    bb.putLong(h.upperBound.map(_.asLong).getOrElse(0L))
    bb.put(frame)
    bb.array()

  def decode(b: Array[Byte]): (FullObjectHeader, Vector[LoggedRecord]) =
    require(b.length >= overhead, s"Full log object shorter than its header: ${b.length}")
    val bb = ByteBuffer.wrap(b)
    val hasBound = bb.get() != 0
    val filed = HLCTimestamp(bb.getLong)
    val bound = bb.getLong
    val header = FullObjectHeader(filed, if hasBound then Some(HLCTimestamp(bound)) else None)
    (header, LogRecords.decodeFrames(java.util.Arrays.copyOfRange(b, overhead, b.length)))
