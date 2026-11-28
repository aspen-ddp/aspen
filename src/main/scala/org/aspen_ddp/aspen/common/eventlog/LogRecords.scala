package org.aspen_ddp.aspen.common.eventlog

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.github.luben.zstd.{Zstd, ZstdInputStream}
import com.google.protobuf.{ByteString, CodedOutputStream}

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.HLCTimestamp

/** A record held in a log object. typeId is the index of the entry's type UUID in the log's type table. */
final case class LoggedRecord(timestamp: HLCTimestamp, typeId: Short, data: Array[Byte]):

  override def equals(other: Any): Boolean = other match
    case o: LoggedRecord =>
      timestamp == o.timestamp && typeId == o.typeId && java.util.Arrays.equals(data, o.data)
    case _ => false

  override def hashCode(): Int =
    (timestamp.asLong.hashCode * 31 + typeId) * 31 + java.util.Arrays.hashCode(data)

object LogRecords:

  private def toProto(r: LoggedRecord): codec.LoggedRecord =
    codec.LoggedRecord(r.timestamp.asLong, r.typeId & 0xFFFF, ByteString.copyFrom(r.data))

  /** One zstd frame over the length-delimited records */
  def encodeFrame(records: Seq[LoggedRecord]): Array[Byte] =
    val raw = new ByteArrayOutputStream()
    records.foreach(r => toProto(r).writeDelimitedTo(raw))
    Zstd.compress(raw.toByteArray)

  /** Every record of every concatenated frame, in order. Empty input yields no records. */
  def decodeFrames(bytes: Array[Byte]): Vector[LoggedRecord] =
    if bytes.isEmpty then
      Vector.empty
    else
      val in = new ZstdInputStream(new ByteArrayInputStream(bytes))
      try
        Iterator.continually(codec.LoggedRecord.parseDelimitedFrom(in))
          .takeWhile(_.isDefined)
          .map(_.get)
          .map(p => LoggedRecord(HLCTimestamp(p.timestamp), p.typeId.toShort, p.data.toByteArray))
          .toVector
      finally
        in.close()

  /** Upper bound on the size of a frame holding rawSize bytes of encoded records */
  def maxFrameSize(rawSize: Int): Int = Zstd.compressBound(rawSize).toInt

  /** The record's length-delimited encoded size (its contribution to a frame's raw size) */
  def recordSize(r: LoggedRecord): Int =
    val n = toProto(r).serializedSize
    CodedOutputStream.computeUInt32SizeNoTag(n) + n
