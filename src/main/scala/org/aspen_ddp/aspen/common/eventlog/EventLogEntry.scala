package org.aspen_ddp.aspen.common.eventlog

import java.util.UUID

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.Codec

import com.google.protobuf.ByteString

/** One entry of the system event log: a timestamped, typed, opaque payload. */
final case class EventLogEntry(timestamp: HLCTimestamp, typeUUID: UUID, data: Array[Byte]):

  def encode(): Array[Byte] =
    codec.EventLogEntry(
      timestamp = timestamp.asLong,
      typeUuid = Some(Codec.encodeUUID(typeUUID)),
      data = ByteString.copyFrom(data)).toByteArray

  override def equals(other: Any): Boolean = other match
    case o: EventLogEntry =>
      timestamp == o.timestamp && typeUUID == o.typeUUID && java.util.Arrays.equals(data, o.data)
    case _ => false

  override def hashCode(): Int =
    (timestamp.asLong.hashCode * 31 + typeUUID.hashCode) * 31 + java.util.Arrays.hashCode(data)

object EventLogEntry:

  def decode(b: Array[Byte]): EventLogEntry =
    val m = codec.EventLogEntry.parseFrom(b)
    EventLogEntry(HLCTimestamp(m.timestamp), Codec.decodeUUID(m.typeUuid.get), m.data.toByteArray)
