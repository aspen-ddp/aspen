package org.aspen_ddp.aspen.common.eventlog

import java.util.UUID

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.network.Codec

enum DropReason:
  case ExceededMaximumLogEntrySize

/** Placeholder logged in place of an entry that could not be stored. */
final case class DroppedEvent(timestamp: HLCTimestamp, droppedType: UUID, reason: DropReason) extends Event:
  override def typeUUID: UUID = DroppedEvent.TypeUUID

  override def describe: String = s"type=$droppedType reason=exceeded maximum log entry size"

object DroppedEvent extends EventFactory:
  val TypeUUID: UUID = UUID.fromString("367b01ad-b822-4827-8603-97fb0308abd6")

  val typeUUID: UUID = TypeUUID

  def entryFor(dropped: EventLogEntry, reason: DropReason): EventLogEntry =
    val protoReason = reason match
      case DropReason.ExceededMaximumLogEntrySize => codec.DropReason.DROP_REASON_EXCEEDED_MAXIMUM_LOG_ENTRY_SIZE
    val data = codec.DroppedEventData(
      droppedType = Some(Codec.encodeUUID(dropped.typeUUID)),
      reason = protoReason).toByteArray
    EventLogEntry(dropped.timestamp, TypeUUID, data)

  def createEvent(e: EventLogEntry): DroppedEvent =
    val m = codec.DroppedEventData.parseFrom(e.data)
    val reason = m.reason match
      case codec.DropReason.DROP_REASON_EXCEEDED_MAXIMUM_LOG_ENTRY_SIZE => DropReason.ExceededMaximumLogEntrySize
      case other => throw new IllegalArgumentException(s"Unknown DropReason $other")
    DroppedEvent(e.timestamp, Codec.decodeUUID(m.droppedType.get), reason)
