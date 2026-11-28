package org.aspen_ddp.aspen.common.eventlog

import org.aspen_ddp.aspen.codec

/** Payload types carried in the `encodedContent` of a ServiceMessage addressed to the
 *  EventLogDurableService. Best-effort wake-ups; correctness comes from polling. */
sealed trait EventLogMessage

/** A client committed an entry to the incoming tree: poll now instead of waiting for the period. */
case object Nudge extends EventLogMessage

/** The observer list in the state object changed: reload it. */
case object ReloadObservers extends EventLogMessage

object EventLogMessage:

  def encode(m: EventLogMessage): Array[Byte] =
    val proto = m match
      case Nudge =>
        codec.EventLogMessage(codec.EventLogMessage.Msg.Nudge(codec.EventLogNudge()))
      case ReloadObservers =>
        codec.EventLogMessage(codec.EventLogMessage.Msg.ReloadObservers(codec.EventLogReloadObservers()))
    proto.toByteArray

  def decode(bytes: Array[Byte]): EventLogMessage =
    codec.EventLogMessage.parseFrom(bytes).msg match
      case codec.EventLogMessage.Msg.Nudge(_) => Nudge
      case codec.EventLogMessage.Msg.ReloadObservers(_) => ReloadObservers
      case codec.EventLogMessage.Msg.Empty =>
        throw new IllegalArgumentException("Empty EventLogMessage")
