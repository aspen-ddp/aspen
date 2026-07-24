package org.aspen_ddp.aspen.compute.systemtask

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec

import java.util.UUID

/** Payload types carried in the `encodedContent` of a ServiceMessage addressed to the
 *  SystemTaskExecutorService. Best-effort wake-ups; correctness comes from polling. */
sealed trait SystemTaskMessage

case class NewSystemTaskAdded(taskId: UUID) extends SystemTaskMessage
case class SystemTaskComplete(taskId: UUID) extends SystemTaskMessage

object SystemTaskMessage:

  def encode(m: SystemTaskMessage): Array[Byte] =
    val proto = m match
      case NewSystemTaskAdded(taskId) =>
        codec.SystemTaskMessage(codec.SystemTaskMessage.Msg.NewSystemTaskAdded(
          codec.NewSystemTaskAdded(taskId = Some(Codec.encodeUUID(taskId)))))
      case SystemTaskComplete(taskId) =>
        codec.SystemTaskMessage(codec.SystemTaskMessage.Msg.SystemTaskComplete(
          codec.SystemTaskComplete(taskId = Some(Codec.encodeUUID(taskId)))))
    proto.toByteArray

  def decode(bytes: Array[Byte]): SystemTaskMessage =
    val proto = codec.SystemTaskMessage.parseFrom(bytes)
    proto.msg match
      case codec.SystemTaskMessage.Msg.NewSystemTaskAdded(m) =>
        NewSystemTaskAdded(Codec.decodeUUID(m.taskId.get))
      case codec.SystemTaskMessage.Msg.SystemTaskComplete(m) =>
        SystemTaskComplete(Codec.decodeUUID(m.taskId.get))
      case codec.SystemTaskMessage.Msg.Empty =>
        throw new IllegalArgumentException("Empty SystemTaskMessage")
