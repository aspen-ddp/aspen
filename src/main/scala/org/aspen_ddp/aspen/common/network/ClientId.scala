package org.aspen_ddp.aspen.common.network

import java.util.UUID

case class ClientId(uuid: UUID) extends AnyVal {
  def toBytes: Array[Byte] = uuid.toString.getBytes
}

object ClientId {
  val Null = new ClientId(new UUID(0, 0))
}
