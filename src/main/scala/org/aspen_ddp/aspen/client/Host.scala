package org.aspen_ddp.aspen.client

import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.network.Codec

import java.util.UUID

final case class HostId(uuid: UUID) extends AnyVal


object Host:
  def apply(buff: Array[Byte]): Host = Codec.decode(codec.Host.parseFrom(buff))


case class Host(hostId: HostId,
                name: String,
                address: String,
                dataPort: Int,
                cncPort: Int,
                storeTransferPort: Int):
  
  def encode(): Array[Byte] = Codec.encode(this).toByteArray