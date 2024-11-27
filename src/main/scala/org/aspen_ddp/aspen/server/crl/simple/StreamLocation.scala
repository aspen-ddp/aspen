package org.aspen_ddp.aspen.server.crl.simple

case class StreamLocation(streamId: StreamId, offset: Long, length: Int)

object StreamLocation:
  // 2 byte file id + 8 byte offset + 4 byte length
  val StaticSize: Int = 2 + 8 + 4
  
  val Null = new StreamLocation(StreamId(-1), -1, -1)
