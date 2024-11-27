package org.aspen_ddp.aspen.server.crl.simple

case class StreamId(number: Int) extends AnyVal

object StreamId:
  // 1 Byte file index number
  val StaticSize: Int = 1
