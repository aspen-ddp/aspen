package org.aspen_ddp.aspen.server.crl.simple

case class StreamId(number: Int) extends AnyVal

object StreamId:
  /** The stream written after `s` in `ring` (SimpleCRL.Sizing.ring), which lists `s` */
  def successor(ring: Vector[StreamId], s: StreamId): StreamId =
    val i = ring.indexOf(s)
    require(i >= 0, s"stream ${s.number} is not in the ring ${ring.map(_.number).mkString(",")}")
    ring((i + 1) % ring.size)
