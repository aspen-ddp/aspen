package org.aspen_ddp.aspen.server.crl.simple

case class StreamId(number: Int) extends AnyVal

object StreamId:
  /** The stream written after `s` in `ring` (SimpleCRL.Sizing.ring), which lists `s` */
  def successor(ring: Vector[StreamId], s: StreamId): StreamId = ring((ring.indexOf(s) + 1) % ring.size)
