package org.aspen_ddp.aspen.server.repair

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.codec
import org.aspen_ddp.aspen.common.metadata.{HostId, HostState}

import scala.concurrent.{ExecutionContext, Future}

/** Host-scoped repair load ceiling.
  *
  * The ceiling belongs to the host rather than the pool because it is a property of the machine:
  * a host may hold thousands of stores across many pools, and what bounds the damage a sweep can
  * do is how many of those it scans at once.
  */
case class HostRepairLimits(maxConcurrentStoreScans: Int):

  def encode(): Array[Byte] =
    codec.HostRepairLimits(maxConcurrentStoreScans = maxConcurrentStoreScans).toByteArray

object HostRepairLimits:

  val Default: HostRepairLimits = HostRepairLimits(maxConcurrentStoreScans = 4)

  /** A non-positive value is treated as unset. runBoundedParallel requires a positive bound, and
    * a zero read from a never-written proto3 field must not be able to stop repair entirely.
    */
  def decode(bytes: Array[Byte]): HostRepairLimits =
    val m = codec.HostRepairLimits.parseFrom(bytes)
    if m.maxConcurrentStoreScans <= 0 then Default
    else HostRepairLimits(m.maxConcurrentStoreScans)

  def read(client: AspenClient, hostId: HostId)(using ec: ExecutionContext): Future[HostRepairLimits] =
    for
      ptr <- client.getHostPointer(hostId)
      kvos <- client.read(ptr, "repair limits")
    yield
      kvos.contents.get(HostState.RepairLimitsKey) match
        case Some(vs) => decode(vs.value.bytes)
        case None => Default
