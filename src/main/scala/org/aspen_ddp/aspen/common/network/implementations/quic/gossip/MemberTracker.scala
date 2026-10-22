package org.aspen_ddp.aspen.common.network.implementations.quic.gossip

import org.aspen_ddp.aspen.common.metadata.HostId

import scala.collection.mutable

/** Host-level transitions derived from scalecube membership changes. */
enum HostEvent:
  case Online(hostId: HostId)
  case Offline(hostId: HostId)
  case Restarted(hostId: HostId)

/** Turns scalecube member added/removed notifications into per-host HostEvents.
  *
  * A host may run several scalecube members at once during a restart, so the live member ids are
  * tracked per host. The last member id ever seen for each host is remembered for the life of the
  * process, which lets a new member id after a full outage be reported as a restart.
  *
  * Not thread-safe. The caller must serialise access.
  */
final class MemberTracker:
  private val liveMembers = mutable.Map[HostId, mutable.Set[String]]()
  private val lastMember = mutable.Map[HostId, String]()

  /** Records a member joining and returns the resulting host events. */
  def added(hostId: HostId, memberId: String): List[HostEvent] =
    val live = liveMembers.getOrElseUpdate(hostId, mutable.Set[String]())
    if live.contains(memberId) then
      Nil
    else if live.nonEmpty then
      live += memberId
      lastMember(hostId) = memberId
      List(HostEvent.Restarted(hostId))
    else
      val previous = lastMember.get(hostId)
      live += memberId
      lastMember(hostId) = memberId
      previous match
        case Some(prev) if prev != memberId => List(HostEvent.Online(hostId), HostEvent.Restarted(hostId))
        case _ => List(HostEvent.Online(hostId))

  /** Records a member leaving and returns the resulting host events. */
  def removed(hostId: HostId, memberId: String): List[HostEvent] =
    liveMembers.get(hostId) match
      case Some(live) if live.contains(memberId) =>
        live -= memberId
        if live.isEmpty then
          liveMembers -= hostId
          List(HostEvent.Offline(hostId))
        else
          Nil
      case _ => Nil

  def isOnline(hostId: HostId): Boolean = liveMembers.contains(hostId)

  def onlineHosts: Set[HostId] = liveMembers.keySet.toSet
