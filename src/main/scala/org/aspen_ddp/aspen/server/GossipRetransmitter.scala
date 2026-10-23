package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.metadata.HostId
import org.aspen_ddp.aspen.common.network.{Gossip, HostEndpointChanged, StoreMoved}
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.*
import scribe.Logging

import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.UUID
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Broadcasts this host's StoreMoved and HostEndpointChanged gossip, then re-broadcasts it every
 *  `Period` until `Lifetime` has passed since it was first sent.
 *
 *  Scalecube gossip reaches only the hosts that are members when it spreads, so a host that was
 *  down for the original broadcast would otherwise keep its stale mapping until something else
 *  invalidated it. Re-sending is cheap because moves and endpoint changes are rare. Broadcasting
 *  a message that is already pending restarts its lifetime.
 *
 *  Pending messages are kept in `stateFile` so the retransmits resume after a restart. The file
 *  lives in the host's cache directory because it is not needed for correctness: losing it, or
 *  failing to write it, only means the retransmits stop early.
 *
 *  ```
 *  messages:
 *    - store-moved: "11111111-1111-1111-1111-111111111111:3"
 *      first-sent: "2026-10-22T15:01:57Z"
 *    - host-endpoint-changed: "22222222-2222-2222-2222-222222222222"
 *      first-sent: "2026-10-22T16:30:00Z"
 *  ```
 *  Values are quoted so YAML cannot read a timestamp as a date or a store id as a number.
 */
class GossipRetransmitter(val stateFile: os.Path,
                          send: Gossip => Unit,
                          now: () => Instant = () => Instant.now(),
                          lifetime: FiniteDuration = GossipRetransmitter.Lifetime) extends Logging:
  import GossipRetransmitter.*

  private var pending: Map[Gossip, Instant] = load(stateFile)

  /** Sends `g` now and schedules it for retransmission */
  def broadcast(g: Gossip): Unit =
    synchronized:
      pending += g -> now()
      save()
    send(g)

  /** Re-sends every pending message still within its lifetime and forgets the rest */
  def retransmit(): Unit =
    val live = synchronized:
      val cutoff = now().minusMillis(lifetime.toMillis)
      val (keep, expired) = pending.partition((_, firstSent) => firstSent.isAfter(cutoff))
      if expired.nonEmpty then
        pending = keep
        save()
      keep.keys.toList
    live.foreach(send)

  /** Each pending message and when it was first sent */
  def pendingMessages: Map[Gossip, Instant] = synchronized(pending)

  private def save(): Unit =
    try
      if pending.isEmpty then
        os.remove(stateFile)
      else
        val tmp = stateFile / os.up / s"${stateFile.last}.tmp"
        os.write.over(tmp, encode(pending), createFolders = true)
        os.move(tmp, stateFile, replaceExisting = true, atomicMove = true)
    catch
      case NonFatal(t) => logger.warn(s"Failed to save pending gossip retransmits to $stateFile: $t")


object GossipRetransmitter extends Logging:

  val StateFilename = "retransmitting-gossip-messages.yaml"

  /** How often pending gossip is re-sent */
  val Period: FiniteDuration = 30.minutes

  /** How long after its first broadcast a message stops being re-sent */
  val Lifetime: FiniteDuration = 24.hours

  /** `<host-dir>/cache/retransmitting-gossip-messages.yaml` */
  def stateFile(hostDir: java.nio.file.Path): os.Path =
    os.Path(hostDir.toAbsolutePath) / "cache" / StateFilename

  private object YInstant extends Format[Instant]:
    override def format(o: Object): Instant = o match
      case v: java.lang.String =>
        try Instant.parse(v)
        catch case e: DateTimeParseException => throw new FormatError(s"Invalid timestamp: $e")
      case _ => throw new FormatError("String Required")

  private object YEntry extends YObject[(Gossip, Instant)]:
    val storeMoved: Optional[StoreId] = Optional("store-moved", StoreId.YStoreId)
    val hostEndpointChanged: Optional[UUID] = Optional("host-endpoint-changed", YUUID)
    val firstSent: Required[Instant] = Required("first-sent", YInstant)

    val attrs: List[Attr] = storeMoved :: hostEndpointChanged :: firstSent :: Nil

    def create(o: Object): (Gossip, Instant) =
      val g = (storeMoved.get(o), hostEndpointChanged.get(o)) match
        case (Some(storeId), None) => StoreMoved(storeId)
        case (None, Some(hostId)) => HostEndpointChanged(HostId(hostId))
        case _ => throw new FormatError("Exactly one of store-moved or host-endpoint-changed is required")
      g -> firstSent.get(o)

  private object YState extends YObject[Map[Gossip, Instant]]:
    val messages: Required[List[(Gossip, Instant)]] = Required("messages", YList(YEntry))

    val attrs: List[Attr] = messages :: Nil

    def create(o: Object): Map[Gossip, Instant] = messages.get(o).toMap

  private[server] def encode(pending: Map[Gossip, Instant]): String =
    val entries = pending.toList.sortBy(_._2).map: (g, firstSent) =>
      val id = g match
        case StoreMoved(storeId) => s"store-moved: ${yamlQuote(storeId.toString)}"
        case HostEndpointChanged(hostId) => s"host-endpoint-changed: ${yamlQuote(hostId.uuid.toString)}"
      s"  - $id\n    first-sent: ${yamlQuote(firstSent.toString)}\n"
    "messages:\n" + entries.mkString

  /** The pending messages saved in `file`, or none if there is no file or it cannot be read */
  private[server] def load(file: os.Path): Map[Gossip, Instant] =
    if !os.exists(file) then
      Map()
    else
      try
        YState.format(loadYamlFile(file.toIO))
      catch
        case NonFatal(t) =>
          logger.warn(s"Discarding unreadable gossip retransmit state $file: $t")
          Map()
