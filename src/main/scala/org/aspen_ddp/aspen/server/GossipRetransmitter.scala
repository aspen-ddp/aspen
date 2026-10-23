package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.common.network.{Codec, Gossip, HostEndpointChanged, StoreMoved, StoreOffline}
import org.aspen_ddp.aspen.common.util.YamlFormat.*
import scribe.Logging

import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.Base64
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Broadcasts this host's StoreMoved, StoreOffline and HostEndpointChanged gossip, then re-broadcasts it every
 *  `Period` until `Lifetime` has passed since it was first sent.
 *
 *  Scalecube gossip reaches only the hosts that are members when it spreads, so a host that was
 *  down for the original broadcast would otherwise keep its stale mapping until something else
 *  invalidated it. Re-sending is cheap because moves and endpoint changes are rare, and receivers
 *  ignore a message no newer than what they hold. A message replaces any pending one about the
 *  same store or host, and starts a fresh lifetime: the older one is obsolete.
 *
 *  Pending messages are kept in `stateFile` so the retransmits resume after a restart. The file
 *  lives in the host's cache directory because it is not needed for correctness: losing it, or
 *  failing to write it, only means the retransmits stop early.
 *
 *  Each message is stored as its base64-encoded protobuf gossip encoding:
 *  ```
 *  messages:
 *    - gossip: "<base64 of Codec.encodeGossip>"
 *      first-sent: "2026-10-22T15:01:57Z"
 *  ```
 *  Values are quoted so YAML cannot read a timestamp as a date.
 */
class GossipRetransmitter(val stateFile: os.Path,
                          send: Gossip => Unit,
                          now: () => Instant = () => Instant.now(),
                          lifetime: FiniteDuration = GossipRetransmitter.Lifetime) extends Logging:
  import GossipRetransmitter.*

  private var pending: Map[Gossip, Instant] = load(stateFile)

  /** Sends `g` now and schedules it for retransmission in place of any pending message about the
   *  same store or host */
  def broadcast(g: Gossip): Unit =
    synchronized:
      pending = pending.filter((p, _) => subject(p) != subject(g)) + (g -> now())
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
  def stateFile(hostDir: os.Path): os.Path =
    hostDir / "cache" / StateFilename

  private object YInstant extends Format[Instant]:
    override def format(o: Object): Instant = o match
      case v: java.lang.String =>
        try Instant.parse(v)
        catch case e: DateTimeParseException => throw new FormatError(s"Invalid timestamp: $e")
      case _ => throw new FormatError("String Required")

  /** What a message is about. A newer message about the same subject supersedes an older one: a store coming back
   *  online supersedes its going offline, and the reverse. */
  private def subject(g: Gossip): Any = g match
    case m: StoreMoved => m.storeId
    case m: StoreOffline => m.storeId
    case m: HostEndpointChanged => m.hostId

  private object YGossip extends Format[Gossip]:
    override def format(o: Object): Gossip = o match
      case v: java.lang.String =>
        val bytes =
          try Base64.getDecoder.decode(v)
          catch case e: IllegalArgumentException => throw new FormatError(s"Invalid base64: $e")
        Codec.decodeGossip(bytes) match
          case Right(g) => g
          case Left(err) => throw new FormatError(err)
      case _ => throw new FormatError("String Required")

  private object YEntry extends YObject[(Gossip, Instant)]:
    val gossip: Required[Gossip] = Required("gossip", YGossip)
    val firstSent: Required[Instant] = Required("first-sent", YInstant)

    val attrs: List[Attr] = gossip :: firstSent :: Nil

    def create(o: Object): (Gossip, Instant) = gossip.get(o) -> firstSent.get(o)

  private object YState extends YObject[Map[Gossip, Instant]]:
    val messages: Required[List[(Gossip, Instant)]] = Required("messages", YList(YEntry))

    val attrs: List[Attr] = messages :: Nil

    def create(o: Object): Map[Gossip, Instant] = messages.get(o).toMap

  private[server] def encode(pending: Map[Gossip, Instant]): String =
    val entries = pending.toList.sortBy(_._2).map: (g, firstSent) =>
      val encoded = Base64.getEncoder.encodeToString(Codec.encodeGossip(g))
      s"  - gossip: ${yamlQuote(encoded)}\n    first-sent: ${yamlQuote(firstSent.toString)}\n"
    "messages:\n" + entries.mkString

  /** The pending messages saved in `file`, or none if there is no file or it cannot be read */
  private[server] def load(file: os.Path): Map[Gossip, Instant] =
    if !os.exists(file) then
      Map()
    else
      try
        YState.format(loadYamlFile(file))
      catch
        case NonFatal(t) =>
          logger.warn(s"Discarding unreadable gossip retransmit state $file: $t")
          Map()
