package org.aspen_ddp.aspen.common.network

import org.aspen_ddp.aspen.client.AspenClient
import org.aspen_ddp.aspen.common.HLCTimestamp
import org.aspen_ddp.aspen.common.metadata.StorageDeviceState
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.util.YamlFormat.*
import scribe.Logging

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Whether a store is offline for a transfer or rebuild, and the timestamp of the event that said so */
final case class StoreAvailability(offline: Boolean, timestamp: HLCTimestamp)

/** Tracks which stores are offline for a transfer or rebuild, so they can be skipped when choosing a transaction's
 *  designated leader. Advisory only: a wrong answer costs a poorly chosen leader, never correctness.
 *
 *  Fed by StoreOffline gossip, which marks a store offline, and StoreMoved gossip, which marks it online at its
 *  `onlineTimestamp`. Each store holds only its newest event, so a retransmitted or reordered message older than
 *  what is held loses. That is why online stores are remembered too: the host that took a store offline keeps
 *  retransmitting its StoreOffline for `GossipRetransmitter.Lifetime` after the store came back. An online entry is
 *  forgotten once it is older than `OnlineRetention`, by which time those retransmits have stopped.
 *
 *  Gossip can be missed, so `recheck` re-reads the pool and device state of every store still held offline.
 *
 *  With a `cacheFile` (hosts), the state is saved on every change and restored at construction. It is a cache:
 *  losing it, or failing to write it, only means the state is relearned from gossip and rechecks.
 *  ```
 *  stores:
 *    - store: "<pool uuid>:<index>"
 *      status: "offline"
 *      timestamp: "<HLCTimestamp as a long>"
 *  ```
 */
class StoreAvailabilityTracker(cacheFile: Option[os.Path] = None) extends Logging:
  import StoreAvailabilityTracker.*

  private var stores: Map[StoreId, StoreAvailability] = prune(cacheFile.map(load).getOrElse(Map()))

  def isOffline(storeId: StoreId): Boolean = synchronized:
    stores.get(storeId).exists(_.offline)

  def offlineStores: Set[StoreId] = synchronized:
    stores.collect { case (storeId, a) if a.offline => storeId }.toSet

  /** Every store held, online ones included. Sent to a client when its gossip subscription starts. */
  def snapshot: Map[StoreId, StoreAvailability] = synchronized(stores)

  /** Applies StoreOffline and StoreMoved gossip; other gossip is ignored */
  def gossipReceived(g: Gossip): Unit = g match
    case m: StoreOffline => update(m.storeId, StoreAvailability(offline = true, m.timestamp))
    case m: StoreMoved => update(m.storeId, StoreAvailability(offline = false, m.onlineTimestamp))
    case _: HostEndpointChanged => ()

  /** Applies each entry of a relay's snapshot */
  def merge(snapshot: Map[StoreId, StoreAvailability]): Unit =
    snapshot.foreach(update)

  /** Holds `a` for `storeId` unless what is held is at least as new. Returns whether it was applied. */
  def update(storeId: StoreId, a: StoreAvailability): Boolean = synchronized:
    if stores.get(storeId).exists(_.timestamp >= a.timestamp) then
      false
    else
      stores += storeId -> a
      save()
      true

  /** Marks online each store held offline whose pool names a device listing it Active. A store whose reads fail
   *  stays offline until the next recheck. Online entries past `OnlineRetention` are forgotten.
   */
  def recheck(client: AspenClient): Future[Unit] =
    given ExecutionContext = client.clientContext
    synchronized:
      val pruned = prune(stores)
      if pruned.size != stores.size then
        stores = pruned
        save()
    val checks = offlineStores.toList.map: storeId =>
      // Taken before the reads, so a StoreOffline sent while they are in flight is newer and wins
      val checkedAt = HLCTimestamp.now
      val f = for
        pool <- client.getStoragePoolState(storeId.poolId)
        device <- client.getStorageDeviceState(pool.stores(storeId.poolIndex).storageDeviceId)
      yield
        val active = device.stores.get(storeId).exists(_.status == StorageDeviceState.StoreStatus.Active)
        if !device.isFailed && active then
          if update(storeId, StoreAvailability(offline = false, checkedAt)) then
            logger.info(s"Store $storeId is no longer offline")
      f.recover:
        case NonFatal(e) => logger.warn(s"Failed to recheck whether offline store $storeId is still offline: $e")
    Future.sequence(checks).map(_ => ())

  private def save(): Unit =
    cacheFile.foreach: file =>
      try
        val tmp = file / os.up / s"${file.last}.tmp"
        os.write.over(tmp, encode(stores), createFolders = true)
        os.move(tmp, file, replaceExisting = true, atomicMove = true)
      catch
        case NonFatal(t) => logger.warn(s"Failed to save offline stores to $file: $t")


object StoreAvailabilityTracker extends Logging:

  val CacheFilename = "offline-stores.yaml"

  /** How long a store is remembered as online. Longer than GossipRetransmitter.Lifetime, with an hour's margin. */
  val OnlineRetention: FiniteDuration = 25.hours

  /** How often hosts and long-lived clients run `recheck` */
  val RecheckPeriod: FiniteDuration = 24.hours

  /** `<host-dir>/cache/offline-stores.yaml` */
  def cacheFile(hostDir: java.nio.file.Path): os.Path =
    os.Path(hostDir.toAbsolutePath) / "cache" / CacheFilename

  private def prune(stores: Map[StoreId, StoreAvailability]): Map[StoreId, StoreAvailability] =
    val cutoff = System.currentTimeMillis() - OnlineRetention.toMillis
    stores.filter((_, a) => a.offline || a.timestamp.wallTime >= cutoff)

  private object YStatus extends Format[Boolean]:
    override def format(o: Object): Boolean = o match
      case "offline" => true
      case "online" => false
      case _ => throw new FormatError("\"offline\" or \"online\" required")

  private object YTimestamp extends Format[HLCTimestamp]:
    override def format(o: Object): HLCTimestamp = o match
      case v: java.lang.String =>
        try HLCTimestamp(v.toLong)
        catch case e: NumberFormatException => throw new FormatError(s"Invalid timestamp: $e")
      case _ => throw new FormatError("String Required")

  private object YEntry extends YObject[(StoreId, StoreAvailability)]:
    val store: Required[StoreId] = Required("store", StoreId.YStoreId)
    val status: Required[Boolean] = Required("status", YStatus)
    val timestamp: Required[HLCTimestamp] = Required("timestamp", YTimestamp)

    val attrs: List[Attr] = store :: status :: timestamp :: Nil

    def create(o: Object): (StoreId, StoreAvailability) =
      store.get(o) -> StoreAvailability(status.get(o), timestamp.get(o))

  private object YState extends YObject[Map[StoreId, StoreAvailability]]:
    val stores: Required[List[(StoreId, StoreAvailability)]] = Required("stores", YList(YEntry))

    val attrs: List[Attr] = stores :: Nil

    def create(o: Object): Map[StoreId, StoreAvailability] = stores.get(o).toMap

  private[network] def encode(stores: Map[StoreId, StoreAvailability]): String =
    val entries = stores.toList.sortBy(_._2.timestamp).map: (storeId, a) =>
      val status = if a.offline then "offline" else "online"
      s"  - store: ${yamlQuote(storeId.toString)}\n    status: ${yamlQuote(status)}\n" +
        s"    timestamp: ${yamlQuote(a.timestamp.asLong.toString)}\n"
    if entries.isEmpty then "stores: []\n" else "stores:\n" + entries.mkString

  /** The state saved in `file`, or none if there is no file or it cannot be read */
  private[network] def load(file: os.Path): Map[StoreId, StoreAvailability] =
    if !os.exists(file) then
      Map()
    else
      try
        YState.format(loadYamlFile(file.toIO))
      catch
        case NonFatal(t) =>
          logger.warn(s"Discarding unreadable offline store state $file: $t")
          Map()
