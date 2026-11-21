package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.common.transaction.TransactionHorizon
import org.aspen_ddp.aspen.server.{DurableFiles, FileStat}
import scribe.Logging

import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

/** The host's join ceiling: a durable upper bound, in milliseconds of the host's wall clock, on the start of every
  * transaction any of its stores has joined (paxos-transactions 10.10). A store joins a
  * transaction it does not hold only if it started below the ceiling already persisted (`mayJoin`,
  * Frontend.startsTooFarAhead), and the host keeps the ceiling ahead of every start a store may join (now plus
  * MaxStartLead) by renewing it in the background (`renew`, every RenewPeriod, to now plus MaxStartLead plus Step). It
  * only ever rises.
  *
  * What it buys: a store loaded again on this host never joined a transaction started at or above the ceiling its
  * previous life ran under, so its forget floor need not reach past that ceiling (`boundAtLoad`, Frontend.loadFloorMillis).
  * Without it the floor is the load plus MaxStartLead, so a store refuses every transaction started during its downtime
  * and in the lead after it, and too many such stores end those transactions Unknown. With it, after a restart longer
  * than MaxStartLead plus Step, the store joins transactions started since its host went down plus about that much.
  *
  * Safe because every join is below a persisted value, and values only rise: the ceiling the last process left behind
  * (`atStart`) bounds every join of every store in that and every earlier process on this host, and the ceiling now
  * bounds every join so far in this one. A store that arrived from another host is bounded by its arrival margin as
  * before (Frontend.arrivedAt). No ceiling persisted (a new host, a file that cannot be read, one this process
  * abandoned) bounds nothing, and the load floor applies alone. A renewal that falls behind mutes new transactions
  * (a liveness cost only, as for a client clock running fast); one that fails abandons the ceiling: its file is removed
  * and nothing is gated for the rest of the process. Should the removal fail too, the stale ceiling keeps gating.
  *
  * Thread safety: `renew` runs on a background thread, `boundAtLoad` on the control thread, `mayJoin` on store
  * executors and the repair scan. The ceiling is a volatile read; the rest is synchronized.
  */
final class JoinCeiling(persistence: Option[JoinCeiling.Persistence],
                        step: FiniteDuration = JoinCeiling.Step,
                        wallClockMillis: () => Long = () => System.currentTimeMillis()) extends Logging:

  /** The ceiling the host's previous process persisted: no store it served joined a transaction started at or above it */
  val atStart: Option[Long] = persistence.flatMap(p => Try(p.read()).toOption.flatten)

  /** The ceiling persisted last. None: none, so nothing is gated (and nothing bounded) */
  @volatile private var durable: Option[Long] = atStart

  /** A renewal failed and the ceiling was removed: no more renewals in this process */
  private var abandoned = false

  /** Stores loaded by this process so far (boundAtLoad) */
  private var loaded: Set[StoreId] = Set()

  /** Whether a store may join a transaction started at `startMillis`: it is below the persisted ceiling */
  def mayJoin(startMillis: Long): Boolean = durable.forall(startMillis < _)

  /** The ceiling persisted now, if any */
  def current: Option[Long] = durable

  /** What bounds the starts of the transactions store `storeId` joined on this host before the load now being made:
    * the ceiling the previous process left if this process has not loaded the store before, the ceiling now if it
    * has. None: unbounded. Call once per load, before the store can join anything
    */
  def boundAtLoad(storeId: StoreId): Option[Long] = synchronized:
    if loaded.contains(storeId) then
      durable
    else
      loaded += storeId
      atStart

  /** Persists a ceiling MaxStartLead plus Step ahead of now, unless the one persisted is already later */
  def renew(): Unit = synchronized:
    persistence.foreach: p =>
      if !abandoned then
        val next = math.max(durable.getOrElse(Long.MinValue),
          wallClockMillis() + TransactionHorizon.MaxStartLead.toMillis + step.toMillis)
        if !durable.contains(next) then
          Try(p.write(next)) match
            case Success(_) => durable = Some(next)
            case Failure(err) => abandon(p, err)

  private def abandon(p: JoinCeiling.Persistence, err: Throwable): Unit =
    Try(p.remove()) match
      case Success(_) =>
        abandoned = true
        durable = None
        logger.error(s"The host's join ceiling could not be renewed and was removed: its stores' next loads on this " +
                     s"host refuse what they may have forgotten from their load alone. Error: $err")
      case Failure(removeErr) =>
        logger.error(s"The host's join ceiling could neither be renewed nor removed: its stores take part in no " +
                     s"transaction started past ${durable.getOrElse(0L)} until a renewal succeeds. Errors: $err; " +
                     s"$removeErr")

object JoinCeiling:
  /** How far past the start lead each renewal sets the ceiling. A store loaded again after a downtime longer than this
    * plus MaxStartLead joins new transactions at once. A renewal falling behind by more mutes new transactions */
  val Step: FiniteDuration = 1.second

  /** How often the host renews its ceiling (Host). Well inside Step, so a slow write does not mute anything */
  val RenewPeriod: FiniteDuration = 250.millis

  /** In the host's root directory */
  val FileName = "join-ceiling"

  /** Where the ceiling is kept */
  trait Persistence:
    /** The ceiling persisted, if any. May throw, which counts as none */
    def read(): Option[Long]

    /** Durably replaces the ceiling persisted */
    def write(ceiling: Long): Unit

    /** Durably removes the ceiling persisted, if any */
    def remove(): Unit

  /** A file in `dir`, written with DurableFiles. One that cannot be stat'd or read bounds nothing: no ceiling gives
   *  the higher load floor, so either answer is safe */
  final class FilePersistence(dir: os.Path) extends Persistence:
    private val file = dir / FileName

    def read(): Option[Long] =
      Try(Option.when(FileStat.exists(file))(os.read(file).trim.toLong)).toOption.flatten

    def write(ceiling: Long): Unit = DurableFiles.write(file, ceiling.toString)

    def remove(): Unit = DurableFiles.remove(file)

  /** No ceiling: nothing gated, nothing bounded. For stores and tests with no host */
  val Unbounded: JoinCeiling = new JoinCeiling(None)
