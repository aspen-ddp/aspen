package org.aspen_ddp.aspen.server.host

import scala.util.control.NonFatal

/** The phase of one copy of a store, read from the marker files of its directory. */
enum Phase:
  /** No marker: a copy that can be loaded and served */
  case Live
  /** Arrived from another host (transferred-in) and awaiting its pool update; carries the transfer's id, trimmed */
  case Arrived(transferId: String)
  /** In outbound/: closed for a transfer out, its state in crl_save.log; carries the transfer id (phase-param.outbound,
   *  trimmed) */
  case Outbound(transferId: String)
  /** In rebuilt/: a finished rebuild awaiting its entry's flip out of Rebuilding; carries the episode
   *  (phase-param.rebuilt, trimmed; "" for none: compare as RebuildingStore.isRebuiltFor does) */
  case Rebuilt(episode: String)
  /** In parked/: a complete copy whose entry is Offline (an AdoptOffline rebuild's, StoreRebuilds.keepRebuiltStoreOffline, T4) */
  case Parked
  /** In lost/: its CRL state is gone or unusable, and it awaits its retire (T10, T11) */
  case Lost
  /** In pending-delete/: a move target only (CopyMoves, from Lost). Never recorded once its move is durable */
  case Retired
  /** Stage 1 only: never returned by `CopyPhase.read`. The registry's record of a copy whose read threw */
  case Unread

/** The one reader of a flat store directory's markers. A copy in a phase
 *  directory has its phase by location (CopyLayout.phaseAt), never by markers. */
object CopyPhase:
  import StoreDirMarkers.Marker.*

  /** The phase of the flat copy at `storePath`: Arrived with its transferred-in marker's content, else Live. Throws a
   *  failed stat or read: a stat that fails is no absence. */
  def read(storePath: os.Path): Phase =
    if StoreDirMarkers.has(storePath, TransferredIn) then
      Phase.Arrived(os.read(storePath / TransferredIn.fileName).trim)
    else
      Phase.Live

  /** The phase of the copy at `storePath`, None where `read` throws, for a caller that waits for a later pass */
  def known(storePath: os.Path): Option[Phase] =
    try Some(read(storePath))
    catch case NonFatal(_) => None
