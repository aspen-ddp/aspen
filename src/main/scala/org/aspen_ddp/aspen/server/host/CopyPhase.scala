package org.aspen_ddp.aspen.server.host

import scala.util.control.NonFatal

/** The phase of one copy of a store, read from the marker files of its directory. */
enum Phase:
  /** No marker: a copy that can be loaded and served */
  case Live
  /** Arrived from another host (transferred-in) and awaiting its pool update; carries the transfer's id, trimmed */
  case Arrived(transferId: String)
  /** Transferring out; carries the marker's trimmed content verbatim, "" when empty */
  case Outbound(transferId: String)
  /** A finished rebuild (rebuild-complete); carries the marker's trimmed content verbatim, "" when empty (compare as RebuildingStore.isRebuildCompleteFor does) */
  case Rebuilt(episode: String)
  /** In lost/: its CRL state is gone or unusable, and it awaits its retire (T10, T11) */
  case Lost
  /** In pending-delete/: a move target only (CopyMoves, from Lost). Never recorded once its move is durable */
  case Retired
  /** Stage 1 only: never returned by `CopyPhase.read`. The registry's record of a copy whose read threw */
  case Unread

/** The one reader of a flat store directory's markers, and the one place that ranks them. A copy in a phase
 *  directory has its phase by location (CopyLayout.phaseAt), never by markers. */
object CopyPhase:
  import StoreDirMarkers.Marker.*

  /** The phase of the flat copy at `storePath`. Precedence is TO > TI > RC, stopping at the first marker present, so
   *  the content of only that marker is read. Throws a failed stat or read: a stat that fails is no absence. */
  def read(storePath: os.Path): Phase =
    def content(marker: StoreDirMarkers.Marker): String = os.read(storePath / marker.fileName).trim
    if StoreDirMarkers.has(storePath, TransferOut) then
      Phase.Outbound(content(TransferOut))
    else if StoreDirMarkers.has(storePath, TransferredIn) then
      Phase.Arrived(content(TransferredIn))
    else if StoreDirMarkers.has(storePath, RebuildComplete) then
      Phase.Rebuilt(content(RebuildComplete))
    else
      Phase.Live

  /** The phase of the copy at `storePath`, None where `read` throws, for a caller that waits for a later pass */
  def known(storePath: os.Path): Option[Phase] =
    try Some(read(storePath))
    catch case NonFatal(_) => None
