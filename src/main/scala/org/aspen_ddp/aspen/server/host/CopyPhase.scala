package org.aspen_ddp.aspen.server.host

import java.util.UUID
import scala.util.Try
import scala.util.control.NonFatal

/** The phase of one copy of a store, read from the marker files of its directory. */
enum Phase:
  /** No marker: a copy that can be loaded and served */
  case Live
  /** Arrived from another host (transferred-in) and awaiting its pool update; carries the transfer's id, trimmed */
  case Arrived(transferId: String)
  /** Transferring out; carries the transfer's id, None for an empty marker */
  case Outbound(transferId: Option[UUID])
  /** A finished rebuild (rebuild-complete); carries its episode, None for an empty marker */
  case Rebuilt(episode: Option[UUID])
  /** Rebuild-required: its CRL state is gone */
  case Lost
  /** Stage 1 only: never returned by `CopyPhase.read`. The registry's record of a copy whose read threw */
  case Unread

/** The one reader of a store directory's markers, and the one place that ranks them. */
object CopyPhase:
  import StoreDirMarkers.Marker.*

  /** The phase of the copy at `storePath`. Precedence is RR > TO > TI > RC, stopping at the first marker present, so
   *  the content of only that marker is read. Throws a failed stat or read: a stat that fails is no absence. */
  def read(storePath: os.Path): Phase =
    def content(marker: StoreDirMarkers.Marker): String = os.read(storePath / marker.fileName).trim
    def uuid(marker: StoreDirMarkers.Marker): Option[UUID] =
      Some(content(marker)).filter(_.nonEmpty).flatMap(c => Try(UUID.fromString(c)).toOption)
    if StoreDirMarkers.has(storePath, RebuildRequired) then
      Phase.Lost
    else if StoreDirMarkers.has(storePath, TransferOut) then
      Phase.Outbound(uuid(TransferOut))
    else if StoreDirMarkers.has(storePath, TransferredIn) then
      Phase.Arrived(content(TransferredIn))
    else if StoreDirMarkers.has(storePath, RebuildComplete) then
      Phase.Rebuilt(uuid(RebuildComplete))
    else
      Phase.Live

  /** The phase of the copy at `storePath`, None where `read` throws, for a caller that waits for a later pass */
  def known(storePath: os.Path): Option[Phase] =
    try Some(read(storePath))
    catch case NonFatal(_) => None
