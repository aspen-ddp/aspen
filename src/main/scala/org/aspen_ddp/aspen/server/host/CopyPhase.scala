package org.aspen_ddp.aspen.server.host

import scala.util.control.NonFatal

/** The phase of one copy of a store, read from the marker files of its directory. */
enum Phase:
  /** No marker: a copy that can be loaded and served */
  case Live
  /** In arrived/: transferred in and awaiting its pool update; carries the transfer's id (phase-param.arrived,
   *  trimmed) */
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

/** The phase of a flat copy. A copy in a phase directory has its phase by location (CopyLayout.phaseAt). No marker
 *  is written or read any more (stage 2, Task 5): the flat path is Live, its only remaining phase. Task 6 moves Live
 *  into live/ and deletes this reader */
object CopyPhase:

  /** The phase of the flat copy at `storePath`: Live */
  def read(storePath: os.Path): Phase = Phase.Live

  /** The phase of the copy at `storePath`, None where `read` throws, for a caller that waits for a later pass */
  def known(storePath: os.Path): Option[Phase] =
    try Some(read(storePath))
    catch case NonFatal(_) => None
