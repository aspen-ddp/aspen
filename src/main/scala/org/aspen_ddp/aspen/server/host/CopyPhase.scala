package org.aspen_ddp.aspen.server.host

/** The phase of one copy of a store: where it is on its device (CopyLayout). Every copy is in a phase directory, its
 *  phase by location (stage 2). */
enum Phase:
  /** In live/: a copy that can be loaded and served */
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
