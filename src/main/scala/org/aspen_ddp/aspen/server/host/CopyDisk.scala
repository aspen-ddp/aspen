package org.aspen_ddp.aspen.server.host

import org.aspen_ddp.aspen.server.{DurableFiles, FileStat}

import scala.util.control.NonFatal

/** Copy moves' disk steps, as plain functions of paths: run on a device's management lane (or, until their callers are
 *  converted, by CopyMoves on the control thread). They touch no control state; the caller records what they report. */
object CopyDisk:
  /** The steps of a move from `from` to `target`, into phase `to`: the target's directory made if absent, `to`'s own
   *  parameter file placed durably (the source phase's file is never touched, so a failed move leaves the source
   *  reading what it read), then the rename forced, through `syncDirectory` and `rename` (CopyMoves' seams). A move onto
   *  the copy's own path is no move: NotMoved. An Unsynced result leaves `target`'s and `from`'s parent directories to
   *  force again (CopyMoves.unforced) */
  def relocate(from: os.Path, target: os.Path, to: Phase,
               syncDirectory: os.Path => Unit, rename: (os.Path, os.Path) => Unit): MoveResult =
    if from == target then
      MoveResult.NotMoved(IllegalArgumentException(s"$to is the path $from, where the copy already is"))
    else
      val prepared =
        try
          val dir = target / os.up
          if !FileStat.exists(dir) then
            os.makeDir.all(dir)
            syncDirectory(dir / os.up)
          for
            file <- CopyLayout.paramFile(to)
            p <- CopyLayout.param(to)
          do DurableFiles.write(from / file, p)
          None
        catch case NonFatal(t) => Some(MoveResult.NotMoved(t))
      prepared.getOrElse:
        try
          DurableFiles.move(from, target, syncDirectory, rename)
          MoveResult.Durable
        catch
          case t: DurableFiles.MoveNotForced => MoveResult.Unsynced(t)
          case NonFatal(t) => MoveResult.NotMoved(t)
