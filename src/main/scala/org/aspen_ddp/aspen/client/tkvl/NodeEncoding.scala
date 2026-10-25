package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.Key

object SplitPlan:

  /** Chooses the keys that move to the new right node when a full node splits.
   *
   *  `sizes` holds every key of the node, including the pending insert, with its encoded size, ascending by
   *  key ordering. Keys are taken from the top until the moved size reaches half the total. At least the last
   *  key always moves. The result is ascending, so its head is the new node's minimum.
   */
  def keysToMove(sizes: List[(Key, Int)]): List[Key] =
    val halfSize = sizes.foldLeft(0)((sz, t) => sz + t._2) / 2

    def rmove(rsizes: List[(Key, Int)], moveList: List[Key], moveSize: Int): List[Key] =
      rsizes match
        case Nil => moveList
        case (k, sz) :: rest =>
          if moveSize + sz >= halfSize then
            moveList
          else
            rmove(rest, k :: moveList, moveSize + sz)

    val rsizes = sizes.reverse
    rmove(rsizes.tail, rsizes.head._1 :: Nil, rsizes.head._2) // Ensure at least 1 move
