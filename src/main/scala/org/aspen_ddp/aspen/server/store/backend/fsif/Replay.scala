package org.aspen_ddp.aspen.server.store.backend.fsif

/** Journal replay is a simulation followed by a single selection pass, never "apply each record as you go".
  * Pure logic: no I/O. Callers write the returned Deltas, in order, to the data file.
  */
object Replay:
  final class InvalidRecord(msg: String) extends Exception(msg)

  final case class Result(index: IndexMap, apply: Vector[JournalRecord.Delta])

  private def checkExt(ext: Int, pos: Int, n: Int, kind: String): Unit =
    if ext < 0 || ext >= n then
      throw new InvalidRecord(s"$kind at journal position $pos has ext $ext outside [0, $n)")

  /** Walks the records over a copy of `start` (which is never mutated) and returns the final index plus the Deltas to
    * apply, in journal order.
    *
    * A Delta(X, i) at position p is kept iff `finalSlot(X) == i` and `p > lastMappingPos(X)`, where lastMappingPos(X)
    * is the position of the last Mapping record for X in any slot (-1 if none).
    *
    * This equals the spec's per-slot formulation ("the Delta comes after the Map that installed X in i, or X was
    * already in i in the index map and no later Map for i exists"): any Mapping for X, including a reinstall into
    * the same slot with a new id or a move, starts a new incarnation of X, so only Deltas after X's last Mapping can
    * belong to the final occupancy. When that last Mapping put X into its final slot i, it is the installer. When
    * no Mapping for X exists, X is in i from `start`, which is position -1. A last Mapping to some other slot makes
    * finalSlot(X) differ from the Delta's slot, or the Delta precedes it, so it is dropped. Deltas with a negative slot, or whose ext ends unmapped, are dropped. Slots have no upper bound (copy-on-write and slot
    * freeing can place an object at or beyond segmentCount); recovery validates the index separately, including
    * double-mapped slots. An ext outside [0, segmentCount) in any record can only come from a writer bug or
    * corruption past the CRC, so it throws InvalidRecord.
    */
  def run(start: IndexMap, records: Iterable[JournalRecord]): Result =
    val index = start.copy()
    val n = index.slots.length
    val recs = records.toIndexedSeq
    val lastMapping = new java.util.HashMap[Int, Int]

    // Pass 1: maintain the mapping and the position of each ext's last Mapping
    var pos = 0
    recs.foreach: r =>
      r match
        case JournalRecord.Mapping(ext, slot, id) =>
          checkExt(ext, pos, n, "Mapping")
          index.slots(ext) = slot
          index.ids(ext) = id
          lastMapping.put(ext, pos)
        case JournalRecord.Delta(ext, _, _, _) =>
          checkExt(ext, pos, n, "Delta")
      pos += 1

    // Pass 2: keep the Deltas belonging to each ext's final occupancy
    val out = Vector.newBuilder[JournalRecord.Delta]
    pos = 0
    recs.foreach: r =>
      r match
        case d: JournalRecord.Delta =>
          if d.slot >= 0 && index.slots(d.ext) == d.slot &&
            pos > lastMapping.getOrDefault(d.ext, -1)
          then out += d
        case _ =>
      pos += 1

    Result(index, out.result())
