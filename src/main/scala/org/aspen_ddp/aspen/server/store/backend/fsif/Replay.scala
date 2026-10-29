package org.aspen_ddp.aspen.server.store.backend.fsif

/** Journal replay is a simulation followed by a single selection pass, never "apply each record as you go".
  * Pure logic: no I/O. Callers write the returned Deltas, in order, to the data file.
  */
object Replay:
  final case class Result(index: IndexMap, apply: Vector[JournalRecord.Delta])

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
    * finalSlot(X) differ from the Delta's slot, or the Delta precedes it, so it is dropped. Deltas whose ext or slot
    * is out of range, or whose ext ends unmapped, are dropped rather than raising an error; recovery validates the
    * index separately, including double-mapped slots.
    */
  def run(start: IndexMap, records: Iterable[JournalRecord]): Result =
    val index = start.copy()
    val n = index.slots.length
    val lastMapping = new java.util.HashMap[Int, Int]

    // Pass 1: maintain the mapping and the position of each ext's last Mapping
    var pos = 0
    records.foreach: r =>
      r match
        case JournalRecord.Mapping(ext, slot, id) =>
          if ext >= 0 && ext < n then
            index.slots(ext) = slot
            index.ids(ext) = id
            lastMapping.put(ext, pos)
        case _ =>
      pos += 1

    // Pass 2: keep the Deltas belonging to each ext's final occupancy
    val out = Vector.newBuilder[JournalRecord.Delta]
    pos = 0
    records.foreach: r =>
      r match
        case d: JournalRecord.Delta =>
          if d.ext >= 0 && d.ext < n && d.slot >= 0 && d.slot < n && index.slots(d.ext) == d.slot &&
            pos > lastMapping.getOrDefault(d.ext, -1)
          then out += d
        case _ =>
      pos += 1

    Result(index, out.result())
