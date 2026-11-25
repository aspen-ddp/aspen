package org.aspen_ddp.aspen.server.store.backend.fsif

/** One "directory" of `under`: every name is prefixed with `prefix`. Several over one SimulatedFsifFiles act as the
  * directories of one machine, so one `crash()` covers the device journal and every store.
  */
final class PrefixedFsifFiles(under: FsifFiles, prefix: String) extends FsifFiles:
  def open(name: String): FsifFile = under.open(prefix + name)
  def size(name: String): Long = under.size(prefix + name)
  def exists(name: String): Boolean = under.exists(prefix + name)
