package org.aspen_ddp.aspen.server

import java.io.IOException
import java.nio.file.{Files, LinkOption, NoSuchFileException}
import java.nio.file.attribute.BasicFileAttributes

/** Existence tests that tell a failed stat from an absent file.
 *
 *  `os.exists`, `os.isFile` and `os.isDir` (Files.exists and friends) answer false on any stat error: an EIO, an
 *  ENOMEM, a permission error, a hung mount. Where an absence leads the host to drop state, serve a store without it,
 *  or declare something lost, that false is a guess. These answer false only when the file is known to be absent
 *  (ENOENT, or a parent that is a file rather than a directory) and throw the stat's error otherwise, so the caller
 *  treats the answer as unknown: it holds back and retries (cross-document-review 2.2). Links are followed, as
 *  os.exists follows them, unless `followLinks` is false. Blocks on I/O, like the os-lib calls they replace.
 *
 *  Every existence test under `server/` goes through here: FileStatUsageSuite fails on an `os.exists`, `os.isDir`
 *  or `os.isFile` anywhere else in it.
 */
object FileStat:
  /** The file's attributes, None if it is known absent; throws the stat's error if its existence is unknown */
  def stat(path: os.Path, followLinks: Boolean = true): Option[BasicFileAttributes] =
    val options = if followLinks then Seq() else Seq(LinkOption.NOFOLLOW_LINKS)
    try Some(Files.readAttributes(path.toNIO, classOf[BasicFileAttributes], options*))
    catch
      case _: NoSuchFileException => None
      case e: IOException =>
        // ENOTDIR (a path under a regular file) reports as a plain FileSystemException: absent only if the parent is
        // known to be something other than a directory
        val parentIsNoDirectory =
          path.segmentCount > 0 &&
            (try !Files.readAttributes((path / os.up).toNIO, classOf[BasicFileAttributes]).isDirectory
             catch case _: IOException => false)
        if parentIsNoDirectory then None else throw e

  /** Whether `path` exists. False only on a known absence; any other stat failure throws */
  def exists(path: os.Path, followLinks: Boolean = true): Boolean = stat(path, followLinks).isDefined

  /** Whether `path` is a regular file. False if absent or not a file; any other stat failure throws */
  def isFile(path: os.Path, followLinks: Boolean = true): Boolean = stat(path, followLinks).exists(_.isRegularFile)

  /** Whether `path` is a directory. False if absent or not a directory; any other stat failure throws */
  def isDir(path: os.Path, followLinks: Boolean = true): Boolean = stat(path, followLinks).exists(_.isDirectory)

  /** Whether `path` exists: Some(answer), or None when its stat failed and existence is unknown */
  def existence(path: os.Path): Option[Boolean] =
    try Some(exists(path))
    catch case _: IOException => None
