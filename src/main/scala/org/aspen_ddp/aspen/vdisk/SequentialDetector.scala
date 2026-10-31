package org.aspen_ddp.aspen.vdisk

/** Detects sequential read streams and sizes their readahead.
  *
  * Up to `maxStreams` streams are tracked, each by where its last read ended. A read starting exactly there
  * continues that stream: its readahead starts at `initialWindow` and doubles with each further read, up to
  * `maxWindow`. Any other read is treated as random, gets no readahead, and starts a new stream that replaces the
  * least recently used one. Small random reads therefore stay small. Not thread-safe; `VirtualDisk` calls it under
  * its monitor.
  */
final class SequentialDetector(maxStreams: Int = 8, initialWindow: Int, maxWindow: Int):
  require(maxStreams >= 1, s"maxStreams ($maxStreams) must be at least 1")

  private final class ReadStream(var next: Long, var window: Int, var lastUse: Long)

  private val streams = scala.collection.mutable.ArrayBuffer[ReadStream]()
  private var clock = 0L

  /** The readahead length past the end of this read, 0 for a random read */
  def onRead(offset: Long, length: Int): Int =
    clock += 1
    streams.find(_.next == offset) match
      case Some(s) =>
        val grown = if s.window == 0 then initialWindow.toLong else s.window.toLong * 2
        s.window = math.min(grown, maxWindow.toLong).toInt
        s.next = offset + length
        s.lastUse = clock
        s.window
      case None =>
        if streams.size < maxStreams then
          streams += ReadStream(offset + length, 0, clock)
        else
          val s = streams.minBy(_.lastUse)
          s.next = offset + length
          s.window = 0
          s.lastUse = clock
        0
