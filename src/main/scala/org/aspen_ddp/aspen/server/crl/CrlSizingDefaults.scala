package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.server.StorageDeviceConfig

/** The sizing a device's CRL is created with, and the bounds on its growth. */
case class CrlSizing(numStreams: Int, streamBytes: Long, maxStreams: Int, minFreePercent: Int)

object CrlSizingDefaults:

  val MinStreams = 4
  val DefaultMaxStreams = 20
  val DefaultMinFreePercent = 5

  /** A stream is capacity / 2048 by default, so the default growth cap of 20 streams is about 1% of the device */
  val CapacityDivisor = 2048L
  val MinStreamBytes: Long = 64L << 20
  val StreamAlignment = 4096L

  /** The sizing for a CRL created on a device of `capacityBytes`: each key the config leaves out takes its default.
   *  @throws IllegalArgumentException for a value that cannot size a CRL */
  def resolve(config: StorageDeviceConfig, capacityBytes: Long): CrlSizing =
    val numStreams = config.crlNumStreams.getOrElse(MinStreams)
    require(numStreams >= MinStreams, s"crl-num-streams must be at least $MinStreams, not $numStreams")

    val streamBytes = config.crlFileSizeMb match
      case Some(mb) =>
        require(mb > 0, s"crl-max-file-size-mb must be positive, not $mb")
        mb.toLong << 20
      case None =>
        math.max(MinStreamBytes, capacityBytes / CapacityDivisor / StreamAlignment * StreamAlignment)

    val maxStreams = config.crlMaxStreams.getOrElse(DefaultMaxStreams)
    require(maxStreams >= numStreams,
      s"crl-max-streams must be at least crl-num-streams ($numStreams), not $maxStreams")

    val minFreePercent = config.crlMinFreePercent.getOrElse(DefaultMinFreePercent)
    require(minFreePercent >= 0 && minFreePercent <= 100,
      s"crl-min-free-percent must be within 0 to 100, not $minFreePercent")

    CrlSizing(numStreams, streamBytes, maxStreams, minFreePercent)
