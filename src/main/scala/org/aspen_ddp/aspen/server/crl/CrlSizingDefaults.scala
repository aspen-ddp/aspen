package org.aspen_ddp.aspen.server.crl

import org.aspen_ddp.aspen.server.StorageDeviceConfig
import scribe.Logging

/** The sizing a device's CRL is created with, and the bounds on its growth. */
case class CrlSizing(numStreams: Int, streamBytes: Long, maxStreams: Int, minFreePercent: Int)

object CrlSizingDefaults extends Logging:

  val MinStreams = 4
  val DefaultMaxStreams = 20
  val DefaultMinFreePercent = 5

  /** A stream is capacity / 2048 by default, so the default growth cap of 20 streams is about 1% of the device */
  val CapacityDivisor = 2048L
  val MinStreamBytes: Long = 64L << 20
  val StreamAlignment = 4096L

  private def defaultStreamBytes(capacityBytes: Long): Long =
    math.max(MinStreamBytes, capacityBytes / CapacityDivisor / StreamAlignment * StreamAlignment)

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
        defaultStreamBytes(capacityBytes)

    val maxStreams = config.crlMaxStreams.getOrElse(DefaultMaxStreams)
    require(maxStreams >= numStreams,
      s"crl-max-streams must be at least crl-num-streams ($numStreams), not $maxStreams")

    val minFreePercent = config.crlMinFreePercent.getOrElse(DefaultMinFreePercent)
    require(minFreePercent >= 0 && minFreePercent <= 100,
      s"crl-min-free-percent must be within 0 to 100, not $minFreePercent")

    CrlSizing(numStreams, streamBytes, maxStreams, minFreePercent)

  /** The sizing for opening a CRL that already exists. It never throws: the CRL opens with the sizing it recorded
   *  whatever the config says, so `numStreams` and `streamBytes` are only what SimpleCRL.apply compares the record
   *  with (an unusable value falls back to the default). A growth bound that would be refused at creation is warned
   *  about and disables growth for this open instead: `maxStreams` 0 allows no stream beyond the ring, and
   *  `minFreePercent` 100 leaves no free space to grow into. */
  def resolveExisting(config: StorageDeviceConfig, capacityBytes: Long): CrlSizing =
    val numStreams = config.crlNumStreams.filter(_ >= 3).getOrElse(MinStreams)
    val streamBytes = config.crlFileSizeMb.filter(_ > 0).fold(defaultStreamBytes(capacityBytes))(_.toLong << 20)
    val maxStreams = config.crlMaxStreams.getOrElse(DefaultMaxStreams)
    val minFreePercent = config.crlMinFreePercent.getOrElse(DefaultMinFreePercent)
    val badMax = maxStreams < MinStreams
    val badFree = minFreePercent < 0 || minFreePercent > 100
    if badMax || badFree then
      logger.warn(s"Invalid CRL growth bounds in the device config (crl-max-streams $maxStreams, " +
        s"crl-min-free-percent $minFreePercent): the existing CRL opens as recorded, with growth disabled")
    CrlSizing(numStreams, streamBytes, if badMax then 0 else maxStreams, if badFree then 100 else minFreePercent)
