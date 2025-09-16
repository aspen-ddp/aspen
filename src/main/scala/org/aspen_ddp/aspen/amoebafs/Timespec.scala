package org.aspen_ddp.aspen.amoebafs

import java.nio.ByteBuffer

case class Timespec(seconds: Long, nanoseconds: Int) {
  def size: Int = 12
  def encodeInto(bb: ByteBuffer): Unit = {
    bb.putLong(seconds)
    bb.putInt(nanoseconds)
  }
  def millis: Long = seconds * 1000 + nanoseconds / 1000
}

object Timespec {

  def apply(bb: ByteBuffer): Timespec = {
    val sec = bb.getLong()
    val nsec = bb.getInt()
    new Timespec(sec, nsec)
  }

  def apply(millis: Long): Timespec = {
    val sec = millis / 1000
    val nsec = (millis - (sec*1000)) * 1000
    Timespec(sec, nsec.toInt)
  }

  def now: Timespec = {
    val ts = System.currentTimeMillis()
    val sec = ts / 1000
    val nsec = (ts - (sec*1000)) * 1000
    Timespec(sec, nsec.toInt)
  }
}
