package org.aspen_ddp.aspen.common.objects

import java.nio.charset.StandardCharsets
import scala.util.control.Breaks._
import org.aspen_ddp.aspen.AmoebaError

sealed abstract class KeyOrdering extends Ordering[Key] {
  def compare(a: Key, b: Key): Int
  def compare(a: Key, b: Array[Byte]): Int = compare(a, Key(b))
  def compare(a: Array[Byte], b: Key): Int = compare(Key(a), b)
  def compare(a: Array[Byte], b: Array[Byte]): Int = compare(Key(a), Key(b))
  def code: Byte
}

object KeyOrdering {
  def fromCode(code: Byte): KeyOrdering = code match {
    case 0 => ByteArrayKeyOrdering
    case 1 => IntegerKeyOrdering
    case 2 => LexicalKeyOrdering
    case _ => throw InvalidKeyOrdering()
  }
}

object ByteArrayKeyOrdering extends KeyOrdering {
  override def code: Byte = 0
  override def compare(a: Key, b: Key): Int = {
    if (b.bytes.length == 0 && a.bytes.length != 0) return 1

    var result = 0 // a and b are the same length and have matching content

    breakable {
      for (i <- a.bytes.indices) {
        if (i > b.bytes.length - 1) then
          result = 1 // a is longer than b and all preceeding bytes are equal
          break
        if (a.bytes(i) < b.bytes(i))
          result = -1 // a is less than b
          break
        if (a.bytes(i) > b.bytes(i))
          result = 1 // a is greater than b
          break
      }
    }

    if (b.bytes.length > a.bytes.length) result = -1 // b is longer than a and all preceeding bytes are equal

    result
  }
}

object IntegerKeyOrdering extends KeyOrdering {
  override def code: Byte = 1
  override def compare(a: Key, b: Key): Int = {
    if (a.bytes.length == 0 && b.bytes.length == 0)
      0
    else if (a.bytes.length == 0 && b.bytes.length != 0)
      -1
    else if (a.bytes.length != 0 && b.bytes.length == 0)
      1
    else {
      val bigA = new java.math.BigInteger(a.bytes)
      val bigB = new java.math.BigInteger(b.bytes)
      bigA.compareTo(bigB)
    }
  }
}

object LexicalKeyOrdering extends KeyOrdering {
  override def code: Byte = 2
  override def compare(a: Key, b: Key): Int = {
    val sa = new String(a.bytes, StandardCharsets.UTF_8)
    val sb = new String(b.bytes, StandardCharsets.UTF_8)
    sa.compareTo(sb)
  }
}
