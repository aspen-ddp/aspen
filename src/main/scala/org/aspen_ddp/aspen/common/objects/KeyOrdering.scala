package org.aspen_ddp.aspen.common.objects

import com.ibm.icu.text.Collator
import com.ibm.icu.util.ULocale
import java.nio.charset.StandardCharsets
import org.aspen_ddp.aspen.AspenError

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
  /** Lexicographic order over unsigned bytes. A key sorts after any key that is a prefix of it */
  override def compare(a: Key, b: Key): Int = java.util.Arrays.compareUnsigned(a.bytes, b.bytes)
}

object IntegerKeyOrdering extends KeyOrdering {
  override def code: Byte = 1
  /** Orders keys by their big-endian two's-complement value, with the empty key below every number.
    *
    * Encodings of the same value that differ only in redundant sign-extension bytes are distinct keys, so they are
    * ordered by length, shortest first. Two encodings of one value with the same length are always identical.
    */
  override def compare(a: Key, b: Key): Int =
    if a.bytes.length == 0 || b.bytes.length == 0 then
      Integer.compare(a.bytes.length, b.bytes.length)
    else
      val byValue = new java.math.BigInteger(a.bytes).compareTo(new java.math.BigInteger(b.bytes))
      if byValue != 0 then
        byValue
      else
        Integer.compare(a.bytes.length, b.bytes.length)
}

/** Orders human-readable UTF-8 strings in normal text sorting order, using ICU4J's Unicode collation.
  *
  * The rules are the language-neutral root locale at tertiary strength: case is ignored on the first pass and
  * lowercase wins a tie, and accented letters sort beside their base letter. For example
  * "apple" < "Banana" < "cherry", "a" < "A" < "b", and "e" < "é" < "f".
  *
  * Why ICU4J
  *   - Tiered key-value lists persist this order in their structure, so every client and host must compute exactly
  *     the same order. If two of them disagree, lookups are routed to the wrong node.
  *   - ICU4J pinned to an exact version in build.sbt gives the same result on any JVM. java.text.Collator's data
  *     ships with the JDK and can change between JDK releases, so hosts on different JDKs could disagree.
  *   - The root locale is a fixed constant. Locale-specific rules would have to be recorded in each tree, and a
  *     default locale would make the order depend on the machine.
  *
  * Upgrading ICU4J
  *   - Each ICU release tracks a newer Unicode and CLDR version. Their collation data can reorder strings, most often
  *     ones that use newly assigned or newly tailored characters, though not only those.
  *   - Any change in order silently corrupts existing trees keyed by this ordering: amoebafs directories and the
  *     namespaced UUID registry. Treat a version bump as an on-disk format change. Either confirm the old and new
  *     versions order every key that can occur identically, or migrate existing trees.
  *   - The same applies to the collator settings below. Changing the strength, the decomposition mode, or the
  *     tie-break changes the order.
  */
object LexicalKeyOrdering extends KeyOrdering {
  override def code: Byte = 2

  // Frozen collators are immutable and safe to share across threads
  private val collator: Collator =
    val c = Collator.getInstance(ULocale.ROOT)
    c.setStrength(Collator.TERTIARY)
    c.setDecomposition(Collator.CANONICAL_DECOMPOSITION)
    c.freeze()

  /** Keys the collator considers equal (canonically equivalent spellings, invalid UTF-8) remain distinct keys,
    * ordered by their unsigned bytes.
    */
  override def compare(a: Key, b: Key): Int =
    val byText = collator.compare(new String(a.bytes, StandardCharsets.UTF_8), new String(b.bytes, StandardCharsets.UTF_8))
    if byText != 0 then
      byText
    else
      java.util.Arrays.compareUnsigned(a.bytes, b.bytes)
}
