package org.aspen_ddp.aspen.common.objects

import java.nio.charset.StandardCharsets
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object KeyOrderingSuite {
  def bk(bytes: Int*): Key = Key(bytes.map(_.toByte).toArray)

  // Every key of length 0 to 3 drawn from bytes that sit at the edges of the signed and unsigned ranges
  val edgeBytes: List[Byte] = List(0x00, 0x01, 0x7F, 0x80, 0xFF).map(_.toByte)

  val byteKeys: List[Key] =
    (0 to 3).toList.flatMap { len =>
      List.fill(len)(edgeBytes).foldLeft(List(List.empty[Byte])) { (acc, choices) =>
        for prefix <- acc; b <- choices yield prefix :+ b
      }
    }.map(l => Key(l.toArray))

  // IntegerKeyOrdering keys in the canonical form Key(Long) produces, plus the empty minimum key
  val integerKeys: List[Key] =
    Key.AbsoluteMinimum :: List(Long.MinValue, -65536L, -257L, -256L, -129L, -128L, -1L, 0L, 1L, 127L, 128L, 255L,
      256L, 32767L, 32768L, 65536L, Long.MaxValue).map(n => Key(n))

  // LexicalKeyOrdering keys that are valid UTF-8
  val lexicalKeys: List[Key] =
    List("", "a", "aa", "ab", "b", "B", "z", "za", "é", "éa", "中", "￿", "😀")
      .map(s => Key(s.getBytes(StandardCharsets.UTF_8)))

  // Keys that encode an integer with redundant sign-extension bytes, alongside their canonical forms
  val nonCanonicalIntegerKeys: List[Key] =
    List(bk(0), bk(0, 0), bk(0, 0, 0), bk(0, 1), bk(1), bk(0xFF), bk(0xFF, 0xFF), bk(0, 0x80), bk(0, 0, 0x80))

  def sign(n: Int): Int = Integer.signum(n)

  /** Reference lexicographic order over unsigned bytes, with a prefix sorting first */
  def unsignedLessThan(a: Key, b: Key): Boolean =
    a.bytes.indices.find(i => i >= b.bytes.length || a.bytes(i) != b.bytes(i)) match
      case None => a.bytes.length < b.bytes.length
      case Some(i) => i < b.bytes.length && (a.bytes(i) & 0xFF) < (b.bytes(i) & 0xFF)
}

class KeyOrderingSuite extends AnyFunSuite with Matchers {
  import KeyOrderingSuite._

  /** Checks the laws a sort relies on: compare is zero exactly for equal keys, antisymmetric, and transitive */
  def checkTotalOrder(ordering: KeyOrdering, keys: List[Key]): Unit =
    for a <- keys; b <- keys do
      val ab = sign(ordering.compare(a, b))
      withClue(s"compare($a, $b) = $ab, compare($b, $a) = ${ordering.compare(b, a)}: ") {
        ab shouldBe -sign(ordering.compare(b, a))
        (ab == 0) shouldBe (a == b)
      }

    for a <- keys; b <- keys if ordering.compare(a, b) < 0; c <- keys if ordering.compare(b, c) < 0 do
      withClue(s"$a < $b < $c but compare($a, $c) = ${ordering.compare(a, c)}: ") {
        ordering.compare(a, c) should be < 0
      }

  // ByteArrayKeyOrdering

  test("ByteArray: shorter key with a larger first byte sorts after a longer key") {
    ByteArrayKeyOrdering.compare(bk(3), bk(2, 0)) should be > 0
    ByteArrayKeyOrdering.compare(bk(2, 0), bk(3)) should be < 0
  }

  test("ByteArray: a key sorts after its own prefix") {
    ByteArrayKeyOrdering.compare(bk(1), bk(1, 0)) should be < 0
    ByteArrayKeyOrdering.compare(bk(1, 0), bk(1)) should be > 0
  }

  test("ByteArray: the empty key sorts before every other key") {
    ByteArrayKeyOrdering.compare(Key.AbsoluteMinimum, Key.AbsoluteMinimum) shouldBe 0
    for k <- byteKeys if k.bytes.nonEmpty do
      ByteArrayKeyOrdering.compare(Key.AbsoluteMinimum, k) should be < 0
      ByteArrayKeyOrdering.compare(k, Key.AbsoluteMinimum) should be > 0
  }

  test("ByteArray: bytes compare as unsigned values") {
    ByteArrayKeyOrdering.compare(bk(0x80), bk(0x7F)) should be > 0
    ByteArrayKeyOrdering.compare(bk(0xFF), bk(0x00)) should be > 0
    ByteArrayKeyOrdering.compare(bk(1, 0xFF), bk(1, 0x01)) should be > 0
  }

  test("ByteArray: equal-length keys compare at their first differing byte") {
    ByteArrayKeyOrdering.compare(bk(1, 2, 3), bk(1, 2, 4)) should be < 0
    ByteArrayKeyOrdering.compare(bk(1, 3, 0), bk(1, 2, 9)) should be > 0
    ByteArrayKeyOrdering.compare(bk(1, 2, 3), bk(1, 2, 3)) shouldBe 0
  }

  test("ByteArray: is a total order over keys of mixed lengths") {
    checkTotalOrder(ByteArrayKeyOrdering, byteKeys)
  }

  test("ByteArray: sorting matches lexicographic unsigned-byte order") {
    byteKeys.reverse.sorted(using ByteArrayKeyOrdering) shouldBe byteKeys.sortWith(unsignedLessThan)
  }

  // IntegerKeyOrdering

  test("Integer: compares by two's-complement numeric value") {
    IntegerKeyOrdering.compare(Key(256L), Key(2L)) should be > 0
    IntegerKeyOrdering.compare(Key(-1L), Key(1L)) should be < 0
    IntegerKeyOrdering.compare(Key(-256L), Key(-1L)) should be < 0
  }

  test("Integer: the empty key sorts before every number") {
    IntegerKeyOrdering.compare(Key.AbsoluteMinimum, Key(Long.MinValue)) should be < 0
    IntegerKeyOrdering.compare(Key(Long.MinValue), Key.AbsoluteMinimum) should be > 0
    IntegerKeyOrdering.compare(Key.AbsoluteMinimum, Key.AbsoluteMinimum) shouldBe 0
  }

  test("Integer: sorts canonically encoded keys by value") {
    integerKeys.reverse.sorted(using IntegerKeyOrdering) shouldBe integerKeys
  }

  test("Integer: distinct encodings of the same value are distinct keys, the shorter first") {
    IntegerKeyOrdering.compare(bk(0), bk(0, 0)) should be < 0
    IntegerKeyOrdering.compare(bk(0, 0), bk(0)) should be > 0
    IntegerKeyOrdering.compare(bk(0xFF), bk(0xFF, 0xFF)) should be < 0
  }

  test("Integer: a redundant encoding still sorts by value against other values") {
    IntegerKeyOrdering.compare(bk(0, 0, 1), bk(2)) should be < 0
    IntegerKeyOrdering.compare(bk(0xFF, 0xFF), bk(0)) should be < 0
    IntegerKeyOrdering.compare(bk(0, 0, 0x80), bk(0x7F)) should be > 0
  }

  test("Integer: is a total order over every byte sequence") {
    checkTotalOrder(IntegerKeyOrdering, (byteKeys ++ integerKeys ++ nonCanonicalIntegerKeys).distinct)
  }

  // LexicalKeyOrdering

  test("Lexical: compares as strings") {
    LexicalKeyOrdering.compare(Key("abc"), Key("abd")) should be < 0
    LexicalKeyOrdering.compare(Key("b"), Key("aa")) should be > 0
    LexicalKeyOrdering.compare(Key("a"), Key("aa")) should be < 0
    LexicalKeyOrdering.compare(Key(""), Key("a")) should be < 0
  }

  def lexSorted(words: String*): List[String] =
    words.reverse.map(Key(_)).sorted(using LexicalKeyOrdering).map(_.stringValue).toList

  test("Lexical: sorts letters case-insensitively first, lowercase before uppercase on a tie") {
    lexSorted("apple", "Banana", "cherry") shouldBe List("apple", "Banana", "cherry")
    lexSorted("a", "A", "b", "B") shouldBe List("a", "A", "b", "B")
    lexSorted("Zebra", "apple") shouldBe List("apple", "Zebra")
  }

  test("Lexical: accented letters sort beside their base letter") {
    lexSorted("e", "é", "f") shouldBe List("e", "é", "f")
    lexSorted("resume", "résumé", "Resume", "rest") shouldBe List("rest", "resume", "Resume", "résumé")
  }

  test("Lexical: digits sort before letters") {
    lexSorted("a", "9", "Z", "0") shouldBe List("0", "9", "a", "Z")
  }

  test("Lexical: canonically equivalent spellings are distinct, adjacent keys") {
    val composed = Key("é")
    val decomposed = Key("é")
    LexicalKeyOrdering.compare(composed, decomposed) should not be 0
    for k <- List(composed, decomposed) do
      LexicalKeyOrdering.compare(Key("e"), k) should be < 0
      LexicalKeyOrdering.compare(k, Key("f")) should be < 0
  }

  test("Lexical: invalid UTF-8 keys are distinct from one another") {
    LexicalKeyOrdering.compare(bk(0x80), bk(0xFF)) should not be 0
    LexicalKeyOrdering.compare(bk(0xC3), bk(0xC3, 0x28)) should not be 0
  }

  test("Lexical: is a total order over every byte sequence") {
    checkTotalOrder(LexicalKeyOrdering, (byteKeys ++ lexicalKeys).distinct)
  }

  // KeyOrdering

  test("fromCode round-trips every ordering and rejects unknown codes") {
    for o <- List(ByteArrayKeyOrdering, IntegerKeyOrdering, LexicalKeyOrdering) do
      KeyOrdering.fromCode(o.code) shouldBe o
    an[InvalidKeyOrdering] should be thrownBy KeyOrdering.fromCode(3)
  }

  test("Array[Byte] overloads agree with the Key form") {
    ByteArrayKeyOrdering.compare(Array[Byte](3), Key(Array[Byte](2, 0))) should be > 0
    ByteArrayKeyOrdering.compare(Key(Array[Byte](3)), Array[Byte](2, 0)) should be > 0
    ByteArrayKeyOrdering.compare(Array[Byte](3), Array[Byte](2, 0)) should be > 0
  }
}
