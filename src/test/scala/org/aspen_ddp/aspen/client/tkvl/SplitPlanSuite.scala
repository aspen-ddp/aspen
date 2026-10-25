package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.common.objects.Key
import org.scalatest.funsuite.AnyFunSuite

/** Expectations were derived by hand from the loop formerly duplicated in KeyValueListNode.insert/rename:
 *  halfSize = total / 2; start with the last key; keep taking the next lower key until the moved size
 *  plus that key's size reaches halfSize.
 */
class SplitPlanSuite extends AnyFunSuite:

  private def sizes(pairs: (String, Int)*): List[(Key, Int)] = pairs.map((k, s) => (Key(k), s)).toList

  private def keys(names: String*): List[Key] = names.map(Key(_)).toList

  test("equal sizes move the upper half"):
    // total 50, half 25: [e]=10, +d=20 < 25, +c=30 >= 25 stops
    assert(SplitPlan.keysToMove(sizes("a" -> 10, "b" -> 10, "c" -> 10, "d" -> 10, "e" -> 10)) == keys("d", "e"))

  test("a single key moves that key"):
    assert(SplitPlan.keysToMove(sizes("a" -> 10)) == keys("a"))

  test("two keys move the last one"):
    // half 10: [b]=10, +a=20 >= 10 stops
    assert(SplitPlan.keysToMove(sizes("a" -> 10, "b" -> 10)) == keys("b"))

  test("a large last key moves alone"):
    // total 140, half 70: [e]=100, +d=110 >= 70 stops
    assert(SplitPlan.keysToMove(sizes("a" -> 10, "b" -> 10, "c" -> 10, "d" -> 10, "e" -> 100)) == keys("e"))

  test("a large first key stays left"):
    // total 55, half 27: [d]=5, +c=10, +b=15 < 27, +a=55 >= 27 stops
    assert(SplitPlan.keysToMove(sizes("a" -> 40, "b" -> 5, "c" -> 5, "d" -> 5)) == keys("b", "c", "d"))
