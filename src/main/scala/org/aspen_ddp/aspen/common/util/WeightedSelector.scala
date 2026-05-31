package org.aspen_ddp.aspen.common.util

import scala.util.Random
import java.util.Arrays

class WeightedSelector[A](items: Seq[(A, Double)]):
  private val (values, cumulativeWeights) = prep(items)
  private val totalWeight = cumulativeWeights.lastOption.getOrElse(0.0)

  private def prep(items: Seq[(A, Double)]): (Array[Any], Array[Double]) =
    val vals = new Array[Any](items.size)
    val weights = new Array[Double](items.size)
    var sum = 0.0

    for ((value, weight), i) <- items.zipWithIndex do
      require(weight >= 0, "Weight cannot be negative")
      sum += weight
      vals(i) = value
      weights(i) = sum

    (vals, weights)

  def next(): Option[A] =
    if totalWeight == 0.0 then None
    else
      val target = Random.nextDouble() * totalWeight

      // Java's binarySearch returns the index if found,
      // or (-(insertion point) - 1) if not found.
      val ip = Arrays.binarySearch(cumulativeWeights, target)
      val index = if ip >= 0 then ip else -ip - 1

      Some(values(index).asInstanceOf[A])


@main def runWeightedSelector(): Unit =
  val selector = WeightedSelector(List(
    ("Cherry", 60.0),
    ("Apple", 5.0),
    ("Orange", 70.0),
    ("Banana", 30.0),
  ))

  // Rapid sampling
  val samples = List.fill(10000)(selector.next()).flatten
  println(s"Apple count:  ${samples.count(_ == "Apple")}")  // ~1000
  println(s"Banana count: ${samples.count(_ == "Banana")}") // ~3000
  println(s"Cherry count: ${samples.count(_ == "Cherry")}") // ~6000
  println(s"Orange count: ${samples.count(_ == "Orange")}") // ~6000
