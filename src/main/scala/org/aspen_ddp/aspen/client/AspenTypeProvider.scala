package org.aspen_ddp.aspen.client

/** Supplies user-defined RegisteredTypeFactory instances to a Host started from the CLI.
 *
 *  Found with java.util.ServiceLoader: a jar registers its implementation by naming the class in
 *  `META-INF/services/org.aspen_ddp.aspen.client.AspenTypeProvider`. ServiceLoader instantiates
 *  providers through a public no-arg constructor, so implementations must be classes rather than
 *  Scala objects. The factories they return may be objects. See UsageGuide.md.
 */
trait AspenTypeProvider:
  def factories: List[RegisteredTypeFactory]
