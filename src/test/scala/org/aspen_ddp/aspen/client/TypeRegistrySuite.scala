package org.aspen_ddp.aspen.client

import java.util.UUID

import org.aspen_ddp.aspen.amoebafs.impl.StaticTypeRegistry
import org.aspen_ddp.aspen.{client, common, server}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object TypeRegistrySuite:
  val sharedUUID: UUID = UUID.fromString("0f0f0f0f-1111-2222-3333-444444444444")

  object FactoryA extends RegisteredTypeFactory:
    val typeUUID: UUID = sharedUUID

  object FactoryB extends RegisteredTypeFactory:
    val typeUUID: UUID = sharedUUID

class TypeRegistrySuite extends AnyFunSuite with Matchers:
  import TypeRegistrySuite.*

  test("the built-in and amoebafs type lists register without duplicates"):
    noException should be thrownBy TypeRegistry(common.TypeFactories.factories, client.TypeFactories.factories,
      server.TypeFactories.factories, StaticTypeRegistry.factories)

  test("two factories sharing a UUID fail registration naming both classes"):
    val e = intercept[TypeRegistry.DuplicateTypeRegistration](TypeRegistry(List(FactoryA), List(FactoryB)))
    e.getMessage should (include (FactoryA.getClass.getTypeName) and include (FactoryB.getClass.getTypeName))
