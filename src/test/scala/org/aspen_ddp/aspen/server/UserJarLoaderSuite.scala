package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.client.{AspenTypeProvider, RegisteredTypeFactory}
import org.aspen_ddp.aspen.client.internal.allocation.DeletionFinalizationAction
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.jar.{JarEntry, JarOutputStream}

object UserJarLoaderSuite:
  object FactoryA extends RegisteredTypeFactory:
    val typeUUID: UUID = UUID.fromString("6a2f6b84-6f57-4c1e-9a11-000000000001")

  object FactoryB extends RegisteredTypeFactory:
    val typeUUID: UUID = UUID.fromString("6a2f6b84-6f57-4c1e-9a11-000000000002")

  // Same UUID as FactoryA
  object FactoryACollision extends RegisteredTypeFactory:
    val typeUUID: UUID = FactoryA.typeUUID

  // Same UUID as one of Aspen's built-in factories
  object BuiltInCollision extends RegisteredTypeFactory:
    val typeUUID: UUID = DeletionFinalizationAction.typeUUID

  class ProviderA extends AspenTypeProvider:
    def factories: List[RegisteredTypeFactory] = List(FactoryA)

  class ProviderB extends AspenTypeProvider:
    def factories: List[RegisteredTypeFactory] = List(FactoryB)

  class ProviderAAgain extends AspenTypeProvider:
    def factories: List[RegisteredTypeFactory] = List(FactoryA)

  class CollidingProvider extends AspenTypeProvider:
    def factories: List[RegisteredTypeFactory] = List(FactoryACollision)

  class BuiltInCollidingProvider extends AspenTypeProvider:
    def factories: List[RegisteredTypeFactory] = List(BuiltInCollision)

  class ThrowingProvider extends AspenTypeProvider:
    def factories: List[RegisteredTypeFactory] = throw new NoClassDefFoundError("com/example/MissingDependency")

class UserJarLoaderSuite extends AnyFunSuite with Matchers:
  import UserJarLoaderSuite.*

  private def withHostDir(f: os.Path => Unit): Unit =
    val dir = os.temp.dir(prefix = "user-jar-loader")
    try f(dir)
    finally os.remove.all(dir)

  // The provider classes are already on the test classpath, so each jar need only hold the service file
  private def writeJar(jar: os.Path, providers: Class[?]*): Unit =
    os.makeDir.all(jar / os.up)
    val out = new JarOutputStream(os.write.outputStream(jar))
    try
      if providers.nonEmpty then
        out.putNextEntry(new JarEntry(UserJarLoader.ServiceFile))
        out.write(providers.map(_.getName).mkString("# user providers\n", "\n", "\n").getBytes("UTF-8"))
        out.closeEntry()
    finally out.close()

  private def loadError(f: => Any): String =
    intercept[UserJarLoader.LoadError](f).getMessage

  test("loads the factories of a single jar"):
    withHostDir: host =>
      writeJar(UserJarLoader.versionDir(host, "v1") / "app.jar", classOf[ProviderA])
      UserJarLoader.load(host, "v1") should be(List(FactoryA))

  test("combines the factories of several jars and providers"):
    withHostDir: host =>
      val dir = UserJarLoader.versionDir(host, "v1")
      writeJar(dir / "a.jar", classOf[ProviderA])
      writeJar(dir / "b.jar", classOf[ProviderB])
      writeJar(dir / "dependency.jar")
      UserJarLoader.load(host, "v1") should contain theSameElementsAs List(FactoryA, FactoryB)

  test("follows a symlinked version directory"):
    withHostDir: host =>
      writeJar(UserJarLoader.versionDir(host, "v2") / "app.jar", classOf[ProviderB])
      os.symlink(host / UserJarLoader.JarsDirName / "latest", os.RelPath("v2"))
      UserJarLoader.load(host, "latest") should be(List(FactoryB))

  test("the same factory supplied twice is not a collision"):
    withHostDir: host =>
      val dir = UserJarLoader.versionDir(host, "v1")
      writeJar(dir / "a.jar", classOf[ProviderA])
      writeJar(dir / "b.jar", classOf[ProviderAAgain])
      UserJarLoader.load(host, "v1").distinct should be(List(FactoryA))

  test("a duplicate UUID across jars names both classes and both jars"):
    withHostDir: host =>
      val dir = UserJarLoader.versionDir(host, "v1")
      writeJar(dir / "a.jar", classOf[ProviderA])
      writeJar(dir / "b.jar", classOf[CollidingProvider])
      val msg = loadError(UserJarLoader.load(host, "v1"))
      msg should include(FactoryA.typeUUID.toString)
      msg should include(FactoryA.getClass.getTypeName)
      msg should include(FactoryACollision.getClass.getTypeName)
      msg should include((dir / "a.jar").toString)
      msg should include((dir / "b.jar").toString)

  test("a UUID that collides with a built-in factory is rejected"):
    withHostDir: host =>
      writeJar(UserJarLoader.versionDir(host, "v1") / "a.jar", classOf[BuiltInCollidingProvider])
      val msg = loadError(UserJarLoader.load(host, "v1"))
      msg should include(DeletionFinalizationAction.getClass.getTypeName)
      msg should include("Aspen built-in")
      msg should include(BuiltInCollision.getClass.getTypeName)

  test("a missing version directory is an error"):
    withHostDir: host =>
      loadError(UserJarLoader.load(host, "v9")) should include("does not exist")

  test("a version directory without jars is an error"):
    withHostDir: host =>
      os.makeDir.all(UserJarLoader.versionDir(host, "v1"))
      loadError(UserJarLoader.load(host, "v1")) should include("No .jar files")

  test("jars that register no provider are an error"):
    withHostDir: host =>
      writeJar(UserJarLoader.versionDir(host, "v1") / "a.jar")
      loadError(UserJarLoader.load(host, "v1")) should include(UserJarLoader.ServiceFile)

  test("a registered provider class that cannot be found is an error"):
    withHostDir: host =>
      val jar = UserJarLoader.versionDir(host, "v1") / "a.jar"
      os.makeDir.all(jar / os.up)
      val out = new JarOutputStream(os.write.outputStream(jar))
      try
        out.putNextEntry(new JarEntry(UserJarLoader.ServiceFile))
        out.write("com.example.NoSuchProvider\n".getBytes("UTF-8"))
        out.closeEntry()
      finally out.close()
      loadError(UserJarLoader.load(host, "v1")) should include("com.example.NoSuchProvider")

  test("a provider missing a dependency is an error naming its jar"):
    withHostDir: host =>
      val jar = UserJarLoader.versionDir(host, "v1") / "a.jar"
      writeJar(jar, classOf[ThrowingProvider])
      val msg = loadError(UserJarLoader.load(host, "v1"))
      msg should include(jar.toString)
      msg should include("com/example/MissingDependency")
