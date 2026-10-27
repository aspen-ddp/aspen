package org.aspen_ddp.aspen.server

import org.aspen_ddp.aspen.AspenError
import org.aspen_ddp.aspen.client.{AspenTypeProvider, RegisteredTypeFactory}

import java.net.URLClassLoader
import java.util.ServiceLoader
import java.util.UUID
import java.util.jar.JarFile
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.Using
import scala.util.control.NonFatal

/** Loads user-supplied RegisteredTypeFactory instances from `<host-dir>/jars/<version>/` for the
 *  `host --load-jars=<version>` command.
 *
 *  Every `*.jar` directly inside the version directory goes onto a single URLClassLoader whose
 *  parent is Aspen's own loader, so a user jar may ship its dependencies either bundled (a fat jar)
 *  or as loose jars alongside it. Parent-first delegation means Aspen's copy of any shared library,
 *  Scala included, always wins. AspenTypeProvider implementations are then found with ServiceLoader.
 *
 *  `<version>` may be a symlink, such as `latest`, so a startup script can stay fixed while the link
 *  is repointed to choose the version loaded on the next start.
 */
object UserJarLoader:

  val JarsDirName = "jars"

  val ServiceFile = s"META-INF/services/${classOf[AspenTypeProvider].getName}"

  class LoadError(msg: String, cause: Throwable = null) extends AspenError(msg, cause)

  def versionDir(hostDir: os.Path, version: String): os.Path = hostDir / JarsDirName / version

  def load(hostDir: os.Path, version: String): List[RegisteredTypeFactory] =
    loadDirectory(versionDir(hostDir, version))

  def loadDirectory(dir: os.Path): List[RegisteredTypeFactory] =
    if ! os.exists(dir) then
      throw LoadError(s"Jar directory does not exist: $dir")

    if ! os.isDir(dir) then
      throw LoadError(s"Jar directory is not a directory: $dir")

    val jars = os.list(dir).filter(p => p.ext == "jar" && os.isFile(p)).toList

    if jars.isEmpty then
      throw LoadError(s"No .jar files found in $dir")

    // Provider class name -> the jars whose service file names it. Used only to attribute errors.
    val registrations: Map[String, List[os.Path]] = jars
      .flatMap(jar => registeredProviders(jar).map(_ -> jar))
      .groupMap(_._1)(_._2)

    // Deliberately never closed: the loaded classes are used for the life of the process
    val loader = new URLClassLoader(jars.map(_.toNIO.toUri.toURL).toArray, getClass.getClassLoader)

    def sourceOf(providerClass: String): String =
      registrations.get(providerClass) match
        case Some(srcJars) => s"${srcJars.map(_.toString).mkString(", ")} ($providerClass)"
        case None => providerClass

    val providers = guarded(s"Failed to load an AspenTypeProvider from $dir"):
      ServiceLoader.load(classOf[AspenTypeProvider], loader).stream().iterator().asScala.toList.map: p =>
        val name = p.`type`().getName
        guarded(s"Failed to instantiate AspenTypeProvider $name from ${sourceOf(name)}"):
          (p.get(), sourceOf(name))

    if providers.isEmpty then
      throw LoadError(s"No AspenTypeProvider is registered by any jar in $dir. Each provider class must be " +
        s"named in the jar's $ServiceFile file")

    val userFactories = providers.flatMap: (provider, source) =>
      guarded(s"AspenTypeProvider from $source failed to supply its factories"):
        // Forces typeUUID so that a missing dependency class surfaces here rather than in TypeRegistry
        provider.factories.map(f => (f, f.typeUUID, source))

    checkForDuplicates(userFactories)

    userFactories.map(_._1)

  /** Fails with a LoadError naming both classes and their sources if two distinct factories share a
   *  UUID. Aspen's built-in factories are included so a collision with one of them is caught too.
   *  The same instance listed twice is not a collision, matching TypeRegistry.
   */
  private def checkForDuplicates(userFactories: List[(RegisteredTypeFactory, UUID, String)]): Unit =
    val builtIn = (org.aspen_ddp.aspen.common.TypeFactories.factories ++
      org.aspen_ddp.aspen.client.TypeFactories.factories ++
      org.aspen_ddp.aspen.server.TypeFactories.factories).map(f => (f, f.typeUUID, "Aspen built-in"))

    var seen = Map[UUID, (RegisteredTypeFactory, String)]()

    (builtIn ++ userFactories).foreach: (factory, uuid, source) =>
      seen.get(uuid) match
        case None => seen += uuid -> (factory, source)
        case Some((prev, prevSource)) =>
          if factory ne prev then
            throw LoadError(s"Duplicate RegisteredTypeFactory UUID $uuid:\n" +
              s"  ${prev.getClass.getTypeName} from $prevSource\n" +
              s"  ${factory.getClass.getTypeName} from $source")

  /** Provider class names listed in the jar's service file, in the format ServiceLoader reads. */
  private def registeredProviders(jar: os.Path): List[String] =
    try
      Using.resource(new JarFile(jar.toIO)): jf =>
        Option(jf.getJarEntry(ServiceFile)) match
          case None => Nil
          case Some(entry) =>
            Using.resource(Source.fromInputStream(jf.getInputStream(entry), "UTF-8")): src =>
              src.getLines().map(_.takeWhile(_ != '#').trim).filter(_.nonEmpty).toList
    catch
      case NonFatal(e) => throw LoadError(s"Unable to read jar file $jar: ${e.getMessage}", e)

  /** Converts any failure, including the LinkageErrors a missing dependency produces, into a LoadError. */
  private def guarded[T](context: String)(f: => T): T =
    try f
    catch
      case e: LoadError => throw e
      case e @ (NonFatal(_) | _: LinkageError) =>
        throw LoadError(s"$context: $e", e)
