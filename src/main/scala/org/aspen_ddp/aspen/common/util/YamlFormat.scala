package org.aspen_ddp.aspen.common.util

import java.io.{File, FileInputStream}
import java.util.UUID

import scala.util.Using

import org.aspen_ddp.aspen.AmoebaError
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

object YamlFormat {

  class FormatError(val msg: String) extends AmoebaError(msg)

  /** Loads and parses the YAML document contained in the given file.
    *
    * The document is fully parsed before the underlying stream is closed, and the stream is
    * closed whether parsing succeeds or fails.
    */
  def loadYamlFile(file: File): Object = {
    val doc = Using.resource(new FileInputStream(file)) { in =>
      new Yaml(new SafeConstructor).load[java.util.AbstractMap[Object, Object]](in)
    }

    if (doc == null)
      throw new FormatError(s"Empty YAML document: $file")

    doc
  }

  /** Parses a YAML document already held in memory.
    *
    * Exists so a config fetched over the network can be validated before it is written to disk.
    * Not folded into loadYamlFile: that overload wraps the stream in SnakeYAML's UnicodeReader,
    * which strips a BOM and detects UTF-8 vs UTF-16, whereas a String has already been decoded
    * by whoever produced it.
    *
    * Throws FormatError for an empty document, and for one that parses to something other than
    * a mapping. Malformed YAML raises SnakeYAML's own ParserException or ComposerException,
    * which is not a FormatError -- a caller guarding this must catch NonFatal, not FormatError.
    */
  def loadYamlString(yaml: String): Object = {
    val doc = new Yaml(new SafeConstructor).load[Object](yaml)

    if (doc == null)
      throw new FormatError("Empty YAML document")

    doc
  }

  trait Format[T] {
    def format(o: Object): T
  }

  object YBool extends Format[Boolean] {
    override def format(o: Object): Boolean = o match {
      case v: java.lang.Boolean => v.booleanValue()
      case _ => throw new FormatError(s"Boolean Required")
    }
  }

  object YString extends Format[String] {
    override def format(o: Object): String = o match {
      case v: java.lang.String => v
      case _ => throw new FormatError(s"String Required")
    }
  }

  object YFile extends Format[File] {
    override def format(o: Object): File = o match {
      case v: java.lang.String =>
        val f = new File(v)
        if (!f.exists())
          throw new FormatError(s"File Not Found: $v")
        f
      case _ => throw new FormatError(s"String Required")
    }
  }

  object YUUID extends Format[UUID] {
    override def format(o: Object): UUID = o match {
      case v: java.lang.String => try {
        UUID.fromString(v)
      } catch {
        case t: Throwable => throw new FormatError(s"Invalid UUID: $t")
      }
      case _ => throw new FormatError(s"String Required")
    }
  }

  object YInt extends Format[Int] {
    override def format(o: Object): Int = o match {
      case v: java.lang.Integer => v.intValue()
      case _ => throw new FormatError(s"Int Required")
    }
  }

  object YDouble extends Format[Double] {
    override def format(o: Object): Double = o match {
      case v: java.lang.Double => v.doubleValue()
      case _ => throw new FormatError(s"Double Required")
    }
  }

  case class YList[T](elementFormat: Format[T]) extends Format[List[T]] {
    def get(o: Object): List[T] = format(o)

    override def format(o: Object): List[T] = o match {
      case v: java.util.List[_] =>
        val jl = v.asInstanceOf[java.util.List[Object]]

        import scala.jdk.CollectionConverters._
        var index = 1

        jl.asScala.map { o =>
          try {
            val t = elementFormat.format(o)
            index += 1
            t
          } catch {
            case e: FormatError => throw new FormatError(s"list element $index => ${e.msg}")
          }
        }.toList

      case _ => throw new FormatError(s"List Required")
    }
  }

  trait Attr {
    val name: String
    val required: Boolean
  }

  case class Required[T](name: String, formatter: Format[T]) extends Attr {
    val required = true

    def get(o: Object): T = o match {
      case m: java.util.Map[_,_] =>
        val e = m.get(name)
        if (e == null)
          throw new FormatError(s"Missing required attribute $name")
        try {
          formatter.format(e.asInstanceOf[Object])
        } catch {
          case e: FormatError => throw new FormatError(s"$name => ${e.msg}")
        }
      case _ => throw new FormatError(s"Object Required")
    }
  }

  case class Optional[T](name: String, formatter: Format[T]) extends Attr {
    val required = false

    def get(o: Object): Option[T] = o match {
      case m: java.util.Map[_,_] =>
        val e = m.get(name)
        if (e == null)
          None
        else {
          try {
            Some(formatter.format(e.asInstanceOf[Object]))
          } catch {
            case e: FormatError => throw new FormatError(s"$name => ${e.msg}")
          }
        }
      case _ => throw new FormatError(s"Object Required")
    }
  }

  abstract class YObject[T] extends Format[T] {
    val attrs: List[Attr]

    val allowExtraKeys = false

    def create(o: Object): T

    def format(o: Object): T = o match {
      case m: java.util.Map[_,_] =>
        val requiredKeys = attrs.filter(a => a.required).map(a => a.name).toSet
        val allowedKeys = attrs.map(a => a.name).toSet

        @scala.annotation.tailrec
        def rget(i: java.util.Iterator[Object], keys: Set[String]): Set[String] = if (!i.hasNext) keys else {
          i.next() match {
            case s: String => rget(i, keys + s)
            case _ => throw new FormatError(s"Object attribute names must be strings")
          }
        }

        val keySet = rget(m.keySet().iterator().asInstanceOf[java.util.Iterator[Object]], Set())

        val extraKeys = keySet &~ allowedKeys
        val missingKeys = requiredKeys &~ keySet

        if (missingKeys.nonEmpty) throw new FormatError(s"Missing required keys: $missingKeys")
        if (!allowExtraKeys && extraKeys.nonEmpty) throw new FormatError(s"Unsupported keys: $extraKeys")

        create(o)

      case _ => throw new FormatError(s"Object Required")
    }
  }

  case class Choice[T](name: String, options: Map[String, Format[T]]) extends Format[T] with Attr {
    val required = true

    def get(o: Object): T = format(o)

    def format(o: Object): T = o match {
      case m: java.util.Map[_,_] =>
        val om = m.asInstanceOf[java.util.Map[Object,Object]]

        val o = om.get(name)

        if (o == null)
          throw new FormatError(s"Missing required attribute $name")

        if (!o.isInstanceOf[String])
          throw new FormatError(s"Missing $name must be a string")

        options.get(o.asInstanceOf[String]) match {
          case Some(formatter) =>
            om.remove(name)
            formatter.format(om)
          case None => throw new FormatError(s"Invalid value for attribute $name")
        }
      case _ => throw new FormatError(s"Object Required")
    }
  }

}
