package org.aspen_ddp.aspen.amoebafs

import java.nio.charset.StandardCharsets

import org.aspen_ddp.aspen.common.objects.{Key, Value}

case class DirectoryEntry(name: String, pointer: InodePointer) {
  def key: Key = Key(name)

  def value: Array[Byte] = pointer.toArray
}

object DirectoryEntry {
  def apply(key: Key, v: Value): DirectoryEntry = {
    new DirectoryEntry(new String(key.bytes, StandardCharsets.UTF_8), InodePointer(v.bytes))
  }
}
