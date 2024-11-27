package org.aspen_ddp.aspen.common.objects

sealed abstract class ReadType

case class MetadataOnly() extends ReadType

case class FullObject() extends ReadType

case class ByteRange(offset: Int, length: Int) extends ReadType

case class SingleKey(key: Key, ordering: KeyOrdering) extends ReadType

case class LargestKeyLessThan(key: Key, ordering: KeyOrdering) extends ReadType

case class LargestKeyLessThanOrEqualTo(key: Key, ordering: KeyOrdering) extends ReadType

case class KeyRange(minimum: Key, maximum: Key, ordering: KeyOrdering) extends ReadType
