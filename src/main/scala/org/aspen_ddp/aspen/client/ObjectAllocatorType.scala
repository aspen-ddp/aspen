package org.aspen_ddp.aspen.client

enum ObjectAllocatorType(val code: Byte):
  case Pool  extends ObjectAllocatorType(0.toByte)
  case Group extends ObjectAllocatorType(1.toByte)
