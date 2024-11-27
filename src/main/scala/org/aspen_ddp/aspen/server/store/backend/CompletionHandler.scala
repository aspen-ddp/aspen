package org.aspen_ddp.aspen.server.store.backend

trait CompletionHandler {
  def complete(op: Completion): Unit
}
