package org.aspen_ddp.aspen

import scribe.handler.LogHandler

/** The one way tests add and remove scribe handlers.
  *
  * scribe's `Logger` is an immutable value: `Logger(name).withHandler(h).replace()` reads the current logger and writes
  * back a copy, with nothing between them. Suites run in parallel in one sbt JVM, so two unserialized swaps of the same
  * logger lose one of the handlers, and the test that added it misses its lines. Every swap here holds one lock, and
  * each targets a named logger, never the root: the root is also replaced outside this helper (`TestLoggingConfig`,
  * `Main.configureLogging`).
  */
object LogCapture:
  /** The logger every Aspen class's logger descends from, for assertions about all the lines Aspen logs */
  val Aspen: String = "org.aspen_ddp.aspen"

  def add(loggerName: String, handler: LogHandler): Unit = synchronized:
    scribe.Logger(loggerName).withHandler(handler).replace()

  def remove(loggerName: String, handler: LogHandler): Unit = synchronized:
    scribe.Logger(loggerName).withoutHandler(handler).replace()

  /** Runs `f` with `handler` on the logger named `loggerName` */
  def during[A](loggerName: String, handler: LogHandler)(f: => A): A =
    add(loggerName, handler)
    try f
    finally remove(loggerName, handler)
