package org.aspen_ddp.aspen.server

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Enforcement for the device I/O isolation project (spec section 9): control-side code under `server/host`,
 *  `server/rebuild` and `server/transfer` does no disk I/O itself. Device I/O lives in `*Disk` objects (and the
 *  DeviceIo classes), whose functions run as jobs on a device's lanes. Two rules, both over the source with comments
 *  and string literals blanked out:
 *
 *  1. Direct I/O: no os-lib filesystem call, `DurableFiles`, `FileStat`, `DeviceMarkers`, CRL save-file or
 *     `RebuildState` I/O, free-space query, or `CopyLayout` reading function outside the exempt files. Type references
 *     (`DurableFiles.MoveNotForced`) are not calls and are not counted.
 *  2. Synchronous job bodies: a call of a `*Disk` object's function (`XDisk.op(...)`) in a control file must be the
 *     argument of a lane helper (`laneHelpers`: `io.submit(kind, store)(XDisk.op(args))` and the wrappers that end in
 *     a submit), directly or as a block `({ ...; XDisk.op(args) })`. Anything else (a `Try(XDisk.op(...))`, a call in a
 *     lambda, a call on its own line) runs on the caller's thread and fails the suite. `pureDiskHelpers` are path
 *     computations that read nothing.
 *
 *  `allowed` names the files that may still do direct I/O, and `syncAllowed` those whose `*Disk` calls run on threads
 *  of their own, each with its reason; both must stay exact. */
class DeviceIoUsageSuite extends AnyFunSuite with Matchers:
  private val root = os.pwd / "src" / "main" / "scala" / "org" / "aspen_ddp" / "aspen" / "server"
  private val dirs = List("host", "rebuild", "transfer")
  private val forbidden =
    (raw"\bos\.(remove|move|list|read|write|makeDir|copy|walk|size|stat|exists|isDir|isFile|perms)\b|" +
     raw"\bDurableFiles\.[a-z]|\bFileStat\.[a-z]|\bDeviceMarkers\.[a-z]|saveStoreState|loadValidStoreState|" +
     raw"RebuildState\.(load|save|remove)|getFreeSpace|getTotalSpace|getUsableSpace|" +
     raw"\bCopyLayout\.(list|copiesOf|locate|phaseAt|readParam)\b").r
  private val diskCall = raw"\b([A-Z]\w*Disk)\.([a-z]\w*)\s*\(".r
  private val exemptNames = Set("DeviceIo.scala", "ThreadedDeviceIo.scala", "LockstepDeviceIo.scala")

  /** The functions whose arguments run as a job on a device lane: `DeviceIo.submit`, and the control helpers that
   *  submit their body (`DeviceDiscovery.submitFor`/`laneJob`/`laneFuture`, `StoreTransfersOut.onLane`,
   *  `DeviceShutdown.writeMarker`, `RebuildingStore.staging`) */
  private val laneHelpers = Set("submit", "submitFor", "laneJob", "laneFuture", "onLane", "writeMarker", "staging")

  /** `*Disk` functions that only compute a path */
  private val pureDiskHelpers = Set("RebuildBookkeepingDisk.stagingOf", "RebuildBookkeepingDisk.stagingRoot")

  /** Files that keep direct I/O, with the reason. Not device I/O: the host root is host-wide by nature (spec
   *  non-goals: a hung host root is a host failure) */
  private val allowed: Map[String, String] = Map(
    "host/JoinCeiling.scala" ->
      "host-root persistence of <host-root>/join-ceiling, written on the host's renew timer, never on a device")

  /** Files whose `*Disk` calls run on threads of their own, never a shared one, with the reason */
  private val syncAllowed: Map[String, String] = Map(
    "transfer/StoreTransferManager.scala" ->
      ("a receive's set-up and a send's temporary-directory removal run on that transfer's own connection thread (one " +
       "transfer, one device); every removal of a received directory is its TransferLane's job (reg.lane.run)"))

  private def sources: List[(String, String)] =
    for
      d <- dirs
      file <- os.walk(root / d).filter(_.ext == "scala").toList.sorted
      if !file.last.endsWith("Disk.scala") && !exemptNames.contains(file.last)
    yield file.relativeTo(root).toString -> os.read(file)

  /** `text` with comments and string literals replaced by spaces, newlines kept, so offsets and lines stay */
  private def strip(text: String): String =
    val out = new StringBuilder(text.length)
    var i = 0
    def blank(c: Char): Char = if c == '\n' then '\n' else ' '
    while i < text.length do
      val c = text(i)
      if c == '/' && i + 1 < text.length && text(i + 1) == '/' then
        while i < text.length && text(i) != '\n' do
          out += ' '
          i += 1
      else if c == '/' && i + 1 < text.length && text(i + 1) == '*' then
        var depth = 0
        var done = false
        while i < text.length && !done do
          if text.startsWith("/*", i) then
            depth += 1
            out ++= "  "
            i += 2
          else if text.startsWith("*/", i) then
            depth -= 1
            out ++= "  "
            i += 2
            if depth == 0 then done = true
          else
            out += blank(text(i))
            i += 1
      else if text.startsWith("\"\"\"", i) then
        val end = text.indexOf("\"\"\"", i + 3)
        val stop = if end < 0 then text.length else end + 3
        while i < stop do
          out += blank(text(i))
          i += 1
      else if c == '"' then
        out += ' '
        i += 1
        while i < text.length && text(i) != '"' && text(i) != '\n' do
          if text(i) == '\\' && i + 1 < text.length then
            out ++= "  "
            i += 2
          else
            out += ' '
            i += 1
        if i < text.length then
          out += blank(text(i))
          i += 1
      else if c == '\'' && i + 2 < text.length && text(i + 2) == '\'' then
        out ++= "   "
        i += 3
      else
        out += c
        i += 1
    out.toString

  private def lineOf(text: String, offset: Int): Int = text.substring(0, offset).count(_ == '\n') + 1

  private def prevNonWs(s: String, from: Int): Int =
    var j = from
    while j >= 0 && s(j).isWhitespace do j -= 1
    j

  private val closers = Map(')' -> '(', ']' -> '[', '}' -> '{')

  /** The index of the opener matching the closer at `close` */
  private def matchBack(s: String, close: Int): Int =
    var depth = 0
    var j = close
    while j >= 0 do
      val c = s(j)
      if closers.contains(c) then depth += 1
      else if closers.values.exists(_ == c) then
        depth -= 1
        if depth == 0 then return j
      j -= 1
    -1

  /** The name of the function whose argument list opens at `open`, skipping earlier argument lists */
  private def calleeAt(s: String, open: Int): Option[String] =
    var j = prevNonWs(s, open - 1)
    while j >= 0 && (s(j) == ')' || s(j) == ']') do
      j = prevNonWs(s, matchBack(s, j) - 1)
    val end = j + 1
    while j >= 0 && (s(j).isLetterOrDigit || s(j) == '_') do j -= 1
    Option.when(end > j + 1)(s.substring(j + 1, end))

  /** The function a call at `at` is an argument of: the innermost unmatched `(` before it (or a `{` block that is an
   *  argument, `f({ ... })` or `f(...) { ... }`); None where it is in no argument list */
  private def enclosingCallee(s: String, at: Int): Option[String] =
    var j = at - 1
    var depth = 0
    while j >= 0 do
      val c = s(j)
      if closers.contains(c) then depth += 1
      else if c == '(' || c == '{' || c == '[' then
        if depth > 0 then depth -= 1
        else if c == '(' then return calleeAt(s, j)
        else if c == '[' then return None
        else
          val p = prevNonWs(s, j - 1)
          if p >= 0 && s(p) == '(' then return calleeAt(s, p)
          else if p >= 0 && (s(p) == ')' || s(p).isLetterOrDigit) then return calleeAt(s, j)
          else return None
      j -= 1
    None

  private def violations: List[String] =
    sources.flatMap: (file, text) =>
      val code = strip(text)
      val lines = text.split("\n", -1)
      def at(offset: Int): String =
        val n = lineOf(code, offset)
        s"$file:$n: ${lines(n - 1).trim}"
      val direct =
        if allowed.contains(file) then Nil
        else forbidden.findAllMatchIn(code).map(m => at(m.start)).toList
      val sync = if syncAllowed.contains(file) then Nil else diskCall.findAllMatchIn(code).toList.flatMap: m =>
        val name = s"${m.group(1)}.${m.group(2)}"
        if pureDiskHelpers.contains(name) || enclosingCallee(code, m.start).exists(laneHelpers.contains) then None
        else Some(at(m.start) + s"  [$name not passed to a lane helper]")
      (direct ++ sync).distinct

  test("no device I/O in control code"):
    withClue("direct device I/O, or a *Disk call outside a lane job, in control code: ") {
      violations shouldBe empty
    }

  test("every allowed file still needs its entry"):
    val needed = sources.filter((f, text) => allowed.contains(f) && forbidden.findFirstIn(strip(text)).isDefined)
    needed.map(_._1).toSet should be(allowed.keySet)

  test("every file allowed synchronous *Disk calls still makes one"):
    val needed = sources.filter((f, text) => syncAllowed.contains(f) && diskCall.findFirstIn(strip(text)).isDefined)
    needed.map(_._1).toSet should be(syncAllowed.keySet)

  test("the scanner sees a synchronous *Disk call and accepts one passed to a lane"):
    def callees(src: String): List[Option[String]] =
      val code = strip(src)
      diskCall.findAllMatchIn(code).toList.map(m => enclosingCallee(code, m.start))
    callees("io.submit(JobKind.Move, Some(s))(CopyDisk.relocate(a, b))") should be(List(Some("submit")))
    callees("submitFor(p, b, k, \"x(\")(\n  DiscoveryDisk.probe(p))(_ => ()): r =>") should be(List(Some("submitFor")))
    callees("staging({ check(); RebuildDisk.complete(a) })") should be(List(Some("staging")))
    callees("val l = Try(CopyDisk.list(p))") should be(List(Some("Try")))
    callees("  CopyDisk.list(p)") should be(List(None))
    callees("xs.map: x =>\n  CopyDisk.list(x)") should be(List(None))
    callees("// CopyDisk.list(p)\n/* CopyDisk.list(p) */ \"CopyDisk.list(p)\"") should be(Nil)
