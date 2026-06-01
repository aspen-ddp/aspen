# Scribe Logging Migration Design

## Summary

Replace the current Log4j 2 + SLF4J logging stack with Scribe, a Scala-native logging library. The migration is a direct 1:1 swap — same trait mixin pattern, same log method signatures, programmatic configuration instead of XML/properties files.

## Current State

- **Dependencies:** `log4j-api`, `log4j-core`, `log4j-api-scala`, `slf4j-log4j12`
- **Usage:** 44 files import `org.apache.logging.log4j.scala.Logging` and mixin the trait
- **Log statements:** ~201 calls across `trace`, `debug`, `info`, `warn`, `error`
- **Configuration:** Two config files in `demo/` (`log4j-conf.xml`, `log4j.properties`), loaded via `setLog4jConfigFile()` in `Main.scala` which sets system properties for async logging and config file path
- **No custom wrappers** — all logging goes directly through the Log4j Scala API trait

## Design

### 1. Dependency Changes

In `build.sbt`, remove:

```sbt
"org.apache.logging.log4j" % "log4j-api" % "2.22.0"
"org.apache.logging.log4j" % "log4j-core" % "2.22.0"
"org.apache.logging.log4j" %% "log4j-api-scala" % "13.1.0"
"org.slf4j" % "slf4j-log4j12" % "2.0.12"
```

Add:

```sbt
"com.outr" %% "scribe" % "3.19.0"
"com.outr" %% "scribe-slf4j" % "3.19.0"
```

`scribe-slf4j` provides the SLF4J backend so third-party libraries (e.g. `nfs4j-core`) that log via SLF4J have their output routed through Scribe.

### 2. Import Migration

Across 44 files, change imports:

| Current | New |
|---|---|
| `import org.apache.logging.log4j.scala.Logging` | `import scribe.Logging` |
| `import org.apache.logging.log4j.scala.{Logger, Logging}` | `import scribe.Logging` |
| `import org.apache.logging.log4j.scala.Logger` | Remove (unused — `logger` inherited from parent's `Logging` mixin) |

Class declarations (`extends Logging`, `with Logging`) and all log statements (`logger.trace(...)`, `logger.info(...)`, etc.) remain unchanged — Scribe's `Logging` trait provides the same `logger` instance with identical method signatures.

### 3. Configuration

**Delete:**
- `demo/log4j.properties`
- `demo/log4j-conf.xml`

**Replace `setLog4jConfigFile` in `Main.scala`** with a Scribe programmatic configuration method:

```scala
import scribe._

def configureLogging(): Unit =
  Logger.root
    .clearHandlers()
    .withHandler(minimumLevel = Some(Level.Trace))
    .replace()
```

This replicates the current behavior: trace-level root logger with console output. Scribe's default formatter includes timestamp, thread, level, class, method, line number, and message.

Update all call sites of `setLog4jConfigFile(f)` in `Main.scala` (6 locations: `run_debug_code`, `amoeba_server`, `rebuild`, `new_pool`, `transfer_store`, `host`) to call `configureLogging()` instead. Remove the `log4j-config-file` command-line argument from these commands.

### 4. System Property Cleanup

Remove the two `System.setProperty` calls in the current `setLog4jConfigFile`:
- `log4j2.contextSelector`
- `log4j2.configurationFile`

These are Log4j-specific and not needed by Scribe.

## Scope

- **44 files** with import changes
- **1 file** (`Main.scala`) with configuration changes at 6 call sites
- **2 files** deleted (config files)
- **1 file** (`build.sbt`) with dependency changes
- **0 test files** affected (no logging in tests)
