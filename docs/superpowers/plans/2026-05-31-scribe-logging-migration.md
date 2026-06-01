# Scribe Logging Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Log4j 2 + SLF4J logging stack with Scribe, a Scala-native logging library.

**Architecture:** Direct 1:1 swap of the logging trait mixin pattern. All 44 files that import `org.apache.logging.log4j.scala.Logging` switch to `import scribe.Logging`. Log call sites (`logger.trace/debug/info/warn/error`) are unchanged. Configuration moves from XML/properties files to programmatic Scala code. The `HostConfig` case class and YAML configs lose their `log4j-config` field.

**Tech Stack:** Scribe 3.19.0, scribe-slf4j 3.19.0

---

### Task 1: Update build.sbt dependencies

**Files:**
- Modify: `build.sbt:25-28`

- [ ] **Step 1: Replace logging dependencies**

In `build.sbt`, replace these four lines:

```scala
"org.apache.logging.log4j"         %  "log4j-api"               % "2.22.0",
"org.apache.logging.log4j"         %  "log4j-core"              % "2.22.0",
"org.apache.logging.log4j"         %% "log4j-api-scala"         % "13.1.0",
"org.slf4j"                        %  "slf4j-log4j12"           % "2.0.12",
```

With:

```scala
"com.outr"                         %% "scribe"                  % "3.19.0",
"com.outr"                         %% "scribe-slf4j"            % "3.19.0",
```

- [ ] **Step 2: Verify dependency resolution**

Run: `sbt update`
Expected: Dependencies resolve successfully with no errors.

- [ ] **Step 3: Commit**

```bash
git add build.sbt
git commit -m "build: replace log4j/slf4j dependencies with scribe"
```

---

### Task 2: Migrate imports in all 44 source files

**Files (41 files with `import org.apache.logging.log4j.scala.Logging`):**
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/Directory.scala:8`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/FileSystem.scala:9`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/Inode.scala:13`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleBaseFile.scala:6`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleDirectory.scala:10`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileHandle.scala:6`
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/nfs/AmoebaNFS.scala:12`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/FinalizationAction.scala:5`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/AllocationFinalizationAction.scala:8`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseObjectReader.scala:11`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/BaseReadDriver.scala:10`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/read/ReadManager.scala:13`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/ClientTransactionDriver.scala:10`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/SimpleClientTransactionDriver.scala:7`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/transaction/TransactionImpl.scala:13`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/KeyValueListNode.scala:11`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/tkvl/TieredKeyValueList.scala:7`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/metadata/management/UpdateAllocationGroupUsageTask.scala:3`
- Modify: `src/main/scala/org/aspen_ddp/aspen/common/network/Codec.scala:5`
- Modify: `src/main/scala/org/aspen_ddp/aspen/compute/SteppedDurableTask.scala:3`
- Modify: `src/main/scala/org/aspen_ddp/aspen/demo/Main.scala:40`
- Modify: `src/main/scala/org/aspen_ddp/aspen/demo/network/ZCnCBackend.scala:13`
- Modify: `src/main/scala/org/aspen_ddp/aspen/demo/network/ZCnCFrontend.scala:6`
- Modify: `src/main/scala/org/aspen_ddp/aspen/demo/network/ZMQNetwork.scala:13`
- Modify: `src/main/scala/org/aspen_ddp/aspen/demo/network/ZStoreTransferBackend.scala:6`
- Modify: `src/main/scala/org/aspen_ddp/aspen/demo/network/ZStoreTransferFrontend.scala:5`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/LogEntry.scala:3`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/Recovery.scala:6`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/SimpleCRL.scala:6`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/Stream.scala:3`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/crl/simple/StreamWriter.scala:3`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/backend/Backend.scala:8`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Frontend.scala:14`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/store/Store.scala:13`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/StoreManager.scala:17`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/TransactionDriver.scala:12`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/Tx.scala:12`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transfer/TransferringIn.scala:3`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transfer/TransferringOut.scala:3`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/usage/StorageDeviceUsageManager.scala:7`
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/usage/StoragePoolUsageManager.scala:12`

**Files (2 files with `import org.apache.logging.log4j.scala.{Logger, Logging}`):**
- Modify: `src/main/scala/org/aspen_ddp/aspen/amoebafs/impl/simple/SimpleFileContent.scala:12`
- Modify: `src/main/scala/org/aspen_ddp/aspen/client/internal/allocation/DeletionFinalizationAction.scala:7`

**Files (1 file with standalone `import org.apache.logging.log4j.scala.Logger` — unused import):**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/transaction/SimpleTransactionDriver.scala:9`

- [ ] **Step 1: Replace all `import org.apache.logging.log4j.scala.Logging` with `import scribe.Logging`**

Run this sed command across all 41 files:

```bash
find src -name "*.scala" -exec grep -l "import org.apache.logging.log4j.scala.Logging$" {} \; | xargs sed -i '' 's/import org.apache.logging.log4j.scala.Logging/import scribe.Logging/'
```

- [ ] **Step 2: Replace `import org.apache.logging.log4j.scala.{Logger, Logging}` with `import scribe.Logging`**

In `SimpleFileContent.scala` and `DeletionFinalizationAction.scala`:

```bash
find src -name "*.scala" -exec grep -l "import org.apache.logging.log4j.scala.{Logger, Logging}" {} \; | xargs sed -i '' 's/import org.apache.logging.log4j.scala.{Logger, Logging}/import scribe.Logging/'
```

- [ ] **Step 3: Remove unused `import org.apache.logging.log4j.scala.Logger` from SimpleTransactionDriver.scala**

In `src/main/scala/org/aspen_ddp/aspen/server/transaction/SimpleTransactionDriver.scala`, delete line 9:

```
import org.apache.logging.log4j.scala.Logger
```

This import is unused — `SimpleTransactionDriver` inherits `logger` from its parent class `TransactionDriver` which mixes in `Logging`.

- [ ] **Step 4: Verify no log4j imports remain**

Run: `grep -rn "org.apache.logging.log4j" src --include="*.scala"`
Expected: No output (zero matches).

- [ ] **Step 5: Commit**

```bash
git add src/
git commit -m "refactor: migrate all logging imports from log4j to scribe"
```

---

### Task 3: Remove log4j-config from HostConfig and YAML configs

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/server/HostConfig.scala`
- Modify: `demo/node_a.yaml`
- Modify: `demo/node_b.yaml`
- Modify: `demo/node_c.yaml`

- [ ] **Step 1: Remove `log4jConfigFile` from HostConfig case class**

In `src/main/scala/org/aspen_ddp/aspen/server/HostConfig.scala`, remove the `log4jConfigFile: File` parameter from the case class (line 27) and all its usages:

The case class changes from:

```scala
case class HostConfig(hostId: HostId,
                      aspenSystemId: UUID,
                      name: String,
                      address: String,
                      dataPort: Int,
                      cncPort: Int,
                      storeTransferPort: Int,
                      log4jConfigFile: File,
                      crl: HostConfig.CRLBackend):
```

To:

```scala
case class HostConfig(hostId: HostId,
                      aspenSystemId: UUID,
                      name: String,
                      address: String,
                      dataPort: Int,
                      cncPort: Int,
                      storeTransferPort: Int,
                      crl: HostConfig.CRLBackend):
```

- [ ] **Step 2: Remove log4j-config from yamlConfig string**

In the `yamlConfig` method, remove the `log4j-config` line. Change:

```scala
def yamlConfig: String =
    val base = s"""hostState-id: $hostId
       |aspen-system-id: $aspenSystemId
       |name: $name
       |address: $address
       |data-port: $dataPort
       |cnc-port: $cncPort
       |store-transfer-port: $storeTransferPort
       |log4j-config: $log4jConfigFile
       |crl:
       |""".stripMargin
```

To:

```scala
def yamlConfig: String =
    val base = s"""hostState-id: $hostId
       |aspen-system-id: $aspenSystemId
       |name: $name
       |address: $address
       |data-port: $dataPort
       |cnc-port: $cncPort
       |store-transfer-port: $storeTransferPort
       |crl:
       |""".stripMargin
```

- [ ] **Step 3: Remove log4jConf from companion object**

In the `HostConfig` companion object, remove the `log4jConf` field declaration (line 76):

```scala
val log4jConf: Required[File]        = Required("log4j-config", YFile)
```

Remove `log4jConf` from the `attrs` list (line 87). Change:

```scala
val attrs: List[Attr] = List(
    hostId,
    aspenSystemId,
    name,
    address,
    dataPort,
    cncPort,
    storeTransferPort,
    log4jConf,
    crl
  )
```

To:

```scala
val attrs: List[Attr] = List(
    hostId,
    aspenSystemId,
    name,
    address,
    dataPort,
    cncPort,
    storeTransferPort,
    crl
  )
```

Remove `log4jConf.get(o)` from the `create` method (line 99). Change:

```scala
def create(o: Object): HostConfig = HostConfig(
    hostId.get(o),
    aspenSystemId.get(o),
    name.get(o),
    address.get(o),
    dataPort.get(o),
    cncPort.get(o),
    storeTransferPort.get(o),
    log4jConf.get(o),
    crl.get(o)
  )
```

To:

```scala
def create(o: Object): HostConfig = HostConfig(
    hostId.get(o),
    aspenSystemId.get(o),
    name.get(o),
    address.get(o),
    dataPort.get(o),
    cncPort.get(o),
    storeTransferPort.get(o),
    crl.get(o)
  )
```

- [ ] **Step 4: Remove the `import java.io.File` if it becomes unused**

Check if `java.io.File` is still used elsewhere in HostConfig.scala. If the only usage was `log4jConfigFile: File` and `YFile`, remove the import. If `File` is still used (e.g., in `loadHostConfig`), keep it.

`loadHostConfig` takes a `File` parameter, so the import stays.

- [ ] **Step 5: Remove `log4j-config` from YAML config files**

In `demo/node_a.yaml`, `demo/node_b.yaml`, and `demo/node_c.yaml`, delete the line:

```yaml
log4j-config: demo/log4j-conf.xml
```

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/server/HostConfig.scala demo/node_a.yaml demo/node_b.yaml demo/node_c.yaml
git commit -m "refactor: remove log4j-config from HostConfig and YAML configs"
```

---

### Task 4: Update Main.scala — replace log4j configuration with Scribe

**Files:**
- Modify: `src/main/scala/org/aspen_ddp/aspen/demo/Main.scala`

- [ ] **Step 1: Replace `setLog4jConfigFile` with `configureLogging`**

Remove the existing method (lines 125-128):

```scala
def setLog4jConfigFile(f: File): Unit =
    // Set all loggers to Asynchronous Logging
    System.setProperty("log4j2.contextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector")
    System.setProperty("log4j2.configurationFile", s"file:${f.getAbsolutePath}")
```

Replace with:

```scala
def configureLogging(): Unit =
    scribe.Logger.root
      .clearHandlers()
      .withHandler(minimumLevel = Some(scribe.Level.Trace))
      .replace()
```

- [ ] **Step 2: Remove `log4jConfigFile` from Args case class**

In the `Args` case class (line 96), remove:

```scala
log4jConfigFile: File=null,
```

- [ ] **Step 3: Remove `log4j-config-file` argument from all command parsers**

Remove all instances of:

```scala
arg[File]("<log4j-config-file>").text("Log4j Configuration File").
    action( (x, c) => c.copy(log4jConfigFile=x)).
```

These appear at lines 164-165, 188-189, 196-197, 223-224, and 260-261.

- [ ] **Step 4: Update method signatures to remove `log4jConfigFile` parameter**

Change each method that takes `log4jConfigFile: File`:

- `run_debug_code(log4jConfigFile: File, cfg: BootstrapConfig.Config)` → `run_debug_code(cfg: BootstrapConfig.Config)`
- `OLD_run_debug_code(log4jConfigFile: File, cfg: BootstrapConfig.Config)` → `OLD_run_debug_code(cfg: BootstrapConfig.Config)`
- `amoeba_server(log4jConfigFile: File, cfg: BootstrapConfig.Config)` → `amoeba_server(cfg: BootstrapConfig.Config)`
- `rebuild(log4jConfigFile: File, storeName: String, cfg: BootstrapConfig.Config)` → `rebuild(storeName: String, cfg: BootstrapConfig.Config)`
- `new_pool(log4jConfigFile: File, ...)` → `new_pool(...)` (remove first param)
- `transfer_store(log4jConfigFile: File, ...)` → `transfer_store(...)` (remove first param)

- [ ] **Step 5: Replace `setLog4jConfigFile` calls with `configureLogging()` and remove print statements**

In each method body, replace:

```scala
println(s"LOG4J CONFIG $log4jConfigFile")
setLog4jConfigFile(log4jConfigFile)
```

With:

```scala
configureLogging()
```

The `host` method at line 630 calls `setLog4jConfigFile(hostCfg.log4jConfigFile)` — replace with `configureLogging()`.

- [ ] **Step 6: Update call sites in the `main` match expression**

At lines 300-304, update to remove `cfg.log4jConfigFile` from each call:

```scala
case "amoeba" => amoeba_server(bootstrapConfig)
case "debug" => run_debug_code(bootstrapConfig)
case "rebuild" => rebuild(cfg.storeName, bootstrapConfig)
case "new-pool" => new_pool(bootstrapConfig, cfg.newPoolName, cfg.idaType, cfg.width, cfg.readThreshold, cfg.writeThreshold, cfg.hosts)
case "transfer-store" => transfer_store(bootstrapConfig, cfg.storeName, cfg.host)
```

- [ ] **Step 7: Remove log4j-conf.xml reference in bootstrap HostConfig creation**

At line 737, remove `new File("../log4j-conf.xml")` from the `HostConfig(...)` constructor call. The parameter list should go from 9 arguments to 8, matching the updated case class.

- [ ] **Step 8: Remove unnecessary File import if applicable**

Check if `java.io.File` is still used in Main.scala after these changes. It likely is (for other config files), so keep it.

- [ ] **Step 9: Commit**

```bash
git add src/main/scala/org/aspen_ddp/aspen/demo/Main.scala
git commit -m "refactor: replace log4j configuration with scribe in Main.scala"
```

---

### Task 5: Delete log4j configuration files

**Files:**
- Delete: `demo/log4j.properties`
- Delete: `demo/log4j-conf.xml`

- [ ] **Step 1: Delete the log4j configuration files**

```bash
rm demo/log4j.properties demo/log4j-conf.xml
```

- [ ] **Step 2: Verify no remaining references to these files**

Run: `grep -rn "log4j" demo/ src/ build.sbt`
Expected: No output (zero matches). If any matches remain, they are bugs from prior tasks — fix them.

- [ ] **Step 3: Commit**

```bash
git add demo/log4j.properties demo/log4j-conf.xml
git commit -m "chore: delete log4j configuration files"
```

---

### Task 6: Compile and test

- [ ] **Step 1: Compile the project**

Run: `sbt compile`
Expected: Compilation succeeds with no errors. If there are compilation errors, they will be from missed import changes or API differences — fix them.

- [ ] **Step 2: Run the test suite**

Run: `sbt test`
Expected: All tests pass. No tests use logging directly, so no test changes should be needed.

- [ ] **Step 3: Fix any issues and commit**

If any compilation or test errors occurred, fix and commit:

```bash
git add -A
git commit -m "fix: resolve compilation issues from scribe migration"
```
