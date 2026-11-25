# Host threading: before/after measurement

The procedure for the measurement that closes the host refactor
(`docs/superpowers/specs/2026-11-05-host-refactor-design.md`, "Suggested order of work", step 5). It compares
BackendTiming's "store commit (FsifBackend)" figure before the refactor (`main`) and after it (`host-refactor`).

There is no numeric target. The refactor ships either way, because the single-thread ceiling must go regardless. If
the figure does not improve, add what the thread dumps and timing show to the "Store commit delay" entry in
`Todo.txt` as the next investigation.

The runs need root and the TCMU setup, so nothing here runs automatically. Steps 1 and 2 are done by hand;
`scripts/host-threading/capture-job.sh` runs each fio job and captures its data, and
`scripts/host-threading/analyze.py` does steps 3 to 5 from the captures.

## Setup: the same as the Todo.txt runs

Use the same hardware and Aspen system as the runs behind the "Store commit delay" entry in `Todo.txt`:
- one host, with a 16-store RS 16/10/12 FSIF pool, all 16 stores on one disk;
- one vdisk in that pool, served by `aspen vdisk-server` and mounted through `scripts/vdisk-e2e/setup.sh`.

The prerequisites are in `scripts/vdisk-e2e/README.md` and `docs/vdisk-tcmu-setup.md`. Keep everything except the
host build the same between the two runs: the system, pool, disk, mount, vdisk-server build and fio jobs. Leave the
machine otherwise idle.

## 1. Build both versions

Build each in its own checkout so that switching is only a matter of which host you start:

    git worktree add ../aspen-before main
    git worktree add ../aspen-after host-refactor
    (cd ../aspen-before && ./aspen --help)
    (cd ../aspen-after && ./aspen --help)

`./aspen --help` compiles and caches the launcher's classpath, so neither run invokes sbt.

## 2. Run the fio jobs, once per build

Pick a results directory (`results` below) and use it for both builds. For each build, `before` first, then `after`:

1. Stop any running host. Start the host from that build's checkout: `./aspen host <host-directory>`.
2. If the vdisk is not set up and mounted yet, run `scripts/vdisk-e2e/setup.sh` as its README shows. It also runs
   `smoke.fio` once, which warms the disk; that run is not measured.
3. Run the two jobs of `scripts/vdisk-e2e/smoke.fio`, the ones used for the `Todo.txt` numbers, one at a time.
   Each is time based and runs for 60 s. Run the script as your own user, not under sudo (it asks for your password
   once, for fio):

       scripts/host-threading/capture-job.sh results before randrw-verify /mnt/vd1
       scripts/host-threading/capture-job.sh results before seqwrite /mnt/vd1

   (`after` in place of `before` for the second build.)

`capture-job.sh` finds the running host JVM (`HOST_PID` overrides it) and records the commit of the checkout it runs
from, so `analyze.py` shows which build each capture measured. It runs fio through sudo and keeps fio's summary,
takes 3 thread dumps 10 s apart starting 15 s into the job, and then copies the part of `<host-directory>/logs/host.log`
written during the job (`HOST_LOG` overrides the log's path). Each capture goes to
`results/<build>/<section>/`; the script refuses to overwrite one.

To compare store-thread counts, start the `after` host with `--store-threads=<n>` and give each count its own label,
for example `after-2t` and `after-4t`. The option caps the store threads. Builds before the adaptive pool ran exactly
one per CPU by default and placed stores by count. Since then the host starts with one thread and adds more, up to the
cap (by default a quarter of the CPUs), as its stores' load needs them. `capture-job.sh` records the host's setting,
and `analyze.py` shows it together with the store threads the dumps show in use. Builds are ordered `before` first,
then from the most threads in use to the fewest.

## 3 to 5. Analyse

After both builds are captured:

    scripts/host-threading/analyze.py results

It prints a Markdown report and writes it to `results/report.md`. It needs only Python 3's standard library. It
reports the following.

- BackendTiming. While writes happen, the host logs one BackendTiming line about once a second
  (`server.store.backend.BackendTiming`). Over the lines whose periods lie wholly between 5 s and 55 s after fio's
  job start, it gives each stage's count, count-weighted average and max. The window skips ramp-up, and its start
  comes from fio's own `job_start`. `--window-start`/`--window-end` change it, and both builds use the same window.
  The gap between "store commit (FsifBackend)" and "fsif commit" is the time a commit's completion spends outside
  FSIF, and the refactor targets that gap.
- fio's summary: IOPS, bandwidth, and completion latency (average and p99) for reads and writes, plus fsync latency.
  fsync is the FLUSH that makes writes durable, so it waits on commits.
- Per-thread CPU seconds per 10 s, between consecutive dumps, from the `cpu=...ms` in each thread's header:
  - after (`host-refactor`): `aspen-host-control` and each `aspen-store-N`, and their sum;
  - before (`main`): the thread running Host's event loop, which is the one whose stacks are in `Host.handleEvent`
    (an unnamed `pool-N-thread-1`, "pool-3-thread-1" in the `Todo.txt` runs);
  - for both builds, the 10 busiest other threads, for example FSIF's `serial` executor and the network threads.
- The stacks in all 3 dumps of every thread using more than 8 s per 10 s (`--busy` changes this limit).
- The locks that threads were blocked on in each dump: monitors for BLOCKED threads, and explicit locks for parked
  threads. This shows whether the stores' commits now queue elsewhere, for example in `HostObjectCache` (a
  `synchronized` cache shared by every store, the expected next contention point).
- Warnings, for example a missing capture, a dump taken outside the job, a window with too few BackendTiming lines,
  or a build whose sections ran on different commits.

Then update the "Store commit delay" entry in `Todo.txt` from the report. Include the commits, "store commit
(FsifBackend)" and "fsif commit", the per-thread CPU, and fio's summary. If "store commit (FsifBackend)" does not
improve, also add the busiest threads and where they spend their time as the next investigation.

## Status

The measurements are summarised under "Store commit delay" and "Adaptive store threading" in `Todo.txt`. The
default is now a fixed pool of min(4, CPUs/4) store threads, stores spread over them by count; `--store-threads=<n>`
sets the count. (Per-device I/O: `--device-io-threads=<n>` data threads per storage device, default 4 (was max(4, CPUs/2) before the FSIF device journal), and
`--device-read-bound=<n>` outstanding backend reads before a device's stores drop new client reads, default 4096.) The adaptive policy (`StoreThreadPool.Adaptive`) is parked and can only be chosen in code: to measure
it again, pass `adaptive = Some(StoreThreadPool.Adaptive())` where `Main.startHost` builds the pool. It packs stores
onto threads by load and adds threads only when work waits for them. Warm the pool up before capturing, as its runs
in `results/` (`after-adaptive`, `after-packed`) did: run each job uncaptured until the host log shows no
"Added store thread" or "Moved store" lines during it, then capture straight away. Any idle time in between lets
it repack.

## Device I/O isolation: before/after (2026-11-24)

Closes `docs/superpowers/specs/2026-11-23-device-io-isolation-design.md` (section 8, goal G5: no throughput or
latency regression on the single-device benchmarks beyond noise). Before is `fd82a6f3` (the branch's start, before any
device-I/O change); after is `device-threads` at the commit that sets the default below.

**G5 status.** FSIF met at the default. RocksDB randrw without fsync is 15-25% below baseline at the default
(understood: a commit-rate effect on a co-located vdisk-server). Single-read latency is equal after the vdisk fix.
The user accepted this on 2026-11-24. The remedies are in `Backlog.txt`. The tables and the "Reading them" section
below were taken before the vdisk fix; where their conclusions differ from "RocksDB latency follow-up" at the end of
this section, the follow-up supersedes them.

### What was run

Same hardware as the runs above (24 CPUs, NVMe, every store directory on one filesystem) and the same jobs
(`scripts/vdisk-e2e/smoke.fio`: the `randrw-verify` and `seqwrite` jobs, 60 s each, time based), through the real path:
`aspen vdisk-server` (always the `device-threads` build) -> TCMU -> iSCSI loopback -> ext4 -> fio. Not run: the
`capture-job.sh` thread dumps and `analyze.py` (a fio-only comparison needed neither); the numbers are fio's own JSON
(`--output-format=json`, `--section=<job>`). For each configuration and each tree:

1. fresh system in an empty directory: `aspen bootstrap` (replication 2/3/3), `aspen host` (the tree under test, no
   options unless a row says so), a 30 s wait (a freshly loaded store refuses transactions older than its forget
   floor, so a CLI call in the first seconds hangs until the takeover);
2. `create-allocation-group agroup 0 random-access-reed-solomon 16 10 12 --max-slice-size 524288`; `create-pool pool1
   random-access-reed-solomon 16 10 12 bootstrap <backend>` (`--backend fsif --max-object-size 5242880
   --segment-count 10240`, or `--backend rocksdb`), `add-pool-to-group`; **kill -9 and restart the host** (the pool's
   stores are created only when the host loads the device state; the creating CLI's nudge did not reach an idle host
   on either tree), then 30 s, then `create-disk vd1 10G --group agroup`;
3. two devices: a second level-0 device set `set2` with a second device directory `dev2` (registered with
   `create-storage-device`, same filesystem), a second 16-store pool `pool2` on `set2`, both pools in `agroup`, so the
   disk's objects spread over both devices. (Putting `dev2` in the `bootstrap` set makes the host rebalance bootstrap
   stores onto it, which wedges the bootstrap pool, a write threshold 3 of 3, on both trees);
4. `scripts/vdisk-e2e/setup.sh` (tcmu-runner stopped, `MKFS=1 SMOKE=0`), one unmeasured full `smoke.fio` pass, then
   `randrw-verify` and `seqwrite` three times each (six fio runs, alternating). Values below are the mean of the
   three runs, then the range (min-max). fsync is fio's FLUSH latency.

Warning: a TCMU SYNCHRONIZE CACHE that takes over 30 s makes the initiator time out and ext4 go read-only. This is
pre-existing and seen on both trees. Avoid it by laying out small files one at a time, not many at once.

The two trees ran back to back on the same disk, in alternating order. A second before run of the single-device FSIF case, hours later, came out 15 percent faster than
the first: **session-to-session drift is as large as the range inside one session**, so a result counts as a change
only when it is clear of both.

### Results

**FSIF, one device (16-store pool on one device)**

| metric | before | before, 2nd session | after, 2 threads | after, 4 threads | after, 8 threads | after, 12 threads |
|---|---|---|---|---|---|---|
| randrw-verify read IOPS | 510 (502-524) | 585 (559-624) | 322 (312-338) | 409 (389-420) | 430 (419-450) | 528 (506-557) |
| randrw-verify write IOPS | 267 (264-270) | 308 (302-318) | 165 (160-170) | 212 (201-218) | 221 (215-230) | 273 (265-285) |
| randrw read clat avg ms | 6.8 (5.7-8.2) | 5.9 (5.4-6.9) | 2.4 (2.2-2.6) | 2.2 (2.2-2.3) | 2.1 (1.8-2.6) | 2.2 (2.1-2.3) |
| randrw read clat p99 ms | 78 (73-90) | 71 (64-83) | 24 (23-24) | 23 (22-24) | 32 (31-33) | 23 (22-26) |
| randrw write clat avg ms | 0.36 (0.33-0.40) | 0.24 (0.23-0.27) | 0.14 (0.14-0.14) | 0.13 (0.13-0.14) | 0.16 (0.15-0.16) | 0.13 (0.12-0.13) |
| randrw fsync avg ms | 540 (429-619) | 472 (372-526) | 1434 (1402-1485) | 1094 (1063-1154) | 1045 (1021-1089) | 816 (766-846) |
| seqwrite MiB/s | 107.5 (106.8-108.3) | 115.5 (114.9-116.3) | 66.6 (65.0-69.6) | 90.6 (90.5-90.6) | 100.9 (89.9-113.4) | 130.4 (129.5-131.8) |
| seqwrite write clat avg ms | 2.55 (2.42-2.73) | 1.74 (1.67-1.86) | 1.77 (1.68-1.87) | 1.85 (1.79-1.94) | 2.05 (1.77-2.23) | 1.91 (1.87-1.93) |
| seqwrite fsync avg ms | 1104 (1095-1115) | 1047 (1035-1056) | 1860 (1777-1902) | 1347 (1344-1352) | 1209 (1066-1348) | 915 (905-921) |

**FSIF, two devices (one 16-store pool on each)**

| metric | before | after, 4 threads | after, default (12) |
|---|---|---|---|
| randrw-verify read IOPS | 499 (484-522) | 386 (353-419) | 500 (464-550) |
| randrw-verify write IOPS | 255 (245-267) | 200 (182-217) | 257 (243-278) |
| randrw read clat avg ms | 3.6 (3.2-4.1) | 1.8 (1.0-2.1) | 2.6 (2.6-2.7) |
| randrw read clat p99 ms | 42 (38-46) | 24 (18-30) | 41 (39-45) |
| randrw write clat avg ms | 0.22 (0.20-0.25) | 0.15 (0.15-0.16) | 0.17 (0.16-0.17) |
| randrw fsync avg ms | 792 (706-856) | 1202 (1136-1302) | 853 (770-905) |
| seqwrite MiB/s | 122.4 (121.8-123.2) | 88.7 (87.1-90.9) | 125.9 (122.5-128.2) |
| seqwrite write clat avg ms | 1.95 (1.88-2.05) | 1.77 (1.73-1.80) | 2.03 (1.98-2.09) |
| seqwrite fsync avg ms | 978 (972-982) | 1380 (1343-1406) | 946 (928-975) |

**RocksDB, one device**

| metric | before | after, 4 threads | after, 8 threads | after, default (12) |
|---|---|---|---|---|
| randrw-verify read IOPS | 98 (98-99) | 107 (105-109) | 115 (114-115) | 137 (132-145) |
| randrw-verify write IOPS | 51 (51-51) | 59 (57-60) | 66 (66-67) | 72 (68-79) |
| randrw read clat avg ms | 5.5 (5.2-5.7) | 7.7 (7.0-8.4) | 10.5 (9.8-11.0) | 10.4 (4.7-13.2) |
| randrw read clat p99 ms | 100 (83-120) | 151 (135-167) | 184 (173-194) | 188 (120-225) |
| randrw write clat avg ms | 0.30 (0.29-0.32) | 0.31 (0.30-0.33) | 0.46 (0.46-0.46) | 0.56 (0.49-0.64) |
| randrw fsync avg ms | 4791 (4764-4834) | 3985 (3903-4114) | 3335 (3304-3370) | 2989 (2919-3027) |
| seqwrite MiB/s | 67.9 (67.5-68.4) | 78.0 (75.9-80.4) | 88.8 (85.7-90.7) | 95.8 (95.4-96.2) |
| seqwrite write clat avg ms | 2.01 (1.97-2.07) | 2.12 (2.08-2.16) | 2.30 (2.12-2.43) | 2.69 (2.59-2.74) |
| seqwrite fsync avg ms | 1813 (1794-1824) | 1567 (1517-1610) | 1359 (1328-1418) | 1242 (1235-1252) |

**RocksDB, two devices**

| metric | before | after, default (12) |
|---|---|---|
| randrw-verify read IOPS | 93 (79-100) | 171 (170-173) |
| randrw-verify write IOPS | 51 (48-52) | 88 (86-90) |
| randrw read clat avg ms | 5.6 (4.3-7.0) | 12.2 (11.3-13.0) |
| randrw read clat p99 ms | 75 (58-103) | 203 (194-217) |
| randrw write clat avg ms | 0.20 (0.18-0.21) | 0.51 (0.46-0.54) |
| randrw fsync avg ms | 4845 (4665-5082) | 2184 (2102-2254) |
| seqwrite MiB/s | 49.0 (48.9-49.2) | 109.4 (108.5-110.6) |
| seqwrite write clat avg ms | 1.69 (1.57-1.80) | 2.53 (2.38-2.75) |
| seqwrite fsync avg ms | 2548 (2540-2560) | 1084 (1072-1097) |

### Reading them

- With the first default (4 data threads per device) the after-tree was 20 to 28 percent slower on FSIF, single and
  two devices, in throughput (randrw 409 against 510 IOPS reads; seqwrite 91 against 108 MiB/s), well outside the
  spread, with longer fsyncs. Cause: the old tree gave FSIF a shared `max(4, CPUs)` = 24 I/O threads, and 16 stores
  on one device now share 4. Throughput rises monotonically with the thread count: 2 threads 322, 4 threads 409, 8
  threads 430, 12 threads (CPUs/2) 528 read IOPS. Per the stop rule's list (2, 8, CPUs/2) the CPUs/2 default fixes it:
  it matches the before-tree within the session drift (528 against 510 and 585 IOPS; seqwrite 130 against 108 and 115
  MiB/s) and, with two devices, 500 against 499 and 126 against 122.
- RocksDB is far slower than FSIF on this workload on both trees (about 100 IOPS, fsync 5 s before). After, it is
  faster in throughput (single device 137 against 98 read IOPS, seqwrite 96 against 68 MiB/s; two devices 171 against
  93 and 109 against 49), and fsync is shorter, but its per-operation completion latencies are longer: read avg 10 ms
  against 5.5 (p99 190 against 100), write avg 0.5 ms against 0.3. They rise with the thread count (7.7, 10.5, 10.4 ms
  at 4, 8, 12 threads) while throughput rises too. The follow-up below explains why: the job is fsync-bound (Little's
  law: 0.56 outstanding before, 0.84-1.44 after), so the rise comes from fsync overlap, not from a higher per-op
  cost. G5 asks for no latency regression: this is one in the per-op completion numbers, for RocksDB only, and it is
  the trade for the throughput.
- One of the first six RocksDB runs with 4 threads (the first RocksDB single-device after-run) hit TCMU I/O errors
  mid-run, ext4 aborted its journal and went read-only (`dmesg`: `I/O error, dev sdb`, 02:18:46). It did not
  recur in the twelve later RocksDB after-runs (4, 8 and 12 threads, two devices), but its host log was not kept, so the cause is unknown.
- The spec's single-device hung-device acceptance (the other devices keep serving) is covered by
  `ThreadedHungDeviceSuite`, not by these runs.

### Defaults chosen

- `--device-io-threads`: **max(4, CPUs/2)** per device (`DeviceIo.defaultDataThreads`; 12 on this 24-CPU host). Was
  4. A host with many devices runs that many threads per device; they are idle when a device is.
- `--device-read-bound`: unchanged, 4096 (not varied: fio's depth of 8 cannot approach it).
- RocksDB background jobs stay 2 per database; rebuild slots stay per destination device (`maxConcurrentRebuilds`
  each). Not exercised by these jobs.

### RocksDB latency follow-up (2026-11-24)

- The smoke randrw job is fsync-bound. By Little's law it has 0.56 operations outstanding before and 0.84-1.44 after.
  Its per-op completion latency rise comes from fsync overlap, not from a higher per-op cost.
- The iodepth-1 read cost (37 us before against 92-111 us after) was vdisk-server's full piece scan of a fragmented
  clean cache, present on both trees. Commits `dda89905` and `6e18b13d` fixed it: both trees now read at 35-42 us
  (HEAD 34.7-39.2, baseline 39.3-42.2, p99 91-114).
- The no-fsync randrw gap is a commit-rate effect. More data threads let the host commit faster, so vdisk sends more,
  smaller transactions, each costing the host a fixed ~380M instructions. On this co-located machine that extra host
  CPU starves vdisk-server. Read IOPS on runs 2-3 after the vdisk fix: baseline 9954-10773; HEAD at 4 threads
  10445-10571, 8 threads 9278-9376, 12 threads 7453-8564.
- The tables above were taken before the vdisk fix, which sped the baseline's randrw by about 45%.
- The TCMU 30 s SYNCHRONIZE CACHE timeout (ext4 goes read-only) is pre-existing and seen on both trees. Avoid it by
  laying out small files one at a time.
- The default stays max(4, CPUs/2); the user decided this on 2026-11-24. Remedies for the remaining gap are in
  `Backlog.txt` (bigger vdisk transactions, RocksDB ranged writes).

## FSIF device journal (2026-11-24)

The FSIF device journal (`docs/superpowers/specs/2026-11-24-fsif-device-journal-design.md`) replaced each FSIF
store's own journal with one preallocated journal per device, written by one thread. Measured with the
microbenchmark only; the fio runs (sections 1-5, which need root and TCMU) are still to do (`Todo.txt`).

### What was run

- `src/test/scala/org/aspen_ddp/aspen/bench/FsifDeviceBench.scala`: 16 FSIF stores (64 KiB slices, 2000 objects
  each) on one `ThreadedDeviceIo`, one serial thread, driven like the vdisk randrw job: depth 8, each op a 50/50 mix of
  a 512-byte ranged write committed on all 16 stores and a read on 10 of them; 10 s per run. The new tree creates the
  journal with the production sizing (4 x 256 MiB) through `FsifJournalDisk.ensure`, as a device's first FSIF create
  does.
- `java -cp "$(sbt -batch 'export Test/fullClasspath' | tail -1)" org.aspen_ddp.aspen.bench.FsifDeviceBench
  $PWD/target/fsifbench <threads> 1 16 8 10`, on NVMe/ext4 (`target/`, not tmpfs), 24 CPUs.
- Baseline: the same benchmark (its pre-journal version) built from `7f75c2fd` in a separate worktree, run
  alternating with the new tree, thread count by thread count, three rounds. The new tree also ran three rounds just
  before (six runs each in all).
- The machine was shared with other workloads (a browser, a torrent client): load average 3.1 at the start, 3.5 to
  16.8 during the alternating rounds.

### Results (ops/s; mean, range, every run)

| Data threads | New tree (6 runs) | Runs | Baseline (3 runs) | Runs |
|---|---|---|---|---|
| 1 | 2231 (288-3705) | 3705, 2620, 510, 3648, 2614, 288 | 39 (32-51) | 51, 33, 32 |
| 2 | 1532 (507-2566) | 1344, 2479, 578, 1719, 2566, 507 | 48 (39-63) | 63, 39, 41 |
| 4 | 2638 (2449-3108) | 2553, 2474, 2449, 3108, 2577, 2666 | 113 (80-132) | 132, 80, 127 |
| 8 | 2409 (1219-3858) | 1740, 2489, 2545, 3858, 1219, 2604 | 240 (178-271) | 271, 178, 270 |
| 16 | 2246 (1670-2691) | 2318, 2358, 2691, 2634, 1804, 1670 | 376 (370-386) | 370, 386, 372 |
| 24 | 2644 (2463-3145) | 2514, 2581, 2470, 2690, 3145, 2463 | 644 (480-748) | 480, 748, 704 |

The baseline matches the investigation's numbers (49, 131, 374 at 12, 602, 767). The journal's creation (4 x 256 MiB
zero-filled and forced) took 0.54-1.19 s.

### Reading them

- The success criterion (at least the old best, 767 ops/s, on 1-2 data threads) is met on average: 2231 at 1 thread
  is about 3.5 times the old best on 24, and 57 times the old tree on 1. Write latency is 3.2-6.2 ms at every count
  (one journal batch per transaction for the whole device), against 20-310 ms before.
- On 1 and 2 threads some runs collapse (288-578 ops/s): their reads take 22-50 ms while commits stay at about 5 ms
  (BackendTiming's `fsif journaled`). Reads, in-place writes and data forces share the data pool, and with one or two
  threads a read queues behind them; it happened in the runs taken under the heaviest other load, so page-cache misses
  are the likely cause (not confirmed). At 4 threads and above no run fell below 1219, and at 4 none below 2449.
- Beyond 4 threads nothing improves: 4, 8, 16 and 24 are within each other's spread.

### Default chosen

- `--device-io-threads`: **4** per device (`DeviceIo.defaultDataThreads`), the smallest count within noise of the
  best that never collapsed. Was max(4, CPUs/2), 12 on this host.
- Still to do: the fio procedure above, both trees back to back (spec section 7 step 2), to confirm vdisk randrw and
  seqwrite throughput and fsync latency at the new default.
