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

## Next run: the adaptive pool

The `before`, `after`, `after-4t` and `after-2t` captures in `results/` stay as they are. This run adds one build,
`after-adaptive`: the `host-refactor` host with no `--store-threads`, so it runs the adaptive pool. The pool starts
with one store thread and adds more under load, so a run that starts cold measures its ramp-up as well. This run
warms the pool up first and captures only after it has stopped changing. That makes it comparable with the
fixed-thread runs, whose threads existed from startup.

1. Compile this checkout (`./aspen` does not compile by itself), then start the newly created host from it, without
   `--store-threads`:

       sbt compile
       ./aspen host <host-directory>

2. Run `scripts/vdisk-e2e/setup.sh` as usual. Its `smoke.fio` load (both jobs, 60 s each) is the first warm-up.
   The host logs each change to the pool. To see what the load did:

       log=<host-directory>/logs/host.log
       grep -E 'Added store thread|Moved store' "$log"

3. Warm up randrw until the pool stops changing. Run the job uncaptured and look for changes made during it:

       offset=$(stat -c %s "$log")
       sudo fio scripts/vdisk-e2e/smoke.fio --directory=/mnt/vd1 --section=randrw-verify
       tail -c +$((offset + 1)) "$log" | grep -E 'Added store thread|Moved store'

   Repeat until the `grep` prints nothing. Then capture straight away, before any other load:

       scripts/host-threading/capture-job.sh results after-adaptive randrw-verify /mnt/vd1

4. Do the same for seqwrite. Its load is spread differently, so the pool may move stores again, although it should
   not need more threads: repeat the step 3 commands with `--section=seqwrite` until nothing is printed, then:

       scripts/host-threading/capture-job.sh results after-adaptive seqwrite /mnt/vd1

5. Analyse all the captures together:

       scripts/host-threading/analyze.py results

   In each job's summary, the `after-adaptive` row's "pool changes in window" should read `+0 threads, 0 moves`, and
   the report then has no warning saying the pool changed. If it did change, the measurement includes ramp-up. Move
   that job's capture aside (`results/after-adaptive/<section>`), warm up again and recapture it. "In use" shows how
   many store threads the pool settled on. Compare its row with `after-2t` and `after-4t`, and especially the CPU per
   store commit.
