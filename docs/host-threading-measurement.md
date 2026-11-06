# Host threading: before/after measurement

The procedure for the measurement that closes the host refactor
(`docs/superpowers/specs/2026-11-05-host-refactor-design.md`, "Suggested order of work", step 5). It compares
BackendTiming's "store commit (FsifBackend)" figure before the refactor (`main`) and after it (`host-refactor`).

There is no numeric target. The refactor ships either way, because the single-thread ceiling must go regardless. If
the figure does not improve, add what the thread dumps and timing show to the "Store commit delay" entry in
`Todo.txt` as the next investigation.

The runs need root and the TCMU setup, so nothing here runs automatically.

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

`./aspen --help` compiles and caches the launcher's classpath, so neither run invokes sbt. Record both commit ids.

## 2. Run the fio jobs, once per build

For each build, `before` first, then `after`:

1. Stop any running host. Start the host from that build's checkout: `./aspen host <host-directory>`. It logs to
   `<host-directory>/logs/host.log`.
2. If the vdisk is not set up and mounted yet, run `scripts/vdisk-e2e/setup.sh` as its README shows. It also runs
   `smoke.fio` once, which warms the disk; do not use that run's figures.
3. Run the two jobs of `scripts/vdisk-e2e/smoke.fio`, the ones used for the `Todo.txt` numbers, one at a time, so
   each has its own window in the log. Each is time based and runs for 60 s:

       sudo fio scripts/vdisk-e2e/smoke.fio --directory=/mnt/vd1 --section=randrw-verify
       sudo fio scripts/vdisk-e2e/smoke.fio --directory=/mnt/vd1 --section=seqwrite

   Note the wall-clock start time of each job, and keep fio's summary (IOPS, bandwidth, completion latency).
4. During each job, take the thread dumps in step 4 below.

## 3. Record BackendTiming over the same 60 s window

While writes happen, the host logs one BackendTiming line about once a second (`server.store.backend.BackendTiming`):

    Backend timing over 1001ms. fsif commit: 412 avg 18.6ms max 41.0ms; ... store commit (FsifBackend): 412 avg 66.9ms max ...

For each job, take the 60 lines that start 5 s after the job starts (to skip its ramp-up) and compute each stage's
count-weighted average over them, for "store commit (FsifBackend)" and "fsif commit". For example, with the 60 lines
in `window.log`:

    for stage in 'store commit (FsifBackend)' 'fsif commit'; do
      grep -o "$stage: [0-9]* avg [0-9.]*ms" window.log |
        awk -v s="$stage" '{ n += $(NF-2); t += $(NF-2) * $NF } END { printf "%s: %d over the window, avg %.1f ms\n", s, n, t / n }'
    done

(`$NF` is the average with its `ms` suffix, which awk drops when it converts it to a number.)

Use the same window, relative to each job's start, for both builds. The gap between the two figures is the time a
commit's completion spends outside FSIF; the refactor targets that gap.

## 4. Thread dumps and per-thread CPU

During each job, between 10 s and 50 s into it, take 3 thread dumps of the host process 10 s apart:

    pid=$(pgrep -f 'aspen.*host')        # check that this names the host JVM
    for i in 1 2 3; do jcmd "$pid" Thread.print > dump-$i.txt; sleep 10; done

Each thread's header line carries its CPU time so far (`cpu=...ms`). For each thread below, record its CPU time in
each dump, and the difference between consecutive dumps (CPU seconds used per 10 s):
- after (`host-refactor`): `aspen-host-control` and each `aspen-store-N`;
- before (`main`): the thread running Host's event loop (an unnamed `pool-N-thread-1`, "pool-3-thread-1" in the
  `Todo.txt` runs; it is the one whose stacks are in `Host.handleEvent`).

Also note any thread that uses most of its 10 s (above about 8 s), and what its stacks show it doing in all 3 dumps.
In the after run, check whether the stores' commits now queue elsewhere: for example in `HostObjectCache` (a
`synchronized` cache shared by every store, the expected next contention point), FSIF's `serial` executor, or the
network threads.

## 5. Record the results

Record, for each build and each job:
- the commit id;
- "store commit (FsifBackend)" and "fsif commit": count and weighted average over the window;
- the per-thread CPU per 10 s from step 4;
- fio's summary.

Update the "Store commit delay" entry in `Todo.txt` with them. If "store commit (FsifBackend)" does not improve, also
add what the dumps show (the busiest threads and where they spend their time) as the next investigation.
