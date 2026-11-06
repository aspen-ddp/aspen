#!/usr/bin/env python3
"""Steps 3 to 5 of docs/host-threading-measurement.md, from what capture-job.sh captured.

    analyze.py <results-dir> [--window-start S] [--window-end S] [--busy S]

Reads <results-dir>/<build>/<section>/ for every build label found and prints a Markdown report, also written to
<results-dir>/report.md. "before" comes first, then the other builds from the most store threads in use to the fewest
(as their thread dumps show; capture-job.sh also records the host's --store-threads setting). The report has:
- a summary per job: store threads, commit rate and latency, the host's total CPU and its CPU per store commit, and
  fio's headline figures;
- BackendTiming: every stage's count, count-weighted average and max over the window, by default the BackendTiming
  periods lying wholly between 5 s and 55 s after fio's job start (fio's own job_start, so ramp-up and file layout
  are excluded), plus the gap between "store commit (FsifBackend)" and "fsif commit";
- fio's summary: IOPS, bandwidth, completion latency and fsync latency;
- the store-thread pool's changes inside the window (threads added, stores moved), with a warning if there were any;
- per-thread CPU seconds per 10 s between consecutive thread dumps, for aspen-host-control, every aspen-store-N, the
  thread running Host.handleEvent (main's event loop), and the busiest other threads;
- stacks of every thread using more than --busy seconds per 10 s (default 8), and the locks threads were blocked on.

Standard library only.
"""

import argparse
import datetime
import json
import os
import re
import sys
from collections import Counter, defaultdict

STORE_COMMIT = "store commit (FsifBackend)"
FSIF_COMMIT = "fsif commit"
SECTIONS = ["randrw-verify", "seqwrite"]
TOP_OTHER_THREADS = 10
MIN_OTHER_CPU = 0.05  # seconds per 10 s; idle threads are left out


# --- BackendTiming -----------------------------------------------------------------------------------------------

LOG_LINE = re.compile(r"^(\d{4}\.\d\d\.\d\d \d\d:\d\d:\d\d:\d{3}) .*?Backend timing over (\d+)ms\. (.*)$")
STAGE = re.compile(r"^(.*): (\d+) avg ([\d.]+)ms max ([\d.]+)ms(?:, avg size \d+)?$")


def log_time_ms(text):
    """Scribe's dateFull, in the host's local time, as epoch milliseconds."""
    t = datetime.datetime.strptime(text, "%Y.%m.%d %H:%M:%S:%f")
    return int(t.timestamp() * 1000)


def backend_timing(host_log, window_start_ms, window_end_ms):
    """Totals per stage over the BackendTiming periods that lie wholly inside the window."""
    totals = {}
    periods = 0
    covered_ms = 0
    with open(host_log, errors="replace") as f:
        for line in f:
            m = LOG_LINE.match(line.rstrip("\n"))
            if not m:
                continue
            end = log_time_ms(m.group(1))
            length = int(m.group(2))
            if end - length < window_start_ms or end > window_end_ms:
                continue
            periods += 1
            covered_ms += length
            for part in m.group(3).split("; "):
                s = STAGE.match(part)
                if not s:
                    continue
                name, count, avg, mx = s.group(1), int(s.group(2)), float(s.group(3)), float(s.group(4))
                t = totals.setdefault(name, {"count": 0, "total_ms": 0.0, "max_ms": 0.0})
                t["count"] += count
                t["total_ms"] += count * avg
                t["max_ms"] = max(t["max_ms"], mx)
    return {"periods": periods, "covered_ms": covered_ms, "stages": totals}


# StoreThreadPool's log lines for each change it makes to the pool
POOL_CHANGE = re.compile(r"^(\d{4}\.\d\d\.\d\d \d\d:\d\d:\d\d:\d{3}) .*?(Added store thread|Moved store) ")


def pool_changes(host_log, window_start_ms, window_end_ms):
    """Threads added and stores moved inside the window. Nonzero means the pool had not settled."""
    added = moved = 0
    with open(host_log, errors="replace") as f:
        for line in f:
            m = POOL_CHANGE.match(line)
            if m and window_start_ms <= log_time_ms(m.group(1)) <= window_end_ms:
                if m.group(2) == "Added store thread":
                    added += 1
                else:
                    moved += 1
    return {"added": added, "moved": moved}


def stage_avg(timing, name):
    t = timing["stages"].get(name) if timing else None
    if not t or t["count"] == 0:
        return None
    return t["total_ms"] / t["count"]


# --- fio -----------------------------------------------------------------------------------------------------------

def read_fio(path):
    text = open(path, errors="replace").read()
    for m in re.finditer(r"^\{", text, re.M):
        try:
            data, _ = json.JSONDecoder().raw_decode(text[m.start():])
            if "jobs" in data:
                return data
        except json.JSONDecodeError:
            continue
    raise ValueError(f"no fio JSON in {path}")


def percentile(lat, p):
    pct = (lat or {}).get("percentile") or {}
    return pct.get(f"{p:.6f}")


def fio_summary(data):
    job = data["jobs"][0]
    out = {
        "job_start_ms": job.get("job_start"),
        "runtime_ms": job.get("job_runtime") or job["write"].get("runtime") or 60000,
        "error": job.get("error", 0),
    }
    for d in ("read", "write"):
        r = job[d]
        clat = r.get("clat_ns") or {}
        out[d] = {
            "ios": r.get("total_ios", 0),
            "iops": r.get("iops", 0.0),
            "bw_kib": r.get("bw", 0),
            "clat_mean_ms": clat.get("mean", 0) / 1e6,
            "clat_p99_ms": (percentile(clat, 99) or 0) / 1e6,
        }
    sync = job.get("sync") or {}
    lat = sync.get("lat_ns") or {}
    out["sync"] = {
        "ios": sync.get("total_ios", 0),
        "mean_ms": lat.get("mean", 0) / 1e6,
        "p99_ms": (percentile(lat, 99) or 0) / 1e6,
    }
    return out


# --- Thread dumps --------------------------------------------------------------------------------------------------

HEADER = re.compile(r'^"(.*)" (?:#(\d+) )?.*?\bcpu=([\d.]+)ms elapsed=([\d.]+)s')
WAITING_TO_LOCK = re.compile(r"- waiting to lock <[^>]+> \(a ([^)]+)\)")
PARKING = re.compile(r"- parking to wait for\s+<[^>]+> \(a ([^)]+)\)")
DUMP_TIME = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)$", re.M)


def read_dump(path):
    """Threads keyed by (name, id): cpu ms, elapsed s, state, stack lines; plus the dump's wall time and uptime."""
    text = open(path, errors="replace").read()
    threads = {}
    current = None
    for line in text.splitlines():
        h = HEADER.match(line)
        if h:
            current = {"cpu_ms": float(h.group(3)), "elapsed_s": float(h.group(4)), "state": "", "stack": []}
            threads[(h.group(1), h.group(2) or "")] = current
        elif current is not None:
            if line.startswith("   java.lang.Thread.State: "):
                current["state"] = line.split(": ", 1)[1]
            elif line.startswith("\t"):
                current["stack"].append(line.strip())
            elif not line.strip():
                current = None
    m = DUMP_TIME.search(text)
    wall_ms = log_time_ms(m.group(1).replace("-", ".") + ":000") if m else None
    uptime = max((t["elapsed_s"] for t in threads.values()), default=None)
    return {"threads": threads, "wall_ms": wall_ms, "uptime_s": uptime}


def is_event_loop(thread):
    return any("Host.handleEvent" in f for f in thread["stack"])


def thread_cpu(dumps):
    """CPU seconds per 10 s for each thread between consecutive dumps, and the threads of interest."""
    rates = defaultdict(list)
    for a, b in zip(dumps, dumps[1:]):
        span = (b["uptime_s"] or 0) - (a["uptime_s"] or 0)
        if span <= 0:
            span = 10.0
        for key, tb in b["threads"].items():
            ta = a["threads"].get(key)
            if ta is not None:
                rates[key].append((tb["cpu_ms"] - ta["cpu_ms"]) / 1000.0 * 10.0 / span)
    return rates


def thread_sort_key(key):
    name = key[0]
    m = re.match(r"aspen-store-(\d+)$", name)
    if name == "aspen-host-control":
        return (0, 0, name)
    if m:
        return (1, int(m.group(1)), name)
    return (2, 0, name)


def frames(stack, n):
    return [f for f in stack if f.startswith("at ")][:n]


def lock_waits(dumps):
    """Per dump: monitors BLOCKED threads wait on, and the explicit locks parked threads wait on."""
    out = []
    for d in dumps:
        c = Counter()
        for t in d["threads"].values():
            for f in t["stack"]:
                m = WAITING_TO_LOCK.match(f)
                if m and t["state"].startswith("BLOCKED"):
                    c[f"monitor {m.group(1)}"] += 1
                    break
                p = PARKING.match(f)
                if p:
                    # Idle pool threads park on their pool or on a lock's Condition; only a lock's own Sync is a wait.
                    if ("Lock" in p.group(1) or "Sync" in p.group(1)) and "Condition" not in p.group(1):
                        c[f"lock {p.group(1)}"] += 1
                    break
        out.append(c)
    return out


# --- Loading -------------------------------------------------------------------------------------------------------

def read_meta(path):
    meta = {}
    if os.path.exists(path):
        for line in open(path):
            if "=" in line:
                k, v = line.rstrip("\n").split("=", 1)
                meta[k] = v
    return meta


def store_threads(meta):
    """The host's --store-threads setting, or "default" (whatever that build's default is)."""
    return meta.get("store_threads", "default")


def load_run(path, window_start_s, window_end_s, warnings, label):
    run = {"meta": read_meta(os.path.join(path, "meta"))}
    fio_out = os.path.join(path, "fio.out")
    if os.path.exists(fio_out):
        try:
            run["fio"] = fio_summary(read_fio(fio_out))
        except (ValueError, KeyError, IndexError) as e:
            warnings.append(f"{label}: cannot read fio results: {e}")
    fio = run.get("fio")
    if fio and fio["error"]:
        warnings.append(f"{label}: fio reported error {fio['error']}")

    if fio and fio["job_start_ms"]:
        start = fio["job_start_ms"]
        runtime_s = fio["runtime_ms"] / 1000.0
        end_s = min(window_end_s, runtime_s)
        if end_s < window_end_s:
            warnings.append(f"{label}: fio ran only {runtime_s:.0f} s; the window ends at {end_s:.0f} s")
        run["window"] = (window_start_s, end_s)
        host_log = os.path.join(path, "host.log")
        if os.path.exists(host_log):
            timing = backend_timing(host_log, start + window_start_s * 1000, start + end_s * 1000)
            run["timing"] = timing
            changes = pool_changes(host_log, start + window_start_s * 1000, start + end_s * 1000)
            run["pool_changes"] = changes
            if changes["added"] or changes["moved"]:
                warnings.append(f"{label}: the store-thread pool changed inside the window ({changes['added']} "
                                f"threads added, {changes['moved']} stores moved): not a steady-state measurement")
            expected = end_s - window_start_s
            if timing["periods"] == 0:
                warnings.append(f"{label}: no BackendTiming lines in the window; were writes reaching the host?")
            elif timing["covered_ms"] < 0.8 * expected * 1000:
                warnings.append(f"{label}: BackendTiming covers only {timing['covered_ms'] / 1000:.0f} s of the "
                                f"{expected:.0f} s window ({timing['periods']} lines)")
        else:
            warnings.append(f"{label}: no host.log")
    elif fio:
        warnings.append(f"{label}: fio JSON has no job_start; this fio is too old to place the window")

    dumps = []
    for i in (1, 2, 3):
        p = os.path.join(path, f"dump-{i}.txt")
        if os.path.exists(p):
            dumps.append(read_dump(p))
    if len(dumps) < 2:
        warnings.append(f"{label}: fewer than 2 thread dumps; no per-thread CPU")
    if fio and fio["job_start_ms"]:
        job_end = fio["job_start_ms"] + fio["runtime_ms"]
        for i, d in enumerate(dumps, 1):
            # The dump's wall time has 1 s resolution.
            if d["wall_ms"] and not (fio["job_start_ms"] - 1000 <= d["wall_ms"] <= job_end + 1000):
                warnings.append(f"{label}: dump {i} was not taken during the fio job")
    run["dumps"] = dumps
    return run


# --- Report --------------------------------------------------------------------------------------------------------

def ms(v):
    if v is None:
        return "—"
    return f"{v:.1f} ms" if v >= 1 else f"{v:.3f} ms"


def num(v, fmt="{:.0f}"):
    return "—" if v is None else fmt.format(v)


def table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return lines


def timing_section(builds, runs):
    names = set()
    for b in builds:
        t = runs.get(b, {}).get("timing")
        if t:
            names |= set(t["stages"])
    if not names:
        return ["(no BackendTiming data in the window)"]
    first = [STORE_COMMIT, FSIF_COMMIT]
    ordered = [n for n in first if n in names] + sorted(names - set(first))

    header = ["stage"]
    for b in builds:
        header += [f"{b} count", f"{b} avg", f"{b} max"]
    rows = []
    for n in ordered:
        row = [n]
        for b in builds:
            t = (runs.get(b, {}).get("timing") or {"stages": {}})["stages"].get(n)
            if t:
                row += [t["count"], ms(t["total_ms"] / t["count"] if t["count"] else None), ms(t["max_ms"])]
            else:
                row += ["—", "—", "—"]
        rows.append(row)
    gap = ["gap: store commit − fsif commit"]
    for b in builds:
        t = runs.get(b, {}).get("timing")
        s, f = stage_avg(t, STORE_COMMIT), stage_avg(t, FSIF_COMMIT)
        gap += ["", ms(s - f) if s is not None and f is not None else "—", ""]
    rows.insert(min(2, len(rows)), gap)

    lines = []
    for b in builds:
        r = runs.get(b, {})
        t = r.get("timing")
        if t:
            w = r["window"]
            lines.append(f"- {b}: {t['periods']} BackendTiming lines covering {t['covered_ms'] / 1000:.1f} s, "
                         f"window {w[0]:.0f}–{w[1]:.0f} s after fio's job start")
    return lines + [""] + table(header, rows)


def host_cpu(run):
    """The host's total CPU seconds per 10 s, averaged over the dump intervals, and the store threads in use: those
    that used at least MIN_OTHER_CPU, as a pool keeps threads it no longer gives stores to, parked."""
    dumps = run.get("dumps") or []
    if len(dumps) < 2:
        return None, None
    rates = thread_cpu(dumps)
    total = sum(sum(r) / len(r) for r in rates.values())
    stores = sum(1 for k, r in rates.items()
                 if re.match(r"aspen-store-\d+$", k[0]) and sum(r) / len(r) >= MIN_OTHER_CPU)
    return total, stores


def summary_section(builds, runs):
    rows = []
    for b in builds:
        r = runs.get(b)
        if r is None:
            continue
        threads = store_threads(r["meta"])
        cpu, in_use = host_cpu(r)
        t = r.get("timing")
        rate = None
        if t and t["covered_ms"] and STORE_COMMIT in t["stages"]:
            rate = t["stages"][STORE_COMMIT]["count"] / (t["covered_ms"] / 1000.0)
        s, f = stage_avg(t, STORE_COMMIT), stage_avg(t, FSIF_COMMIT)
        per_commit = cpu / 10.0 / rate * 1e6 if cpu is not None and rate else None
        fio = r.get("fio")
        rows.append([
            b,
            f"{threads}, {in_use} in use" if in_use is not None else threads,
            num(rate),
            ms(s),
            ms(s - f) if s is not None and f is not None else "—",
            num(cpu, "{:.1f}"),
            num(per_commit),
            num(fio["read"]["iops"] + fio["write"]["iops"]) if fio else "—",
            ms(fio["read"]["clat_mean_ms"]) if fio else "—",
            ms(fio["sync"]["mean_ms"]) if fio else "—",
            (lambda c: f"+{c['added']} threads, {c['moved']} moves" if c else "—")(r.get("pool_changes")),
        ])
    header = ["build", "store threads", "store commits/s", "store commit avg", "gap", "host CPU s/10 s",
              "CPU µs/store commit", "fio IOPS", "read clat avg", "fsync avg",
              "pool changes in window"]
    return (["Host CPU is every thread's CPU between the dumps; CPU per store commit divides it by the store "
             "commit rate in the BackendTiming window. Pool changes are the store threads added and stores moved inside that "
             "window: anything but zero means the pool was still adjusting during the measurement."] + [""] + table(header, rows))


def fio_section(builds, runs):
    metrics = [
        ("read IOPS", lambda f: num(f["read"]["iops"])),
        ("write IOPS", lambda f: num(f["write"]["iops"])),
        ("read bandwidth", lambda f: num(f["read"]["bw_kib"] / 1024, "{:.1f} MiB/s")),
        ("write bandwidth", lambda f: num(f["write"]["bw_kib"] / 1024, "{:.1f} MiB/s")),
        ("read clat avg", lambda f: ms(f["read"]["clat_mean_ms"])),
        ("read clat p99", lambda f: ms(f["read"]["clat_p99_ms"])),
        ("write clat avg", lambda f: ms(f["write"]["clat_mean_ms"])),
        ("write clat p99", lambda f: ms(f["write"]["clat_p99_ms"])),
        ("fsync count", lambda f: num(f["sync"]["ios"])),
        ("fsync lat avg", lambda f: ms(f["sync"]["mean_ms"])),
        ("fsync lat p99", lambda f: ms(f["sync"]["p99_ms"])),
    ]
    rows = []
    for name, get in metrics:
        row = [name]
        for b in builds:
            f = runs.get(b, {}).get("fio")
            row.append(get(f) if f else "—")
        rows.append(row)
    return table(["metric"] + list(builds), rows)


def cpu_section(build, run, busy):
    dumps = run.get("dumps") or []
    if len(dumps) < 2:
        return ["(no thread dumps)"], []
    rates = thread_cpu(dumps)
    event_loop = {k for d in dumps for k, t in d["threads"].items() if is_event_loop(t)}
    interest = {k for k in rates if k[0] == "aspen-host-control" or re.match(r"aspen-store-\d+$", k[0])}
    interest |= event_loop & set(rates)
    busy_threads = [k for k in rates if max(rates[k]) > busy]
    others = sorted((k for k in rates if k not in interest), key=lambda k: -sum(rates[k]) / len(rates[k]))
    top = [k for i, k in enumerate(others)
           if k in busy_threads or (i < TOP_OTHER_THREADS and sum(rates[k]) / len(rates[k]) >= MIN_OTHER_CPU)]
    shown = sorted(interest, key=thread_sort_key) + top

    header = ["thread"] + [f"dump {i}→{i + 1}" for i in range(1, len(dumps))] + ["avg"]
    rows = []
    for k in shown:
        r = rates[k]
        avg = sum(r) / len(r)
        name = k[0] + (" (Host.handleEvent)" if k in event_loop else "")
        if k in top:
            name += " (busiest other)"
        marks = [f"**{v:.2f}**" if v > busy else f"{v:.2f}" for v in r]
        rows.append([name] + marks + [f"{avg:.2f}"])
    stores = [k for k in interest if k[0].startswith("aspen-store-")]
    lines = [f"CPU seconds per 10 s; above {busy:g} in bold."]
    if stores:
        total = sum(sum(rates[k]) / len(rates[k]) for k in stores)
        lines.append(f"{len(stores)} store threads, {total:.2f} s per 10 s together.")
    return lines + [""] + table(header, rows), busy_threads


def stacks_section(run, keys):
    lines = []
    for k in sorted(keys, key=thread_sort_key):
        lines.append(f"**{k[0]}**")
        for i, d in enumerate(run["dumps"], 1):
            t = d["threads"].get(k)
            if t is None:
                continue
            lines.append(f"- dump {i} ({t['state'] or 'unknown state'}):")
            lines += [f"  - `{f[3:]}`" for f in frames(t["stack"], 8)]
        lines.append("")
    return lines


def locks_section(run):
    waits = lock_waits(run.get("dumps") or [])
    names = sorted(set().union(*waits), key=lambda n: -sum(w[n] for w in waits)) if waits else []
    if not names:
        return ["No thread was blocked on a monitor or parked on a lock."]
    rows = [[f"`{n}`"] + [w[n] for w in waits] for n in names]
    return table(["waiting for"] + [f"dump {i}" for i in range(1, len(waits) + 1)], rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results")
    ap.add_argument("--window-start", type=float, default=5.0, help="seconds after fio's job start (default 5)")
    ap.add_argument("--window-end", type=float, default=55.0, help="seconds after fio's job start (default 55)")
    ap.add_argument("--busy", type=float, default=8.0, help="CPU seconds per 10 s that counts as busy (default 8)")
    a = ap.parse_args()

    if not os.path.isdir(a.results):
        sys.exit(f"analyze: no directory {a.results}")
    found = sorted(d for d in os.listdir(a.results) if os.path.isdir(os.path.join(a.results, d)))
    if not found:
        sys.exit(f"analyze: nothing captured in {a.results}")

    warnings = []
    runs = defaultdict(dict)  # section -> build -> run
    for b in found:
        for s in SECTIONS:
            p = os.path.join(a.results, b, s)
            if os.path.isdir(p):
                runs[s][b] = load_run(p, a.window_start, a.window_end, warnings, f"{b}/{s}")
            else:
                warnings.append(f"{b}/{s}: not captured")

    def in_use(b):
        """The most store threads the build ran in any of its jobs' dumps."""
        return max((host_cpu(r[b])[1] or 0 for r in runs.values() if b in r), default=0)

    # "before" first, then the most store threads in use to the fewest: what a build's setting means depends on its
    # commit (the default was one per CPU, then a pool growing with load), so the dumps decide.
    builds = sorted(found, key=lambda b: (b != "before", -in_use(b), b))

    out = ["# Host threading: before/after measurement", ""]
    rows = []
    for b in builds:
        meta = next((r[b]["meta"] for r in runs.values() if b in r and r[b]["meta"]), {})
        commits = {r[b]["meta"].get("commit") for r in runs.values() if b in r} - {None}
        if len(commits) > 1:
            warnings.append(f"{b}: its sections ran on different commits: {', '.join(sorted(commits))}")
        settings = {store_threads(r[b]["meta"]) for r in runs.values() if b in r}
        if len(settings) > 1:
            warnings.append(f"{b}: its sections ran with different store threads: {', '.join(sorted(settings))}")
        dirty = " (uncommitted changes)" if meta.get("dirty") == "yes" else ""
        rows.append([b, store_threads(meta), in_use(b), f"`{meta.get('commit', '?')}`{dirty}",
                     f"`{meta.get('checkout', '?')}`"])
    out += table(["build", "store threads", "in use", "commit", "checkout"], rows) + [""]
    out += ["\"store threads\" is the host's --store-threads, and \"default\" that build's own default: one per CPU "
            "until commit 192e4c9e; then an adaptive pool (one thread at first, growing with load up to a quarter of "
            "the CPUs) in after-adaptive and after-packed; since then a fixed min(4, CPUs/4). \"in use\" is the most store threads that used CPU between its thread dumps. A build without store threads (before "
            "the refactor) runs every store on one event thread.", ""]

    for s in SECTIONS:
        if s not in runs:
            continue
        r = runs[s]
        out += [f"## {s}", "", "### Summary", ""] + summary_section(builds, r) + [""]
        out += ["### BackendTiming", ""] + timing_section(builds, r) + [""]
        out += ["### fio", ""] + fio_section(builds, r) + [""]
        for b in builds:
            if b not in r:
                continue
            cpu, busy_threads = cpu_section(b, r[b], a.busy)
            out += [f"### Threads: {b}", ""] + cpu + [""]
            if busy_threads:
                out += [f"Stacks of threads above {a.busy:g} s per 10 s:", ""] + stacks_section(r[b], busy_threads)
            out += ["Threads waiting for a lock:", ""] + locks_section(r[b]) + [""]

    if warnings:
        out += ["## Warnings", ""] + [f"- {w}" for w in warnings] + [""]

    text = "\n".join(out)
    print(text)
    with open(os.path.join(a.results, "report.md"), "w") as f:
        f.write(text)
    if warnings:
        print(f"analyze: {len(warnings)} warning(s); see the end of the report", file=sys.stderr)


if __name__ == "__main__":
    main()
