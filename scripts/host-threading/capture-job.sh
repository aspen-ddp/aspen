#!/usr/bin/env bash
#
# Runs one smoke.fio job against the running host and captures what analyze.py needs (docs/host-threading-measurement.md).
#
#   capture-job.sh <results-dir> <build-label> <section> [fio-directory]
#
#   results-dir    where captures go; each run gets <results-dir>/<build-label>/<section>/
#   build-label    "before" or "after" (any label works; analyze.py compares "before" with "after")
#   section        smoke.fio section: randrw-verify or seqwrite
#   fio-directory  the mounted vdisk (default /mnt/vd1)
#
# The meta file records the host's --store-threads setting ("default": one per CPU), which analyze.py reports and
# orders the builds by. Use one label per setting, such as after-2t and after-4t.
# Run it as your own user, not under sudo: fio runs through sudo, and jcmd runs as the host JVM's owner.
# It finds the host JVM (HOST_PID overrides that) and records the commit of the checkout it runs from, so the
# "before" and "after" runs cannot be confused. During the job it takes 3 thread dumps, 10 s apart, starting 15 s in.
# Afterwards it copies the part of host.log the job wrote (HOST_LOG overrides the log's path).
set -euo pipefail

die() { echo "capture-job: $*" >&2; exit 1; }

[[ $# -ge 3 && $# -le 4 ]] || die "usage: capture-job.sh <results-dir> <build-label> <section> [fio-directory]"

results=$1
label=$2
section=$3
fio_dir=${4:-/mnt/vd1}

case $section in
  randrw-verify|seqwrite) ;;
  *) die "section must be randrw-verify or seqwrite, not '$section'" ;;
esac

[[ $EUID -ne 0 ]] || die "run as your own user; fio is run through sudo"
[[ -d $fio_dir ]] || die "no directory $fio_dir; is the vdisk mounted?"

here="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fio_file="$here/../vdisk-e2e/smoke.fio"
[[ -f $fio_file ]] || die "missing $fio_file"

#--- The host JVM ------------------------------------------------------------------------

if [[ -n ${HOST_PID:-} ]]
then
  pid=$HOST_PID
else
  # pgrep's match also catches shells whose command line merely contains the text, so keep only processes with
  # the main class and "host" as separate arguments.
  pids=()
  for p in $(pgrep -f 'org\.aspen_ddp\.aspen\.cmdline\.Main host' || true)
  do
    mapfile -d '' a < "/proc/$p/cmdline" 2> /dev/null || continue
    for ((i = 0; i + 1 < ${#a[@]}; i++))
    do
      if [[ ${a[i]} == org.aspen_ddp.aspen.cmdline.Main && ${a[i+1]} == host ]]
      then
        pids+=("$p")
        break
      fi
    done
  done
  [[ ${#pids[@]} -gt 0 ]] || die "no running 'aspen host' JVM found"
  [[ ${#pids[@]} -eq 1 ]] || die "several 'aspen host' JVMs (${pids[*]}); set HOST_PID"
  pid=${pids[0]}
fi

[[ -r /proc/$pid/cmdline ]] || die "no process $pid"
mapfile -d '' args < "/proc/$pid/cmdline"

owner="$(stat -c %U "/proc/$pid")"
if [[ $owner == "$(id -un)" ]]
then
  jcmd_cmd=(jcmd)
else
  jcmd_cmd=(sudo -u "$owner" jcmd)
fi

# The launcher's first classpath entry is the checkout's compiled-classes directory, <checkout>/target/...
# After "host" come the host directory and the host's options; --store-threads is recorded, and "default" means
# StoreThreadPool's default of one per CPU (builds before the option always used that).
classpath=""
host_dir=""
store_threads=default
for ((i = 0; i < ${#args[@]}; i++))
do
  case ${args[i]} in
    -cp|-classpath|--class-path) classpath=${args[i+1]:-} ;;
    host)
      for ((j = i + 1; j < ${#args[@]}; j++))
      do
        case ${args[j]} in
          --store-threads=*) store_threads=${args[j]#--store-threads=} ;;
          --store-threads) store_threads=${args[j+1]:-}; j=$((j + 1)) ;;
          --*=*) ;;
          --*) j=$((j + 1)) ;;  # every other host option takes a value
          *) [[ -n $host_dir ]] || host_dir=${args[j]} ;;
        esac
      done
      break
      ;;
  esac
done
[[ $store_threads == default || $store_threads =~ ^[1-9][0-9]*$ ]] || die "cannot read --store-threads ('$store_threads')"

first_cp=${classpath%%:*}
[[ $first_cp == */target/* ]] || die "cannot find the checkout from the host's classpath ('$first_cp')"
checkout=${first_cp%%/target/*}
commit="$(git -C "$checkout" rev-parse HEAD)"
dirty="$(git -C "$checkout" status --porcelain --untracked-files=no)"

if [[ -n ${HOST_LOG:-} ]]
then
  host_log=$HOST_LOG
else
  [[ -n $host_dir ]] || die "cannot find the host directory in the host's arguments; set HOST_LOG"
  [[ $host_dir == /* ]] || host_dir="$(readlink "/proc/$pid/cwd")/$host_dir"
  host_log="$host_dir/logs/host.log"
fi
[[ -r $host_log ]] || die "cannot read $host_log; set HOST_LOG"

#--- Capture -----------------------------------------------------------------------------

out="$results/$label/$section"
[[ ! -e $out ]] || die "$out already exists; move it away to capture again"
mkdir -p "$out"

{
  echo "pid=$pid"
  echo "checkout=$checkout"
  echo "commit=$commit"
  echo "dirty=$([[ -n $dirty ]] && echo yes || echo no)"
  echo "host_log=$host_log"
  echo "fio_dir=$fio_dir"
  echo "store_threads=$store_threads"
  echo "cpus=$(nproc)"
} > "$out/meta"

echo "capture-job: $label/$section: host pid $pid, $checkout at ${commit:0:10}$([[ -n $dirty ]] && echo ' (dirty)')," \
  "store threads: $store_threads"
[[ -z $dirty ]] || echo "capture-job: warning: $checkout has uncommitted changes" >&2

sudo -v

log_offset="$(stat -c %s "$host_log")"
start=$SECONDS

sudo fio "$fio_file" --directory="$fio_dir" --section="$section" --output-format=normal,json \
  > "$out/fio.out" 2> "$out/fio.err" &
fio_pid=$!

for i in 1 2 3
do
  wait_s=$(( start + 5 + 10 * i - SECONDS ))
  (( wait_s <= 0 )) || sleep "$wait_s"
  kill -0 "$fio_pid" 2> /dev/null || die "fio ended before dump $i; see $out/fio.err"
  "${jcmd_cmd[@]}" "$pid" Thread.print > "$out/dump-$i.txt"
  echo "capture-job: thread dump $i taken"
done

wait "$fio_pid" || die "fio failed; see $out/fio.err"

tail -c +"$(( log_offset + 1 ))" "$host_log" > "$out/host.log"

echo "capture-job: done: $out"
