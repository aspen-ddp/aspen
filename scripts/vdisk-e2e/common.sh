# Shared settings and helpers for the vdisk end-to-end scripts. Sourced, not run.
# shellcheck shell=bash disable=SC2034

E2E_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$E2E_DIR/../.." && pwd)"

DISK="${DISK:-vd1}"
SIZE="${SIZE:-10G}"
BLOCK_SIZE="${BLOCK_SIZE:-4096}"
CONTROL="${CONTROL:-hw_block_size=$BLOCK_SIZE}"
IQN="${IQN:-iqn.2026-10.org.aspen-ddp:${DISK}}"
MOUNT="${MOUNT:-/mnt/$DISK}"
ASPEN="${ASPEN:-$REPO/aspen}"
CLIENT_CONFIG="${CLIENT_CONFIG:-}"
NETWORK_CACHE_DIR="${NETWORK_CACHE_DIR:-/var/lib/aspen/vdisk-cache}"
LOG="${LOG:-/var/log/aspen-vdisk-server.log}"
# How long vdisk-server may take to start (Aspen client, D-Bus, startup scan)
START_TIMEOUT="${START_TIMEOUT:-60}"

BUS_NAME=org.kernel.TCMUService1
HANDLER_PATH=/org/kernel/TCMUService1/aspen
DEVICE_LINK="/dev/disk/by-path/ip-127.0.0.1:3260-iscsi-$IQN-lun-0"

die() {
  echo "error: $*" >&2
  exit 1
}

require_root() {
  [[ $EUID -eq 0 ]] || die "run as root"
}

# vdisk-server replaces tcmu-runner: both serve org.kernel.TCMUService1 and the TCMU devices
refuse_tcmu_runner() {
  if systemctl is-active --quiet tcmu-runner 2>/dev/null || pgrep -x tcmu-runner >/dev/null; then
    die "tcmu-runner is running; stop and disable it first (systemctl disable --now tcmu-runner)"
  fi
}

# The JVM's command line. The aspen launcher runs java as its child, so the JVM is what has to be killed.
SERVER_PATTERN='org\.aspen_ddp\.aspen\.cmdline\.Main vdisk-server'

server_pid() {
  local pids
  pids="$(pgrep -f "$SERVER_PATTERN")" || return 1
  echo "${pids%%$'\n'*}"
}

# Starts vdisk-server in the background and waits until it serves (its startup scan has finished)
start_server() {
  local pid lines deadline
  if pid="$(server_pid)"; then
    echo "vdisk-server already running (pid $pid)"
    return 0
  fi
  [[ -n $CLIENT_CONFIG ]] || die "set CLIENT_CONFIG to the aspen-client-config.yaml to use"
  [[ -f $CLIENT_CONFIG ]] || die "client config $CLIENT_CONFIG not found"
  mkdir -p "$NETWORK_CACHE_DIR"
  touch "$LOG"
  lines="$(wc -l < "$LOG")"
  # Its own session, so that a ^C to this script does not reach it
  setsid "$ASPEN" vdisk-server "$CLIENT_CONFIG" --network-cache-dir "$NETWORK_CACHE_DIR" </dev/null >>"$LOG" 2>&1 &
  # The launcher, which exits with the JVM
  pid=$!
  deadline=$((SECONDS + START_TIMEOUT))
  until grep -q "Serving virtual disks through TCMU" <<<"$(tail -n "+$((lines + 1))" "$LOG")"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      tail -n "+$((lines + 1))" "$LOG" >&2
      die "vdisk-server exited during startup; see $LOG"
    fi
    ((SECONDS < deadline)) || die "vdisk-server did not start within ${START_TIMEOUT}s; see $LOG"
    sleep 0.5
  done
  echo "vdisk-server serving (pid $(server_pid), log $LOG)"
}

# Crash-only: there is no clean shutdown, so stopping is kill -9
kill_server() {
  local pid deadline
  if pid="$(server_pid)"; then
    pkill -9 -f "$SERVER_PATTERN"
    deadline=$((SECONDS + 10))
    while server_pid >/dev/null; do
      ((SECONDS < deadline)) || die "vdisk-server (pid $pid) did not exit within 10s of kill -9"
      sleep 0.1
    done
    echo "killed vdisk-server (pid $pid)"
  fi
}

# The Task 10 checks: vdisk-server exports the handler object and targetcli sees the user:aspen backstore type
check_dbus() {
  local intro
  intro="$(busctl introspect "$BUS_NAME" "$HANDLER_PATH")" ||
    die "busctl introspect $BUS_NAME $HANDLER_PATH failed"
  grep -q "CheckConfig" <<<"$intro" || die "$HANDLER_PATH has no CheckConfig method:"$'\n'"$intro"
  grep -q "ConfigDesc" <<<"$intro" || die "$HANDLER_PATH has no ConfigDesc property:"$'\n'"$intro"
  grep -q "user:aspen" <<<"$(targetcli ls /backstores)" || die "targetcli ls /backstores does not show user:aspen"
  echo "D-Bus: $BUS_NAME exports $HANDLER_PATH and targetcli shows user:aspen"
}

# The backstore's configfs directory, /sys/kernel/config/target/core/user_<N>/$DISK
backstore_dir() {
  local d
  for d in /sys/kernel/config/target/core/user_*/"$DISK"; do
    if [[ -d $d ]]; then
      echo "$d"
      return 0
    fi
  done
  return 1
}

wait_for_device() {
  local deadline=$((SECONDS + 30))
  until [[ -b $DEVICE_LINK ]]; do
    ((SECONDS < deadline)) || die "$DEVICE_LINK did not appear"
    sleep 0.5
  done
  readlink -f "$DEVICE_LINK"
}

login() {
  iscsiadm -m discovery -t st -p 127.0.0.1 >/dev/null
  if ! grep -q "$IQN" <<<"$(iscsiadm -m session 2>/dev/null)"; then
    iscsiadm -m node -T "$IQN" -p 127.0.0.1 --login
  fi
}

logout() {
  if grep -q "$IQN" <<<"$(iscsiadm -m session 2>/dev/null)"; then
    iscsiadm -m node -T "$IQN" -p 127.0.0.1 --logout
  fi
}

create_backstore() {
  if ! targetcli "/backstores/user:aspen/$DISK" ls >/dev/null 2>&1; then
    # targetcli prepends "aspen/" to the cfgstring, which is just the disk name
    targetcli "/backstores/user:aspen" create "name=$DISK" "size=$1" "cfgstring=$DISK" "control=$CONTROL"
  fi
}

delete_backstore() {
  if targetcli "/backstores/user:aspen/$DISK" ls >/dev/null 2>&1; then
    targetcli "/backstores/user:aspen" delete "$DISK"
  fi
}
