#!/usr/bin/env bash
# Kills vdisk-server with kill -9 while fio writes and verifies on the mounted disk, restarts it, and checks that
# the I/O resumes: fio finishes with every block verified and the filesystem is still mounted read-write.
# Run as root after setup.sh. Variables: as setup.sh, plus DELAY (seconds of I/O before the kill, default 10).
#
# The restarted server's startup scan reopens the device, resetting the ring so the kernel requeues the commands
# the dead process held. The restart must complete within the device's cmd_time_out (attrib/cmd_time_out, 30 s by
# default), or the kernel fails the queued commands and fio and ext4 see errors.
set -euo pipefail

# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

DELAY="${DELAY:-10}"

require_root
mountpoint -q "$MOUNT" || die "$MOUNT is not mounted; run setup.sh first"
server_pid >/dev/null || die "vdisk-server is not running; run setup.sh first"
dir="$(backstore_dir)" || die "no configfs directory for backstore $DISK"
echo "cmd_time_out is $(cat "$dir/attrib/cmd_time_out") s"

fio "$E2E_DIR/restart.fio" --directory="$MOUNT" &
fio_pid=$!
# A failure below must not leave fio running against the disk
trap 'kill "$fio_pid" 2>/dev/null || true' EXIT
sleep "$DELAY"
kill -0 "$fio_pid" 2>/dev/null || die "fio finished within ${DELAY}s, before the restart; lower DELAY"

started=$SECONDS
kill_server
start_server
echo "vdisk-server was down for $((SECONDS - started)) s"
check_dbus

wait "$fio_pid" || die "fio failed across the restart"
trap - EXIT
grep -qx rw <<<"$(findmnt -no OPTIONS "$MOUNT" | tr ',' '\n')" || die "$MOUNT is no longer mounted read-write"
rm -f "$MOUNT/restart.dat"
echo "I/O resumed across the vdisk-server restart"
