#!/bin/bash
#
# Launcher for the Aspen command line client.
#
# The runtime classpath is exported from sbt and cached under target/ rather than being
# hardcoded here, so this script contains no machine-specific paths and can be checked in.
#
set -u

# Always operate from the repo root, regardless of where t is invoked from.
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

CP_CACHE="target/t-classpath"

#--- JDK selection ---------------------------------------------------------------------

# Honor an existing JAVA_HOME; otherwise fall back to whatever java is on the PATH.
JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/java}"

if [[ ! -x "$JAVA_BIN" ]]
then
  JAVA_BIN="$(command -v java)"
fi

if [[ ! -x "$JAVA_BIN" ]]
then
  echo "t: no java found; set JAVA_HOME or put java on the PATH" >&2
  exit 1
fi

#--- Classpath cache -------------------------------------------------------------------

# The classpath only changes when the build definition does, so it is cached and
# regenerated on: a missing cache, a newer build file, or a vanished classes directory
# (which is what "sbt clean" leaves behind).
needs_refresh() {
  [[ -s $CP_CACHE ]] || return 0

  local f
  for f in build.sbt project/build.properties project/plugins.sbt
  do
    if [[ -f $f && $f -nt $CP_CACHE ]]
    then
      return 0
    fi
  done

  # The first entry is this project's compiled-classes directory.
  local first
  IFS=: read -r first _ < "$CP_CACHE"
  [[ -d $first ]] || return 0

  return 1
}

if needs_refresh
then
  echo "t: regenerating the classpath (this invokes sbt and may take a moment)..." >&2

  cp_new="$(sbt -batch --error 'export runtime:fullClasspath' 2>/dev/null | tail -n 1 | tr -d '\r')"

  # Only overwrite the cache if sbt produced something that looks like a path list. A
  # failed export must not leave a truncated cache behind.
  if [[ -z $cp_new || $cp_new != /* ]]
  then
    echo "t: failed to export the runtime classpath from sbt" >&2
    exit 1
  fi

  mkdir -p "$(dirname "$CP_CACHE")"
  printf '%s' "$cp_new" > "$CP_CACHE.tmp" && mv "$CP_CACHE.tmp" "$CP_CACHE"
fi

CLASSPATH="$(cat "$CP_CACHE")"

#--- Run -------------------------------------------------------------------------------

# Sources are not compiled on every invocation; that would make each command pay for an
# sbt startup. Run "sbt compile" by hand after editing, or set T_COMPILE=1.
if [[ -n ${T_COMPILE:-} ]]
then
  sbt compile || exit 1
fi

if [[ ${1:-} == "bootstrap" ]]
then
  sbt compile || exit 1

  rm -rf demo/node_a
  rm -rf demo/node_b
  rm -rf demo/node_c
fi

"$JAVA_BIN" -cp "$CLASSPATH" org.aspen_ddp.aspen.cmdline.Main "$@"

rc=$?

if [[ ${1:-} == "bootstrap" && $rc -eq 0 ]]
then

  mkdir -p demo/node_a/stores
  mkdir -p demo/node_b/stores
  mkdir -p demo/node_c/stores

  mv demo/bootstrap/00000000-0000-0000-0000-000000000000:0 demo/node_a/stores
  mv demo/bootstrap/00000000-0000-0000-0000-000000000000:1 demo/node_b/stores
  mv demo/bootstrap/00000000-0000-0000-0000-000000000000:2 demo/node_c/stores

fi

# Propagate the CLI's exit status. Without this the script's status is that of the trailing
# "if", which is 0 whenever the condition is false -- masking every command failure.
exit $rc
