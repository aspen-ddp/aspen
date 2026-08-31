#!/bin/bash
#
# Launcher for the Aspen command line client.
#
# The runtime classpath is exported from sbt and cached under target/ rather than being
# hardcoded here, so this script contains no machine-specific paths and can be checked in.
#
set -u

# Setup and launch want different working directories. The classpath cache, the build-file
# staleness checks, and "sbt compile" all have to run from the repo root. The CLI, on the
# other hand, resolves its file arguments against the process cwd, so it must see the
# directory the user actually invoked t from -- otherwise a relative path either fails
# validation or, worse, silently resolves to a same-named file under the repo root.
#
# So: run setup from the repo root, then return to the invocation directory before exec'ing.
invocation_dir="$PWD"

# Finding the repo root means resolving this script's own path, which may be a symlink --
# t linked onto the PATH, say. dirname on the link would name the directory holding it
# rather than the checkout. macOS ships no "readlink -f" and its BSD readlink follows only
# one level, so walk the chain by hand.
#
# The loop needs no cycle guard. Reaching it means the kernel already resolved this same
# chain to exec the script, so the chain is finite and acyclic; a cycle fails at exec with
# ELOOP and never gets here.
src="${BASH_SOURCE[0]}"

while [[ -L $src ]]
do
  link_dir="$(cd -P "$(dirname "$src")" && pwd)" || exit 1
  src="$(readlink "$src")"

  # A relative link target resolves against the directory holding the link, not the cwd.
  [[ $src == /* ]] || src="$link_dir/$src"
done

repo_root="$(cd -P "$(dirname "$src")" && pwd)" || exit 1
cd "$repo_root" || exit 1

# Absolute, so reading the cache does not depend on the cwd at the time of the read.
CP_CACHE="$repo_root/target/t-classpath"

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

# Back to where the user ran t from, so relative path arguments mean what they typed. The
# classpath is absolute (enforced by the "/*" guard on the sbt export above), so it keeps
# working from here.
cd "$invocation_dir" || exit 1

"$JAVA_BIN" -cp "$CLASSPATH" org.aspen_ddp.aspen.cmdline.Main "$@"

rc=$?

# Propagate the CLI's exit status. Without this the script's status is that of the trailing
# "if", which is 0 whenever the condition is false -- masking every command failure.
exit $rc
