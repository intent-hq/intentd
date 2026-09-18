#!/usr/bin/env bash
# Per-slot CARGO_TARGET_DIR bookkeeping for the tinybox runners
# (intent-hq/intent#5256, monorepo#1253). ci.yml keeps one persistent target
# dir per <repo>/<slot>/<job> under /raid/ci-target; several cleaners can
# delete those dirs (the per-job disk preflight in ci.yml, the scheduled
# tinybox-prune.yml, ad-hoc host cleanups). Their only "is a job using this?"
# signal used to be recursive mtime activity, which a quiet compile phase
# can defeat. This script adds an explicit in-use marker and the purge
# selection that honours it, so every cleaner shares one definition of
# "skippable".
#
#   mark DIR                write DIR/.in-use (run id, job, runner, time)
#   unmark DIR              remove DIR/.in-use (no-op when absent)
#   is-in-use DIR           exit 0 when DIR carries a marker younger than
#                           IN_USE_MAX_MIN minutes (default 240 — longer than
#                           any tinybox job timeout, so a marker left behind
#                           by a runner that died mid-job expires on its own)
#   purge-candidates ROOT --quiet-min N [--skip-dir D] [--skip-slot S]
#                           print, oldest top-level mtime first, every dir at
#                           exactly ROOT/*/*/* that is NOT: the exact dir D,
#                           under slot S (ROOT/*/S/*), in use (marker), or
#                           active (anything inside modified < N min ago).
#                           Skip reasons go to stderr; the caller deletes.
#   is-purgeable DIR --quiet-min N
#                           exit 0 when DIR still exists, carries no fresh
#                           marker and nothing inside was modified < N min
#                           ago — the same test purge-candidates applies,
#                           for the consumer to repeat immediately before
#                           deleting: the producer runs ahead of the deleting
#                           loop, so a job may mark a buffered candidate
#                           after it was selected. Skip reason on stderr.
#
# Marker facts are written as `key=value` lines so a host investigation can
# read who held the dir. Nothing here deletes anything.
set -euo pipefail

MARKER=.in-use
IN_USE_MAX_MIN="${IN_USE_MAX_MIN:-240}"

usage() {
  sed -n '2,34p' "$0" >&2
  exit 2
}

# Marker mtime doubles as its validity: a fresh marker means a live (or
# very recently died) job. `|| true` keeps a dir vanishing mid-check from
# failing the caller under pipefail.
is_in_use() {
  [ -n "$(find "$1/$MARKER" -maxdepth 0 -type f -mmin -"$IN_USE_MAX_MIN" -print -quit 2>/dev/null || true)" ]
}

cmd_mark() {
  local dir="$1"
  mkdir -p -- "$dir"
  {
    echo "run_id=${GITHUB_RUN_ID:-}"
    echo "run_attempt=${GITHUB_RUN_ATTEMPT:-}"
    echo "job=${GITHUB_JOB:-}"
    echo "runner=${RUNNER_NAME:-}"
    echo "host=$(hostname 2>/dev/null || true)"
    echo "pid=$$"
    echo "marked_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >"$dir/$MARKER"
  echo "marked in use: $dir/$MARKER"
}

cmd_unmark() {
  local dir="$1"
  rm -f -- "$dir/$MARKER"
  echo "released: $dir/$MARKER"
}

cmd_is_in_use() {
  is_in_use "$1"
}

# The live part of the selection, shared by purge-candidates and
# is-purgeable so both sides of a deleting loop agree: marker first, then the
# recursive activity check — a job writing into the dir keeps it fresh even
# without a marker (legacy jobs, other repos sharing the root).
purgeable() {
  local dir="$1" quiet_min="$2"
  if [ ! -d "$dir" ]; then
    echo "skipping (gone): $dir" >&2
    return 1
  fi
  if is_in_use "$dir"; then
    echo "skipping (in use, $MARKER < ${IN_USE_MAX_MIN}min): $dir" >&2
    return 1
  fi
  if [ -n "$(find "$dir" -mmin -"$quiet_min" -print -quit 2>/dev/null || true)" ]; then
    echo "skipping (active <${quiet_min}min): $dir" >&2
    return 1
  fi
}

check_quiet_min() {
  case "$2" in ''|*[!0-9]*) echo "$1: --quiet-min N (minutes, integer >= 1) is required" >&2; usage ;; esac
  [ "$2" -ge 1 ] || { echo "$1: --quiet-min must be >= 1" >&2; usage; }
}

cmd_is_purgeable() {
  local dir="$1" quiet_min=""
  shift
  while [ $# -gt 0 ]; do
    case "$1" in
      --quiet-min) quiet_min="$2"; shift 2 ;;
      *) echo "is-purgeable: unknown argument: $1" >&2; usage ;;
    esac
  done
  check_quiet_min is-purgeable "$quiet_min"
  purgeable "$dir" "$quiet_min"
}

cmd_purge_candidates() {
  local root="$1" quiet_min="" skip_dir="" skip_slot=""
  shift
  while [ $# -gt 0 ]; do
    case "$1" in
      --quiet-min) quiet_min="$2"; shift 2 ;;
      --skip-dir) skip_dir="$2"; shift 2 ;;
      --skip-slot) skip_slot="$2"; shift 2 ;;
      *) echo "purge-candidates: unknown argument: $1" >&2; usage ;;
    esac
  done
  check_quiet_min purge-candidates "$quiet_min"
  case "$root" in /?*) ;; *) echo "purge-candidates: ROOT must be an absolute path" >&2; usage ;; esac
  root="${root%/}"
  local entry dir
  while IFS= read -r entry; do
    dir="${entry#* }"
    # Re-assert the allowlist: only ever name dirs strictly under ROOT.
    case "$dir" in "$root/"?*) ;; *) continue ;; esac
    if [ -n "$skip_dir" ] && [ "$dir" = "$skip_dir" ]; then
      echo "skipping (own dir): $dir" >&2
      continue
    fi
    if [ -n "$skip_slot" ]; then
      case "$dir" in "$root"/*/"$skip_slot"/*) echo "skipping (own slot): $dir" >&2; continue ;; esac
    fi
    purgeable "$dir" "$quiet_min" || continue
    printf '%s\n' "$dir"
  done < <(find "$root" -mindepth 3 -maxdepth 3 -type d -printf '%T@ %p\n' 2>/dev/null | sort -n)
}

[ $# -ge 2 ] || usage
cmd="$1"
shift
case "$cmd" in
  mark) [ $# -eq 1 ] || usage; cmd_mark "$1" ;;
  unmark) [ $# -eq 1 ] || usage; cmd_unmark "$1" ;;
  is-in-use) [ $# -eq 1 ] || usage; cmd_is_in_use "$1" ;;
  is-purgeable) cmd_is_purgeable "$@" ;;
  purge-candidates) cmd_purge_candidates "$@" ;;
  *) echo "unknown command: $cmd" >&2; usage ;;
esac
