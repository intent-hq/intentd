#!/usr/bin/env bash
# Run only the nextest targets this branch touched, plainly or under llvm-cov.
#
#   scripts/changed-tests.sh [--dry-run] [--base REF] [--instrumented]
#                            [--build-jobs N] [--test-threads N] [--runner CMD]
#                            [--plan-json PATH] [--check-map]
#   scripts/changed-tests.sh --print-edges | --print-rdeps
#
# Each flag falls back to an environment variable: DRY_RUN=1, BASE (default
# origin/main), BUILD_JOBS, TEST_THREADS, NEXTEST_RUNNER. NEXTEST_SHOW_PROGRESS /
# CARGO_TERM_PROGRESS_WHEN are inherited as-is (the monorepo Makefile sets
# them). The script works from any cwd: it changes to the repo root itself.
#
# All test children (including a custom runner) use with-test-policy.sh to
# enforce INTENTD_ASSERT_BOUND_CALLER=1, even when inherited env disables it.
# Without a runner every plan runs as its own `cargo nextest run` from the repo
# root. With one, the runner (a shell-quoted command line, exec'd once from the
# caller's working directory) receives every plan instead:
#   CMD --plan "<plan>"... --base REF --label test-changed
#       [--build-jobs N] [--test-threads N]
# and its exit status is the script's. The monorepo `make test-changed` points
# it at its scripts/resumable_nextest.py so the run leaves a gate-run record.
#
# --instrumented runs every plan under the same instrumentation the merge
# queue's coverage-e2e / coverage-all jobs use, with the same env and the same
# auggie_context_e2e exclusion (see scripts/coverage-e2e.sh):
#   INTENTD_TEST_TIMEOUT_MULTIPLIER=3 cargo llvm-cov --no-report nextest <plan>
#       [--build-jobs N] [--test-threads N] -E 'not test(/^auggie_context_e2e::/)'
# (a plan that carries its own -E has the exclusion ANDed into it, since
# nextest ORs repeated -E flags).
# No `cargo llvm-cov clean`, no report: the point is executing the changed
# tests under instrumentation (intent-hq/intentd#1947), not a coverage number.
# --instrumented cannot be combined with --runner (usage error).
#
# The changed set is `git diff --name-only $(git merge-base HEAD BASE)`
# (committed, staged and unstaged edits) plus the untracked files from
# `git ls-files --others`. Paths map to per-crate nextest selections:
#   crates/<c>/tests/<t>.rs          -p <c> --test <t>   (--tests once deleted)
#   crates/<c>/tests/<m>.rs, or      -p <c> --test <s> -E 'binary_id(=<c>::<s>)&test(/^(<m>)::/)'
#     crates/<c>/tests/<m>/**        when tests/<s>.rs is a suite root (suite_*.rs)
#                                    declaring `mod <m>;` and no other test file
#                                    #[path]-includes tests/<m>/ (else --tests)
#   crates/<c>/tests/<dir>/**        -p <c> --tests
#   crates/<c>/src/**                -p <c> --lib --bins --tests
#                                    (--lib only when src/lib.rs exists);
#                                    crates/intent-services/src/** also selects
#                                    every crates/intent-svc-* feature crate;
#                                    that and crates/intent-svc-*/src/** also
#                                    select crates/intent-app
#   crates/<c>/benches|examples/**   ignored (nextest does not run them)
#   crates/<c>/<anything else>       -p <c>  (all test targets of <c>)
#   crates/intent-core/ (src/** or   also every crate depending on intent-core,
#     <anything else>)               directly or transitively, as for src/**
#                                    (dependencies read from crates/*/Cargo.toml)
#   a glob in .config/e2e-test-map.txt
#                                    also the end-to-end modules it maps to:
#                                    -p <c> --test <s> -E 'binary_id(=<c>::<s>)&test(/^(<m>)::/)'
#                                    (the suite-module shape; a module-less
#                                    entry selects --test <s> whole)
#   .config/e2e-test-map.txt itself  every mapped end-to-end module
#   outside crates/, inert           ignored: *.md, LICENSE, NOTICE, .gitignore,
#                                    .github/**, docs/**, deny.toml,
#                                    release-plz.toml, dist-workspace.toml
#   outside crates/, anything else   full suite (exit 3) -- tests read repo
#                                    files such as scripts/install.sh via
#                                    CARGO_MANIFEST_DIR, so fail closed
# Suite modules of one crate share a single -E (standalone --test targets of the
# same plan are ORed in as binary_id(=<c>::<t>)), so only the changed files'
# tests run, not every module of the suite.
# A broader selection subsumes narrower ones for the same crate. Cargo applies
# target flags to every -p package on one command line, so crates with
# different selections run as separate `cargo nextest run` invocations and
# crates with identical selections share one. Reverse dependencies are
# propagated in full only from intent-core (intentd#2698); otherwise only from
# intent-services to its feature crates and intent-app, from a feature crate
# to intent-app, and through the end-to-end map: crates/<c>/src selects <c>'s
# own tests plus any mapped end-to-end modules; the full suite (`make test`,
# the merge queue) stays the complete gate.
#
# The map is validated on every run, and --check-map validates it alone (and
# requires it to exist): each source glob must match a tracked or untracked
# file, each test binary root crates/<c>/tests/<s>.rs must exist, and each
# segment of a module path must be declared by its parent with its file
# present. An invalid map exits 2 naming every bad line.
#
# Exit codes: 0 = nothing to test, every invocation passed, or --check-map
# found the map valid; 2 = usage error, an invalid end-to-end map
# or a git command failed; 3 = a build-wide file changed (Cargo.toml,
# Cargo.lock, crates/*/Cargo.toml, crates/*/build.rs, .config/nextest.toml,
# rust-toolchain.toml, .cargo/**) or a non-inert path outside crates/ changed
# -- run the full `make test` instead; 4 = `git merge-base HEAD BASE` failed
# (BASE unknown, or not connected to HEAD in a shallow checkout -- an
# environment problem, not a test result; intent-hq/intent#5414); any other
# code is the first failing cargo invocation's (or the runner's) exit code.
# Under --instrumented a
# build-wide change never exits 3: the full instrumented suite already runs in
# the merge queue, so the script prints a notice naming the files and still
# runs the mapped crates/ selection (a build-wide file under crates/<c>/
# selects every test target of <c>); an empty selection exits 0.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
test_policy="$repo_root/scripts/with-test-policy.sh"
caller_dir=$PWD
base=${BASE:-origin/main}
build_jobs=${BUILD_JOBS:-}
test_threads=${TEST_THREADS:-}
dry_run=${DRY_RUN:-}
runner=${NEXTEST_RUNNER:-}
instrumented=""
plan_json=""
check_map=""
print_mode=""

usage() {
  echo "Usage: $0 [--dry-run] [--base REF] [--instrumented] [--build-jobs N] [--test-threads N] [--runner CMD] [--plan-json PATH] [--check-map] | --print-edges | --print-rdeps" >&2
  exit 2
}

n_args=$#
while (($# > 0)); do
  case "$1" in
    --dry-run) dry_run=1 ;;
    --instrumented) instrumented=1 ;;
    --check-map) check_map=1 ;;
    --print-edges | --print-rdeps)
      [[ -z "$print_mode" ]] || usage
      print_mode=${1#--print-}
      ;;
    --base=*) base=${1#--base=} ;;
    --build-jobs=*) build_jobs=${1#--build-jobs=} ;;
    --test-threads=*) test_threads=${1#--test-threads=} ;;
    --runner=*) runner=${1#--runner=} ;;
    --plan-json=*) plan_json=${1#--plan-json=} ;;
    --base | --build-jobs | --test-threads | --runner | --plan-json)
      [[ $# -ge 2 && -n "$2" ]] || usage
      case "$1" in
        --base) base=$2 ;;
        --build-jobs) build_jobs=$2 ;;
        --test-threads) test_threads=$2 ;;
        --runner) runner=$2 ;;
        --plan-json) plan_json=$2 ;;
      esac
      shift
      ;;
    *) usage ;;
  esac
  shift
done
[[ -n "$base" ]] || usage
[[ -z "$instrumented" || -z "$runner" ]] || usage
[[ -n "$dry_run" && "$dry_run" != 0 ]] || dry_run=""
# A print mode takes no other flag.
[[ -z "$print_mode" || $n_args -eq 1 ]] || usage

label=test-changed
[[ -z "$instrumented" ]] || label=coverage-changed
[[ -z "$check_map" ]] || label=check-map

log() {
  echo "[$label] $*"
}

die() {
  local code=$1
  shift
  echo "[$label] $*" >&2
  exit "$code"
}

cd "$repo_root"

# intent-core is the leaf every other crate builds on, and its shared rules
# (harness versions, wire types) are asserted by tests across the workspace:
# a src (or other non-test) change there also selects every workspace crate
# depending on it, directly or transitively ("dependent", like the feature
# crates below). intentd#2698 changed a harness version in intent-core,
# passed every changed test, and failed an intent-acp test in the merge
# queue. Its reverse dependencies are nearly the whole workspace, so this is
# close to the full suite, but it also holds under --instrumented, where a
# full-suite fallback is not available. Other crates do not propagate: most
# reach intentd's end-to-end suites through intent-services, so propagating
# them would run the full suite for most changes.
rdeps_roots="intent-core"

# Prints "<dependent>\t<dependency>" for each dependency between workspace
# crates declared in crates/*/Cargo.toml: any [dependencies],
# [dev-dependencies] or [build-dependencies] table, target-specific ones
# included, inline (`<name> = { path = "../<dir>" }`, `<name>.workspace =
# true`, a `package = ` rename) or as a [<table>.<name>] section. A
# dependency is a workspace crate when its path names crates/<dir>, its
# `package =` or else its key names a crates/<name> directory (workspace
# inheritance). Comments and other
# sections are skipped. A line scanner, not a TOML parser.
workspace_edges() {
  local manifest crate line section dep key
  for manifest in crates/*/Cargo.toml; do
    [[ -f "$manifest" ]] || continue
    crate=${manifest#crates/}
    crate=${crate%/Cargo.toml}
    section=""
    while IFS= read -r line || [[ -n "$line" ]]; do
      line=${line#"${line%%[![:space:]]*}"}
      case "$line" in
        "" | '#'*) continue ;;
        '['*)
          section=${line#\[}
          section=${section%%]*}
          # A [<table>.<name>] section is one dependency; its key is the name.
          case "$section" in
            *dependencies.*)
              key=${section##*dependencies.}
              key=${key//\"/}
              [[ ! -f "crates/$key/Cargo.toml" || "$key" == "$crate" ]] ||
                printf '%s\t%s\n' "$crate" "$key"
              ;;
          esac
          continue
          ;;
      esac
      case "$section" in
        *dependencies.*)
          dep=""
          if [[ "$line" == path*=*\"../*\"* ]]; then
            dep=${line#*\"../}
            dep=${dep%%\"*}
            dep=${dep%/}
          elif [[ "$line" == package*=*\"*\"* ]]; then
            dep=${line#*\"}
            dep=${dep%%\"*}
          fi
          ;;
        dependencies | dev-dependencies | build-dependencies | target.*.dependencies | target.*.dev-dependencies | target.*.build-dependencies)
          dep=""
          if [[ "$line" == *path*=*\"../*\"* ]]; then
            dep=${line#*path*=*\"../}
            dep=${dep%%\"*}
            dep=${dep%/}
          elif [[ "$line" == *package*=*\"*\"* ]]; then
            # A rename inheriting from [workspace.dependencies] has no path.
            dep=${line#*package*=*\"}
            dep=${dep%%\"*}
          else
            key=${line%%=*}
            key=${key%%[[:space:]]*}
            key=${key//\"/}
            dep=${key%%.*}
          fi
          ;;
        *) continue ;;
      esac
      [[ -z "$dep" || "$dep" == "$crate" || ! -f "crates/$dep/Cargo.toml" ]] ||
        printf '%s\t%s\n' "$crate" "$dep"
    done <"$manifest"
  done
}

# Prints the workspace crates depending on crate $1, directly or
# transitively, one per line in sorted order, from the workspace_edges
# records in $2.
reverse_dependents() {
  local root=$1 edges=$2 frontier=$1 found=$'\n' next dependent dependency
  while [[ -n "$frontier" ]]; do
    next=""
    while IFS=$'\t' read -r dependent dependency; do
      [[ -n "$dependent" && $'\n'"$frontier"$'\n' == *$'\n'"$dependency"$'\n'* ]] || continue
      [[ "$dependent" != "$root" && "$found" != *$'\n'"$dependent"$'\n'* ]] || continue
      found+="$dependent"$'\n'
      next+="$dependent"$'\n'
    done <<<"$edges"
    frontier=${next%$'\n'}
  done
  printf '%s' "${found#$'\n'}" | sort -u
}

# --print-edges prints the scanner's "<dependent>\t<dependency>" records,
# sorted; --print-rdeps prints "<root>\t<dependents, space-separated>" for
# each propagated root. Both expose exactly what the selection reads, for
# scripts/check-rdeps-parity.py to compare with `cargo metadata` in CI (a
# line scanner can miss a manifest style and drop crates silently, as the
# path-less `package =` rename did before intentd#2704). They read only
# crates/*/Cargo.toml: no git, BASE or end-to-end map.
if [[ -n "$print_mode" ]]; then
  edges=$(workspace_edges | sort -u)
  if [[ "$print_mode" == edges ]]; then
    [[ -z "$edges" ]] || printf '%s\n' "$edges"
  else
    for root in $rdeps_roots; do
      dependents=""
      while IFS= read -r dependent; do
        [[ -z "$dependent" ]] || dependents+="${dependents:+ }$dependent"
      done <<<"$(reverse_dependents "$root" "$edges")"
      printf '%s\t%s\n' "$root" "$dependents"
    done
  fi
  exit 0
fi

git rev-parse --git-dir >/dev/null 2>&1 || die 2 "$repo_root is not a git checkout"

# The end-to-end test map: source globs whose change also selects the
# end-to-end test modules that exercise them, because crates/<c>/src selects
# only <c>'s own tests and those modules live in another crate (intentd#2617:
# a script-binding change in intent-acp broke an intentd end-to-end test that
# only the merge queue ran). Format and rules are in the file's header.
e2e_map=.config/e2e-test-map.txt
e2e_entries=""
map_errors=""
map_count=0

# Sets resolve_problem (and returns 1) unless every segment of module path $2
# is declared by its parent, starting at the test binary root $1, and the file
# it resolves to exists. This is a line scanner, not a parser: a declaration
# is `[pub[(...)]] mod <seg>;` (or an inline `mod <seg> {`) after any
# `#[...]` attributes on its own or the preceding lines (blank and `//` lines
# may sit between). As rustc does, #[path = "<p>"] is relative to the
# declaring file's directory; otherwise a directory owner (the root, a mod.rs
# file or a #[path]-loaded file) looks in its own directory and any other
# <name>.rs in <name>/, for <seg>.rs then <seg>/mod.rs. An inline module's
# segments resolve in the same file (its file-module children are not
# supported).
resolve_module() {
  local file=$1 path=$2 rest=$2 seg line trimmed attribute attr found dir child owner=1
  resolve_problem=""
  if [[ "$path" == ::* || "$path" == *:: || "$path" == *::::* ]]; then
    resolve_problem="module $path: empty segment"
    return 1
  fi
  while [[ -n "$rest" ]]; do
    seg=${rest%%::*}
    if [[ "$rest" == *::* ]]; then rest=${rest#*::}; else rest=""; fi
    attr=""
    found=""
    while IFS= read -r line || [[ -n "$line" ]]; do
      trimmed=${line#"${line%%[![:space:]]*}"}
      # Leading attributes, several on one line allowed; a path attribute
      # names the module file.
      while [[ "$trimmed" == '#['*']'* ]]; do
        attribute=${trimmed%%]*}
        trimmed=${trimmed#*]}
        trimmed=${trimmed#"${trimmed%%[![:space:]]*}"}
        if [[ "$attribute" == '#[path'*=*\"*\"* ]]; then
          attr=${attribute#*\"}
          attr=${attr%\"*}
        fi
      done
      case "$trimmed" in
        "" | //*) continue ;;
        pub\ *) trimmed=${trimmed#pub } ;;
        pub\(*\)\ *) trimmed=${trimmed#*) } ;;
      esac
      case "$trimmed" in
        "mod $seg;"*)
          found=file
          break
          ;;
        "mod $seg {"* | "mod $seg{"*)
          found=inline
          break
          ;;
      esac
      attr=""
    done <"$file"
    if [[ -z "$found" ]]; then
      resolve_problem="module $path: $file declares no 'mod $seg;'"
      return 1
    fi
    [[ "$found" == file ]] || continue
    dir=${file%/*}
    if [[ -n "$attr" ]]; then
      child="$dir/$attr"
    else
      [[ -n "$owner" || "${file##*/}" == mod.rs ]] || dir=${file%.rs}
      child="$dir/$seg.rs"
      [[ -f "$child" || ! -f "$dir/$seg/mod.rs" ]] || child="$dir/$seg/mod.rs"
    fi
    if [[ ! -f "$child" ]]; then
      resolve_problem="module $path: $child does not exist"
      return 1
    fi
    file=$child
    owner=$attr
  done
}

# True when tests/$2.rs of crate $1 is a test target: cargo builds every
# tests/*.rs as one unless the manifest sets autotests = false, in which case
# only its [[test]] names count (intentd's suite modules are not targets).
is_test_target() {
  local manifest="crates/$1/Cargo.toml" line section="" autotests="" names=$'\n'
  [[ -f "crates/$1/tests/$2.rs" ]] || return 1
  [[ -f "$manifest" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line=${line#"${line%%[![:space:]]*}"}
    case "$line" in
      '['*) section=$line ;;
      autotests*=*false*) [[ "$section" != '[package]'* ]] || autotests=false ;;
      name*=*\"*\"*)
        if [[ "$section" == '[[test]]'* ]]; then
          line=${line#*\"}
          names+="${line%%\"*}"$'\n'
        fi
        ;;
    esac
  done <"$manifest"
  [[ "$autotests" != false || "$names" == *$'\n'"$2"$'\n'* ]]
}

# Reads the map into e2e_entries ("<glob>\t<crate>\t<binary>\t<module>"
# lines), collecting one map_errors line per invalid entry.
load_e2e_map() {
  local n=0 line glob target module extra crate binary root files file hit seen_globs=$'\n'
  [[ -f "$e2e_map" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    n=$((n + 1))
    glob="" target="" module="" extra=""
    read -r glob target module extra <<<"$line"
    [[ -n "$glob" && "$glob" != \#* ]] || continue
    if [[ -n "$extra" || "$target" != ?*::?* || "$target" == *::*::* ]]; then
      map_errors+="$e2e_map:$n: expected '<source glob> <crate>::<test binary> [<module path>]'"$'\n'
      continue
    fi
    map_count=$((map_count + 1))
    crate=${target%%::*}
    binary=${target#*::}
    # Entries usually repeat a glob once per module: look each up once.
    hit=""
    if [[ "$seen_globs" == *$'\n'"$glob"$'\n'* ]]; then
      hit=1
    else
      files=$(git -c core.quotePath=false ls-files --cached --others --exclude-standard -- "$glob" </dev/null) ||
        die 2 "git ls-files -- $glob failed (exit $?)"
      while IFS= read -r file; do
        # shellcheck disable=SC2053 # the glob is a pattern on purpose
        if [[ -n "$file" && -f "$file" && "$file" == $glob ]]; then
          hit=1
          seen_globs+="$glob"$'\n'
          break
        fi
      done <<<"$files"
    fi
    [[ -n "$hit" ]] || map_errors+="$e2e_map:$n: source glob '$glob' matches no file"$'\n'
    root="crates/$crate/tests/$binary.rs"
    if [[ ! -f "$root" ]]; then
      map_errors+="$e2e_map:$n: $target: $root does not exist"$'\n'
    elif ! is_test_target "$crate" "$binary"; then
      map_errors+="$e2e_map:$n: $target: crates/$crate/Cargo.toml sets autotests = false and declares no [[test]] named $binary"$'\n'
    elif [[ -n "$module" ]] && ! resolve_module "$root" "$module"; then
      map_errors+="$e2e_map:$n: $target $resolve_problem"$'\n'
    elif [[ -n "$hit" ]]; then
      e2e_entries+="$glob"$'\t'"$crate"$'\t'"$binary"$'\t'"$module"$'\n'
    fi
  done <"$e2e_map"
}

load_e2e_map
[[ -z "$check_map" || -f "$e2e_map" ]] || die 2 "$e2e_map does not exist"
if [[ -n "$map_errors" ]]; then
  while IFS= read -r line; do
    [[ -z "$line" ]] || echo "[$label] $line" >&2
  done <<<"$map_errors"
  die 2 "invalid end-to-end test map: fix or remove the entries above ($e2e_map header explains the format)"
fi
if [[ -n "$check_map" ]]; then
  log "$e2e_map: $map_count entries valid"
  exit 0
fi

merge_base=$(git merge-base HEAD "$base" 2>/dev/null) ||
  die 4 "cannot resolve BASE '$base'; run 'git fetch origin main' or set BASE=<ref>"

# Optional machine-readable argv, including an empty selection. It describes
# only the original plans; callback scope derivation cannot change execution.
# Resolve output relative to the caller, consistently with --runner.
[[ -z "$plan_json" || "$plan_json" == /* ]] || plan_json="$caller_dir/$plan_json"
export_plans() {
  [[ -n "$plan_json" ]] || return 0
  local head
  head=$(git rev-parse HEAD) || die 2 "cannot identify plan HEAD"
  python3 -I -B -S - "$plan_json" "$head" "$merge_base" "${plans-}" <<'PYTHON'
import json, pathlib, sys
path, head, base, plans = sys.argv[1:]
pathlib.Path(path).write_text(json.dumps({
    "version": 1, "head": head, "mergeBase": base,
    "plans": [line.split() for line in plans.splitlines() if line.strip()],
}, indent=2) + "\n")
PYTHON
}

# Paths outside crates/ that no test can observe.
is_inert() {
  case "$1" in
    *.md | LICENSE | NOTICE | .gitignore | deny.toml | release-plz.toml | dist-workspace.toml) return 0 ;;
    .github/* | docs/*) return 0 ;;
  esac
  return 1
}

is_fallback() {
  case "$1" in
    Cargo.toml | Cargo.lock | rust-toolchain.toml | .config/nextest.toml | .cargo/*) return 0 ;;
    crates/*/Cargo.toml | crates/*/build.rs) return 0 ;;
    crates/*) return 1 ;;
  esac
  # Anything else outside crates/ may be read by a test through
  # CARGO_MANIFEST_DIR (scripts/install.sh, repo-wide lints), so fail closed.
  ! is_inert "$1"
}

# Prints the suite target (tests/suite_*.rs stem) of crate $1 declaring the
# test module $2 (`mod <m>;` on a line of its own), or nothing.
suite_of() {
  local root line
  for root in "crates/$1/tests"/suite_*.rs; do
    [[ -f "$root" ]] || continue
    while IFS= read -r line; do
      if [[ "$line" == "mod $2;" ]]; then
        root=${root##*/}
        printf '%s\n' "${root%.rs}"
        return 0
      fi
    done <"$root"
  done
}

# True when a test file of crate $1 other than tests/$2.rs and tests/$2/**
# #[path]-includes something under tests/$2/: such a directory is shared, so
# a change in it cannot be narrowed to module $2.
dir_shared() {
  local crate=$1 module=$2 file
  while IFS= read -r file; do
    case "$file" in
      "crates/$crate/tests/$module.rs" | "crates/$crate/tests/$module"/*) ;;
      *) return 0 ;;
    esac
  done < <(grep -rlE --include='*.rs' "#\[path = \"([^\"]*/)?$module/" "crates/$crate/tests" 2>/dev/null || true)
  return 1
}

# Prints "<crate>\t<kind>[\t<test>]" for a path under crates/, kinds ranked
# test|module < tests < src < all; prints nothing for ignored paths. A module
# record's third field is "<suite>:<module>".
map_path() {
  local path=$1 rest crate sub name suite
  [[ "$path" == crates/*/* ]] || return 0
  rest=${path#crates/}
  crate=${rest%%/*}
  sub=${rest#*/}
  case "$sub" in
    tests/*)
      name=${sub#tests/}
      if [[ "$name" == *.rs && "$name" != */* && -f "$path" ]]; then
        name=${name%.rs}
        suite=$(suite_of "$crate" "$name")
        if [[ -z "$suite" ]]; then
          printf '%s\ttest\t%s\n' "$crate" "$name"
        # The instrumented run excludes auggie_context_e2e like coverage-e2e
        # does, so a plan naming only that module would select nothing.
        elif [[ -z "$instrumented" || "$crate" != intentd || "$name" != auggie_context_e2e ]]; then
          printf '%s\tmodule\t%s:%s\n' "$crate" "$suite" "$name"
        fi
      elif [[ "$name" == */* ]] && suite=$(suite_of "$crate" "${name%%/*}") &&
        [[ -n "$suite" ]] && ! dir_shared "$crate" "${name%%/*}"; then
        printf '%s\tmodule\t%s:%s\n' "$crate" "$suite" "${name%%/*}"
      else
        printf '%s\ttests\n' "$crate"
      fi
      ;;
    src/*) printf '%s\tsrc\n' "$crate" ;;
    benches/* | examples/*) ;;
    *) printf '%s\tall\n' "$crate" ;;
  esac
}

# Prints a selection record for each map entry whose glob matches path $1
# (every entry when $1 is empty): "<crate>\tmodule\t<binary>:<module path>",
# or "<crate>\ttest\t<binary>" for an entry naming a whole binary.
map_e2e() {
  local glob crate binary module
  while IFS=$'\t' read -r glob crate binary module; do
    [[ -n "$glob" ]] || continue
    # shellcheck disable=SC2053 # the glob is a pattern on purpose
    [[ -z "$1" || "$1" == $glob ]] || continue
    # As in map_path: the instrumented run excludes auggie_context_e2e, so
    # naming only that module would select nothing.
    [[ -z "$instrumented" || "$crate" != intentd || "${module%%::*}" != auggie_context_e2e ]] || continue
    if [[ -n "$module" ]]; then
      printf '%s\tmodule\t%s:%s\n' "$crate" "$binary" "$module"
    else
      printf '%s\ttest\t%s\n' "$crate" "$binary"
    fi
  done <<<"$e2e_entries"
}

fallback=""
mapped=""
seen=""
e2e_notes=""
classify() {
  local path=$1 e2e
  [[ -n "$path" && "$seen" != *$'\n'"$path"$'\n'* ]] || return 0
  seen+=$'\n'"$path"$'\n'
  # Editing the map runs every module it names (a new entry is exercised by
  # the change that adds it) instead of deferring to the full suite; deleting
  # it falls through to the outside-crates/ fallback.
  if [[ "$path" == "$e2e_map" && -f "$e2e_map" ]]; then
    e2e=$(map_e2e "")
    mapped+="$e2e"$'\n'
    e2e_notes+="note: $e2e_map changed; every mapped end-to-end module runs"$'\n'
    return 0
  fi
  e2e=""
  [[ -z "$e2e_entries" ]] || e2e=$(map_e2e "$path")
  if [[ -n "$e2e" ]]; then
    mapped+="$e2e"$'\n'
    e2e_notes+="note: $path is mapped to end-to-end tests in $e2e_map"$'\n'
  fi
  if is_fallback "$path"; then
    fallback+="$path"$'\n'
    # Under --instrumented a build-wide file inside a crate still selects
    # that crate (the "<anything else>" row of the table).
    [[ -z "$instrumented" ]] || mapped+="$(map_path "$path")"$'\n'
  else
    mapped+="$(map_path "$path")"$'\n'
  fi
}

# Committed + staged + unstaged edits since the merge base, then untracked
# files. Both are read NUL-delimited so paths arrive raw (line-oriented
# porcelain output C-quotes names with spaces or non-ASCII characters) and
# with rename detection off so both sides of a move count as changed. The
# output goes through a file so a failing git command is not silently read
# as an empty change set.
changed_file=$(mktemp "${TMPDIR:-/tmp}/changed-tests.XXXXXX")
trap 'rm -f "$changed_file"' EXIT
git diff --name-only -z --no-renames "$merge_base" -- >"$changed_file" ||
  die 2 "git diff --name-only $merge_base failed (exit $?)"
git ls-files -z --others --exclude-standard >>"$changed_file" ||
  die 2 "git ls-files --others failed (exit $?)"
while IFS= read -r -d '' path; do
  classify "$path"
done <"$changed_file"

if [[ -n "$fallback" ]]; then
  if [[ -n "$instrumented" ]]; then
    log "build-wide change(s) vs $base; the full instrumented suite runs in the merge queue (coverage-e2e / coverage-all), running the mapped crates/ selection only:"
    printf '%s' "$fallback" | while IFS= read -r path; do echo "  $path"; done
  else
    echo "[$label] build-wide change(s) vs $base need the full suite -- run 'make test':" >&2
    printf '%s' "$fallback" | while IFS= read -r path; do echo "  $path" >&2; done
    exit 3
  fi
fi

while IFS= read -r note; do
  [[ -z "$note" ]] || log "$note"
done <<<"$e2e_notes"

rdeps_propagated=""
# Dependents of a propagated root: their own dependents are selected too, so
# no "downstream crates are not selected" note is printed for them.
rdeps_selected=""
rdeps_edges=""
for root in $rdeps_roots; do
  root_changed=""
  while IFS=$'\t' read -r rec_crate rec_kind _; do
    if [[ "$rec_crate" == "$root" && ("$rec_kind" == src || "$rec_kind" == all) ]]; then
      root_changed=1
    fi
  done <<<"$mapped"
  [[ -n "$root_changed" ]] || continue
  [[ -n "$rdeps_edges" ]] || rdeps_edges=$(workspace_edges)
  dependents=""
  while IFS= read -r dependent; do
    [[ -n "$dependent" ]] || continue
    mapped+="$dependent"$'\t'"dependent"$'\n'
    dependents+="${dependents:+, }$dependent"
    rdeps_selected+=" $dependent "
  done <<<"$(reverse_dependents "$root" "$rdeps_edges")"
  rdeps_propagated+=" $root "
  if [[ -n "$dependents" ]]; then
    log "note: crates/$root changed; its reverse dependencies run too ($dependents)"
  else
    log "note: crates/$root changed; no workspace crate depends on it"
  fi
done

# The feature crates (crates/intent-svc-*) sit above intent-services and build
# against its feature API, so an intent-services/src edit also selects every
# feature crate's own tests ("dependent": the src selection, without a note).
# intent-app composes the feature crates and holds the tests that need them
# together with intent-transport, so a src edit in intent-services or in any
# feature crate also selects intent-app.
feature_crates=""
if [[ "$mapped" == *$'\n'"intent-services"$'\t'"src"* || "$mapped" == "intent-services"$'\t'"src"* ]]; then
  for manifest in crates/intent-svc-*/Cargo.toml; do
    [[ -f "$manifest" ]] || continue
    feature_crate=${manifest#crates/}
    feature_crate=${feature_crate%/Cargo.toml}
    mapped+="$feature_crate"$'\t'"dependent"$'\n'
    feature_crates+="${feature_crates:+, }$feature_crate"
  done
fi
composed_src=""
while IFS=$'\t' read -r rec_crate rec_kind _; do
  case "$rec_crate" in
    intent-services | intent-svc-*)
      if [[ "$rec_kind" == src ]]; then composed_src=1; fi
      ;;
  esac
done <<<"$mapped"
if [[ -n "$composed_src" && -f crates/intent-app/Cargo.toml ]]; then
  mapped+="intent-app"$'\t'"dependent"$'\n'
  feature_crates+="${feature_crates:+, }intent-app"
fi

# Stock macOS ships Bash 3.2, which has no associative arrays: per-crate
# lookups scan the newline-separated "<crate>\t<kind>[\t<test>]" records.
crates=""
while IFS=$'\t' read -r crate _; do
  [[ -n "$crate" ]] && crates+="$crate"$'\n'
done <<<"$mapped"
crates=$(printf '%s' "$crates" | sort -u)

if [[ -z "$crates" ]]; then
  log "nothing to test (no Rust changes vs $base)"
  export_plans
  exit 0
fi

# Sets crate_filter to the target flags for one crate ("" = every test target).
# Suite modules add their suite as a --test target and one -E naming the
# changed modules; standalone targets of the same plan are ORed into that -E.
crate_selection() {
  local crate=$1 best=test tests="" modules="" c kind test
  while IFS=$'\t' read -r c kind test; do
    [[ "$c" == "$crate" ]] || continue
    case "$kind" in
      all) best=all ;;
      src) [[ "$best" == all ]] || best=src ;;
      dependent) [[ "$best" == all || "$best" == src ]] || best=dependent ;;
      tests) [[ "$best" == all || "$best" == src || "$best" == dependent ]] || best=tests ;;
      test) tests+="$test"$'\n' ;;
      module) modules+="$test"$'\n' ;;
    esac
  done <<<"$mapped"
  case "$best" in
    all) crate_filter="" ;;
    src | dependent)
      crate_filter="--bins --tests"
      [[ -f "crates/$crate/src/lib.rs" ]] && crate_filter="--lib $crate_filter"
      if [[ "$best" == dependent || "$rdeps_propagated$rdeps_selected" == *" $crate "* ]]; then
        :
      elif [[ "$crate" == intent-services && -n "$feature_crates" ]]; then
        log "note: crates/$crate/src changed; its feature crates and intent-app ($feature_crates) run too, other downstream crates are not selected"
      elif [[ "$crate" == intent-svc-* && -f crates/intent-app/Cargo.toml ]]; then
        log "note: crates/$crate/src changed; intent-app runs too, other downstream crates are not selected"
      else
        log "note: crates/$crate/src changed; only $crate's own tests run (downstream crates are not selected)"
      fi
      ;;
    tests) crate_filter="--tests" ;;
    test)
      crate_filter=""
      local expr="" suite module names
      tests=$(printf '%s' "$tests" | sort -u)
      while IFS= read -r test; do
        [[ -n "$test" ]] || continue
        crate_filter+="${crate_filter:+ }--test $test"
        expr+="${expr:+|}binary_id(=$crate::$test)"
      done <<<"$tests"
      # A binary selected whole (its root changed, or a map entry names it
      # without a module) already runs its modules: drop them, so it is
      # named once.
      local kept=""
      while IFS= read -r module; do
        [[ -n "$module" && $'\n'"$tests"$'\n' != *$'\n'"${module%%:*}"$'\n'* ]] || continue
        kept+="$module"$'\n'
      done <<<"$modules"
      modules=$kept
      if [[ -n "$modules" ]]; then
        modules=$(printf '%s' "$modules" | sort -u)
        while IFS= read -r suite; do
          [[ -n "$suite" ]] || continue
          names=""
          while IFS=: read -r c module; do
            [[ "$c" == "$suite" ]] && names+="${names:+|}$module"
          done <<<"$modules"
          crate_filter+="${crate_filter:+ }--test $suite"
          expr+="${expr:+|}(binary_id(=$crate::$suite)&test(/^($names)::/))"
        done <<<"$(while IFS= read -r module; do printf '%s\n' "${module%%:*}"; done <<<"$modules" | sort -u)"
        crate_filter+=" -E $expr"
      fi
      ;;
  esac
}

selections=""
while IFS= read -r crate; do
  crate_selection "$crate"
  selections+="$crate"$'\t'"$crate_filter"$'\n'
done <<<"$crates"

# One invocation per distinct filter, in first-crate order: cargo applies
# target flags to every -p package on the command line.
plans=""
grouped=""
while IFS=$'\t' read -r crate filter; do
  [[ -n "$crate" && "$grouped" != *" $crate "* ]] || continue
  members=""
  while IFS=$'\t' read -r other other_filter; do
    if [[ -n "$other" && "$other_filter" == "$filter" ]]; then
      members+="-p $other "
      grouped+=" $other "
    fi
  done <<<"$selections"
  plans+="$members$filter"$'\n'
done <<<"$selections"

export_plans

extra=""
[[ -n "$build_jobs" ]] && extra+=" --build-jobs $build_jobs"
[[ -n "$test_threads" ]] && extra+=" --test-threads $test_threads"

# The instrumented form mirrors scripts/coverage-e2e.sh: same env, same
# auggie_context_e2e exclusion (it needs a real auggie binary).
cov_env="INTENTD_TEST_TIMEOUT_MULTIPLIER=3"
cov_filter="not test(/^auggie_context_e2e::/)"

# Splits plan $1 (flags assembled above: no quoting, no glob characters) into
# plan_args and its filterset plan_expr. Under --instrumented plan_expr also
# carries the coverage exclusion, ANDed into the plan's own -E (nextest ORs
# repeated -E flags, which would re-admit every excluded test).
plan_words() {
  local word expect_expr=""
  plan_args=()
  plan_expr=""
  # shellcheck disable=SC2086
  for word in $1; do
    if [[ -n "$expect_expr" ]]; then
      plan_expr=$word
      expect_expr=""
    elif [[ "$word" == -E ]]; then
      expect_expr=1
    else
      plan_args+=("$word")
    fi
  done
  if [[ -n "$instrumented" ]]; then
    plan_expr=${plan_expr:+($plan_expr) and ($cov_filter)}
    plan_expr=${plan_expr:-$cov_filter}
  fi
}

# The plan as a pasteable command line: the filterset goes last, quoted.
describe() {
  plan_words "${1% }"
  local tail=""
  [[ -z "$plan_expr" ]] || tail=" -E '$plan_expr'"
  if [[ -n "$instrumented" ]]; then
    echo "$cov_env cargo llvm-cov --no-report nextest ${plan_args[*]}$extra$tail"
  else
    echo "cargo nextest run ${plan_args[*]}$extra$tail"
  fi
}

while IFS= read -r plan; do
  [[ -n "$plan" ]] || continue
  log "$(describe "$plan")"
done <<<"$plans"

[[ -z "$dry_run" ]] || exit 0

if [[ -n "$runner" ]]; then
  # The runner takes over from here: exec drops the EXIT trap, so the scratch
  # file goes first. The command line is shell-quoted (paths may be quoted in
  # the Makefile); the plans and flags are appended as single words.
  rm -f "$changed_file"
  eval "set -- $runner"
  while IFS= read -r plan; do
    [[ -n "$plan" ]] || continue
    set -- "$@" --plan "${plan% }"
  done <<<"$plans"
  set -- "$@" --base "$base" --label test-changed
  [[ -n "$build_jobs" ]] && set -- "$@" --build-jobs "$build_jobs"
  [[ -n "$test_threads" ]] && set -- "$@" --test-threads "$test_threads"
  cd "$caller_dir"
  exec "$BASH" "$test_policy" "$@"
fi

# The plans are read from fd 3 so cargo keeps the caller's stdin.
scope_index=0
while IFS= read -r -u 3 plan; do
  [[ -n "$plan" ]] || continue
  # $extra holds only the flags assembled above; split it on purpose.
  plan_words "$plan"
  # shellcheck disable=SC2086
  set -- "${plan_args[@]}" $extra
  [[ -z "$plan_expr" ]] || set -- "$@" -E "$plan_expr"
  scope_index=$((scope_index + 1))
  set +e
  if [[ -n "$instrumented" ]]; then
    command=("$BASH" "$test_policy" cargo llvm-cov --no-report nextest "$@")
    if [[ -n "${COVERAGE_STORAGE_DIR:-}" ]]; then
      command=(python3 -I -B "$repo_root/scripts/coverage-storage.py" run
        --output "$COVERAGE_STORAGE_DIR" --stage "scope-$scope_index" -- "${command[@]}")
    fi
    INTENTD_TEST_TIMEOUT_MULTIPLIER=3 "${command[@]}"
  else
    "$BASH" "$test_policy" cargo nextest run "$@"
  fi
  status=$?
  set -e
  [[ "$status" -eq 0 ]] || die "$status" "$(describe "$plan") exited $status"
done 3<<<"$plans"
