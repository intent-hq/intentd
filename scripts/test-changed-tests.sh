#!/usr/bin/env bash
# Regression tests for changed-tests.sh: runs the real script against a
# fixture git checkout with stubbed cargo / runner binaries (no toolchain, no
# network) and asserts the plans it prints, the argv it hands to cargo or the
# runner, its exit codes, and the --instrumented (llvm-cov) command shape.
# Ends with a Bash 3.2 compatibility pass (stock macOS bash; the monorepo
# Makefile runs this script there).
#
# Run directly: ./scripts/test-changed-tests.sh
set -euo pipefail

# The caller's environment must not steer the script under test (a shell with
# BASE=HEAD or DRY_RUN=1 exported would change every expected argv).
unset BASE DRY_RUN BUILD_JOBS TEST_THREADS NEXTEST_SHOW_PROGRESS CARGO_TERM_PROGRESS_WHEN
unset NEXTEST_RUNNER INTENTD_TEST_TIMEOUT_MULTIPLIER
unset RUN_STDIN INTENTD_ASSERT_BOUND_CALLER COVERAGE_STORAGE_DIR
unset CARGO_LOCALE_LOG RUNNER_LOCALE_LOG RUN_LOCALE

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
script="$here/changed-tests.sh"
# The interpreter that runs the script under test defaults to the one running
# this suite; the Bash 3.2 compatibility pass at the end re-executes the suite
# with it set to a real Bash 3.
script_bash=${CHANGED_TESTS_TEST_BASH:-$BASH}
temp_dir=$(cd "$(mktemp -d)" && pwd -P)
bin_dir="$temp_dir/bin"
# The fixture checkout carries a copy of the script at scripts/ so the script
# resolves the repo root from its own location, as the real one does.
repo="$temp_dir/repo"
mkdir -p "$bin_dir" "$repo/scripts"
cp "$script" "$repo/scripts/changed-tests.sh"
cp "$here/coverage-storage.py" "$repo/scripts/"
if [[ -f "$here/with-test-policy.sh" ]]; then
  cp "$here/with-test-policy.sh" "$repo/scripts/"
fi
script="$repo/scripts/changed-tests.sh"
trap 'rm -rf "$temp_dir"' EXIT

fail() {
  echo "changed-tests test failed: $*" >&2
  exit 1
}

for command in bash dirname env grep mktemp rm sort python3; do
  ln -s "$(command -v "$command")" "$bin_dir/$command"
done

# glibc's en_US.UTF-8 collation ignores '-' and '_' (intentd sorts before
# intent-services, ab before a_z), which is what the locale cases below run
# the script under. A host that has no compiled en_US.UTF-8 gets a private build in the
# temp dir, reached through LOCPATH, when localedef and the en_US source
# exist (glibc ignores an empty LOCPATH). Without either (macOS collates
# by code point), those cases are skipped with a notice. RUN_LOCALE=<name>
# run_script hands the locale to the script alone, through env: a prefix
# LC_ALL on the function would switch this shell too, which cannot see
# LOCPATH and warns.
real_env=$(command -v env)
ignores_punctuation() {
  [[ "$(printf 'intent-services\nintentd\na_z\nab\n' | LOCPATH="$1" LC_ALL=en_US.UTF-8 sort 2>/dev/null)" == $'ab\na_z\nintentd\nintent-services' ]]
}
en_us_locpath=""
en_us=1
if ! ignores_punctuation ""; then
  en_us_locpath="$temp_dir/locale"
  mkdir -p "$en_us_locpath"
  localedef -i en_US -f UTF-8 "$en_us_locpath/en_US.UTF-8" >/dev/null 2>&1 || true
  ignores_punctuation "$en_us_locpath" || en_us=""
fi
[[ -n "$en_us" ]] ||
  echo "changed-tests tests: no en_US.UTF-8 locale that ignores '-' and '_' (glibc's) is available; en_US.UTF-8 cases skipped"

# git wrapper: the subcommand named by GIT_STUB_FAIL fails like a broken
# checkout would; everything else reaches the real git.
real_git=$(command -v git)
cat >"$bin_dir/git" <<SH
#!/usr/bin/env bash
if [[ -n "\${GIT_STUB_FAIL-}" && "\${1-}" == "\$GIT_STUB_FAIL" ]]; then
  echo "fatal: stubbed git \$1 failure" >&2
  exit 128
fi
exec "$real_git" "\$@"
SH
chmod +x "$bin_dir/git"

# Stub cargo: every invocation is appended to CARGO_TEST_LOG as
# "<cwd>: [INTENTD_TEST_TIMEOUT_MULTIPLIER=N ]<argv>", the LC_ALL it got
# ("<unset>" when none) to CARGO_LOCALE_LOG, and it exits with
# CARGO_STUB_EXIT (default 0). It drains stdin like a real nextest child
# could, so the script must not feed it the remaining plans.
cat >"$bin_dir/cargo" <<'SH'
#!/usr/bin/env bash
[[ "${INTENTD_ASSERT_BOUND_CALLER-}" == 1 ]] || { echo "cargo: caller policy is not armed" >&2; exit 91; }
printf '%s: %s%s\n' "$PWD" "${INTENTD_TEST_TIMEOUT_MULTIPLIER:+INTENTD_TEST_TIMEOUT_MULTIPLIER=$INTENTD_TEST_TIMEOUT_MULTIPLIER }" "$*" >>"$CARGO_TEST_LOG"
printf '%s\n' "${LC_ALL-<unset>}" >>"$CARGO_LOCALE_LOG"
while IFS= read -r line; do printf '%s\n' "$line" >>"$CARGO_STDIN_LOG"; done
exit "${CARGO_STUB_EXIT:-0}"
SH
chmod +x "$bin_dir/cargo"

# Stub runner: appends "call:" plus one line per argv word to RUNNER_TEST_LOG,
# its cwd to RUNNER_CWD_LOG, the LC_ALL it got to RUNNER_LOCALE_LOG, and exits
# with RUNNER_STUB_EXIT (default 0).
cat >"$bin_dir/runner" <<'SH'
#!/usr/bin/env bash
[[ "${INTENTD_ASSERT_BOUND_CALLER-}" == 1 ]] || { echo "runner: caller policy is not armed" >&2; exit 91; }
{ echo "call:"; printf '%s\n' "$@"; } >>"$RUNNER_TEST_LOG"
printf '%s\n' "$PWD" >>"$RUNNER_CWD_LOG"
printf '%s\n' "${LC_ALL-<unset>}" >>"$RUNNER_LOCALE_LOG"
exit "${RUNNER_STUB_EXIT:-0}"
SH
chmod +x "$bin_dir/runner"

export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.invalid
export GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.invalid

g() {
  git -C "$repo" "$@"
}

# Fixture workspace: alpha has a lib and two integration tests with shared
# helpers, beta is a binary-only crate, gamma mirrors alpha's smoke test name,
# intentd carries suite targets (tests/suite_*.rs roots declaring file
# modules, like the real crate): suite_e2e holds the auggie_context_e2e module
# the instrumented run excludes, e2e_core (with a module directory) and
# e2e_shared (whose directory another target #[path]-includes); suite_wss
# holds e2e_wss_x; e2e_wss_agent_lifecycle stays a standalone target.
# delta's src files are named by .config/e2e-test-map.txt, which maps them to
# intentd end-to-end modules: a nested module (e2e_core::inner), a #[path]
# module of a non-root file (e2e_shared::helper), a #[path] module of a
# standalone target (helper) and a whole binary (suite_wss).
write() {
  mkdir -p "$repo/$(dirname "$1")"
  printf '%s\n' "${2:-$1}" >"$repo/$1"
}
write Cargo.toml '[workspace]'
write Cargo.lock
write rust-toolchain.toml
write .config/nextest.toml
write .cargo/config.toml
write crates/alpha/Cargo.toml $'[package]\nname = "alpha"\n\n[dependencies]\nintent-core = { path = "../intent-core", version = "0.1" }'
write crates/alpha/src/lib.rs
write crates/alpha/src/util/mod.rs
write crates/alpha/tests/one.rs
write crates/alpha/tests/two.rs
write crates/alpha/tests/common/mod.rs
write crates/alpha/tests/fixtures/data.json
write crates/alpha/tests/fixtures/café.json
write crates/alpha/benches/bench.rs
write crates/alpha/examples/demo.rs
write crates/beta/Cargo.toml $'[package]\nname = "beta"\n\n[dependencies]\n# intent-core = { path = "../intent-core" }\nserde = "1"'
write crates/beta/build.rs
write crates/beta/src/main.rs
write crates/beta/tests/smoke.rs
write crates/beta/migrations/001.sql
write crates/gamma/Cargo.toml $'[package]\nname = "gamma"\n\n[dev-dependencies]\ncore = { package = "intent-core", path = "../intent-core/" }'
write crates/gamma/src/lib.rs
write crates/gamma/tests/smoke.rs
write crates/intent-core/Cargo.toml $'[package]\nname = "intent-core"'
write crates/intent-core/src/lib.rs
write crates/intent-core/tests/core.rs
write crates/intent-core/resources/data.txt
write crates/intent-services/Cargo.toml $'[package]\nname = "intent-services"\n\n[target."cfg(unix)".dependencies]\nintent-core.workspace = true'
write crates/intent-services/src/lib.rs
write crates/intent-services/tests/smoke.rs
write crates/intent-svc-transfer/Cargo.toml $'[package]\nname = "intent-svc-transfer"\n\n[dependencies]\nintent-services = { path = "../intent-services" }'
write crates/intent-svc-transfer/src/lib.rs
write crates/intent-svc-transfer/tests/smoke.rs
write crates/intent-app/Cargo.toml $'[package]\nname = "intent-app"\n\n[dependencies]\nintent-services = { path = "../intent-services" }'
write crates/intent-app/src/lib.rs
write crates/intent-app/tests/smoke.rs
write crates/intentd/Cargo.toml $'[package]\nname = "intentd"\n\n[dependencies.intent-services]\nworkspace = true'
write crates/intentd/src/main.rs
write crates/intentd/tests/auggie_context_e2e.rs
write crates/intentd/tests/e2e_core.rs $'//! fixture module\n\nmod inner;'
write crates/intentd/tests/e2e_core/inner.rs
write crates/intentd/tests/e2e_shared.rs $'#[path = "e2e_shared/helper.rs"]\n#[allow(dead_code)]\npub(crate) mod helper;'
write crates/intentd/tests/e2e_shared/helper.rs
write crates/intentd/tests/e2e_wss_x.rs
write crates/intentd/tests/suite_e2e.rs $'//! fixture suite\nmod common;\n\nmod auggie_context_e2e;\nmod e2e_core;\nmod e2e_shared;'
write crates/intentd/tests/suite_wss.rs $'mod common;\nmod e2e_wss_x;'
write crates/intentd/tests/common/mod.rs
write crates/intentd/tests/e2e_wss_agent_lifecycle.rs $'mod common;\n#[path = "e2e_shared/helper.rs"]\nmod helper;'
write crates/delta/Cargo.toml $'[package]\nname = "delta"\n\n[build-dependencies.alpha]\npath = "../alpha"'
write crates/delta/src/lib.rs
write crates/delta/src/script.rs
write crates/delta/src/monitor.rs
write crates/delta/src/whole.rs
write .config/e2e-test-map.txt '# Fixture end-to-end map.
# <source glob>              <crate>::<test binary>            [<module path>]

crates/delta/src/script.rs   intentd::suite_e2e                e2e_core::inner
crates/delta/src/script.rs   intentd::e2e_wss_agent_lifecycle  helper
crates/delta/src/monitor*.rs intentd::suite_e2e                e2e_shared::helper
crates/delta/src/whole.rs    intentd::suite_wss'
write README.md
write docs/guide.md
write scripts/tool.sh
g init -q
g add -A
g commit -q -m base
base_sha=$(g rev-parse HEAD)
g update-ref refs/remotes/origin/main HEAD
g checkout -q -b feature

clear_logs() {
  local log
  for log in cargo cargo-stdin cargo-locale runner runner-cwd runner-locale; do
    : >"$temp_dir/$log.log"
  done
}

reset_repo() {
  g reset -q --hard refs/remotes/origin/main
  g clean -fdq
  clear_logs
}

edit() {
  echo "// changed" >>"$repo/$1"
}

commit_all() {
  g add -A
  g commit -q -m "${1:-change}"
}

# Env prefixes on the call (DRY_RUN=1 run_script ...) reach the script; the
# inputs it reads default to unset here so the suite's own environment cannot
# leak into the expected argv. The script runs from RUN_CWD (default: the
# fixture checkout); it must find its repo root on its own. Ordinary calls
# get EOF, even when this suite inherits an open terminal. Only the explicit
# stdin-drain control supplies finite input through RUN_STDIN, and
# RUN_LOCALE=en_US.UTF-8 runs it under that locale (see ignores_punctuation).
run_script() {
  set +e
  (
    if [[ -n "${RUN_LOCALE-}" ]]; then
      set -- "$real_env" LOCPATH="$en_us_locpath" LC_ALL="$RUN_LOCALE" "$script_bash" "$script" "$@"
    else
      set -- "$script_bash" "$script" "$@"
    fi
    cd "${RUN_CWD:-$repo}" &&
      PATH="$bin_dir" BASE="${BASE-}" BUILD_JOBS="${BUILD_JOBS-}" TEST_THREADS="${TEST_THREADS-}" \
        DRY_RUN="${DRY_RUN-}" NEXTEST_RUNNER="${NEXTEST_RUNNER-}" \
        CARGO_TEST_LOG="$temp_dir/cargo.log" CARGO_STDIN_LOG="$temp_dir/cargo-stdin.log" \
        RUNNER_TEST_LOG="$temp_dir/runner.log" \
        RUNNER_CWD_LOG="$temp_dir/runner-cwd.log" \
        CARGO_LOCALE_LOG="$temp_dir/cargo-locale.log" RUNNER_LOCALE_LOG="$temp_dir/runner-locale.log" \
        "$@"
  ) <"${RUN_STDIN:-/dev/null}" >"$temp_dir/stdout" 2>"$temp_dir/stderr"
  status=$?
  set -e
  stdout=$(<"$temp_dir/stdout")
  stderr=$(<"$temp_dir/stderr")
  cargo_log=$(<"$temp_dir/cargo.log")
  runner_log=$(<"$temp_dir/runner.log")
  cargo_locale_log=$(<"$temp_dir/cargo-locale.log")
  runner_locale_log=$(<"$temp_dir/runner-locale.log")
}

expect_cargo() {
  local expected="" line
  for line in "$@"; do
    expected+="$repo: nextest run $line"$'\n'
  done
  [[ "$cargo_log" == "${expected%$'\n'}" ]] || fail "$case_name: cargo argv was"$'\n'"$cargo_log"$'\n'"expected"$'\n'"${expected%$'\n'}"
}

# The instrumented argv: same env and trailing filterset as coverage-e2e.sh.
expect_cov() {
  local expected="" line
  for line in "$@"; do
    expected+="$repo: INTENTD_TEST_TIMEOUT_MULTIPLIER=3 llvm-cov --no-report nextest $line -E not test(/^auggie_context_e2e::/)"$'\n'
  done
  [[ "$cargo_log" == "${expected%$'\n'}" ]] || fail "$case_name: cargo argv was"$'\n'"$cargo_log"$'\n'"expected"$'\n'"${expected%$'\n'}"
}

expect_plan() {
  local line
  for line in "$@"; do
    [[ "$stdout" == *"[test-changed] cargo nextest run $line"* ]] || fail "$case_name: plan is missing '$line':"$'\n'"$stdout"
  done
  [[ "$(grep -c '^\[test-changed\] cargo nextest run ' <<<"$stdout")" -eq $# ]] || fail "$case_name: expected $# plan line(s):"$'\n'"$stdout"
}

expect_cov_plan() {
  local line
  for line in "$@"; do
    [[ "$stdout" == *"[coverage-changed] INTENTD_TEST_TIMEOUT_MULTIPLIER=3 cargo llvm-cov --no-report nextest $line -E 'not test(/^auggie_context_e2e::/)'"* ]] || fail "$case_name: plan is missing '$line':"$'\n'"$stdout"
  done
  [[ "$(grep -c '^\[coverage-changed\] INTENTD_TEST_TIMEOUT_MULTIPLIER=3 cargo llvm-cov ' <<<"$stdout")" -eq $# ]] || fail "$case_name: expected $# plan line(s):"$'\n'"$stdout"
}

expect_ok() {
  [[ "$status" -eq 0 ]] || fail "$case_name: exited $status: $stderr"
  [[ -z "$stderr" ]] || fail "$case_name: unexpected stderr: $stderr"
}

case_name="clean tree"
reset_repo
run_script
expect_ok
[[ "$stdout" == "[test-changed] nothing to test (no Rust changes vs origin/main)" ]] || fail "$case_name printed '$stdout'"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"

case_name="committed single test file"
reset_repo
edit crates/alpha/tests/one.rs
commit_all
run_script
expect_ok
expect_plan "-p alpha --test one"
expect_cargo "-p alpha --test one"
[[ "$stdout" != *"note:"* ]] || fail "$case_name printed the src note: $stdout"

case_name="script invoked from another cwd still runs cargo from the repo root"
reset_repo
edit crates/alpha/tests/one.rs
RUN_CWD="$temp_dir" run_script
expect_ok
expect_cargo "-p alpha --test one"

case_name="unstaged, staged and untracked edits"
reset_repo
edit crates/alpha/tests/one.rs
edit crates/alpha/tests/two.rs
g add crates/alpha/tests/two.rs
write crates/gamma/tests/fresh.rs
run_script
expect_ok
expect_cargo "-p alpha --test one --test two" "-p gamma --test fresh"

case_name="committed change under a feature branch with main moved on"
reset_repo
edit crates/alpha/tests/one.rs
commit_all
g checkout -q --detach refs/remotes/origin/main
edit crates/beta/tests/smoke.rs
commit_all "main moved on"
g update-ref refs/remotes/origin/main HEAD
g checkout -q feature
run_script
expect_ok
expect_cargo "-p alpha --test one"
g update-ref refs/remotes/origin/main "$base_sha"

case_name="shared test helpers select every integration test of the crate"
reset_repo
edit crates/alpha/tests/common/mod.rs
edit crates/alpha/tests/one.rs
run_script
expect_ok
expect_plan "-p alpha --tests"
expect_cargo "-p alpha --tests"

case_name="test fixture selects every integration test of the crate"
reset_repo
edit crates/alpha/tests/fixtures/data.json
run_script
expect_ok
expect_cargo "-p alpha --tests"

# Git C-quotes such names in line-oriented output; the script must read raw
# NUL-delimited paths.
case_name="untracked fixture with a space in its name"
reset_repo
write "crates/alpha/tests/fixtures/new fixture.json"
run_script
expect_ok
expect_cargo "-p alpha --tests"

case_name="tracked fixture with a non-ASCII name"
reset_repo
edit crates/alpha/tests/fixtures/café.json
run_script
expect_ok
expect_cargo "-p alpha --tests"

# Both sides of a rename count as changed (rename detection is off), so a
# moved test is a deleted one plus a new one.
case_name="renamed integration test counts the old and new path"
reset_repo
g mv crates/alpha/tests/one.rs crates/alpha/tests/moved.rs
run_script
expect_ok
expect_cargo "-p alpha --tests"

case_name="file moved from src into tests keeps the src selection"
reset_repo
g mv crates/alpha/src/util/mod.rs crates/alpha/tests/old.rs
run_script
expect_ok
expect_plan "-p alpha --lib --bins --tests"
expect_cargo "-p alpha --lib --bins --tests"
[[ "$stdout" == *"note: crates/alpha/src changed"* ]] || fail "$case_name: no src note: $stdout"

case_name="deleted integration test falls back to --tests"
reset_repo
g rm -q crates/alpha/tests/two.rs
run_script
expect_ok
expect_cargo "-p alpha --tests"

case_name="src change selects lib, bins and integration tests with a note"
reset_repo
edit crates/alpha/src/util/mod.rs
edit crates/alpha/tests/one.rs
run_script
expect_ok
expect_plan "-p alpha --lib --bins --tests"
expect_cargo "-p alpha --lib --bins --tests"
[[ "$stdout" == *"[test-changed] note: crates/alpha/src changed; only alpha's own tests run (downstream crates are not selected)"* ]] || fail "$case_name: no src note: $stdout"
[[ "$(grep -c 'note:' <<<"$stdout")" -eq 1 ]] || fail "$case_name: note printed more than once: $stdout"

case_name="intent-services src change also selects the feature crates and intent-app above it"
reset_repo
edit crates/intent-services/src/lib.rs
run_script
expect_ok
expect_plan "-p intent-app -p intent-services -p intent-svc-transfer --lib --bins --tests"
expect_cargo "-p intent-app -p intent-services -p intent-svc-transfer --lib --bins --tests"
[[ "$stdout" == *"[test-changed] note: crates/intent-services/src changed; its feature crates and intent-app (intent-svc-transfer, intent-app) run too, other downstream crates are not selected"* ]] || fail "$case_name: no src note: $stdout"
[[ "$(grep -c 'note:' <<<"$stdout")" -eq 1 ]] || fail "$case_name: expected one note: $stdout"

case_name="intent-services test-only change does not select the feature crates"
reset_repo
edit crates/intent-services/tests/smoke.rs
run_script
expect_ok
expect_cargo "-p intent-services --test smoke"

case_name="feature crate src change selects intent-app, not intent-services"
reset_repo
edit crates/intent-svc-transfer/src/lib.rs
run_script
expect_ok
expect_cargo "-p intent-app -p intent-svc-transfer --lib --bins --tests"
[[ "$stdout" == *"[test-changed] note: crates/intent-svc-transfer/src changed; intent-app runs too, other downstream crates are not selected"* ]] || fail "$case_name: no src note: $stdout"
[[ "$(grep -c 'note:' <<<"$stdout")" -eq 1 ]] || fail "$case_name: expected one note: $stdout"

case_name="feature crate test-only change does not select intent-app"
reset_repo
edit crates/intent-svc-transfer/tests/smoke.rs
run_script
expect_ok
expect_cargo "-p intent-svc-transfer --test smoke"

case_name="intent-app src change selects only intent-app"
reset_repo
edit crates/intent-app/src/lib.rs
run_script
expect_ok
expect_cargo "-p intent-app --lib --bins --tests"

# intent-core is the leaf every crate builds on: a src change there selects
# every workspace crate depending on it, directly or transitively, through
# any dependency table (intentd#2698: an intent-core harness-version change
# broke an intent-acp test that only the merge queue ran). The fixture covers
# a plain path dependency (alpha), a renamed dev-dependency (gamma), a
# target-specific workspace-inherited one (intent-services), table forms
# (intentd, delta) and transitive edges (delta via alpha; intentd, intent-app
# and intent-svc-transfer via intent-services); beta only names intent-core
# in a comment.
core_rdeps_lib="-p alpha -p delta -p gamma -p intent-app -p intent-core -p intent-services -p intent-svc-transfer --lib --bins --tests"
core_rdeps_note="[test-changed] note: crates/intent-core changed; its reverse dependencies run too (alpha, delta, gamma, intent-app, intent-services, intent-svc-transfer, intentd)"

case_name="intent-core src change selects every reverse dependency"
reset_repo
edit crates/intent-core/src/lib.rs
commit_all
run_script
expect_ok
expect_plan "$core_rdeps_lib" "-p intentd --bins --tests"
expect_cargo "$core_rdeps_lib" "-p intentd --bins --tests"
[[ "$stdout" == *"$core_rdeps_note"* ]] || fail "$case_name: no reverse-dependency note: $stdout"
[[ "$(grep -c 'note:' <<<"$stdout")" -eq 1 ]] || fail "$case_name: expected one note: $stdout"

case_name="intent-core non-src crate file also selects every reverse dependency"
reset_repo
edit crates/intent-core/resources/data.txt
run_script
expect_ok
expect_cargo "-p alpha -p delta -p gamma -p intent-app -p intent-services -p intent-svc-transfer --lib --bins --tests" "-p intent-core" "-p intentd --bins --tests"
[[ "$stdout" == *"$core_rdeps_note"* ]] || fail "$case_name: no reverse-dependency note: $stdout"

case_name="intent-core test-only change selects only intent-core"
reset_repo
edit crates/intent-core/tests/core.rs
run_script
expect_ok
expect_cargo "-p intent-core --test core"

case_name="intent-core reverse dependencies subsume narrower selections"
reset_repo
edit crates/intent-core/src/lib.rs
edit crates/alpha/tests/one.rs
edit crates/delta/src/script.rs
run_script
expect_ok
expect_cargo "$core_rdeps_lib" "-p intentd --bins --tests"

case_name="intent-core dry run prints the reverse-dependency plan"
reset_repo
edit crates/intent-core/src/lib.rs
DRY_RUN=1 run_script
expect_ok
expect_plan "$core_rdeps_lib" "-p intentd --bins --tests"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"

case_name="instrumented intent-core src change selects every reverse dependency"
reset_repo
edit crates/intent-core/src/lib.rs
run_script --instrumented
expect_ok
expect_cov "$core_rdeps_lib" "-p intentd --bins --tests"
[[ "$stdout" == *"[coverage-changed] note: crates/intent-core changed; its reverse dependencies run too"* ]] || fail "$case_name: no reverse-dependency note: $stdout"

# A rename that inherits from [workspace.dependencies] names no path: the
# package key identifies the crate (inline and table forms).
case_name="intent-core renamed workspace dependency without a path is followed"
reset_repo
write crates/gamma/Cargo.toml $'[package]\nname = "gamma"\n\n[dev-dependencies]\ncore = { package = "intent-core", workspace = true }'
write crates/beta/Cargo.toml $'[package]\nname = "beta"\n\n[dependencies.core]\npackage = "intent-core"\nworkspace = true'
commit_all "renamed manifests"
g update-ref refs/remotes/origin/main HEAD
edit crates/intent-core/src/lib.rs
run_script
g update-ref refs/remotes/origin/main "$base_sha"
expect_ok
expect_cargo "$core_rdeps_lib" "-p beta -p intentd --bins --tests"
[[ "$stdout" == *"its reverse dependencies run too (alpha, beta, delta, gamma, "* ]] || fail "$case_name: beta/gamma not in note: $stdout"

case_name="crates selected through intent-core print no downstream-not-selected note"
reset_repo
edit crates/intent-core/src/lib.rs
edit crates/alpha/src/lib.rs
edit crates/intent-services/src/lib.rs
run_script
expect_ok
expect_cargo "$core_rdeps_lib" "-p intentd --bins --tests"
[[ "$stdout" != *"not selected"* ]] || fail "$case_name: misleading note: $stdout"
[[ "$(grep -c 'note:' <<<"$stdout")" -eq 1 ]] || fail "$case_name: expected one note: $stdout"

# --print-edges / --print-rdeps expose the scanner's view for
# scripts/check-rdeps-parity.py, which compares it with cargo metadata in CI.
fixture_edges=$'alpha\tintent-core\ndelta\talpha\ngamma\tintent-core\nintent-app\tintent-services\nintent-services\tintent-core\nintent-svc-transfer\tintent-services\nintentd\tintent-services'
fixture_rdeps=$'intent-core\talpha delta gamma intent-app intent-services intent-svc-transfer intentd'

case_name="--print-edges prints every workspace dependency, sorted"
reset_repo
run_script --print-edges
expect_ok
[[ "$stdout" == "$fixture_edges" ]] || fail "$case_name: edges were"$'\n'"$stdout"$'\n'"expected"$'\n'"$fixture_edges"
[[ -z "$cargo_log" ]] || fail "$case_name: cargo ran: $cargo_log"

# Byte order whatever the locale (see ignores_punctuation).
if [[ -n "$en_us" ]]; then
  case_name="--print-edges / --print-rdeps sort in byte order under en_US.UTF-8"
  reset_repo
  RUN_LOCALE=en_US.UTF-8 run_script --print-edges
  expect_ok
  [[ "$stdout" == "$fixture_edges" ]] || fail "$case_name: edges were"$'\n'"$stdout"
  RUN_LOCALE=en_US.UTF-8 run_script --print-rdeps
  expect_ok
  [[ "$stdout" == "$fixture_rdeps" ]] || fail "$case_name: rdeps were '$stdout'"
fi

# The selection itself sorts in byte order whatever the locale: the crate
# list and its reverse-dependency note, a crate's --test targets and a suite's
# changed modules are the same under en_US.UTF-8 as under C.
# expect_same_under_en_us runs the script on the current tree under C, then
# under en_US.UTF-8, and requires byte-identical stdout, stderr and cargo argv;
# the en_US.UTF-8 results (equal to C's) stay in stdout / cargo_log after it.
expect_same_under_en_us() {
  local c_status c_stdout c_stderr c_cargo
  clear_logs
  LC_ALL=C run_script "$@"
  c_status=$status c_stdout=$stdout c_stderr=$stderr c_cargo=$cargo_log
  clear_logs
  RUN_LOCALE=en_US.UTF-8 run_script "$@"
  [[ "$status" -eq "$c_status" ]] || fail "$case_name: exited $status under en_US.UTF-8, $c_status under C: $stderr"
  [[ "$stdout" == "$c_stdout" ]] ||
    fail "$case_name: stdout under en_US.UTF-8 was"$'\n'"$stdout"$'\n'"under C"$'\n'"$c_stdout"
  [[ "$stderr" == "$c_stderr" ]] ||
    fail "$case_name: stderr under en_US.UTF-8 was"$'\n'"$stderr"$'\n'"under C"$'\n'"$c_stderr"
  [[ "$cargo_log" == "$c_cargo" ]] ||
    fail "$case_name: cargo argv under en_US.UTF-8 was"$'\n'"$cargo_log"$'\n'"under C"$'\n'"$c_cargo"
}

if [[ -n "$en_us" ]]; then
  case_name="intent-core reverse dependencies are listed in byte order under en_US.UTF-8"
  reset_repo
  edit crates/intent-core/src/lib.rs
  commit_all
  expect_same_under_en_us
  expect_ok
  expect_cargo "$core_rdeps_lib" "-p intentd --bins --tests"
  [[ "$stdout" == *"$core_rdeps_note"* ]] || fail "$case_name: no reverse-dependency note: $stdout"

  # '_' sorts before letters in byte order; en_US.UTF-8 ignores it, which
  # would put ab before a_z.
  case_name="--test targets and suite modules are listed in byte order under en_US.UTF-8"
  reset_repo
  write crates/alpha/tests/a_z.rs
  write crates/alpha/tests/ab.rs
  write crates/intentd/tests/suite_e2e.rs $'//! fixture suite\nmod common;\n\nmod auggie_context_e2e;\nmod e2e_core;\nmod e2e_shared;\nmod m_z;\nmod mb;'
  write crates/intentd/tests/m_z.rs
  write crates/intentd/tests/mb.rs
  commit_all "underscore names"
  edit crates/alpha/tests/ab.rs
  edit crates/alpha/tests/a_z.rs
  edit crates/intentd/tests/mb.rs
  edit crates/intentd/tests/m_z.rs
  expect_same_under_en_us --base HEAD
  expect_ok
  expect_cargo "-p alpha --test a_z --test ab" "-p intentd --test suite_e2e -E (binary_id(=intentd::suite_e2e)&test(/^(m_z|mb)::/))"
fi

case_name="--print-rdeps prints intent-core's transitive dependents"
reset_repo
run_script --print-rdeps
expect_ok
[[ "$stdout" == "$fixture_rdeps" ]] || fail "$case_name: rdeps were '$stdout', expected '$fixture_rdeps'"

case_name="--print-edges follows a renamed workspace dependency without a path"
reset_repo
write crates/gamma/Cargo.toml $'[package]\nname = "gamma"\n\n[dev-dependencies]\ncore = { package = "intent-core", workspace = true }'
write crates/beta/Cargo.toml $'[package]\nname = "beta"\n\n[dependencies.core]\npackage = "intent-core"\nworkspace = true'
run_script --print-edges
expect_ok
[[ "$stdout" == $'alpha\tintent-core\nbeta\tintent-core\n'* ]] || fail "$case_name: beta edge missing: $stdout"
[[ "$stdout" == *$'\ngamma\tintent-core\n'* ]] || fail "$case_name: gamma edge missing: $stdout"
run_script --print-rdeps
expect_ok
[[ "$stdout" == $'intent-core\talpha beta delta gamma intent-app intent-services intent-svc-transfer intentd' ]] ||
  fail "$case_name: rdeps were '$stdout'"

case_name="print modes need no git checkout, BASE or valid end-to-end map"
reset_repo
nogit="$temp_dir/nogit"
rm -rf "$nogit"
mkdir -p "$nogit/scripts"
cp -R "$repo/crates" "$nogit/"
cp "$script" "$nogit/scripts/"
mkdir -p "$nogit/.config"
printf 'not a valid map line\n' >"$nogit/.config/e2e-test-map.txt"
set +e
stdout=$(cd "$nogit" && PATH="$bin_dir" BASE=no-such-ref "$script_bash" "$nogit/scripts/changed-tests.sh" --print-rdeps 2>"$temp_dir/stderr")
status=$?
set -e
stderr=$(<"$temp_dir/stderr")
expect_ok
[[ "$stdout" == "$fixture_rdeps" ]] || fail "$case_name: rdeps were '$stdout'"
rm -rf "$nogit"

case_name="a print mode combined with another flag is a usage error"
reset_repo
for args in "--print-edges --print-rdeps" "--print-rdeps --dry-run" "--instrumented --print-edges" "--print-edges --base origin/main"; do
  # shellcheck disable=SC2086 # split the flag list on purpose
  run_script $args
  [[ "$status" -eq 2 && "$stderr" == *"Usage:"* ]] || fail "$case_name: '$args' exited $status: $stderr"
done

case_name="a crate other than intent-core does not propagate to its dependents"
reset_repo
edit crates/alpha/src/lib.rs
run_script
expect_ok
expect_cargo "-p alpha --lib --bins --tests"

case_name="src change in a crate without a lib drops --lib"
reset_repo
edit crates/beta/src/main.rs
run_script
expect_ok
expect_cargo "-p beta --bins --tests"

case_name="other crate file selects every test target and subsumes the rest"
reset_repo
edit crates/beta/migrations/001.sql
edit crates/beta/src/main.rs
edit crates/beta/tests/smoke.rs
run_script
expect_ok
expect_plan "-p beta"
expect_cargo "-p beta"
[[ "$stdout" != *"note:"* ]] || fail "$case_name printed the src note for a subsumed selection: $stdout"

case_name="benches, examples and inert non-crate paths are ignored"
reset_repo
edit crates/alpha/benches/bench.rs
edit crates/alpha/examples/demo.rs
edit README.md
edit docs/guide.md
write .github/workflows/x.yml
write LICENSE
write NOTICE
write .gitignore
write deny.toml
write release-plz.toml
write dist-workspace.toml
run_script
expect_ok
[[ "$stdout" == *"nothing to test"* ]] || fail "$case_name printed '$stdout'"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"

case_name="crates with different selections run separately, identical ones share"
reset_repo
edit crates/alpha/tests/common/mod.rs
write crates/gamma/tests/fixtures/x.json
edit crates/beta/src/main.rs
run_script
expect_ok
expect_plan "-p alpha -p gamma --tests" "-p beta --bins --tests"
expect_cargo "-p alpha -p gamma --tests" "-p beta --bins --tests"

case_name="same-named test files across crates share one invocation"
reset_repo
edit crates/beta/tests/smoke.rs
edit crates/gamma/tests/smoke.rs
run_script
expect_ok
expect_cargo "-p beta -p gamma --test smoke"

case_name="different test files across crates stay separate"
reset_repo
edit crates/alpha/tests/one.rs
edit crates/gamma/tests/smoke.rs
run_script
expect_ok
expect_cargo "-p alpha --test one" "-p gamma --test smoke"

case_name="cargo drains finite caller input without consuming later plans"
reset_repo
edit crates/alpha/tests/one.rs
edit crates/gamma/tests/smoke.rs
printf 'first input line\nsecond input line\n' >"$temp_dir/input"
RUN_STDIN="$temp_dir/input" run_script
expect_ok
expect_cargo "-p alpha --test one" "-p gamma --test smoke"
[[ "$(<"$temp_dir/cargo-stdin.log")" == "$(<"$temp_dir/input")" ]] || fail "$case_name: cargo did not receive exactly the caller input"

: >"$temp_dir/cargo.log"
: >"$temp_dir/cargo-stdin.log"
RUN_STDIN="$temp_dir/input" run_script --instrumented
expect_ok
expect_cov "-p alpha --test one" "-p gamma --test smoke"
[[ "$(<"$temp_dir/cargo-stdin.log")" == "$(<"$temp_dir/input")" ]] || fail "$case_name (instrumented): cargo did not receive exactly the caller input"

case_name="build-jobs and test-threads are appended"
reset_repo
edit crates/alpha/tests/one.rs
BUILD_JOBS=-2 TEST_THREADS=4 run_script
expect_ok
expect_plan "-p alpha --test one --build-jobs -2 --test-threads 4"
expect_cargo "-p alpha --test one --build-jobs -2 --test-threads 4"
reset_repo
edit crates/alpha/tests/one.rs
run_script --build-jobs 3 --test-threads=num-cpus
expect_ok
expect_cargo "-p alpha --test one --build-jobs 3 --test-threads num-cpus"

case_name="dry run prints the plan without invoking cargo"
reset_repo
edit crates/alpha/tests/one.rs
edit crates/beta/src/main.rs
DRY_RUN=1 run_script
expect_ok
expect_plan "-p alpha --test one" "-p beta --bins --tests"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"
run_script --dry-run
expect_ok
[[ -z "$cargo_log" ]] || fail "$case_name (--dry-run) invoked cargo: $cargo_log"
DRY_RUN=0 run_script
expect_ok
expect_cargo "-p alpha --test one" "-p beta --bins --tests"

case_name="cargo failure stops the run and propagates its exit code"
reset_repo
edit crates/alpha/tests/one.rs
edit crates/beta/src/main.rs
CARGO_STUB_EXIT=100 run_script
[[ "$status" -eq 100 ]] || fail "$case_name exited $status (expected 100): $stderr"
expect_cargo "-p alpha --test one"
[[ "$stderr" == "[test-changed] cargo nextest run -p alpha --test one exited 100" ]] || fail "$case_name stderr: $stderr"

# With a runner the script plans as usual, then hands every plan to one
# runner invocation instead of calling cargo itself.
expect_runner() {
  local expected="call:"$'\n' word
  for word in "$@"; do
    expected+="$word"$'\n'
  done
  [[ "$runner_log" == "${expected%$'\n'}" ]] || fail "$case_name: runner argv was"$'\n'"$runner_log"$'\n'"expected"$'\n'"${expected%$'\n'}"
  [[ -z "$cargo_log" ]] || fail "$case_name invoked cargo alongside the runner: $cargo_log"
}

# Canonical execution overrides any inherited disabling value at the child
# boundary, including the custom runner that owns resumed test execution.
for inherited_policy in '' 0 false 1; do
  case_name="child policy overrides inherited '$inherited_policy'"
  reset_repo
  edit crates/alpha/tests/one.rs
  INTENTD_ASSERT_BOUND_CALLER="$inherited_policy" run_script
  expect_ok
  expect_cargo "-p alpha --test one"
  reset_repo
  edit crates/alpha/tests/one.rs
  INTENTD_ASSERT_BOUND_CALLER="$inherited_policy" run_script --instrumented
  expect_ok
  expect_cov "-p alpha --test one"
  reset_repo
  edit crates/alpha/tests/one.rs
  INTENTD_ASSERT_BOUND_CALLER="$inherited_policy" NEXTEST_RUNNER="$bin_dir/runner" run_script
  expect_ok
  expect_runner --plan "-p alpha --test one" --base origin/main --label test-changed
done

# The byte-order pin is the script's own: cargo and the runner run the tests
# under the caller's LC_ALL, or none when the caller set none, as make test
# and the merge queue do.
case_name="cargo and the runner get the caller's LC_ALL"
reset_repo
edit crates/alpha/tests/one.rs
LC_ALL=POSIX run_script
expect_ok
expect_cargo "-p alpha --test one"
[[ "$cargo_locale_log" == POSIX ]] || fail "$case_name: cargo got LC_ALL '$cargo_locale_log'"
clear_logs
LC_ALL=POSIX run_script --instrumented
expect_ok
[[ "$cargo_locale_log" == POSIX ]] || fail "$case_name: instrumented cargo got LC_ALL '$cargo_locale_log'"
clear_logs
LC_ALL=POSIX NEXTEST_RUNNER="$bin_dir/runner" run_script
expect_ok
expect_runner --plan "-p alpha --test one" --base origin/main --label test-changed
[[ "$runner_locale_log" == POSIX ]] || fail "$case_name: runner got LC_ALL '$runner_locale_log'"

# A subshell, so this suite's own LC_ALL is untouched afterwards.
case_name="cargo and the runner get no LC_ALL when the caller set none"
(
  unset LC_ALL
  reset_repo
  edit crates/alpha/tests/one.rs
  run_script
  expect_ok
  expect_cargo "-p alpha --test one"
  [[ "$cargo_locale_log" == "<unset>" ]] || fail "$case_name: cargo got LC_ALL '$cargo_locale_log'"
  clear_logs
  NEXTEST_RUNNER="$bin_dir/runner" run_script
  expect_ok
  [[ "$runner_locale_log" == "<unset>" ]] || fail "$case_name: runner got LC_ALL '$runner_locale_log'"
)

case_name="runner receives every plan in one invocation"
reset_repo
edit crates/alpha/tests/one.rs
write crates/gamma/tests/fresh.rs
NEXTEST_RUNNER="$bin_dir/runner" BUILD_JOBS=-2 TEST_THREADS=4 run_script
expect_ok
expect_plan "-p alpha --test one --build-jobs -2 --test-threads 4" "-p gamma --test fresh --build-jobs -2 --test-threads 4"
expect_runner --plan "-p alpha --test one" --plan "-p gamma --test fresh" \
  --base origin/main --label test-changed --build-jobs -2 --test-threads 4
[[ "$(grep -c '^call:$' <<<"$runner_log")" -eq 1 ]] || fail "$case_name: runner called more than once: $runner_log"

# The runner runs from the caller's working directory, not the repo root.
case_name="runner is exec'd from the caller's cwd"
reset_repo
edit crates/alpha/tests/one.rs
RUN_CWD="$temp_dir" NEXTEST_RUNNER="$bin_dir/runner" run_script
expect_ok
expect_runner --plan "-p alpha --test one" --base origin/main --label test-changed
[[ "$(<"$temp_dir/runner-cwd.log")" == "$temp_dir" ]] || fail "$case_name: runner cwd was $(<"$temp_dir/runner-cwd.log")"

case_name="runner command line is shell-quoted and gets the explicit BASE"
reset_repo
edit crates/alpha/tests/one.rs
commit_all
g branch -q -f other HEAD
edit crates/gamma/tests/smoke.rs
run_script --base other --runner "'$bin_dir/runner' --cache-dir '/tmp/gate runs'"
expect_ok
expect_runner --cache-dir "/tmp/gate runs" --plan "-p gamma --test smoke" --base other --label test-changed

case_name="runner failure propagates its exit code"
reset_repo
edit crates/alpha/tests/one.rs
NEXTEST_RUNNER="$bin_dir/runner" RUNNER_STUB_EXIT=7 run_script
[[ "$status" -eq 7 ]] || fail "$case_name exited $status (expected 7): $stderr"
[[ -z "$stderr" ]] || fail "$case_name: unexpected stderr: $stderr"
expect_runner --plan "-p alpha --test one" --base origin/main --label test-changed

case_name="runner is not invoked for a dry run, an empty plan or a fallback"
reset_repo
edit crates/alpha/tests/one.rs
NEXTEST_RUNNER="$bin_dir/runner" DRY_RUN=1 run_script
expect_ok
expect_plan "-p alpha --test one"
[[ -z "$runner_log$cargo_log" ]] || fail "$case_name (dry run) invoked: $runner_log$cargo_log"
reset_repo
NEXTEST_RUNNER="$bin_dir/runner" run_script
expect_ok
[[ "$stdout" == *"nothing to test"* ]] || fail "$case_name (empty) printed '$stdout'"
[[ -z "$runner_log$cargo_log" ]] || fail "$case_name (empty) invoked: $runner_log$cargo_log"
reset_repo
edit Cargo.lock
NEXTEST_RUNNER="$bin_dir/runner" run_script
[[ "$status" -eq 3 ]] || fail "$case_name (fallback) exited $status (expected 3): $stderr"
[[ -z "$runner_log$cargo_log" ]] || fail "$case_name (fallback) invoked: $runner_log$cargo_log"

# The monorepo Makefile names its paths as "$VAR" references inside the runner
# string (expanded by the script's eval), so apostrophes and spaces reach the
# runner as single words.
case_name="runner string expands quoted env references to whole words"
reset_repo
edit crates/alpha/tests/one.rs
export GATE_REPO_ROOT="/tmp/reviewer's repo" GATE_CACHE_DIR="/tmp/reviewer's gate runs"
NEXTEST_RUNNER='"$RUNNER_BIN" --repo-root "$GATE_REPO_ROOT" --cache-dir "$GATE_CACHE_DIR" --resume "$RESUME"' \
  RUNNER_BIN="$bin_dir/runner" RESUME=1 run_script
unset GATE_REPO_ROOT GATE_CACHE_DIR
expect_ok
expect_runner --repo-root "/tmp/reviewer's repo" --cache-dir "/tmp/reviewer's gate runs" --resume 1 \
  --plan "-p alpha --test one" --base origin/main --label test-changed

# Build-wide files make the subset unreliable: exit 3 names them and defers to
# the full `make test`.
for path in Cargo.toml Cargo.lock rust-toolchain.toml .config/nextest.toml .cargo/config.toml \
  crates/alpha/Cargo.toml crates/beta/build.rs; do
  case_name="build-wide change $path"
  reset_repo
  edit "$path"
  edit crates/alpha/tests/one.rs
  run_script
  [[ "$status" -eq 3 ]] || fail "$case_name exited $status (expected 3): $stderr"
  [[ -z "$stdout" ]] || fail "$case_name printed '$stdout'"
  [[ "$stderr" == *"need the full suite -- run 'make test':"*"  $path"* ]] || fail "$case_name stderr: $stderr"
  [[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"
done
# Tests read repo files outside crates/ through CARGO_MANIFEST_DIR
# (scripts/install.sh, repo-wide lints), so any non-inert path out there also
# defers to the full suite, even alongside a precisely mapped test change.
for path in scripts/install.sh scripts/tool.sh packaging/deb/control unknown.toml; do
  case_name="non-crate change $path"
  reset_repo
  write "$path" "// changed"
  edit crates/alpha/tests/one.rs
  run_script
  [[ "$status" -eq 3 ]] || fail "$case_name exited $status (expected 3): $stderr"
  [[ -z "$stdout" ]] || fail "$case_name printed '$stdout'"
  [[ "$stderr" == *"need the full suite -- run 'make test':"*"  $path"* ]] || fail "$case_name stderr: $stderr"
  [[ "$stderr" != *"crates/alpha/tests/one.rs"* ]] || fail "$case_name listed a mapped path: $stderr"
  [[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"
done
case_name="untracked build-wide file with a non-ASCII name"
reset_repo
write .cargo/café.toml
run_script
[[ "$status" -eq 3 ]] || fail "$case_name exited $status (expected 3): $stderr"
[[ "$stderr" == *"  .cargo/café.toml"* ]] || fail "$case_name stderr: $stderr"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"
case_name="build-wide change in a dry run"
reset_repo
write .cargo/audit.toml
DRY_RUN=1 run_script
[[ "$status" -eq 3 ]] || fail "$case_name exited $status (expected 3): $stderr"
[[ "$stderr" == *"  .cargo/audit.toml"* ]] || fail "$case_name stderr: $stderr"
for rename in "Cargo.lock Cargo.lock.backup" "crates/beta/build.rs crates/beta/retired.rs"; do
  case_name="renamed build-wide file ($rename) still needs the full suite"
  reset_repo
  # shellcheck disable=SC2086
  g mv $rename
  run_script
  [[ "$status" -eq 3 ]] || fail "$case_name exited $status (expected 3): $stderr"
  [[ -z "$stdout" ]] || fail "$case_name printed '$stdout'"
  [[ "$stderr" == *"  ${rename%% *}"* ]] || fail "$case_name stderr: $stderr"
  [[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"
done

# A failing git command is an error, never an empty change set.
for subcommand in diff ls-files; do
  case_name="git $subcommand failure"
  reset_repo
  edit crates/alpha/tests/one.rs
  GIT_STUB_FAIL=$subcommand run_script
  [[ "$status" -eq 2 ]] || fail "$case_name exited $status (expected 2): $stderr"
  [[ -z "$stdout" ]] || fail "$case_name printed '$stdout'"
  [[ "$stderr" == "fatal: stubbed git $subcommand failure"$'\n'"[test-changed] git $subcommand "*" failed (exit 128)" ]] || fail "$case_name stderr: $stderr"
  [[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"
done

case_name="unresolvable BASE"
reset_repo
edit crates/alpha/tests/one.rs
BASE=origin/nope run_script
[[ "$status" -eq 4 ]] || fail "$case_name exited $status (expected 4): $stderr"
[[ -z "$stdout" ]] || fail "$case_name printed '$stdout'"
[[ "$stderr" == "[test-changed] cannot resolve BASE 'origin/nope'; run 'git fetch origin main' or set BASE=<ref>" ]] || fail "$case_name stderr: $stderr"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"
run_script --base origin/nope
[[ "$status" -eq 4 ]] || fail "$case_name (--base) exited $status (expected 4): $stderr"

# intent-hq/intent#5414: the BASE object exists (fetched at depth 1) but shares
# no history with HEAD, so merge-base fails; distinct from a usage error.
case_name="BASE disconnected from HEAD"
reset_repo
edit crates/alpha/tests/one.rs
orphan=$(g commit-tree -m orphan "$(g write-tree)")
run_script --base "$orphan"
[[ "$status" -eq 4 ]] || fail "$case_name exited $status (expected 4): $stderr"
[[ -z "$stdout" ]] || fail "$case_name printed '$stdout'"
[[ "$stderr" == "[test-changed] cannot resolve BASE '$orphan'; run 'git fetch origin main' or set BASE=<ref>" ]] || fail "$case_name stderr: $stderr"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"

case_name="explicit BASE"
reset_repo
edit crates/alpha/tests/one.rs
commit_all
g branch -q -f other HEAD
edit crates/gamma/tests/smoke.rs
commit_all
BASE=other run_script
expect_ok
expect_cargo "-p gamma --test smoke"
run_script --base=other --dry-run
expect_ok
expect_plan "-p gamma --test smoke"

case_name="usage errors"
reset_repo
edit crates/alpha/tests/one.rs
for args in --bogus "--base" "--build-jobs" "--runner" "extra" "--instrumented --runner $bin_dir/runner"; do
  # shellcheck disable=SC2086
  run_script $args
  [[ "$status" -eq 2 ]] || fail "$case_name '$args' exited $status (expected 2): $stderr"
  [[ "$stderr" == "Usage: "* ]] || fail "$case_name '$args' stderr: $stderr"
done
[[ -z "$cargo_log$runner_log" ]] || fail "$case_name invoked cargo or the runner: $cargo_log$runner_log"

# --instrumented: the same selection runs under llvm-cov with coverage-e2e's
# env and auggie_context_e2e exclusion, labelled coverage-changed.
case_name="instrumented run wraps every plan in cargo llvm-cov"
reset_repo
edit crates/alpha/tests/one.rs
edit crates/beta/src/main.rs
run_script --instrumented
expect_ok
expect_cov_plan "-p alpha --test one" "-p beta --bins --tests"
expect_cov "-p alpha --test one" "-p beta --bins --tests"
[[ "$stdout" != *"[test-changed]"* ]] || fail "$case_name used the test-changed label: $stdout"

case_name="instrumented run appends build-jobs and test-threads before the filterset"
reset_repo
edit crates/alpha/tests/one.rs
BUILD_JOBS=-2 TEST_THREADS=4 run_script --instrumented
expect_ok
expect_cov "-p alpha --test one --build-jobs -2 --test-threads 4"

case_name="instrumented dry run prints the llvm-cov plan without invoking cargo"
reset_repo
edit crates/alpha/tests/one.rs
run_script --instrumented --dry-run
expect_ok
expect_cov_plan "-p alpha --test one"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"

case_name="instrumented clean tree"
reset_repo
run_script --instrumented
expect_ok
[[ "$stdout" == "[coverage-changed] nothing to test (no Rust changes vs origin/main)" ]] || fail "$case_name printed '$stdout'"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"

case_name="instrumented cargo failure propagates its exit code"
reset_repo
edit crates/alpha/tests/one.rs
CARGO_STUB_EXIT=100 run_script --instrumented
[[ "$status" -eq 100 ]] || fail "$case_name exited $status (expected 100): $stderr"
[[ "$stderr" == "[coverage-changed] INTENTD_TEST_TIMEOUT_MULTIPLIER=3 cargo llvm-cov --no-report nextest -p alpha --test one -E 'not test(/^auggie_context_e2e::/)' exited 100" ]] || fail "$case_name stderr: $stderr"

case_name="instrumented storage observer preserves scopes, stdin, policy and failure"
reset_repo
edit crates/alpha/tests/one.rs
edit crates/gamma/tests/smoke.rs
storage_dir="$temp_dir/storage"
mkdir -p "$storage_dir"
python3 - "$storage_dir" "$repo" <<'PYTHON'
import json, pathlib, sys
out, root = map(pathlib.Path, sys.argv[1:])
(out / 'paths.json').write_text(json.dumps({name: str(root) for name in
    ['workspace', 'target', 'profiles', 'temporary', 'home']}))
PYTHON
printf 'payload\n' > "$temp_dir/storage-input"
LC_ALL=POSIX COVERAGE_STORAGE_DIR="$storage_dir" RUN_STDIN="$temp_dir/storage-input" run_script --instrumented
expect_ok
expect_cov "-p alpha --test one" "-p gamma --test smoke"
[[ "$cargo_locale_log" == $'POSIX\nPOSIX' ]] || fail "$case_name: cargo got LC_ALL '$cargo_locale_log'"
[[ "$(cat "$temp_dir/cargo-stdin.log")" == payload ]] || fail "$case_name: caller input was changed"
python3 - "$storage_dir/resources.jsonl" <<'PYTHON'
import json, sys
rows = [json.loads(line) for line in open(sys.argv[1])]
assert [(r['stage'], r['event']) for r in rows] == [
    ('scope-1', 'start'), ('scope-1', 'exit'), ('scope-2', 'start'), ('scope-2', 'exit')], rows
assert all(r['exit_code'] == 0 for r in rows if r['event'] == 'exit')
PYTHON
: > "$storage_dir/resources.jsonl"
COVERAGE_STORAGE_DIR="$storage_dir" CARGO_STUB_EXIT=23 run_script --instrumented
[[ "$status" -eq 23 ]] || fail "$case_name: observer changed failure to $status"
python3 - "$storage_dir/resources.jsonl" <<'PYTHON'
import json, sys
rows = [json.loads(line) for line in open(sys.argv[1])]
assert len(rows) == 2 and rows[-1]['exit_code'] == 23 and rows[-1]['stage'] == 'scope-1', rows
PYTHON

# A build-wide change never exits 3 under --instrumented: the merge queue's
# coverage jobs already run the full instrumented suite, so the script prints
# a notice and still runs the mapped crates/ selection.
case_name="instrumented build-wide change runs the mapped selection with a notice"
reset_repo
edit Cargo.lock
edit .cargo/config.toml
edit crates/alpha/tests/one.rs
run_script --instrumented
expect_ok
[[ "$stdout" == *"[coverage-changed] build-wide change(s) vs origin/main; the full instrumented suite runs in the merge queue (coverage-e2e / coverage-all), running the mapped crates/ selection only:"$'\n'"  .cargo/config.toml"$'\n'"  Cargo.lock"* ]] || fail "$case_name stdout: $stdout"
expect_cov_plan "-p alpha --test one"
expect_cov "-p alpha --test one"

case_name="instrumented build-wide file under a crate selects every test target of that crate"
reset_repo
edit crates/beta/build.rs
edit crates/alpha/Cargo.toml
edit crates/alpha/tests/one.rs
run_script --instrumented
expect_ok
[[ "$stdout" == *"  crates/alpha/Cargo.toml"*"  crates/beta/build.rs"* ]] || fail "$case_name stdout: $stdout"
expect_cov "-p alpha -p beta"

case_name="instrumented non-crate change runs the mapped selection with a notice"
reset_repo
write scripts/tool.sh "// changed"
edit crates/alpha/tests/one.rs
run_script --instrumented
expect_ok
[[ "$stdout" == *"  scripts/tool.sh"* ]] || fail "$case_name stdout: $stdout"
expect_cov "-p alpha --test one"

case_name="instrumented build-wide change with an empty selection exits 0"
reset_repo
edit Cargo.lock
run_script --instrumented
expect_ok
[[ "$stdout" == *"  Cargo.lock"*"nothing to test"* ]] || fail "$case_name stdout: $stdout"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"

# The instrumented filterset excludes auggie_context_e2e, so a plan naming
# only that module would select nothing; the plain run still names it.
case_name="instrumented run skips auggie_context_e2e, plain run keeps it"
reset_repo
edit crates/intentd/tests/auggie_context_e2e.rs
run_script --instrumented
expect_ok
[[ "$stdout" == *"nothing to test"* ]] || fail "$case_name (instrumented) printed '$stdout'"
[[ -z "$cargo_log" ]] || fail "$case_name (instrumented) invoked cargo: $cargo_log"
edit crates/intentd/tests/e2e_wss_agent_lifecycle.rs
run_script --instrumented
expect_ok
expect_cov "-p intentd --test e2e_wss_agent_lifecycle"
: >"$temp_dir/cargo.log"
run_script
expect_ok
expect_cargo "-p intentd --test e2e_wss_agent_lifecycle --test suite_e2e -E binary_id(=intentd::e2e_wss_agent_lifecycle)|(binary_id(=intentd::suite_e2e)&test(/^(auggie_context_e2e)::/))"

# Suite targets: a file module (or its directory) selects its suite narrowed
# to that module's tests; nextest ORs repeated -E flags, so each plan carries
# exactly one filterset, with standalone targets ORed in by binary_id.
case_name="suite module file selects its suite filtered to the module"
reset_repo
edit crates/intentd/tests/e2e_core.rs
run_script
expect_ok
expect_cargo "-p intentd --test suite_e2e -E (binary_id(=intentd::suite_e2e)&test(/^(e2e_core)::/))"
[[ "$stdout" == *"[test-changed] cargo nextest run -p intentd --test suite_e2e -E '(binary_id(=intentd::suite_e2e)&test(/^(e2e_core)::/))'"* ]] ||
  fail "$case_name plan line is not a pasteable command: $stdout"

case_name="suite module directory selects its module"
reset_repo
edit crates/intentd/tests/e2e_core/inner.rs
run_script
expect_ok
expect_cargo "-p intentd --test suite_e2e -E (binary_id(=intentd::suite_e2e)&test(/^(e2e_core)::/))"

case_name="module directory another target includes selects every test"
reset_repo
edit crates/intentd/tests/e2e_shared/helper.rs
run_script
expect_ok
expect_cargo "-p intentd --tests"

case_name="modules of two suites and a standalone target share one filterset"
reset_repo
edit crates/intentd/tests/e2e_wss_x.rs
edit crates/intentd/tests/e2e_shared.rs
edit crates/intentd/tests/e2e_core.rs
edit crates/intentd/tests/e2e_wss_agent_lifecycle.rs
BUILD_JOBS=3 run_script
expect_ok
expect_cargo "-p intentd --test e2e_wss_agent_lifecycle --test suite_e2e --test suite_wss --build-jobs 3 -E binary_id(=intentd::e2e_wss_agent_lifecycle)|(binary_id(=intentd::suite_e2e)&test(/^(e2e_core|e2e_shared)::/))|(binary_id(=intentd::suite_wss)&test(/^(e2e_wss_x)::/))"

case_name="editing a suite root runs the whole suite"
reset_repo
edit crates/intentd/tests/suite_wss.rs
run_script
expect_ok
expect_cargo "-p intentd --test suite_wss"

case_name="deleted suite module selects every test"
reset_repo
rm "$repo/crates/intentd/tests/e2e_wss_x.rs"
run_script
expect_ok
expect_cargo "-p intentd --tests"

case_name="instrumented suite module ANDs the exclusion into its filterset"
reset_repo
edit crates/intentd/tests/e2e_core.rs
TEST_THREADS=2 run_script --instrumented
expect_ok
cov_expected="$repo: INTENTD_TEST_TIMEOUT_MULTIPLIER=3 llvm-cov --no-report nextest -p intentd --test suite_e2e --test-threads 2 -E ((binary_id(=intentd::suite_e2e)&test(/^(e2e_core)::/))) and (not test(/^auggie_context_e2e::/))"
[[ "$cargo_log" == "$cov_expected" ]] || fail "$case_name: cargo argv was"$'\n'"$cargo_log"$'\n'"expected"$'\n'"$cov_expected"
[[ "$stdout" == *"nextest -p intentd --test suite_e2e --test-threads 2 -E '((binary_id(=intentd::suite_e2e)&test(/^(e2e_core)::/))) and (not test(/^auggie_context_e2e::/))'"* ]] ||
  fail "$case_name plan: $stdout"

case_name="runner receives a suite plan's filterset as one plan word"
reset_repo
edit crates/intentd/tests/e2e_wss_x.rs
NEXTEST_RUNNER=runner run_script
expect_ok
[[ "$runner_log" == *$'\n--plan\n-p intentd --test suite_wss -E (binary_id(=intentd::suite_wss)&test(/^(e2e_wss_x)::/))\n'* ]] ||
  fail "$case_name runner argv: $runner_log"

# Structured export must describe the same argv without changing execution.
case_name="structured plan export preserves execution and empty selections"
reset_repo
edit crates/alpha/src/lib.rs
edit crates/beta/src/main.rs
run_script --instrumented --plan-json "$temp_dir/plans.json"
[[ "$status" -eq 0 ]] || fail "$case_name: $stderr"
expect_cov "-p alpha --lib --bins --tests" "-p beta --bins --tests"
python3 -I -B -S - "$temp_dir/plans.json" "$(g rev-parse HEAD)" <<'PYTHON'
import json, sys
p = json.load(open(sys.argv[1]))
assert p == {"version": 1, "head": sys.argv[2], "mergeBase": sys.argv[2],
             "plans": [["-p", "alpha", "--lib", "--bins", "--tests"],
                       ["-p", "beta", "--bins", "--tests"]]}, p
PYTHON
reset_repo
run_script --instrumented --dry-run --plan-json "$temp_dir/plans.json"
[[ "$status" -eq 0 && -z "$cargo_log" ]] || fail "$case_name empty: $stderr"
python3 -I -B -S - "$temp_dir/plans.json" <<'PYTHON'
import json, sys
assert json.load(open(sys.argv[1]))["plans"] == []
PYTHON
run_script --instrumented --plan-json "$temp_dir/absent/plans.json"
[[ "$status" -ne 0 && -z "$cargo_log" ]] || fail "$case_name ignored export failure"

# .config/e2e-test-map.txt: a changed source the map names also selects the
# end-to-end modules it maps to (intentd#2617: a script-binding change broke an
# end-to-end test only the merge queue ran), in the suite-module filter shape.
e2e_script_plan="-p intentd --test e2e_wss_agent_lifecycle --test suite_e2e -E (binary_id(=intentd::e2e_wss_agent_lifecycle)&test(/^(helper)::/))|(binary_id(=intentd::suite_e2e)&test(/^(e2e_core::inner)::/))"

case_name="mapped source selects its end-to-end modules"
reset_repo
edit crates/delta/src/script.rs
run_script
expect_ok
expect_cargo "-p delta --lib --bins --tests" "$e2e_script_plan"
[[ "$stdout" == *"[test-changed] note: crates/delta/src/script.rs is mapped to tests of other targets in .config/e2e-test-map.txt"* ]] ||
  fail "$case_name: no mapping note: $stdout"
[[ "$stdout" == *"[test-changed] cargo nextest run -p intentd --test e2e_wss_agent_lifecycle --test suite_e2e -E '(binary_id(=intentd::e2e_wss_agent_lifecycle)&test(/^(helper)::/))|(binary_id(=intentd::suite_e2e)&test(/^(e2e_core::inner)::/))'"* ]] ||
  fail "$case_name plan line is not a pasteable command: $stdout"

case_name="instrumented mapped source selects its end-to-end modules"
reset_repo
edit crates/delta/src/script.rs
run_script --instrumented
expect_ok
cov_expected="$repo: INTENTD_TEST_TIMEOUT_MULTIPLIER=3 llvm-cov --no-report nextest -p delta --lib --bins --tests -E not test(/^auggie_context_e2e::/)"$'\n'"$repo: INTENTD_TEST_TIMEOUT_MULTIPLIER=3 llvm-cov --no-report nextest -p intentd --test e2e_wss_agent_lifecycle --test suite_e2e -E ((binary_id(=intentd::e2e_wss_agent_lifecycle)&test(/^(helper)::/))|(binary_id(=intentd::suite_e2e)&test(/^(e2e_core::inner)::/))) and (not test(/^auggie_context_e2e::/))"
[[ "$cargo_log" == "$cov_expected" ]] || fail "$case_name: cargo argv was"$'\n'"$cargo_log"$'\n'"expected"$'\n'"$cov_expected"

case_name="mapped glob matches a new untracked source"
reset_repo
write crates/delta/src/monitor_v2.rs
run_script
expect_ok
expect_cargo "-p delta --lib --bins --tests" "-p intentd --test suite_e2e -E (binary_id(=intentd::suite_e2e)&test(/^(e2e_shared::helper)::/))"

case_name="map entry without a module selects the whole binary"
reset_repo
edit crates/delta/src/whole.rs
run_script
expect_ok
expect_cargo "-p delta --lib --bins --tests" "-p intentd --test suite_wss"

case_name="unmapped source in a mapped crate selects no end-to-end tests"
reset_repo
edit crates/delta/src/lib.rs
run_script
expect_ok
expect_cargo "-p delta --lib --bins --tests"
[[ "$stdout" != *"end-to-end"* ]] || fail "$case_name printed a mapping note: $stdout"

case_name="a broader selection of the mapped crate subsumes its end-to-end modules"
reset_repo
edit crates/delta/src/script.rs
edit crates/intentd/src/main.rs
run_script
expect_ok
expect_cargo "-p delta --lib --bins --tests" "-p intentd --bins --tests"

case_name="a mapped binary that also changed as a whole is named once"
reset_repo
edit crates/delta/src/script.rs
edit crates/intentd/tests/e2e_wss_agent_lifecycle.rs
run_script
expect_ok
expect_cargo "-p delta --lib --bins --tests" "-p intentd --test e2e_wss_agent_lifecycle --test suite_e2e -E binary_id(=intentd::e2e_wss_agent_lifecycle)|(binary_id(=intentd::suite_e2e)&test(/^(e2e_core::inner)::/))"

case_name="mapped modules join a changed module of the same suite"
reset_repo
edit crates/delta/src/monitor.rs
edit crates/intentd/tests/e2e_core.rs
run_script
expect_ok
expect_cargo "-p delta --lib --bins --tests" "-p intentd --test suite_e2e -E (binary_id(=intentd::suite_e2e)&test(/^(e2e_core|e2e_shared::helper)::/))"

case_name="editing the map selects every mapped end-to-end module, not the full suite"
reset_repo
echo "# a new comment" >>"$repo/.config/e2e-test-map.txt"
run_script
expect_ok
expect_cargo "-p intentd --test suite_wss --test e2e_wss_agent_lifecycle --test suite_e2e -E binary_id(=intentd::suite_wss)|(binary_id(=intentd::e2e_wss_agent_lifecycle)&test(/^(helper)::/))|(binary_id(=intentd::suite_e2e)&test(/^(e2e_core::inner|e2e_shared::helper)::/))"
[[ "$stdout" == *"[test-changed] note: .config/e2e-test-map.txt changed; every mapped test module runs"* ]] ||
  fail "$case_name: no map note: $stdout"

case_name="a checkout without the map keeps the per-crate selection"
reset_repo
g rm -q .config/e2e-test-map.txt
commit_all "no map"
edit crates/delta/src/script.rs
run_script --base HEAD
expect_ok
expect_cargo "-p delta --lib --bins --tests"

# Map validation: every invocation (and --check-map alone) rejects an entry
# whose source glob, test binary or module no longer exists, naming the line.
case_name="check-map accepts a valid map"
reset_repo
run_script --check-map
expect_ok
[[ "$stdout" == "[check-map] .config/e2e-test-map.txt: 4 entries valid" ]] || fail "$case_name printed '$stdout'"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"

# expect_map_error <expected stderr line>...: both a selection run and
# --check-map exit 2 naming every problem, without running cargo.
expect_map_error() {
  local mode line
  for mode in run check; do
    if [[ "$mode" == check ]]; then
      run_script --check-map
    else
      edit crates/alpha/tests/one.rs
      run_script
    fi
    [[ "$status" -eq 2 ]] || fail "$case_name ($mode) exited $status (expected 2): $stderr"
    for line in "$@"; do
      [[ "$stderr" == *"$line"* ]] || fail "$case_name ($mode): stderr is missing '$line':"$'\n'"$stderr"
    done
    [[ "$(grep -c '\.config/e2e-test-map\.txt:[0-9]*:' <<<"$stderr")" -eq $# ]] || fail "$case_name ($mode): expected $# problem line(s):"$'\n'"$stderr"
    [[ "$stderr" == *"invalid end-to-end test map"* ]] || fail "$case_name ($mode): no summary: $stderr"
    [[ -z "$cargo_log" ]] || fail "$case_name ($mode) invoked cargo: $cargo_log"
  done
}

case_name="map naming a removed module segment"
reset_repo
write crates/intentd/tests/e2e_core.rs '//! inner was removed'
expect_map_error ".config/e2e-test-map.txt:4: intentd::suite_e2e module e2e_core::inner: crates/intentd/tests/e2e_core.rs declares no 'mod inner;'"

case_name="map naming a module whose #[path] file is gone"
reset_repo
rm "$repo/crates/intentd/tests/e2e_shared/helper.rs"
expect_map_error ".config/e2e-test-map.txt:5: intentd::e2e_wss_agent_lifecycle module helper: crates/intentd/tests/e2e_shared/helper.rs does not exist" \
  ".config/e2e-test-map.txt:6: intentd::suite_e2e module e2e_shared::helper: crates/intentd/tests/e2e_shared/helper.rs does not exist"

case_name="map naming a removed module file"
reset_repo
rm "$repo/crates/intentd/tests/e2e_core/inner.rs"
expect_map_error ".config/e2e-test-map.txt:4: intentd::suite_e2e module e2e_core::inner: crates/intentd/tests/e2e_core/inner.rs does not exist"

case_name="map naming a removed test binary"
reset_repo
echo "crates/delta/src/lib.rs intentd::suite_gone e2e_core" >>"$repo/.config/e2e-test-map.txt"
expect_map_error ".config/e2e-test-map.txt:8: intentd::suite_gone: crates/intentd/tests/suite_gone.rs does not exist"

case_name="map source glob matching no file"
reset_repo
rm "$repo/crates/delta/src/whole.rs"
echo "crates/delta/src/gone*.rs intentd::suite_wss" >>"$repo/.config/e2e-test-map.txt"
expect_map_error ".config/e2e-test-map.txt:7: source glob 'crates/delta/src/whole.rs' matches no file" \
  ".config/e2e-test-map.txt:8: source glob 'crates/delta/src/gone*.rs' matches no file"

case_name="malformed map lines"
reset_repo
printf '%s\n' "crates/delta/src/lib.rs intentd" "crates/delta/src/lib.rs intentd::suite_e2e e2e_core extra" \
  "crates/delta/src/lib.rs intentd::suite_e2e e2e_core::" >>"$repo/.config/e2e-test-map.txt"
expect_map_error ".config/e2e-test-map.txt:8: expected '<source glob> <crate>::<test binary> [<module path>]'" \
  ".config/e2e-test-map.txt:9: expected '<source glob> <crate>::<test binary> [<module path>]'" \
  ".config/e2e-test-map.txt:10: intentd::suite_e2e module e2e_core::: empty segment"

case_name="check-map requires the map"
reset_repo
rm "$repo/.config/e2e-test-map.txt"
run_script --check-map
[[ "$status" -eq 2 ]] || fail "$case_name exited $status (expected 2): $stderr"
[[ "$stderr" == "[check-map] .config/e2e-test-map.txt does not exist" ]] || fail "$case_name stderr: $stderr"

case_name="map naming a test file that is not a test target"
reset_repo
write crates/intentd/Cargo.toml $'[package]\nname = "intentd"\nautotests = false\n\n[[test]]\nname = "suite_e2e"\n\n[[test]]\nname = "suite_wss"\n\n[[test]]\nname = "e2e_wss_agent_lifecycle"\n\n[dev-dependencies]\nname = "e2e_core"'
echo "crates/delta/src/lib.rs intentd::e2e_core inner" >>"$repo/.config/e2e-test-map.txt"
expect_map_error ".config/e2e-test-map.txt:8: intentd::e2e_core: crates/intentd/Cargo.toml sets autotests = false and declares no [[test]] named e2e_core"

case_name="autotests = false map with registered targets is valid"
reset_repo
write crates/intentd/Cargo.toml $'[package]\nname = "intentd"\nautotests = false\n\n[[test]]\nname = "suite_e2e"\n\n[[test]]\nname = "suite_wss"\n\n[[test]]\nname = "e2e_wss_agent_lifecycle"'
run_script --check-map
expect_ok
[[ "$stdout" == "[check-map] .config/e2e-test-map.txt: 4 entries valid" ]] || fail "$case_name printed '$stdout'"

# rustc resolves the children of a #[path]-loaded file in that file's own
# directory, as for a mod.rs file.
case_name="map module below a #[path]-loaded file"
reset_repo
write crates/intentd/tests/e2e_shared/helper.rs 'mod deep;'
write crates/intentd/tests/e2e_shared/deep.rs
echo "crates/delta/src/lib.rs intentd::suite_e2e e2e_shared::helper::deep" >>"$repo/.config/e2e-test-map.txt"
run_script --check-map
expect_ok
[[ "$stdout" == "[check-map] .config/e2e-test-map.txt: 5 entries valid" ]] || fail "$case_name printed '$stdout'"

case_name="map declarations: comments, attribute spacing, one-line attributes, inline modules"
reset_repo
write crates/intentd/tests/suite_lex.rs $'//! lex suite\n#[path = "lex/a.rs"]\n\n// a comment between the attribute and the module\nmod a;\n#[path="lex/b.rs"]\n#[cfg(unix)]\npub(crate) mod b;\n#[cfg(unix)] mod c;\n#[path = "lex/d.rs"] pub mod d;\nmod e {\n    mod f {}\n}\nmod g; // trailing'
write crates/intentd/tests/lex/a.rs
write crates/intentd/tests/lex/b.rs
write crates/intentd/tests/c.rs
write crates/intentd/tests/lex/d.rs
write crates/intentd/tests/g/mod.rs
printf '%s\n' "crates/delta/src/lib.rs intentd::suite_lex a" "crates/delta/src/lib.rs intentd::suite_lex b" \
  "crates/delta/src/lib.rs intentd::suite_lex c" "crates/delta/src/lib.rs intentd::suite_lex d" \
  "crates/delta/src/lib.rs intentd::suite_lex e::f" "crates/delta/src/lib.rs intentd::suite_lex g" >>"$repo/.config/e2e-test-map.txt"
run_script --check-map
expect_ok
[[ "$stdout" == "[check-map] .config/e2e-test-map.txt: 10 entries valid" ]] || fail "$case_name printed '$stdout'"

case_name="deleting the map needs the full suite"
reset_repo
g rm -q .config/e2e-test-map.txt
edit crates/delta/src/script.rs
run_script
[[ "$status" -eq 3 ]] || fail "$case_name exited $status (expected 3): $stderr"
[[ "$stderr" == *"  .config/e2e-test-map.txt"* ]] || fail "$case_name stderr: $stderr"
[[ -z "$cargo_log" ]] || fail "$case_name invoked cargo: $cargo_log"

case_name="instrumented run skips a mapped auggie_context_e2e module"
reset_repo
echo "crates/delta/src/lib.rs intentd::suite_e2e auggie_context_e2e" >>"$repo/.config/e2e-test-map.txt"
commit_all
edit crates/delta/src/lib.rs
run_script --instrumented --base HEAD
expect_ok
expect_cov "-p delta --lib --bins --tests"
: >"$temp_dir/cargo.log"
run_script --base HEAD
expect_ok
expect_cargo "-p delta --lib --bins --tests" "-p intentd --test suite_e2e -E (binary_id(=intentd::suite_e2e)&test(/^(auggie_context_e2e)::/))"

# <crate>::lib map entries select library unit-test modules of another crate
# (intentd#2543: an adapter pin bump in intent-providers broke intent-services
# unit tests that only the merge queue ran). alpha's lib gets an inline test
# module and a #[path] test module of a non-root file, as intent-services'
# model_preference::tests and agent_workers::tests are.
lib_fixture() {
  write crates/alpha/src/lib.rs $'mod util;\nmod workers;\n#[cfg(test)]\nmod tests {\n}'
  write crates/alpha/src/workers.rs $'#[cfg(test)]\n#[path = "workers_tests.rs"]\nmod tests;'
  write crates/alpha/src/workers_tests.rs
  printf '%s\n' "$@" >>"$repo/.config/e2e-test-map.txt"
  commit_all "lib fixture"
}

case_name="mapped source selects library unit-test modules of another crate"
reset_repo
lib_fixture "crates/delta/src/script.rs alpha::lib workers::tests" "crates/delta/src/script.rs alpha::lib tests"
edit crates/delta/src/script.rs
run_script --base HEAD
expect_ok
expect_cargo "-p alpha --lib -E (binary_id(=alpha)&test(/^(tests|workers::tests)::/))" "-p delta --lib --bins --tests" "$e2e_script_plan"
[[ "$stdout" == *"[test-changed] cargo nextest run -p alpha --lib -E '(binary_id(=alpha)&test(/^(tests|workers::tests)::/))'"* ]] ||
  fail "$case_name plan line is not a pasteable command: $stdout"

case_name="a whole-lib entry selects --lib without a filterset"
reset_repo
lib_fixture "crates/delta/src/whole.rs alpha::lib" "crates/delta/src/whole.rs alpha::lib tests"
edit crates/delta/src/whole.rs
run_script --base HEAD
expect_ok
expect_cargo "-p alpha --lib" "-p delta --lib --bins --tests" "-p intentd --test suite_wss"

case_name="library modules share one filterset with the crate's other mapped targets"
reset_repo
lib_fixture "crates/delta/src/whole.rs alpha::lib tests" "crates/delta/src/whole.rs alpha::one"
edit crates/delta/src/whole.rs
run_script --base HEAD
expect_ok
expect_cargo "-p alpha --lib --test one -E (binary_id(=alpha)&test(/^(tests)::/))|binary_id(=alpha::one)" "-p delta --lib --bins --tests" "-p intentd --test suite_wss"

case_name="a whole lib and a whole test binary need no filterset"
reset_repo
lib_fixture "crates/delta/src/whole.rs alpha::lib" "crates/delta/src/whole.rs alpha::one"
edit crates/delta/src/whole.rs
run_script --base HEAD
expect_ok
expect_cargo "-p alpha --lib --test one" "-p delta --lib --bins --tests" "-p intentd --test suite_wss"

case_name="a src change of the mapped crate subsumes its library modules"
reset_repo
lib_fixture "crates/delta/src/whole.rs alpha::lib tests"
edit crates/delta/src/whole.rs
edit crates/alpha/src/workers.rs
run_script --base HEAD
expect_ok
expect_cargo "-p alpha -p delta --lib --bins --tests" "-p intentd --test suite_wss"

case_name="instrumented library module ANDs the exclusion into its filterset"
reset_repo
lib_fixture "crates/delta/src/whole.rs alpha::lib tests"
edit crates/delta/src/whole.rs
run_script --instrumented --base HEAD
expect_ok
cov_expected="$repo: INTENTD_TEST_TIMEOUT_MULTIPLIER=3 llvm-cov --no-report nextest -p alpha --lib -E ((binary_id(=alpha)&test(/^(tests)::/))) and (not test(/^auggie_context_e2e::/))"$'\n'"$repo: INTENTD_TEST_TIMEOUT_MULTIPLIER=3 llvm-cov --no-report nextest -p delta --lib --bins --tests -E not test(/^auggie_context_e2e::/)"$'\n'"$repo: INTENTD_TEST_TIMEOUT_MULTIPLIER=3 llvm-cov --no-report nextest -p intentd --test suite_wss -E not test(/^auggie_context_e2e::/)"
[[ "$cargo_log" == "$cov_expected" ]] || fail "$case_name: cargo argv was"$'\n'"$cargo_log"$'\n'"expected"$'\n'"$cov_expected"

case_name="check-map accepts library modules"
reset_repo
lib_fixture "crates/delta/src/lib.rs alpha::lib tests" "crates/delta/src/lib.rs alpha::lib workers::tests" \
  "crates/delta/src/lib.rs alpha::lib util" "crates/delta/src/lib.rs alpha::lib"
run_script --check-map
expect_ok
[[ "$stdout" == "[check-map] .config/e2e-test-map.txt: 8 entries valid" ]] || fail "$case_name printed '$stdout'"

case_name="map naming a removed library module"
reset_repo
lib_fixture "crates/delta/src/lib.rs alpha::lib workers::tests" "crates/delta/src/lib.rs alpha::lib gone"
write crates/alpha/src/workers.rs '//! tests were removed'
expect_map_error ".config/e2e-test-map.txt:8: alpha::lib module workers::tests: crates/alpha/src/workers.rs declares no 'mod tests;'" \
  ".config/e2e-test-map.txt:9: alpha::lib module gone: crates/alpha/src/lib.rs declares no 'mod gone;'"

case_name="map naming the lib of a crate without one, or with a tests/lib.rs"
reset_repo
write crates/gamma/tests/lib.rs
printf '%s\n' "crates/delta/src/lib.rs beta::lib" "crates/delta/src/lib.rs gamma::lib" >>"$repo/.config/e2e-test-map.txt"
expect_map_error ".config/e2e-test-map.txt:8: beta::lib: crates/beta/src/lib.rs does not exist" \
  ".config/e2e-test-map.txt:9: gamma::lib is ambiguous: crates/gamma/tests/lib.rs also exists"

case_name="map naming a lib its manifest moves or opts out of tests"
reset_repo
write crates/alpha/Cargo.toml $'[package]\nname = "alpha"\n\n[lib]\ntest = false'
write crates/gamma/Cargo.toml $'[package]\nname = "gamma"\npath = "ignored-outside-lib"\n\n[lib]\npath = "src/other.rs"'
printf '%s\n' "crates/delta/src/lib.rs alpha::lib" "crates/delta/src/lib.rs gamma::lib" >>"$repo/.config/e2e-test-map.txt"
expect_map_error ".config/e2e-test-map.txt:8: alpha::lib: crates/alpha/Cargo.toml sets [lib] test = false" \
  ".config/e2e-test-map.txt:9: gamma::lib: crates/gamma/Cargo.toml sets [lib] path = \"src/other.rs\", not src/lib.rs"

case_name="a library module and a suite module of one crate share one filterset"
reset_repo
write crates/intentd/src/lib.rs $'#[cfg(test)]\nmod tests {\n}'
lib_fixture "crates/delta/src/lib.rs intentd::lib tests" "crates/delta/src/lib.rs intentd::suite_e2e e2e_core::inner"
edit crates/delta/src/lib.rs
run_script --base HEAD
expect_ok
expect_cargo "-p delta --lib --bins --tests" "-p intentd --lib --test suite_e2e -E (binary_id(=intentd)&test(/^(tests)::/))|(binary_id(=intentd::suite_e2e)&test(/^(e2e_core::inner)::/))"

case_name="editing the map also selects every mapped library module"
reset_repo
lib_fixture "crates/delta/src/lib.rs alpha::lib tests"
echo "# a new comment" >>"$repo/.config/e2e-test-map.txt"
run_script --base HEAD
expect_ok
expect_cargo "-p alpha --lib -E (binary_id(=alpha)&test(/^(tests)::/))" "-p intentd --test suite_wss --test e2e_wss_agent_lifecycle --test suite_e2e -E binary_id(=intentd::suite_wss)|(binary_id(=intentd::e2e_wss_agent_lifecycle)&test(/^(helper)::/))|(binary_id(=intentd::suite_e2e)&test(/^(e2e_core::inner|e2e_shared::helper)::/))"

echo "changed-tests tests passed under $("$script_bash" -c 'echo "bash $BASH_VERSION"')"
[[ -z "${CHANGED_TESTS_TEST_BASH:-}" ]] || exit 0

# Stock macOS /bin/bash is 3.2 (intent-hq/intent#4706): the monorepo Makefile
# runs this script there. `bash -n` alone accepts Bash 4+ builtins and
# expansions, so reject them by pattern too, then rerun the fixtures under a
# real Bash 3 when one can be found.
bash -n "$here/with-test-policy.sh" || fail "with-test-policy.sh does not parse"
bash -n "$script" || fail "changed-tests.sh does not parse"
bash -n "${BASH_SOURCE[0]}" || fail "test-changed-tests.sh does not parse"
bash4_constructs='(^|[^A-Za-z0-9_])(declare|local|typeset)([[:blank:]]+-[A-Za-z]+)*[[:blank:]]+-[A-Za-z]*[An][A-Za-z]*([^A-Za-z]|$)|(^|[^A-Za-z0-9_])(mapfile|readarray|coproc)([^A-Za-z0-9_]|$)|\$\{([A-Za-z_][A-Za-z_0-9]*|[0-9]+|[@*#?!$-])(\[[^]]*\])?(\^\^?|,,?)[^}]*\}|&>>|\|&|;;?&'
# Full-line comments, the pattern itself and the gate_sample calls below are
# not scanned.
gate_matches() {
  grep -nE "$bash4_constructs" "$@" | grep -vE '^([^:]*:)?[0-9]+:[[:blank:]]*#' |
    grep -v -F -e 'bash4_constructs' -e 'gate_sample' || true
}
gate_sample() {
  local expected=$1 sample=$2 hit
  hit=$(printf '%s\n' "$sample" | gate_matches)
  case "$expected:${hit:+hit}" in
    hit:hit | miss:) ;;
    *) fail "gate regex $expected sample misclassified: $sample" ;;
  esac
}
gate_sample hit 'declare -A m=()'
gate_sample hit 'local -n ref=x'
gate_sample hit 'mapfile -t a'
gate_sample hit 'echo ${var,,}'
gate_sample hit 'cmd |& tee'
gate_sample hit 'x) y ;;&'
gate_sample miss 'local path=$1 rest crate sub name'
gate_sample miss 'echo ${rest%%/*}'
gate_sample miss 'echo ${path##* -> }'
gate_sample miss 'x) y ;;'
gate_sample miss '# mapfile is unavailable on Bash 3'
gate_hits=$(gate_matches "$script" "$here/with-test-policy.sh" "${BASH_SOURCE[0]}")
[[ -z "$gate_hits" ]] || fail "Bash 4+ constructs found (stock macOS bash is 3.2):"$'\n'"$gate_hits"

# Lookup order, kept in step with the monorepo's scripts/find-bash3.sh (this
# repo stands alone, so it carries its own copy): BASH3_BIN, the `make bash3`
# cache (BASH3_CACHE_DIR, default ${XDG_CACHE_HOME:-~/.cache}/intent-bash3), a
# bash3 on PATH, Homebrew bash@3, then a 3.x /bin/bash.
bash3_cache_dir() {
  printf '%s\n' "${BASH3_CACHE_DIR:-${XDG_CACHE_HOME:-${HOME:-}/.cache}/intent-bash3}"
}

find_bash3() {
  local candidate resolved brew_prefix cache_bash
  brew_prefix=$(brew --prefix bash@3 2>/dev/null) || brew_prefix=""
  cache_bash="$(bash3_cache_dir)/bash"
  for candidate in "${BASH3_BIN:-}" "$cache_bash" bash3 "${brew_prefix:+$brew_prefix/bin/bash}" \
    /opt/homebrew/opt/bash@3/bin/bash /usr/local/opt/bash@3/bin/bash /bin/bash; do
    [[ -n "$candidate" ]] || continue
    resolved=$(command -v "$candidate" 2>/dev/null) || continue
    [[ -x "$resolved" ]] || continue
    "$resolved" -c '[[ "${BASH_VERSINFO[0]}" -eq 3 ]]' 2>/dev/null || continue
    printf '%s\n' "$resolved"
    return 0
  done
  return 1
}

# How to get an interpreter, for the skip notice and the REQUIRE_BASH3 failure
# (the monorepo's bash3_missing_hint wording).
bash3_missing_hint() {
  printf '%s\n' "run 'make bash3' from the monorepo to build one into $(bash3_cache_dir), or point BASH3_BIN at one"
}

# No Bash 3 found: a skip notice by default; a non-empty REQUIRE_BASH3 (CI)
# makes it a failure instead.
report_missing_bash3() {
  if [[ -n "${REQUIRE_BASH3:-}" ]]; then
    fail "no Bash 3 interpreter found and REQUIRE_BASH3 is set ($(bash3_missing_hint); or unset REQUIRE_BASH3 to skip the real 3.2 run)"
  fi
  echo "changed-tests tests: no Bash 3 interpreter found ($(bash3_missing_hint)); real 3.2 run skipped, static gate only"
}

# The lookup finds the `make bash3` cache on its own, in the monorepo's
# scripts/find-bash3.sh order. PATH is an empty dir so a bash3 or brew on the
# host cannot answer instead of the stubs.
lookup_dir="$temp_dir/bash3-lookup"
mkdir -p "$lookup_dir/empty-path" "$lookup_dir/xdg/intent-bash3" "$lookup_dir/home/.cache/intent-bash3" \
  "$lookup_dir/moved"
for stub in "$lookup_dir/xdg/intent-bash3/bash" "$lookup_dir/home/.cache/intent-bash3/bash" \
  "$lookup_dir/moved/bash" "$lookup_dir/explicit-bash"; do
  printf '#!/bin/sh\nexit 0\n' >"$stub"
  chmod 755 "$stub"
done
run_lookup() {
  (
    unset BASH3_BIN BASH3_CACHE_DIR XDG_CACHE_HOME
    # shellcheck disable=SC2123 # an empty search path is the point
    PATH="$lookup_dir/empty-path"
    for assignment in "$@"; do export "${assignment?}"; done
    find_bash3
  ) || true
}
[[ "$(run_lookup XDG_CACHE_HOME="$lookup_dir/xdg")" == "$lookup_dir/xdg/intent-bash3/bash" ]] ||
  fail "find_bash3 does not find the make bash3 cache under XDG_CACHE_HOME"
[[ "$(run_lookup HOME="$lookup_dir/home")" == "$lookup_dir/home/.cache/intent-bash3/bash" ]] ||
  fail "find_bash3 does not find the make bash3 cache under ~/.cache"
[[ "$(run_lookup BASH3_CACHE_DIR="$lookup_dir/moved" XDG_CACHE_HOME="$lookup_dir/xdg")" == "$lookup_dir/moved/bash" ]] ||
  fail "find_bash3 does not honor BASH3_CACHE_DIR"
[[ "$(run_lookup BASH3_BIN="$lookup_dir/explicit-bash" XDG_CACHE_HOME="$lookup_dir/xdg")" == "$lookup_dir/explicit-bash" ]] ||
  fail "find_bash3 does not prefer BASH3_BIN over the cache"
# A rejected cache falls through: to nothing, or to a stock 3.x /bin/bash
# (macOS), the only candidate an empty PATH leaves.
printf '#!/bin/sh\nexit 1\n' >"$lookup_dir/moved/bash"
fallthrough=$(run_lookup BASH3_CACHE_DIR="$lookup_dir/moved")
[[ -z "$fallthrough" || "$fallthrough" == /bin/bash ]] ||
  fail "find_bash3 does not fall through past a cached binary that is not Bash 3: got $fallthrough"
# Without an interpreter, the notice names `make bash3` and REQUIRE_BASH3 turns
# the skip into a failure.
missing=$(BASH3_CACHE_DIR="$lookup_dir/none" REQUIRE_BASH3='' report_missing_bash3) ||
  fail "the Bash 3 skip notice exits non-zero without REQUIRE_BASH3"
[[ "$missing" == *"run 'make bash3'"*"$lookup_dir/none"*"real 3.2 run skipped"* ]] ||
  fail "the Bash 3 skip notice does not name make bash3 and the cache: $missing"
if missing=$( (BASH3_CACHE_DIR="$lookup_dir/none" REQUIRE_BASH3=1 report_missing_bash3) 2>&1); then
  fail "REQUIRE_BASH3=1 does not fail when no Bash 3 is found"
fi
[[ "$missing" == *"REQUIRE_BASH3 is set"*"run 'make bash3'"* ]] ||
  fail "the REQUIRE_BASH3 failure does not name make bash3: $missing"

if [[ "${BASH_VERSINFO[0]}" -eq 3 ]]; then
  : # the fixtures above already ran under Bash 3
elif bash3=$(find_bash3); then
  CHANGED_TESTS_TEST_BASH="$bash3" "$bash3" "${BASH_SOURCE[0]}"
else
  report_missing_bash3
fi
