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
script="$repo/scripts/changed-tests.sh"
trap 'rm -rf "$temp_dir"' EXIT

fail() {
  echo "changed-tests test failed: $*" >&2
  exit 1
}

for command in bash dirname mktemp rm sort; do
  ln -s "$(command -v "$command")" "$bin_dir/$command"
done

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
# "<cwd>: [INTENTD_TEST_TIMEOUT_MULTIPLIER=N ]<argv>" and exits with
# CARGO_STUB_EXIT (default 0). It drains stdin like a real nextest child
# could, so the script must not feed it the remaining plans.
cat >"$bin_dir/cargo" <<'SH'
#!/usr/bin/env bash
printf '%s: %s%s\n' "$PWD" "${INTENTD_TEST_TIMEOUT_MULTIPLIER:+INTENTD_TEST_TIMEOUT_MULTIPLIER=$INTENTD_TEST_TIMEOUT_MULTIPLIER }" "$*" >>"$CARGO_TEST_LOG"
while IFS= read -r _; do :; done
exit "${CARGO_STUB_EXIT:-0}"
SH
chmod +x "$bin_dir/cargo"

# Stub runner: appends "call:" plus one line per argv word to RUNNER_TEST_LOG,
# its cwd to RUNNER_CWD_LOG, and exits with RUNNER_STUB_EXIT (default 0).
cat >"$bin_dir/runner" <<'SH'
#!/usr/bin/env bash
{ echo "call:"; printf '%s\n' "$@"; } >>"$RUNNER_TEST_LOG"
printf '%s\n' "$PWD" >>"$RUNNER_CWD_LOG"
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
# intentd carries the auggie_context_e2e binary the instrumented run excludes.
write() {
  mkdir -p "$repo/$(dirname "$1")"
  printf '%s\n' "${2:-$1}" >"$repo/$1"
}
write Cargo.toml '[workspace]'
write Cargo.lock
write rust-toolchain.toml
write .config/nextest.toml
write .cargo/config.toml
write crates/alpha/Cargo.toml
write crates/alpha/src/lib.rs
write crates/alpha/src/util/mod.rs
write crates/alpha/tests/one.rs
write crates/alpha/tests/two.rs
write crates/alpha/tests/common/mod.rs
write crates/alpha/tests/fixtures/data.json
write crates/alpha/tests/fixtures/café.json
write crates/alpha/benches/bench.rs
write crates/alpha/examples/demo.rs
write crates/beta/Cargo.toml
write crates/beta/build.rs
write crates/beta/src/main.rs
write crates/beta/tests/smoke.rs
write crates/beta/migrations/001.sql
write crates/gamma/Cargo.toml
write crates/gamma/src/lib.rs
write crates/gamma/tests/smoke.rs
write crates/intentd/Cargo.toml
write crates/intentd/src/main.rs
write crates/intentd/tests/auggie_context_e2e.rs
write crates/intentd/tests/e2e_wss_agent_lifecycle.rs
write README.md
write docs/guide.md
write scripts/tool.sh
g init -q
g add -A
g commit -q -m base
base_sha=$(g rev-parse HEAD)
g update-ref refs/remotes/origin/main HEAD
g checkout -q -b feature

reset_repo() {
  g reset -q --hard refs/remotes/origin/main
  g clean -fdq
  : >"$temp_dir/cargo.log"
  : >"$temp_dir/runner.log"
  : >"$temp_dir/runner-cwd.log"
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
# fixture checkout); it must find its repo root on its own.
run_script() {
  set +e
  (
    cd "${RUN_CWD:-$repo}" &&
      PATH="$bin_dir" BASE="${BASE-}" BUILD_JOBS="${BUILD_JOBS-}" TEST_THREADS="${TEST_THREADS-}" \
        DRY_RUN="${DRY_RUN-}" NEXTEST_RUNNER="${NEXTEST_RUNNER-}" \
        CARGO_TEST_LOG="$temp_dir/cargo.log" RUNNER_TEST_LOG="$temp_dir/runner.log" \
        RUNNER_CWD_LOG="$temp_dir/runner-cwd.log" \
        "$script_bash" "$script" "$@"
  ) >"$temp_dir/stdout" 2>"$temp_dir/stderr"
  status=$?
  set -e
  stdout=$(<"$temp_dir/stdout")
  stderr=$(<"$temp_dir/stderr")
  cargo_log=$(<"$temp_dir/cargo.log")
  runner_log=$(<"$temp_dir/runner.log")
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
    expected+="$repo: INTENTD_TEST_TIMEOUT_MULTIPLIER=3 llvm-cov --no-report nextest $line -E not binary(auggie_context_e2e)"$'\n'
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
    [[ "$stdout" == *"[coverage-changed] INTENTD_TEST_TIMEOUT_MULTIPLIER=3 cargo llvm-cov --no-report nextest $line -E 'not binary(auggie_context_e2e)'"* ]] || fail "$case_name: plan is missing '$line':"$'\n'"$stdout"
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
[[ "$stderr" == "[coverage-changed] INTENTD_TEST_TIMEOUT_MULTIPLIER=3 cargo llvm-cov --no-report nextest -p alpha --test one -E 'not binary(auggie_context_e2e)' exited 100" ]] || fail "$case_name stderr: $stderr"

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
# only that binary would select nothing; the plain run still names it.
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
expect_cargo "-p intentd --test auggie_context_e2e --test e2e_wss_agent_lifecycle"

echo "changed-tests tests passed under $("$script_bash" -c 'echo "bash $BASH_VERSION"')"
[[ -z "${CHANGED_TESTS_TEST_BASH:-}" ]] || exit 0

# Stock macOS /bin/bash is 3.2 (intent-hq/intent#4706): the monorepo Makefile
# runs this script there. `bash -n` alone accepts Bash 4+ builtins and
# expansions, so reject them by pattern too, then rerun the fixtures under a
# real Bash 3 when one can be found.
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
gate_hits=$(gate_matches "$script" "${BASH_SOURCE[0]}")
[[ -z "$gate_hits" ]] || fail "Bash 4+ constructs found (stock macOS bash is 3.2):"$'\n'"$gate_hits"

find_bash3() {
  local candidate resolved brew_prefix
  brew_prefix=$(brew --prefix bash@3 2>/dev/null) || brew_prefix=""
  for candidate in "${BASH3_BIN:-}" bash3 "${brew_prefix:+$brew_prefix/bin/bash}" \
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

if [[ "${BASH_VERSINFO[0]}" -eq 3 ]]; then
  : # the fixtures above already ran under Bash 3
elif bash3=$(find_bash3); then
  CHANGED_TESTS_TEST_BASH="$bash3" "$bash3" "${BASH_SOURCE[0]}"
else
  echo "changed-tests tests: no Bash 3 interpreter found (set BASH3_BIN); real 3.2 run skipped, static gate only"
fi
