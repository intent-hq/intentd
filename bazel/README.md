# Bazel build (parallel to Cargo)

intentd builds with Bazel (bzlmod, `rules_rust`, `crate_universe`) alongside
Cargo. **Cargo stays the source of truth.** Nothing in the Cargo, CI or release
paths depends on these files.

```sh
bazel build //...                 # every crate and both binaries
bazel build //:intentd            # alias of //crates/intentd
bazel-bin/crates/intentd/intentd --version
```

Install [bazelisk](https://github.com/bazelbuild/bazelisk) as `bazel`; it reads
`.bazelversion`.

## Where things come from

| Input | Source of truth | Bazel side |
|---|---|---|
| Third-party crates and features | `Cargo.toml` + `Cargo.lock` | `crate.from_cargo` resolves them on fetch; BUILD files call `all_crate_deps()` |
| Rust toolchain version | `rust-toolchain.toml` | `bazel/generated.MODULE.bazel` |
| Workspace members, first-party deps, default features, build scripts, bins | `cargo metadata` | generated `crates/*/BUILD.bazel` |
| Build commit (`system.info`, hello handshake, startup banner) | `git rev-parse HEAD` or `INTENTD_BUILD_COMMIT` | `bazel/workspace_status.sh` → `//bazel:build_commit_env` |

`Cargo.lock` is the dependency lock. `MODULE.bazel.lock` is committed: it
records the `bazel_dep` registry hashes and crate_universe's resolution of
`Cargo.toml` + `Cargo.lock`, so a checkout with an up-to-date lock skips the
roughly one-minute Cargo resolution. Refresh it after any Cargo manifest or
lockfile change with `python3 scripts/bazel-drift.py --fix` (see "Drift guard" below).
The `drift` CI job fails on a stale lock. **Releases need no Bazel action**: a
release-plz bump that changes only workspace-crate versions is tolerated (see
"Drift guard").

crate_universe would record the `[patch]` path crate (`vendor/pprof`) by its
absolute host path, which made the lock checkout-specific.
`bazel/rules_rust_relative_path_crates.patch`, applied to rules_rust through
`single_version_override` in `MODULE.bazel`, stores it relative to the
workspace root instead. Drop the patch once rules_rust does this upstream.

crate_universe also writes a `BUILD.bazel` into `vendor/pprof` while it
resolves. That file is git-ignored, and `.bazelignore` keeps the directory out
of the main repo's packages.

## Generated files

`scripts/bazel-gen.py` writes `crates/*/BUILD.bazel`, the root `BUILD.bazel`,
`bazel/generated.MODULE.bazel` and `bazel/prod-workspace/**` from
`cargo metadata`, `rust-toolchain.toml` and `.config/nextest.toml`. Do not
edit them by hand.

```sh
python3 scripts/bazel-drift.py --fix  # regenerate and refresh MODULE.bazel.lock
python3 scripts/bazel-gen.py --check  # exit 1 when a generated file is stale
```

Regenerate when you add or remove a crate, a first-party (path) dependency, a
feature, a binary, a `build.rs` or a `[[test]]` entry, or when you bump
`rust-toolchain.toml` or edit `.config/nextest.toml`. **A new test file needs
no Bazel action** (see "Tests"). Third-party dependency changes need only the
lock refresh that `--fix` does.

Each generated package has:

- `rust_library` / `rust_proc_macro` named after the crate (`intent_core`),
  and `rust_binary` targets named after the Cargo bins (`intentd`,
  `intentd-sitter`), so outputs keep Cargo's file names.
- `<crate>_dev` twins of the library and binaries for the test graph (see
  "Tests").
- `COMPILE_DATA`: every non-`.rs` file in the package outside `tests/`,
  `benches/` and `examples/`, so `include_str!`, `include_bytes!` and
  `sqlx::migrate!()` inputs need no listing.
- `cargo_build_script` for a `build.rs`, unless `OVERRIDES` in the generator
  replaces it.
- one `rust_integration_tests(...)` call, plus explicit test rules for the
  unit tests and for split integration tests.

Hand decisions that Cargo metadata cannot express live in two places only:

- the tables at the top of `scripts/bazel-gen.py`: `OVERRIDES` (per crate),
  `MANUAL_TESTS`, `KNOWN_FAILING`, `MODULE_SHARDS`, `MANUAL_SHARDS`,
  `TEST_TIMEOUTS` and `FIXTURE_TESTS` (tests).
- `bazel/crates.MODULE.bazel` (crate_universe annotations for third-party
  crates).

## Tests

```sh
bazel test //...                                   # every non-manual test target
bazel test //crates/intent-core/...                # one crate
bazel test //crates/intentd:e2e_wss_git_reads      # one integration test file
bazel test //crates/intent-core:lib_test --test_arg=ids::  # libtest filter
bazel query 'attr(tags, manual, tests(//...))'     # list the manual targets
```

Targets per crate, all built against the test graph (`@crates_dev` and the
`_dev` libraries, so dev-only features never reach production targets):

| Cargo / nextest | Bazel target |
|---|---|
| lib unit tests | `lib_test` |
| bin unit tests (`bin/<name>`) | `bin_<name>_test` |
| `tests/<name>.rs`, `tests/<name>/main.rs` | `<name>` |

`.bazelrc` maps nextest's `retries = 1` to `--flaky_test_attempts=2`.
Bazel times out whole test binaries (unit-test binaries get `long`, 900 s)
rather than single tests. nextest overrides that reserve every test thread
become the `exclusive` tag; tests in a nextest test-group or listed by test
name run in a `<target>_serial` companion with `--test-threads=1`.
Tests see the host `PATH` (`test --test_env=PATH`) so they find `node` and other
host tools as under Cargo. This is a known non-hermetic input, to be replaced by a
hermetic node toolchain (rules_nodejs) in the test-hermeticity phase.

### Adding a test

Add the file; nothing else. `bazel/rust_integration_tests.bzl` globs
`tests/*.rs` and `tests/*/main.rs` at load time, like Cargo's autodiscovery,
and gives each test the crate's dependencies, test binaries
(`CARGO_BIN_EXE_*`), environment and tags. `tests/<dir>/**` modules are
compiled into every test of the crate. A crate's first test file also needs
nothing, since every crate has the call, and a new unit test normally needs
nothing either.

The drift guard fails on a new test only when it needs a policy the defaults
do not give it. Run `python3 scripts/bazel-drift.py --fix` and commit the
regenerated BUILD file when you:

- add a test that reads other crates' Rust sources at run time and is not
  named `*_lint` (the generator detects `source_lint`, `workspace_root(` or a
  `../…rs` path in the sources; `*_lint` tests get the sources by name), or
  uses `env!("CARGO")`. The same holds for the first unit test of a crate
  that does so, since `lib_test` covers all of `src/`;
- name a new test in `.config/nextest.toml` or one of the generator's tables;
- add a crate, or a `[[test]]` entry in a `Cargo.toml`.

The per-test table in each `rust_integration_tests(overrides = ...)` call
holds those deviations (`manual` with its reason, `exclusive`, `timeout`,
`workspace_sources`, `cargo`). The macro fails loading when an override names
a test file that no longer exists, so stale entries do not linger.

### Manual targets

`bazel test //...` skips targets tagged `manual`; run them by label. Every
one has its reason next to it in the generated BUILD file and in the
generator's tables. They are:

- tests Bazel cannot run as an action (trybuild in `store_pool_api` runs
  Cargo itself) or whose `required-features` are off in
  `cargo test --workspace`;
- intent-services unit-test shards that time out or fail nondeterministically
  in one shared libtest process (`MANUAL_SHARDS`);
- `*_fixture` companions: unit tests needing a fixture prepared outside the
  repository (`FIXTURE_TESTS`). They inherit `INTENT_ACP_CALLBACK_ADAPTER_FIXTURE`
  / `TRANSFER_SELECTION_FIXTURE_ROOT` from your environment, as the monorepo
  `make test` sets them;
- `*_known_failing` companions (below).

### Known-failing companions

The suite is written for nextest's process-per-test model; libtest runs a
whole binary in one process, so some tests that share process-global state or
tight wall-clock budgets fail under Bazel and pass under nextest. Each is
listed by name, with a reason, in `KNOWN_FAILING`. The main target skips it
(`--skip`), and a manual `<target>_known_failing` companion runs exactly those
tests (`--exact`), so they stay visible:

```sh
bazel test //crates/intentd:e2e_wss_script_persistence_known_failing
```

Remove a test from the table once it passes under Bazel.

When one binary backs several targets (shards, `_serial`, `_known_failing`,
`_fixture`), it is compiled once as `<target>_binary` and the others run it
through `bazel/shared_binary_test.bzl`.

### intent-services shards

Its unit-test binary (~7,600 tests) is split by top-level module into
`lib_test_shard1`…`8`, plus `lib_test_shard_rest` for every module no shard
names. The assignment is the fixed `MODULE_SHARDS` table, not computed from
file sizes, so editing or adding unit tests never changes the BUILD file; a
new module runs in `_shard_rest`. To rebalance, paste the output of
`python3 scripts/bazel-gen.py --suggest-shards intent-services` into the
table, check the `MANUAL_SHARDS` reasons still match their modules, and
regenerate.

## Drift guard

```sh
python3 scripts/bazel-drift.py          # exit 1, with the fix command, on drift
python3 scripts/bazel-drift.py --fix    # regenerate, refresh MODULE.bazel.lock, recheck
```

The check fails when a generated file is stale (`bazel-gen.py --check`, which
lists each file and marks a missing crate BUILD file as a new crate), when
`MODULE.bazel.lock` records a host-absolute path, or when the lock no longer
matches `MODULE.bazel` or the Cargo manifests and `Cargo.lock`
(`bazel mod deps --lockfile_mode=error`; crate_universe records their digests).
A third-party bump in `Cargo.lock` therefore needs only `--fix` and a commit of
the refreshed lock.

**Release bumps pass.** The generated BUILD files carry no crate versions:
each loads `CARGO_VERSIONS` from `@cargo_versions`, which
`bazel/cargo_versions.bzl` reads from the current `Cargo.toml` files when Bazel
loads the build. Bazel watches those manifests, so after a bump the binaries
report the new `CARGO_PKG_VERSION` (`--version`) with no regeneration. The
extension is reproducible, so the lock does not record it. When the only lock
difference is workspace-crate versions (each crate's `version`, path
dependencies' `version`, and source-less `Cargo.lock` packages), the guard
prints a "tolerated" note and exits 0, so a release-plz PR and the PRs after it
stay green with no Bazel action. It compares today's Cargo files against the
ones recorded in the lock, recovered from the commit that last changed
`MODULE.bazel.lock` (CI checks out full history for this), then reruns
`mod deps --lockfile_mode=error` with those recorded files temporarily put
back: if the lock is still stale, `MODULE.bazel` or a crate annotation changed
alongside the bump, and the guard fails. Third-party versions, features, path
changes, annotations or a new crate still fail. `--fix` at any time refreshes everything. Set `BAZEL` to pick the launcher (default `bazelisk`, then
`bazel`). Its unit tests are `python3 -B scripts/test-bazel-ci.py`.

## CI

`.github/workflows/bazel.yml` is informational (not a required check, not in
the merge queue) and runs on pull requests and `main` pushes that touch Rust or
Bazel inputs:

- `drift`: the drift guard above, on a small runner.
- `bazel`: `bazel build //...`, `bazel test --build_tests_only //...`, then
  `bazel build --config=<name> //...` for each lint config (`clippy`,
  `rustfmt`) that `.bazelrc` defines. Every command uses `--config=ci`: plain
  logs and `--nocache_test_results`, because tests still read undeclared host
  inputs (drop that flag once they are audited). `--config=ci` also caps test concurrency at
  `--local_test_jobs=4`: each libtest binary already uses every CPU, and Bazel
  has no equivalent of nextest's thread cap or serial groups across binaries, so
  an uncapped run overloads the 8-CPU runner and timing-sensitive tests fail. This is a
  known limitation until the process-per-test phase. The lock stays in `update`
  mode, so after a release bump the job re-resolves from `Cargo.lock`; the
  refreshed lock is never committed or uploaded, and the `drift` job is what
  enforces it.

The job installs `libssl-dev`, `zlib1g-dev` and `pkg-config` when missing,
since openssl-sys and libz-sys link the system libraries on linux-gnu. The
repository cache and disk cache travel through the GitHub Actions cache:
pull requests only restore them, and `main` pushes save them. The disk cache
key includes a fingerprint of the OS release, C compiler and OpenSSL/zlib
versions, so non-hermetic native actions are never reused on a machine with
different system libraries.

## Remote cache

`--config=remote-cache` reads from a self-hosted HTTP cache (bazel-remote
behind the cache host's token gate, per the CI research). It never uploads
(`--remote_upload_local_results=false`); `--config=remote-cache-write` adds
uploads for trusted `main` runs. The endpoint and token come from the
environment:

```sh
export BAZEL_REMOTE_CACHE_URL=https://cache.example/bazel   # endpoint
export BAZEL_REMOTE_CACHE_TOKEN=...                          # read-only token
bazel build --config=remote-cache --remote_cache="$BAZEL_REMOTE_CACHE_URL" //...
```

`bazel/remote-cache-credentials.sh` is the credential helper. It sends the
token as a Bearer header only to `BAZEL_REMOTE_CACHE_URL`'s host. Put the two
flags in the git-ignored `user.bazelrc` to make them permanent. In CI, set the
`BAZEL_REMOTE_CACHE_URL` variable and the `BAZEL_REMOTE_CACHE_RO_TOKEN` /
`BAZEL_REMOTE_CACHE_RW_TOKEN` secrets; the workflow then enables the cache with
no code change. Pull requests always get the read-only token and config, and
the cache server must reject writes from that token as well.
