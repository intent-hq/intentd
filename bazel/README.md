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
CI builds with `--lockfile_mode=error`, so a stale lock fails the job.

crate_universe would record the `[patch]` path crate (`vendor/pprof`) by its
absolute host path, which made the lock checkout-specific.
`bazel/rules_rust_relative_path_crates.patch`, applied to rules_rust through
`single_version_override` in `MODULE.bazel`, stores it relative to the
workspace root instead. Drop the patch once rules_rust does this upstream.

crate_universe also writes a `BUILD.bazel` into `vendor/pprof` while it
resolves. That file is git-ignored, and `.bazelignore` keeps the directory out
of the main repo's packages.

## Generated files

`scripts/bazel-gen.py` writes `crates/*/BUILD.bazel` and
`bazel/generated.MODULE.bazel` from `cargo metadata` and
`rust-toolchain.toml`. Do not edit them by hand.

```sh
python3 scripts/bazel-gen.py          # regenerate after a Cargo.toml change
python3 scripts/bazel-gen.py --check  # exit 1 when a generated file is stale
```

Rerun the generator when you add or remove a crate, a first-party (path)
dependency, a feature, a binary or a `build.rs`, or when you bump
`rust-toolchain.toml`. Third-party dependency changes need no Bazel edit.

Each generated package has:

- `rust_library` / `rust_proc_macro` named after the crate (`intent_core`),
  and `rust_binary` targets named after the Cargo bins (`intentd`,
  `intentd-sitter`), so outputs keep Cargo's file names.
- `COMPILE_DATA`: every non-`.rs` file in the package outside `tests/`,
  `benches/` and `examples/`, so `include_str!`, `include_bytes!` and
  `sqlx::migrate!()` inputs need no listing.
- `cargo_build_script` for a `build.rs`, unless `OVERRIDES` in the generator
  replaces it.

Hand decisions that Cargo metadata cannot express live in two places only:

- `OVERRIDES` in `scripts/bazel-gen.py` (per first-party crate).
- `bazel/crates.MODULE.bazel` (crate_universe annotations for third-party
  crates).

## Extending

- **Tests and lints**: add them in the generator (`rust_test` per lib, bin and
  `tests/*.rs`, using `all_crate_deps(normal_dev = True)` and
  `proc_macro_dev = True`) rather than in the generated files. Test-only
  first-party features (`intent-providers/test-support`,
  `intent-services/repository-test-fixtures`) need feature-enabled library
  variants, because Bazel has no Cargo-style feature unification.

## Drift guard

```sh
python3 scripts/bazel-drift.py          # exit 1, with the fix command, on drift
python3 scripts/bazel-drift.py --fix    # regenerate, refresh MODULE.bazel.lock, recheck
```

The check fails when a generated file is stale (`bazel-gen.py --check`), when
`MODULE.bazel.lock` records a host-absolute path, or when the lock no longer
matches `MODULE.bazel` or the Cargo manifests and `Cargo.lock`
(`bazel mod deps --lockfile_mode=error`; crate_universe records their digests).
A third-party bump in `Cargo.lock` therefore needs only `--fix` and a commit of
the refreshed lock. Set `BAZEL` to pick the launcher (default `bazelisk`, then
`bazel`). Its unit tests are `python3 -B scripts/test-bazel-ci.py`.

## CI

`.github/workflows/bazel.yml` is informational (not a required check, not in
the merge queue) and runs on pull requests and `main` pushes that touch Rust or
Bazel inputs:

- `drift`: the drift guard above, on a small runner.
- `bazel`: `bazel build //...`, `bazel test --build_tests_only //...`, then
  `bazel build --config=<name> //...` for each lint config (`clippy`,
  `rustfmt`) that `.bazelrc` defines. Every command uses `--config=ci`: plain
  logs, a frozen lock and `--nocache_test_results`, because tests still read
  undeclared host inputs. Drop that flag once test inputs are audited.

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
