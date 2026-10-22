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
lockfile change with `scripts/bazel-drift.sh --fix` (see "Drift guard" below).
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
- **CI drift guard**: run `python3 scripts/bazel-gen.py --check`.
- **Remote cache**: put `--remote_cache` settings behind a named config in
  `.bazelrc`, and keep per-user settings in the git-ignored `user.bazelrc`.
